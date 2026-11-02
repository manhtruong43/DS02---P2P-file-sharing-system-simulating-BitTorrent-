"""
Kiểm thử các cơ chế kiểm tra đầu vào & toàn vẹn được bổ sung sau đợt rà soát mã:

  1. Metadata Generator từ chối tham số sai (chunk_size 0 / âm / quá lớn, file rỗng, file không tồn tại).
  2. Đọc metadata từ chối file .torrent.json bị sửa / không nhất quán.
  3. Seeder từ chối khởi động khi file nguồn không khớp metadata (sai kích thước hoặc sai nội dung).
  4. Kiểm tra hash toàn file: phân biệt tải THÀNH CÔNG / THẤT BẠI, kể cả khi file đã đủ chunk từ đầu,
     và tự tải lại chunk bị hỏng trên đĩa.
  5. Xác thực Tracker: không ghi giá trị key vào log; tracker chưa cấu hình key thì từ chối mọi request.
"""
import os
import sys
import json
import asyncio
import hashlib
import logging
import tempfile

import pytest
from fastapi.testclient import TestClient

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.common.config import TRACKER_API_KEY, MAX_CHUNK_SIZE
from src.common.metadata import TorrentMetadata, MetadataError
from src.peer.chunk_manager import SourceFileError
from src.peer.peer_node import PeerNode
import src.tracker.tracker as tracker_module


def _temp_file(data: bytes) -> str:
    with tempfile.NamedTemporaryFile(delete=False) as tf:
        tf.write(data)
        return tf.name


def _cleanup(*paths):
    for p in paths:
        if p and os.path.exists(p):
            os.remove(p)


def test_1_metadata_generator_rejects_invalid_parameters():
    """Validation Test 1: chunk_size phải trong [1, 3 MB]; không chia sẻ file rỗng hoặc file không tồn tại."""
    src = _temp_file(b"12345678")
    empty = _temp_file(b"")
    try:
        for bad_chunk in (0, -1, MAX_CHUNK_SIZE + 1, 5 * 1024 * 1024, True, "256"):
            with pytest.raises(MetadataError):
                TorrentMetadata(src, chunk_size=bad_chunk).generate()
        with pytest.raises(MetadataError):
            TorrentMetadata(empty, chunk_size=4).generate()
        with pytest.raises(FileNotFoundError):
            TorrentMetadata(src + ".does_not_exist", chunk_size=4).generate()

        # Tham số hợp lệ vẫn hoạt động: 8 byte, chunk 4 byte -> 2 chunk
        data = TorrentMetadata(src, chunk_size=4).generate()
        assert data["num_chunks"] == 2
    finally:
        _cleanup(src, empty)


def test_2_metadata_load_rejects_tampered_or_inconsistent_files():
    """Validation Test 2: file .torrent.json bị sửa hoặc không nhất quán phải bị từ chối khi đọc."""
    src = _temp_file(os.urandom(1000))
    meta_path = src + ".torrent.json"
    try:
        good = TorrentMetadata(src, chunk_size=256).generate()  # 4 chunk

        def write_and_load(mutate):
            data = json.loads(json.dumps(good))
            data.pop("file_info", None)
            mutate(data)
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(data, f)
            return TorrentMetadata.load(meta_path)

        # Bản gốc hợp lệ đọc được và có đủ file_info
        loaded = write_and_load(lambda d: None)
        assert loaded["file_info"]["chunk_count"] == 4

        bad_mutations = {
            "num_chunks sai": lambda d: d.update(num_chunks=5),
            "thiếu 1 hash": lambda d: d["chunk_hashes"].pop(),
            "hash không phải hex": lambda d: d["chunk_hashes"].__setitem__(0, "z" * 64),
            "hash viết hoa / sai độ dài": lambda d: d.update(file_hash=d["file_hash"].upper()[:60]),
            "chunk_size = 0": lambda d: d.update(chunk_size=0),
            "file_size âm": lambda d: d.update(file_size=-1),
            "file_name là đường dẫn": lambda d: d.update(file_name="../../etc/passwd"),
            "tracker không phải http": lambda d: d.update(trackers=["ftp://x"]),
            "thiếu trường bắt buộc": lambda d: d.pop("chunk_hashes"),
        }
        for name, mutate in bad_mutations.items():
            with pytest.raises(MetadataError):
                write_and_load(mutate)
                pytest.fail(f"metadata '{name}' should have been rejected")

        # JSON hỏng
        with open(meta_path, "w", encoding="utf-8") as f:
            f.write("{ not json")
        with pytest.raises(MetadataError):
            TorrentMetadata.load(meta_path)
    finally:
        _cleanup(src, meta_path)


def test_3_seeder_refuses_source_file_that_does_not_match_metadata():
    """Validation Test 3: seeder sai kích thước hoặc sai nội dung (cùng kích thước) phải từ chối khởi động."""
    original = os.urandom(4096)
    src = _temp_file(original)
    meta_path = src + ".torrent.json"
    wrong_content = src + ".wrong_content"
    wrong_size = src + ".wrong_size"
    try:
        TorrentMetadata(src, chunk_size=1024).save(meta_path)

        tampered = bytearray(original)
        tampered[2048] ^= 0xFF  # đổi 1 byte trong chunk 2
        with open(wrong_content, "wb") as f:
            f.write(bytes(tampered))
        with open(wrong_size, "wb") as f:
            f.write(original[:3000])

        for bad_file in (wrong_content, wrong_size):
            node = PeerNode(host="127.0.0.1", port=51190)
            with pytest.raises(SourceFileError):
                node.init_from_metadata(meta_path, bad_file, is_seeder=True)

        # File đúng thì seeder khởi động bình thường với 100% chunk
        good = PeerNode(host="127.0.0.1", port=51191)
        good.init_from_metadata(meta_path, src, is_seeder=True)
        assert good.chunk_mgr.is_complete()
    finally:
        _cleanup(src, meta_path, wrong_content, wrong_size)


def test_4_final_file_hash_success_failure_and_repair():
    """
    Validation Test 4 – kiểm tra hash toàn file trong vòng tải tự động:
      a) File đã đủ chunk từ đầu (resume xong) vẫn được verify -> download_ok = True.
      b) Chunk bị hỏng trên đĩa sau khi "đủ chunk" -> phát hiện khi verify toàn file,
         tự tải lại chunk đó từ seeder -> download_ok = True.
      c) Metadata có file_hash không khớp các chunk -> KHÔNG được coi là thành công:
         download_ok = False (trước đây vẫn báo hoàn tất như thường).
    """
    async def _run():
        raw = os.urandom(8 * 1024)
        src = _temp_file(raw)
        meta_path = src + ".torrent.json"
        bad_meta_path = src + ".badhash.torrent.json"
        out_a = src + ".out_a"
        out_c = src + ".out_c"
        seeder = None
        try:
            meta = TorrentMetadata(src, chunk_size=1024)
            data = meta.generate()
            meta.save(meta_path)

            seeder = PeerNode(host="127.0.0.1", port=51201)
            seeder.init_from_metadata(meta_path, src, is_seeder=True)
            await seeder.start_server()
            source = {"peer_id": "127.0.0.1:51201", "host": "127.0.0.1", "port": 51201, "chunks": list(range(8))}

            # a) file đúng có sẵn trên đĩa -> resume đủ chunk -> verify vẫn chạy
            with open(out_a, "wb") as f:
                f.write(raw)
            leecher_a = PeerNode(host="127.0.0.1", port=51202)
            leecher_a.init_from_metadata(meta_path, out_a, is_seeder=False)
            assert leecher_a.chunk_mgr.is_complete()
            leecher_a.is_running = True
            await asyncio.wait_for(leecher_a.download_manager_loop(), timeout=10)
            assert leecher_a.download_ok is True

            # b) làm hỏng chunk 3 trên đĩa rồi chạy lại vòng tải -> tự phát hiện và tải lại
            with open(out_a, "r+b") as f:
                f.seek(3 * 1024)
                f.write(b"\x00" * 16)
            leecher_b = PeerNode(host="127.0.0.1", port=51203)
            leecher_b.init_from_metadata(meta_path, out_a, is_seeder=False)
            # Resume scan đã loại chunk 3; giả lập trường hợp xấu hơn: chunk hỏng SAU khi đã được ghi nhận
            leecher_b.chunk_mgr.chunks_owned = set(range(8))
            leecher_b.active_peers = [source]
            leecher_b.is_running = True
            await asyncio.wait_for(leecher_b.download_manager_loop(), timeout=10)
            assert leecher_b.download_ok is True
            assert leecher_b.chunk_source.get(3) == "127.0.0.1:51201"
            with open(out_a, "rb") as f:
                assert f.read() == raw

            # c) metadata có file_hash sai (chunk_hashes vẫn đúng) -> tải đủ chunk nhưng phải báo THẤT BẠI
            bad = dict(data)
            bad["file_hash"] = hashlib.sha256(b"something else").hexdigest()
            bad.pop("file_info", None)
            with open(bad_meta_path, "w", encoding="utf-8") as f:
                json.dump(bad, f)
            leecher_c = PeerNode(host="127.0.0.1", port=51204)
            leecher_c.init_from_metadata(bad_meta_path, out_c, is_seeder=False)
            assert leecher_c.file_id == seeder.file_id  # file_id giữ nguyên, chỉ file_hash bị sai
            leecher_c.active_peers = [source]
            leecher_c.is_running = True
            await asyncio.wait_for(leecher_c.download_manager_loop(), timeout=10)
            assert leecher_c.chunk_mgr.is_complete()
            assert leecher_c.download_ok is False
            assert leecher_c.download_completed_event.is_set()
        finally:
            if seeder is not None:
                await seeder.stop_server()
            _cleanup(src, meta_path, bad_meta_path, out_a, out_c)

    asyncio.run(_run())


def test_5_tracker_auth_does_not_log_key_and_fails_closed(caplog, monkeypatch):
    """
    Validation Test 5:
      - Request với key sai bị 401 và giá trị key KHÔNG xuất hiện trong log.
      - Tracker chưa cấu hình key (rỗng) -> từ chối MỌI request, kể cả request gửi key rỗng.
    """
    client = TestClient(tracker_module.app)
    leaked = "this_secret_value_must_not_be_logged_123"

    with caplog.at_level(logging.WARNING, logger="tracker"):
        res = client.get("/stats", headers={"X-API-Key": leaked})
    assert res.status_code == 401
    assert any("[AUTH]" in r.getMessage() for r in caplog.records)
    assert all(leaked not in r.getMessage() for r in caplog.records)

    # Key đúng vẫn được chấp nhận
    assert client.get("/stats", headers={"X-API-Key": TRACKER_API_KEY}).status_code == 200

    # Fail closed khi tracker không có key
    monkeypatch.setattr(tracker_module, "TRACKER_API_KEY", "")
    assert client.get("/stats").status_code == 401
    assert client.get("/stats", headers={"X-API-Key": ""}).status_code == 401
    assert client.get("/stats", headers={"X-API-Key": TRACKER_API_KEY}).status_code == 401
