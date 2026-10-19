import os
import sys
import tempfile
import asyncio
import pytest
import grpc
from fastapi.testclient import TestClient

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.common.config import TRACKER_API_KEY
from src.common import peer_pb2, peer_pb2_grpc
from src.common.metadata import TorrentMetadata
from src.tracker.tracker import app, state
from src.peer.peer_node import PeerNode
from src.peer.chunk_manager import ChunkManager, ChunkStatus


@pytest.fixture(autouse=True)
def reset_tracker_state():
    state.files.clear()
    state.peers.clear()
    yield
    state.files.clear()
    state.peers.clear()


def test_1_corrupt_chunk_rejected_and_penalized():
    """
    Security / Invalid Data Test 1:
    Peer độc hại hoặc mạng lỗi gửi chunk bị hỏng / sai mã băm SHA-256.
    Leecher phải phát hiện mã băm không khớp, từ chối ghi đĩa,
    đánh dấu chunk MISSING và cộng điểm phạt (penalize) cho peer đó.
    """
    async def _run():
        num_chunks = 4
        chunk_size = 256
        good_data = b"LegitimateDataBlock_" * (num_chunks * chunk_size // 20)
        good_data = good_data[: num_chunks * chunk_size]

        with tempfile.NamedTemporaryFile(delete=False) as tf:
            tf.write(good_data)
            good_file = tf.name

        meta_path = good_file + ".torrent.json"
        dest_file = good_file + ".security_dest"

        try:
            meta = TorrentMetadata(good_file, chunk_size=chunk_size)
            meta_dict = meta.generate()
            meta.save(meta_path)

            cm = ChunkManager(
                file_path=dest_file,
                file_size=meta_dict["file_size"],
                chunk_size=chunk_size,
                chunk_hashes=meta_dict["chunk_hashes"],
                expected_file_hash=meta_dict["file_hash"]
            )
            cm.initialize_storage(is_seeder=False)

            # Cố tình gửi dữ liệu bị hỏng (corrupt data) cho chunk 0
            corrupted_data = b"CORRUPTED_BYTES_MALICIOUS_CONTENT_" * 8
            corrupted_data = corrupted_data[:chunk_size]

            # Ghi chunk bị hỏng -> write_chunk_atomic phải trả về False
            ok = await cm.write_chunk_atomic(0, corrupted_data)
            assert ok is False
            assert 0 not in cm.chunks_owned
            assert cm.status[0] == ChunkStatus.MISSING

            # Bây giờ ghi đúng dữ liệu -> phải thành công
            good_chunk_0 = good_data[0:chunk_size]
            ok_good = await cm.write_chunk_atomic(0, good_chunk_0)
            assert ok_good is True
            assert 0 in cm.chunks_owned
            assert cm.status[0] == ChunkStatus.DONE
        finally:
            for p in [good_file, meta_path, dest_file]:
                if os.path.exists(p):
                    os.remove(p)

    asyncio.run(_run())


def test_2_tracker_unauthorized_access_and_invalid_inputs():
    """
    Security / Invalid Data Test 2:
    - Tracker từ chối các request không có hoặc sai X-API-Key (HTTP 401).
    - Tracker chặn các yêu cầu không hợp lệ (file_id chưa đăng ký, port ngoài phạm vi).
    """
    client = TestClient(app)

    # 1. Truy cập không có API key -> 401
    res1 = client.get("/peers/non_existent_file")
    assert res1.status_code == 401

    # 2. Truy cập với API key sai -> 401
    res2 = client.get("/peers/non_existent_file", headers={"X-API-Key": "wrong_secret_token"})
    assert res2.status_code == 401

    # 3. Truy cập với API key đúng nhưng file_id không tồn tại -> 404
    valid_headers = {"X-API-Key": TRACKER_API_KEY}
    res3 = client.get("/peers/unknown_hash_12345678", headers=valid_headers)
    assert res3.status_code == 404

    # 4. Gửi payload đăng ký sai schema (size âm) -> 422 Unprocessable Entity
    invalid_reg = {
        "file_id": "test_hash_val",
        "name": "sample.iso",
        "size": -100, # Kích thước âm
        "chunk_count": 0
    }
    res4 = client.post("/register", json=invalid_reg, headers=valid_headers)
    assert res4.status_code == 422


def test_3_grpc_rejects_out_of_range_chunk_and_unknown_file():
    """
    Security / Invalid Data Test 3:
    Gửi trực tiếp các yêu cầu gRPC không hợp lệ tới một Seeder thật:
    - chunk_index âm và chunk_index vượt quá số chunk -> success=False
    - file_id không tồn tại trên peer -> success=False
    - Handshake với file_id sai -> success=False
    Seeder không được crash và vẫn phục vụ yêu cầu hợp lệ sau đó.
    """
    async def _run():
        num_chunks = 4
        chunk_size = 128
        raw_data = os.urandom(num_chunks * chunk_size)

        with tempfile.NamedTemporaryFile(delete=False) as tf:
            tf.write(raw_data)
            src_file = tf.name
        meta_path = src_file + ".torrent.json"

        seeder = None
        try:
            meta = TorrentMetadata(src_file, chunk_size=chunk_size)
            meta.save(meta_path)

            seeder = PeerNode(host="127.0.0.1", port=50801)
            seeder.init_from_metadata(meta_path, src_file, is_seeder=True)
            await seeder.start_server()
            file_id = seeder.file_id

            async with grpc.aio.insecure_channel("127.0.0.1:50801") as channel:
                stub = peer_pb2_grpc.PeerServiceStub(channel)

                # chunk_index âm
                res_neg = await stub.GetChunk(
                    peer_pb2.ChunkRequest(file_id=file_id, chunk_index=-1), timeout=5
                )
                assert res_neg.success is False
                assert res_neg.data == b""

                # chunk_index vượt phạm vi
                res_big = await stub.GetChunk(
                    peer_pb2.ChunkRequest(file_id=file_id, chunk_index=num_chunks + 100), timeout=5
                )
                assert res_big.success is False
                assert res_big.data == b""

                # file_id không tồn tại
                res_unknown = await stub.GetChunk(
                    peer_pb2.ChunkRequest(file_id="ffffffff_unknown_file", chunk_index=0), timeout=5
                )
                assert res_unknown.success is False
                assert res_unknown.data == b""

                # Handshake với file_id sai
                hs_bad = await stub.Handshake(
                    peer_pb2.HandshakeRequest(peer_id="attacker:1", file_id="ffffffff_unknown_file"), timeout=5
                )
                assert hs_bad.success is False

                # Seeder vẫn hoạt động bình thường với yêu cầu hợp lệ
                res_ok = await stub.GetChunk(
                    peer_pb2.ChunkRequest(file_id=file_id, chunk_index=0), timeout=5
                )
                assert res_ok.success is True
                assert res_ok.data == raw_data[0:chunk_size]
        finally:
            if seeder is not None:
                await seeder.stop_server()
            for p in [src_file, meta_path]:
                if os.path.exists(p):
                    os.remove(p)

    asyncio.run(_run())


def test_4_tracker_rejects_malformed_announce_and_filters_bad_chunks():
    """
    Security / Invalid Data Test 4:
    - Announce cho file chưa đăng ký -> 404.
    - Announce thiếu trường bắt buộc / sai kiểu dữ liệu / port ngoài phạm vi -> 422.
    - Announce chứa chunk index âm hoặc vượt số chunk -> tracker tự lọc bỏ,
      chỉ ghi nhận các chunk hợp lệ.
    """
    client = TestClient(app)
    headers = {"X-API-Key": TRACKER_API_KEY}
    f_id = "security_announce_file_12345678"

    # Announce khi file chưa được đăng ký -> 404
    res_unregistered = client.post("/announce", json={
        "peer_id": "127.0.0.1:6001",
        "file_id": f_id,
        "host": "127.0.0.1",
        "port": 6001,
        "chunks_owned": [0]
    }, headers=headers)
    assert res_unregistered.status_code == 404

    # Đăng ký file có 4 chunk
    reg = client.post("/register", json={
        "file_id": f_id, "name": "data.bin", "size": 4000, "chunk_count": 4
    }, headers=headers)
    assert reg.status_code == 200

    # Thiếu trường bắt buộc (không có host) -> 422
    res_missing = client.post("/announce", json={
        "peer_id": "127.0.0.1:6001",
        "file_id": f_id,
        "port": 6001
    }, headers=headers)
    assert res_missing.status_code == 422

    # Port ngoài phạm vi 1..65535 -> 422
    res_port = client.post("/announce", json={
        "peer_id": "127.0.0.1:70000",
        "file_id": f_id,
        "host": "127.0.0.1",
        "port": 70000,
        "chunks_owned": [0]
    }, headers=headers)
    assert res_port.status_code == 422

    # Sai kiểu dữ liệu (chunks_owned là chuỗi) -> 422
    res_type = client.post("/announce", json={
        "peer_id": "127.0.0.1:6001",
        "file_id": f_id,
        "host": "127.0.0.1",
        "port": 6001,
        "chunks_owned": "all_chunks"
    }, headers=headers)
    assert res_type.status_code == 422

    # Chunk index không hợp lệ bị lọc: chỉ 0 và 3 hợp lệ
    res_filter = client.post("/announce", json={
        "peer_id": "127.0.0.1:6001",
        "file_id": f_id,
        "host": "127.0.0.1",
        "port": 6001,
        "chunks_owned": [-5, 0, 3, 4, 999]
    }, headers=headers)
    assert res_filter.status_code == 200
    assert res_filter.json()["chunks_recorded"] == 2

    peers = client.get(f"/peers/{f_id}", headers=headers).json()["peers"]
    assert len(peers) == 1
    assert peers[0]["chunks"] == [0, 3]


def test_5_oversized_grpc_message_rejected():
    """
    Security / Invalid Data Test 5:
    Giới hạn kích thước message: gRPC mặc định chỉ nhận message tối đa 4 MB.
    Một peer cố gửi chunk 5 MB (vượt giới hạn) -> phía nhận từ chối message,
    chunk không được ghi, trạng thái quay về MISSING và peer gửi bị phạt điểm.
    """
    async def _run():
        chunk_size = 5 * 1024 * 1024  # 5 MB > giới hạn 4 MB của gRPC
        raw_data = os.urandom(chunk_size)

        with tempfile.NamedTemporaryFile(delete=False) as tf:
            tf.write(raw_data)
            src_file = tf.name
        meta_path = src_file + ".torrent.json"
        out_file = src_file + ".oversized_out"

        seeder = None
        try:
            meta = TorrentMetadata(src_file, chunk_size=chunk_size)
            meta.save(meta_path)

            seeder = PeerNode(host="127.0.0.1", port=50811)
            seeder.init_from_metadata(meta_path, src_file, is_seeder=True)
            await seeder.start_server()

            leecher = PeerNode(host="127.0.0.1", port=50812)
            leecher.init_from_metadata(meta_path, out_file, is_seeder=False)
            seeder_info = {
                "peer_id": "127.0.0.1:50811",
                "host": "127.0.0.1",
                "port": 50811,
                "chunks": [0]
            }

            ok = await leecher.download_chunk_from_peer(0, seeder_info)
            assert ok is False
            assert 0 not in leecher.chunk_mgr.chunks_owned
            assert leecher.chunk_mgr.status[0] == ChunkStatus.MISSING
            assert leecher.bad_peers["127.0.0.1:50811"] > 0
        finally:
            if seeder is not None:
                await seeder.stop_server()
            for p in [src_file, meta_path, out_file]:
                if os.path.exists(p):
                    os.remove(p)

    asyncio.run(_run())
