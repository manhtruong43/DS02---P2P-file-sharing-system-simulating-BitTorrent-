"""
Kiểm thử độ bền (resilience) – các hành vi được bổ sung sau đợt rà soát mã thứ hai:

  1. Metadata Generator từ chối file có quá nhiều chunk TRƯỚC khi băm (không tốn RAM/CPU vô ích).
  2. Bộ lập lịch chia đều chunk giữa các nguồn ngay trong cùng một đợt giao việc.
  3. Tải thất bại ở bước verify cuối -> run() trả về False, peer rời swarm và tắt gRPC server.
  4. Seeder khởi động TRƯỚC tracker -> tự thử đăng ký lại có backoff tới khi tracker sẵn sàng.
  5. Tracker khởi động lại (mất toàn bộ trạng thái trong RAM) -> peer tự đăng ký lại file khi announce.

Test 4 và 5 chạy Tracker thật trong một TIẾN TRÌNH RIÊNG (subprocess).
"""
import os
import socket
import sys
import json
import time
import asyncio
import hashlib
import logging
import tempfile
import subprocess
from collections import Counter

import grpc
import httpx
import pytest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import src.common.metadata as metadata_module
from src.common import peer_pb2, peer_pb2_grpc
from src.common.config import TRACKER_API_KEY, MAX_NUM_CHUNKS
from src.common.metadata import TorrentMetadata, MetadataError
from src.peer.peer_node import PeerNode

TRACKER_SCRIPT = os.path.join(ROOT_DIR, "src", "tracker.py")
CLOSED_TRACKER = "http://127.0.0.1:9"  # không có gì lắng nghe ở đây


def _temp_file(data: bytes) -> str:
    with tempfile.NamedTemporaryFile(delete=False) as tf:
        tf.write(data)
        return tf.name


def _cleanup(*paths):
    for p in paths:
        if p and os.path.exists(p):
            os.remove(p)


def _start_tracker(port: int) -> subprocess.Popen:
    """Chạy tracker thật trong tiến trình riêng; key lấy từ biến môi trường (conftest.py)."""
    return subprocess.Popen(
        [sys.executable, TRACKER_SCRIPT, "--host", "127.0.0.1", "--port", str(port)],
        cwd=ROOT_DIR, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def _wait_tracker_healthy(port: int, timeout: float = 15) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if httpx.get(f"http://127.0.0.1:{port}/health", timeout=0.5).status_code == 200:
                return
        except Exception:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"tracker on port {port} did not become healthy")


def _stop(proc: subprocess.Popen, port: int = None) -> None:
    """
    Tắt tracker. Trên Windows, python.exe trong .venv là launcher sinh ra tiến trình python thật
    làm con, nên phải tắt cả cây (taskkill /T). Nếu biết cổng thì chờ tới khi cổng thực sự đóng,
    tránh trường hợp tracker mới khởi động khi tracker cũ vẫn còn giữ cổng.
    """
    if proc and proc.poll() is None:
        if os.name == "nt":
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=5)
    if port is not None:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.3)
                if s.connect_ex(("127.0.0.1", port)) != 0:
                    return
            time.sleep(0.2)


def _peers_on_tracker(port: int, file_id: str):
    res = httpx.get(f"http://127.0.0.1:{port}/peers/{file_id}",
                    headers={"X-API-Key": TRACKER_API_KEY}, timeout=2)
    return res.status_code, (res.json().get("peers", []) if res.status_code == 200 else [])


def test_1_metadata_rejects_too_many_chunks_before_hashing(monkeypatch):
    """Resilience Test 1: số chunk vượt MAX_NUM_CHUNKS bị từ chối ngay, KHÔNG đọc/băm file."""
    size = MAX_NUM_CHUNKS + 10  # chunk_size = 1 byte -> nhiều hơn giới hạn
    src = _temp_file(b"x" * size)
    try:
        def must_not_hash(*args, **kwargs):
            raise AssertionError("file was hashed before the chunk-count check")
        monkeypatch.setattr(metadata_module, "compute_file_hashes", must_not_hash)

        with pytest.raises(MetadataError, match="chunks"):
            TorrentMetadata(src, chunk_size=1).generate()
    finally:
        _cleanup(src)


def test_2_scheduler_balances_sources_within_one_round():
    """
    Resilience Test 2: trong MỘT đợt lập lịch, 5 chunk đầu phải chia đều cho các nguồn
    (2 nguồn -> 3/2, 3 nguồn -> 2/2/1), lặp lại nhiều lần để loại trừ may rủi.
    Bộ đếm tải phải về 0 khi các task kết thúc.
    """
    src = _temp_file(os.urandom(40 * 1024))
    meta_path = src + ".torrent.json"
    out = src + ".sched_out"
    try:
        TorrentMetadata(src, chunk_size=1024).save(meta_path)

        async def one_round(num_sources: int):
            node = PeerNode(host="127.0.0.1", port=51301)
            node.init_from_metadata(meta_path, out, is_seeder=False)
            node.active_peers = [
                {"peer_id": f"127.0.0.1:{p}", "host": "127.0.0.1", "port": p, "chunks": list(range(40))}
                for p in range(1, num_sources + 1)
            ]
            assigned = []

            async def fake_download(chunk_idx, peer_info):
                assigned.append(peer_info["port"])
                await asyncio.sleep(30)

            node._download_task = fake_download
            node.is_running = True
            loop_task = asyncio.create_task(node.download_manager_loop())
            await asyncio.sleep(0.03)
            loop_task.cancel()
            tasks = list(node.download_tasks.values())
            for t in tasks:
                t.cancel()
            await asyncio.gather(loop_task, *tasks, return_exceptions=True)
            await asyncio.sleep(0)
            return Counter(assigned), sum(node.inflight_per_peer.values())

        for num_sources, expected in ((2, [3, 2]), (3, [2, 2, 1])):
            for _ in range(30):
                counts, leftover = asyncio.run(one_round(num_sources))
                assert sorted(counts.values(), reverse=True) == expected
                assert leftover == 0
    finally:
        _cleanup(src, meta_path, out)


def test_3_failed_final_verification_stops_the_peer():
    """
    Resilience Test 3: metadata có file_hash sai (chunk_hashes đúng) -> tải đủ chunk nhưng verify cuối thất bại.
    run() phải trả về False, peer ngừng chia sẻ: gRPC server đã tắt, không còn phục vụ chunk.
    """
    async def _run():
        raw = os.urandom(8 * 1024)
        src = _temp_file(raw)
        meta_path = src + ".torrent.json"
        bad_meta = src + ".badhash.torrent.json"
        out = src + ".failed_out"
        seeder = None
        try:
            data = TorrentMetadata(src, chunk_size=1024).generate()
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(data, f)
            bad = dict(data)
            bad.pop("file_info", None)
            bad["file_hash"] = hashlib.sha256(b"not the real file").hexdigest()
            with open(bad_meta, "w", encoding="utf-8") as f:
                json.dump(bad, f)

            seeder = PeerNode(host="127.0.0.1", port=51311)
            seeder.init_from_metadata(meta_path, src, is_seeder=True)
            await seeder.start_server()

            leecher = PeerNode(host="127.0.0.1", port=51312)
            leecher.init_from_metadata(bad_meta, out, is_seeder=False)
            leecher.tracker_url = CLOSED_TRACKER  # không có tracker: chỉ dùng danh sách peer cho sẵn
            leecher.active_peers = [
                {"peer_id": "127.0.0.1:51311", "host": "127.0.0.1", "port": 51311, "chunks": list(range(8))}
            ]

            result = await asyncio.wait_for(leecher.run(), timeout=30)
            assert result is False
            assert leecher.download_ok is False
            assert leecher.chunk_mgr.is_complete()

            # Server của leecher đã tắt -> không thể xin chunk từ nó nữa
            async with grpc.aio.insecure_channel("127.0.0.1:51312") as channel:
                stub = peer_pb2_grpc.PeerServiceStub(channel)
                with pytest.raises(grpc.aio.AioRpcError) as err:
                    await stub.GetChunk(peer_pb2.ChunkRequest(file_id=leecher.file_id, chunk_index=0), timeout=2)
                assert err.value.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED)
        finally:
            if seeder is not None:
                await seeder.stop_server()
            _cleanup(src, meta_path, bad_meta, out)

    asyncio.run(_run())


def test_4_seeder_retries_registration_until_tracker_is_up(caplog):
    """
    Resilience Test 4: seeder bắt đầu đăng ký khi tracker CHƯA chạy; tracker chỉ được bật sau khi
    đã có ít nhất một lần đăng ký thất bại thật. Seeder phải thử lại có backoff và đăng ký thành
    công; tracker ghi nhận file.
    """
    port = 8191
    src = _temp_file(os.urandom(4096))
    meta_path = src + ".torrent.json"
    tracker = None

    attempts = []  # kết quả từng lần gọi /register: False = thất bại, True = thành công

    async def _run():
        nonlocal tracker
        TorrentMetadata(src, trackers=[f"http://127.0.0.1:{port}"], chunk_size=1024).save(meta_path)
        seeder = PeerNode(host="127.0.0.1", port=51321)
        seeder.init_from_metadata(meta_path, src, is_seeder=True)

        # Ghi lại kết quả từng lần đăng ký (bọc hàm thật, không thay đổi hành vi)
        real_post_register = seeder._post_register

        async def recording_post_register(client):
            ok = await real_post_register(client)
            attempts.append(ok)
            return ok
        seeder._post_register = recording_post_register

        register = asyncio.create_task(seeder.register_file_with_tracker(max_attempts=10))
        # Chỉ bật tracker SAU KHI đã có ít nhất một lần đăng ký thất bại thật.
        # (Không chờ cứng theo thời gian: trên Windows, kết nối tới cổng đóng mất ~2 s mới bị
        # từ chối, nên một mốc thời gian cố định có thể khiến lần đầu tiên đã thành công.)
        deadline = time.monotonic() + 20
        while False not in attempts:
            assert not register.done(), "registration finished before the tracker was started"
            assert time.monotonic() < deadline, "no failed registration attempt observed"
            await asyncio.sleep(0.05)
        tracker = _start_tracker(port)
        ok = await asyncio.wait_for(register, timeout=30)
        assert ok is True
        return seeder.file_id

    try:
        with caplog.at_level(logging.INFO):
            file_id = asyncio.run(_run())
        # Có lần thất bại (tracker chưa chạy) và lần cuối cùng thành công (sau retry có backoff)
        assert attempts[0] is False
        assert attempts[-1] is True
        assert any("[REGISTER] Attempt" in r.getMessage() for r in caplog.records)
        status, _ = _peers_on_tracker(port, file_id)
        assert status == 200  # tracker đã biết file
    finally:
        _stop(tracker)
        _cleanup(src, meta_path)


def test_5_peer_re_registers_file_after_tracker_restart(caplog):
    """
    Resilience Test 5: tracker đang chạy thì bị tắt và bật lại (mất toàn bộ trạng thái trong RAM).
    Lần announce kế tiếp nhận 404 -> peer tự đăng ký lại file rồi announce lại thành công;
    tracker mới lại thấy peer trong danh sách.
    """
    port = 8192
    src = _temp_file(os.urandom(4096))
    meta_path = src + ".torrent.json"
    tracker = None
    try:
        TorrentMetadata(src, trackers=[f"http://127.0.0.1:{port}"], chunk_size=1024).save(meta_path)
        tracker = _start_tracker(port)
        _wait_tracker_healthy(port)

        async def _run():
            nonlocal tracker
            peer = PeerNode(host="127.0.0.1", port=51331)
            peer.init_from_metadata(meta_path, src, is_seeder=True)
            peer.http = httpx.AsyncClient(headers=peer.http_headers, timeout=4.0)
            try:
                assert await peer.register_file_with_tracker() is True
                assert await peer.announce_to_tracker() is True

                # Tracker restart: trạng thái cũ mất hết
                _stop(tracker, port)
                tracker = _start_tracker(port)
                _wait_tracker_healthy(port)
                assert _peers_on_tracker(port, peer.file_id)[0] == 404

                assert await peer.announce_to_tracker() is True
                return peer.file_id
            finally:
                await peer.http.aclose()

        with caplog.at_level(logging.WARNING):
            file_id = asyncio.run(_run())
        assert any("re-registering" in r.getMessage() for r in caplog.records)
        status, peers = _peers_on_tracker(port, file_id)
        assert status == 200
        assert [p["peer_id"] for p in peers] == ["127.0.0.1:51331"]
    finally:
        _stop(tracker)
        _cleanup(src, meta_path)
