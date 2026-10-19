"""
Engine tests: chạy ĐÚNG vòng tải tự động download_manager_loop() của PeerNode
(không gọi tay download_chunk_from_peer), để kiểm tra các cơ chế tự động:
  1. Tải song song đa nguồn: engine tự chia chunk cho nhiều peer.
  2. Failover tự động: một source peer bị tắt giữa chừng, engine tự chuyển sang peer khác.
  3. Peer độc hại gửi chunk hỏng: engine từ chối, chặn peer đó và tải lại từ peer trung thực.

Các seeder dùng giới hạn băng thông upload để quá trình tải kéo dài đủ lâu,
giống mạng thật (trên loopback mọi thứ xong quá nhanh để quan sát).
"""
import os
import sys
import asyncio
import tempfile
from collections import Counter

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.common.metadata import TorrentMetadata
from src.peer.peer_node import PeerNode, BAD_PEER_THRESHOLD

NUM_CHUNKS = 24
CHUNK_SIZE = 16 * 1024          # 16 KB -> file 384 KB
UPLOAD_LIMIT_KBPS = 256         # mỗi seeder upload tối đa 256 KB/s
LOOP_TIMEOUT = 30               # giây


def _make_source_file():
    raw = os.urandom(NUM_CHUNKS * CHUNK_SIZE)
    with tempfile.NamedTemporaryFile(delete=False) as tf:
        tf.write(raw)
        src_file = tf.name
    meta_path = src_file + ".torrent.json"
    TorrentMetadata(src_file, chunk_size=CHUNK_SIZE).save(meta_path)
    return src_file, meta_path


def _peer_info(port: int):
    return {
        "peer_id": f"127.0.0.1:{port}",
        "host": "127.0.0.1",
        "port": port,
        "chunks": list(range(NUM_CHUNKS)),
    }


async def _start_seeder(port, meta_path, src_file, corrupt=False):
    node = PeerNode(host="127.0.0.1", port=port, corrupt=corrupt, upload_limit_kbps=UPLOAD_LIMIT_KBPS)
    node.init_from_metadata(meta_path, src_file, is_seeder=True)
    await node.start_server()
    return node


def _make_leecher(port, meta_path, out_file, source_ports):
    leecher = PeerNode(host="127.0.0.1", port=port)
    leecher.init_from_metadata(meta_path, out_file, is_seeder=False)
    # Không có tracker trong test: danh sách peer được cho sẵn
    leecher.active_peers = [_peer_info(p) for p in source_ports]
    leecher.is_running = True
    return leecher


def _cleanup(*paths):
    for p in paths:
        if os.path.exists(p):
            os.remove(p)


def test_1_engine_parallel_multi_source_download():
    """
    Engine Test 1:
    Hai seeder cùng có đủ file. Vòng tải tự động phải chia chunk cho CẢ HAI nguồn
    (không dồn hết vào một peer), tải xong và hash file cuối khớp.
    """
    async def _run():
        src_file, meta_path = _make_source_file()
        out_file = src_file + ".engine_multi"
        seeders = []
        try:
            seeders.append(await _start_seeder(51101, meta_path, src_file))
            seeders.append(await _start_seeder(51102, meta_path, src_file))
            leecher = _make_leecher(51103, meta_path, out_file, [51101, 51102])

            await asyncio.wait_for(leecher.download_manager_loop(), timeout=LOOP_TIMEOUT)

            assert leecher.chunk_mgr.is_complete() is True
            assert leecher.chunk_mgr.verify_final_file() is True

            per_source = Counter(leecher.chunk_source.values())
            assert set(per_source) == {"127.0.0.1:51101", "127.0.0.1:51102"}
            # Tải được chia tương đối đều: mỗi nguồn ít nhất 1/4 số chunk
            assert min(per_source.values()) >= NUM_CHUNKS // 4
        finally:
            for s in seeders:
                await s.stop_server()
            _cleanup(src_file, meta_path, out_file)

    asyncio.run(_run())


def test_2_engine_automatic_failover_when_source_crashes():
    """
    Engine Test 2:
    Đang tải từ 2 seeder thì seeder 1 bị tắt đột ngột (crash-stop).
    Engine phải tự phát hiện lỗi, phạt điểm seeder 1, tự yêu cầu lại các chunk
    từ seeder 2 và hoàn thành file đúng hash, KHÔNG cần can thiệp bằng tay.
    """
    async def _run():
        src_file, meta_path = _make_source_file()
        out_file = src_file + ".engine_failover"
        seeder_1 = seeder_2 = None
        try:
            seeder_1 = await _start_seeder(51111, meta_path, src_file)
            seeder_2 = await _start_seeder(51112, meta_path, src_file)
            leecher = _make_leecher(51113, meta_path, out_file, [51111, 51112])

            async def crash_seeder_1_midway():
                # Chờ tới khi đã nhận được vài chunk rồi mới tắt seeder 1
                while len(leecher.chunk_mgr.chunks_owned) < NUM_CHUNKS // 4:
                    await asyncio.sleep(0.02)
                await seeder_1.server.stop(grace=None)

            crash_task = asyncio.create_task(crash_seeder_1_midway())
            await asyncio.wait_for(leecher.download_manager_loop(), timeout=LOOP_TIMEOUT)
            await crash_task

            assert leecher.chunk_mgr.is_complete() is True
            assert leecher.chunk_mgr.verify_final_file() is True

            # Seeder 1 bị phát hiện lỗi và phạt điểm
            assert leecher.bad_peers.get("127.0.0.1:51111", 0) > 0
            # Seeder 2 gánh phần còn lại sau khi seeder 1 chết
            per_source = Counter(leecher.chunk_source.values())
            assert per_source["127.0.0.1:51112"] >= NUM_CHUNKS // 2
        finally:
            for s in (seeder_1, seeder_2):
                if s is not None:
                    await s.stop_server()
            _cleanup(src_file, meta_path, out_file)

    asyncio.run(_run())


def test_3_engine_rejects_and_blacklists_corrupt_peer():
    """
    Engine Test 3:
    Một seeder độc hại gửi dữ liệu sai (fault injection --corrupt), một seeder trung thực.
    Engine phải: từ chối mọi chunk sai hash, phạt điểm tới mức chặn peer độc hại,
    tải lại từ peer trung thực; file cuối khớp hash và KHÔNG có chunk nào từ peer độc hại.
    """
    async def _run():
        src_file, meta_path = _make_source_file()
        out_file = src_file + ".engine_corrupt"
        seeders = []
        try:
            seeders.append(await _start_seeder(51121, meta_path, src_file, corrupt=True))
            seeders.append(await _start_seeder(51122, meta_path, src_file))
            leecher = _make_leecher(51123, meta_path, out_file, [51121, 51122])

            await asyncio.wait_for(leecher.download_manager_loop(), timeout=LOOP_TIMEOUT)

            assert leecher.chunk_mgr.is_complete() is True
            assert leecher.chunk_mgr.verify_final_file() is True

            # Không chunk nào được ghi nhận từ peer độc hại
            assert "127.0.0.1:51121" not in set(leecher.chunk_source.values())
            # Peer độc hại bị phạt tới ngưỡng chặn
            assert leecher.bad_peers.get("127.0.0.1:51121", 0) >= BAD_PEER_THRESHOLD
        finally:
            for s in seeders:
                await s.stop_server()
            _cleanup(src_file, meta_path, out_file)

    asyncio.run(_run())
