import os
import sys
import tempfile
import asyncio
import pytest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.common.hashing import compute_file_hashes
from src.common.metadata import TorrentMetadata
from src.peer.chunk_manager import ChunkManager
from src.peer.peer_node import PeerNode

def test_1_concurrent_chunk_write_atomic():
    """
    Concurrency Test 1:
    Nhiều task async cùng ghi các chunk khác nhau vào 1 file tại cùng một thời điểm.
    Đảm bảo I/O Lock ngăn chặn xung đột và file cuối cùng nguyên vẹn 100%.
    """
    async def _run():
        num_chunks = 20
        chunk_size = 1024
        raw_data = b"".join([bytes([i % 256]) * chunk_size for i in range(num_chunks)])

        with tempfile.NamedTemporaryFile(delete=False) as tf:
            tf.write(raw_data)
            src_path = tf.name

        dest_path = src_path + ".concurrent_out"
        try:
            file_hash, chunk_hashes, file_size = compute_file_hashes(src_path, chunk_size)
            cm = ChunkManager(
                file_path=dest_path,
                file_size=file_size,
                chunk_size=chunk_size,
                chunk_hashes=chunk_hashes,
                expected_file_hash=file_hash
            )
            cm.initialize_storage(is_seeder=False)

            # Spawn concurrent write tasks in random order
            async def write_worker(idx):
                chunk_slice = raw_data[idx * chunk_size : (idx + 1) * chunk_size]
                await asyncio.sleep(0.001 * (idx % 5)) # Jitter to simulate network arrival
                ok = await cm.write_chunk_atomic(idx, chunk_slice)
                assert ok is True

            tasks = [write_worker(i) for i in range(num_chunks)]
            await asyncio.gather(*tasks)

            assert cm.is_complete() is True
            assert cm.verify_final_file() is True

            with open(dest_path, "rb") as rf:
                assert rf.read() == raw_data
        finally:
            for p in [src_path, dest_path]:
                if os.path.exists(p):
                    os.remove(p)

    asyncio.run(_run())

def test_2_concurrent_peer_requests_to_single_seeder():
    """
    Concurrency Test 2:
    Nhiều Peer (3 Leecher) cùng đồng thời gửi yêu cầu các chunk từ 1 Seeder duy nhất.
    Đảm bảo Seeder xử lý nhiều kết nối/luồng đồng thời không bị nghẽn hay lỗi dữ liệu.
    """
    async def _run():
        num_chunks = 10
        chunk_size = 512
        raw_data = b"SeederDataBlock_" * (num_chunks * chunk_size // 16)
        raw_data = raw_data[: num_chunks * chunk_size]

        with tempfile.NamedTemporaryFile(delete=False) as sf:
            sf.write(raw_data)
            seeder_file = sf.name

        meta_path = seeder_file + ".torrent.json"
        leecher_files = [seeder_file + f".leecher_{i}" for i in range(3)]

        try:
            meta = TorrentMetadata(seeder_file, chunk_size=chunk_size)
            meta.save(meta_path)

            seeder = PeerNode(host="127.0.0.1", port=50301)
            seeder.init_from_metadata(meta_path, seeder_file, is_seeder=True)
            await seeder.start_server()

            target_peer = {
                "peer_id": "127.0.0.1:50301",
                "host": "127.0.0.1",
                "port": 50301,
                "chunks": list(range(num_chunks))
            }

            leechers = []
            for i in range(3):
                l_node = PeerNode(host="127.0.0.1", port=50310 + i)
                l_node.init_from_metadata(meta_path, leecher_files[i], is_seeder=False)
                leechers.append(l_node)

            # Each leecher concurrently requests chunks
            async def download_all(node):
                for ch in range(num_chunks):
                    ok = await node.download_chunk_from_peer(ch, target_peer)
                    assert ok is True

            # All 3 leechers pull concurrently from the same seeder
            await asyncio.gather(*(download_all(l) for l in leechers))

            for l in leechers:
                assert l.chunk_mgr.is_complete() is True
                assert l.chunk_mgr.verify_final_file() is True

            await seeder.stop_server()
        finally:
            for p in [seeder_file, meta_path] + leecher_files:
                if os.path.exists(p):
                    os.remove(p)

    asyncio.run(_run())
