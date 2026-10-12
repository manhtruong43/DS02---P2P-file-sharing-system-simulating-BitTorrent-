import os
import sys
import tempfile
import time
import asyncio
import pytest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.common.metadata import TorrentMetadata
from src.peer.peer_node import PeerNode

def test_download_performance_scaling():
    """
    Performance Test:
    Đo thời gian tải và throughput khi tải file qua mạng gRPC:
    Kịch bản: Tải dữ liệu với 1 source peer vs 2 source peers chia sẻ tải song song.
    """
    async def _run():
        num_chunks = 12
        chunk_size = 2048 # 2 KB per chunk -> 24 KB
        raw_data = b"PerformanceTestBenchmarkPayload_" * (num_chunks * chunk_size // 32)
        raw_data = raw_data[: num_chunks * chunk_size]

        with tempfile.NamedTemporaryFile(delete=False) as tf:
            tf.write(raw_data)
            src_file = tf.name

        meta_path = src_file + ".torrent.json"
        out_single = src_file + ".out_single"
        out_multi = src_file + ".out_multi"

        try:
            meta = TorrentMetadata(src_file, chunk_size=chunk_size)
            meta.save(meta_path)

            # Node 1: Seeder on 50501
            seeder_1 = PeerNode(host="127.0.0.1", port=50501)
            seeder_1.init_from_metadata(meta_path, src_file, is_seeder=True)
            await seeder_1.start_server()

            # Node 2: Seeder on 50502
            seeder_2 = PeerNode(host="127.0.0.1", port=50502)
            seeder_2.init_from_metadata(meta_path, src_file, is_seeder=True)
            await seeder_2.start_server()

            # Test 1: Single Peer Download
            leecher_1 = PeerNode(host="127.0.0.1", port=50503)
            leecher_1.init_from_metadata(meta_path, out_single, is_seeder=False)
            peer_info_1 = {"peer_id": "127.0.0.1:50501", "host": "127.0.0.1", "port": 50501, "chunks": list(range(num_chunks))}

            start_t1 = time.perf_counter()
            for c in range(num_chunks):
                assert await leecher_1.download_chunk_from_peer(c, peer_info_1) is True
            duration_single = time.perf_counter() - start_t1

            # Test 2: Dual Peer Parallel Download
            leecher_2 = PeerNode(host="127.0.0.1", port=50504)
            leecher_2.init_from_metadata(meta_path, out_multi, is_seeder=False)
            peer_info_2 = {"peer_id": "127.0.0.1:50502", "host": "127.0.0.1", "port": 50502, "chunks": list(range(num_chunks))}

            start_t2 = time.perf_counter()
            # Split chunks across 2 seeders concurrently
            tasks = []
            for c in range(num_chunks):
                chosen = peer_info_1 if (c % 2 == 0) else peer_info_2
                tasks.append(leecher_2.download_chunk_from_peer(c, chosen))
            results = await asyncio.gather(*tasks)
            duration_multi = time.perf_counter() - start_t2

            assert all(results) is True
            assert leecher_2.chunk_mgr.is_complete() is True

            print(f"\n[PERFORMANCE RESULTS]")
            print(f"File Size: {len(raw_data)} bytes ({num_chunks} chunks)")
            print(f"Single Peer Duration: {duration_single:.4f}s")
            print(f"Dual Peers Parallel Duration: {duration_multi:.4f}s")
            assert duration_single > 0
            assert duration_multi > 0

            await seeder_1.stop_server()
            await seeder_2.stop_server()
        finally:
            for p in [src_file, meta_path, out_single, out_multi]:
                if os.path.exists(p):
                    os.remove(p)

    asyncio.run(_run())
