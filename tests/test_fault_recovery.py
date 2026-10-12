import os
import sys
import tempfile
import asyncio
import pytest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.common.metadata import TorrentMetadata
from src.peer.peer_node import PeerNode

def test_1_source_peer_crash_and_failover():
    """
    Fault & Recovery Test 1:
    Một source peer bị sập (crash-stop) giữa chừng khi đang tải.
    Leecher phát hiện mất kết nối, tự động chuyển (failover) sang source peer khác
    để tiếp tục tải các chunk còn lại thành công.
    """
    async def _run():
        num_chunks = 6
        chunk_size = 512
        raw_data = b"CrashTestReliability" * ((num_chunks * chunk_size) // 20)
        raw_data = raw_data[: num_chunks * chunk_size]

        with tempfile.NamedTemporaryFile(delete=False) as sf:
            sf.write(raw_data)
            src_file = sf.name

        meta_path = src_file + ".torrent.json"
        leecher_out = src_file + ".leecher_recovered"

        try:
            meta = TorrentMetadata(src_file, chunk_size=chunk_size)
            meta.save(meta_path)

            # Seeder 1 on 50401 (will crash after serving 2 chunks)
            seeder_1 = PeerNode(host="127.0.0.1", port=50401)
            seeder_1.init_from_metadata(meta_path, src_file, is_seeder=True)
            await seeder_1.start_server()

            # Seeder 2 on 50402 (backup peer, stays alive)
            seeder_2 = PeerNode(host="127.0.0.1", port=50402)
            seeder_2.init_from_metadata(meta_path, src_file, is_seeder=True)
            await seeder_2.start_server()

            leecher = PeerNode(host="127.0.0.1", port=50403)
            leecher.init_from_metadata(meta_path, leecher_out, is_seeder=False)
            leecher.active_peers = [
                {"peer_id": "127.0.0.1:50401", "host": "127.0.0.1", "port": 50401, "chunks": list(range(num_chunks))},
                {"peer_id": "127.0.0.1:50402", "host": "127.0.0.1", "port": 50402, "chunks": list(range(num_chunks))}
            ]

            # Step 1: Download chunks 0 and 1 from Seeder 1
            ok0 = await leecher.download_chunk_from_peer(0, leecher.active_peers[0])
            ok1 = await leecher.download_chunk_from_peer(1, leecher.active_peers[0])
            assert ok0 is True and ok1 is True

            # Step 2: Crash Seeder 1 abruptly!
            await seeder_1.stop_server()

            # Step 3: Attempting chunk 2 from dead Seeder 1 fails
            dead_ok = await leecher.download_chunk_from_peer(2, leecher.active_peers[0])
            assert dead_ok is False
            # Bad peer penalty recorded
            assert leecher.bad_peers["127.0.0.1:50401"] > 0

            # Step 4: Failover to Seeder 2 for remaining chunks
            for ch in range(2, num_chunks):
                retry_ok = await leecher.download_chunk_from_peer(ch, leecher.active_peers[1])
                assert retry_ok is True

            assert leecher.chunk_mgr.is_complete() is True
            assert leecher.chunk_mgr.verify_final_file() is True

            await seeder_2.stop_server()
        finally:
            for p in [src_file, meta_path, leecher_out]:
                if os.path.exists(p):
                    os.remove(p)

    asyncio.run(_run())

def test_2_download_interruption_and_resume():
    """
    Fault & Recovery Test 2:
    Quá trình tải bị dừng đột ngột giữa chừng (chỉ tải được một số chunk).
    Khởi động lại PeerNode: node đọc trạng thái file, verify các chunk đã có,
    chỉ tải tiếp các chunk còn thiếu mà không phải tải lại từ đầu (Resume).
    """
    async def _run():
        num_chunks = 8
        chunk_size = 512
        raw_data = b"".join([bytes([(i * 17) % 256]) * chunk_size for i in range(num_chunks)])

        with tempfile.NamedTemporaryFile(delete=False) as sf:
            sf.write(raw_data)
            src_file = sf.name

        meta_path = src_file + ".torrent.json"
        resume_file = src_file + ".interrupted_resume"

        try:
            meta = TorrentMetadata(src_file, chunk_size=chunk_size)
            meta.save(meta_path)

            seeder = PeerNode(host="127.0.0.1", port=50411)
            seeder.init_from_metadata(meta_path, src_file, is_seeder=True)
            await seeder.start_server()

            seeder_info = {
                "peer_id": "127.0.0.1:50411",
                "host": "127.0.0.1",
                "port": 50411,
                "chunks": list(range(num_chunks))
            }

            # Phase 1: Leecher 1 downloads first 3 chunks then crashes
            leecher_1 = PeerNode(host="127.0.0.1", port=50412)
            leecher_1.init_from_metadata(meta_path, resume_file, is_seeder=False)
            leecher_1.active_peers = [seeder_info]

            for c in range(3):
                assert await leecher_1.download_chunk_from_peer(c, seeder_info) is True

            assert len(leecher_1.chunk_mgr.chunks_owned) == 3
            del leecher_1 # Simulated crash / process termination

            # Phase 2: Start a new Leecher instance on the same file -> Resume!
            leecher_2 = PeerNode(host="127.0.0.1", port=50413)
            leecher_2.init_from_metadata(meta_path, resume_file, is_seeder=False)
            leecher_2.active_peers = [seeder_info]

            # Verify that 3 chunks were already restored from local disk
            assert len(leecher_2.chunk_mgr.chunks_owned) == 3
            assert {0, 1, 2}.issubset(leecher_2.chunk_mgr.chunks_owned)

            # Download remaining 5 chunks
            for c in range(3, num_chunks):
                assert await leecher_2.download_chunk_from_peer(c, seeder_info) is True

            assert leecher_2.chunk_mgr.is_complete() is True
            assert leecher_2.chunk_mgr.verify_final_file() is True

            await seeder.stop_server()
        finally:
            for p in [src_file, meta_path, resume_file]:
                if os.path.exists(p):
                    os.remove(p)

    asyncio.run(_run())
