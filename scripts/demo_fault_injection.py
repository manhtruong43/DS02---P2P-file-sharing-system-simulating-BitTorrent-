import os
import sys
import tempfile
import asyncio
import time

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.common.metadata import TorrentMetadata
from src.peer.peer_node import PeerNode
from src.peer.chunk_manager import ChunkStatus

async def run_fault_injection_demo():
    print("=" * 70)
    print(" DEMO XỬ LÝ LỖI & PHỤC HỒI (FAULT INJECTION & RECOVERY) ")
    print(" Demo Kịch bản 4, 5 (Peer Crash-Stop & Failover)")
    print(" Demo Kịch bản 6 (Phát hiện Chunk giả mạo/hỏng & Tải lại)")
    print("=" * 70)

    # 1. Chuẩn bị dữ liệu mẫu
    num_chunks = 8
    chunk_size = 1024
    raw_data = b"FAULT_TOLERANCE_DEMO_BITTORRENT_" * (num_chunks * chunk_size // 32)
    raw_data = raw_data[: num_chunks * chunk_size]

    with tempfile.NamedTemporaryFile(delete=False) as tf:
        tf.write(raw_data)
        src_file = tf.name

    meta_path = src_file + ".torrent.json"
    out_file = src_file + ".fault_test_out"

    try:
        meta = TorrentMetadata(src_file, chunk_size=chunk_size)
        meta.save(meta_path)

        print("\n[BƯỚC 1] Khởi động 2 Seeder: Seeder 1 (Port 50601) và Seeder 2 (Port 50602)...")
        seeder_1 = PeerNode(host="127.0.0.1", port=50601)
        seeder_1.init_from_metadata(meta_path, src_file, is_seeder=True)
        await seeder_1.start_server()

        seeder_2 = PeerNode(host="127.0.0.1", port=50602)
        seeder_2.init_from_metadata(meta_path, src_file, is_seeder=True)
        await seeder_2.start_server()

        leecher = PeerNode(host="127.0.0.1", port=50603)
        leecher.init_from_metadata(meta_path, out_file, is_seeder=False)
        leecher.active_peers = [
            {"peer_id": "127.0.0.1:50601", "host": "127.0.0.1", "port": 50601, "chunks": list(range(num_chunks))},
            {"peer_id": "127.0.0.1:50602", "host": "127.0.0.1", "port": 50602, "chunks": list(range(num_chunks))}
        ]

        print("\n[BƯỚC 2] Leecher bắt đầu tải Chunk 0, 1 từ Seeder 1...")
        assert await leecher.download_chunk_from_peer(0, leecher.active_peers[0])
        assert await leecher.download_chunk_from_peer(1, leecher.active_peers[0])
        print(" -> Đã tải thành công Chunk 0 và Chunk 1 từ Seeder 1.")

        print("\n[BƯỚC 3] TIÊM LỖI 1: TẮT ĐỘT NGỘT SEEDER 1 (Simulated Crash-Stop)...")
        await seeder_1.stop_server()
        print(" -> Seeder 1 đã bị ngắt kết nối hoàn toàn.")

        print("\n[BƯỚC 4] Leecher phát hiện lỗi kết nối tới Seeder 1, kích hoạt FAILOVER sang Seeder 2...")
        # Thử tải từ Seeder 1 sẽ thất bại
        failed = await leecher.download_chunk_from_peer(2, leecher.active_peers[0])
        print(f" -> Yêu cầu tới Seeder 1 thất bại: {failed} | Điểm phạt bad_peer: {leecher.bad_peers['127.0.0.1:50601']}")
        
        # Tự động chuyển sang Seeder 2
        print(" -> Tự động chuyển sang Seeder 2 để tải tiếp Chunk 2...")
        recovered = await leecher.download_chunk_from_peer(2, leecher.active_peers[1])
        print(f" -> Tải thành công từ Seeder 2: {recovered}")
        assert recovered is True

        print("\n[BƯỚC 5] TIÊM LỖI 2: TIÊM CHUNK BỊ HỎNG / SAI MÃ BĂM (Corrupted Chunk Injection)...")
        fake_bad_chunk = b"MALICIOUS_TAMPERED_BYTES_X" * (chunk_size // 26)
        fake_bad_chunk = fake_bad_chunk[:chunk_size]
        
        write_status = await leecher.chunk_mgr.write_chunk_atomic(3, fake_bad_chunk)
        print(f" -> Kết quả ghi chunk giả mạo vào đĩa: {write_status} (Bị từ chối vì không khớp SHA-256)")
        assert write_status is False
        assert 3 not in leecher.chunk_mgr.chunks_owned

        print("\n[BƯỚC 6] Leecher tải lại Chunk 3 chuẩn xác từ Seeder 2...")
        assert await leecher.download_chunk_from_peer(3, leecher.active_peers[1])
        print(" -> Chunk 3 hợp lệ đã được ghi đĩa thành công.")

        # Tải nốt các chunk còn lại
        for c in range(4, num_chunks):
            await leecher.download_chunk_from_peer(c, leecher.active_peers[1])

        print("\n[BƯỚC 7] Kiểm tra tính toàn vẹn file sau khi phục hồi lỗi...")
        final_valid = leecher.chunk_mgr.verify_final_file()
        print(f" -> File hash verification: {final_valid}")
        assert final_valid is True

        print("\n" + "=" * 70)
        print(" KẾT LUẬN: HỆ THỐNG ĐÃ VƯỢT QUA TOÀN BỘ CÁC KỊCH BẢN LỖI TRỰC TIẾP!")
        print("=" * 70)

        await seeder_2.stop_server()
    finally:
        for p in [src_file, meta_path, out_file]:
            if os.path.exists(p):
                os.remove(p)

if __name__ == "__main__":
    asyncio.run(run_fault_injection_demo())
