import os
import sys
import subprocess
import time

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

def main():
    print("=" * 70)
    print(" DS02 BITTORRENT SIMULATION - COMPLETE SYSTEM DEMO ")
    print(" Kịch bản 1: Tạo metadata & Đăng ký Tracker")
    print(" Kịch bản 2 & 7: Tải song song từ nhiều peer & Xác thực hash cuối")
    print(" Kịch bản 3: Peer vừa tải vừa upload (chia sẻ tức thì qua Immediate HAVE)")
    print("=" * 70)

    # 1. Tạo file dữ liệu mẫu
    data_file = "demo_sample_file.bin"
    chunk_size = 64 * 1024 # 64 KB
    num_chunks = 16
    total_size = num_chunks * chunk_size # 1 MB
    print(f"\n[1] Tạo file mẫu {data_file} ({total_size // 1024} KB, {num_chunks} chunks)...")
    with open(data_file, "wb") as f:
        f.write(os.urandom(total_size))

    # 2. Tạo metadata
    print("[2] Tạo file metadata .torrent.json...")
    meta_path = f"{data_file}.torrent.json"
    subprocess.run([sys.executable, "src/metadata.py", data_file, "http://127.0.0.1:8000", "--out", meta_path], check=True)

    processes = []
    try:
        # 3. Khởi động Tracker Server
        print("\n[3] Khởi động Tracker Server trên port 8000...")
        tracker_log = open("tracker.log", "w")
        p_tracker = subprocess.Popen([sys.executable, "src/tracker.py", "--port", "8000"], stdout=tracker_log, stderr=subprocess.STDOUT)
        processes.append(p_tracker)
        time.sleep(2)

        # 4. Khởi động Seeder (Node có 100% dữ liệu)
        print("[4] Khởi động Seeder trên port 50051...")
        seeder_log = open("seeder.log", "w")
        p_seeder = subprocess.Popen([
            sys.executable, "src/peer_node.py",
            "--port", "50051",
            "--metadata", meta_path,
            "--output", data_file,
            "--seed"
        ], stdout=seeder_log, stderr=subprocess.STDOUT)
        processes.append(p_seeder)
        time.sleep(2)

        # 5. Khởi động Leecher 1 (Node tải từ Seeder và chia sẻ tiếp)
        out_leecher_1 = "downloaded_peer_1.bin"
        print(f"[5] Khởi động Leecher 1 trên port 50052 (lưu vào {out_leecher_1})...")
        l1_log = open("peer_1.log", "w")
        p_l1 = subprocess.Popen([
            sys.executable, "src/peer_node.py",
            "--port", "50052",
            "--metadata", meta_path,
            "--output", out_leecher_1
        ], stdout=l1_log, stderr=subprocess.STDOUT)
        processes.append(p_l1)

        # 6. Khởi động Leecher 2 (Node tải song song từ Seeder VÀ Leecher 1)
        out_leecher_2 = "downloaded_peer_2.bin"
        print(f"[6] Khởi động Leecher 2 trên port 50053 (lưu vào {out_leecher_2})...")
        l2_log = open("peer_2.log", "w")
        p_l2 = subprocess.Popen([
            sys.executable, "src/peer_node.py",
            "--port", "50053",
            "--metadata", meta_path,
            "--output", out_leecher_2
        ], stdout=l2_log, stderr=subprocess.STDOUT)
        processes.append(p_l2)

        print("\nCác Peer đang trao đổi chunk qua mạng gRPC...")
        for sec in range(1, 15):
            time.sleep(1)
            sys.stdout.write(f"\rThời gian trôi qua: {sec}s / 15s")
            sys.stdout.flush()

        print("\n\n" + "=" * 70)
        print(" KẾT QUẢ KIỂM TRA HỆ THỐNG ")
        print("=" * 70)

        # Kiểm tra file hoàn thành
        from src.common.hashing import verify_entire_file
        from src.common.metadata import TorrentMetadata
        meta_info = TorrentMetadata.load(meta_path)
        expected_hash = meta_info["file_info"]["file_hash"]

        for label, out_f in [("Leecher 1", out_leecher_1), ("Leecher 2", out_leecher_2)]:
            if os.path.exists(out_f) and verify_entire_file(out_f, expected_hash):
                print(f"[{label}] Tải thành công trọn vẹn! Hash file trùng khớp tuyệt đối 100%!")
            else:
                print(f"[{label}] Đang tải hoặc chưa hoàn tất.")

        print("\nXem chi tiết hoạt động trong các file log:")
        print(" - tracker.log : Ghi nhận đăng ký, announce và heartbeat")
        print(" - seeder.log  : Phục vụ chunk cho Leecher 1 & Leecher 2")
        print(" - peer_1.log  : Vừa tải từ Seeder vừa phục vụ chunk cho Leecher 2")
        print(" - peer_2.log  : Tải song song đa nguồn")

    finally:
        print("\nĐang dọn dẹp các tiến trình...")
        for p in processes:
            p.terminate()
            p.kill()

        # Dọn dẹp file tạm
        for f in [data_file, meta_path, "downloaded_peer_1.bin", "downloaded_peer_2.bin"]:
            if os.path.exists(f):
                try:
                    os.remove(f)
                except Exception:
                    pass

    print("\nDemo hoàn tất!")

if __name__ == "__main__":
    main()
