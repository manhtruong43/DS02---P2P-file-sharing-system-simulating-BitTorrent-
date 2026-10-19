"""
DS02 – Demo hoạt động bình thường của hệ thống (Kịch bản 1, 2, 3, 7 – Mục 9 đề bài)

  KB1: Tạo metadata và chia sẻ file      -> file .torrent.json + log seeder đăng ký tracker
  KB2: Tải song song từ >= 2 peer         -> chunk đến từ nhiều peer khác nhau, nhiều chunk cùng lúc
  KB3: Peer upload trước khi tải xong     -> Leecher 1 phục vụ chunk cho Leecher 2 khi chưa đủ file
  KB7: Ghép file và verify hash cuối      -> hash file tải về trùng hash gốc

Mỗi node là MỘT TIẾN TRÌNH RIÊNG:
  Tracker (8000)  |  Seeder (6881)  |  Leecher 1 (6882)  |  Leecher 2 (6883)
Cổng peer 6881-6889 là dải cổng truyền thống của BitTorrent, nằm dưới dải cổng tạm (ephemeral)
của Windows/Linux nên không bị trùng với cổng mà hệ điều hành cấp cho các kết nối ra ngoài.
Mỗi peer có thư mục dữ liệu riêng; nội dung file chỉ đi giữa các peer qua gRPC.
Băng thông upload mỗi peer được giới hạn (mặc định 512 KB/s) để quá trình tải kéo dài
vài giây, đủ để quan sát tải song song và chia sẻ chéo trên cùng một máy.

Cách chạy (từ thư mục gốc repo):
  python scripts/demo_all.py
  python scripts/demo_all.py --size-mb 8 --upload-limit 1024
Log của từng node: demo_workspace/demo_all/logs/
"""
import os
import sys
import time
import json
import argparse
from collections import Counter

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from scripts.demo_common import (
    banner, step, info, prepare_workspace, create_sample_file, create_metadata,
    sha256_of, ensure_ports_free, start_tracker, start_peer, stop_all, wait_peer_on_tracker,
    get_tracker_peers, read_log, grep, short, chunk_sources, wait_downloads,
    Evidence, print_log_locations,
)

TRACKER_PORT = 8000
SEEDER_PORT = 6881
LEECHER1_PORT = 6882
LEECHER2_PORT = 6883
TRACKER_URL = f"http://127.0.0.1:{TRACKER_PORT}"


def main() -> None:
    parser = argparse.ArgumentParser(description="DS02 demo: KB1, KB2, KB3, KB7")
    parser.add_argument("--size-mb", type=int, default=4, help="Kích thước file mẫu (MB)")
    parser.add_argument("--chunk-kb", type=int, default=128, help="Kích thước chunk (KB)")
    parser.add_argument("--upload-limit", type=float, default=512, help="Giới hạn upload mỗi peer (KB/s)")
    args = parser.parse_args()

    banner("DS02 BITTORRENT – DEMO HOẠT ĐỘNG BÌNH THƯỜNG (KB1, KB2, KB3, KB7)")
    ensure_ports_free([TRACKER_PORT, SEEDER_PORT, LEECHER1_PORT, LEECHER2_PORT])

    work_dir, log_dir = prepare_workspace("demo_all")
    seeder_dir = os.path.join(work_dir, "seeder")
    l1_dir = os.path.join(work_dir, "leecher_1")
    l2_dir = os.path.join(work_dir, "leecher_2")
    for d in (seeder_dir, l1_dir, l2_dir):
        os.makedirs(d, exist_ok=True)

    evidence = Evidence()
    nodes = []
    try:
        # ------------------------------------------------------------------
        step(f"[KB1] Seeder tạo file mẫu {args.size_mb} MB và sinh metadata (.torrent.json)")
        source = os.path.join(seeder_dir, "demo_file.bin")
        create_sample_file(source, args.size_mb)
        meta_path, meta = create_metadata(source, TRACKER_URL, args.chunk_kb * 1024)
        total_chunks = meta["num_chunks"]
        info(f"Metadata: {meta_path}")
        info(f"file_id   = {meta['file_id'][:16]}..")
        info(f"file_size = {meta['file_size']} B | chunk_size = {meta['chunk_size']} B | num_chunks = {total_chunks}")
        info(f"file_hash = {meta['file_hash'][:16]}.. | chunk_hashes: {len(meta['chunk_hashes'])} mã SHA-256")
        info(f"trackers  = {meta['trackers']}")
        required = ["file_id", "file_name", "file_size", "chunk_size", "num_chunks",
                    "chunk_hashes", "file_hash", "trackers"]
        with open(meta_path, encoding="utf-8") as f:
            saved = json.load(f)
        missing = [k for k in required if k not in saved]

        # ------------------------------------------------------------------
        step(f"Khởi động Tracker (cổng {TRACKER_PORT}) – chỉ quản lý metadata & danh sách peer")
        tracker = start_tracker(log_dir, TRACKER_PORT)
        nodes.append(tracker)
        info("Tracker sẵn sàng.")

        step(f"[KB1] Khởi động Seeder (cổng {SEEDER_PORT}) – đăng ký file với Tracker")
        seeder = start_peer("Seeder", log_dir, SEEDER_PORT, meta_path, source,
                            seed=True, upload_limit_kbps=args.upload_limit)
        nodes.append(seeder)
        wait_peer_on_tracker(TRACKER_URL, meta["file_id"], seeder)
        info("Tracker đã ghi nhận Seeder với đủ 100% chunk.")

        # ------------------------------------------------------------------
        step(f"[KB2, KB3] Khởi động Leecher 1 (cổng {LEECHER1_PORT}) và Leecher 2 (cổng {LEECHER2_PORT})")
        out1 = os.path.join(l1_dir, "downloaded.bin")
        out2 = os.path.join(l2_dir, "downloaded.bin")
        leecher1 = start_peer("Leecher 1", log_dir, LEECHER1_PORT, meta_path, out1,
                              upload_limit_kbps=args.upload_limit)
        nodes.append(leecher1)
        time.sleep(1.0)  # Leecher 1 đi trước một chút để có chunk chia sẻ lại cho Leecher 2
        leecher2 = start_peer("Leecher 2", log_dir, LEECHER2_PORT, meta_path, out2,
                              upload_limit_kbps=args.upload_limit)
        nodes.append(leecher2)
        info(f"Upload mỗi peer giới hạn {args.upload_limit:g} KB/s. Đang tải...")

        finished = wait_downloads([leecher1, leecher2], total_chunks)
        peers_on_tracker = get_tracker_peers(TRACKER_URL, meta["file_id"])
        time.sleep(0.5)  # để log [HAVE] / [SUMMARY] cuối cùng kịp ghi ra file

        # ------------------------------------------------------------------
        step("Phân tích log và kiểm tra kết quả")
        seeder_log = read_log(seeder.log_path)
        tracker_log = read_log(tracker.log_path)
        l1_log = read_log(leecher1.log_path)
        l2_log = read_log(leecher2.log_path)

        # KB1
        register_lines = grep(seeder_log, "[REGISTER]") + grep(tracker_log, "Registered file")
        evidence.add(
            "KB1 – Tạo metadata & chia sẻ file",
            f"metadata đủ {len(required)} trường bắt buộc, seeder đăng ký tracker thành công",
            not missing and len(register_lines) >= 2,
            [f"metadata: {os.path.basename(meta_path)} – thiếu trường: {missing or 'không'}"]
            + [short(l) for l in register_lines],
        )

        # KB2
        sources_l2 = chunk_sources(l2_log)
        per_source = Counter(src for src, _ in sources_l2.values())
        request_lines = grep(l2_log, "[REQUEST]")
        evidence.add(
            "KB2 – Tải song song từ >= 2 peer",
            "Leecher 2 nhận chunk từ: " + ", ".join(f"{s} ({n} chunk)" for s, n in sorted(per_source.items())),
            len(per_source) >= 2,
            [short(l) for l in grep(l2_log, "[SUMMARY]")] + [short(l) for l in request_lines[:3]],
        )

        # KB3
        upload_while_downloading = grep(l1_log, "STILL DOWNLOADING")
        evidence.add(
            "KB3 – Peer upload trước khi tải xong",
            f"Leecher 1 phục vụ {len(upload_while_downloading)} chunk khi chính nó CHƯA tải đủ file",
            len(upload_while_downloading) > 0,
            [short(l) for l in upload_while_downloading[:2]],
        )

        # KB7
        hash_ok = []
        proof = []
        for name, path, log in (("Leecher 1", out1, l1_log), ("Leecher 2", out2, l2_log)):
            actual = sha256_of(path) if os.path.exists(path) else "(không có file)"
            ok = actual == meta["file_hash"]
            hash_ok.append(ok)
            proof.append(f"{name}: sha256 = {actual[:16]}.. {'==' if ok else '!='} file_hash {meta['file_hash'][:16]}..")
            proof += [short(l) for l in grep(log, "[VERIFY]")]
        evidence.add(
            "KB7 – Ghép file & verify hash cuối",
            "hash file tải về của cả 2 leecher trùng hash gốc trong metadata",
            finished and all(hash_ok),
            proof,
        )

        evidence.add(
            "Phân tán – >= 3 node độc lập",
            f"Tracker ghi nhận {len(peers_on_tracker)} peer đang hoạt động: "
            + ", ".join(p["peer_id"] for p in peers_on_tracker),
            len(peers_on_tracker) >= 3,
        )

        evidence.print_report(max_proof=4)
        print_log_locations(log_dir)
    finally:
        step("Dừng tất cả tiến trình")
        stop_all(nodes)
        info("Đã dừng.")


if __name__ == "__main__":
    main()
