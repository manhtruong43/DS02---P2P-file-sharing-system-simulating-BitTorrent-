"""
DS02 – Demo lỗi LIVE và phục hồi (Kịch bản 4, 5, 6 – Mục 9 đề bài)

Phần A – Crash-stop một source peer:
  KB4: Tắt một source trong lúc tải  -> leecher phát hiện peer mất (PEER DOWN / TIMEOUT)
  KB5: Chuyển sang source khác       -> chunk được yêu cầu lại từ peer khác (FAILOVER), file vẫn đúng hash
  Tracker (8000) | Seeder 1 (6884) | Seeder 2 (6885) | Leecher (6886)
  Khi Leecher tải được ~25% file, script GIẾT HẲN tiến trình Seeder 1 (kill, không cho báo /leave).

Phần B – Peer độc hại gửi chunk sai:
  KB6: Phát hiện chunk không hợp lệ  -> HASH MISMATCH, không ghi đĩa, phạt/chặn peer, tải lại từ peer trung thực
  Tracker (8000) | Bad Seeder (6887, chạy với --corrupt) | Good Seeder (6888) | Leecher (6889)

Mỗi node là một tiến trình riêng, có thư mục dữ liệu riêng.

Cách chạy (từ thư mục gốc repo):
  python scripts/demo_fault_injection.py              # chạy cả A và B
  python scripts/demo_fault_injection.py --part crash # chỉ phần A
  python scripts/demo_fault_injection.py --part corrupt
Log của từng node: demo_workspace/fault_crash/logs/ và demo_workspace/fault_corrupt/logs/
"""
import os
import sys
import time
import shutil
import argparse
from collections import Counter

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from scripts.demo_common import (
    banner, step, info, warn, prepare_workspace, create_sample_file, create_metadata,
    sha256_of, ensure_ports_free, start_tracker, start_peer, stop_all, wait_peer_on_tracker,
    read_log, grep, short, log_time, chunks_written, chunk_sources, wait_downloads,
    Evidence, print_log_locations, RED, BOLD, RESET,
)

TRACKER_PORT = 8000
TRACKER_URL = f"http://127.0.0.1:{TRACKER_PORT}"


def make_seeder_copies(work_dir: str, size_mb: int, chunk_kb: int, names):
    """Tạo file gốc, metadata, và một bản sao riêng cho từng seeder (mỗi node có dữ liệu riêng)."""
    origin_dir = os.path.join(work_dir, "origin")
    os.makedirs(origin_dir, exist_ok=True)
    origin = os.path.join(origin_dir, "fault_demo.bin")
    create_sample_file(origin, size_mb)
    meta_path, meta = create_metadata(origin, TRACKER_URL, chunk_kb * 1024)
    copies = {}
    for name in names:
        d = os.path.join(work_dir, name)
        os.makedirs(d, exist_ok=True)
        copies[name] = os.path.join(d, "fault_demo.bin")
        shutil.copyfile(origin, copies[name])
    leecher_dir = os.path.join(work_dir, "leecher")
    os.makedirs(leecher_dir, exist_ok=True)
    return meta_path, meta, copies, os.path.join(leecher_dir, "downloaded.bin")


def describe_metadata(meta) -> None:
    info(f"file_id = {meta['file_id'][:16]}.. | {meta['file_size']} B | {meta['num_chunks']} chunk x {meta['chunk_size']} B")


# ---------------------------------------------------------------------------
# Phần A – Crash-stop & failover
# ---------------------------------------------------------------------------
def part_crash(args, evidence: Evidence) -> None:
    ports = {"seeder_1": 6884, "seeder_2": 6885, "leecher": 6886}
    banner("PHẦN A – TẮT ĐỘT NGỘT MỘT SOURCE PEER (KB4, KB5)")
    ensure_ports_free([TRACKER_PORT] + list(ports.values()))
    work_dir, log_dir = prepare_workspace("fault_crash")
    nodes = []
    try:
        step("Chuẩn bị dữ liệu: file gốc + metadata, mỗi seeder có bản sao riêng")
        meta_path, meta, copies, out = make_seeder_copies(
            work_dir, args.size_mb, args.chunk_kb, ["seeder_1", "seeder_2"])
        describe_metadata(meta)
        total = meta["num_chunks"]

        step("Khởi động Tracker, Seeder 1 và Seeder 2")
        tracker = start_tracker(log_dir, TRACKER_PORT)
        nodes.append(tracker)
        s1 = start_peer("Seeder 1", log_dir, ports["seeder_1"], meta_path, copies["seeder_1"],
                        seed=True, upload_limit_kbps=args.upload_limit)
        s2 = start_peer("Seeder 2", log_dir, ports["seeder_2"], meta_path, copies["seeder_2"],
                        seed=True, upload_limit_kbps=args.upload_limit)
        nodes += [s1, s2]
        wait_peer_on_tracker(TRACKER_URL, meta["file_id"], s1)
        wait_peer_on_tracker(TRACKER_URL, meta["file_id"], s2)
        info(f"Cả 2 seeder đã đăng ký với tracker (upload giới hạn {args.upload_limit:g} KB/s mỗi seeder).")

        step(f"Khởi động Leecher (cổng {ports['leecher']}) – tải song song từ 2 seeder")
        leecher = start_peer("Leecher", log_dir, ports["leecher"], meta_path, out)
        nodes.append(leecher)

        kill_after = max(1, total * args.kill_at_percent // 100)
        deadline = time.monotonic() + 60
        while chunks_written(read_log(leecher.log_path)) < kill_after and time.monotonic() < deadline:
            time.sleep(0.05)
        done_before_kill = chunks_written(read_log(leecher.log_path))

        step(f"{RED}{BOLD}TIÊM LỖI LIVE: GIẾT tiến trình Seeder 1 (cổng {ports['seeder_1']}) khi Leecher "
             f"đã có {done_before_kill}/{total} chunk{RESET}")
        killed_at = s1.kill()
        info(f"Seeder 1 bị kill lúc {killed_at.strftime('%H:%M:%S.%f')[:-3]} (crash-stop, không gửi /leave)")

        step("Leecher tự phát hiện lỗi và tự chuyển nguồn – không có can thiệp nào thêm")
        finished = wait_downloads([leecher], total)
        time.sleep(0.5)

        step("Phân tích log")
        l_log = read_log(leecher.log_path)
        s1_target = f"127.0.0.1:{ports['seeder_1']}"
        s2_target = f"127.0.0.1:{ports['seeder_2']}"

        down_lines = [l for l in l_log
                      if ("[PEER DOWN]" in l or "[TIMEOUT]" in l) and s1_target in l
                      and (log_time(l) or killed_at) >= killed_at]
        detect_ms = None
        if down_lines:
            detect_ms = (log_time(down_lines[0]) - killed_at).total_seconds() * 1000
        evidence.add(
            "KB4 – Tắt một source trong lúc tải",
            "Leecher phát hiện Seeder 1 mất kết nối"
            + (f" sau {max(detect_ms, 0):.0f} ms" if detect_ms is not None else ""),
            bool(down_lines),
            [short(l) for l in down_lines] + [short(l) for l in grep(l_log, "[RETRY]")[:1]],
        )

        sources = chunk_sources(l_log)
        after_kill = {c: src for c, (src, t) in sources.items() if t and t > killed_at}
        from_s1_after = [c for c, src in after_kill.items() if src == s1_target]
        failover_lines = [l for l in grep(l_log, "[FAILOVER]") if s2_target in l]
        actual = sha256_of(out) if os.path.exists(out) else ""
        hash_ok = actual == meta["file_hash"]
        per_source = Counter(src for src, _ in sources.values())
        evidence.add(
            "KB5 – Chuyển sang source khác",
            f"{len(failover_lines)} chunk được yêu cầu lại từ Seeder 2; sau khi kill, "
            f"{len(after_kill)} chunk đều đến từ Seeder 2; hash file cuối "
            + ("KHỚP" if hash_ok else "KHÔNG KHỚP"),
            finished and bool(failover_lines) and not from_s1_after and hash_ok,
            [short(l) for l in failover_lines[:2]]
            + [short(l) for l in grep(l_log, "[SUMMARY]")]
            + [f"sha256 file tải về = {actual[:16]}.. | file_hash = {meta['file_hash'][:16]}.."],
        )
        info("Nguồn chunk: " + ", ".join(f"{s}: {n}" for s, n in sorted(per_source.items())))
        print_log_locations(log_dir)
    finally:
        stop_all(nodes)


# ---------------------------------------------------------------------------
# Phần B – Peer gửi chunk hỏng
# ---------------------------------------------------------------------------
def part_corrupt(args, evidence: Evidence) -> None:
    ports = {"bad_seeder": 6887, "good_seeder": 6888, "leecher": 6889}
    banner("PHẦN B – PEER ĐỘC HẠI GỬI CHUNK SAI DỮ LIỆU (KB6)")
    ensure_ports_free([TRACKER_PORT] + list(ports.values()))
    work_dir, log_dir = prepare_workspace("fault_corrupt")
    nodes = []
    try:
        step("Chuẩn bị dữ liệu: file gốc + metadata, mỗi seeder có bản sao riêng")
        meta_path, meta, copies, out = make_seeder_copies(
            work_dir, args.size_mb, args.chunk_kb, ["bad_seeder", "good_seeder"])
        describe_metadata(meta)
        total = meta["num_chunks"]

        step(f"Khởi động Tracker, {RED}Bad Seeder (--corrupt){RESET} và Good Seeder")
        tracker = start_tracker(log_dir, TRACKER_PORT)
        nodes.append(tracker)
        bad = start_peer("Bad Seeder", log_dir, ports["bad_seeder"], meta_path, copies["bad_seeder"],
                         seed=True, corrupt=True, upload_limit_kbps=args.upload_limit)
        good = start_peer("Good Seeder", log_dir, ports["good_seeder"], meta_path, copies["good_seeder"],
                          seed=True, upload_limit_kbps=args.upload_limit)
        nodes += [bad, good]
        wait_peer_on_tracker(TRACKER_URL, meta["file_id"], bad)
        wait_peer_on_tracker(TRACKER_URL, meta["file_id"], good)
        info("Bad Seeder trả lời mọi yêu cầu bằng dữ liệu ngẫu nhiên (sai SHA-256) qua gRPC.")

        step(f"Khởi động Leecher (cổng {ports['leecher']})")
        leecher = start_peer("Leecher", log_dir, ports["leecher"], meta_path, out)
        nodes.append(leecher)
        finished = wait_downloads([leecher], total)
        time.sleep(0.5)

        step("Phân tích log")
        l_log = read_log(leecher.log_path)
        bad_target = f"127.0.0.1:{ports['bad_seeder']}"
        good_target = f"127.0.0.1:{ports['good_seeder']}"

        mismatch = grep(l_log, "[HASH MISMATCH]")
        integrity = [l for l in grep(l_log, "[INTEGRITY FAULT]") if bad_target in l]
        failover = [l for l in grep(l_log, "[FAILOVER]") if good_target in l]
        sources = chunk_sources(l_log)
        from_bad = [c for c, (src, _) in sources.items() if src == bad_target]
        actual = sha256_of(out) if os.path.exists(out) else ""
        hash_ok = actual == meta["file_hash"]
        injected = grep(read_log(bad.log_path), "[FAULT INJECTION]")

        evidence.add(
            "KB6 – Phát hiện chunk không hợp lệ",
            f"Bad Seeder gửi {len(injected)} chunk sai; Leecher phát hiện {len(mismatch)} lần HASH MISMATCH, "
            f"không ghi đĩa, tải lại {len(failover)} chunk từ Good Seeder; "
            f"{len(from_bad)} chunk được chấp nhận từ Bad Seeder",
            finished and bool(mismatch) and bool(integrity) and not from_bad and hash_ok,
            [short(l) for l in mismatch[:1]]
            + [short(l) for l in integrity[-1:]]
            + [short(l) for l in failover[:1]]
            + [short(l) for l in grep(l_log, "[SUMMARY]")]
            + [f"sha256 file tải về = {actual[:16]}.. | file_hash = {meta['file_hash'][:16]}.."],
        )
        if len(mismatch) > 3:
            warn(f"Ghi chú: {len(mismatch)} chunk sai đến trước khi Bad Seeder bị chặn – đó là các yêu cầu "
                 f"đã gửi song song trước thời điểm điểm phạt chạm ngưỡng.")
        print_log_locations(log_dir)
    finally:
        stop_all(nodes)


def main() -> None:
    parser = argparse.ArgumentParser(description="DS02 demo lỗi live: KB4, KB5, KB6")
    parser.add_argument("--part", choices=["all", "crash", "corrupt"], default="all")
    parser.add_argument("--size-mb", type=int, default=4, help="Kích thước file mẫu (MB)")
    parser.add_argument("--chunk-kb", type=int, default=128, help="Kích thước chunk (KB)")
    parser.add_argument("--upload-limit", type=float, default=256, help="Giới hạn upload mỗi seeder (KB/s)")
    parser.add_argument("--kill-at-percent", type=int, default=25,
                        help="Phần A: kill Seeder 1 khi leecher tải được bao nhiêu %% file")
    args = parser.parse_args()

    banner("DS02 BITTORRENT – DEMO LỖI LIVE & PHỤC HỒI (KB4, KB5, KB6)")
    evidence = Evidence()
    if args.part in ("all", "crash"):
        part_crash(args, evidence)
    if args.part in ("all", "corrupt"):
        part_corrupt(args, evidence)
    evidence.print_report(max_proof=5)


if __name__ == "__main__":
    main()
