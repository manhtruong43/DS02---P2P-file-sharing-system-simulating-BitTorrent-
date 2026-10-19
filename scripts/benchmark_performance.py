"""
DS02 BitTorrent - Performance & Scalability Benchmark

Thí nghiệm A: Thời gian tải / throughput theo SỐ PEER NGUỒN (1, 2, 3, 5, 10).
Thí nghiệm B: Thời gian tải / throughput theo KÍCH THƯỚC CHUNK (16 KB -> 1 MB).

Mỗi Seeder chạy trong MỘT TIẾN TRÌNH RIÊNG (subprocess) và giao tiếp với Leecher
qua gRPC trên mạng loopback, đúng yêu cầu "các thành phần là tiến trình độc lập".

Mô phỏng mạng thật: mỗi Seeder bị giới hạn băng thông UPLOAD (mặc định 2 MB/s,
đổi bằng --upload-limit, đặt 0 để bỏ giới hạn). Trên loopback băng thông gần như vô hạn,
nếu không giới hạn thì không thể thấy lợi ích của tải đa nguồn.
Thí nghiệm không có Tracker, nên announce lên Tracker của leecher được tắt để chỉ đo
thời gian truyền chunk giữa các peer.

Kết quả được ghi vào docs/benchmark/:
  - results_peers.csv, results_chunksize.csv   (số liệu thô từng lần chạy)
  - peers_vs_time.png, chunksize_vs_throughput.png (biểu đồ cho báo cáo)
  - results.md                                  (bảng Markdown để dán vào báo cáo)

Cách chạy (từ thư mục gốc repo):
  python scripts/benchmark_performance.py            # đầy đủ (~1-3 phút)
  python scripts/benchmark_performance.py --quick    # chạy nhanh để thử
  python scripts/benchmark_performance.py --upload-limit 0   # không giới hạn băng thông
"""
import os
import sys
import csv
import time
import shutil
import asyncio
import argparse
import platform
import statistics
import subprocess
import tempfile

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import grpc

from src.common.config import MAX_CONCURRENT_DOWNLOADS
from src.common.metadata import TorrentMetadata
from src.peer.peer_node import PeerNode
from src.peer.peer_servicer import PeerServicer
from src.common import peer_pb2_grpc

SEEDER_BASE_PORT = 50900
LEECHER_BASE_PORT = 50960
SERIES_COLOR = "#2a78d6"
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
GRID_COLOR = "#e4e3df"


# ---------------------------------------------------------------------------
# Chế độ --serve: chạy MỘT seeder trong tiến trình riêng (được gọi bằng subprocess)
# ---------------------------------------------------------------------------
class ThrottledServicer(PeerServicer):
    """
    PeerServicer có giới hạn băng thông upload (chỉ dùng cho benchmark).
    Mô phỏng một đường truyền upload duy nhất: các chunk được "gửi" lần lượt,
    mỗi chunk mất len(data) / rate giây. rate = 0 nghĩa là không giới hạn.
    """
    def __init__(self, node_context, rate_bytes_per_s: float):
        super().__init__(node_context)
        self.rate = rate_bytes_per_s
        self.uplink = asyncio.Lock()

    async def GetChunk(self, request, context):
        response = await super().GetChunk(request, context)
        if self.rate > 0 and response.success:
            async with self.uplink:
                await asyncio.sleep(len(response.data) / self.rate)
        return response


async def serve_forever(port: int, metadata_path: str, file_path: str, upload_limit_mb: float) -> None:
    node = PeerNode(host="127.0.0.1", port=port)
    node.init_from_metadata(metadata_path, file_path, is_seeder=True)
    server = grpc.aio.server()
    peer_pb2_grpc.add_PeerServiceServicer_to_server(
        ThrottledServicer(node, upload_limit_mb * 1024 * 1024), server
    )
    server.add_insecure_port(f"127.0.0.1:{port}")
    await server.start()
    try:
        await server.wait_for_termination()
    finally:
        await server.stop(grace=1.0)


# ---------------------------------------------------------------------------
# Quản lý các tiến trình seeder
# ---------------------------------------------------------------------------
async def wait_until_ready(port: int, timeout: float = 15.0) -> None:
    """Chờ tới khi gRPC server của seeder ở port này sẵn sàng nhận kết nối."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
                await asyncio.wait_for(channel.channel_ready(), timeout=1.0)
                return
        except Exception:
            await asyncio.sleep(0.2)
    raise RuntimeError(f"Seeder on port {port} did not become ready within {timeout}s")


async def start_seeders(count: int, metadata_path: str, file_path: str, log_dir: str, upload_limit_mb: float):
    """Khởi động `count` seeder, mỗi seeder là một tiến trình Python độc lập."""
    procs = []
    for i in range(count):
        port = SEEDER_BASE_PORT + i
        log_file = open(os.path.join(log_dir, f"seeder_{port}.log"), "w")
        proc = subprocess.Popen(
            [
                sys.executable, os.path.abspath(__file__), "--serve",
                "--port", str(port),
                "--metadata", metadata_path,
                "--file", file_path,
                "--upload-limit", str(upload_limit_mb),
            ],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            cwd=ROOT_DIR,
        )
        procs.append((proc, log_file, port))
    for _, _, port in procs:
        await wait_until_ready(port)
    return procs


def stop_seeders(procs) -> None:
    for proc, log_file, _ in procs:
        if proc.poll() is None:
            proc.terminate()
    for proc, log_file, _ in procs:
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        log_file.close()


# ---------------------------------------------------------------------------
# Một lần tải đo đạc
# ---------------------------------------------------------------------------
async def timed_download(metadata_path: str, out_path: str, seeder_ports, leecher_port: int):
    """
    Tải toàn bộ file từ các seeder cho trước.
    Chunk được chia vòng tròn (round-robin) cho các seeder; số chunk tải đồng thời
    bị giới hạn bởi MAX_CONCURRENT_DOWNLOADS giống như peer thật.
    Trả về (thời gian giây, số byte).
    """
    if os.path.exists(out_path):
        os.remove(out_path)

    leecher = PeerNode(host="127.0.0.1", port=leecher_port)
    leecher.init_from_metadata(metadata_path, out_path, is_seeder=False)

    # Không có Tracker trong thí nghiệm -> tắt announce để chỉ đo truyền chunk
    async def _no_announce(chunk_idx: int) -> None:
        return None
    leecher.announce_chunk_immediately = _no_announce

    total_chunks = leecher.chunk_mgr.total_chunks
    peers = [
        {"peer_id": f"127.0.0.1:{p}", "host": "127.0.0.1", "port": p, "chunks": list(range(total_chunks))}
        for p in seeder_ports
    ]

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

    async def fetch(idx: int) -> bool:
        async with semaphore:
            return await leecher.download_chunk_from_peer(idx, peers[idx % len(peers)])

    start = time.perf_counter()
    results = await asyncio.gather(*(fetch(i) for i in range(total_chunks)))
    duration = time.perf_counter() - start

    if not all(results):
        raise RuntimeError(f"Download failed for {results.count(False)} chunk(s)")
    if not leecher.chunk_mgr.verify_final_file():
        raise RuntimeError("Final file hash mismatch after download")

    size = leecher.chunk_mgr.file_size
    os.remove(out_path)
    return duration, size


# ---------------------------------------------------------------------------
# Thí nghiệm A: theo số peer nguồn
# ---------------------------------------------------------------------------
async def experiment_peers(work_dir, log_dir, file_path, peer_counts, chunk_size, repeats, upload_limit_mb):
    meta_path = os.path.join(work_dir, "bench_peers.torrent.json")
    TorrentMetadata(file_path, chunk_size=chunk_size).save(meta_path)

    rows = []
    procs = await start_seeders(max(peer_counts), meta_path, file_path, log_dir, upload_limit_mb)
    try:
        for count in peer_counts:
            ports = [SEEDER_BASE_PORT + i for i in range(count)]
            for run in range(1, repeats + 1):
                duration, size = await timed_download(
                    meta_path, os.path.join(work_dir, "out_peers.bin"), ports, LEECHER_BASE_PORT
                )
                throughput = size / (1024 * 1024) / duration
                rows.append({"peers": count, "run": run, "seconds": round(duration, 4),
                             "throughput_mb_s": round(throughput, 2)})
                print(f"  [A] {count:>2} peer nguồn | lần {run} | {duration:.3f}s | {throughput:.2f} MB/s")
    finally:
        stop_seeders(procs)
    return rows


# ---------------------------------------------------------------------------
# Thí nghiệm B: theo kích thước chunk
# ---------------------------------------------------------------------------
async def experiment_chunk_size(work_dir, log_dir, file_path, chunk_sizes, peer_count, repeats, upload_limit_mb):
    rows = []
    for chunk_size in chunk_sizes:
        meta_path = os.path.join(work_dir, f"bench_chunk_{chunk_size}.torrent.json")
        TorrentMetadata(file_path, chunk_size=chunk_size).save(meta_path)
        procs = await start_seeders(peer_count, meta_path, file_path, log_dir, upload_limit_mb)
        try:
            ports = [SEEDER_BASE_PORT + i for i in range(peer_count)]
            num_chunks = -(-os.path.getsize(file_path) // chunk_size)
            for run in range(1, repeats + 1):
                duration, size = await timed_download(
                    meta_path, os.path.join(work_dir, "out_chunk.bin"), ports, LEECHER_BASE_PORT + 1
                )
                throughput = size / (1024 * 1024) / duration
                rows.append({"chunk_kb": chunk_size // 1024, "num_chunks": num_chunks, "run": run,
                             "seconds": round(duration, 4), "throughput_mb_s": round(throughput, 2)})
                print(f"  [B] chunk {chunk_size // 1024:>5} KB ({num_chunks:>4} chunk) | lần {run} | "
                      f"{duration:.3f}s | {throughput:.2f} MB/s")
        finally:
            stop_seeders(procs)
    return rows


# ---------------------------------------------------------------------------
# Tổng hợp, ghi CSV, vẽ biểu đồ
# ---------------------------------------------------------------------------
def summarize(rows, key):
    groups = {}
    for r in rows:
        groups.setdefault(r[key], []).append(r)
    summary = []
    for k in sorted(groups):
        secs = [r["seconds"] for r in groups[k]]
        thr = [r["throughput_mb_s"] for r in groups[k]]
        summary.append({
            key: k,
            "mean_s": statistics.mean(secs),
            "min_s": min(secs),
            "max_s": max(secs),
            "mean_mb_s": statistics.mean(thr),
            "extra": groups[k][0],
        })
    return summary


def write_csv(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def style_axes(ax):
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID_COLOR)
    ax.tick_params(colors=TEXT_SECONDARY, labelsize=10)
    ax.yaxis.grid(True, color=GRID_COLOR, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.xaxis.label.set_color(TEXT_SECONDARY)
    ax.yaxis.label.set_color(TEXT_SECONDARY)
    ax.title.set_color(TEXT_PRIMARY)


def plot_line(path, x_labels, means, mins, maxs, title, xlabel, ylabel):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(7, 4.2), dpi=150)
    fig.patch.set_facecolor("#fcfcfb")
    ax.set_facecolor("#fcfcfb")
    xs = list(range(len(x_labels)))

    # Dải min-max của các lần chạy lặp lại
    ax.fill_between(xs, mins, maxs, color=SERIES_COLOR, alpha=0.15, linewidth=0)
    ax.plot(xs, means, color=SERIES_COLOR, linewidth=2, marker="o", markersize=7,
            markeredgecolor="#fcfcfb", markeredgewidth=2)

    # Chỉ ghi nhãn giá trị ở điểm đầu và điểm cuối
    for i in (0, len(xs) - 1):
        ax.annotate(f"{means[i]:.2f}", (xs[i], means[i]), textcoords="offset points",
                    xytext=(0, 10), ha="center", fontsize=10, color=TEXT_PRIMARY)

    ax.set_xticks(xs)
    ax.set_xticklabels(x_labels)
    ax.set_ylim(bottom=0, top=max(maxs) * 1.25)
    ax.set_title(title, fontsize=12, loc="left", pad=12)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    style_axes(ax)
    fig.tight_layout()
    fig.savefig(path, facecolor=fig.get_facecolor())
    plt.close(fig)


def build_markdown(peer_summary, chunk_summary, file_mb, base_chunk_kb, chunk_peers, repeats, upload_limit_mb):
    lines = []
    lines.append("# Kết quả đo hiệu năng DS02\n")
    lines.append(f"- Thời điểm đo: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"- Máy: {platform.system()} {platform.release()} | Python {platform.python_version()} | "
                 f"{os.cpu_count()} CPU")
    lines.append(f"- File thử nghiệm: {file_mb} MB dữ liệu ngẫu nhiên; mỗi cấu hình chạy {repeats} lần, lấy trung bình")
    lines.append(f"- Giới hạn tải đồng thời của leecher: {MAX_CONCURRENT_DOWNLOADS} chunk")
    limit_text = f"{upload_limit_mb:g} MB/s mỗi seeder" if upload_limit_mb > 0 else "không giới hạn"
    lines.append(f"- Băng thông upload mô phỏng: {limit_text}")
    lines.append("- Mỗi seeder là một tiến trình riêng; giao tiếp gRPC qua loopback (127.0.0.1)")
    lines.append("- Không có Tracker trong thí nghiệm (danh sách peer cho trước), announce được tắt\n")

    lines.append(f"## Thí nghiệm A – Theo số peer nguồn (chunk {base_chunk_kb} KB)\n")
    lines.append("| Số peer nguồn | Thời gian TB (s) | Min – Max (s) | Throughput TB (MB/s) | Speedup |")
    lines.append("|:---:|:---:|:---:|:---:|:---:|")
    base = peer_summary[0]["mean_s"]
    for s in peer_summary:
        lines.append(f"| {s['peers']} | {s['mean_s']:.3f} | {s['min_s']:.3f} – {s['max_s']:.3f} | "
                     f"{s['mean_mb_s']:.2f} | {base / s['mean_s']:.2f}x |")

    lines.append(f"\n## Thí nghiệm B – Theo kích thước chunk ({chunk_peers} peer nguồn)\n")
    lines.append("| Chunk size | Số chunk | Thời gian TB (s) | Min – Max (s) | Throughput TB (MB/s) |")
    lines.append("|:---:|:---:|:---:|:---:|:---:|")
    for s in chunk_summary:
        lines.append(f"| {s['chunk_kb']} KB | {s['extra']['num_chunks']} | {s['mean_s']:.3f} | "
                     f"{s['min_s']:.3f} – {s['max_s']:.3f} | {s['mean_mb_s']:.2f} |")

    lines.append("\n> Lưu ý khi phân tích: các tiến trình cùng chạy trên một máy qua loopback. Giới hạn băng thông")
    lines.append("> upload được mô phỏng bằng phần mềm (sleep theo số byte gửi), không phải đo mạng vật lý.")
    lines.append(f"> Speedup bị chặn trên bởi giới hạn tải đồng thời của leecher ({MAX_CONCURRENT_DOWNLOADS} chunk).")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def benchmark(args) -> None:
    out_dir = os.path.join(ROOT_DIR, args.out_dir)
    os.makedirs(out_dir, exist_ok=True)
    work_dir = tempfile.mkdtemp(prefix="ds02_bench_")
    log_dir = os.path.join(work_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)

    if args.quick:
        file_mb, peer_counts, chunk_sizes, repeats = 2, [1, 2, 3], [64 * 1024, 256 * 1024], 1
    else:
        file_mb = args.file_mb
        peer_counts = [1, 2, 3, 5, 10]
        chunk_sizes = [16 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024]
        repeats = args.repeats
    base_chunk = 256 * 1024
    chunk_peers = 3

    print("=" * 70)
    print(" DS02 BITTORRENT - PERFORMANCE & SCALABILITY BENCHMARK")
    print("=" * 70)
    limit_text = f"{args.upload_limit:g} MB/s/seeder" if args.upload_limit > 0 else "không giới hạn"
    print(f"File thử nghiệm: {file_mb} MB | Lặp mỗi cấu hình: {repeats} lần | Upload: {limit_text}")
    print(f"Thư mục tạm: {work_dir}\n")

    file_path = os.path.join(work_dir, "bench_source.bin")
    with open(file_path, "wb") as f:
        f.write(os.urandom(file_mb * 1024 * 1024))

    try:
        print("Thí nghiệm A: thay đổi số peer nguồn")
        peer_rows = await experiment_peers(work_dir, log_dir, file_path, peer_counts, base_chunk, repeats,
                                           args.upload_limit)
        print("\nThí nghiệm B: thay đổi kích thước chunk")
        chunk_rows = await experiment_chunk_size(work_dir, log_dir, file_path, chunk_sizes, chunk_peers, repeats,
                                                 args.upload_limit)
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)

    write_csv(os.path.join(out_dir, "results_peers.csv"), peer_rows)
    write_csv(os.path.join(out_dir, "results_chunksize.csv"), chunk_rows)

    peer_summary = summarize(peer_rows, "peers")
    chunk_summary = summarize(chunk_rows, "chunk_kb")

    plot_line(
        os.path.join(out_dir, "peers_vs_time.png"),
        [str(s["peers"]) for s in peer_summary],
        [s["mean_s"] for s in peer_summary],
        [s["min_s"] for s in peer_summary],
        [s["max_s"] for s in peer_summary],
        f"Thời gian tải file {file_mb} MB theo số peer nguồn",
        "Số peer nguồn",
        "Thời gian tải (giây)",
    )
    chunk_thr = {}
    for r in chunk_rows:
        chunk_thr.setdefault(r["chunk_kb"], []).append(r["throughput_mb_s"])
    keys = sorted(chunk_thr)
    plot_line(
        os.path.join(out_dir, "chunksize_vs_throughput.png"),
        [f"{k} KB" for k in keys],
        [statistics.mean(chunk_thr[k]) for k in keys],
        [min(chunk_thr[k]) for k in keys],
        [max(chunk_thr[k]) for k in keys],
        f"Throughput theo kích thước chunk ({chunk_peers} peer nguồn)",
        "Kích thước chunk",
        "Throughput (MB/s)",
    )

    markdown = build_markdown(peer_summary, chunk_summary, file_mb, base_chunk // 1024, chunk_peers, repeats,
                              args.upload_limit)
    with open(os.path.join(out_dir, "results.md"), "w", encoding="utf-8") as f:
        f.write(markdown)

    print("\n" + markdown)
    print(f"Đã lưu CSV, biểu đồ PNG và results.md vào: {out_dir}")


def main() -> None:
    parser = argparse.ArgumentParser(description="DS02 performance benchmark")
    parser.add_argument("--quick", action="store_true", help="Chạy nhanh (ít cấu hình, 1 lần lặp)")
    parser.add_argument("--repeats", type=int, default=3, help="Số lần lặp mỗi cấu hình")
    parser.add_argument("--file-mb", type=int, default=8, help="Kích thước file thử nghiệm (MB)")
    parser.add_argument("--out-dir", default=os.path.join("docs", "benchmark"), help="Thư mục lưu kết quả")
    parser.add_argument("--upload-limit", type=float, default=2.0,
                        help="Giới hạn băng thông upload mỗi seeder (MB/s), 0 = không giới hạn")
    # Chế độ nội bộ: chạy một seeder trong tiến trình riêng
    parser.add_argument("--serve", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--port", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--metadata", help=argparse.SUPPRESS)
    parser.add_argument("--file", help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.serve:
        asyncio.run(serve_forever(args.port, args.metadata, args.file, args.upload_limit))
    else:
        asyncio.run(benchmark(args))


if __name__ == "__main__":
    main()
