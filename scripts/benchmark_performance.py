import os
import sys
import tempfile
import time
import asyncio

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.common.metadata import TorrentMetadata
from src.peer.peer_node import PeerNode

async def benchmark():
    print("=" * 70)
    print(" DS02 BITTORRENT - PERFORMANCE & SCALABILITY BENCHMARK ")
    print("=" * 70)

    # 1. Tạo file dữ liệu thử nghiệm (2 MB)
    file_size_kb = 2048 # 2 MB
    chunk_size = 64 * 1024 # 64 KB
    num_chunks = file_size_kb * 1024 // chunk_size # 32 chunks
    total_bytes = num_chunks * chunk_size

    print(f"Kích thước file thử nghiệm: {file_size_kb} KB ({num_chunks} chunks x {chunk_size // 1024} KB)")

    with tempfile.NamedTemporaryFile(delete=False) as tf:
        tf.write(os.urandom(total_bytes))
        src_file = tf.name

    meta_path = src_file + ".torrent.json"
    meta = TorrentMetadata(src_file, chunk_size=chunk_size)
    meta.save(meta_path)

    # Tạo 3 Seeder
    seeders = []
    seeder_ports = [50701, 50702, 50703]
    for i, port in enumerate(seeder_ports):
        node = PeerNode(host="127.0.0.1", port=port)
        node.init_from_metadata(meta_path, src_file, is_seeder=True)
        await node.start_server()
        seeders.append(node)

    results = []
    leecher_files = []

    try:
        for peer_count in [1, 2, 3]:
            out_file = src_file + f".bench_out_{peer_count}"
            leecher_files.append(out_file)

            leecher = PeerNode(host="127.0.0.1", port=50710 + peer_count)
            leecher.init_from_metadata(meta_path, out_file, is_seeder=False)
            leecher.active_peers = [
                {
                    "peer_id": f"127.0.0.1:{seeder_ports[j]}",
                    "host": "127.0.0.1",
                    "port": seeder_ports[j],
                    "chunks": list(range(num_chunks))
                }
                for j in range(peer_count)
            ]

            start_time = time.perf_counter()

            # Phân bổ chunks cho các peer nguồn song song
            tasks = []
            for ch in range(num_chunks):
                assigned_peer = leecher.active_peers[ch % peer_count]
                tasks.append(leecher.download_chunk_from_peer(ch, assigned_peer))

            completed = await asyncio.gather(*tasks)
            duration = time.perf_counter() - start_time
            throughput_kb_s = (total_bytes / 1024) / duration

            assert all(completed) is True
            assert leecher.chunk_mgr.verify_final_file() is True

            results.append({
                "peers": peer_count,
                "duration": round(duration, 3),
                "throughput": round(throughput_kb_s, 2)
            })
            print(f" -> Tải với {peer_count} Peer nguồn: Thời gian = {duration:.3f}s | Throughput = {throughput_kb_s:.2f} KB/s")

        print("\n" + "=" * 70)
        print(" BẢNG KẾT QUẢ HIỆU NĂNG (DÙNG CHO BÁO CÁO KỸ THUẬT) ")
        print("=" * 70)
        print("| Số lượng Peer nguồn | Thời gian tải (giây) | Throughput (KB/s) | Tăng tốc (Speedup) |")
        print("|:---:|:---:|:---:|:---:|")
        base_dur = results[0]["duration"]
        for r in results:
            speedup = round(base_dur / r["duration"], 2)
            print(f"| {r['peers']} | {r['duration']}s | {r['throughput']} KB/s | {speedup}x |")
        print("=" * 70)

    finally:
        for s in seeders:
            await s.stop_server()
        for p in [src_file, meta_path] + leecher_files:
            if os.path.exists(p):
                os.remove(p)

if __name__ == "__main__":
    asyncio.run(benchmark())
