import os
import sys

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.common.metadata import TorrentMetadata, DEFAULT_CHUNK_SIZE, DEFAULT_TRACKER_URL

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate metadata for a file.")
    parser.add_argument("file", help="Path to the file to share")
    parser.add_argument("tracker", nargs="?", default=DEFAULT_TRACKER_URL, help="Tracker URL")
    parser.add_argument("--out", "-o", help="Output metadata file (.torrent.json)")
    args = parser.parse_args()

    metadata_gen = TorrentMetadata(args.file, trackers=[args.tracker])
    metadata_gen.generate()
    out_path = args.out or f"{args.file}.torrent.json"
    metadata_gen.save(out_path)
    print(f"Generated metadata for {args.file} -> {out_path}")
