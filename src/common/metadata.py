import os
import json
import logging
from typing import List, Dict, Any, Optional

try:
    from src.common.config import DEFAULT_CHUNK_SIZE, DEFAULT_TRACKER_URL
    from src.common.hashing import compute_file_hashes
except ImportError:
    from common.config import DEFAULT_CHUNK_SIZE, DEFAULT_TRACKER_URL
    from common.hashing import compute_file_hashes

logger = logging.getLogger(__name__)

class TorrentMetadata:
    def __init__(
        self,
        file_path: str,
        trackers: Optional[List[str]] = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE
    ):
        self.file_path = file_path
        self.trackers = trackers or [DEFAULT_TRACKER_URL]
        self.chunk_size = chunk_size
        self.file_name = os.path.basename(file_path) if file_path else ""
        self.file_size = 0
        self.chunk_hashes: List[str] = []
        self.file_hash = ""
        self.num_chunks = 0
        self.file_id = ""
        self.data: Dict[str, Any] = {}

    def generate(self) -> Dict[str, Any]:
        """Reads file, hashes chunks, and builds required metadata specification."""
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"File not found: {self.file_path}")

        file_hash, chunk_hashes, file_size = compute_file_hashes(self.file_path, self.chunk_size)
        
        self.file_size = file_size
        self.file_hash = file_hash
        self.chunk_hashes = chunk_hashes
        self.num_chunks = len(chunk_hashes)
        # file_id is the unique hash identifier of the file
        self.file_id = file_hash

        self.data = {
            "file_id": self.file_id,
            "file_name": self.file_name,
            "file_size": self.file_size,
            "chunk_size": self.chunk_size,
            "num_chunks": self.num_chunks,
            "chunk_hashes": self.chunk_hashes,
            "file_hash": self.file_hash,
            "trackers": self.trackers,
            # Legacy compatibility fields
            "tracker_url": self.trackers[0] if self.trackers else "",
            "file_info": {
                "name": self.file_name,
                "size": self.file_size,
                "chunk_size": self.chunk_size,
                "chunk_count": self.num_chunks,
                "file_hash": self.file_hash,
                "chunk_hashes": self.chunk_hashes
            }
        }
        return self.data

    def save(self, output_path: str) -> None:
        """Saves generated metadata into a .torrent.json file."""
        if not self.data:
            self.generate()
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=4)
        logger.info(f"Metadata saved to {output_path}")

    @classmethod
    def load(cls, file_path: str) -> Dict[str, Any]:
        """Loads metadata dictionary from a JSON file."""
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        # Ensure backward compatibility with either schema
        if "file_info" not in data and "file_hash" in data:
            data["file_info"] = {
                "name": data.get("file_name", ""),
                "size": data.get("file_size", 0),
                "chunk_size": data.get("chunk_size", DEFAULT_CHUNK_SIZE),
                "chunk_count": data.get("num_chunks", len(data.get("chunk_hashes", []))),
                "file_hash": data.get("file_hash", data.get("file_id", "")),
                "chunk_hashes": data.get("chunk_hashes", [])
            }
        if "file_id" not in data and "file_info" in data:
            data["file_id"] = data["file_info"].get("file_hash", "")
        if "trackers" not in data and "tracker_url" in data:
            data["trackers"] = [data["tracker_url"]]
        return data

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="DS02 Metadata Generator")
    parser.add_argument("file", help="Path to input file")
    parser.add_argument("tracker", nargs="?", default=DEFAULT_TRACKER_URL, help="Tracker URL")
    parser.add_argument("--out", "-o", help="Output .torrent.json path")
    args = parser.parse_args()

    meta = TorrentMetadata(args.file, trackers=[args.tracker])
    meta.generate()
    out = args.out or f"{args.file}.torrent.json"
    meta.save(out)
    print(f"Generated metadata for {args.file} -> {out}")
