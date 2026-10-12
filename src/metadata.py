import os
import hashlib
import json
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

# Default chunk size: 256 KB
DEFAULT_CHUNK_SIZE = 256 * 1024

class TorrentMetadata:
    def __init__(self, file_path: str, tracker_url: str, chunk_size: int = DEFAULT_CHUNK_SIZE):
        self.file_path = file_path
        self.tracker_url = tracker_url
        self.chunk_size = chunk_size
        self.file_name = os.path.basename(file_path)
        self.file_size = 0
        self.chunk_hashes = []
        self.file_hash = ""
        self.metadata = {}

    def generate(self) -> Dict[str, Any]:
        """Reads the file, calculates hashes, and generates metadata."""
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"File not found: {self.file_path}")

        self.file_size = os.path.getsize(self.file_path)
        file_hash_obj = hashlib.sha256()

        with open(self.file_path, "rb") as f:
            while True:
                chunk = f.read(self.chunk_size)
                if not chunk:
                    break
                # Hash the chunk
                chunk_hash = hashlib.sha256(chunk).hexdigest()
                self.chunk_hashes.append(chunk_hash)
                # Update whole file hash
                file_hash_obj.update(chunk)

        self.file_hash = file_hash_obj.hexdigest()

        self.metadata = {
            "tracker_url": self.tracker_url,
            "file_info": {
                "name": self.file_name,
                "size": self.file_size,
                "chunk_size": self.chunk_size,
                "chunk_count": len(self.chunk_hashes),
                "file_hash": self.file_hash,
                "chunk_hashes": self.chunk_hashes
            }
        }
        return self.metadata

    def save(self, output_path: str):
        """Saves the generated metadata to a JSON file."""
        if not self.metadata:
            self.generate()
        
        with open(output_path, "w") as f:
            json.dump(self.metadata, f, indent=4)
        logger.info(f"Metadata saved to {output_path}")

    @classmethod
    def load(cls, file_path: str) -> Dict[str, Any]:
        """Loads metadata from a JSON file."""
        with open(file_path, "r") as f:
            return json.load(f)

if __name__ == "__main__":
    # Simple test logic
    import argparse
    parser = argparse.ArgumentParser(description="Generate metadata for a file.")
    parser.add_argument("file", help="Path to the file to share")
    parser.add_argument("tracker", help="Tracker URL")
    parser.add_argument("--out", "-o", help="Output metadata file (.torrent.json)")
    args = parser.parse_args()

    metadata_gen = TorrentMetadata(args.file, args.tracker)
    metadata_gen.generate()
    
    out_path = args.out or f"{args.file}.torrent.json"
    metadata_gen.save(out_path)
    print(f"Generated metadata for {args.file} -> {out_path}")
