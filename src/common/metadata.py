import os
import re
import json
import math
import logging
from typing import List, Dict, Any, Optional

try:
    from src.common.config import (
        DEFAULT_CHUNK_SIZE, DEFAULT_TRACKER_URL,
        MIN_CHUNK_SIZE, MAX_CHUNK_SIZE, MAX_FILE_SIZE, MAX_NUM_CHUNKS
    )
    from src.common.hashing import compute_file_hashes
except ImportError:
    from common.config import (
        DEFAULT_CHUNK_SIZE, DEFAULT_TRACKER_URL,
        MIN_CHUNK_SIZE, MAX_CHUNK_SIZE, MAX_FILE_SIZE, MAX_NUM_CHUNKS
    )
    from common.hashing import compute_file_hashes

logger = logging.getLogger(__name__)

SHA256_HEX_RE = re.compile(r"^[0-9a-f]{64}$")
REQUIRED_FIELDS = ["file_id", "file_name", "file_size", "chunk_size",
                   "num_chunks", "chunk_hashes", "file_hash", "trackers"]


class MetadataError(ValueError):
    """Raised when metadata is invalid or inconsistent (bad input file, bad parameters, tampered JSON)."""


def validate_chunk_size(chunk_size) -> None:
    if isinstance(chunk_size, bool) or not isinstance(chunk_size, int):
        raise MetadataError(f"chunk_size must be an integer, got {chunk_size!r}")
    if not (MIN_CHUNK_SIZE <= chunk_size <= MAX_CHUNK_SIZE):
        raise MetadataError(
            f"chunk_size {chunk_size} out of range [{MIN_CHUNK_SIZE}, {MAX_CHUNK_SIZE}] bytes "
            f"(a chunk must fit in one gRPC message, max 4 MB)"
        )


def validate_metadata(data: Dict[str, Any]) -> None:
    """
    Checks that a metadata dictionary is complete and internally consistent.
    Raises MetadataError describing the first problem found.
    """
    missing = [k for k in REQUIRED_FIELDS if k not in data]
    if missing:
        raise MetadataError(f"metadata is missing required fields: {missing}")

    # Types and ranges
    for key in ("file_size", "chunk_size", "num_chunks"):
        value = data[key]
        if isinstance(value, bool) or not isinstance(value, int):
            raise MetadataError(f"{key} must be an integer, got {value!r}")
    validate_chunk_size(data["chunk_size"])
    if not (1 <= data["file_size"] <= MAX_FILE_SIZE):
        raise MetadataError(f"file_size {data['file_size']} out of range [1, {MAX_FILE_SIZE}] bytes")
    if not (1 <= data["num_chunks"] <= MAX_NUM_CHUNKS):
        raise MetadataError(f"num_chunks {data['num_chunks']} out of range [1, {MAX_NUM_CHUNKS}]")

    # Consistency between size, chunk size and number of chunks
    expected_chunks = math.ceil(data["file_size"] / data["chunk_size"])
    if data["num_chunks"] != expected_chunks:
        raise MetadataError(
            f"num_chunks {data['num_chunks']} does not match file_size/chunk_size (expected {expected_chunks})"
        )

    # Hashes
    hashes = data["chunk_hashes"]
    if not isinstance(hashes, list) or len(hashes) != data["num_chunks"]:
        count = len(hashes) if isinstance(hashes, list) else "not a list"
        raise MetadataError(f"chunk_hashes must contain exactly {data['num_chunks']} hashes, got {count}")
    for i, h in enumerate(hashes):
        if not isinstance(h, str) or not SHA256_HEX_RE.match(h):
            raise MetadataError(f"chunk_hashes[{i}] is not a lowercase 64-character SHA-256 hex string")
    for key in ("file_hash", "file_id"):
        if not isinstance(data[key], str) or not SHA256_HEX_RE.match(data[key]):
            raise MetadataError(f"{key} is not a lowercase 64-character SHA-256 hex string")

    # File name: a plain name only, never a path (prevents path traversal if a client uses it)
    name = data["file_name"]
    if (not isinstance(name, str) or not name or len(name) > 255
            or "/" in name or "\\" in name or name in (".", "..") or "\x00" in name):
        raise MetadataError(f"file_name {name!r} is not a plain file name")

    # Trackers
    trackers = data["trackers"]
    if not isinstance(trackers, list) or not trackers:
        raise MetadataError("trackers must be a non-empty list of URLs")
    for url in trackers:
        if not isinstance(url, str) or not url.startswith(("http://", "https://")):
            raise MetadataError(f"tracker URL {url!r} must start with http:// or https://")


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
        if not os.path.isfile(self.file_path):
            raise FileNotFoundError(f"File not found: {self.file_path}")

        # Validate parameters BEFORE reading anything (chunk_size=0 would otherwise produce 0 chunks)
        validate_chunk_size(self.chunk_size)
        size_on_disk = os.path.getsize(self.file_path)
        if size_on_disk == 0:
            raise MetadataError(f"Cannot share an empty file: {self.file_path}")
        if size_on_disk > MAX_FILE_SIZE:
            raise MetadataError(f"File too large: {size_on_disk} bytes > limit {MAX_FILE_SIZE} bytes")
        # Check the number of chunks from the size alone, BEFORE hashing: with chunk_size = 1 byte a
        # large file would otherwise build millions of hashes (RAM + time) only to be rejected afterwards.
        expected_chunks = math.ceil(size_on_disk / self.chunk_size)
        if expected_chunks > MAX_NUM_CHUNKS:
            raise MetadataError(
                f"chunk_size {self.chunk_size} B is too small for a {size_on_disk} B file: "
                f"{expected_chunks} chunks > limit {MAX_NUM_CHUNKS}"
            )

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
        validate_metadata(self.data)
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
        """
        Loads metadata from a JSON file, normalizes both schemas (flat fields and legacy
        "file_info") and validates it. Raises MetadataError if the metadata is invalid.
        """
        with open(file_path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError as e:
                raise MetadataError(f"{file_path} is not valid JSON: {e}") from e
        if not isinstance(data, dict):
            raise MetadataError(f"{file_path} must contain a JSON object")

        # Legacy schema (only "file_info") -> fill the flat fields
        info = data.get("file_info") if isinstance(data.get("file_info"), dict) else None
        if info is not None:
            data.setdefault("file_name", info.get("name", ""))
            data.setdefault("file_size", info.get("size"))
            data.setdefault("chunk_size", info.get("chunk_size"))
            data.setdefault("num_chunks", info.get("chunk_count"))
            data.setdefault("chunk_hashes", info.get("chunk_hashes"))
            data.setdefault("file_hash", info.get("file_hash", ""))
        if "file_id" not in data and "file_hash" in data:
            data["file_id"] = data["file_hash"]
        if "trackers" not in data and "tracker_url" in data:
            data["trackers"] = [data["tracker_url"]]

        validate_metadata(data)

        # The peer code reads "file_info"; always rebuild it from the validated flat fields
        # so the two views can never disagree.
        data["file_info"] = {
            "name": data["file_name"],
            "size": data["file_size"],
            "chunk_size": data["chunk_size"],
            "chunk_count": data["num_chunks"],
            "file_hash": data["file_hash"],
            "chunk_hashes": data["chunk_hashes"]
        }
        return data


def main() -> None:
    import sys
    import argparse
    parser = argparse.ArgumentParser(description="DS02 Metadata Generator")
    parser.add_argument("file", help="Path to the file to share")
    parser.add_argument("tracker", nargs="?", default=DEFAULT_TRACKER_URL, help="Tracker URL")
    parser.add_argument("--out", "-o", help="Output .torrent.json path")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE,
                        help=f"Chunk size in bytes ({MIN_CHUNK_SIZE}..{MAX_CHUNK_SIZE}), default {DEFAULT_CHUNK_SIZE}")
    args = parser.parse_args()

    meta = TorrentMetadata(args.file, trackers=[args.tracker], chunk_size=args.chunk_size)
    try:
        meta.generate()
    except (MetadataError, FileNotFoundError) as e:
        print(f"[METADATA ERROR] {e}", file=sys.stderr)
        sys.exit(1)
    out = args.out or f"{args.file}.torrent.json"
    meta.save(out)
    print(f"Generated metadata for {args.file} -> {out} "
          f"({meta.num_chunks} chunks x {meta.chunk_size} B)")


if __name__ == "__main__":
    main()
