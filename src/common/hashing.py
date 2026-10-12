import hashlib
from typing import List, Tuple

def compute_chunk_hash(data: bytes) -> str:
    """Computes SHA-256 hex digest for a single byte chunk."""
    return hashlib.sha256(data).hexdigest()

def verify_chunk_data(data: bytes, expected_hash: str) -> bool:
    """Verifies that data matches expected SHA-256 hash."""
    return compute_chunk_hash(data).lower() == expected_hash.lower()

def compute_file_hashes(file_path: str, chunk_size: int) -> Tuple[str, List[str], int]:
    """
    Reads a file chunk-by-chunk and returns:
    (whole_file_sha256, list_of_chunk_sha256, total_file_size)
    """
    file_hasher = hashlib.sha256()
    chunk_hashes = []
    total_size = 0

    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            total_size += len(chunk)
            chunk_hashes.append(compute_chunk_hash(chunk))
            file_hasher.update(chunk)

    return file_hasher.hexdigest(), chunk_hashes, total_size

def verify_entire_file(file_path: str, expected_file_hash: str, buffer_size: int = 65536) -> bool:
    """Verifies full file content SHA-256 matches expected hash."""
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(buffer_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest().lower() == expected_file_hash.lower()
