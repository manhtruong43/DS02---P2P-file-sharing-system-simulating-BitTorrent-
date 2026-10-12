import os
import asyncio
import logging
from enum import Enum
from typing import Dict, Set, List, Optional

try:
    from src.common.hashing import verify_chunk_data, verify_entire_file, compute_chunk_hash
except ImportError:
    from common.hashing import verify_chunk_data, verify_entire_file, compute_chunk_hash

logger = logging.getLogger("chunk_manager")

class ChunkStatus(Enum):
    MISSING = "MISSING"
    DOWNLOADING = "DOWNLOADING"
    DONE = "DONE"

class ChunkManager:
    """
    Manages chunk states (Piece Manager), chunk split/merge, atomic disk I/O,
    and download resumption for a specific torrent file.
    """
    def __init__(self, file_path: str, file_size: int, chunk_size: int, chunk_hashes: List[str], expected_file_hash: str):
        self.file_path = file_path
        self.file_size = file_size
        self.chunk_size = chunk_size
        self.chunk_hashes = chunk_hashes
        self.expected_file_hash = expected_file_hash
        self.total_chunks = len(chunk_hashes)
        
        # Concurrency control: write lock to ensure atomic disk writes
        self.io_lock = asyncio.Lock()
        
        # State tracking: chunk_index -> ChunkStatus
        self.status: Dict[int, ChunkStatus] = {i: ChunkStatus.MISSING for i in range(self.total_chunks)}
        self.chunks_owned: Set[int] = set()

    def initialize_storage(self, is_seeder: bool = False) -> None:
        """Initializes storage file or resumes downloading by verifying existing chunks."""
        parent_dir = os.path.dirname(self.file_path)
        if parent_dir and not os.path.exists(parent_dir):
            os.makedirs(parent_dir, exist_ok=True)

        if is_seeder:
            if not os.path.exists(self.file_path):
                raise FileNotFoundError(f"Seeder source file not found: {self.file_path}")
            # Verify file size matches
            actual_size = os.path.getsize(self.file_path)
            if actual_size != self.file_size:
                logger.warning(f"File size mismatch: expected {self.file_size}, got {actual_size}")
            self.chunks_owned = set(range(self.total_chunks))
            for i in range(self.total_chunks):
                self.status[i] = ChunkStatus.DONE
            logger.info(f"Seeder initialized with all {self.total_chunks} chunks.")
            return

        if not os.path.exists(self.file_path):
            # Pre-allocate sparse file of expected size
            with open(self.file_path, "wb") as f:
                f.truncate(self.file_size)
            logger.info(f"Initialized blank file of size {self.file_size} bytes: {self.file_path}")
        else:
            # Resume existing download: check verified chunks
            logger.info(f"Existing file found at {self.file_path}. Verifying chunks to resume...")
            verified_count = 0
            with open(self.file_path, "rb") as f:
                for idx in range(self.total_chunks):
                    offset = idx * self.chunk_size
                    f.seek(offset)
                    data = f.read(self.chunk_size)
                    if data and verify_chunk_data(data, self.chunk_hashes[idx]):
                        self.chunks_owned.add(idx)
                        self.status[idx] = ChunkStatus.DONE
                        verified_count += 1
                    else:
                        self.status[idx] = ChunkStatus.MISSING
            logger.info(f"Resume scan complete: {verified_count}/{self.total_chunks} chunks verified and ready.")

    def read_chunk(self, chunk_index: int) -> bytes:
        """Reads a specific chunk from the storage file."""
        if chunk_index not in self.chunks_owned:
            raise ValueError(f"Chunk {chunk_index} is not owned by this node.")

        offset = chunk_index * self.chunk_size
        with open(self.file_path, "rb") as f:
            f.seek(offset)
            # Last chunk might be smaller than chunk_size
            read_len = min(self.chunk_size, self.file_size - offset)
            return f.read(read_len)

    async def write_chunk_atomic(self, chunk_index: int, data: bytes) -> bool:
        """
        Atomically writes verified chunk data to disk protected by asyncio.Lock().
        Returns True if hash verified and written successfully, False otherwise.
        """
        if chunk_index < 0 or chunk_index >= self.total_chunks:
            logger.error(f"Invalid chunk index: {chunk_index}")
            return False

        # Verify integrity before writing
        expected_hash = self.chunk_hashes[chunk_index]
        if not verify_chunk_data(data, expected_hash):
            actual_hash = compute_chunk_hash(data)
            logger.error(f"Corrupt chunk {chunk_index}! Expected {expected_hash}, got {actual_hash}")
            self.status[chunk_index] = ChunkStatus.MISSING
            return False

        offset = chunk_index * self.chunk_size
        async with self.io_lock:
            with open(self.file_path, "r+b") as f:
                f.seek(offset)
                f.write(data)
            
            self.chunks_owned.add(chunk_index)
            self.status[chunk_index] = ChunkStatus.DONE

        logger.info(f"[ATOMIC WRITE] Chunk {chunk_index}/{self.total_chunks} saved & verified.")
        return True

    def mark_downloading(self, chunk_index: int) -> bool:
        """Marks a chunk as in-flight if it is currently MISSING."""
        if self.status.get(chunk_index) == ChunkStatus.MISSING:
            self.status[chunk_index] = ChunkStatus.DOWNLOADING
            return True
        return False

    def mark_missing(self, chunk_index: int) -> None:
        """Resets chunk status back to MISSING upon download failure or timeout."""
        if chunk_index not in self.chunks_owned:
            self.status[chunk_index] = ChunkStatus.MISSING

    def is_complete(self) -> bool:
        return len(self.chunks_owned) == self.total_chunks

    def verify_final_file(self) -> bool:
        """Verifies full assembled file hash against expected file_hash."""
        if not self.is_complete():
            return False
        return verify_entire_file(self.file_path, self.expected_file_hash)
