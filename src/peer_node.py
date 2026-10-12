import asyncio
import hashlib
import json
import logging
import os
from dotenv import load_dotenv
import random
import time
from typing import Dict, List, Set, Optional

import grpc
import httpx

import peer_pb2
import peer_pb2_grpc
from metadata import TorrentMetadata

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

class PeerServicer(peer_pb2_grpc.PeerServiceServicer):
    def __init__(self, node_context):
        self.node = node_context

    async def GetChunk(self, request: peer_pb2.ChunkRequest, context: grpc.aio.ServicerContext) -> peer_pb2.ChunkResponse:
        file_id = request.file_id
        chunk_index = request.chunk_index
        
        # Check if we have the file
        if file_id != self.node.file_id:
            return peer_pb2.ChunkResponse(success=False, error_message="File not managed by this peer")
            
        # Check if we have the chunk
        if chunk_index not in self.node.chunks_owned:
            return peer_pb2.ChunkResponse(success=False, error_message="Chunk not available")
            
        try:
            chunk_data = self.node.read_chunk(chunk_index)
            return peer_pb2.ChunkResponse(success=True, data=chunk_data)
        except Exception as e:
            logger.error(f"Error reading chunk {chunk_index}: {e}")
            return peer_pb2.ChunkResponse(success=False, error_message=str(e))

class PeerNode:
    def __init__(self, host: str, port: int, tracker_url: str):
        self.host = host
        self.port = port
        self.peer_id = f"{host}:{port}"
        self.tracker_url = tracker_url
        
        self.file_id: Optional[str] = None
        self.metadata: Optional[Dict] = None
        self.chunks_owned: Set[int] = set()
        self.output_file: Optional[str] = None
        
        self.api_key = os.getenv("TRACKER_API_KEY", "default-secret-key")
        self.http_headers = {"X-API-Key": self.api_key}
        
        self.server = None
        self.is_running = False
        
        self.active_peers: List[Dict] = []
        self.download_tasks: Dict[int, asyncio.Task] = {}
        self.MAX_CONCURRENT_DOWNLOADS = 5
        
    def init_from_metadata(self, metadata_path: str, output_path: str, is_seeder: bool = False):
        """Initialize peer for a specific file based on metadata."""
        self.metadata = TorrentMetadata.load(metadata_path)
        self.file_id = self.metadata["file_info"]["file_hash"]
        self.tracker_url = self.metadata["tracker_url"]
        self.output_file = output_path
        
        chunk_count = self.metadata["file_info"]["chunk_count"]
        
        if is_seeder:
            # We have all chunks
            self.chunks_owned = set(range(chunk_count))
        else:
            if not os.path.exists(output_path):
                # Initialize empty file
                with open(output_path, "wb") as f:
                    f.truncate(self.metadata["file_info"]["size"])
            else:
                # Resume download: check which chunks are already downloaded and verified
                logger.info("Found existing output file. Verifying existing chunks to resume download...")
                for i in range(chunk_count):
                    try:
                        chunk_data = self.read_chunk(i)
                        if len(chunk_data) > 0 and self.verify_chunk(i, chunk_data):
                            self.chunks_owned.add(i)
                    except Exception as e:
                        pass
                logger.info(f"Resumed with {len(self.chunks_owned)}/{chunk_count} chunks already downloaded.")
                    
    def read_chunk(self, chunk_index: int) -> bytes:
        """Reads a specific chunk from the local file."""
        chunk_size = self.metadata["file_info"]["chunk_size"]
        offset = chunk_index * chunk_size
        
        with open(self.output_file, "rb") as f:
            f.seek(offset)
            return f.read(chunk_size)
            
    def write_chunk(self, chunk_index: int, data: bytes):
        """Writes a chunk to the local file."""
        chunk_size = self.metadata["file_info"]["chunk_size"]
        offset = chunk_index * chunk_size
        
        with open(self.output_file, "r+b") as f:
            f.seek(offset)
            f.write(data)
            
    def verify_chunk(self, chunk_index: int, data: bytes) -> bool:
        """Verifies a chunk's hash against metadata."""
        expected_hash = self.metadata["file_info"]["chunk_hashes"][chunk_index]
        actual_hash = hashlib.sha256(data).hexdigest()
        return expected_hash == actual_hash

    async def start_server(self):
        """Starts the gRPC server."""
        self.server = grpc.aio.server()
        peer_pb2_grpc.add_PeerServiceServicer_to_server(PeerServicer(self), self.server)
        listen_addr = f"{self.host}:{self.port}"
        self.server.add_insecure_port(listen_addr)
        await self.server.start()
        logger.info(f"Peer server listening on {listen_addr}")
        
    async def stop_server(self):
        if self.server:
            await self.server.stop(0)
            
    async def tracker_announce_loop(self):
        """Periodically announces status to tracker."""
        async with httpx.AsyncClient(headers=self.http_headers) as client:
            while self.is_running:
                try:
                    payload = {
                        "peer_id": self.peer_id,
                        "file_id": self.file_id,
                        "host": self.host,
                        "port": self.port,
                        "chunks_owned": list(self.chunks_owned)
                    }
                    await client.post(f"{self.tracker_url}/announce", json=payload)
                    logger.debug("Announced to tracker")
                except Exception as e:
                    logger.error(f"Tracker announce failed: {e}")
                
                await asyncio.sleep(10) # Announce every 10 seconds
                
    async def fetch_peers_loop(self):
        """Periodically fetches active peers from tracker."""
        async with httpx.AsyncClient(headers=self.http_headers) as client:
            while self.is_running:
                try:
                    response = await client.get(f"{self.tracker_url}/peers/{self.file_id}?exclude_peer={self.peer_id}")
                    if response.status_code == 200:
                        self.active_peers = response.json()["peers"]
                        logger.debug(f"Found {len(self.active_peers)} active peers")
                except Exception as e:
                    logger.error(f"Failed to fetch peers: {e}")
                    
                await asyncio.sleep(5)
                
    async def download_chunk(self, chunk_index: int, peer_info: Dict):
        """Attempts to download a chunk from a specific peer."""
        target = f"{peer_info['host']}:{peer_info['port']}"
        logger.info(f"Requesting chunk {chunk_index} from {target}")
        
        try:
            async with grpc.aio.insecure_channel(target) as channel:
                stub = peer_pb2_grpc.PeerServiceStub(channel)
                req = peer_pb2.ChunkRequest(file_id=self.file_id, chunk_index=chunk_index)
                
                # Timeout is crucial for fault tolerance
                response = await stub.GetChunk(req, timeout=5)
                
                if response.success:
                    if self.verify_chunk(chunk_index, response.data):
                        self.write_chunk(chunk_index, response.data)
                        self.chunks_owned.add(chunk_index)
                        logger.info(f"Successfully downloaded and verified chunk {chunk_index}")
                        return True
                    else:
                        logger.error(f"Chunk {chunk_index} from {target} failed hash verification!")
                        return False
                else:
                    logger.warning(f"Peer {target} failed to provide chunk {chunk_index}: {response.error_message}")
                    return False
        except Exception as e:
            logger.error(f"Connection error to {target} for chunk {chunk_index}: {e}")
            return False

    async def download_manager_loop(self):
        """Manages the download of missing chunks."""
        total_chunks = self.metadata["file_info"]["chunk_count"]
        
        while self.is_running:
            if len(self.chunks_owned) == total_chunks:
                logger.info("Download complete! Checking final file hash...")
                self.verify_entire_file()
                break
                
            # Find missing chunks
            missing_chunks = set(range(total_chunks)) - self.chunks_owned
            
            # Remove chunks currently being downloaded
            chunks_to_request = missing_chunks - set(self.download_tasks.keys())
            
            # Rarest-first chunk selection
            chunk_frequency = {chunk_idx: 0 for chunk_idx in chunks_to_request}
            for p in self.active_peers:
                for chunk_idx in p["chunks"]:
                    if chunk_idx in chunk_frequency:
                        chunk_frequency[chunk_idx] += 1
            
            # Sort chunks by frequency (ascending) -> rarest first
            chunks_to_request = sorted(chunks_to_request, key=lambda x: chunk_frequency[x])
            
            for chunk_idx in chunks_to_request:
                # Concurrency limit check
                if len(self.download_tasks) >= self.MAX_CONCURRENT_DOWNLOADS:
                    break
                    
                # Find a peer that has this chunk
                available_peers = [p for p in self.active_peers if chunk_idx in p["chunks"]]
                
                if available_peers:
                    # Pick a random peer to distribute load and handle failures better
                    peer = random.choice(available_peers)
                    task = asyncio.create_task(self.download_chunk(chunk_idx, peer))
                    self.download_tasks[chunk_idx] = task
                    
                    # Add a callback to remove from tasks when done
                    task.add_done_callback(lambda t, idx=chunk_idx: self.download_tasks.pop(idx, None))
                
            await asyncio.sleep(1)
            
    def verify_entire_file(self):
        """Verifies the final assembled file against the original hash."""
        expected_hash = self.metadata["file_info"]["file_hash"]
        actual_hash_obj = hashlib.sha256()
        
        with open(self.output_file, "rb") as f:
            while True:
                chunk = f.read(65536)
                if not chunk:
                    break
                actual_hash_obj.update(chunk)
                
        actual_hash = actual_hash_obj.hexdigest()
        if actual_hash == expected_hash:
            logger.info("SUCCESS: Final file hash matches metadata perfectly!")
        else:
            logger.error("FAILURE: Final file hash mismatch!")

    async def register_file_with_tracker(self):
        """Seeders register the file with the tracker initially."""
        async with httpx.AsyncClient(headers=self.http_headers) as client:
            payload = {
                "file_id": self.file_id,
                "name": self.metadata["file_info"]["name"],
                "size": self.metadata["file_info"]["size"],
                "chunk_count": self.metadata["file_info"]["chunk_count"]
            }
            try:
                await client.post(f"{self.tracker_url}/register", json=payload)
                logger.info("Registered file with tracker successfully.")
            except Exception as e:
                logger.error(f"Failed to register file: {e}")

    async def run(self):
        self.is_running = True
        await self.start_server()
        
        # Start background tasks
        announce_task = asyncio.create_task(self.tracker_announce_loop())
        fetch_task = asyncio.create_task(self.fetch_peers_loop())
        dl_task = asyncio.create_task(self.download_manager_loop())
        
        try:
            # Keep running until interrupted
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            self.is_running = False
            await self.stop_server()
            announce_task.cancel()
            fetch_task.cancel()
            dl_task.cancel()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run a Peer Node")
    parser.add_argument("--host", default="127.0.0.1", help="Host address to bind to")
    parser.add_argument("--port", type=int, required=True, help="Port to bind to")
    parser.add_argument("--tracker", default="http://127.0.0.1:8000", help="Tracker URL")
    parser.add_argument("--metadata", required=True, help="Path to .torrent.json file")
    parser.add_argument("--output", required=True, help="Path to save/read file")
    parser.add_argument("--seed", action="store_true", help="Run as seeder (initial owner)")
    
    args = parser.parse_args()
    
    node = PeerNode(args.host, args.port, args.tracker)
    node.init_from_metadata(args.metadata, args.output, is_seeder=args.seed)
    
    if args.seed:
        asyncio.run(node.register_file_with_tracker())
        
    try:
        asyncio.run(node.run())
    except KeyboardInterrupt:
        print("Peer node stopped.")
