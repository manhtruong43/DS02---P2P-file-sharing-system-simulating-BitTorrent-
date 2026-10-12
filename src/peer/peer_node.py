import asyncio
import logging
import random
import time
from typing import Dict, List, Set, Optional

import grpc
import httpx

try:
    from src.common import peer_pb2, peer_pb2_grpc
    from src.common.config import (
        DEFAULT_PEER_HOST,
        DEFAULT_TRACKER_URL,
        TRACKER_API_KEY,
        MAX_CONCURRENT_DOWNLOADS,
        GRPC_TIMEOUT,
        FETCH_PEERS_INTERVAL,
        HEARTBEAT_INTERVAL
    )
    from src.common.metadata import TorrentMetadata
    from src.peer.chunk_manager import ChunkManager
    from src.peer.peer_servicer import PeerServicer
except ImportError:
    try:
        from common import peer_pb2, peer_pb2_grpc
        from common.config import (
            DEFAULT_PEER_HOST,
            DEFAULT_TRACKER_URL,
            TRACKER_API_KEY,
            MAX_CONCURRENT_DOWNLOADS,
            GRPC_TIMEOUT,
            FETCH_PEERS_INTERVAL,
            HEARTBEAT_INTERVAL
        )
        from common.metadata import TorrentMetadata
        from peer.chunk_manager import ChunkManager
        from peer.peer_servicer import PeerServicer
    except ImportError:
        import peer_pb2
        import peer_pb2_grpc
        from metadata import TorrentMetadata
        from chunk_manager import ChunkManager
        from peer_servicer import PeerServicer
        DEFAULT_PEER_HOST = "127.0.0.1"
        DEFAULT_TRACKER_URL = "http://127.0.0.1:8000"
        TRACKER_API_KEY = "ds02_secret_key_123"
        MAX_CONCURRENT_DOWNLOADS = 5
        GRPC_TIMEOUT = 5
        FETCH_PEERS_INTERVAL = 4
        HEARTBEAT_INTERVAL = 10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] [%(levelname)s] %(message)s"
)
logger = logging.getLogger("peer_node")

class PeerNode:
    """
    DS02 BitTorrent Peer Node:
    - Acts as both seeder (server) and leecher (client).
    - Downloads chunks concurrently from multiple peers using rarest-first.
    - Employs immediate HAVE/announce upon chunk completion for rapid peer sharing.
    - Handles network faults, peer churn, corrupt data rejection, and graceful shutdown.
    """
    def __init__(self, host: str = DEFAULT_PEER_HOST, port: int = 5001, tracker_url: str = DEFAULT_TRACKER_URL):
        self.host = host
        self.port = port
        self.peer_id = f"{host}:{port}"
        self.tracker_url = tracker_url

        self.file_id: Optional[str] = None
        self.metadata: Optional[Dict] = None
        self.chunk_mgr: Optional[ChunkManager] = None
        self.output_file: Optional[str] = None

        self.api_key = TRACKER_API_KEY
        self.http_headers = {"X-API-Key": self.api_key}

        self.server: Optional[grpc.aio.Server] = None
        self.is_running = False

        self.active_peers: List[Dict] = []
        self.bad_peers: Dict[str, int] = {} # target -> failure_count
        self.download_tasks: Dict[int, asyncio.Task] = {}
        self.max_concurrent_downloads = MAX_CONCURRENT_DOWNLOADS
        self.download_completed_event = asyncio.Event()

    def init_from_metadata(self, metadata_path: str, output_path: str, is_seeder: bool = False):
        """Loads metadata and initializes piece storage."""
        self.metadata = TorrentMetadata.load(metadata_path)
        self.file_id = self.metadata["file_id"]
        
        # Select tracker from metadata if available
        if "trackers" in self.metadata and self.metadata["trackers"]:
            self.tracker_url = self.metadata["trackers"][0]
        elif "tracker_url" in self.metadata:
            self.tracker_url = self.metadata["tracker_url"]

        self.output_file = output_path
        file_info = self.metadata["file_info"]

        self.chunk_mgr = ChunkManager(
            file_path=output_path,
            file_size=file_info["size"],
            chunk_size=file_info["chunk_size"],
            chunk_hashes=file_info["chunk_hashes"],
            expected_file_hash=file_info["file_hash"]
        )
        self.chunk_mgr.initialize_storage(is_seeder=is_seeder)
        logger.info(f"Initialized PeerNode [{self.peer_id}] | File: {file_info['name']} | Chunks: {len(self.chunk_mgr.chunks_owned)}/{self.chunk_mgr.total_chunks}")

    async def start_server(self):
        """Starts the gRPC peer server."""
        self.server = grpc.aio.server()
        peer_pb2_grpc.add_PeerServiceServicer_to_server(PeerServicer(self), self.server)
        listen_addr = f"{self.host}:{self.port}"
        self.server.add_insecure_port(listen_addr)
        await self.server.start()
        logger.info(f"gRPC Peer Server listening on {listen_addr}")

    async def stop_server(self):
        """Stops the gRPC server."""
        if self.server:
            logger.info("Stopping gRPC Peer Server...")
            await self.server.stop(grace=1.0)

    async def register_file_with_tracker(self):
        """Called by seeder to register file metadata in the tracker."""
        async with httpx.AsyncClient(headers=self.http_headers, timeout=5.0) as client:
            payload = {
                "file_id": self.file_id,
                "name": self.metadata["file_info"]["name"],
                "size": self.metadata["file_info"]["size"],
                "chunk_count": self.chunk_mgr.total_chunks
            }
            try:
                res = await client.post(f"{self.tracker_url}/register", json=payload)
                if res.status_code == 200:
                    logger.info("Registered file with tracker successfully.")
                else:
                    logger.error(f"Tracker register returned status {res.status_code}: {res.text}")
            except Exception as e:
                logger.error(f"Failed to register file with tracker: {e}")

    async def announce_to_tracker(self):
        """Sends current bitfield to tracker."""
        async with httpx.AsyncClient(headers=self.http_headers, timeout=4.0) as client:
            payload = {
                "peer_id": self.peer_id,
                "file_id": self.file_id,
                "host": self.host,
                "port": self.port,
                "chunks_owned": list(self.chunk_mgr.chunks_owned)
            }
            try:
                res = await client.post(f"{self.tracker_url}/announce", json=payload)
                if res.status_code == 200:
                    logger.debug(f"Announce successful. Chunks reported: {len(self.chunk_mgr.chunks_owned)}")
            except Exception as e:
                logger.warning(f"Tracker announce failed: {e}")

    async def announce_chunk_immediately(self, chunk_idx: int):
        """
        Immediately updates tracker bitfield as soon as a chunk completes.
        Allows other peers to start downloading this chunk without waiting for 10s timer.
        """
        try:
            await self.announce_to_tracker()
            logger.info(f"[IMMEDIATE ANNOUNCE] Announced chunk {chunk_idx} availability to network.")
        except Exception as e:
            logger.debug(f"Immediate announce failed: {e}")

    async def leave_tracker(self):
        """Gracefully removes this peer from tracker registry."""
        try:
            async with httpx.AsyncClient(headers=self.http_headers, timeout=3.0) as client:
                payload = {"peer_id": self.peer_id, "file_id": self.file_id}
                await client.post(f"{self.tracker_url}/leave", json=payload)
                logger.info(f"Notified tracker of graceful leave for peer {self.peer_id}")
        except Exception as e:
            logger.debug(f"Graceful leave notice failed: {e}")

    async def tracker_announce_loop(self):
        """Periodic background announce loop."""
        while self.is_running:
            await self.announce_to_tracker()
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def fetch_peers_loop(self):
        """Periodically queries tracker for active peers."""
        async with httpx.AsyncClient(headers=self.http_headers, timeout=4.0) as client:
            while self.is_running:
                try:
                    res = await client.get(f"{self.tracker_url}/peers/{self.file_id}?exclude_peer={self.peer_id}")
                    if res.status_code == 200:
                        peers_data = res.json().get("peers", [])
                        # Filter out permanently dead/blacklisted peers
                        self.active_peers = [p for p in peers_data if self.bad_peers.get(f"{p['host']}:{p['port']}", 0) < 5]
                        logger.debug(f"Active peers in network: {len(self.active_peers)}")
                except Exception as e:
                    logger.warning(f"Failed to fetch peers from tracker: {e}")
                await asyncio.sleep(FETCH_PEERS_INTERVAL)

    async def download_chunk_from_peer(self, chunk_index: int, peer_info: Dict) -> bool:
        """
        Requests, downloads, validates, and atomically writes a chunk from target peer.
        Enforces timeout, integrity check, and penalizes failing peers.
        """
        target = f"{peer_info['host']}:{peer_info['port']}"
        logger.info(f"Downloading chunk {chunk_index} from peer {target}...")

        try:
            async with grpc.aio.insecure_channel(target) as channel:
                stub = peer_pb2_grpc.PeerServiceStub(channel)
                req = peer_pb2.ChunkRequest(file_id=self.file_id, chunk_index=chunk_index)
                
                # Enforce RPC timeout for fault tolerance
                response = await stub.GetChunk(req, timeout=GRPC_TIMEOUT)

                if response.success:
                    # Write atomically and verify hash
                    write_ok = await self.chunk_mgr.write_chunk_atomic(chunk_index, response.data)
                    if write_ok:
                        logger.info(f"Chunk {chunk_index} verified successfully from {target}")
                        # Immediately announce chunk so other peers can download it
                        asyncio.create_task(self.announce_chunk_immediately(chunk_index))
                        # Reset target penalty on success
                        self.bad_peers[target] = 0
                        return True
                    else:
                        logger.error(f"[INTEGRITY FAULT] Chunk {chunk_index} from {target} failed hash verification! Penalizing peer.")
                        self.bad_peers[target] = self.bad_peers.get(target, 0) + 2
                        self.chunk_mgr.mark_missing(chunk_index)
                        return False
                else:
                    logger.warning(f"Peer {target} rejected chunk {chunk_index}: {response.error_message}")
                    self.bad_peers[target] = self.bad_peers.get(target, 0) + 1
                    self.chunk_mgr.mark_missing(chunk_index)
                    return False
        except Exception as e:
            logger.warning(f"[NETWORK FAULT] Connection error to {target} for chunk {chunk_index}: {e}")
            self.bad_peers[target] = self.bad_peers.get(target, 0) + 1
            self.chunk_mgr.mark_missing(chunk_index)
            return False

    async def download_manager_loop(self):
        """
        Core BitTorrent download manager:
        - Implements Rarest-First chunk selection.
        - Enforces concurrency limit (MAX_CONCURRENT_DOWNLOADS).
        - Automatically retries missing chunks from alternate peers.
        - Verifies complete file hash upon download completion.
        """
        while self.is_running:
            if self.chunk_mgr.is_complete():
                logger.info("All chunks downloaded! Verifying complete file hash...")
                if self.chunk_mgr.verify_final_file():
                    logger.info("VERIFICATION SUCCESS: Final file hash matches metadata perfectly!")
                else:
                    logger.error("VERIFICATION FAILURE: Final file hash mismatch!")
                self.download_completed_event.set()
                break

            # Find missing chunks that are not currently in-flight
            missing_chunks = [
                i for i in range(self.chunk_mgr.total_chunks)
                if i not in self.chunk_mgr.chunks_owned and i not in self.download_tasks
            ]

            if not missing_chunks:
                await asyncio.sleep(0.5)
                continue

            # Calculate chunk rarity (rarest-first selection)
            chunk_frequency = {c: 0 for c in missing_chunks}
            for p in self.active_peers:
                p_chunks = set(p.get("chunks", []))
                for c in missing_chunks:
                    if c in p_chunks:
                        chunk_frequency[c] += 1

            # Sort ascending: lowest availability downloaded first
            sorted_chunks = sorted(missing_chunks, key=lambda c: chunk_frequency[c])

            for chunk_idx in sorted_chunks:
                if len(self.download_tasks) >= self.max_concurrent_downloads:
                    break

                # Find candidate peers who have this chunk
                candidates = [
                    p for p in self.active_peers
                    if chunk_idx in p.get("chunks", []) and self.bad_peers.get(f"{p['host']}:{p['port']}", 0) < 5
                ]

                if candidates:
                    # Pick peer with lowest bad penalty or random among best
                    chosen_peer = min(candidates, key=lambda p: self.bad_peers.get(f"{p['host']}:{p['port']}", 0))
                    
                    self.chunk_mgr.mark_downloading(chunk_idx)
                    
                    task = asyncio.create_task(self.download_chunk_from_peer(chunk_idx, chosen_peer))
                    self.download_tasks[chunk_idx] = task
                    task.add_done_callback(lambda t, idx=chunk_idx: self.download_tasks.pop(idx, None))

            await asyncio.sleep(0.5)

    async def run(self):
        """Starts peer server, announces, and begins downloading."""
        self.is_running = True
        await self.start_server()

        # Initial announce
        await self.announce_to_tracker()

        announce_task = asyncio.create_task(self.tracker_announce_loop())
        fetch_task = asyncio.create_task(self.fetch_peers_loop())
        dl_task = asyncio.create_task(self.download_manager_loop())

        try:
            while self.is_running:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            pass
        finally:
            await self.shutdown(announce_task, fetch_task, dl_task)

    async def shutdown(self, *tasks):
        """Graceful shutdown cleanup."""
        self.is_running = False
        for t in tasks:
            if t and not t.done():
                t.cancel()
        await self.leave_tracker()
        await self.stop_server()
        logger.info(f"PeerNode [{self.peer_id}] successfully stopped.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run DS02 BitTorrent Peer Node")
    parser.add_argument("--host", default=DEFAULT_PEER_HOST, help="Host address to bind")
    parser.add_argument("--port", type=int, required=True, help="Port to bind")
    parser.add_argument("--tracker", default=DEFAULT_TRACKER_URL, help="Tracker URL")
    parser.add_argument("--metadata", required=True, help="Path to .torrent.json file")
    parser.add_argument("--output", required=True, help="Path to output file")
    parser.add_argument("--seed", action="store_true", help="Run as seeder (initial owner)")

    args = parser.parse_args()

    node = PeerNode(host=args.host, port=args.port, tracker_url=args.tracker)
    node.init_from_metadata(metadata_path=args.metadata, output_path=args.output, is_seeder=args.seed)

    if args.seed:
        asyncio.run(node.register_file_with_tracker())

    try:
        asyncio.run(node.run())
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt, stopping peer node...")
