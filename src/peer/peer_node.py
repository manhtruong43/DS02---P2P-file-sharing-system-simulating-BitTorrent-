import asyncio
import logging
import random
import time
from collections import Counter
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
        MAX_RETRY_PER_CHUNK,
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
            MAX_RETRY_PER_CHUNK,
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
        TRACKER_API_KEY = ""
        MAX_CONCURRENT_DOWNLOADS = 5
        MAX_RETRY_PER_CHUNK = 3
        GRPC_TIMEOUT = 5
        FETCH_PEERS_INTERVAL = 4
        HEARTBEAT_INTERVAL = 10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] [%(levelname)s] %(message)s"
)
# Silence per-request logs of the HTTP client library; tracker calls are logged by this module
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("peer_node")

# A peer whose penalty score reaches this value is no longer chosen as a source
BAD_PEER_THRESHOLD = 5
# Exponential backoff between retries of the same chunk: base * 2^(attempt-1), capped
RETRY_BACKOFF_BASE = 0.2
RETRY_BACKOFF_MAX = 2.0
# After MAX_RETRY_PER_CHUNK failed attempts, wait this long before trying the chunk again from any peer
RETRY_COOLDOWN = 5.0


def peer_target(peer_info: Dict) -> str:
    return f"{peer_info['host']}:{peer_info['port']}"


class PeerNode:
    """
    DS02 BitTorrent Peer Node:
    - Acts as both seeder (server) and leecher (client).
    - Downloads chunks concurrently from multiple peers using rarest-first selection,
      spreading requests over the least-loaded sources.
    - Bounded retry with exponential backoff; failed chunks are re-requested from a different peer (failover).
    - Employs immediate HAVE/announce upon chunk completion for rapid peer sharing.
    - Handles network faults, peer churn, corrupt data rejection, and graceful shutdown.
    """
    def __init__(
        self,
        host: str = DEFAULT_PEER_HOST,
        port: int = 5001,
        tracker_url: str = DEFAULT_TRACKER_URL,
        corrupt: bool = False,
        upload_limit_kbps: float = 0
    ):
        self.host = host
        self.port = port
        self.peer_id = f"{host}:{port}"
        self.tracker_url = tracker_url
        # Every log line of this node carries its peer_id
        self.log = logging.getLogger(f"peer[{self.peer_id}]")

        self.file_id: Optional[str] = None
        self.metadata: Optional[Dict] = None
        self.chunk_mgr: Optional[ChunkManager] = None
        self.output_file: Optional[str] = None

        self.api_key = TRACKER_API_KEY
        self.http_headers = {"X-API-Key": self.api_key}
        # One shared HTTP session for all tracker calls (created in run()).
        # Creating a new client per request costs ~60-200 ms of CPU and blocks the event loop.
        self.http: Optional[httpx.AsyncClient] = None

        self.server: Optional[grpc.aio.Server] = None
        self.is_running = False

        self.active_peers: List[Dict] = []
        self.known_peer_targets: Set[str] = set()
        self.bad_peers: Dict[str, int] = {}  # target -> penalty score
        self.download_tasks: Dict[int, asyncio.Task] = {}
        self.max_concurrent_downloads = MAX_CONCURRENT_DOWNLOADS
        self.max_retry_per_chunk = MAX_RETRY_PER_CHUNK
        self.download_completed_event = asyncio.Event()

        # Download bookkeeping for load balancing, retry and failover
        self.inflight_per_peer: Counter = Counter()        # target -> chunks currently being fetched from it
        self.chunk_attempts: Dict[int, int] = {}           # chunk -> failed attempts since last success/cooldown
        self.chunk_failed_peers: Dict[int, Set[str]] = {}  # chunk -> peers that already failed for this chunk
        self.chunk_retry_at: Dict[int, float] = {}         # chunk -> monotonic time when it may be retried
        self.chunk_source: Dict[int, str] = {}             # chunk -> peer it was finally received from

        # Fault injection for demos: serve random bytes instead of real chunk data
        self.corrupt = corrupt
        # Upload bandwidth limit in bytes/s (0 = unlimited)
        self.upload_limit_bps = upload_limit_kbps * 1024

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
            expected_file_hash=file_info["file_hash"],
            node_label=self.peer_id
        )
        self.chunk_mgr.initialize_storage(is_seeder=is_seeder)
        role = "SEEDER" if is_seeder else "LEECHER"
        if self.corrupt:
            role += " (FAULT INJECTION: serves corrupted chunks)"
        if self.upload_limit_bps > 0:
            role += f" | upload limit {self.upload_limit_bps / 1024:g} KB/s"
        self.log.info(
            f"Initialized as {role} | File: {file_info['name']} | "
            f"Chunks: {len(self.chunk_mgr.chunks_owned)}/{self.chunk_mgr.total_chunks}"
        )

    # ------------------------------------------------------------------
    # gRPC server
    # ------------------------------------------------------------------
    async def start_server(self):
        """Starts the gRPC peer server."""
        self.server = grpc.aio.server()
        peer_pb2_grpc.add_PeerServiceServicer_to_server(PeerServicer(self), self.server)
        listen_addr = f"{self.host}:{self.port}"
        self.server.add_insecure_port(listen_addr)
        await self.server.start()
        self.log.info(f"gRPC Peer Server listening on {listen_addr}")

    async def stop_server(self):
        """Stops the gRPC server."""
        if self.server:
            self.log.info("Stopping gRPC Peer Server...")
            await self.server.stop(grace=1.0)

    # ------------------------------------------------------------------
    # Tracker communication
    # ------------------------------------------------------------------
    async def register_file_with_tracker(self):
        """Called by seeder to register file metadata in the tracker (one-off call, own HTTP session)."""
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
                    self.log.info(f"[REGISTER] File {self.file_id[:8]}.. registered with tracker {self.tracker_url}")
                else:
                    self.log.error(f"[REGISTER] Tracker returned status {res.status_code}: {res.text}")
            except Exception as e:
                self.log.error(f"[REGISTER] Failed to register file with tracker: {e}")

    async def announce_to_tracker(self) -> bool:
        """Sends current bitfield to tracker using the shared HTTP session."""
        if self.http is None:
            # Node is not running (e.g. used directly in tests/benchmarks without a tracker)
            return False
        payload = {
            "peer_id": self.peer_id,
            "file_id": self.file_id,
            "host": self.host,
            "port": self.port,
            "chunks_owned": sorted(self.chunk_mgr.chunks_owned)
        }
        try:
            res = await self.http.post(f"{self.tracker_url}/announce", json=payload)
            if res.status_code == 200:
                self.log.debug(f"Announce successful. Chunks reported: {len(self.chunk_mgr.chunks_owned)}")
                return True
            self.log.warning(f"[ANNOUNCE] Tracker returned status {res.status_code}: {res.text}")
        except Exception as e:
            self.log.warning(f"[ANNOUNCE] Tracker announce failed: {e}")
        return False

    async def announce_chunk_immediately(self, chunk_idx: int):
        """
        Immediately updates tracker bitfield as soon as a chunk completes.
        Allows other peers to start downloading this chunk without waiting for the periodic timer.
        """
        if await self.announce_to_tracker():
            self.log.info(f"[HAVE] Announced chunk {chunk_idx} to tracker - other peers can now download it from me")

    async def leave_tracker(self):
        """Gracefully removes this peer from tracker registry."""
        if self.http is None:
            return
        try:
            payload = {"peer_id": self.peer_id, "file_id": self.file_id}
            await self.http.post(f"{self.tracker_url}/leave", json=payload, timeout=3.0)
            self.log.info(f"[LEAVE] Notified tracker of graceful leave")
        except Exception as e:
            self.log.debug(f"Graceful leave notice failed: {e}")

    async def tracker_announce_loop(self):
        """Periodic background announce loop (also acts as heartbeat)."""
        while self.is_running:
            await self.announce_to_tracker()
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def fetch_peers_loop(self):
        """Periodically queries tracker for active peers and logs peers joining / leaving the swarm."""
        while self.is_running:
            try:
                res = await self.http.get(
                    f"{self.tracker_url}/peers/{self.file_id}",
                    params={"exclude_peer": self.peer_id}
                )
                if res.status_code == 200:
                    peers_data = res.json().get("peers", [])
                    current = {peer_target(p) for p in peers_data}
                    for t in sorted(current - self.known_peer_targets):
                        self.log.info(f"[DISCOVERY] Peer {t} joined the swarm")
                    for t in sorted(self.known_peer_targets - current):
                        self.log.info(f"[CHURN] Peer {t} left the swarm (removed from tracker peer list)")
                    self.known_peer_targets = current
                    self.active_peers = peers_data
                else:
                    self.log.warning(f"[PEERS] Tracker returned status {res.status_code}")
            except Exception as e:
                self.log.warning(f"[PEERS] Failed to fetch peers from tracker: {e}")
            await asyncio.sleep(FETCH_PEERS_INTERVAL)

    # ------------------------------------------------------------------
    # Chunk download
    # ------------------------------------------------------------------
    async def download_chunk_from_peer(self, chunk_index: int, peer_info: Dict) -> bool:
        """
        Requests, downloads, validates, and atomically writes a chunk from target peer.
        Enforces timeout, integrity check, and penalizes failing peers.
        """
        target = peer_target(peer_info)
        self.log.info(f"[REQUEST] Chunk {chunk_index} from peer {target}")

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
                        self.log.info(f"[RECEIVED] Chunk {chunk_index} verified from {target}")
                        # Immediately announce chunk so other peers can download it
                        asyncio.create_task(self.announce_chunk_immediately(chunk_index))
                        # Reset target penalty on success
                        self.bad_peers[target] = 0
                        return True
                    else:
                        self.bad_peers[target] = self.bad_peers.get(target, 0) + 2
                        self.log.error(
                            f"[INTEGRITY FAULT] Chunk {chunk_index} from {target} failed SHA-256 verification! "
                            f"Penalty for {target} = {self.bad_peers[target]}"
                        )
                        self.chunk_mgr.mark_missing(chunk_index)
                        return False
                else:
                    self.bad_peers[target] = self.bad_peers.get(target, 0) + 1
                    self.log.warning(f"[REJECTED] Peer {target} refused chunk {chunk_index}: {response.error_message}")
                    self.chunk_mgr.mark_missing(chunk_index)
                    return False
        except grpc.aio.AioRpcError as e:
            self.bad_peers[target] = self.bad_peers.get(target, 0) + 1
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                self.log.warning(
                    f"[PEER DOWN] Cannot reach {target} for chunk {chunk_index} (connection refused/reset). "
                    f"Penalty for {target} = {self.bad_peers[target]}"
                )
            elif e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                self.log.warning(
                    f"[TIMEOUT] {target} did not answer chunk {chunk_index} within {GRPC_TIMEOUT}s. "
                    f"Penalty for {target} = {self.bad_peers[target]}"
                )
            else:
                self.log.warning(f"[NETWORK FAULT] {target} chunk {chunk_index}: {e.code().name} {e.details()}")
            self.chunk_mgr.mark_missing(chunk_index)
            return False
        except Exception as e:
            self.bad_peers[target] = self.bad_peers.get(target, 0) + 1
            self.log.warning(f"[NETWORK FAULT] Connection error to {target} for chunk {chunk_index}: {e}")
            self.chunk_mgr.mark_missing(chunk_index)
            return False

    def _choose_peer(self, chunk_idx: int) -> Optional[Dict]:
        """
        Chooses the source peer for a chunk:
        - only peers that own the chunk and are not blacklisted (penalty < BAD_PEER_THRESHOLD);
        - prefer peers that have NOT already failed for this chunk (failover to another source);
        - among them pick the least loaded peer (fewest in-flight requests), then lowest penalty,
          random tie-break, so requests are spread over all available sources.
        """
        failed = self.chunk_failed_peers.get(chunk_idx, set())
        candidates = [
            p for p in self.active_peers
            if chunk_idx in p.get("chunks", [])
            and self.bad_peers.get(peer_target(p), 0) < BAD_PEER_THRESHOLD
        ]
        if not candidates:
            return None
        fresh = [p for p in candidates if peer_target(p) not in failed]
        pool = fresh if fresh else candidates
        return min(
            pool,
            key=lambda p: (
                self.inflight_per_peer[peer_target(p)],
                self.bad_peers.get(peer_target(p), 0),
                random.random()
            )
        )

    async def _download_task(self, chunk_idx: int, peer_info: Dict) -> None:
        """Wraps one chunk download with load accounting, bounded retry, backoff and failover bookkeeping."""
        target = peer_target(peer_info)
        self.inflight_per_peer[target] += 1
        try:
            ok = await self.download_chunk_from_peer(chunk_idx, peer_info)
        finally:
            self.inflight_per_peer[target] -= 1

        if ok:
            self.chunk_source[chunk_idx] = target
            self.chunk_attempts.pop(chunk_idx, None)
            self.chunk_failed_peers.pop(chunk_idx, None)
            self.chunk_retry_at.pop(chunk_idx, None)
            return

        attempts = self.chunk_attempts.get(chunk_idx, 0) + 1
        self.chunk_attempts[chunk_idx] = attempts
        self.chunk_failed_peers.setdefault(chunk_idx, set()).add(target)

        if attempts >= self.max_retry_per_chunk:
            # Bounded retry: give the swarm time to change (peers rejoin, tracker refreshes) before trying again
            self.log.warning(
                f"[RETRY] Chunk {chunk_idx} failed {attempts}/{self.max_retry_per_chunk} attempts; "
                f"cooling down {RETRY_COOLDOWN:.0f}s before retrying from any peer"
            )
            self.chunk_attempts[chunk_idx] = 0
            self.chunk_failed_peers[chunk_idx] = set()
            self.chunk_retry_at[chunk_idx] = time.monotonic() + RETRY_COOLDOWN
        else:
            backoff = min(RETRY_BACKOFF_BASE * (2 ** (attempts - 1)), RETRY_BACKOFF_MAX)
            self.log.info(
                f"[RETRY] Chunk {chunk_idx} marked MISSING after failure from {target}; "
                f"retry {attempts}/{self.max_retry_per_chunk} in {backoff:.1f}s, preferring another peer"
            )
            self.chunk_retry_at[chunk_idx] = time.monotonic() + backoff

    def _log_download_summary(self) -> None:
        per_source = Counter(self.chunk_source.values())
        if per_source:
            parts = ", ".join(f"{src}: {n} chunk(s)" for src, n in sorted(per_source.items()))
            self.log.info(f"[SUMMARY] Chunks received per source peer -> {parts}")

    async def download_manager_loop(self):
        """
        Core BitTorrent download manager:
        - Rarest-First chunk selection (random tie-break so peers do not all fetch the same chunk).
        - Enforces concurrency limit (MAX_CONCURRENT_DOWNLOADS).
        - Spreads requests over the least-loaded source peers (parallel multi-source download).
        - Bounded retry with backoff; failed chunks are re-requested from a different peer (failover).
        - Verifies complete file hash upon download completion.
        """
        if self.chunk_mgr.is_complete():
            self.download_completed_event.set()
            return

        start_time = time.monotonic()
        while self.is_running:
            if self.chunk_mgr.is_complete():
                elapsed = time.monotonic() - start_time
                self.log.info(f"All {self.chunk_mgr.total_chunks} chunks downloaded in {elapsed:.2f}s. Verifying complete file hash...")
                self._log_download_summary()
                if self.chunk_mgr.verify_final_file():
                    self.log.info("[VERIFY] SUCCESS: final file SHA-256 matches metadata file_hash. Now seeding.")
                else:
                    self.log.error("[VERIFY] FAILURE: final file hash mismatch!")
                self.download_completed_event.set()
                break

            now = time.monotonic()
            # Missing chunks that are not in-flight and whose backoff has expired
            missing_chunks = [
                i for i in range(self.chunk_mgr.total_chunks)
                if i not in self.chunk_mgr.chunks_owned
                and i not in self.download_tasks
                and self.chunk_retry_at.get(i, 0.0) <= now
            ]

            if not missing_chunks or len(self.download_tasks) >= self.max_concurrent_downloads:
                await asyncio.sleep(0.1)
                continue

            # Rarest-first: count how many active peers own each missing chunk
            peer_chunk_sets = [set(p.get("chunks", [])) for p in self.active_peers]
            chunk_frequency = {c: sum(1 for s in peer_chunk_sets if c in s) for c in missing_chunks}
            random.shuffle(missing_chunks)
            sorted_chunks = sorted(missing_chunks, key=lambda c: chunk_frequency[c])

            for chunk_idx in sorted_chunks:
                if len(self.download_tasks) >= self.max_concurrent_downloads:
                    break
                if chunk_frequency[chunk_idx] == 0:
                    continue

                chosen_peer = self._choose_peer(chunk_idx)
                if chosen_peer is None:
                    continue

                target = peer_target(chosen_peer)
                previous_failures = self.chunk_failed_peers.get(chunk_idx)
                if previous_failures and target not in previous_failures:
                    self.log.info(
                        f"[FAILOVER] Re-requesting chunk {chunk_idx} from {target} "
                        f"(failed before from: {', '.join(sorted(previous_failures))})"
                    )

                self.chunk_mgr.mark_downloading(chunk_idx)
                task = asyncio.create_task(self._download_task(chunk_idx, chosen_peer))
                self.download_tasks[chunk_idx] = task
                task.add_done_callback(lambda t, idx=chunk_idx: self.download_tasks.pop(idx, None))

            await asyncio.sleep(0.05)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def run(self):
        """Starts peer server, announces, and begins downloading."""
        self.is_running = True
        self.http = httpx.AsyncClient(headers=self.http_headers, timeout=4.0)
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
        if self.http is not None:
            await self.http.aclose()
            self.http = None
        self.log.info("PeerNode successfully stopped.")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Run DS02 BitTorrent Peer Node")
    parser.add_argument("--host", default=DEFAULT_PEER_HOST, help="Host address to bind")
    parser.add_argument("--port", type=int, required=True, help="Port to bind")
    parser.add_argument("--tracker", default=DEFAULT_TRACKER_URL, help="Tracker URL")
    parser.add_argument("--metadata", required=True, help="Path to .torrent.json file")
    parser.add_argument("--output", required=True, help="Path to output file")
    parser.add_argument("--seed", action="store_true", help="Run as seeder (initial owner)")
    parser.add_argument("--corrupt", action="store_true",
                        help="FAULT INJECTION (demo only): serve random bytes instead of real chunk data")
    parser.add_argument("--upload-limit", type=float, default=0,
                        help="Upload bandwidth limit in KB/s (0 = unlimited)")
    args = parser.parse_args()

    node = PeerNode(host=args.host, port=args.port, tracker_url=args.tracker,
                    corrupt=args.corrupt, upload_limit_kbps=args.upload_limit)
    node.init_from_metadata(metadata_path=args.metadata, output_path=args.output, is_seeder=args.seed)

    if args.seed:
        asyncio.run(node.register_file_with_tracker())

    try:
        asyncio.run(node.run())
    except KeyboardInterrupt:
        logger.info("Received KeyboardInterrupt, stopping peer node...")


if __name__ == "__main__":
    main()
