import asyncio
import time
import logging
from typing import Dict, List, Set, Optional
from fastapi import FastAPI, HTTPException, Depends, Header, Query
from pydantic import BaseModel, Field
import uvicorn

try:
    from src.common.config import (
        DEFAULT_TRACKER_HOST,
        DEFAULT_TRACKER_PORT,
        TRACKER_API_KEY,
        PEER_TIMEOUT
    )
except ImportError:
    from common.config import (
        DEFAULT_TRACKER_HOST,
        DEFAULT_TRACKER_PORT,
        TRACKER_API_KEY,
        PEER_TIMEOUT
    )

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [TRACKER] [%(levelname)s] %(message)s"
)
logger = logging.getLogger("tracker")

app = FastAPI(
    title="DS02 BitTorrent Tracker Server",
    description="Tracker Server managing metadata, peer lists, and liveness for P2P network",
    version="1.0.0"
)

# Authentication dependency
async def verify_api_key(x_api_key: Optional[str] = Header(None)):
    if not x_api_key or x_api_key != TRACKER_API_KEY:
        logger.warning(f"Unauthorized access attempt with API Key: {x_api_key}")
        raise HTTPException(status_code=401, detail="Invalid or missing X-API-Key")
    return x_api_key

# Request Models
class FileRegistration(BaseModel):
    file_id: str = Field(..., min_length=8, max_length=128)
    name: str = Field(..., min_length=1)
    size: int = Field(..., ge=1)
    chunk_count: int = Field(..., ge=1)

class PeerAnnounce(BaseModel):
    peer_id: str = Field(..., min_length=3)
    file_id: str = Field(..., min_length=8)
    host: str = Field(..., min_length=1)
    port: int = Field(..., ge=1, le=65535)
    chunks_owned: List[int] = Field(default_factory=list)

class PeerHeartbeat(BaseModel):
    peer_id: str
    file_id: str

class PeerLeave(BaseModel):
    peer_id: str
    file_id: str

# In-memory storage with concurrency protection
class TrackerState:
    def __init__(self):
        self.lock = asyncio.Lock()
        # files = { file_id: FileRegistration }
        self.files: Dict[str, FileRegistration] = {}
        # peers = { file_id: { peer_id: { host, port, chunks: set, last_seen } } }
        self.peers: Dict[str, Dict[str, dict]] = {}

state = TrackerState()

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": time.time()}

@app.post("/register")
async def register_file(file_info: FileRegistration, api_key: str = Depends(verify_api_key)):
    """Called by a seeder to register file metadata in the tracker."""
    async with state.lock:
        state.files[file_info.file_id] = file_info
        if file_info.file_id not in state.peers:
            state.peers[file_info.file_id] = {}
    
    logger.info(f"Registered file: {file_info.file_id} | Name: {file_info.name} | Size: {file_info.size}B | Chunks: {file_info.chunk_count}")
    return {"status": "success", "file_id": file_info.file_id}

@app.post("/announce")
async def announce(announce_info: PeerAnnounce, api_key: str = Depends(verify_api_key)):
    """Called by peers periodically or immediately upon chunk completion to announce presence & bitfield."""
    file_id = announce_info.file_id
    peer_id = announce_info.peer_id

    async with state.lock:
        if file_id not in state.files:
            raise HTTPException(status_code=404, detail="File not registered on tracker")

        if file_id not in state.peers:
            state.peers[file_id] = {}

        # Validate chunk indices
        max_chunks = state.files[file_id].chunk_count
        valid_chunks = {c for c in announce_info.chunks_owned if 0 <= c < max_chunks}

        state.peers[file_id][peer_id] = {
            "host": announce_info.host,
            "port": announce_info.port,
            "chunks": valid_chunks,
            "last_seen": time.time()
        }

    logger.debug(f"Announce from peer {peer_id} for file {file_id[:8]}.. (Chunks: {len(valid_chunks)}/{max_chunks})")
    return {"status": "success", "chunks_recorded": len(valid_chunks)}

@app.post("/heartbeat")
async def heartbeat(hb: PeerHeartbeat, api_key: str = Depends(verify_api_key)):
    """Duy trì trạng thái online mà không cần gửi toàn bộ bitfield."""
    async with state.lock:
        if hb.file_id in state.peers and hb.peer_id in state.peers[hb.file_id]:
            state.peers[hb.file_id][hb.peer_id]["last_seen"] = time.time()
            return {"status": "success", "message": "Heartbeat updated"}
    return {"status": "not_found", "message": "Peer not found, please announce"}

@app.post("/leave")
async def peer_leave(leave_info: PeerLeave, api_key: str = Depends(verify_api_key)):
    """Called when a peer shuts down gracefully to immediately remove it from active peer list."""
    async with state.lock:
        if leave_info.file_id in state.peers:
            if leave_info.peer_id in state.peers[leave_info.file_id]:
                del state.peers[leave_info.file_id][leave_info.peer_id]
                logger.info(f"Peer {leave_info.peer_id} gracefully left file {leave_info.file_id[:8]}..")
                return {"status": "success", "message": "Peer removed"}
    return {"status": "not_found", "message": "Peer was not registered"}

@app.get("/peers/{file_id}")
async def get_peers(
    file_id: str,
    exclude_peer: Optional[str] = Query(None),
    api_key: str = Depends(verify_api_key)
):
    """Returns active peers and prunes dead peers exceeding PEER_TIMEOUT."""
    async with state.lock:
        if file_id not in state.files:
            raise HTTPException(status_code=404, detail="File not registered on tracker")

        now = time.time()
        active_peers = []
        peers_map = state.peers.get(file_id, {})

        # Prune expired peers
        dead_peers = [p_id for p_id, p_info in peers_map.items() if (now - p_info["last_seen"]) > PEER_TIMEOUT]
        for p_id in dead_peers:
            logger.info(f"Pruned dead peer {p_id} from file {file_id[:8]}.. (TTL exceeded: {now - peers_map[p_id]['last_seen']:.1f}s)")
            del peers_map[p_id]

        for p_id, p_info in peers_map.items():
            if exclude_peer and p_id == exclude_peer:
                continue
            active_peers.append({
                "peer_id": p_id,
                "host": p_info["host"],
                "port": p_info["port"],
                "chunks": sorted(list(p_info["chunks"])),
                "last_seen": round(now - p_info["last_seen"], 2)
            })

    return {
        "file_id": file_id,
        "total_active_peers": len(active_peers),
        "peers": active_peers
    }

@app.get("/stats")
async def get_system_stats(api_key: str = Depends(verify_api_key)):
    """Returns real-time network overview for monitoring & demo verification."""
    async with state.lock:
        file_stats = []
        for f_id, f_obj in state.files.items():
            peer_count = len(state.peers.get(f_id, {}))
            file_stats.append({
                "file_id": f_id,
                "name": f_obj.name,
                "size": f_obj.size,
                "chunks": f_obj.chunk_count,
                "active_peers": peer_count
            })
    return {
        "total_registered_files": len(file_stats),
        "files": file_stats
    }

def start_tracker(host: str = DEFAULT_TRACKER_HOST, port: int = DEFAULT_TRACKER_PORT):
    uvicorn.run(app, host=host, port=port, log_level="info")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Start DS02 BitTorrent Tracker")
    parser.add_argument("--host", default=DEFAULT_TRACKER_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_TRACKER_PORT)
    args = parser.parse_args()
    start_tracker(args.host, args.port)
