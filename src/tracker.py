import time
from typing import Dict, List, Set
from fastapi import FastAPI, HTTPException, Depends, Header
from pydantic import BaseModel
import uvicorn
import logging
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="BitTorrent Tracker")

TRACKER_API_KEY = os.getenv("TRACKER_API_KEY", "default-secret-key")

async def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != TRACKER_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API Key")
    return x_api_key

# Peer state
# PEER_TIMEOUT = 60 seconds (if no announce/heartbeat in 60s, remove peer)
PEER_TIMEOUT = 60

class FileRegistration(BaseModel):
    file_id: str
    name: str
    size: int
    chunk_count: int

class PeerAnnounce(BaseModel):
    peer_id: str
    file_id: str
    host: str
    port: int
    chunks_owned: List[int] # List of chunk indices this peer currently has

# In-memory database
# files = { file_id: FileRegistration }
files: Dict[str, FileRegistration] = {}

# peers = { file_id: { peer_id: { "host": str, "port": int, "chunks": set(), "last_seen": float } } }
peers: Dict[str, Dict[str, dict]] = {}

@app.post("/register")
async def register_file(file_info: FileRegistration, api_key: str = Depends(verify_api_key)):
    """Called by the seeder to register a new file in the tracker."""
    files[file_info.file_id] = file_info
    if file_info.file_id not in peers:
        peers[file_info.file_id] = {}
    logger.info(f"Registered file: {file_info.file_id} ({file_info.name})")
    return {"status": "success", "file_id": file_info.file_id}

@app.post("/announce")
async def announce(announce_info: PeerAnnounce, api_key: str = Depends(verify_api_key)):
    """Called by peers periodically to update their status (chunks they have and liveness)."""
    file_id = announce_info.file_id
    peer_id = announce_info.peer_id
    
    if file_id not in files:
        raise HTTPException(status_code=404, detail="File not registered")
    
    if file_id not in peers:
        peers[file_id] = {}
        
    peers[file_id][peer_id] = {
        "host": announce_info.host,
        "port": announce_info.port,
        "chunks": set(announce_info.chunks_owned),
        "last_seen": time.time()
    }
    logger.debug(f"Announce from {peer_id} for file {file_id}, chunks: {len(announce_info.chunks_owned)}")
    return {"status": "success"}

@app.get("/peers/{file_id}")
async def get_peers(file_id: str, exclude_peer: str = None, api_key: str = Depends(verify_api_key)):
    """Returns a list of active peers for a file."""
    if file_id not in files:
        raise HTTPException(status_code=404, detail="File not registered")
    
    active_peers = []
    current_time = time.time()
    
    if file_id in peers:
        for peer_id, peer_info in list(peers[file_id].items()):
            # Check timeout
            if current_time - peer_info["last_seen"] > PEER_TIMEOUT:
                logger.info(f"Removing dead peer {peer_id} from {file_id}")
                del peers[file_id][peer_id]
                continue
                
            if exclude_peer and peer_id == exclude_peer:
                continue
                
            active_peers.append({
                "peer_id": peer_id,
                "host": peer_info["host"],
                "port": peer_info["port"],
                "chunks": list(peer_info["chunks"])
            })
            
    return {"file_id": file_id, "peers": active_peers}

if __name__ == "__main__":
    import os
    host = os.getenv("TRACKER_HOST", "127.0.0.1")
    port = int(os.getenv("TRACKER_PORT", "8000"))
    uvicorn.run(app, host=host, port=port)
