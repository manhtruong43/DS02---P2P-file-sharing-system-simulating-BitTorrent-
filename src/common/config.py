import os
from dotenv import load_dotenv

load_dotenv()

# Network & Service Defaults
DEFAULT_TRACKER_HOST = os.getenv("TRACKER_HOST", "127.0.0.1")
DEFAULT_TRACKER_PORT = int(os.getenv("TRACKER_PORT", "8000"))
DEFAULT_TRACKER_URL = os.getenv("TRACKER_URL", f"http://{DEFAULT_TRACKER_HOST}:{DEFAULT_TRACKER_PORT}")

# Peer Defaults
DEFAULT_PEER_HOST = os.getenv("PEER_HOST", "127.0.0.1")
DEFAULT_CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", str(256 * 1024))) # 256 KB

# Security
TRACKER_API_KEY = os.getenv("TRACKER_API_KEY", "ds02_secret_key_123")

# Timeouts & Limits (Seconds)
PEER_TIMEOUT = 60 # Seconds before tracker evicts dead peer (TTL)
HEARTBEAT_INTERVAL = 10 # Announce/heartbeat loop interval
FETCH_PEERS_INTERVAL = 4 # Interval for polling new peers
GRPC_TIMEOUT = 5 # RPC call timeout
MAX_CONCURRENT_DOWNLOADS = 5
MAX_RETRY_PER_CHUNK = 3
