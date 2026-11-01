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
# The API key MUST come from the environment (.env file or real environment variable).
# There is deliberately no default value in source code (no secret committed to GitHub).
# An empty key means "not configured": the tracker then rejects every request and
# the entry points (tracker / peer) refuse to start, see require_api_key().
TRACKER_API_KEY = os.getenv("TRACKER_API_KEY", "").strip()
API_KEY_PLACEHOLDER = "change_me_to_a_random_secret"

# Timeouts & Limits (Seconds)
PEER_TIMEOUT = 60 # Seconds before tracker evicts dead peer (TTL)
HEARTBEAT_INTERVAL = 10 # Announce/heartbeat loop interval
FETCH_PEERS_INTERVAL = 4 # Interval for polling new peers
GRPC_TIMEOUT = 5 # RPC call timeout
MAX_CONCURRENT_DOWNLOADS = 5
MAX_RETRY_PER_CHUNK = 3

# Size limits (validated by the Metadata Generator and when metadata is loaded)
# gRPC rejects messages larger than 4 MB by default; a chunk travels in ONE ChunkResponse
# together with a few bytes of protobuf framing, so the chunk itself must stay below that.
MIN_CHUNK_SIZE = 1                       # bytes
MAX_CHUNK_SIZE = 3 * 1024 * 1024         # 3 MB, safely below the 4 MB gRPC message limit
MAX_FILE_SIZE = 4 * 1024 * 1024 * 1024   # 4 GB per shared file
MAX_NUM_CHUNKS = 1_000_000               # upper bound on chunk_hashes length in a metadata file


def require_api_key() -> str:
    """
    Returns the configured API key or stops the program with a clear message.
    Called by the tracker and peer entry points before they start serving.
    """
    if not TRACKER_API_KEY:
        raise SystemExit(
            "[CONFIG ERROR] TRACKER_API_KEY chưa được cấu hình.\n"
            "  Tạo file .env từ .env.example (Copy-Item .env.example .env hoặc cp .env.example .env)\n"
            "  rồi đặt TRACKER_API_KEY thành một chuỗi bí mật, ví dụ:\n"
            "  python -c \"import secrets; print(secrets.token_hex(16))\""
        )
    return TRACKER_API_KEY
