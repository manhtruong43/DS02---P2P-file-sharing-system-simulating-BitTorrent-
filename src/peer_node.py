import os
import sys
import asyncio

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.peer.peer_node import PeerNode, DEFAULT_PEER_HOST, DEFAULT_TRACKER_URL

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run a Peer Node")
    parser.add_argument("--host", default=DEFAULT_PEER_HOST, help="Host address to bind to")
    parser.add_argument("--port", type=int, required=True, help="Port to bind to")
    parser.add_argument("--tracker", default=DEFAULT_TRACKER_URL, help="Tracker URL")
    parser.add_argument("--metadata", required=True, help="Path to .torrent.json file")
    parser.add_argument("--output", required=True, help="Path to save/read file")
    parser.add_argument("--seed", action="store_true", help="Run as seeder (initial owner)")

    args = parser.parse_args()

    node = PeerNode(host=args.host, port=args.port, tracker_url=args.tracker)
    node.init_from_metadata(args.metadata, args.output, is_seeder=args.seed)

    if args.seed:
        asyncio.run(node.register_file_with_tracker())

    try:
        asyncio.run(node.run())
    except KeyboardInterrupt:
        print("Peer node stopped.")
