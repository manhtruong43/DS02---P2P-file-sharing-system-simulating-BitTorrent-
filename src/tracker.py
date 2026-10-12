import os
import sys

# Ensure repository root is in python path
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.tracker.tracker import app, start_tracker, DEFAULT_TRACKER_HOST, DEFAULT_TRACKER_PORT

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Start DS02 BitTorrent Tracker")
    parser.add_argument("--host", default=DEFAULT_TRACKER_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_TRACKER_PORT)
    args = parser.parse_args()
    start_tracker(args.host, args.port)
