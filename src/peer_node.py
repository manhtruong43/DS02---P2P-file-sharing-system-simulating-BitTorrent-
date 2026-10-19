import os
import sys

# Ensure repository root is in python path so "src.*" imports work when run as a script
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.peer.peer_node import main

if __name__ == "__main__":
    # Usage:
    #   python src/peer_node.py --port 50051 --metadata demo.bin.torrent.json --output demo.bin --seed
    #   python src/peer_node.py --port 50052 --metadata demo.bin.torrent.json --output download_1.bin
    #   python src/peer_node.py --port 50054 --metadata demo.bin.torrent.json --output bad.bin --seed --corrupt
    #   python src/peer_node.py --port 50051 --metadata demo.bin.torrent.json --output demo.bin --seed --upload-limit 256
    main()
