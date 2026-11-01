import os
import sys

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.common.metadata import main

if __name__ == "__main__":
    # Usage:
    #   python src/metadata.py demo.bin http://127.0.0.1:8000
    #   python src/metadata.py demo.bin http://127.0.0.1:8000 --chunk-size 131072 --out demo.bin.torrent.json
    main()
