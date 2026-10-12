# DS02_BitTorrent

A Peer-to-Peer file sharing system simulating BitTorrent.

## Architecture

This project consists of:
- **Tracker Server**: A FastAPI application to track peers and files.
- **Peer Node**: A gRPC + asyncio based node that acts as both seeder and leecher.

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Generate gRPC code:
   ```bash
   python -m grpc_tools.protoc -I./src --python_out=./src --grpc_python_out=./src ./src/peer.proto
   ```
3. Configure environment variables (Security/Auth):
   ```bash
   # On Windows PowerShell/CMD:
   copy .env.example .env
   
   # On Linux/Mac:
   cp .env.example .env
   ```

## Running the System

Follow these steps to demonstrate the P2P file sharing capabilities:

**1. Start the Tracker Server**
Open a new terminal and run:
```bash
python src/tracker.py
```
*The tracker will start on `http://127.0.0.1:8000`.*

**2. Create a test file and generate metadata**
Open a second terminal:
```bash
# Create a dummy file (Windows PowerShell)
echo "Hello BitTorrent World! This is a test file for the P2P network." > dummy.txt

# Generate the .torrent.json metadata file
python src/metadata.py dummy.txt http://127.0.0.1:8000
```

**3. Start the Seeder (Peer 1)**
In the same terminal, start the seeder which has the complete file:
```bash
python src/peer_node.py --port 5001 --metadata dummy.txt.torrent.json --output dummy.txt --seed
```

**4. Start Leechers (Peers 2 and 3)**
Open a third terminal and start a peer to download the file:
```bash
python src/peer_node.py --port 5002 --metadata dummy.txt.torrent.json --output download_1.txt
```

Open a fourth terminal to start another peer:
```bash
python src/peer_node.py --port 5003 --metadata dummy.txt.torrent.json --output download_2.txt
```

You will see the leechers fetching chunks from the seeder (and from each other) and verifying the final file hash once complete. To test the resume functionality, simply terminate a leecher (`Ctrl+C`) and start it again.
