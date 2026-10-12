import asyncio
import os
import subprocess
import time
import signal
import sys

def run_command(cmd, log_file=None):
    kwargs = {}
    if log_file:
        f = open(log_file, "w")
        kwargs['stdout'] = f
        kwargs['stderr'] = subprocess.STDOUT
    return subprocess.Popen(cmd, shell=True, **kwargs)

async def main():
    print("Starting BitTorrent Simulation Demo...")
    
    # 1. Create a dummy file to share
    dummy_file = "dummy_data.bin"
    print(f"Creating dummy file {dummy_file} (2 MB)...")
    with open(dummy_file, "wb") as f:
        f.write(os.urandom(2 * 1024 * 1024))
        
    # 2. Generate metadata
    print("Generating metadata...")
    subprocess.run(f"python src/metadata.py {dummy_file} http://127.0.0.1:8000", shell=True)
    metadata_file = f"{dummy_file}.torrent.json"
    
    processes = []
    try:
        # 3. Start Tracker
        print("Starting Tracker Server on port 8000...")
        tracker_proc = run_command("python src/tracker.py", "tracker.log")
        processes.append(tracker_proc)
        time.sleep(2) # Wait for tracker to start
        
        # 4. Start Seeder
        print("Starting Seeder on port 50051...")
        seeder_proc = run_command(f"python src/peer_node.py --port 50051 --metadata {metadata_file} --output {dummy_file} --seed", "seeder.log")
        processes.append(seeder_proc)
        time.sleep(2) # Wait for seeder to register and announce
        
        # 5. Start Peer A
        peer_a_file = "download_A.bin"
        print("Starting Peer A on port 50052...")
        peer_a_proc = run_command(f"python src/peer_node.py --port 50052 --metadata {metadata_file} --output {peer_a_file}", "peer_a.log")
        processes.append(peer_a_proc)
        
        # 6. Start Peer B
        peer_b_file = "download_B.bin"
        print("Starting Peer B on port 50053...")
        peer_b_proc = run_command(f"python src/peer_node.py --port 50053 --metadata {metadata_file} --output {peer_b_file}", "peer_b.log")
        processes.append(peer_b_proc)
        
        print("Peers are downloading. Waiting 20 seconds...")
        for i in range(20):
            sys.stdout.write(f"\rTime elapsed: {i+1}s")
            sys.stdout.flush()
            time.sleep(1)
        print("\nChecking results...")
        
    finally:
        print("\nCleaning up processes...")
        for p in processes:
            p.terminate()
            p.kill()
            
    print("\nDemo finished. Please check tracker.log, seeder.log, peer_a.log, and peer_b.log for detailed execution traces.")

if __name__ == "__main__":
    asyncio.run(main())
