import os
import sys
import tempfile
import time
import pytest
import asyncio
from fastapi.testclient import TestClient

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.common.config import TRACKER_API_KEY
from src.common.hashing import compute_chunk_hash, compute_file_hashes, verify_entire_file
from src.common.metadata import TorrentMetadata
from src.tracker.tracker import app, state, PEER_TIMEOUT
from src.peer.chunk_manager import ChunkManager, ChunkStatus
from src.peer.peer_node import PeerNode

@pytest.fixture(autouse=True)
def reset_tracker_state():
    state.files.clear()
    state.peers.clear()
    yield
    state.files.clear()
    state.peers.clear()

def test_1_metadata_generation_and_hashing():
    """Functional Test 1: Tạo metadata đúng các trường và hash khớp dữ liệu gốc."""
    with tempfile.NamedTemporaryFile(delete=False) as tf:
        tf.write(b"Distributed Systems DS02 BitTorrent Simulation Course Project 2026")
        tf_name = tf.name

    try:
        meta = TorrentMetadata(tf_name, trackers=["http://127.0.0.1:8000"], chunk_size=16)
        data = meta.generate()

        assert "file_id" in data
        assert data["file_name"] == os.path.basename(tf_name)
        assert data["file_size"] == 66
        assert data["chunk_size"] == 16
        assert data["num_chunks"] == 5
        assert len(data["chunk_hashes"]) == 5
        assert len(data["file_hash"]) == 64
        assert data["trackers"] == ["http://127.0.0.1:8000"]
    finally:
        if os.path.exists(tf_name):
            os.remove(tf_name)

def test_2_chunk_split_and_merge_integrity():
    """Functional Test 2: Chia file thành các chunk và ghép lại cho hash trùng khớp 100%."""
    raw_data = b"Chunk0Data_____" + b"Chunk1Data_____" + b"Chunk2Data_____" + b"LastPiece"
    with tempfile.NamedTemporaryFile(delete=False) as tf:
        tf.write(raw_data)
        src_path = tf.name

    dest_path = src_path + ".reconstructed"
    try:
        file_hash, chunk_hashes, size = compute_file_hashes(src_path, chunk_size=15)
        cm = ChunkManager(dest_path, file_size=size, chunk_size=15, chunk_hashes=chunk_hashes, expected_file_hash=file_hash)
        cm.initialize_storage(is_seeder=False)

        # Write each chunk
        for i, ch_hash in enumerate(chunk_hashes):
            chunk_slice = raw_data[i*15 : (i+1)*15]
            assert asyncio.run(cm.write_chunk_atomic(i, chunk_slice)) is True

        assert cm.is_complete() is True
        assert cm.verify_final_file() is True

        # Verify reconstructed file content
        with open(dest_path, "rb") as rf:
            assert rf.read() == raw_data
    finally:
        if os.path.exists(src_path):
            os.remove(src_path)
        if os.path.exists(dest_path):
            os.remove(dest_path)

def test_3_tracker_register_file():
    """Functional Test 3: Seeder đăng ký file với Tracker thành công."""
    client = TestClient(app)
    headers = {"X-API-Key": TRACKER_API_KEY}
    payload = {
        "file_id": "test_file_hash_1234567890abcdef",
        "name": "sample_video.mp4",
        "size": 1048576,
        "chunk_count": 4
    }
    response = client.post("/register", json=payload, headers=headers)
    assert response.status_code == 200
    assert response.json()["status"] == "success"

def test_4_tracker_peer_announce_and_get_peers():
    """Functional Test 4: Peer announce bitfield và nhận danh sách peer chính xác."""
    client = TestClient(app)
    headers = {"X-API-Key": TRACKER_API_KEY}
    f_id = "test_file_hash_announce_12345678"
    
    # Register file first
    client.post("/register", json={"file_id": f_id, "name": "data.bin", "size": 1000, "chunk_count": 4}, headers=headers)

    # Peer 1 announce
    p1_payload = {
        "peer_id": "127.0.0.1:5001",
        "file_id": f_id,
        "host": "127.0.0.1",
        "port": 5001,
        "chunks_owned": [0, 1, 2, 3]
    }
    res1 = client.post("/announce", json=p1_payload, headers=headers)
    assert res1.status_code == 200

    # Peer 2 queries peers
    res_peers = client.get(f"/peers/{f_id}", headers=headers)
    assert res_peers.status_code == 200
    peers = res_peers.json()["peers"]
    assert len(peers) == 1
    assert peers[0]["peer_id"] == "127.0.0.1:5001"
    assert peers[0]["chunks"] == [0, 1, 2, 3]

def test_5_tracker_ttl_prune_expired_peer():
    """Functional Test 5: Tracker tự động xóa peer hết hạn sau timeout TTL."""
    client = TestClient(app)
    headers = {"X-API-Key": TRACKER_API_KEY}
    f_id = "test_file_ttl_12345678"
    client.post("/register", json={"file_id": f_id, "name": "data.bin", "size": 1000, "chunk_count": 4}, headers=headers)

    # Announce peer
    client.post("/announce", json={
        "peer_id": "127.0.0.1:9999",
        "file_id": f_id,
        "host": "127.0.0.1",
        "port": 9999,
        "chunks_owned": [0]
    }, headers=headers)

    # Fake time expiration in state
    state.peers[f_id]["127.0.0.1:9999"]["last_seen"] = time.time() - (PEER_TIMEOUT + 10)

    # Query peers -> should prune expired peer
    res = client.get(f"/peers/{f_id}", headers=headers)
    assert res.status_code == 200
    assert len(res.json()["peers"]) == 0

def test_6_tracker_graceful_leave():
    """Functional Test 6: Peer gửi request /leave được xóa khỏi Tracker ngay lập tức."""
    client = TestClient(app)
    headers = {"X-API-Key": TRACKER_API_KEY}
    f_id = "test_file_leave_12345678"
    client.post("/register", json={"file_id": f_id, "name": "data.bin", "size": 1000, "chunk_count": 4}, headers=headers)
    client.post("/announce", json={
        "peer_id": "127.0.0.1:5002",
        "file_id": f_id,
        "host": "127.0.0.1",
        "port": 5002,
        "chunks_owned": [0, 1]
    }, headers=headers)

    # Check peer exists
    res = client.get(f"/peers/{f_id}", headers=headers)
    assert len(res.json()["peers"]) == 1

    # Leave
    leave_res = client.post("/leave", json={"peer_id": "127.0.0.1:5002", "file_id": f_id}, headers=headers)
    assert leave_res.status_code == 200

    # Verify removed
    res_after = client.get(f"/peers/{f_id}", headers=headers)
    assert len(res_after.json()["peers"]) == 0

def test_7_single_seeder_download_complete():
    """Functional Test 7: Tải trọn vẹn file từ 1 Seeder qua gRPC và khớp hash cuối."""
    async def _run():
        with tempfile.NamedTemporaryFile(delete=False) as sf:
            sf.write(b"Full Seeder Content Test for Functional Test 7!")
            seeder_file = sf.name

        leecher_file = seeder_file + ".leecher_out"
        meta_path = seeder_file + ".torrent.json"

        try:
            meta = TorrentMetadata(seeder_file, chunk_size=16)
            meta.save(meta_path)

            seeder = PeerNode(host="127.0.0.1", port=50101)
            seeder.init_from_metadata(meta_path, seeder_file, is_seeder=True)
            await seeder.start_server()

            leecher = PeerNode(host="127.0.0.1", port=50102)
            leecher.init_from_metadata(meta_path, leecher_file, is_seeder=False)
            leecher.active_peers = [{
                "peer_id": "127.0.0.1:50101",
                "host": "127.0.0.1",
                "port": 50101,
                "chunks": list(range(leecher.chunk_mgr.total_chunks))
            }]

            for c in range(leecher.chunk_mgr.total_chunks):
                ok = await leecher.download_chunk_from_peer(c, leecher.active_peers[0])
                assert ok is True

            assert leecher.chunk_mgr.is_complete() is True
            assert leecher.chunk_mgr.verify_final_file() is True

            await seeder.stop_server()
        finally:
            for p in [seeder_file, leecher_file, meta_path]:
                if os.path.exists(p):
                    os.remove(p)
    asyncio.run(_run())

def test_8_parallel_multi_peer_download():
    """Functional Test 8: Tải song song từ 2 Peer (Peer A có nửa đầu, Peer B có nửa sau)."""
    async def _run():
        full_data = b"PartA_Chunk0Data" + b"PartA_Chunk1Data" + b"PartB_Chunk2Data" + b"PartB_Chunk3Data"
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(full_data)
            src_file = f.name

        file_a = src_file + ".a"
        file_b = src_file + ".b"
        file_c = src_file + ".c"
        meta_path = src_file + ".torrent.json"

        try:
            meta = TorrentMetadata(src_file, chunk_size=16)
            meta.save(meta_path)

            with open(file_a, "wb") as fa:
                fa.write(full_data)
            node_a = PeerNode(host="127.0.0.1", port=50201)
            node_a.init_from_metadata(meta_path, file_a, is_seeder=False)
            node_a.chunk_mgr.chunks_owned = {0, 1}
            await node_a.start_server()

            with open(file_b, "wb") as fb:
                fb.write(full_data)
            node_b = PeerNode(host="127.0.0.1", port=50202)
            node_b.init_from_metadata(meta_path, file_b, is_seeder=False)
            node_b.chunk_mgr.chunks_owned = {2, 3}
            await node_b.start_server()

            node_c = PeerNode(host="127.0.0.1", port=50203)
            node_c.init_from_metadata(meta_path, file_c, is_seeder=False)
            node_c.active_peers = [
                {"peer_id": "127.0.0.1:50201", "host": "127.0.0.1", "port": 50201, "chunks": [0, 1]},
                {"peer_id": "127.0.0.1:50202", "host": "127.0.0.1", "port": 50202, "chunks": [2, 3]}
            ]

            tasks = [
                node_c.download_chunk_from_peer(0, node_c.active_peers[0]),
                node_c.download_chunk_from_peer(1, node_c.active_peers[0]),
                node_c.download_chunk_from_peer(2, node_c.active_peers[1]),
                node_c.download_chunk_from_peer(3, node_c.active_peers[1]),
            ]
            results = await asyncio.gather(*tasks)
            assert all(results) is True
            assert node_c.chunk_mgr.is_complete() is True
            assert node_c.chunk_mgr.verify_final_file() is True

            await node_a.stop_server()
            await node_b.stop_server()
        finally:
            for p in [src_file, file_a, file_b, file_c, meta_path]:
                if os.path.exists(p):
                    os.remove(p)
    asyncio.run(_run())
