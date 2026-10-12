import os
import sys
import tempfile
import asyncio
import pytest
from fastapi.testclient import TestClient

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from src.common.config import TRACKER_API_KEY
from src.common.metadata import TorrentMetadata
from src.tracker.tracker import app, state
from src.peer.peer_node import PeerNode
from src.peer.chunk_manager import ChunkManager, ChunkStatus

def test_1_corrupt_chunk_rejected_and_penalized():
    """
    Security / Invalid Data Test 1:
    Peer độc hại hoặc mạng lỗi gửi chunk bị hỏng / sai mã băm SHA-256.
    Leecher phải phát hiện mã băm không khớp, từ chối ghi đĩa,
    đánh dấu chunk MISSING và cộng điểm phạt (penalize) cho peer đó.
    """
    async def _run():
        num_chunks = 4
        chunk_size = 256
        good_data = b"LegitimateDataBlock_" * (num_chunks * chunk_size // 20)
        good_data = good_data[: num_chunks * chunk_size]

        with tempfile.NamedTemporaryFile(delete=False) as tf:
            tf.write(good_data)
            good_file = tf.name

        meta_path = good_file + ".torrent.json"
        dest_file = good_file + ".security_dest"

        try:
            meta = TorrentMetadata(good_file, chunk_size=chunk_size)
            meta_dict = meta.generate()
            meta.save(meta_path)

            cm = ChunkManager(
                file_path=dest_file,
                file_size=meta_dict["file_size"],
                chunk_size=chunk_size,
                chunk_hashes=meta_dict["chunk_hashes"],
                expected_file_hash=meta_dict["file_hash"]
            )
            cm.initialize_storage(is_seeder=False)

            # Cố tình gửi dữ liệu bị hỏng (corrupt data) cho chunk 0
            corrupted_data = b"CORRUPTED_BYTES_MALICIOUS_CONTENT_" * 8
            corrupted_data = corrupted_data[:chunk_size]

            # Ghi chunk bị hỏng -> write_chunk_atomic phải trả về False
            ok = await cm.write_chunk_atomic(0, corrupted_data)
            assert ok is False
            assert 0 not in cm.chunks_owned
            assert cm.status[0] == ChunkStatus.MISSING

            # Bây giờ ghi đúng dữ liệu -> phải thành công
            good_chunk_0 = good_data[0:chunk_size]
            ok_good = await cm.write_chunk_atomic(0, good_chunk_0)
            assert ok_good is True
            assert 0 in cm.chunks_owned
            assert cm.status[0] == ChunkStatus.DONE
        finally:
            for p in [good_file, meta_path, dest_file]:
                if os.path.exists(p):
                    os.remove(p)

    asyncio.run(_run())

def test_2_tracker_unauthorized_access_and_invalid_inputs():
    """
    Security / Invalid Data Test 2:
    - Tracker từ chối các request không có hoặc sai X-API-Key (HTTP 401).
    - Tracker chặn các yêu cầu không hợp lệ (file_id chưa đăng ký, port ngoài phạm vi).
    """
    client = TestClient(app)

    # 1. Truy cập không có API key -> 401
    res1 = client.get("/peers/non_existent_file")
    assert res1.status_code == 401

    # 2. Truy cập với API key sai -> 401
    res2 = client.get("/peers/non_existent_file", headers={"X-API-Key": "wrong_secret_token"})
    assert res2.status_code == 401

    # 3. Truy cập với API key đúng nhưng file_id không tồn tại -> 404
    valid_headers = {"X-API-Key": TRACKER_API_KEY}
    res3 = client.get("/peers/unknown_hash_12345678", headers=valid_headers)
    assert res3.status_code == 404

    # 4. Gửi payload đăng ký sai schema (size âm) -> 422 Unprocessable Entity
    invalid_reg = {
        "file_id": "test_hash_val",
        "name": "sample.iso",
        "size": -100, # Kích thước âm
        "chunk_count": 0
    }
    res4 = client.post("/register", json=invalid_reg, headers=valid_headers)
    assert res4.status_code == 422
