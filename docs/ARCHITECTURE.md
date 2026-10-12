# DS02 BitTorrent Simulation – Kiến Trúc & Thiết Kế Hệ Phân Tán

> **Môn học:** Distributed Systems  
> **Đề tài:** DS02 – P2P File Sharing System Simulating BitTorrent  
> **Mục tiêu:** Chia sẻ file phi tập trung, chia chunk, tải song song đa nguồn, chịu lỗi và nhất quán dữ liệu.

---

## 1. Sơ đồ Kiến trúc Tổng thể

```
                 ┌────────────────────────────────────────────────────────┐
                 │                    TRACKER SERVER                      │
                 │  - FastAPI HTTP REST Engine (Port 8000)                │
                 │  - Quản lý Metadata & Danh sách Peer (Peer Discovery)  │
                 │  - TTL / Heartbeat Pruning & Concurrency Lock          │
                 │  - KHÔNG LƯU TRỮ VÀ KHÔNG PHỤC VỤ DỮ LIỆU FILE         │
                 └───────────────▲────────────────────────▲───────────────┘
                                 │                        │
               Register / Announce                        │ Announce / Get Peers
                                 │                        │
                    ┌────────────┴───────────┐            │
                    │         PEER A         │            │
                    │   (Seeder: 100% Chunks)│            │
                    │   gRPC Port 50051      │            │
                    └────────────┬───────────┘            │
                                 │                        │
                   Chunk Request │ gRPC       ┌───────────┴────────────┐
                   Chunk Response│ Stream     │         PEER B         │
                                 │            │    (Leecher / Seeder)  │
                                 ▼            │    gRPC Port 50052     │
                    ┌────────────────────────┐└───────────┬────────────┘
                    │         PEER C         │            │
                    │       (Leecher)        │◄───────────┘
                    │    gRPC Port 50053     │   Parallel Chunk Transfer
                    └────────────────────────┘   Immediate HAVE / Announce
```

---

## 2. Các Thành phần Hệ thống

### 2.1. Tracker Server (`src/tracker/`)
- **Vai trò:** Bootstrap server & Peer registry.
- **API Endpoints:**
  - `POST /register`: Seeder đăng ký file mới (`file_id`, `name`, `size`, `chunk_count`).
  - `POST /announce`: Peer báo cáo bitfield và cập nhật liveness.
  - `POST /heartbeat`: Giữ kết nối online.
  - `POST /leave`: Peer ngắt kết nối chủ động (xóa ngay lập tức khỏi peer list).
  - `GET /peers/{file_id}`: Trả về danh sách peer đang active (tự động dọn dẹp peer chết quá `PEER_TIMEOUT = 60s`).
  - `GET /stats`: Giám sát toàn bộ trạng thái mạng P2P.
- **Bảo mật:** Xác thực qua header `X-API-Key`.

### 2.2. Metadata Generator (`src/common/metadata.py`)
- **Định dạng file:** `.torrent.json`.
- **Trường dữ liệu bắt buộc:**
  ```json
  {
    "file_id": "sha256_hex_digest_of_file",
    "file_name": "example.bin",
    "file_size": 2097152,
    "chunk_size": 262144,
    "num_chunks": 8,
    "chunk_hashes": ["hash0", "hash1", "..."],
    "file_hash": "sha256_hex_digest",
    "trackers": ["http://127.0.0.1:8000"]
  }
  ```

### 2.3. Chunk & Piece Manager (`src/peer/chunk_manager.py`)
- **Quản lý trạng thái từng chunk:** `MISSING`, `DOWNLOADING`, `DONE`.
- **Atomic Disk Write:** Sử dụng `asyncio.Lock()` đảm bảo khi nhiều luồng tải chunk song song ghi vào cùng file không xảy ra race condition.
- **Resume Capability:** Khi khởi động lại, quét và verify các chunk đã có để tiếp tục tải các chunk còn thiếu.

### 2.4. Peer Node Engine (`src/peer/peer_node.py`)
- **Giao thức:** Protocol Buffers & gRPC (`GetChunk`, `Handshake`, `GetBitfield`).
- **Thuật toán tải Rarest-First:** Ưu tiên tải các chunk có tần suất xuất hiện thấp nhất trong mạng.
- **Immediate HAVE / Announce:** Ngay khi tải xong 1 chunk, peer lập tức cập nhật bitfield lên Tracker giúp các Leecher khác lấy được ngay.
- **Giới hạn đồng thời:** Giới hạn tối đa `MAX_CONCURRENT_DOWNLOADS = 5`.

---

## 3. Mô hình Đồng thời & Nhất quán (Concurrency & Consistency)

| Tình huống xung đột | Cơ chế kiểm soát |
|---|---|
| **Nhiều luồng ghi chunk đồng thời xuống đĩa** | `asyncio.Lock()` tại `ChunkManager.write_chunk_atomic()` đảm bảo atomic file seek & write. |
| **Nhiều peer cùng yêu cầu 1 chunk từ 1 Seeder** | gRPC aio asynchronous server xử lý non-blocking đa kết nối đồng thời. |
| **Tracker cập nhật peer list từ nhiều peer cùng lúc** | `asyncio.Lock()` bao bọc toàn bộ read/write state của Tracker. |
| **Mô hình nhất quán:** | **Eventual Consistency** cho peer list trên Tracker; **Strong Integrity** cho từng chunk (bắt buộc đúng hash SHA-256). |

---

## 4. Mô hình Lỗi & Cơ chế Phục hồi (Failure Model & Fault Tolerance)

1. **Peer Crash-Stop (Ngắt kết nối đột ngột):**
   - *Phát hiện:* gRPC timeout (`GRPC_TIMEOUT = 5s`) hoặc kết nối bị từ chối.
   - *Xử lý:* Tăng điểm phạt `bad_peers`, chuyển ngay sang peer thay thế (failover).
2. **Chunk bị hỏng / giả mạo dữ liệu:**
   - *Phát hiện:* Kiểm tra SHA-256 ngay khi nhận chunk; nếu mismatch thì từ chối ghi đĩa.
   - *Xử lý:* Đánh dấu chunk `MISSING`, phạt điểm peer xấu, yêu cầu tải lại từ peer khác.
3. **Download bị gián đoạn giữa chừng:**
   - *Xử lý:* Lưu trực tiếp dữ liệu chunk verified xuống đĩa; khi khởi động lại quét kiểm tra hash và tải tiếp (Resume).
4. **Peer chết trên Tracker:**
   - *Xử lý:* Cơ chế TTL/Lease tự động xóa peer sau 60 giây không gửi announce.
