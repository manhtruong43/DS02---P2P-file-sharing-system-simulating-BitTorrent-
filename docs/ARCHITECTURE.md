# DS02 BitTorrent Simulation – Kiến Trúc & Thiết Kế Hệ Phân Tán

> **Môn học:** Distributed Systems
> **Đề tài:** DS02 – P2P File Sharing System Simulating BitTorrent
> **Mục tiêu:** Chia sẻ file phi tập trung, chia chunk, tải song song đa nguồn, chịu lỗi và nhất quán dữ liệu.
> Tài liệu này mô tả đúng hành vi của mã nguồn hiện tại; mọi tham số đều lấy từ `src/common/config.py` và `src/peer/peer_node.py`.

---

## 1. Sơ đồ Kiến trúc Tổng thể

```
                 ┌────────────────────────────────────────────────────────┐
                 │                    TRACKER SERVER                      │
                 │  - FastAPI HTTP REST (port 8000)                       │
                 │  - Metadata registry + peer list + bitfield từng peer  │
                 │  - TTL 60s (lease), asyncio.Lock bảo vệ trạng thái     │
                 │  - KHÔNG LƯU TRỮ VÀ KHÔNG PHỤC VỤ DỮ LIỆU FILE         │
                 └──────▲───────────────────▲────────────────────▲────────┘
        register/announce│        announce/ │get_peers   announce/│get_peers
                         │                  │                     │
              ┌──────────┴─────┐   ┌────────┴───────┐   ┌─────────┴──────┐
              │    PEER A      │   │    PEER B      │   │    PEER C      │
              │    Seeder      │   │ Leecher→Seeder │   │ Leecher→Seeder │
              │ gRPC server    │   │ gRPC server    │   │ gRPC server    │
              │ + client       │   │ + client       │   │ + client       │
              └───────┬────────┘   └───┬────────▲───┘   └───▲──────▲─────┘
                      │  GetChunk      │        │ GetChunk  │      │
                      └────────────────┼────────┼───────────┘      │
                                       │        └──────────────────┘
                                       └─── GetChunk (B tải chunk C đã có)
```

- Mọi thành phần là **tiến trình độc lập**, chỉ giao tiếp qua mạng: HTTP/JSON tới Tracker, gRPC/Protocol Buffers giữa các Peer.
- Mỗi peer **vừa là server vừa là client**: chunk vừa tải xong và verify xong có thể được phục vụ ngay cho peer khác.

---

## 2. Các Thành phần Hệ thống

### 2.1. Tracker Server (`src/tracker/tracker.py`)
- **Vai trò:** bootstrap server và peer registry. Chỉ lưu `file_id → metadata tóm tắt` và `file_id → {peer_id: host, port, bitfield, last_seen}`.
- **API (mọi endpoint trừ `/health` yêu cầu header `X-API-Key`):**

| Endpoint | Message tương ứng | Mô tả |
|---|---|---|
| `POST /register` | REGISTER_FILE | Seeder đăng ký file (`file_id`, `name`, `size`, `chunk_count`) |
| `POST /announce` | ANNOUNCE / HAVE | Peer gửi bitfield hiện có; đồng thời gia hạn lease (`last_seen`) |
| `POST /heartbeat` | HEARTBEAT | Gia hạn lease mà không gửi bitfield |
| `POST /leave` | LEAVE | Peer rời mạng chủ động, bị xóa ngay khỏi danh sách |
| `GET /peers/{file_id}?exclude_peer=` | GET_PEERS / PEER_LIST | Trả danh sách peer còn sống kèm bitfield; xóa peer quá TTL trước khi trả |
| `GET /stats` | – | Giám sát: số file, số peer mỗi file |
| `GET /health` | – | Kiểm tra tracker còn sống |

### 2.2. Metadata Generator (`src/common/metadata.py`, chạy qua `src/metadata.py`)
- Đọc file theo từng chunk, băm SHA-256 từng chunk và toàn file, ghi ra `.torrent.json`:
  ```json
  {
    "file_id": "sha256 của toàn file",
    "file_name": "example.bin",
    "file_size": 4194304,
    "chunk_size": 262144,
    "num_chunks": 16,
    "chunk_hashes": ["hash0", "hash1", "..."],
    "file_hash": "sha256 của toàn file",
    "trackers": ["http://127.0.0.1:8000"]
  }
  ```
- `file_id` = hash toàn file → **định danh theo nội dung**: hai file khác nội dung không thể trùng `file_id`.
- **Kiểm tra khi sinh metadata:** `chunk_size` phải trong `[1 B, 3 MB]` (một chunk phải vừa một message gRPC ≤ 4 MB), không chia sẻ file rỗng, file ≤ 4 GB, số chunk ≤ 1.000.000 (tính từ kích thước file **trước khi băm**, không tốn RAM/CPU cho file bị từ chối). CLI: `python src/metadata.py <file> <tracker> --chunk-size <bytes>`.
- **Kiểm tra khi đọc metadata** (`TorrentMetadata.load`, mọi peer gọi trước khi tạo file): đủ 8 trường bắt buộc; `num_chunks = ⌈file_size / chunk_size⌉`; đúng `num_chunks` mã băm, mỗi mã là SHA-256 hex 64 ký tự; `file_name` chỉ là tên file (không chứa `/`, `\`, `..` → chống path traversal); tracker là URL `http(s)`. Sai → `MetadataError`, peer từ chối khởi động.

### 2.3. Piece Manager & Storage (`src/peer/chunk_manager.py`)
- Trạng thái từng chunk: `MISSING → DOWNLOADING → DONE` (lỗi thì quay về `MISSING`).
- Leecher cấp phát trước file rỗng đúng kích thước; chunk được ghi đúng vị trí `index × chunk_size` → ghép file **đúng thứ tự** dù chunk đến lộn xộn.
- `write_chunk_atomic()`: **verify SHA-256 trước**, sai thì từ chối không ghi; đúng thì ghi dưới `asyncio.Lock()`.
- **Resume:** khi khởi động lại trên file đã có, quét và verify từng chunk; chunk đúng hash được đánh dấu `DONE`, chỉ tải tiếp phần còn thiếu.
- **Seeder phải chứng minh sở hữu file:** khi chạy `--seed`, mọi chunk của file nguồn được băm và so với metadata. Sai kích thước hoặc có chunk sai hash → `SourceFileError`, seeder từ chối khởi động (không phát dữ liệu sai ra mạng).
- `drop_corrupted_chunks()`: khi hash toàn file sai, quét lại các chunk trên đĩa, chunk nào hỏng thì đưa về `MISSING` để tải lại.

### 2.4. Peer Server (`src/peer/peer_servicer.py`)
- gRPC `PeerService` (định nghĩa trong `src/common/peer.proto`):

| RPC | Message | Mô tả |
|---|---|---|
| `GetChunk(ChunkRequest) → ChunkResponse` | REQUEST_CHUNK / CHUNK_DATA / ERROR | Trả dữ liệu chunk; từ chối nếu `file_id` lạ hoặc chunk chưa có |
| `Handshake(HandshakeRequest) → HandshakeResponse` | HANDSHAKE | Kiểm tra hai peer cùng `file_id` |
| `GetBitfield(BitfieldRequest) → BitfieldResponse` | BITFIELD | Trả danh sách chunk đang có |

- Log `[UPLOAD]` kèm tiến độ của chính peer (`own 20/32 chunks - STILL DOWNLOADING`) → bằng chứng peer upload khi chưa tải xong.
- **Giới hạn băng thông upload** (`--upload-limit KB/s`): một đường upload duy nhất, mỗi chunk chiếm `len / rate` giây.
- **Fault injection** (`--corrupt`, chỉ dùng cho demo): trả dữ liệu ngẫu nhiên thay cho chunk thật.

### 2.5. Peer Client – Download Engine (`src/peer/peer_node.py`)
- Một phiên HTTP dùng chung (`httpx.AsyncClient`) cho mọi lời gọi tracker.
- **Đăng ký file có retry:** seeder thử `REGISTER_FILE` tối đa 10 lần, backoff 0,5s → 1s → 2s → 4s → 5s; không đăng ký được thì dừng với thông báo lỗi.
- **Tự đăng ký lại:** nếu tracker trả 404 cho announce (tracker vừa khởi động lại, mất trạng thái trong bộ nhớ), peer gửi lại `REGISTER_FILE` từ metadata của mình rồi announce lại.
- Ba vòng lặp bất đồng bộ chạy song song với gRPC server:
  - `tracker_announce_loop`: announce mỗi `HEARTBEAT_INTERVAL = 10s` (đồng thời là heartbeat gia hạn lease).
  - `fetch_peers_loop`: lấy danh sách peer mỗi `FETCH_PEERS_INTERVAL = 4s`, log `[DISCOVERY]` khi có peer mới, `[CHURN]` khi peer biến mất.
  - `download_manager_loop`: chọn và tải chunk (mục 4).
- Khi một chunk verify xong → announce ngay lên tracker (`[HAVE]`) để peer khác thấy chunk đó sớm, không chờ chu kỳ 10s.

---

## 3. Định danh và Khám phá

| Đối tượng | Định danh | Ghi chú |
|---|---|---|
| File | `file_id` = SHA-256 toàn file | Định danh theo nội dung |
| Chunk | `(file_id, chunk_index)` | Kiểm tra bằng `chunk_hashes[index]` |
| Peer | `peer_id` = `host:port` | Duy nhất trong mạng vì mỗi peer lắng nghe một địa chỉ riêng |

**Peer discovery:** peer chỉ cần metadata (chứa URL tracker) → `announce` để xuất hiện trong danh sách → `GET /peers` để biết ai đang có chunk nào. Bitfield của các peer được lấy từ tracker (eventual consistency, trễ tối đa ~4s cộng thời gian announce).

---

## 4. Thuật toán Tải

1. Đọc metadata → announce → lấy danh sách peer và bitfield từ tracker.
2. Danh sách chunk ứng viên = chunk **chưa có**, **không đang tải**, và **đã hết thời gian chờ retry**.
3. **Rarest-first:** đếm số peer đang có mỗi chunk, ưu tiên chunk hiếm nhất (xáo trộn ngẫu nhiên trước khi sắp xếp để các leecher không cùng tải một thứ tự).
4. **Chọn nguồn** cho mỗi chunk (`_choose_peer`):
   - chỉ chọn peer có chunk đó và có điểm phạt `< BAD_PEER_THRESHOLD = 5`;
   - ưu tiên peer **chưa từng lỗi** với chunk này (failover);
   - trong đó chọn peer **đang có ít request nhất** (cân bằng tải), rồi điểm phạt thấp nhất, rồi ngẫu nhiên.
   - Bộ đếm `inflight_per_peer` được **tăng ngay lúc giao chunk** (trước khi task tải bắt đầu chạy) và giảm khi task kết thúc/hủy, nên các lần chọn nguồn trong cùng một đợt lập lịch thấy tải mới nhất → 5 chunk đầu với 2 nguồn luôn chia 3/2, với 3 nguồn chia 2/2/1.
5. **Giới hạn đồng thời:** tối đa `MAX_CONCURRENT_DOWNLOADS = 5` chunk đang tải cùng lúc.
6. Nhận chunk → verify SHA-256 → ghi đĩa có lock → cập nhật bitfield → `[HAVE]` lên tracker.
7. Lỗi → đánh dấu `MISSING`, cộng điểm phạt cho nguồn, **retry có giới hạn** (mục 6).
8. Đủ chunk → verify hash toàn file → log `[VERIFY]` và `[SUMMARY]` (số chunk nhận từ mỗi nguồn); peer tiếp tục chạy như seeder.
   - Bước verify chạy **cả khi file đã đủ chunk ngay từ đầu** (seeder, hoặc resume đã đủ).
   - Kết quả ghi vào `download_ok`: `True` = file khớp `file_hash`; `False` = **tải thất bại**.
   - Hash toàn file sai → quét lại chunk trên đĩa, tải lại chunk hỏng (tối đa 2 lần); nếu mọi chunk đều đúng mà hash toàn file vẫn sai thì metadata không nhất quán → báo `[VERIFY] FAILURE ... Download marked FAILED`.
   - **Tải thất bại thì peer rời swarm:** `run()` trả về `False`, gửi `/leave` cho tracker, tắt gRPC server (không chia sẻ file chưa xác minh được), CLI thoát với mã lỗi `2`.

---

## 5. Đồng thời & Nhất quán

| Tình huống xung đột | Cơ chế kiểm soát |
|---|---|
| Nhiều task cùng tải một chunk | Chunk đang tải nằm trong `download_tasks`, không được giao lần thứ hai cho tới khi task kết thúc |
| Nhiều chunk cùng ghi xuống một file | `asyncio.Lock()` trong `write_chunk_atomic()`; mỗi chunk ghi vào vùng offset riêng |
| Một seeder bị nhiều peer yêu cầu cùng lúc | gRPC aio server xử lý đồng thời; giới hạn băng thông upload xếp hàng các lần gửi |
| Tracker nhận announce đồng thời từ nhiều peer | `asyncio.Lock()` bao toàn bộ thao tác đọc/ghi trạng thái tracker |

**Mô hình nhất quán:**
- **Eventual consistency** cho danh sách peer và bitfield trên tracker: thông tin có thể trễ vài giây; nếu peer bị hỏi chunk mà nó không có, nó trả lỗi và leecher chọn nguồn khác.
- **Strong integrity** cho dữ liệu: không chunk nào được ghi nếu sai SHA-256; file cuối phải khớp `file_hash`.

**Kiểm chứng:** `tests/test_concurrency.py` (20 task ghi đồng thời, 3 leecher cùng tải từ 1 seeder), `tests/test_engine.py`.

---

## 6. Mô hình Lỗi & Cơ chế Phục hồi

**Failure model:** peer crash-stop; mất/treo kết nối; peer gửi dữ liệu sai (lỗi hoặc cố ý); tracker được giả định không lỗi trong phạm vi đề tài.

| Lỗi | Phát hiện | Xử lý |
|---|---|---|
| Source peer chết (crash-stop) | gRPC `UNAVAILABLE` ngay khi kết nối bị từ chối/reset (đo được 4 ms trên Linux, ~0,5 s trên Windows) → log `[PEER DOWN]` | Phạt +1, chunk về `MISSING`, retry từ peer khác (`[FAILOVER]`) |
| Peer treo, không trả lời | Hết `GRPC_TIMEOUT = 5s` → `[TIMEOUT]` | Như trên |
| Peer từ chối chunk | `success = false` → `[REJECTED]` | Phạt +1, chọn nguồn khác |
| Chunk sai hash | Verify SHA-256 → `[HASH MISMATCH]`, `[INTEGRITY FAULT]` | Không ghi đĩa, phạt +2, tải lại từ peer khác; peer bị loại khi điểm phạt ≥ 5 |
| Peer chết không báo tracker | Hết lease `PEER_TIMEOUT = 60s` | Tracker xóa peer khỏi danh sách; leecher log `[CHURN]` |
| Leecher bị tắt giữa chừng | – | Khởi động lại: quét verify chunk trên đĩa, chỉ tải tiếp phần thiếu (`[RESUME]`) |
| Chunk hỏng trên đĩa sau khi đã ghi | Hash toàn file sai khi `[VERIFY]` | Quét lại, chunk hỏng về `MISSING`, tải lại (tối đa 2 lần) |
| Seeder có file nguồn sai | Băm toàn bộ chunk lúc khởi động | Seeder từ chối chạy (`SourceFileError`) |
| Tracker khởi động chậm hơn seeder | Đăng ký thất bại / không kết nối được | Retry đăng ký có backoff; Docker: healthcheck `/health`, peer chỉ khởi động khi tracker healthy |
| Tracker khởi động lại (mất trạng thái) | Announce nhận 404 | Peer tự đăng ký lại file rồi announce lại |

**Retry có giới hạn:** mỗi chunk tối đa `MAX_RETRY_PER_CHUNK = 3` lần với backoff lũy thừa `0,2s → 0,4s` (tối đa 2s); hết 3 lần thì nghỉ `5s` rồi thử lại từ bất kỳ peer nào. Tải thành công từ một peer đưa điểm phạt của peer đó về 0.

---

## 7. Bảo mật

| Yêu cầu | Cách đáp ứng |
|---|---|
| Xác thực | Mọi API tracker (trừ `/health`) yêu cầu `X-API-Key`; sai/thiếu → HTTP 401. So sánh bằng `hmac.compare_digest` (thời gian hằng). Tracker chưa cấu hình key → từ chối **mọi** request (fail closed) |
| Secret không nằm trong repo | Key **chỉ** đọc từ biến môi trường / file `.env` (bị `.gitignore` và `.dockerignore` chặn), **không có giá trị mặc định trong mã**; repo chỉ có `.env.example`. Thiếu key → tracker và peer từ chối khởi động |
| Không lộ secret qua log | Log xác thực chỉ ghi lý do và địa chỉ client (`[AUTH] Rejected request from ... invalid X-API-Key`), không bao giờ ghi giá trị key |
| Validate input | Pydantic: `file_id` 8–128 ký tự, `size ≥ 1`, `chunk_count ≥ 1`, `port` 1–65535, sai kiểu → 422; tracker lọc chunk index ngoài `[0, chunk_count)`; peer từ chối `file_id` lạ và chunk index không có; metadata được kiểm tra đầy đủ khi sinh và khi đọc (mục 2.2) |
| Giới hạn kích thước | Chunk ≤ 3 MB, file ≤ 4 GB (kiểm tra ở metadata); gRPC chỉ nhận message ≤ 4 MB (vượt → `RESOURCE_EXHAUSTED`); số tải đồng thời ≤ 5 |
| Toàn vẹn dữ liệu | SHA-256 từng chunk và toàn file |
| Log hoạt động | Đăng ký, announce, chunk lỗi, peer bị từ chối/phạt đều được log |

Kiểm chứng: `tests/test_security.py` (5 test), `tests/test_validation.py` (5 test), `tests/test_resilience.py` (5 test: giới hạn số chunk, cân bằng tải trong một đợt lập lịch, peer dừng khi tải thất bại, retry đăng ký khi tracker khởi động chậm, đăng ký lại sau khi tracker restart).

---

## 8. Log & Quan sát

Mỗi dòng log có **thời gian, tên node (peer_id), mức độ và loại sự kiện**, ví dụ:
```
2026-10-19 21:05:55,773 [peer[127.0.0.1:6886]] [WARNING] [PEER DOWN] Cannot reach 127.0.0.1:6884 for chunk 6 ...
```

| Sự kiện | Ý nghĩa |
|---|---|
| `[REGISTER]` `[HAVE]` `[LEAVE]` | Giao tiếp với tracker |
| `[DISCOVERY]` `[CHURN]` | Peer tham gia / rời swarm |
| `[REQUEST]` `[RECEIVED]` `[UPLOAD]` | Truyền chunk giữa các peer |
| `[PEER DOWN]` `[TIMEOUT]` `[REJECTED]` `[HASH MISMATCH]` `[INTEGRITY FAULT]` | Phát hiện lỗi |
| `[RETRY]` `[FAILOVER]` `[RESUME]` | Phục hồi |
| `[SUMMARY]` `[VERIFY]` | Kết quả tải |

---

## 9. Triển khai

| Cách | Lệnh | Ghi chú |
|---|---|---|
| Demo tự động | `python scripts/demo_all.py`, `python scripts/demo_fault_injection.py` | Mỗi node một tiến trình, cổng 8000 và 6881–6889, in bảng bằng chứng |
| Chạy tay | `python src/tracker.py`, `python src/peer_node.py ...` | Xem README |
| Docker Compose | `docker compose -f deploy/docker-compose.yml up --build` | 4 container trên mạng `p2p_net`; tracker có healthcheck, peer chỉ khởi động khi tracker healthy; chỉ metadata dùng volume chung, dữ liệu mỗi node ở volume riêng |

---

## 10. Hạn chế & Hướng phát triển

- Tracker tập trung là điểm lỗi đơn (single point of failure) → có thể mở rộng thành replicated tracker hoặc DHT (Kademlia).
- Leecher lấy bitfield từ tracker theo chu kỳ; RPC `Handshake` / `GetBitfield` đã có ở phía server nhưng client chưa dùng để trao đổi bitfield trực tiếp giữa các peer.
- Chưa có choking/unchoking (tit-for-tat) và chưa mã hóa kênh truyền (TLS/mTLS).
- Kết nối gRPC giữa các peer chưa xác thực; chỉ tracker yêu cầu API key.
