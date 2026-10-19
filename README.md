# DS02 – Hệ Thống Chia Sẻ File P2P Mô Phỏng BitTorrent

> **Môn học:** Distributed Systems (Hệ phân tán) – Course Project  
> **Mã đề tài:** DS02  
> **Khái niệm trọng tâm:** P2P, Tracker, Chunking & Replication, Parallel Downloading, Concurrency Lock, Fault Tolerance & Churn.  
> **Công nghệ:** Python 3.11+, FastAPI (Tracker HTTP/REST), gRPC & Protocol Buffers (Peer Data Stream), AsyncIO, Docker.

---

## 👥 Bảng Phân Công Công Việc (Task Assignment Table)

*(Theo yêu cầu Mục 13 – Mỗi thành viên phụ trách một phần việc cụ thể và có commit đóng góp)*

| STT | Thành viên | MSSV | Phần phụ trách | Module / Thư mục |
|:---:|---|---|---|---|
| 1 | **Lỗ Mạnh Trường** | *(Điền MSSV)* | Tracker Server, API Authentication, Metadata Generator, Protocol Buffers | `src/tracker/`, `src/common/` |
| 2 | **Bùi Trung Kiên** | *(Điền MSSV)* | Peer Engine, Download Manager, Rarest-First, Piece Manager, Atomic I/O Lock | `src/peer/` |
| 3 | **Phạm Nguyễn Dũng** | *(Điền MSSV)* | Bộ kiểm thử tự động (15 tests), Kịch bản Demo lỗi & đo hiệu năng, Docker Deploy | `tests/`, `scripts/`, `deploy/`, `docs/` |

---

## 🏛️ Kiến Trúc Hệ Thống

```mermaid
graph TD
    subgraph Tracker Layer
        T[Tracker Server<br/>FastAPI HTTP:8000<br/>Metadata & Peer Registry]
    end

    subgraph P2P Swarm Layer
        A[Peer A - Seeder<br/>gRPC:50051<br/>100% Chunks]
        B[Peer B - Leecher/Seeder<br/>gRPC:50052<br/>Vừa tải vừa upload]
        C[Peer C - Leecher<br/>gRPC:50053<br/>Tải song song đa nguồn]
    end

    A -->|1. Register File & Announce| T
    B -->|2. Announce & Get Peers| T
    C -->|3. Announce & Get Peers| T

    A -.->|Chunk Transfer gRPC| B
    A -.->|Chunk Transfer gRPC| C
    B -.->|Immediate HAVE & Chunk Transfer| C
```

> ⚠️ **Cam kết nguyên tắc BitTorrent:** Tracker **chỉ quản lý metadata và danh sách peer**; Tracker **tuyệt đối KHÔNG lưu trữ và KHÔNG cung cấp nội dung file**.

---

## 📂 Cấu Trúc Thư Mục Repository

```text
DS02_BitTorrent/
├── README.md                           # Tài liệu tổng quan, bảng phân công & hướng dẫn
├── requirements.txt                    # Danh sách thư viện phụ thuộc
├── .env.example                        # Template biến môi trường
├── .gitignore                          # Loại trừ secret .env, cache, file test tạm
├── src/
│   ├── common/                         # Dùng chung: config, hashing SHA-256, metadata, protobuf
│   │   ├── config.py
│   │   ├── hashing.py
│   │   ├── metadata.py
│   │   ├── peer.proto
│   │   ├── peer_pb2.py
│   │   └── peer_pb2_grpc.py
│   ├── tracker/                        # Tracker Server
│   │   └── tracker.py
│   └── peer/                           # Peer Node
│       ├── chunk_manager.py            # Piece Manager & Atomic Disk I/O Lock
│       ├── peer_servicer.py            # gRPC Server phục vụ chunk
│       └── peer_node.py                # Download engine, Rarest-first, Immediate HAVE
├── deploy/                             # Triển khai hệ thống
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── README.md
├── tests/                              # Bộ kiểm thử tự động (18 tests)
│   ├── test_functional.py              # 8 functional tests
│   ├── test_concurrency.py             # 2 concurrency tests
│   ├── test_fault_recovery.py          # 2 fault & recovery tests
│   ├── test_security.py                # 5 security / invalid-data tests
│   └── test_performance.py            # 1 performance scalability test
├── scripts/                            # Kịch bản thực thi & đo đạc
│   ├── demo_all.py                     # Demo đầy đủ luồng tải song song & chia sẻ tức thì
│   ├── demo_fault_injection.py         # Demo tiêm lỗi: Crash node & Chunk giả mạo
│   └── benchmark_performance.py       # Đo thời gian/throughput theo số peer & chunk size, xuất biểu đồ
└── docs/                               # Tài liệu học thuật & báo cáo
    ├── ARCHITECTURE.md                 # Thiết kế kiến trúc, giao thức, mô hình lỗi & đồng thời
    ├── REPORT_TEMPLATE.md              # Khung báo cáo kỹ thuật 19 mục chuẩn
    └── benchmark/                      # Kết quả đo hiệu năng (CSV, biểu đồ PNG, results.md)
```

---

## 🚀 Hướng Dẫn Cài Đặt & Chạy Trên Máy Sạch (Redeploy Guide)

### 1. Chuẩn bị môi trường
- Python 3.10 trở lên.
- Cài đặt các thư viện cần thiết:
```bash
pip install -r requirements.txt
```

### 2. Thiết lập biến môi trường
```bash
# Trên Windows PowerShell:
Copy-Item .env.example .env

# Trên Linux / macOS:
cp .env.example .env
```
Mở file `.env` và đổi `TRACKER_API_KEY` thành chuỗi bí mật của riêng bạn (không commit file `.env`):
```bash
python -c "import secrets; print(secrets.token_hex(16))"
```

---

## 🎬 7 Kịch Bản Demo Bắt Buộc (Mục 9)

### Cách 1: Chạy Tự Động Toàn Diện (Khuyến nghị cho buổi Demo)

1. **Demo Hoạt Động Bình Thường (Kịch bản 1, 2, 3, 7):**
   ```bash
   python scripts/demo_all.py
   ```
   *Minh chứng:*
   - Seeder đăng ký file với Tracker.
   - Leecher 1 tải từ Seeder và **ngay lập tức gửi thông báo HAVE** chia sẻ chunk cho Leecher 2.
   - Leecher 2 tải song song từ cả Seeder và Leecher 1.
   - Xác thực mã băm SHA-256 toàn bộ file tải về trùng khớp 100% với file gốc.

2. **Demo Xử Lý Lỗi Live & Phục Hồi (Kịch bản 4, 5, 6):**
   ```bash
   python scripts/demo_fault_injection.py
   ```
   *Minh chứng:*
   - **Tắt đột ngột Seeder 1:** Leecher phát hiện timeout/refusal, phạt điểm bad_peer và tự động failover sang Seeder 2.
   - **Tiêm Chunk giả mạo/sai hash:** Leecher phát hiện mã băm không khớp, từ chối ghi đĩa, phạt điểm nút gửi và tải lại từ nút tin cậy.

3. **Chạy Thí Nghiệm Hiệu Năng & Đo Đạc Thông Lượng:**
   ```bash
   python scripts/benchmark_performance.py           # đầy đủ (~1 phút)
   python scripts/benchmark_performance.py --quick   # chạy nhanh để thử
   ```
   Kết quả (CSV, biểu đồ PNG, bảng Markdown) được lưu trong `docs/benchmark/`.

---

### Cách 2: Chạy Từng Tiến Trình Bằng Tay Trong Nhiều Terminal

**Terminal 1: Khởi động Tracker Server**
```bash
python src/tracker.py --port 8000
```

**Terminal 2: Tạo dữ liệu mẫu & metadata**
```bash
# Tạo file mẫu 2 MB
python -c "import os; open('demo.bin', 'wb').write(os.urandom(2*1024*1024))"

# Sinh metadata .torrent.json
python src/metadata.py demo.bin http://127.0.0.1:8000
```

**Terminal 3: Khởi động Seeder (Node 1)**
```bash
python src/peer_node.py --port 50051 --metadata demo.bin.torrent.json --output demo.bin --seed
```

**Terminal 4: Khởi động Leecher 1 (Node 2)**
```bash
python src/peer_node.py --port 50052 --metadata demo.bin.torrent.json --output download_1.bin
```

**Terminal 5: Khởi động Leecher 2 (Node 3)**
```bash
python src/peer_node.py --port 50053 --metadata demo.bin.torrent.json --output download_2.bin
```

---

## 🧪 Kiểm Thử Tự Động (Automated Testing Suite)

Hệ thống đi kèm **18 bài kiểm thử tự động** thỏa mãn đầy đủ yêu cầu Mục 11:

```bash
# Chạy toàn bộ tests:
pytest tests/ -v
```

### Thống kê bài kiểm thử:
- **Functional Tests (8 bài):** Băm file/chunk, chia/ghép file, đăng ký tracker, announce & lấy peer, timeout TTL, rời mạng graceful, tải từ 1 seeder, tải song song đa nguồn.
- **Concurrency Tests (2 bài):** Khóa `asyncio.Lock()` khi nhiều luồng cùng ghi đĩa; Seeder phục vụ nhiều peer đồng thời.
- **Fault & Recovery Tests (2 bài):** Chịu lỗi peer sập nguồn giữa chừng (failover); ngắt tiến trình và khôi phục tiếp tục tải (Resume).
- **Security & Data Validation Tests (5 bài):** Chặn truy cập không có/sai `X-API-Key`; bắt lỗi chunk sai hash SHA-256; gRPC từ chối chunk index ngoài phạm vi và file_id lạ; tracker từ chối announce sai định dạng và lọc chunk index không hợp lệ; từ chối message vượt giới hạn 4 MB.
- **Performance Test (1 bài):** Đo thời gian và thông lượng giữa tải đơn nguồn và tải đa nguồn.

---

## 🐳 Triển Khai Với Docker Compose

Khởi động toàn bộ mạng P2P cô lập gồm 1 Tracker, 1 Seeder và 2 Leecher chỉ với 1 lệnh (cần tạo file `.env` trước):
```bash
docker compose -f deploy/docker-compose.yml up --build
```
Kiểm tra trạng thái mạng P2P từ máy host (thay `<API_KEY>` bằng giá trị trong `.env`):
```bash
curl -H "X-API-Key: <API_KEY>" http://localhost:8000/stats
```
Dọn dẹp container:
```bash
docker compose -f deploy/docker-compose.yml down -v
```
Chi tiết xem `deploy/README.md`.
