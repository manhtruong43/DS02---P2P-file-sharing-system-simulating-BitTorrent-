# KHUNG BÁO CÁO KỸ THUẬT ĐỀ TÀI DS02
## HỆ THỐNG CHIA SẺ FILE P2P MÔ PHỎNG BITTORRENT
*(Cấu trúc chuẩn theo Mục 14 Course Project Specification)*

---

### 1. Trang bìa
- Tên trường, khoa, môn học: **Distributed Systems (Hệ phân tán)**
- Tên đề tài: **DS02 – P2P File Sharing System Simulating BitTorrent**
- Giảng viên hướng dẫn: [Tên Giảng Viên]
- Danh sách sinh viên thực hiện: Họ tên, MSSV, Lớp.

### 2. Thông tin nhóm và Phân công công việc
*(Bảng phân công chi tiết theo Mục 13)*

| STT | Thành viên | MSSV | Phần phụ trách | Thư mục code | Commit Hash mẫu |
|---|---|---|---|---|---|
| 1 | **Lỗ Mạnh Trường** | *(Điền MSSV)* | Tracker Server, API Authentication, Metadata Generator, Protocol Buffers | `src/tracker/`, `src/common/` | `426b36e`, `d20297b` |
| 2 | **Bùi Trung Kiên** | *(Điền MSSV)* | Peer Node, Download Engine, Rarest-First, Piece Manager, Atomic I/O Lock | `src/peer/` | `d20297b`, ... |
| 3 | **Phạm Nguyễn Dũng** | *(Điền MSSV)* | Test Suite (15 tests), Kịch bản Demo lỗi & đo hiệu năng, Docker Deploy | `tests/`, `scripts/`, `deploy/` | `d20297b`, ... |

### 3. Tóm tắt (Abstract)
Tóm tắt bài toán chia sẻ file P2P, kiến trúc tracker kết hợp peer-to-peer gRPC, các kỹ thuật chia nhỏ dữ liệu (chunking), tải song song đa nguồn (parallel downloading), kiểm tra toàn vẹn băm SHA-256 và cơ chế chịu lỗi khi có node rời mạng (churn).

### 4. Mô tả bài toán và Yêu cầu
- Động lực của hệ thống P2P so với kiến trúc Client-Server truyền thống.
- Yêu cầu chức năng: chia nhỏ file, metadata, phát hiện peer, tải song song, ghép file và kiểm tra toàn vẹn.
- Yêu cầu phi chức năng: tính phân tán, khả năng chịu lỗi, tính nhất quán và bảo mật.

### 5. Các vấn đề của Hệ phân tán trong đề tài DS02
- Phân tích vì sao Tracker **tuyệt đối không giữ nội dung file**.
- Vấn đề định danh (Identity) và khám phá nút (Peer Discovery).
- Vấn đề không đồng bộ thời gian và độ trễ mạng.
- Xung đột ghi đồng thời (Concurrency conflict) khi tải nhiều mảnh cùng lúc.
- Hiện tượng nút mạng tham gia/rời mạng ngẫu nhiên (Churn).

### 6. Phân tích và Lựa chọn giải pháp công nghệ
- So sánh Tracker tập trung vs DHT (Distributed Hash Table).
- Lựa chọn giao thức: HTTP/REST cho Tracker (FastAPI) + gRPC/HTTP2 cho Peer transfer.
- Lựa chọn thuật toán băm: SHA-256 (băm từng chunk và băm toàn file).
- Phân tích kích thước chunk size (256 KB vs 64 KB vs 1 MB) và tác động đến hiệu năng/overhead.

### 7. Kiến trúc Hệ thống
- Sơ đồ kiến trúc phân tầng (Architecture Diagram).
- Luồng dữ liệu (Data Flow) giữa Seeder, Tracker và các Leecher.

### 8. Thiết kế Thành phần và Giao thức Giao tiếp
- Chi tiết API Tracker: `/register`, `/announce`, `/peers/{file_id}`, `/leave`, `/heartbeat`, `/stats`.
- Chi tiết Protocol Buffers message: `GetChunk`, `Handshake`, `GetBitfield`.
- Đặc tả cấu trúc file `.torrent.json`.

### 9. Định danh và Khám phá (Peer Discovery)
- Cách sinh `peer_id` và `file_id`.
- Cơ chế polling và cơ chế Immediate HAVE/Announce khi có chunk mới.

### 10. Đồng thời, Đồng bộ và Nhất quán
- Xung đột nhiều luồng cùng ghi đĩa -> Giải pháp: `asyncio.Lock()` bảo vệ I/O.
- Mô hình nhất quán: **Eventual Consistency** trên Tracker và **Strong Integrity** trên Peer chunks.

### 11. Chịu lỗi và Phục hồi (Fault Tolerance & Recovery)
- Failure model: Crash-stop, rớt gói mạng, Byzantine chunk (chunk sai hash).
- Cơ chế giải quyết: Timeout, bounded retry, blacklist bad peer, resume download từ các chunk đã xác thực.

### 12. Bảo mật
- Xác thực API Key với header `X-API-Key`.
- Quản lý biến môi trường an toàn (`.env` và `.env.example`).
- Input validation: chống tràn số chunk, chống path traversal.

### 13. Triển khai Hệ thống (Implementation)
- Cấu trúc module mã nguồn: `src/common/`, `src/tracker/`, `src/peer/`.
- Container hóa với Docker và Docker Compose.

### 14. Kiểm thử và Kết quả (Testing)
- Thống kê 15 bài kiểm thử tự động `pytest`:
  - 8 Functional tests
  - 2 Concurrency tests
  - 2 Fault & Recovery tests
  - 2 Security tests
  - 1 Performance test

### 15. Đánh giá Hiệu năng (Performance Evaluation)
- Bảng số liệu đo đạc thực tế: Tải với 1 peer vs 2 peers vs 3 peers.
- Phân tích Speedup và Throughput (KB/s).

### 16. Hạn chế và Hướng phát triển
- Hướng phát triển: Choking/Unchoking (Tit-for-Tat), tích hợp DHT Kademlia, hỗ trợ mã hóa TLS/mTLS.

### 17. Kết luận
Tổng kết các kết quả đạt được đối chiếu với mục tiêu ban đầu.

### 18. Tài liệu tham khảo
- BitTorrent Protocol Specification (BEP 0003).
- Giáo trình Hệ phân tán (Distributed Systems: Principles and Paradigms - Tanenbaum).
- Tài liệu gRPC, FastAPI, AsyncIO.

### 19. Phụ lục
- Hướng dẫn cài đặt và chạy demo từng bước.
- Log chi tiết các kịch bản demo.
