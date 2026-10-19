# Triển khai hệ thống với Docker & Docker Compose

Thư mục này chứa cấu hình đóng gói toàn bộ hệ thống phân tán P2P BitTorrent vào Docker containers.
Mỗi node chạy trong một container riêng, có hostname riêng và chỉ giao tiếp với nhau qua mạng `p2p_net`.

## Cấu trúc các dịch vụ

| Dịch vụ | Container Name | Vai trò | Port |
|---|---|---|---|
| `tracker` | `bittorrent_tracker` | Tracker Server quản lý metadata và danh sách peer (không lưu nội dung file) | `8000` |
| `seeder` | `bittorrent_seeder` | Seeder ban đầu, tạo file 2 MB (8 chunk × 256 KB) và nắm giữ 100% chunk | `50051` |
| `leecher1` | `bittorrent_leecher_1` | Peer tải file từ Seeder và chia sẻ lại | `50052` |
| `leecher2` | `bittorrent_leecher_2` | Peer tải file song song từ Seeder và Leecher 1 | `50053` |

## Volume (lưu trữ)

| Volume | Gắn vào | Nội dung |
|---|---|---|
| `shared_meta` | tất cả node (leecher chỉ đọc) | **Chỉ** file metadata `.torrent.json` (tương đương việc tải file `.torrent` về) |
| `seeder_data` | `seeder` | File gốc của Seeder |
| `leecher1_data` | `leecher1` | File Leecher 1 tải về |
| `leecher2_data` | `leecher2` | File Leecher 2 tải về |

Dữ liệu file của mỗi node nằm trong volume riêng, nên nội dung file **chỉ có thể** đi giữa các container qua gRPC,
đúng yêu cầu "không dùng shared file thay cho giao tiếp mạng".

## Hướng dẫn chạy

Tất cả lệnh chạy từ **thư mục gốc của repository**.

1. **Tạo file `.env` (bắt buộc, chứa API key bí mật):**
   ```bash
   # Windows PowerShell
   Copy-Item .env.example .env
   # Linux / macOS
   cp .env.example .env
   ```
   Sau đó mở `.env` và đổi `TRACKER_API_KEY` thành một chuỗi ngẫu nhiên:
   ```bash
   python -c "import secrets; print(secrets.token_hex(16))"
   ```

2. **Khởi động toàn bộ mạng P2P:**
   ```bash
   docker compose -f deploy/docker-compose.yml up --build
   ```

3. **Kiểm tra trạng thái mạng P2P từ máy host** (thay `<API_KEY>` bằng giá trị trong `.env`):
   ```bash
   curl -H "X-API-Key: <API_KEY>" http://localhost:8000/stats
   ```

4. **Xem log trực tiếp của một peer:**
   ```bash
   docker logs -f bittorrent_leecher_1
   ```

5. **Kiểm tra hash file đã tải trùng với file gốc:**
   ```bash
   docker exec bittorrent_seeder sha256sum /data/test_data.bin
   docker exec bittorrent_leecher_1 sha256sum /data/download_1.bin
   docker exec bittorrent_leecher_2 sha256sum /data/download_2.bin
   ```

6. **Dừng và dọn dẹp hệ thống (xóa cả volume):**
   ```bash
   docker compose -f deploy/docker-compose.yml down -v
   ```
