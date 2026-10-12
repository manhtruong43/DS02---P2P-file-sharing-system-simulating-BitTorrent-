# Triển khai hệ thống với Docker & Docker Compose

Thư mục này chứa cấu hình đóng gói toàn bộ hệ thống phân tán P2P BitTorrent vào Docker containers.

## Cấu trúc các dịch vụ

| Dịch vụ | Container Name | Vai trò | Port |
|---|---|---|---|
| `tracker` | `bittorrent_tracker` | Tracker Server quản lý danh sách peers và metadata | `8000` |
| `seeder` | `bittorrent_seeder` | Seeder ban đầu nắm giữ 100% file gốc | `50051` |
| `leecher1` | `bittorrent_leecher_1` | Peer tải file từ Seeder và chia sẻ lại | `50052` |
| `leecher2` | `bittorrent_leecher_2` | Peer tải file song song từ Seeder và Leecher 1 | `50053` |

## Hướng dẫn chạy

1. **Khởi động toàn bộ mạng P2P:**
   ```bash
   docker-compose -f deploy/docker-compose.yml up --build
   ```

2. **Kiểm tra trạng thái mạng P2P từ máy chủ host:**
   ```bash
   curl -H "X-API-Key: ds02_secret_key_123" http://localhost:8000/stats
   ```

3. **Xem log trực tiếp của một peer:**
   ```bash
   docker logs -f bittorrent_leecher_1
   ```

4. **Dừng và dọn dẹp hệ thống:**
   ```bash
   docker-compose -f deploy/docker-compose.yml down -v
   ```
