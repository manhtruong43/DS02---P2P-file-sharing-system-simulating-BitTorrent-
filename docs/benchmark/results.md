# Kết quả đo hiệu năng DS02

- Thời điểm đo: 2026-10-13 17:51:04
- Máy: Windows 11 | Python 3.13.2 | 16 CPU
- File thử nghiệm: 8 MB dữ liệu ngẫu nhiên; mỗi cấu hình chạy 3 lần, lấy trung bình
- Giới hạn tải đồng thời của leecher: 5 chunk
- Băng thông upload mô phỏng: 2 MB/s mỗi seeder
- Mỗi seeder là một tiến trình riêng; giao tiếp gRPC qua loopback (127.0.0.1)
- Không có Tracker trong thí nghiệm (danh sách peer cho trước), announce được tắt

## Thí nghiệm A – Theo số peer nguồn (chunk 256 KB)

| Số peer nguồn | Thời gian TB (s) | Min – Max (s) | Throughput TB (MB/s) | Speedup |
|:---:|:---:|:---:|:---:|:---:|
| 1 | 4.433 | 4.413 – 4.444 | 1.80 | 1.00x |
| 2 | 2.208 | 2.176 – 2.233 | 3.62 | 2.01x |
| 3 | 1.537 | 1.518 – 1.548 | 5.21 | 2.88x |
| 5 | 1.046 | 1.016 – 1.079 | 7.65 | 4.24x |
| 10 | 1.051 | 1.042 – 1.069 | 7.61 | 4.22x |

## Thí nghiệm B – Theo kích thước chunk (3 peer nguồn)

| Chunk size | Số chunk | Thời gian TB (s) | Min – Max (s) | Throughput TB (MB/s) |
|:---:|:---:|:---:|:---:|:---:|
| 16 KB | 512 | 3.360 | 3.316 – 3.401 | 2.38 |
| 64 KB | 128 | 1.983 | 1.967 – 1.998 | 4.03 |
| 256 KB | 32 | 1.540 | 1.533 – 1.546 | 5.20 |
| 1024 KB | 8 | 1.561 | 1.557 – 1.566 | 5.12 |

> Lưu ý khi phân tích: các tiến trình cùng chạy trên một máy qua loopback. Giới hạn băng thông
> upload được mô phỏng bằng phần mềm (sleep theo số byte gửi), không phải đo mạng vật lý.
> Speedup bị chặn trên bởi giới hạn tải đồng thời của leecher (5 chunk).
