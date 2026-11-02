# Kết quả đo hiệu năng DS02

- Thời điểm đo: 2026-11-02 23:00:24
- Máy: Windows 11 | Python 3.13.2 | 16 CPU
- File thử nghiệm: 8 MB dữ liệu ngẫu nhiên; mỗi cấu hình chạy 3 lần, lấy trung bình
- Giới hạn tải đồng thời của leecher: 5 chunk
- Băng thông upload mô phỏng: 2 MB/s mỗi seeder
- Mỗi seeder là một tiến trình riêng; giao tiếp gRPC qua loopback (127.0.0.1)
- Leecher dùng vòng tải tự động của hệ thống (rarest-first + chọn nguồn ít tải nhất `_choose_peer`);
  benchmark không tự phân chunk cho seeder
- Không có Tracker trong thí nghiệm (danh sách peer cho trước), announce được tắt

## Thí nghiệm A – Theo số peer nguồn (chunk 256 KB)

| Số peer nguồn | Thời gian TB (s) | Min – Max (s) | Throughput TB (MB/s) | Speedup | Nguồn được dùng | Chunk/nguồn (min – max) |
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| 1 | 4.485 | 4.455 – 4.538 | 1.78 | 1.00x | 1/1 | 32 – 32 |
| 2 | 2.332 | 2.317 – 2.347 | 3.43 | 1.92x | 2/2 | 16 – 16 |
| 3 | 1.774 | 1.673 – 1.853 | 4.52 | 2.53x | 3/3 | 10 – 12 |
| 5 | 1.233 | 1.205 – 1.276 | 6.49 | 3.64x | 5/5 | 6 – 7 |
| 10 | 1.287 | 1.247 – 1.339 | 6.22 | 3.48x | 9/10 | 0 – 5 |

## Thí nghiệm B – Theo kích thước chunk (3 peer nguồn)

| Chunk size | Số chunk | Thời gian TB (s) | Min – Max (s) | Throughput TB (MB/s) |
|:---:|:---:|:---:|:---:|:---:|
| 16 KB | 512 | 6.225 | 6.178 – 6.291 | 1.29 |
| 64 KB | 128 | 2.474 | 2.421 – 2.538 | 3.24 |
| 256 KB | 32 | 1.761 | 1.673 – 1.893 | 4.56 |
| 1024 KB | 8 | 1.640 | 1.633 – 1.646 | 4.88 |

> Lưu ý khi phân tích: các tiến trình cùng chạy trên một máy qua loopback. Giới hạn băng thông
> upload được mô phỏng bằng phần mềm (sleep theo số byte gửi), không phải đo mạng vật lý.
> Speedup bị chặn trên bởi giới hạn tải đồng thời của leecher (5 chunk):
> với nhiều hơn 5 nguồn, mỗi thời điểm chỉ tối đa 5 nguồn được dùng song song.
> Cột *Chunk/nguồn* cho biết engine đã chia tải giữa các nguồn đều đến mức nào (min – max qua các lần chạy).
