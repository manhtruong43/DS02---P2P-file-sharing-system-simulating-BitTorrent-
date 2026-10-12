# DS02 – Hệ thống chia sẻ file P2P mô phỏng BitTorrent

> **Môn học:** Distributed Systems – Course Project
> **Mã đề tài:** DS02
> **Khái niệm trọng tâm:** P2P, tracker, chia nhỏ dữ liệu (chunking) và nhân bản (replication), tải song song (parallel downloading), churn

---

## 1. Mốc thời gian quan trọng

| Mốc | Ngày |
|---|---|
| Thi giữa kỳ (dự kiến, có thể thay đổi) | 16/11/2026 |
| **Nộp báo cáo + source code (qua GitHub)** | **22/11/2026** |
| Demo + vấn đáp (đợt 1) | 23/11/2026 |
| Demo + vấn đáp (đợt 2) | 30/11/2026 |

**Việc cần làm ngay:** đăng ký nhóm (đúng mã số sinh viên) trên Google Sheet của giảng viên; đăng ký chủ đề, thành viên, kiến trúc sơ bộ và công nghệ dự kiến với giảng viên.

---

## 2. Hình thức thực hiện

- Nhóm **tối đa 3 thành viên**.
- Mỗi thành viên phụ trách **một phần việc cụ thể** và có **bằng chứng đóng góp trên GitHub** (commit riêng của từng người).
- Mỗi thành viên phải **hiểu toàn bộ kiến trúc** và giải thích được phần mình làm.
- Các nhóm cùng chọn DS02 có thể dùng công nghệ/giải pháp khác nhau.

### Mục tiêu học tập (phải thể hiện được)

- Phân tích bài toán, xác định các vấn đề của hệ phân tán.
- Chọn kiến trúc, giao thức, thuật toán phù hợp.
- Xây dựng các tiến trình/peer/service độc lập giao tiếp qua mạng.
- Xử lý nhiều yêu cầu/người dùng đồng thời.
- Quản lý trạng thái và đảm bảo nhất quán.
- Phát hiện, xử lý và phục hồi từ một số lỗi.
- Kiểm thử và đánh giá hệ thống.
- Viết báo cáo kỹ thuật, demo và bảo vệ.

---

## 3. Mục tiêu của DS02

Xây dựng hệ thống cho phép **chia file thành các chunk** và **tải song song từ nhiều peer**. Peer đã tải xong một chunk phải có khả năng **cung cấp chunk đó cho peer khác**.

> ⚠️ **Tracker chỉ quản lý metadata và danh sách peer; tracker KHÔNG ĐƯỢC cung cấp toàn bộ nội dung file.**

---

## 4. Thành phần tối thiểu

| # | Thành phần | Mô tả |
|---|---|---|
| 1 | **Tracker / Bootstrap server** | Quản lý metadata, danh sách peer, trạng thái online/offline |
| 2 | **≥ 3 peer** | Chạy như các tiến trình độc lập, vừa tải vừa upload |
| 3 | **Metadata generator** | Tạo file metadata (kiểu `.torrent`) từ file gốc |
| 4 | **Chunk splitter / merger** | Chia file thành chunk và ghép lại đúng thứ tự |
| 5 | **Cơ chế tải/upload chunk đồng thời** | Thread pool / async I/O, giới hạn số kết nối |

---

## 5. Yêu cầu chức năng DS02

### 5.1. Metadata

Metadata **bắt buộc** chứa:

- File ID
- Tên file và kích thước
- Chunk size và số lượng chunk
- **Hash của từng chunk**
- **Hash của toàn bộ file**
- Thông tin tracker (địa chỉ/URL)

Ví dụ (JSON):

```json
{
  "file_id": "b3f1c9...",
  "file_name": "demo.iso",
  "file_size": 104857600,
  "chunk_size": 262144,
  "num_chunks": 400,
  "chunk_hashes": ["sha256-0", "sha256-1", "..."],
  "file_hash": "sha256-...",
  "trackers": ["http://tracker.example:8000"]
}
```

### 5.2. Chia sẻ và phát hiện peer

- Seeder chia file thành các chunk.
- Seeder **đăng ký file** với tracker.
- Tracker quản lý các peer đang có **toàn bộ hoặc một phần** file.
- Peer **announce danh sách chunk** mình hiện có (bitfield).

### 5.3. Tải file

- Tải từ **ít nhất 2 peer**.
- Tải **nhiều chunk song song**.
- Peer **vừa tải vừa upload** được.
- Chunk lỗi được **yêu cầu lại từ peer khác**.
- **Giới hạn** số kết nối hoặc số thread tải.

### 5.4. Toàn vẹn và phục hồi

- **Kiểm tra hash từng chunk**, từ chối chunk sai.
- Ghép chunk **đúng thứ tự**.
- **Kiểm tra hash file cuối**.
- **Tiếp tục tải khi một source peer rời mạng.**
- *(Khuyến khích)* Resume download sau khi chương trình khởi động lại.

---

## 6. Yêu cầu chung cho mọi đề tài (áp dụng cho DS02)

### 6.1. Tính phân tán

- Tối thiểu **3 tiến trình/peer/client/service độc lập** (process, VM hoặc container).
- Giao tiếp qua mạng: socket, HTTP, WebSocket, gRPC, message queue hoặc tương đương.
- Mỗi thành phần có **định danh duy nhất** (Peer ID).
- Xử lý **nhiều kết nối/yêu cầu đồng thời**.
- **Không** dùng biến toàn cục, shared memory hoặc shared file thay cho giao tiếp mạng.
- **Log** ghi rõ: node, timestamp, loại sự kiện, kết quả xử lý.
- Có thể triển khai: nhiều máy trong LAN, nhiều mạng Internet, VM/cloud, container, hoặc kết hợp.

### 6.2. Kiến trúc và giao tiếp (phải thiết kế và trình bày)

- Các thành phần và trách nhiệm của từng thành phần.
- Cơ chế đặt tên và định danh.
- Cơ chế peer discovery.
- Giao thức giao tiếp.
- Định dạng thông điệp.
- Cách quản lý trạng thái phân tán.
- Cách phát hiện thành phần online/offline.
- Timeout, acknowledgement, retry.
- Xử lý thông điệp trùng lặp / sai thứ tự.
- Ràng buộc về hiệu năng, khả năng mở rộng, tính sẵn sàng.

### 6.3. Đồng thời và nhất quán

Phải có **ít nhất 1 kịch bản đồng thời** và:

- Xác định các xung đột có thể xảy ra.
- Định nghĩa mô hình nhất quán mong muốn.
- Thiết kế cơ chế kiểm soát đồng thời.
- Kiểm thử bằng thao tác đồng thời.
- Chứng minh trạng thái cuối thỏa yêu cầu.

*Gợi ý cho DS02:* hai luồng cùng ghi một chunk, hai peer cùng yêu cầu một chunk từ một nguồn, cập nhật bitfield trên tracker đồng thời, lock/atomic write khi ghi chunk xuống đĩa.

### 6.4. Chịu lỗi và phục hồi

Xác định **failure model** và demo **ít nhất 2 kịch bản lỗi**, ví dụ:

- Peer rời mạng đột ngột.
- Mất kết nối.
- Tải file bị gián đoạn.
- Thông điệp mất / trùng / sai thứ tự.

Cơ chế phù hợp: timeout, bounded retry, acknowledgement, heartbeat/lease, deduplication, checkpoint/lưu trạng thái trung gian, reconnect, resume, failover.

### 6.5. Bảo mật cơ bản

- Xác thực user/peer/service.
- Kiểm soát truy cập tài nguyên.
- Validate và sanitize dữ liệu đầu vào.
- **Không lưu mật khẩu dạng plaintext** (dùng bcrypt/argon2…).
- **Không commit token/mật khẩu/secret key lên GitHub**; dùng biến môi trường.
- **Giới hạn kích thước** message và file.
- Log các hoạt động quan trọng.
- Dùng TLS / mã hóa message (khuyến khích).

---

## 7. Kiến trúc đề xuất cho DS02

```
                 ┌─────────────────────────┐
                 │        TRACKER          │
                 │ - metadata registry     │
                 │ - peer list / bitfield  │
                 │ - heartbeat / TTL       │
                 └───────▲───────▲─────────┘
        register/announce│       │announce/get_peers
          ┌──────────────┘       └──────────────┐
   ┌──────┴──────┐        chunk req/resp  ┌─────┴───────┐
   │ Peer A      │◄──────────────────────►│ Peer B      │
   │ (Seeder)    │                        │ (Leecher)   │
   └──────▲──────┘                        └─────▲───────┘
          │            chunk req/resp           │
          └──────────────►┌─────────────┐◄──────┘
                          │ Peer C      │
                          │ (Leecher)   │
                          └─────────────┘
```

### 7.1. Tracker

- API: `REGISTER_FILE`, `ANNOUNCE`, `GET_PEERS`, `HEARTBEAT`, `LEAVE`.
- Lưu: file_id → metadata, danh sách peer (peer_id, IP, port, bitfield, last_seen).
- Xóa peer hết hạn sau timeout (TTL / lease).
- **Không lưu / không phục vụ nội dung chunk.**

### 7.2. Peer

- **Server module:** lắng nghe, phục vụ yêu cầu chunk từ peer khác.
- **Client module:** hỏi tracker, chọn peer nguồn, tải chunk.
- **Piece manager:** bitfield, trạng thái chunk (MISSING / DOWNLOADING / DONE), hàng đợi chunk.
- **Storage:** lưu chunk (thư mục tạm), ghép file cuối, lưu state để resume.
- **Giao diện:** CLI hoặc web đơn giản (hiển thị tiến độ, peer đang kết nối, log).

### 7.3. Giao thức đề xuất

| Loại message | Hướng | Mô tả |
|---|---|---|
| `REGISTER_FILE` | Seeder → Tracker | Đăng ký metadata file |
| `ANNOUNCE` | Peer → Tracker | Báo bitfield / chunk mới có |
| `GET_PEERS` | Peer → Tracker | Lấy danh sách peer của file |
| `PEER_LIST` | Tracker → Peer | Trả danh sách peer |
| `HEARTBEAT` | Peer → Tracker | Duy trì trạng thái online |
| `HANDSHAKE` | Peer ↔ Peer | Trao đổi Peer ID, file_id |
| `BITFIELD` | Peer ↔ Peer | Danh sách chunk đang có |
| `REQUEST_CHUNK` | Peer → Peer | Yêu cầu chunk theo index |
| `CHUNK_DATA` | Peer → Peer | Dữ liệu chunk + hash |
| `HAVE` | Peer → Peer/Tracker | Thông báo vừa có chunk mới |
| `ERROR` | Mọi hướng | Lỗi (chunk không có, vượt giới hạn…) |

Định dạng: JSON (control) + binary frame (data) hoặc Protocol Buffers. Mỗi message nên có: `message_id`, `sender_id`, `type`, `timestamp`, `payload`.

### 7.3.1. Công nghệ gợi ý

- Ngôn ngữ: Python (asyncio / threading), Go, Java, Node.js.
- Giao tiếp: TCP socket, HTTP/REST (tracker), WebSocket, gRPC.
- Hash: SHA-256. Đóng gói: Docker + Docker Compose.

### 7.4. Thuật toán tải đề xuất

1. Lấy metadata → hỏi tracker → nhận danh sách peer.
2. Kết nối tối đa **N** peer (giới hạn kết nối), trao đổi bitfield.
3. Chọn chunk cần tải (random hoặc **rarest-first**), gán cho peer có chunk đó.
4. Tải song song với **thread pool / semaphore** (giới hạn M luồng).
5. Nhận chunk → **verify hash** → ghi đĩa → cập nhật bitfield → `HAVE`/`ANNOUNCE`.
6. Lỗi (timeout, hash sai, peer rớt) → đánh dấu chunk MISSING → **retry từ peer khác** (bounded retry, backoff).
7. Đủ chunk → ghép theo thứ tự → verify hash file cuối.

---

## 8. Đồng thời, nhất quán và chịu lỗi – phần cần thiết kế trong DS02

### 8.1. Đồng thời

| Xung đột | Cơ chế đề xuất |
|---|---|
| Nhiều luồng cùng tải một chunk | Trạng thái chunk có lock (DOWNLOADING), chỉ một luồng được nhận |
| Ghi chunk xuống đĩa đồng thời | Ghi file tạm + rename atomic / lock theo chunk |
| Tracker cập nhật peer list từ nhiều peer | Lock/mutex hoặc cấu trúc dữ liệu thread-safe |
| Một peer bị nhiều peer yêu cầu cùng lúc | Giới hạn upload slot, hàng đợi yêu cầu |

Mô hình nhất quán: **eventual consistency** cho danh sách peer/bitfield trên tracker; **strong integrity** cho chunk (hash bắt buộc đúng).

### 8.2. Failure model (ví dụ cần nêu trong báo cáo)

- Peer **crash-stop** (rời mạng đột ngột).
- Mạng có thể **mất / trễ / trùng** gói.
- Peer có thể gửi **chunk sai** (lỗi hoặc cố ý).
- Tracker **không bị lỗi** trong phạm vi cơ bản (nếu muốn nâng cao: replicated tracker).

### 8.3. Cơ chế xử lý lỗi

| Lỗi | Phát hiện | Xử lý |
|---|---|---|
| Source peer rời mạng | Timeout / connection reset / hết heartbeat | Gán lại chunk cho peer khác |
| Chunk sai hash | Verify SHA-256 | Loại bỏ, tải lại, (tùy chọn) chặn peer |
| Mất kết nối giữa chừng | Timeout đọc | Retry có giới hạn + backoff |
| Peer bị tắt khi đang tải | Lưu state/bitfield ra đĩa | **Resume** sau khi khởi động lại |
| Tracker xóa peer chết | TTL / lease | Xóa sau timeout |

---

## 9. Kịch bản demo bắt buộc

| # | Kịch bản | Bằng chứng cần có |
|---|---|---|
| 1 | Tạo metadata và chia sẻ file | File metadata, log seeder đăng ký tracker |
| 2 | Tải song song từ **≥ 2 peer** | Log chunk đến từ nhiều peer khác nhau, chạy đồng thời |
| 3 | Peer **upload trước khi tải xong** | Log peer B phục vụ chunk cho peer C khi B chưa đủ file |
| 4 | **Tắt một source** trong lúc tải | Log phát hiện peer mất |
| 5 | **Chuyển sang source khác** | Log chunk được yêu cầu lại từ peer khác |
| 6 | **Phát hiện chunk không hợp lệ** | Cố tình làm hỏng chunk → log hash mismatch → tải lại |
| 7 | Ghép file và **verify hash cuối** | Hash file tải về trùng hash gốc |

**Yêu cầu buổi demo chung:** ≥ 3 node, thấy được giao tiếp mạng, có 1 kịch bản đồng thời, có **ít nhất 1 lỗi live**, hiển thị log/trace để giải thích, **tất cả thành viên tham gia**.

---

## 10. Tính năng nâng cao (cộng điểm / tùy chọn)

- **Rarest-first** chunk selection.
- **Choking/unchoking** hoặc tit-for-tat.
- **DHT** thay tracker tập trung.
- **Magnet link**.
- **Replicated tracker** (nhiều tracker, failover).
- **Giới hạn băng thông**.
- **Resume download** (lưu state).
- **Kết nối mã hóa** (TLS).
- **Mô phỏng churn** và đánh giá độ sẵn sàng chunk.

> Gợi ý ưu tiên với thời gian có hạn: **Resume + Rarest-first + Churn simulation + TLS** — dễ làm, dễ demo, có số liệu để đưa vào báo cáo.

---

## 11. Kiểm thử và đánh giá

### 11.1. Số lượng test tối thiểu

| Loại | Tối thiểu |
|---|---|
| Functional test | **8** |
| Concurrency / conflict test | **2** |
| Fault & recovery test | **2** |
| Security / invalid-data test | **2** |
| Performance / scalability experiment | **1** |

### 11.2. Bảng test gợi ý

**Functional**

1. Tạo metadata đúng (số chunk, hash).
2. Chia file → ghép lại → hash trùng file gốc.
3. Seeder đăng ký với tracker thành công.
4. Peer nhận đúng peer list từ tracker.
5. Tải trọn vẹn file từ 1 seeder.
6. Tải song song từ ≥ 2 peer.
7. Peer vừa tải vừa upload.
8. Giới hạn số kết nối/thread hoạt động.
9. Tracker xóa peer hết hạn.

**Concurrency**

- Nhiều luồng tải cùng lúc, không chunk nào bị ghi đè/hỏng.
- Nhiều peer cùng yêu cầu chunk từ một seeder.

**Fault & recovery**

- Tắt source giữa chừng → tải tiếp từ peer khác.
- Restart peer → resume từ state đã lưu.

**Security / invalid data**

- Chunk sai hash bị từ chối.
- Message sai định dạng / vượt kích thước bị từ chối.
- Chunk index ngoài phạm vi / file_id không tồn tại.
- (Nếu có) Peer chưa xác thực bị từ chối.

**Performance (chọn ít nhất 1)**

- Thời gian tải với 1, 2, 3, 5, 10 peer nguồn (đồ thị).
- Throughput theo số luồng song song / kích thước chunk.
- Độ sẵn sàng chunk theo tỉ lệ churn.

### 11.3. Chỉ số có thể đo

Thời gian tải file, throughput, delivery success rate, failure-detection time, recovery time, số kết nối đồng thời, CPU/RAM/băng thông.

---

## 12. Bảo mật – checklist triển khai cho DS02

- [ ] Peer/tracker xác thực (token, API key, hoặc chữ ký) – secret lấy từ **biến môi trường**.
- [ ] `.env` nằm trong `.gitignore`; chỉ commit `.env.example`.
- [ ] Giới hạn kích thước message, chunk, số kết nối.
- [ ] Validate: `file_id`, chunk index, độ dài, kiểu dữ liệu, đường dẫn file (chống path traversal).
- [ ] Mật khẩu (nếu có tài khoản) được băm.
- [ ] Log hoạt động quan trọng (đăng ký, announce, chunk lỗi, peer bị từ chối).
- [ ] (Khuyến khích) TLS giữa các thành phần.

---

## 13. Cấu trúc GitHub repository bắt buộc

```
project-root/
├── README.md
├── .env.example
├── src/
│   ├── tracker/
│   ├── peer/
│   └── common/          # message format, hash, config
├── deploy/              # Dockerfile, docker-compose.yml, cấu hình server
├── tests/               # test code + kịch bản test
├── scripts/             # chạy hệ thống, tạo workload, fault injection
└── docs/
    ├── report.pdf
    └── slides.pdf       # nếu có
```

### README.md phải có

- Mô tả dự án, kiến trúc.
- Yêu cầu môi trường, hướng dẫn cài đặt và chạy.
- Các kịch bản demo (lệnh chạy từng bước).
- **Bảng phân công công việc** (task-assignment table).
- Hướng dẫn **triển khai lại** (redeploy).

### Yêu cầu repository

- Có **commit history của từng thành viên**.
- Có **tag/release cuối** (ví dụ `v1.0.0`).
- Cấp quyền cho giảng viên nếu repo private.

### Mẫu bảng phân công

| Thành viên | MSSV | Phần phụ trách | Module / thư mục | Bằng chứng (commit/PR) |
|---|---|---|---|---|
| Thành viên 1 | | Tracker + metadata generator | `src/tracker/`, `src/common/` | |
| Thành viên 2 | | Peer: download engine, chunk, hash, resume | `src/peer/` | |
| Thành viên 3 | | Test, scripts, deploy, fault injection, báo cáo | `tests/`, `scripts/`, `deploy/`, `docs/` | |

---

## 14. Cấu trúc báo cáo (20–30 trang, không kể phụ lục)

1. Trang bìa
2. Thông tin nhóm và phân công
3. Tóm tắt (Abstract)
4. Mô tả bài toán và yêu cầu
5. Các vấn đề hệ phân tán
6. Phân tích và lựa chọn giải pháp
7. Kiến trúc hệ thống
8. Thiết kế thành phần và giao thức
9. Định danh và khám phá (peer discovery)
10. Đồng thời, đồng bộ và nhất quán
11. Chịu lỗi và phục hồi
12. Bảo mật
13. Triển khai (implementation)
14. Kiểm thử và kết quả
15. Đánh giá hiệu năng
16. Hạn chế và hướng phát triển
17. Kết luận
18. Tài liệu tham khảo
19. Phụ lục

**Nội dung nên có riêng cho DS02:** phân tích vì sao tracker không giữ dữ liệu file; so sánh tracker tập trung và DHT; phân tích ảnh hưởng của chunk size; mô hình churn và tác động đến độ sẵn sàng chunk.

---

## 15. Thuyết trình và vấn đáp

| Phần | Thời lượng |
|---|---|
| Giới thiệu bài toán và kiến trúc | 3–5 phút |
| Demo | 7–10 phút |
| Vấn đáp | ~5 phút |

---

## 16. Các trường hợp KHÔNG đạt yêu cầu (cần tránh)

- Hệ thống chạy trong **một tiến trình duy nhất**.
- Các thành phần **không giao tiếp qua mạng**.
- **Tracker cung cấp toàn bộ nội dung file.** *(lỗi đặc thù DS02)*
- **Không demo kịch bản đồng thời.**
- **Không demo lỗi và phục hồi.**
- Source code **không chạy được** theo hướng dẫn.
- Không nộp báo cáo hoặc không demo.
- Thành viên **không chứng minh được đóng góp**.

---

## 17. Liêm chính học thuật

- Được dùng thư viện/framework/mã nguồn mở nhưng **phải trích dẫn**.
- **Không** dùng nguyên sản phẩm có sẵn chỉ đổi giao diện (ví dụ: bọc một BitTorrent client có sẵn).
- **Cơ chế cốt lõi** (chia chunk, tracker, quản lý chunk, tải song song, verify, retry) **phải do nhóm tự cài đặt** và giải thích được.
- Phải trích dẫn nguồn cho thuật toán, mã, hình, tài liệu tham khảo.
- Nếu dùng AI: nhóm phải **kiểm tra, hiểu và chịu trách nhiệm** với sản phẩm.

---

## 18. Lộ trình đề xuất (từ 05/10 đến 22/11/2026)

| Giai đoạn | Thời gian | Công việc chính |
|---|---|---|
| 0 | 05/10 – 11/10 | Đăng ký nhóm/đề tài; chốt công nghệ; vẽ kiến trúc sơ bộ; tạo repo, khung thư mục, phân công |
| 1 | 12/10 – 25/10 | Metadata generator, chunk split/merge, hash; tracker cơ bản (register, announce, get_peers, heartbeat) |
| 2 | 26/10 – 08/11 | Peer server/client; handshake, bitfield, request/response chunk; tải song song đa nguồn; verify hash |
| 3 | 09/11 – 15/11 | Chịu lỗi: timeout, retry, đổi nguồn, TTL tracker; resume; log chuẩn; (ôn thi giữa kỳ 16/11) |
| 4 | 16/11 – 19/11 | Test đầy đủ (≥ 15 test), thí nghiệm hiệu năng/churn, script demo, Docker Compose |
| 5 | 20/11 – 22/11 | Hoàn thiện báo cáo, README, tag `v1.0.0`, **nộp** |
| 6 | 23/11 – 30/11 | Luyện demo, chuẩn bị vấn đáp (mỗi thành viên giải thích được toàn hệ thống) |

---

## 19. Checklist hoàn thiện cuối cùng

**Hệ thống**
- [ ] ≥ 3 peer + 1 tracker chạy như tiến trình/container riêng, giao tiếp qua mạng
- [ ] Peer ID duy nhất, log đủ node/timestamp/event/result
- [ ] Metadata đủ trường (file ID, tên, size, chunk size, số chunk, hash chunk, hash file, tracker)
- [ ] Seeder đăng ký; peer announce bitfield; tracker xóa peer hết hạn
- [ ] Tải song song ≥ 2 peer, nhiều chunk cùng lúc, giới hạn thread/kết nối
- [ ] Vừa tải vừa upload
- [ ] Verify hash chunk + hash file; từ chối chunk sai; tải lại từ peer khác
- [ ] Tiếp tục tải khi source rời mạng
- [ ] Tracker không phục vụ nội dung file

**Đồng thời / lỗi / bảo mật**
- [ ] Có kịch bản đồng thời và cơ chế kiểm soát được giải thích
- [ ] Có ≥ 2 kịch bản lỗi được demo
- [ ] Không có secret trong repo; có `.env.example`
- [ ] Giới hạn kích thước, validate input, xác thực cơ bản

**Kiểm thử**
- [ ] ≥ 8 functional, ≥ 2 concurrency, ≥ 2 fault/recovery, ≥ 2 security, ≥ 1 performance

**Sản phẩm bàn giao**
- [ ] Repo đúng cấu trúc: `README.md`, `src/`, `deploy/`, `tests/`, `scripts/`, `docs/`, `.env.example`
- [ ] `docs/report.pdf` (20–30 trang), `docs/slides.pdf`
- [ ] Bảng phân công + commit của từng thành viên
- [ ] Hướng dẫn redeploy chạy được từ máy sạch
- [ ] Tag/release `v1.0.0`; cấp quyền cho giảng viên (nếu private)

**Demo**
- [ ] 7 kịch bản bắt buộc ở mục 9 đã luyện
- [ ] Có ít nhất 1 lỗi live
- [ ] Mọi thành viên cùng tham gia và trả lời được vấn đáp

---

*Tài liệu được tổng hợp từ "Course Project Specification – Distributed Systems", phần mục tiêu, định dạng, yêu cầu chung (mục 4), đề tài DS02 (mục 6), kiểm thử (mục 9), sản phẩm bàn giao (mục 10), trình bày (mục 11), trường hợp không đạt (mục 12) và liêm chính học thuật (mục 13). Các phần "Kiến trúc đề xuất", "Lộ trình" và "Test gợi ý" là gợi ý của người soạn, không phải yêu cầu chính thức.*
