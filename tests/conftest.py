"""
Cấu hình chung cho pytest.

Từ khi API key không còn giá trị mặc định trong mã nguồn, bộ test cần một key để chạy.
Nếu môi trường chưa có TRACKER_API_KEY (ví dụ máy sạch chưa tạo .env), đặt một key
CHỈ DÙNG CHO TEST trước khi bất kỳ module nào của hệ thống được import.
python-dotenv không ghi đè biến môi trường đã có, nên mọi thành phần trong test dùng chung key này.
"""
import os

os.environ.setdefault("TRACKER_API_KEY", "pytest_only_key_not_a_real_secret")
