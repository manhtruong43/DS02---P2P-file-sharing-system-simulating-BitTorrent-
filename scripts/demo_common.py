"""
Tiện ích dùng chung cho các script demo DS02.

- Khởi động Tracker / Peer thành TIẾN TRÌNH RIÊNG (subprocess), mỗi tiến trình ghi log riêng.
- Chờ tiến trình sẵn sàng qua mạng (HTTP health check, gRPC channel ready, tracker peer list).
- Tắt tiến trình kiểu crash-stop (kill) hoặc dừng bình thường.
- Đọc log để rút ra bằng chứng cho từng kịch bản demo.

Các script demo chỉ điều khiển tiến trình và đọc log; mọi trao đổi dữ liệu giữa các node
đi qua mạng (HTTP tới Tracker, gRPC giữa các Peer).
"""
import os
import re
import sys
import time
import shutil
import socket
import hashlib
import subprocess
from datetime import datetime
from typing import Dict, List, Optional, Tuple

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import httpx

from src.common.config import TRACKER_API_KEY
from src.common.metadata import TorrentMetadata

TRACKER_SCRIPT = os.path.join(ROOT_DIR, "src", "tracker.py")
PEER_SCRIPT = os.path.join(ROOT_DIR, "src", "peer_node.py")
WORKSPACE_ROOT = os.path.join(ROOT_DIR, "demo_workspace")

LOG_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})")
RECEIVED_RE = re.compile(r"\[RECEIVED\] Chunk (\d+) verified from (\S+)")

# Bật màu trên terminal Windows 10+ / Linux / macOS
if os.name == "nt":
    os.system("")
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
BOLD = "\033[1m"
RESET = "\033[0m"


# ---------------------------------------------------------------------------
# In ấn
# ---------------------------------------------------------------------------
def banner(title: str) -> None:
    print("\n" + "=" * 78)
    print(f" {BOLD}{title}{RESET}")
    print("=" * 78)


def step(text: str) -> None:
    print(f"\n{CYAN}>>> {text}{RESET}")


def info(text: str) -> None:
    print(f"    {text}")


def warn(text: str) -> None:
    print(f"    {YELLOW}{text}{RESET}")


# ---------------------------------------------------------------------------
# Thư mục làm việc & dữ liệu mẫu
# ---------------------------------------------------------------------------
def prepare_workspace(name: str) -> Tuple[str, str]:
    """Tạo (xóa sạch nếu đã có) demo_workspace/<name>/ và thư mục logs bên trong."""
    work_dir = os.path.join(WORKSPACE_ROOT, name)
    shutil.rmtree(work_dir, ignore_errors=True)
    log_dir = os.path.join(work_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    return work_dir, log_dir


def create_sample_file(path: str, size_mb: int) -> None:
    with open(path, "wb") as f:
        f.write(os.urandom(size_mb * 1024 * 1024))


def create_metadata(file_path: str, tracker_url: str, chunk_size: int) -> Tuple[str, Dict]:
    """Sinh file .torrent.json bằng Metadata Generator của hệ thống."""
    meta_path = file_path + ".torrent.json"
    meta = TorrentMetadata(file_path, trackers=[tracker_url], chunk_size=chunk_size)
    data = meta.generate()
    meta.save(meta_path)
    return meta_path, data


def sha256_of(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Kiểm tra cổng mạng
# ---------------------------------------------------------------------------
def port_in_use(port: int, host: str = "127.0.0.1") -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.3)
        return s.connect_ex((host, port)) == 0


def ensure_ports_free(ports: List[int]) -> None:
    busy = [p for p in ports if port_in_use(p)]
    if busy:
        print(f"\n{RED}Các cổng sau đang bị chiếm: {busy}.{RESET}")
        print("Hãy tắt tracker/peer đang chạy ở terminal khác (Ctrl + C) rồi chạy lại demo.")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Quản lý tiến trình
# ---------------------------------------------------------------------------
class NodeProcess:
    """Một node (tracker hoặc peer) chạy trong tiến trình riêng, log ra file riêng."""

    def __init__(self, name: str, args: List[str], log_path: str, port: int):
        self.name = name
        self.args = args
        self.log_path = log_path
        self.port = port
        self.proc: Optional[subprocess.Popen] = None
        self._log_file = None
        self.killed_at: Optional[datetime] = None

    def start(self) -> "NodeProcess":
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"
        self._log_file = open(self.log_path, "w", encoding="utf-8")
        self.proc = subprocess.Popen(
            [sys.executable] + self.args,
            stdout=self._log_file,
            stderr=subprocess.STDOUT,
            cwd=ROOT_DIR,
            env=env,
        )
        return self

    def is_alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def kill(self) -> datetime:
        """Crash-stop: giết tiến trình ngay lập tức, không cho nó báo /leave với tracker."""
        self.killed_at = datetime.now()
        if self.is_alive():
            self.proc.kill()
            self.proc.wait(timeout=10)
        return self.killed_at

    def stop(self) -> None:
        """Dừng bình thường khi kết thúc demo."""
        if self.is_alive():
            try:
                if os.name != "nt":
                    import signal
                    self.proc.send_signal(signal.SIGINT)  # peer gửi /leave rồi thoát
                else:
                    self.proc.terminate()
                self.proc.wait(timeout=6)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
        if self._log_file and not self._log_file.closed:
            self._log_file.close()


def start_tracker(log_dir: str, port: int = 8000) -> NodeProcess:
    node = NodeProcess(
        "Tracker",
        [TRACKER_SCRIPT, "--host", "127.0.0.1", "--port", str(port)],
        os.path.join(log_dir, "tracker.log"),
        port,
    ).start()
    wait_tracker_ready(f"http://127.0.0.1:{port}", node)
    return node


def start_peer(
    name: str,
    log_dir: str,
    port: int,
    metadata_path: str,
    output_path: str,
    seed: bool = False,
    corrupt: bool = False,
    upload_limit_kbps: float = 0,
) -> NodeProcess:
    args = [PEER_SCRIPT, "--host", "127.0.0.1", "--port", str(port),
            "--metadata", metadata_path, "--output", output_path]
    if seed:
        args.append("--seed")
    if corrupt:
        args.append("--corrupt")
    if upload_limit_kbps > 0:
        args += ["--upload-limit", str(upload_limit_kbps)]
    log_name = name.lower().replace(" ", "_") + ".log"
    return NodeProcess(name, args, os.path.join(log_dir, log_name), port).start()


def explain_dead(node: NodeProcess, lines: int = 15) -> None:
    """In vài dòng cuối log của một node đã chết để biết nguyên nhân."""
    print(f"{RED}{node.name} đã dừng bất thường (exit code {node.proc.poll() if node.proc else '?'}). "
          f"Các dòng cuối trong {node.log_path}:{RESET}")
    for line in read_log(node.log_path)[-lines:]:
        print(f"      {line}")


def stop_all(nodes: List[NodeProcess]) -> None:
    # Dừng peer trước, tracker sau cùng để peer còn kịp báo /leave
    for node in [n for n in nodes if n.name != "Tracker"] + [n for n in nodes if n.name == "Tracker"]:
        node.stop()


# ---------------------------------------------------------------------------
# Chờ sẵn sàng qua mạng
# ---------------------------------------------------------------------------
def wait_tracker_ready(tracker_url: str, node: NodeProcess, timeout: float = 20) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not node.is_alive():
            break
        try:
            if httpx.get(f"{tracker_url}/health", timeout=1).status_code == 200:
                return
        except Exception:
            pass
        time.sleep(0.3)
    print(f"{RED}Tracker không khởi động được.{RESET}")
    explain_dead(node)
    sys.exit(1)


def get_tracker_peers(tracker_url: str, file_id: str) -> List[Dict]:
    try:
        res = httpx.get(f"{tracker_url}/peers/{file_id}",
                        headers={"X-API-Key": TRACKER_API_KEY}, timeout=2)
        if res.status_code == 200:
            return res.json().get("peers", [])
    except Exception:
        pass
    return []


def wait_peer_on_tracker(tracker_url: str, file_id: str, node: NodeProcess, timeout: float = 30) -> None:
    """Chờ tới khi tracker thấy peer này trong danh sách (đã register/announce xong)."""
    target = f"127.0.0.1:{node.port}"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not node.is_alive():
            break
        if any(p["peer_id"] == target for p in get_tracker_peers(tracker_url, file_id)):
            return
        time.sleep(0.3)
    print(f"{RED}{node.name} không xuất hiện trên tracker sau {timeout:.0f}s.{RESET}")
    explain_dead(node)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Đọc log
# ---------------------------------------------------------------------------
def read_log(path: str) -> List[str]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.read().splitlines()


def log_time(line: str) -> Optional[datetime]:
    m = LOG_TS_RE.match(line)
    if not m:
        return None
    return datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S,%f")


def grep(lines: List[str], needle: str) -> List[str]:
    return [l for l in lines if needle in l]


def short(line: str, width: int = 150) -> str:
    """Bỏ phần ngày tháng, giữ giờ:phút:giây + nội dung, cắt cho vừa màn hình."""
    m = LOG_TS_RE.match(line)
    text = line[11:] if m else line
    return text if len(text) <= width else text[: width - 3] + "..."


def chunks_written(lines: List[str]) -> int:
    return len(grep(lines, "[ATOMIC WRITE]"))


def chunk_sources(lines: List[str]) -> Dict[int, Tuple[str, Optional[datetime]]]:
    """chunk_index -> (peer nguồn, thời điểm nhận) từ các dòng [RECEIVED]."""
    result = {}
    for l in lines:
        m = RECEIVED_RE.search(l)
        if m:
            result[int(m.group(1))] = (m.group(2), log_time(l))
    return result


def download_finished(lines: List[str]) -> bool:
    return bool(grep(lines, "[VERIFY]"))


def wait_downloads(nodes: List[NodeProcess], total_chunks: int, timeout: float = 90) -> bool:
    """Hiển thị tiến độ tải của các leecher cho tới khi tất cả xong hoặc hết giờ."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        logs = {n.name: read_log(n.log_path) for n in nodes}
        progress = " | ".join(f"{name}: {chunks_written(l):>3}/{total_chunks}" for name, l in logs.items())
        sys.stdout.write(f"\r    Tiến độ  {progress}   ")
        sys.stdout.flush()
        if all(download_finished(l) for l in logs.values()):
            print()
            return True
        dead = [n for n in nodes if not n.is_alive()]
        if dead:
            print()
            for n in dead:
                explain_dead(n)
            return False
        time.sleep(0.5)
    print()
    warn(f"Hết thời gian chờ {timeout:.0f}s.")
    return False


# ---------------------------------------------------------------------------
# Bảng kết quả
# ---------------------------------------------------------------------------
class Evidence:
    """Thu thập kết quả từng kịch bản để in bảng tổng kết cuối demo."""

    def __init__(self):
        self.rows: List[Tuple[str, str, bool, List[str]]] = []

    def add(self, scenario: str, claim: str, ok: bool, proof_lines: Optional[List[str]] = None) -> None:
        self.rows.append((scenario, claim, ok, proof_lines or []))

    def all_ok(self) -> bool:
        return all(ok for _, _, ok, _ in self.rows)

    def print_report(self, max_proof: int = 2) -> None:
        banner("BẢNG BẰNG CHỨNG (trích từ log của từng node)")
        for scenario, claim, ok, proof in self.rows:
            mark = f"{GREEN}ĐẠT{RESET}" if ok else f"{RED}CHƯA ĐẠT{RESET}"
            print(f"\n[{mark}] {BOLD}{scenario}{RESET}: {claim}")
            for line in proof[:max_proof]:
                print(f"        {line}")
        print()
        if self.all_ok():
            print(f"{GREEN}{BOLD}TẤT CẢ KỊCH BẢN ĐỀU ĐẠT.{RESET}")
        else:
            print(f"{RED}{BOLD}CÓ KỊCH BẢN CHƯA ĐẠT - xem log chi tiết bên dưới.{RESET}")


def print_log_locations(log_dir: str) -> None:
    print(f"\nLog chi tiết của từng node nằm trong: {log_dir}")
    for name in sorted(os.listdir(log_dir)):
        print(f"  - {name}")
