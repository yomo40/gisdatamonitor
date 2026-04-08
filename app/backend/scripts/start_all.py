from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
import webbrowser
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from gisdatamonitor_backend.config import get_settings  # noqa: E402


BACKEND_DIR = Path(__file__).resolve().parents[1]


def _run_python_script(script_name: str, *args: str) -> None:
    cmd = [sys.executable, str(BACKEND_DIR / "scripts" / script_name), *args]
    subprocess.run(cmd, cwd=BACKEND_DIR, check=True)


def _sqlite_file_path() -> Path:
    settings = get_settings()
    if settings.database_backend != "sqlite":
        raise RuntimeError("仅支持 SQLite 数据库模式。")
    sqlite_path = settings.database_url.replace("sqlite:///", "", 1)
    sqlite_file = Path(sqlite_path)
    if not sqlite_file.is_absolute():
        sqlite_file = (BACKEND_DIR / sqlite_file).resolve()
    return sqlite_file


def _is_static_data_ready(sqlite_file: Path) -> bool:
    if not sqlite_file.exists():
        return False
    try:
        with sqlite3.connect(sqlite_file) as conn:
            cur = conn.cursor()
            required_tables = ("boundary_jx", "dem_tiles", "dem_derivatives", "baker_facilities")
            for table in required_tables:
                cur.execute(f"SELECT 1 FROM {table} LIMIT 1")
                if cur.fetchone() is None:
                    return False
        return True
    except sqlite3.Error:
        return False


def _ensure_scene_localization(sqlite_file: Path) -> None:
    if not sqlite_file.exists():
        return
    scene_labels = {
        "world": ("全球视角", "全球能源安全基线场景"),
        "finance": ("金融视角", "聚焦能源价格与市场波动"),
        "tech": ("技术视角", "聚焦产业链与基础设施压力"),
        "happy": ("稳定视角", "以低风险稳定态势为主"),
    }
    try:
        with sqlite3.connect(sqlite_file) as conn:
            cur = conn.cursor()
            for scene_id, (name, desc) in scene_labels.items():
                cur.execute(
                    """
                    UPDATE scene_preset
                    SET scene_name = ?, description = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE scene_id = ?
                    """,
                    (name, desc, scene_id),
                )
            conn.commit()
    except sqlite3.Error:
        return


def _normalize_bind_host(host: str) -> str:
    normalized = (host or "").strip()
    if normalized in {"", "::"}:
        return "0.0.0.0"
    return normalized


def _is_port_available(port: int, host: str = "0.0.0.0") -> bool:
    bind_host = _normalize_bind_host(host)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind((bind_host, port))
            return True
        except OSError:
            return False


def _is_gis_service_running(port: int, timeout_sec: float = 0.35) -> bool:
    url = f"http://127.0.0.1:{port}/ping"
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=max(0.1, float(timeout_sec))) as resp:  # noqa: S310
            if resp.status != 200:
                return False
            payload = json.loads(resp.read().decode("utf-8", errors="ignore"))
            return payload.get("status") == "ok"
    except (urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError):
        return False


def _find_running_gis_service_port(start_port: int, span: int = 20) -> int | None:
    begin = max(1, int(start_port))
    end = begin + max(0, int(span))
    for port in range(begin, end + 1):
        if _is_gis_service_running(port, timeout_sec=0.2):
            return port
    return None


def _resolve_port(requested_port: int, *, bind_host: str, auto_port: bool, span: int = 20) -> int:
    if _is_port_available(requested_port, bind_host):
        return requested_port
    if not auto_port:
        raise RuntimeError(f"端口 {requested_port} 已被占用，请更换端口或关闭占用进程。")
    for candidate in range(requested_port + 1, requested_port + span + 1):
        if _is_port_available(candidate, bind_host):
            return candidate
    raise RuntimeError(f"端口 {requested_port} 起连续 {span} 个端口均不可用，请手动指定 --port。")


def _wait_service_ready(port: int, timeout_sec: float = 45.0) -> bool:
    deadline = time.time() + max(1.0, float(timeout_sec))
    while time.time() < deadline:
        if _is_gis_service_running(port):
            return True
        time.sleep(0.25)
    return False


def _kill_process_tree(proc: subprocess.Popen[bytes] | None) -> None:
    if proc is None:
        return
    if proc.poll() is not None:
        return

    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        return

    proc.terminate()
    try:
        proc.wait(timeout=8)
    except subprocess.TimeoutExpired:
        proc.kill()


def _open_frontend(port: int, path: str = "/", enabled: bool = True) -> None:
    url = f"http://127.0.0.1:{port}{path}"
    print(f"[前端] {url}", flush=True)
    if not enabled:
        return
    try:
        webbrowser.open(url, new=1, autoraise=True)
    except Exception:
        return


def _run_uvicorn_managed(
    *,
    host: str,
    port: int,
    reload_enabled: bool,
    open_browser: bool,
    enable_app_startup_sync: bool,
) -> int:
    uvicorn_cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "gisdatamonitor_backend.main:app",
        "--host",
        host,
        "--port",
        str(port),
    ]
    if reload_enabled:
        uvicorn_cmd.append("--reload")

    print("[启动] 4/4 启动 Web 服务（前台托管）...", flush=True)
    proc: subprocess.Popen[bytes] | None = None
    stop_requested = False

    def _request_stop(_signum: int, _frame: object) -> None:
        nonlocal stop_requested
        stop_requested = True

    handled_signals = [signal.SIGINT, signal.SIGTERM]
    if hasattr(signal, "SIGBREAK"):
        handled_signals.append(signal.SIGBREAK)  # type: ignore[arg-type]
    previous_handlers: dict[int, object] = {}

    try:
        for sig in handled_signals:
            previous_handlers[int(sig)] = signal.getsignal(sig)
            signal.signal(sig, _request_stop)

        child_env = os.environ.copy()
        child_env["GISDATAMONITOR_SYNC_RUN_ON_STARTUP"] = "true" if enable_app_startup_sync else "false"
        proc = subprocess.Popen(uvicorn_cmd, cwd=BACKEND_DIR, env=child_env)
        if _wait_service_ready(port):
            _open_frontend(port, path="/", enabled=open_browser)
            print("[托管] 服务运行中，按 Ctrl+C 退出并释放后端进程。", flush=True)
        else:
            print("[警告] 服务启动超时，仍将继续托管进程，请检查日志。", flush=True)

        while True:
            if stop_requested:
                print("[停止] 收到终止信号，正在停止后端进程...", flush=True)
                return 0
            code = proc.poll()
            if code is not None:
                print(f"[退出] 后端进程已结束，退出码={code}", flush=True)
                return int(code)
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("[停止] 收到中断信号，正在停止后端进程...", flush=True)
        return 0
    finally:
        for sig in handled_signals:
            old = previous_handlers.get(int(sig))
            if old is not None:
                signal.signal(sig, old)  # type: ignore[arg-type]
        _kill_process_tree(proc)


def main() -> None:
    parser = argparse.ArgumentParser(description="一键启动 GISDataMonitor（初始化 + 同步 + 托管运行）")
    parser.add_argument("--host", default="0.0.0.0", help="Uvicorn 监听地址，默认 0.0.0.0")
    parser.add_argument("--port", type=int, default=8080, help="Uvicorn 端口，默认 8080")
    parser.add_argument("--no-reload", action="store_true", help="关闭 Uvicorn 热重载")
    parser.add_argument("--skip-bootstrap", action="store_true", help="跳过数据库结构初始化步骤")
    parser.add_argument("--skip-ingest", action="store_true", help="跳过静态数据导入检查与导入")
    parser.add_argument("--force-ingest", action="store_true", help="强制重新导入静态数据")
    parser.add_argument("--skip-sync", action="store_true", help="跳过启动前实时同步")
    parser.add_argument("--backfill-30d", action="store_true", help="启动前执行 30 天历史事件回补")
    parser.add_argument("--backfill-days", type=int, default=30, help="历史回补天数，默认 30")
    parser.add_argument(
        "--enable-app-startup-sync",
        action="store_true",
        help="允许后端进程启动时再额外执行一次同步（默认关闭，避免前端启动阻塞）",
    )
    parser.add_argument("--no-auto-port", action="store_true", help="端口占用时不自动切换端口")
    parser.add_argument("--no-browser", action="store_true", help="启动后不自动打开前端页面")
    args = parser.parse_args()

    if args.skip_bootstrap:
        print("[启动] 1/4 已按参数跳过数据库结构初始化。", flush=True)
    else:
        print("[启动] 1/4 初始化数据库结构...", flush=True)
        _run_python_script("bootstrap_db.py")

    sqlite_file = _sqlite_file_path()
    _ensure_scene_localization(sqlite_file)
    ingest_required = (not args.skip_ingest) and (args.force_ingest or not _is_static_data_ready(sqlite_file))

    if ingest_required:
        print("[启动] 2/4 导入静态数据（首次会较慢）...", flush=True)
        _run_python_script("ingest_static_data.py")
    elif args.skip_ingest:
        print("[启动] 2/4 已按参数跳过静态数据导入检查。", flush=True)
    else:
        print("[启动] 2/4 静态数据已就绪，跳过导入。", flush=True)

    if args.skip_sync:
        print("[启动] 3/4 已按参数跳过启动前实时同步。", flush=True)
    else:
        print("[启动] 3/4 执行一次实时同步...", flush=True)
        try:
            _run_python_script(
                "pull_last24h_once.py",
                "--max-cycles",
                "4",
                "--sleep-sec",
                "2.0",
                "--min-events-24h",
                "120",
                "--min-geo-events-24h",
                "4",
                "--min-events-7d",
                "500",
                "--min-geo-events-7d",
                "15",
                "--min-days-covered-7d",
                "5",
            )
        except subprocess.CalledProcessError as exc:
            print(f"[警告] 实时同步失败（不阻塞服务启动）：{exc}", flush=True)

    if args.backfill_30d:
        backfill_days = max(1, int(args.backfill_days))
        print(f"[启动] 3.5/4 执行历史回补（{backfill_days} 天）...", flush=True)
        try:
            _run_python_script(
                "backfill_last30d_history.py",
                "--days",
                str(backfill_days),
                "--gdelt-bucket-hours",
                "24",
                "--gdelt-max-records",
                "180",
                "--gdelt-sleep-sec",
                "0.25",
            )
        except subprocess.CalledProcessError as exc:
            print(f"[警告] 历史回补失败（不阻塞服务启动）：{exc}", flush=True)

    existing_port = _find_running_gis_service_port(args.port, span=20)
    if existing_port is not None:
        print(f"[提示] 检测到已有服务运行在 {existing_port}，将启动新的托管实例。", flush=True)

    bind_host = _normalize_bind_host(args.host)
    if not _is_port_available(args.port, bind_host):
        resolved_port = _resolve_port(args.port, bind_host=bind_host, auto_port=not args.no_auto_port)
        if resolved_port != args.port:
            print(f"[启动] 端口 {args.port} 被占用，自动切换到 {resolved_port}。", flush=True)
    else:
        resolved_port = args.port

    if not args.enable_app_startup_sync:
        print("[启动] 已关闭后端启动时额外同步，优先保证前端快速可用。", flush=True)

    exit_code = _run_uvicorn_managed(
        host=args.host,
        port=resolved_port,
        reload_enabled=not args.no_reload,
        open_browser=not args.no_browser,
        enable_app_startup_sync=args.enable_app_startup_sync,
    )
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
