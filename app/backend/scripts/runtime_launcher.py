from __future__ import annotations

import argparse
import os
import socket
import sys
import threading
import time
import urllib.request
import webbrowser
from pathlib import Path

import uvicorn

PROXY_ENV_KEYS = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
    "GISDATAMONITOR_HTTP_PROXY",
    "GISDATAMONITOR_HTTPS_PROXY",
)


def _runtime_root() -> Path:
    if getattr(sys, "frozen", False):
        exe_dir = Path(sys.executable).resolve().parent
        if (exe_dir / "app").exists():
            return exe_dir
        if (exe_dir.parent / "app").exists():
            return exe_dir.parent
        return exe_dir
    return Path(__file__).resolve().parents[3]


def _is_port_available(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((host, port))
            return True
        except OSError:
            return False


def _resolve_port(host: str, preferred_port: int, span: int = 20) -> int:
    if _is_port_available(host, preferred_port):
        return preferred_port
    for candidate in range(preferred_port + 1, preferred_port + span + 1):
        if _is_port_available(host, candidate):
            return candidate
    raise RuntimeError(f"端口 {preferred_port} 起连续 {span} 个端口均不可用。")


def _clear_proxy_env() -> None:
    for key in PROXY_ENV_KEYS:
        os.environ.pop(key, None)


def _env_int(name: str, default: int, minimum: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(minimum, value)


def _wait_for_health_and_open(
    *,
    url: str,
    host: str,
    port: int,
    timeout_sec: int,
    interval_sec: float = 1.0,
) -> None:
    health_url = f"http://{host}:{port}/api/v1/system/health"
    deadline = time.monotonic() + float(timeout_sec)
    while time.monotonic() < deadline:
        try:
            request = urllib.request.Request(
                health_url,
                headers={"User-Agent": "GISDataMonitorLauncher/1.0"},
                method="GET",
            )
            with urllib.request.urlopen(request, timeout=2) as response:
                if 200 <= int(getattr(response, "status", 0)) < 300:
                    print("[启动] 服务已就绪，正在打开浏览器...")
                    webbrowser.open(url, new=1, autoraise=True)
                    return
        except Exception:
            pass
        time.sleep(max(0.2, float(interval_sec)))
    print(f"[启动] {timeout_sec}s 内未检测到服务就绪，请手动访问: {url}")


def main() -> None:
    parser = argparse.ArgumentParser(description="GISDataMonitor 运行时启动器")
    parser.add_argument("--host", default="127.0.0.1", help="监听地址，默认 127.0.0.1")
    parser.add_argument("--port", type=int, default=8080, help="监听端口，默认 8080")
    parser.add_argument("--no-browser", action="store_true", help="启动后不自动打开浏览器")
    parser.add_argument("--disable-startup-sync", action="store_true", help="禁用启动时同步")
    args = parser.parse_args()

    _clear_proxy_env()

    runtime_root = _runtime_root()
    backend_dir = (runtime_root / "app" / "backend").resolve()
    frontend_dir = (runtime_root / "app" / "frontend").resolve()
    sqlite_path = (backend_dir / "gisdatamonitor.sqlite3").resolve()
    if not sqlite_path.exists():
        raise SystemExit(f"数据库不存在: {sqlite_path}")
    if not frontend_dir.exists():
        raise SystemExit(f"前端目录不存在: {frontend_dir}")

    os.environ.setdefault("GISDATAMONITOR_RUNTIME_ROOT", str(runtime_root))
    os.environ.setdefault("GISDATAMONITOR_FRONTEND_DIR", str(frontend_dir))
    os.environ.setdefault("GISDATAMONITOR_DATABASE_URL", f"sqlite:///{sqlite_path.as_posix()}")
    if args.disable_startup_sync:
        os.environ["GISDATAMONITOR_SYNC_RUN_ON_STARTUP"] = "false"

    final_port = _resolve_port(args.host, args.port)
    url = f"http://{args.host}:{final_port}/"
    print(f"[启动] GISDataMonitor 服务地址: {url}")
    if args.disable_startup_sync:
        print("[启动] 服务初始化中（已禁用启动同步）")
    else:
        print("[启动] 服务初始化中（含启动同步，阻塞模式）")
    if not args.no_browser:
        wait_timeout = _env_int(
            "GISDATAMONITOR_BROWSER_WAIT_READY_TIMEOUT_SEC",
            default=900,
            minimum=60,
        )
        print(f"[启动] 待服务就绪后自动打开: {url}")
        threading.Thread(
            target=_wait_for_health_and_open,
            kwargs={
                "url": url,
                "host": args.host,
                "port": final_port,
                "timeout_sec": wait_timeout,
            },
            daemon=True,
        ).start()

    uvicorn.run(
        "gisdatamonitor_backend.main:app",
        host=args.host,
        port=final_port,
        reload=False,
        log_level="info",
    )


if __name__ == "__main__":
    main()
