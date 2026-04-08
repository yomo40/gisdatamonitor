from __future__ import annotations

import argparse
import base64
import fnmatch
import hashlib
import json
import os
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zipfile import ZIP_DEFLATED, ZipFile


ROOT = Path(__file__).resolve().parents[1]
BUILD_ROOT = ROOT / "build" / "full_offline_package"
DIST_DIR = ROOT / "dist"

DATA_DIR = ROOT / "data"
BACKEND_DIR = ROOT / "app" / "backend"
FRONTEND_DIR = ROOT / "app" / "frontend"

DB_PATH = BACKEND_DIR / "gisdatamonitor.sqlite3"
OFFLINE_DIR = BACKEND_DIR / "cache" / "offline"
CONNECTOR_CACHE_DIR = BACKEND_DIR / "cache" / "connectors"

FRONTEND_JS_FILES = ("main.js", "leaflet.js", "monitor.js")

SOURCE_LEAK_PATTERNS = (
    "*.py",
    "*.pyc",
    "*.ts",
    "*.tsx",
    "*.java",
    "*.go",
    "*.rs",
    "*.c",
    "*.cpp",
    "*.h",
)

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


def _safe_print(text: str, *, err: bool = False) -> None:
    stream = sys.stderr if err else sys.stdout
    try:
        print(text, file=stream)
    except UnicodeEncodeError:
        encoded = text.encode(stream.encoding or "utf-8", errors="replace")
        stream.buffer.write(encoded + b"\n")
        stream.flush()


def _run(cmd: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
    _safe_print(f"[cmd] {' '.join(cmd)}")
    child_env = os.environ.copy()
    child_env["PYTHONUTF8"] = "1"
    for key in PROXY_ENV_KEYS:
        child_env.pop(key, None)
    if env:
        child_env.update(env)
    proc = subprocess.run(
        cmd,
        cwd=cwd or ROOT,
        env=child_env,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
    )
    if proc.stdout.strip():
        _safe_print(proc.stdout.strip())
    if proc.stderr.strip():
        _safe_print(proc.stderr.strip(), err=True)
    if check and proc.returncode != 0:
        raise RuntimeError(f"command failed ({proc.returncode}): {' '.join(cmd)}")
    return proc


def _sha256_file(path: Path, chunk: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _ensure_tools(python_exe: str) -> None:
    _run([python_exe, "-m", "pip", "install", "--upgrade", "pip"], check=True)
    _run(
        [
            python_exe,
            "-m",
            "pip",
            "install",
            "pyinstaller>=6.10.0",
        ],
        check=True,
    )


def _scan_source_leaks(root: Path) -> list[str]:
    hits: list[str] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(root).as_posix()
        for pattern in SOURCE_LEAK_PATTERNS:
            if fnmatch.fnmatch(path.name.lower(), pattern):
                hits.append(rel)
                break
    return sorted(hits)


def _scan_zip_source_leaks(zip_path: Path) -> list[str]:
    hits: list[str] = []
    with ZipFile(zip_path, mode="r") as zf:
        for name in zf.namelist():
            lower_name = name.lower()
            base = Path(lower_name).name
            for pattern in SOURCE_LEAK_PATTERNS:
                if fnmatch.fnmatch(base, pattern):
                    hits.append(name)
                    break
    return sorted(hits)


def _run_data_gate(python_exe: str, strict: bool, report_path: Path) -> dict[str, Any]:
    cmd = [
        python_exe,
        str(ROOT / "scripts" / "package_data_full_zip.py"),
        "--output",
        str(DIST_DIR / "GISDataMonitor-data-full.zip"),
        "--dry-run",
        "--report",
        str(report_path),
    ]
    if strict:
        cmd.append("--strict")
    _run(cmd, check=True)
    return json.loads(report_path.read_text(encoding="utf-8"))


def _obfuscate_frontend_js(frontend_root: Path) -> None:
    assets_dir = frontend_root / "assets"
    for name in FRONTEND_JS_FILES:
        js_path = assets_dir / name
        if not js_path.exists():
            raise FileNotFoundError(f"frontend js not found: {js_path}")
        source = js_path.read_text(encoding="utf-8")
        payload = base64.b64encode(source.encode("utf-8")).decode("ascii")
        wrapped = (
            "(()=>{"
            f"const __p='{payload}';"
            "const __bin=atob(__p);"
            "const __bytes=Uint8Array.from(__bin,(c)=>c.charCodeAt(0));"
            "const __s=(typeof TextDecoder!=='undefined'?new TextDecoder('utf-8').decode(__bytes):decodeURIComponent(Array.from(__bytes,(b)=>'%'+b.toString(16).padStart(2,'0')).join('')));"
            "(0,eval)(__s);"
            "})();\n"
        )
        js_path.write_text(wrapped, encoding="utf-8")


def _copy_runtime_data(stage_root: Path) -> None:
    target_data = stage_root / "data"
    target_backend = stage_root / "app" / "backend"
    target_frontend = stage_root / "app" / "frontend"

    if target_data.exists():
        shutil.rmtree(target_data)
    shutil.copytree(DATA_DIR, target_data)

    if target_frontend.exists():
        shutil.rmtree(target_frontend)
    shutil.copytree(FRONTEND_DIR, target_frontend)
    _obfuscate_frontend_js(target_frontend)

    target_backend.mkdir(parents=True, exist_ok=True)
    shutil.copy2(DB_PATH, target_backend / "gisdatamonitor.sqlite3")

    cache_offline_target = target_backend / "cache" / "offline"
    cache_connectors_target = target_backend / "cache" / "connectors"
    cache_offline_target.mkdir(parents=True, exist_ok=True)
    cache_connectors_target.mkdir(parents=True, exist_ok=True)
    for path in OFFLINE_DIR.glob("*.json"):
        shutil.copy2(path, cache_offline_target / path.name)
    for path in CONNECTOR_CACHE_DIR.glob("*.json"):
        shutil.copy2(path, cache_connectors_target / path.name)


def _build_launcher_exe_pyinstaller(
    python_exe: str,
    output_root: Path,
) -> Path:
    if output_root.exists():
        shutil.rmtree(output_root)
    dist_dir = output_root / "dist"
    work_dir = output_root / "build"
    spec_dir = output_root
    output_root.mkdir(parents=True, exist_ok=True)

    cmd = [
        python_exe,
        "-m",
        "PyInstaller",
        "--noconfirm",
        "--clean",
        "--name",
        "GISDataMonitorLauncher",
        "--onedir",
        "--console",
        "--paths",
        str(BACKEND_DIR / "src"),
        "--hidden-import",
        "gisdatamonitor_backend.main",
        "--collect-submodules",
        "rasterio",
        "--exclude-module",
        "IPython",
        "--exclude-module",
        "torch",
        "--exclude-module",
        "torchvision",
        "--exclude-module",
        "matplotlib",
        "--exclude-module",
        "PySide6",
        "--exclude-module",
        "tkinter",
        "--exclude-module",
        "sympy",
        "--exclude-module",
        "numba",
        "--exclude-module",
        "scipy",
        str(BACKEND_DIR / "scripts" / "runtime_launcher.py"),
        "--distpath",
        str(dist_dir),
        "--workpath",
        str(work_dir),
        "--specpath",
        str(spec_dir),
    ]
    _run(cmd, check=True)
    launcher_dir = dist_dir / "GISDataMonitorLauncher"
    if not launcher_dir.exists():
        raise RuntimeError(f"pyinstaller output not found: {launcher_dir}")
    exe_path = launcher_dir / "GISDataMonitorLauncher.exe"
    if not exe_path.exists():
        raise RuntimeError(f"launcher exe not found: {exe_path}")
    return launcher_dir


def _write_start_scripts(stage_root: Path) -> None:
    bat = stage_root / "start_gisdatamonitor.bat"
    ps1 = stage_root / "start_gisdatamonitor.ps1"
    bat.write_text(
        "@echo off\r\n"
        "setlocal\r\n"
        "set ROOT=%~dp0\r\n"
        "\"%ROOT%GISDataMonitorLauncher\\GISDataMonitorLauncher.exe\" %*\r\n",
        encoding="utf-8",
    )
    ps1.write_text(
        "$root = Split-Path -Parent $MyInvocation.MyCommand.Path\n"
        "& (Join-Path $root 'GISDataMonitorLauncher\\GISDataMonitorLauncher.exe') @args\n"
        "exit $LASTEXITCODE\n",
        encoding="utf-8",
    )


def _get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _smoke_test_launcher(stage_package_root: Path, timeout_sec: int = 30) -> None:
    launcher_exe = stage_package_root / "GISDataMonitorLauncher" / "GISDataMonitorLauncher.exe"
    if not launcher_exe.exists():
        raise RuntimeError(f"launcher missing for smoke test: {launcher_exe}")
    port = _get_free_port()
    cmd = [
        str(launcher_exe),
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        "--no-browser",
        "--disable-startup-sync",
    ]
    proc = subprocess.Popen(
        cmd,
        cwd=str(stage_package_root),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    start = time.time()
    url = f"http://127.0.0.1:{port}/api/v1/system/health"
    try:
        while time.time() - start < timeout_sec:
            if proc.poll() is not None:
                stdout, stderr = proc.communicate(timeout=3)
                raise RuntimeError(
                    f"smoke test launcher exited early ({proc.returncode})\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}"
                )
            try:
                with urllib.request.urlopen(url, timeout=2) as response:
                    if response.status == 200:
                        return
            except (urllib.error.URLError, TimeoutError):
                time.sleep(1)
                continue
        stdout, stderr = proc.communicate(timeout=3)
        raise RuntimeError(
            f"smoke test timeout ({timeout_sec}s)\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        )
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()


def _zip_dir(source_dir: Path, zip_path: Path) -> tuple[int, int]:
    if zip_path.exists():
        zip_path.unlink()
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    file_count = 0
    total_bytes = 0
    with ZipFile(zip_path, mode="w", compression=ZIP_DEFLATED, compresslevel=6, allowZip64=True) as zf:
        for path in sorted(source_dir.rglob("*")):
            if not path.is_file():
                continue
            rel = path.relative_to(source_dir.parent).as_posix()
            zf.write(path, arcname=rel)
            file_count += 1
            total_bytes += int(path.stat().st_size)
    return file_count, total_bytes


def _build_manifest(stage_package_root: Path, zip_path: Path, gate_report: dict[str, Any]) -> dict[str, Any]:
    files: list[dict[str, Any]] = []
    total = 0
    for path in sorted(stage_package_root.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(stage_package_root.parent).as_posix()
        size = int(path.stat().st_size)
        total += size
        files.append({"path": rel, "size": size, "sha256": _sha256_file(path)})
    return {
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "package_file": zip_path.name,
        "file_count": len(files),
        "total_bytes": total,
        "gate_report": gate_report,
        "files": files,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="构建带全量数据的离线可运行程序包（含代码保护）")
    parser.add_argument(
        "--output",
        default=str(DIST_DIR / "GISDataMonitor-full-offline-win11-x64.zip"),
        help="输出 zip 路径",
    )
    parser.add_argument("--strict", action="store_true", help="启用严格新闻量门禁")
    parser.add_argument("--python-exe", default=sys.executable, help="用于构建的 Python 解释器")
    parser.add_argument("--skip-tool-install", action="store_true", help="跳过 pyinstaller/pyarmor 安装")
    args = parser.parse_args()

    output_zip = Path(args.output).resolve()
    report_path = output_zip.with_name("GISDataMonitor-full-offline-win11-x64.pack_report.json")
    manifest_path = output_zip.with_name("GISDataMonitor-full-offline-win11-x64.manifest.json")
    sha_path = output_zip.with_name("GISDataMonitor-full-offline-win11-x64.sha256")

    BUILD_ROOT.mkdir(parents=True, exist_ok=True)
    stage_root = BUILD_ROOT / "stage"
    stage_package_root = stage_root / "GISDataMonitor"
    pyinstaller_dist = BUILD_ROOT / "pyinstaller_dist"
    gate_report_path = BUILD_ROOT / "data_gate_report.json"

    report: dict[str, Any] = {
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "output_zip": str(output_zip),
        "strict": bool(args.strict),
        "steps": [],
    }
    try:
        if stage_root.exists():
            shutil.rmtree(stage_root)
        stage_package_root.mkdir(parents=True, exist_ok=True)

        report["steps"].append({"name": "data_gate", "started_at": datetime.now(tz=UTC).isoformat()})
        gate_report = _run_data_gate(args.python_exe, args.strict, gate_report_path)
        report["gate"] = gate_report

        if not args.skip_tool_install:
            report["steps"].append({"name": "install_tools", "started_at": datetime.now(tz=UTC).isoformat()})
            _ensure_tools(args.python_exe)

        report["steps"].append({"name": "build_launcher_pyinstaller", "started_at": datetime.now(tz=UTC).isoformat()})
        launcher_dir = _build_launcher_exe_pyinstaller(
            args.python_exe,
            output_root=pyinstaller_dist,
        )

        report["steps"].append({"name": "copy_runtime_data", "started_at": datetime.now(tz=UTC).isoformat()})
        shutil.copytree(launcher_dir, stage_package_root / "GISDataMonitorLauncher", dirs_exist_ok=True)
        _copy_runtime_data(stage_package_root)
        _write_start_scripts(stage_package_root)

        report["steps"].append({"name": "smoke_test", "started_at": datetime.now(tz=UTC).isoformat()})
        _smoke_test_launcher(stage_package_root)

        report["steps"].append({"name": "source_scan_pre_zip", "started_at": datetime.now(tz=UTC).isoformat()})
        source_hits_pre = _scan_source_leaks(stage_package_root)
        report["source_scan_pre_zip"] = {
            "passed": len(source_hits_pre) == 0,
            "hits": source_hits_pre[:200],
            "hit_count": len(source_hits_pre),
        }
        if source_hits_pre:
            raise RuntimeError(f"source leakage detected before zip ({len(source_hits_pre)} files)")

        report["steps"].append({"name": "zip", "started_at": datetime.now(tz=UTC).isoformat()})
        file_count, total_bytes = _zip_dir(stage_package_root, output_zip)
        zip_sha = _sha256_file(output_zip)
        sha_path.write_text(f"{zip_sha} *{output_zip.name}\n", encoding="utf-8")

        report["steps"].append({"name": "source_scan_post_zip", "started_at": datetime.now(tz=UTC).isoformat()})
        source_hits_post = _scan_zip_source_leaks(output_zip)
        report["source_scan_post_zip"] = {
            "passed": len(source_hits_post) == 0,
            "hits": source_hits_post[:200],
            "hit_count": len(source_hits_post),
        }
        if source_hits_post:
            raise RuntimeError(f"source leakage detected in zip ({len(source_hits_post)} files)")

        manifest = _build_manifest(stage_package_root, output_zip, gate_report)
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

        report["result"] = "success"
        report["package"] = {
            "file_count": file_count,
            "total_bytes": total_bytes,
            "zip_size_bytes": int(output_zip.stat().st_size),
            "zip_sha256": zip_sha,
        }
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

        print(f"[完成] {output_zip}")
        print(f"[完成] {manifest_path}")
        print(f"[完成] {sha_path}")
        print(f"[完成] {report_path}")
    except Exception as exc:  # noqa: BLE001
        report["result"] = "failed"
        report["error"] = str(exc)
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[失败] {exc}", file=sys.stderr)
        print(f"[报告] {report_path}", file=sys.stderr)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
