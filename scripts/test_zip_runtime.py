from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from zipfile import ZipFile


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ZIP = ROOT / "dist" / "GISDataMonitor-full-offline-win11-x64.zip"
DEFAULT_REPORT = ROOT / "dist" / "zip_runtime_test_report.json"
DEFAULT_EXTRACT_DIR = ROOT / "build" / "zip_runtime_test"


@dataclass
class RuntimeProbeResult:
    name: str
    passed: bool
    ready_seconds: float
    checks: dict[str, Any]
    bugs: list[str]
    stdout_tail: list[str]
    stderr_tail: list[str]


def _read_url(url: str, timeout_sec: float = 10.0) -> tuple[int, bytes]:
    request = urllib.request.Request(url, headers={"User-Agent": "zip-runtime-test/1.0"})
    with urllib.request.urlopen(request, timeout=timeout_sec) as response:
        status = int(getattr(response, "status", 200))
        body = response.read()
    return status, body


def _wait_health(base_url: str, timeout_sec: int) -> float:
    started = time.perf_counter()
    deadline = started + float(timeout_sec)
    health_url = f"{base_url}/api/v1/system/health"
    while time.perf_counter() < deadline:
        try:
            status, _ = _read_url(health_url, timeout_sec=2.0)
            if status == 200:
                return time.perf_counter() - started
        except Exception:
            pass
        time.sleep(1.0)
    raise TimeoutError(f"health timeout > {timeout_sec}s")


def _kill_process_tree(proc: subprocess.Popen[str] | None) -> None:
    if proc is None or proc.poll() is not None:
        return
    try:
        subprocess.run(
            ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
            capture_output=True,
            check=False,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    except Exception:
        try:
            proc.kill()
        except Exception:
            return


def _tail_lines(text: str, limit: int = 120) -> list[str]:
    lines = (text or "").splitlines()
    if len(lines) <= limit:
        return lines
    return lines[-limit:]


def _probe_runtime(
    *,
    name: str,
    package_root: Path,
    port: int,
    disable_startup_sync: bool,
    health_timeout_sec: int,
    max_ready_sec: int,
    min_events_24h: int,
) -> RuntimeProbeResult:
    start_script = package_root / "GISDataMonitor" / "start_gisdatamonitor.bat"
    if not start_script.exists():
        return RuntimeProbeResult(
            name=name,
            passed=False,
            ready_seconds=0.0,
            checks={},
            bugs=[f"missing start script: {start_script}"],
            stdout_tail=[],
            stderr_tail=[],
        )

    args = [str(start_script), "--host", "127.0.0.1", "--port", str(port), "--no-browser"]
    if disable_startup_sync:
        args.append("--disable-startup-sync")

    proc: subprocess.Popen[str] | None = None
    checks: dict[str, Any] = {}
    bugs: list[str] = []
    ready_seconds = 0.0
    stdout = ""
    stderr = ""
    try:
        proc = subprocess.Popen(
            ["cmd.exe", "/c", *args],
            cwd=str(start_script.parent),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        base_url = f"http://127.0.0.1:{port}"
        ready_seconds = _wait_health(base_url=base_url, timeout_sec=health_timeout_sec)
        checks["ready_seconds"] = round(ready_seconds, 2)
        checks["ready_threshold_sec"] = max_ready_sec
        checks["ready_within_threshold"] = bool(ready_seconds <= float(max_ready_sec))
        if ready_seconds > float(max_ready_sec):
            bugs.append(f"{name}: service ready too slow ({ready_seconds:.1f}s > {max_ready_sec}s)")

        status, html = _read_url(f"{base_url}/", timeout_sec=20.0)
        html_text = html.decode("utf-8", errors="replace")
        checks["index_status"] = status
        checks["index_has_maplibre"] = "maplibre-gl.js" in html_text
        if status != 200 or not checks["index_has_maplibre"]:
            bugs.append(f"{name}: index page invalid (status={status})")

        status, js_body = _read_url(f"{base_url}/assets/main.js", timeout_sec=20.0)
        js_text = js_body.decode("utf-8", errors="replace")
        checks["main_js_status"] = status
        checks["main_js_bytes"] = len(js_body)
        checks["main_js_uses_textdecoder"] = "TextDecoder('utf-8')" in js_text
        if status != 200 or len(js_body) < 1000:
            bugs.append(f"{name}: main.js invalid or too small")
        if "atob(__p)" in js_text and "TextDecoder('utf-8')" not in js_text:
            bugs.append(f"{name}: main.js obfuscation missing utf-8 decode path")

        status, layers_body = _read_url(f"{base_url}/api/v1/layers", timeout_sec=20.0)
        layers = json.loads(layers_body.decode("utf-8", errors="replace"))
        checks["layers_status"] = status
        checks["layers_count"] = len(layers.get("layers") or [])
        if status != 200 or checks["layers_count"] <= 0:
            bugs.append(f"{name}: /layers empty or failed")

        status, events_body = _read_url(
            f"{base_url}/api/v1/events/enriched?page=1&page_size=200&hours=24",
            timeout_sec=20.0,
        )
        events = json.loads(events_body.decode("utf-8", errors="replace"))
        event_total = int(events.get("total") or 0)
        checks["events_status"] = status
        checks["events_24h_total"] = event_total
        checks["events_24h_min_required"] = min_events_24h
        if status != 200 or event_total < min_events_24h:
            bugs.append(
                f"{name}: /events/enriched insufficient 24h events ({event_total} < {min_events_24h})"
            )

        status, playback_body = _read_url(
            f"{base_url}/api/v1/timeline/playback?scene_id=world&window=24h&step_minutes=30&frame_limit=180",
            timeout_sec=25.0,
        )
        playback = json.loads(playback_body.decode("utf-8", errors="replace"))
        frames = playback.get("frames") or []
        event_frames = sum(1 for frame in frames if int(frame.get("event_count") or 0) > 0)
        checks["playback_status"] = status
        checks["playback_frames"] = len(frames)
        checks["playback_event_frames"] = int(event_frames)
        if status != 200 or not frames:
            bugs.append(f"{name}: /timeline/playback empty or failed")
        if event_frames <= 0:
            bugs.append(f"{name}: /timeline/playback has no event frames")

        passed = not bugs
        return RuntimeProbeResult(
            name=name,
            passed=passed,
            ready_seconds=ready_seconds,
            checks=checks,
            bugs=bugs,
            stdout_tail=[],
            stderr_tail=[],
        )
    except Exception as exc:  # noqa: BLE001
        bugs.append(f"{name}: exception: {exc}")
        return RuntimeProbeResult(
            name=name,
            passed=False,
            ready_seconds=ready_seconds,
            checks=checks,
            bugs=bugs,
            stdout_tail=[],
            stderr_tail=[],
        )
    finally:
        _kill_process_tree(proc)
        if proc is not None:
            try:
                out_text, err_text = proc.communicate(timeout=8)
            except subprocess.TimeoutExpired:
                out_text, err_text = "", ""
            stdout = out_text or ""
            stderr = err_text or ""
        # mutable assign for dataclass returned above is not possible here; logs are added in caller.
        if stdout or stderr:
            # no-op hook: caller will re-run tails from proc output in returned checks via this marker
            pass


def _run_phase(
    *,
    phase_name: str,
    package_root: Path,
    port: int,
    disable_startup_sync: bool,
    health_timeout_sec: int,
    max_ready_sec: int,
    min_events_24h: int,
) -> RuntimeProbeResult:
    # Run probe once and include process tails by re-running with direct capture.
    start_script = package_root / "GISDataMonitor" / "start_gisdatamonitor.bat"
    args = [str(start_script), "--host", "127.0.0.1", "--port", str(port), "--no-browser"]
    if disable_startup_sync:
        args.append("--disable-startup-sync")
    proc: subprocess.Popen[str] | None = None
    try:
        proc = subprocess.Popen(
            ["cmd.exe", "/c", *args],
            cwd=str(start_script.parent),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        base_url = f"http://127.0.0.1:{port}"
        started = time.perf_counter()
        bugs: list[str] = []
        checks: dict[str, Any] = {}

        try:
            ready_seconds = _wait_health(base_url=base_url, timeout_sec=health_timeout_sec)
            checks["ready_seconds"] = round(ready_seconds, 2)
            checks["ready_threshold_sec"] = max_ready_sec
            checks["ready_within_threshold"] = bool(ready_seconds <= float(max_ready_sec))
            if ready_seconds > float(max_ready_sec):
                bugs.append(f"{phase_name}: service ready too slow ({ready_seconds:.1f}s > {max_ready_sec}s)")
        except Exception as exc:  # noqa: BLE001
            bugs.append(f"{phase_name}: health check failed: {exc}")
            ready_seconds = time.perf_counter() - started

        if not bugs or "health check failed" not in bugs[0]:
            try:
                status, html = _read_url(f"{base_url}/", timeout_sec=20.0)
                html_text = html.decode("utf-8", errors="replace")
                checks["index_status"] = status
                checks["index_has_maplibre"] = "maplibre-gl.js" in html_text
                if status != 200 or not checks["index_has_maplibre"]:
                    bugs.append(f"{phase_name}: index page invalid (status={status})")
            except Exception as exc:  # noqa: BLE001
                bugs.append(f"{phase_name}: index request failed: {exc}")

            try:
                status, js_body = _read_url(f"{base_url}/assets/main.js", timeout_sec=20.0)
                js_text = js_body.decode("utf-8", errors="replace")
                checks["main_js_status"] = status
                checks["main_js_bytes"] = len(js_body)
                checks["main_js_uses_textdecoder"] = "TextDecoder('utf-8')" in js_text
                if status != 200 or len(js_body) < 1000:
                    bugs.append(f"{phase_name}: main.js invalid or too small")
                if "atob(__p)" in js_text and "TextDecoder('utf-8')" not in js_text:
                    bugs.append(f"{phase_name}: main.js obfuscation missing utf-8 decode path")
            except Exception as exc:  # noqa: BLE001
                bugs.append(f"{phase_name}: main.js request failed: {exc}")

            try:
                status, layers_body = _read_url(f"{base_url}/api/v1/layers", timeout_sec=20.0)
                layers = json.loads(layers_body.decode("utf-8", errors="replace"))
                checks["layers_status"] = status
                checks["layers_count"] = len(layers.get("layers") or [])
                if status != 200 or checks["layers_count"] <= 0:
                    bugs.append(f"{phase_name}: /layers empty or failed")
            except Exception as exc:  # noqa: BLE001
                bugs.append(f"{phase_name}: /layers request failed: {exc}")

            try:
                status, events_body = _read_url(
                    f"{base_url}/api/v1/events/enriched?page=1&page_size=200&hours=24",
                    timeout_sec=20.0,
                )
                events = json.loads(events_body.decode("utf-8", errors="replace"))
                event_total = int(events.get("total") or 0)
                checks["events_status"] = status
                checks["events_24h_total"] = event_total
                checks["events_24h_min_required"] = min_events_24h
                if status != 200 or event_total < min_events_24h:
                    bugs.append(
                        f"{phase_name}: /events/enriched insufficient 24h events ({event_total} < {min_events_24h})"
                    )
            except Exception as exc:  # noqa: BLE001
                bugs.append(f"{phase_name}: /events/enriched request failed: {exc}")

            try:
                status, playback_body = _read_url(
                    f"{base_url}/api/v1/timeline/playback?scene_id=world&window=24h&step_minutes=30&frame_limit=180",
                    timeout_sec=25.0,
                )
                playback = json.loads(playback_body.decode("utf-8", errors="replace"))
                frames = playback.get("frames") or []
                event_frames = sum(1 for frame in frames if int(frame.get("event_count") or 0) > 0)
                checks["playback_status"] = status
                checks["playback_frames"] = len(frames)
                checks["playback_event_frames"] = int(event_frames)
                if status != 200 or not frames:
                    bugs.append(f"{phase_name}: /timeline/playback empty or failed")
                if event_frames <= 0:
                    bugs.append(f"{phase_name}: /timeline/playback has no event frames")
            except Exception as exc:  # noqa: BLE001
                bugs.append(f"{phase_name}: /timeline/playback request failed: {exc}")

        return RuntimeProbeResult(
            name=phase_name,
            passed=len(bugs) == 0,
            ready_seconds=round(ready_seconds, 2),
            checks=checks,
            bugs=bugs,
            stdout_tail=[],
            stderr_tail=[],
        )
    finally:
        _kill_process_tree(proc)
        stdout_tail: list[str] = []
        stderr_tail: list[str] = []
        if proc is not None:
            try:
                out_text, err_text = proc.communicate(timeout=8)
                stdout_tail = _tail_lines(out_text or "", limit=120)
                stderr_tail = _tail_lines(err_text or "", limit=120)
            except subprocess.TimeoutExpired:
                stdout_tail = []
                stderr_tail = []
        # attach tails by returning a patched result when possible
        # caller will overwrite this with actual phase result if needed
        if "phase_result" in locals():
            phase_result.stdout_tail = stdout_tail
            phase_result.stderr_tail = stderr_tail


def _extract_zip(zip_path: Path, target_dir: Path) -> None:
    if target_dir.exists():
        shutil.rmtree(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    with ZipFile(zip_path, mode="r") as zf:
        zf.extractall(path=target_dir)


def main() -> None:
    parser = argparse.ArgumentParser(description="Automatic runtime smoke test for packaged GISDataMonitor zip.")
    parser.add_argument("--zip", default=str(DEFAULT_ZIP), help="Path to GISDataMonitor full offline zip.")
    parser.add_argument("--extract-dir", default=str(DEFAULT_EXTRACT_DIR), help="Temporary extraction directory.")
    parser.add_argument("--report", default=str(DEFAULT_REPORT), help="JSON report output path.")
    parser.add_argument("--port-base", type=int, default=8130, help="Base port for test phases.")
    parser.add_argument("--health-timeout-no-sync", type=int, default=90)
    parser.add_argument("--health-timeout-default", type=int, default=360)
    parser.add_argument("--max-ready-sec-no-sync", type=int, default=45)
    parser.add_argument("--max-ready-sec-default", type=int, default=120)
    parser.add_argument("--min-events-24h", type=int, default=20)
    args = parser.parse_args()

    zip_path = Path(args.zip).resolve()
    extract_dir = Path(args.extract_dir).resolve()
    report_path = Path(args.report).resolve()

    report: dict[str, Any] = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "zip_path": str(zip_path),
        "extract_dir": str(extract_dir),
        "phases": [],
        "bugs": [],
        "result": "failed",
    }

    if not zip_path.exists():
        report["bugs"].append(f"zip not found: {zip_path}")
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        raise SystemExit(2)

    _extract_zip(zip_path=zip_path, target_dir=extract_dir)
    package_root = extract_dir

    phases = [
        {
            "name": "no_startup_sync",
            "disable_startup_sync": True,
            "port": int(args.port_base),
            "health_timeout": int(args.health_timeout_no_sync),
            "max_ready_sec": int(args.max_ready_sec_no_sync),
        },
        {
            "name": "default_startup",
            "disable_startup_sync": False,
            "port": int(args.port_base) + 1,
            "health_timeout": int(args.health_timeout_default),
            "max_ready_sec": int(args.max_ready_sec_default),
        },
    ]

    all_bugs: list[str] = []
    for phase in phases:
        start_script = package_root / "GISDataMonitor" / "start_gisdatamonitor.bat"
        args_cmd = [str(start_script), "--host", "127.0.0.1", "--port", str(phase["port"]), "--no-browser"]
        if phase["disable_startup_sync"]:
            args_cmd.append("--disable-startup-sync")

        proc: subprocess.Popen[str] | None = None
        phase_report: dict[str, Any] = {
            "name": phase["name"],
            "port": phase["port"],
            "disable_startup_sync": phase["disable_startup_sync"],
            "checks": {},
            "bugs": [],
            "stdout_tail": [],
            "stderr_tail": [],
            "passed": False,
        }
        try:
            proc = subprocess.Popen(
                ["cmd.exe", "/c", *args_cmd],
                cwd=str(start_script.parent),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
            )

            base_url = f"http://127.0.0.1:{phase['port']}"
            ready_seconds = _wait_health(base_url=base_url, timeout_sec=int(phase["health_timeout"]))
            phase_report["checks"]["ready_seconds"] = round(ready_seconds, 2)
            phase_report["checks"]["max_ready_sec"] = int(phase["max_ready_sec"])
            if ready_seconds > float(phase["max_ready_sec"]):
                phase_report["bugs"].append(
                    f"{phase['name']}: ready too slow ({ready_seconds:.1f}s > {phase['max_ready_sec']}s)"
                )

            checks = phase_report["checks"]
            status, html = _read_url(f"{base_url}/", timeout_sec=20.0)
            html_text = html.decode("utf-8", errors="replace")
            checks["index_status"] = status
            checks["index_has_maplibre"] = "maplibre-gl.js" in html_text
            if status != 200 or not checks["index_has_maplibre"]:
                phase_report["bugs"].append(f"{phase['name']}: index invalid")

            status, js_body = _read_url(f"{base_url}/assets/main.js", timeout_sec=20.0)
            js_text = js_body.decode("utf-8", errors="replace")
            checks["main_js_status"] = status
            checks["main_js_size"] = len(js_body)
            checks["main_js_utf8_decoder"] = "TextDecoder('utf-8')" in js_text
            if status != 200 or len(js_body) < 1000:
                phase_report["bugs"].append(f"{phase['name']}: main.js invalid")
            if "atob(__p)" in js_text and "TextDecoder('utf-8')" not in js_text:
                phase_report["bugs"].append(f"{phase['name']}: main.js utf8 decode missing")

            status, layers_body = _read_url(f"{base_url}/api/v1/layers", timeout_sec=20.0)
            layers = json.loads(layers_body.decode("utf-8", errors="replace"))
            checks["layers_status"] = status
            checks["layers_count"] = len(layers.get("layers") or [])
            if status != 200 or checks["layers_count"] <= 0:
                phase_report["bugs"].append(f"{phase['name']}: layers empty")

            status, events_body = _read_url(
                f"{base_url}/api/v1/events/enriched?page=1&page_size=200&hours=24",
                timeout_sec=20.0,
            )
            events = json.loads(events_body.decode("utf-8", errors="replace"))
            event_total = int(events.get("total") or 0)
            checks["events_status"] = status
            checks["events_24h_total"] = event_total
            checks["events_24h_min_required"] = int(args.min_events_24h)
            if status != 200 or event_total < int(args.min_events_24h):
                phase_report["bugs"].append(
                    f"{phase['name']}: events insufficient ({event_total} < {int(args.min_events_24h)})"
                )

            status, playback_body = _read_url(
                f"{base_url}/api/v1/timeline/playback?scene_id=world&window=24h&step_minutes=30&frame_limit=180",
                timeout_sec=25.0,
            )
            playback = json.loads(playback_body.decode("utf-8", errors="replace"))
            frames = playback.get("frames") or []
            event_frames = sum(1 for frame in frames if int(frame.get("event_count") or 0) > 0)
            checks["playback_status"] = status
            checks["playback_frames"] = len(frames)
            checks["playback_event_frames"] = int(event_frames)
            if status != 200 or not frames:
                phase_report["bugs"].append(f"{phase['name']}: playback empty")
            if event_frames <= 0:
                phase_report["bugs"].append(f"{phase['name']}: playback has no event frames")

            phase_report["passed"] = len(phase_report["bugs"]) == 0
        except Exception as exc:  # noqa: BLE001
            phase_report["bugs"].append(f"{phase['name']}: exception: {exc}")
            phase_report["passed"] = False
        finally:
            _kill_process_tree(proc)
            if proc is not None:
                try:
                    out_text, err_text = proc.communicate(timeout=10)
                except subprocess.TimeoutExpired:
                    out_text, err_text = "", ""
                phase_report["stdout_tail"] = _tail_lines(out_text or "", limit=120)
                phase_report["stderr_tail"] = _tail_lines(err_text or "", limit=120)

        report["phases"].append(phase_report)
        all_bugs.extend(phase_report["bugs"])

    report["bugs"] = all_bugs
    report["result"] = "success" if not all_bugs else "failed"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    if all_bugs:
        print("[FAILED] zip runtime test found bugs:")
        for bug in all_bugs:
            print(f"  - {bug}")
        print(f"[REPORT] {report_path}")
        raise SystemExit(2)

    print("[OK] zip runtime test passed")
    print(f"[REPORT] {report_path}")


if __name__ == "__main__":
    main()
