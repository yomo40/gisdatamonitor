from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zipfile import ZIP_DEFLATED, ZipFile

import sqlite3


ROOT = Path(__file__).resolve().parents[1]
DIST_DIR = ROOT / "dist"
DATA_DIR = ROOT / "data"
BACKEND_DIR = ROOT / "app" / "backend"
DB_PATH = BACKEND_DIR / "gisdatamonitor.sqlite3"
OFFLINE_DIR = BACKEND_DIR / "cache" / "offline"
CONNECTOR_CACHE_DIR = BACKEND_DIR / "cache" / "connectors"

OFFLINE_FILES = (
    OFFLINE_DIR / "events_last24h.json",
    OFFLINE_DIR / "events_last7d.json",
    OFFLINE_DIR / "events_last30d.json",
)

FORBIDDEN_EXTENSIONS = {
    ".py",
    ".pyc",
    ".ts",
    ".tsx",
    ".js",
    ".java",
    ".go",
    ".rs",
    ".c",
    ".cpp",
    ".h",
}
FORBIDDEN_TOP_LEVEL = {"scripts", "docs", ".git"}
FORBIDDEN_PREFIXES = ("app/backend/src/", "app/frontend/")

THRESHOLDS = {
    "event_30d": 2000,
    "event_7d": 300,
    "active_days_30d": 25,
    "source_count_30d": 3,
    "event_72h": 1,  # >0
    "geo_event_30d": 1000,
    "geo_event_72h": 50,
}


@dataclass
class ScanResult:
    passed: bool
    hits: list[str]


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def _sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _collect_package_files() -> list[Path]:
    missing: list[Path] = []
    files: list[Path] = []

    if not DATA_DIR.exists():
        missing.append(DATA_DIR)
    else:
        files.extend(path for path in DATA_DIR.rglob("*") if path.is_file())

    if not DB_PATH.exists():
        missing.append(DB_PATH)
    else:
        files.append(DB_PATH)

    for file_path in OFFLINE_FILES:
        if not file_path.exists():
            missing.append(file_path)
        else:
            files.append(file_path)

    if CONNECTOR_CACHE_DIR.exists():
        files.extend(path for path in CONNECTOR_CACHE_DIR.glob("*.json") if path.is_file())
    else:
        missing.append(CONNECTOR_CACHE_DIR)

    if missing:
        missing_text = ", ".join(str(path) for path in missing)
        raise FileNotFoundError(f"打包白名单路径缺失: {missing_text}")

    dedup = sorted(set(files))
    return dedup


def _scan_relative_path(rel_path: str) -> str | None:
    normalized = rel_path.replace("\\", "/")
    lower = normalized.lower()
    parts = [segment for segment in normalized.split("/") if segment]
    if not parts:
        return None

    top = parts[0].lower()
    if top in FORBIDDEN_TOP_LEVEL:
        return f"命中禁止目录: {normalized}"

    for prefix in FORBIDDEN_PREFIXES:
        if lower.startswith(prefix):
            return f"命中禁止目录前缀: {normalized}"

    for segment in parts:
        if segment.lower().startswith(".env"):
            return f"命中禁止 .env* 路径: {normalized}"

    suffix = Path(normalized).suffix.lower()
    if suffix in FORBIDDEN_EXTENSIONS:
        return f"命中禁止源码后缀({suffix}): {normalized}"
    return None


def _scan_files_for_leakage(paths: list[Path]) -> ScanResult:
    hits: list[str] = []
    for path in paths:
        rel_path = _rel(path)
        hit = _scan_relative_path(rel_path)
        if hit:
            hits.append(hit)
    return ScanResult(passed=not hits, hits=hits)


def _scan_archive_for_leakage(zip_path: Path) -> ScanResult:
    hits: list[str] = []
    with ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            hit = _scan_relative_path(name)
            if hit:
                hits.append(hit)
    return ScanResult(passed=not hits, hits=hits)


def _read_gate_metrics(db_path: Path) -> dict[str, int]:
    metrics: dict[str, int] = {}
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        metrics["event_30d"] = int(
            cur.execute("SELECT COUNT(*) FROM event_normalized WHERE event_time >= datetime('now', '-30 day')").fetchone()[0]
        )
        metrics["event_7d"] = int(
            cur.execute("SELECT COUNT(*) FROM event_normalized WHERE event_time >= datetime('now', '-7 day')").fetchone()[0]
        )
        metrics["active_days_30d"] = int(
            cur.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT substr(event_time, 1, 10) AS day_key
                    FROM event_normalized
                    WHERE event_time >= datetime('now', '-30 day')
                    GROUP BY day_key
                )
                """
            ).fetchone()[0]
        )
        metrics["source_count_30d"] = int(
            cur.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT source
                    FROM event_normalized
                    WHERE event_time >= datetime('now', '-30 day')
                    GROUP BY source
                )
                """
            ).fetchone()[0]
        )
        metrics["event_72h"] = int(
            cur.execute("SELECT COUNT(*) FROM event_normalized WHERE event_time >= datetime('now', '-3 day')").fetchone()[0]
        )
        metrics["geo_event_30d"] = int(
            cur.execute(
                """
                SELECT COUNT(*)
                FROM event_normalized
                WHERE event_time >= datetime('now', '-30 day')
                  AND geometry_json IS NOT NULL
                  AND TRIM(COALESCE(geometry_json, '')) <> ''
                """
            ).fetchone()[0]
        )
        metrics["geo_event_72h"] = int(
            cur.execute(
                """
                SELECT COUNT(*)
                FROM event_normalized
                WHERE event_time >= datetime('now', '-3 day')
                  AND geometry_json IS NOT NULL
                  AND TRIM(COALESCE(geometry_json, '')) <> ''
                """
            ).fetchone()[0]
        )
    return metrics


def _evaluate_gate(metrics: dict[str, int]) -> tuple[bool, dict[str, dict[str, int]]]:
    gaps: dict[str, dict[str, int]] = {}
    for key, expected in THRESHOLDS.items():
        actual = int(metrics.get(key, 0))
        if actual < expected:
            gaps[key] = {"expected_min": expected, "actual": actual}
    return not gaps, gaps


def _run_remediation(python_exe: str) -> list[dict[str, Any]]:
    commands = [
        [
            python_exe,
            str(BACKEND_DIR / "scripts" / "backfill_last30d_history.py"),
            "--days",
            "30",
        ],
        [
            python_exe,
            str(BACKEND_DIR / "scripts" / "pull_last24h_once.py"),
            "--max-cycles",
            "4",
        ],
    ]
    records: list[dict[str, Any]] = []
    for cmd in commands:
        started = time.time()
        proc = subprocess.run(
            cmd,
            cwd=ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        duration = round(time.time() - started, 3)
        tail_stdout = "\n".join(proc.stdout.splitlines()[-50:])
        tail_stderr = "\n".join(proc.stderr.splitlines()[-50:])
        records.append(
            {
                "cmd": cmd,
                "returncode": int(proc.returncode),
                "duration_sec": duration,
                "stdout_tail": tail_stdout,
                "stderr_tail": tail_stderr,
            }
        )
        if proc.returncode != 0:
            break
    return records


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_manifest(files: list[Path], output_zip: Path, gate_metrics: dict[str, int]) -> dict[str, Any]:
    file_entries: list[dict[str, Any]] = []
    total_bytes = 0
    for file_path in files:
        rel_path = _rel(file_path)
        size = int(file_path.stat().st_size)
        total_bytes += size
        file_entries.append(
            {
                "path": rel_path,
                "size": size,
                "sha256": _sha256_file(file_path),
            }
        )
    return {
        "generated_at": _now_iso(),
        "package_file": output_zip.name,
        "file_count": len(file_entries),
        "total_bytes": total_bytes,
        "gate_metrics": gate_metrics,
        "thresholds": THRESHOLDS,
        "files": file_entries,
    }


def _write_sha_file(zip_path: Path, sha_path: Path) -> str:
    digest = _sha256_file(zip_path)
    sha_path.write_text(f"{digest} *{zip_path.name}\n", encoding="utf-8")
    return digest


def _print_gate(title: str, metrics: dict[str, int], gaps: dict[str, dict[str, int]]) -> None:
    print(f"[门禁] {title}")
    for key in [
        "event_30d",
        "event_7d",
        "active_days_30d",
        "source_count_30d",
        "event_72h",
        "geo_event_30d",
        "geo_event_72h",
    ]:
        threshold = THRESHOLDS[key]
        actual = metrics.get(key, 0)
        status = "OK" if actual >= threshold else "FAIL"
        print(f"  - {key}: {actual} / >= {threshold} [{status}]")
    if gaps:
        print("  缺口:")
        for key, value in gaps.items():
            print(f"    * {key}: actual={value['actual']} < expected={value['expected_min']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="构建 GISDataMonitor-data-full.zip 离线数据包（ZIP64）")
    parser.add_argument(
        "--output",
        default=str(DIST_DIR / "GISDataMonitor-data-full.zip"),
        help="输出 ZIP 路径（默认 dist/GISDataMonitor-data-full.zip）",
    )
    parser.add_argument("--strict", action="store_true", help="启用严格门禁（新闻量+防泄漏）")
    parser.add_argument("--dry-run", action="store_true", help="仅执行门禁与扫描，不实际打包")
    parser.add_argument(
        "--report",
        default="",
        help="pack_report.json 输出路径（默认与 zip 同目录）",
    )
    parser.add_argument(
        "--python-exe",
        default=sys.executable,
        help="补数流程使用的 Python 可执行文件（默认当前解释器）",
    )
    args = parser.parse_args()

    output_zip = Path(args.output).resolve()
    manifest_path = output_zip.with_name("GISDataMonitor-data-full.manifest.json")
    sha_path = output_zip.with_name("GISDataMonitor-data-full.sha256")
    report_path = Path(args.report).resolve() if args.report else output_zip.with_name("pack_report.json")

    report: dict[str, Any] = {
        "generated_at": _now_iso(),
        "strict": bool(args.strict),
        "dry_run": bool(args.dry_run),
        "output_zip": str(output_zip),
        "manifest_path": str(manifest_path),
        "sha_path": str(sha_path),
        "report_path": str(report_path),
        "thresholds": THRESHOLDS,
    }

    try:
        files = _collect_package_files()
        report["candidate_file_count"] = len(files)
        report["candidate_total_bytes"] = int(sum(path.stat().st_size for path in files))
        print(f"[输入] 白名单文件数: {len(files)}")
        print(f"[输入] 白名单总大小: {report['candidate_total_bytes']} bytes")

        pre_scan = _scan_files_for_leakage(files)
        report["source_scan_pre"] = {"passed": pre_scan.passed, "hits": pre_scan.hits}
        print(f"[扫描] 打包前泄漏扫描: {'通过' if pre_scan.passed else '失败'}")
        if not pre_scan.passed:
            raise RuntimeError("打包前泄漏扫描失败")

        metrics_before = _read_gate_metrics(DB_PATH)
        gate_passed_before, gaps_before = _evaluate_gate(metrics_before)
        report["gate_before"] = {"passed": gate_passed_before, "metrics": metrics_before, "gaps": gaps_before}
        _print_gate("初检", metrics_before, gaps_before)

        remediation_records: list[dict[str, Any]] = []
        metrics_after = metrics_before
        gate_passed_after = gate_passed_before
        gaps_after = gaps_before

        if args.strict and not gate_passed_before:
            print("[门禁] 未达标，执行自动补数链路...")
            remediation_records = _run_remediation(args.python_exe)
            report["remediation"] = remediation_records

            failed_cmd = next((item for item in remediation_records if item.get("returncode", 1) != 0), None)
            if failed_cmd is not None:
                raise RuntimeError("补数命令执行失败")

            metrics_after = _read_gate_metrics(DB_PATH)
            gate_passed_after, gaps_after = _evaluate_gate(metrics_after)
            report["gate_after"] = {"passed": gate_passed_after, "metrics": metrics_after, "gaps": gaps_after}
            _print_gate("补数后复检", metrics_after, gaps_after)
            if not gate_passed_after:
                raise RuntimeError("补数后新闻量门禁仍未达标")
        else:
            report["remediation"] = remediation_records
            report["gate_after"] = {"passed": gate_passed_after, "metrics": metrics_after, "gaps": gaps_after}

        if args.dry_run:
            print("[模式] dry-run，跳过 ZIP/manifest/sha256 生成")
            report["result"] = "dry_run_passed"
            _write_json(report_path, report)
            print(f"[报告] {report_path}")
            return

        output_zip.parent.mkdir(parents=True, exist_ok=True)
        if output_zip.exists():
            output_zip.unlink()

        print("[打包] 开始写入 ZIP64 ...")
        with ZipFile(output_zip, mode="w", compression=ZIP_DEFLATED, compresslevel=6, allowZip64=True) as zf:
            for file_path in files:
                arcname = _rel(file_path)
                zf.write(file_path, arcname=arcname)

        post_scan = _scan_archive_for_leakage(output_zip)
        report["source_scan_post"] = {"passed": post_scan.passed, "hits": post_scan.hits}
        print(f"[扫描] 打包后泄漏扫描: {'通过' if post_scan.passed else '失败'}")
        if not post_scan.passed:
            raise RuntimeError("打包后泄漏扫描失败")

        manifest_payload = _build_manifest(files, output_zip, metrics_after)
        _write_json(manifest_path, manifest_payload)

        zip_sha256 = _write_sha_file(output_zip, sha_path)
        zip_size = int(output_zip.stat().st_size)
        report["result"] = "success"
        report["package"] = {
            "zip_size_bytes": zip_size,
            "zip_sha256": zip_sha256,
            "file_count": manifest_payload["file_count"],
            "total_bytes": manifest_payload["total_bytes"],
        }

        _write_json(report_path, report)
        print(f"[完成] ZIP: {output_zip}")
        print(f"[完成] MANIFEST: {manifest_path}")
        print(f"[完成] SHA256: {sha_path}")
        print(f"[完成] REPORT: {report_path}")
        print(f"[统计] 文件数: {manifest_payload['file_count']}, 原始总字节: {manifest_payload['total_bytes']}, 压缩包字节: {zip_size}")
    except Exception as exc:  # noqa: BLE001
        report["result"] = "failed"
        report["error"] = str(exc)
        _write_json(report_path, report)
        print(f"[失败] {exc}", file=sys.stderr)
        print(f"[报告] {report_path}", file=sys.stderr)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
