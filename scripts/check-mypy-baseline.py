#!/usr/bin/env python3
"""Enforce a non-growing, diagnostic-specific baseline for strict mypy errors.

The project is reducing an inherited strict-mode backlog. CI accepts existing
diagnostics by file, error code, and message, allows fixes, and rejects an
increase in any diagnostic's count.
"""

from __future__ import annotations

import argparse
import collections
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
BASELINE = ROOT / "mypy-baseline.json"


Diagnostic = tuple[str, str, str]


def _diagnostics() -> tuple[collections.Counter[Diagnostic], subprocess.CompletedProcess[str]]:
    completed = subprocess.run(
        [sys.executable, "-m", "mypy", "-O", "json", "--no-error-summary", "src"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode not in {0, 1}:
        sys.stderr.write(completed.stdout)
        sys.stderr.write(completed.stderr)
        raise SystemExit(completed.returncode)
    diagnostics: collections.Counter[Diagnostic] = collections.Counter()
    for line in completed.stdout.splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        if record.get("severity") != "error":
            continue
        diagnostics[
            (
                str(record["file"]),
                str(record.get("code") or "unknown"),
                str(record["message"]),
            )
        ] += 1
    return diagnostics, completed


def _load_baseline() -> collections.Counter[Diagnostic]:
    payload = json.loads(BASELINE.read_text(encoding="utf-8"))
    if payload.get("version") != 2:
        raise SystemExit(f"unsupported mypy baseline version in {BASELINE}")
    diagnostics: collections.Counter[Diagnostic] = collections.Counter()
    for row in payload["diagnostics"]:
        diagnostics[
            (
                str(row["file"]),
                str(row["code"]),
                str(row["message"]),
            )
        ] += int(row["count"])
    return diagnostics


def _write_baseline(diagnostics: collections.Counter[Diagnostic]) -> None:
    rows: list[dict[str, Any]] = []
    for (file, code, message), count in sorted(diagnostics.items()):
        rows.append(
            {
                "file": file,
                "code": code,
                "message": message,
                "count": count,
            }
        )
    BASELINE.write_text(
        json.dumps({"version": 2, "diagnostics": rows}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--write-baseline",
        "--update",
        action="store_true",
        help="Replace the baseline with the current strict-mypy diagnostics.",
    )
    args = parser.parse_args()
    current, _completed = _diagnostics()
    if args.write_baseline:
        _write_baseline(current)
        print(f"wrote {BASELINE.relative_to(ROOT)} with {current.total()} diagnostics")
        return 0

    baseline = _load_baseline()
    additions = current - baseline
    fixed = baseline - current
    print(
        f"mypy baseline: current={current.total()} allowed={baseline.total()} "
        f"fixed={fixed.total()} new={additions.total()}"
    )
    for (file, code, message), count in sorted(fixed.items()):
        print(f"FIXED {file} [{code}] x{count}: {message}")
    if additions:
        for (file, code, message), count in sorted(additions.items()):
            print(f"NEW {file} [{code}] x{count}: {message}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
