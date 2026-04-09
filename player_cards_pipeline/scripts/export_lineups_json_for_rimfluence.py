#!/usr/bin/env python3
"""Convert CBBD lineup CSV outputs into JSON/NDJSON for downstream analytics.

This is designed for tooling like rimfluence that prefers JSON records and a
parsed athlete list rather than wide CSV columns such as athletes[0].name.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Any


ATHLETE_COL_RE = re.compile(r"^athletes\[(\d+)\]\.(.+)$")


def parse_scalar(value: str) -> Any:
    text = value.strip()
    if text == "":
        return None
    lower = text.lower()
    if lower == "true":
        return True
    if lower == "false":
        return False
    if re.fullmatch(r"-?\d+", text):
        try:
            return int(text)
        except ValueError:
            return text
    if re.fullmatch(r"-?\d*\.\d+", text):
        try:
            return float(text)
        except ValueError:
            return text
    return text


def parse_athletes(row: dict[str, str]) -> list[dict[str, Any]]:
    indexed: dict[int, dict[str, Any]] = {}
    for key, raw in row.items():
        match = ATHLETE_COL_RE.match(key)
        if not match:
            continue
        idx = int(match.group(1))
        field = match.group(2)
        bucket = indexed.setdefault(idx, {})
        bucket[field] = parse_scalar(raw)
    return [indexed[i] for i in sorted(indexed.keys())]


def row_to_record(row: dict[str, str]) -> dict[str, Any]:
    record: dict[str, Any] = {}
    for key, raw in row.items():
        if ATHLETE_COL_RE.match(key):
            continue
        record[key] = parse_scalar(raw)
    record["athletes"] = parse_athletes(row)
    return record


def read_csv_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return [row_to_record(r) for r in reader]


def write_json(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)


def write_ndjson(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False))
            f.write("\n")


def convert_one(table_dir: Path, out_dir: Path, stem: str) -> int:
    in_path = table_dir / f"{stem}.csv"
    rows = read_csv_records(in_path)
    write_json(out_dir / f"{stem}.json", rows)
    write_ndjson(out_dir / f"{stem}.ndjson", rows)
    print(f"[json-export] {stem}: rows={len(rows)} from={in_path}")
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Export lineup CSV tables to JSON and NDJSON.")
    parser.add_argument("--year", type=int, required=True, help="Season year (e.g. 2026 => 2025-2026).")
    parser.add_argument(
        "--out-root",
        default="cbbd_seasons",
        help="Root containing season folder produced by the pull script.",
    )
    args = parser.parse_args()

    season_label = f"{args.year - 1}-{args.year}"
    season_dir = Path(args.out_root) / season_label
    table_dir = season_dir / "tables"
    out_dir = season_dir / "json"

    stems = ["lineups_regular", "lineups_postseason", "lineups_fullseason"]
    counts = {stem: convert_one(table_dir, out_dir, stem) for stem in stems}

    manifest = {
        "season_year": args.year,
        "season_label": season_label,
        "source_tables_dir": str(table_dir),
        "json_dir": str(out_dir),
        "rows": counts,
    }
    write_json(out_dir / "lineups_export_manifest.json", [manifest])
    print(f"[json-export] done season={season_label}")


if __name__ == "__main__":
    main()
