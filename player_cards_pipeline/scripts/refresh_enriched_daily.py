#!/usr/bin/env python3
"""Refresh Hoop Explorer enriched player exports for a card season.

The Exports page documents the downloads but is not itself a download
endpoint. The unattended job accepts the direct archive URL through
``--export-url`` or ``HOOP_EXPLORER_EXPORT_URL``.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import tempfile
import urllib.request
import zipfile
from pathlib import Path


def download(url: str, destination: Path) -> None:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; NCAACards/1.0)",
            "Accept": "application/zip,application/octet-stream,*/*",
        },
    )
    with urllib.request.urlopen(req, timeout=180) as response:
        payload = response.read()
    if not payload:
        raise RuntimeError("Hoop Explorer export download was empty.")
    destination.write_bytes(payload)


def source_files(root: Path, json_year: int) -> list[Path]:
    return sorted(root.rglob(f"players_all_Men_{json_year}_*.json"))


def copy_sources(files: list[Path], destination: Path, json_year: int) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    for old in destination.glob(f"players_all_Men_{json_year}_*.json"):
        old.unlink()
    for source in files:
        shutil.copy2(source, destination / source.name)


def verify(files: list[Path]) -> None:
    tiers = set()
    with_net_pts = 0
    for path in files:
        tiers.add(path.stem.rsplit("_", 1)[-1])
        obj = json.loads(path.read_text(encoding="utf-8"))
        players = obj.get("players", []) if isinstance(obj, dict) else []
        if any(isinstance(row, dict) and isinstance(row.get("net_pts"), dict) for row in players):
            with_net_pts += 1
    if not files or not tiers.intersection({"Low", "Medium", "High"}):
        raise RuntimeError("Export did not contain expected Low/Medium/High player JSON files.")
    if with_net_pts == 0:
        raise RuntimeError("Export did not contain net_pts; refusing to replace the cached source.")
    print(f"Hoop Explorer export check: files={len(files)} net_pts_files={with_net_pts}", flush=True)


def combine(root: Path, source_dir: Path) -> None:
    script = root / "player_cards_pipeline" / "scripts" / "combine_enriched_players_json.py"
    output = root / "player_cards_pipeline" / "data" / "manual" / "enriched_players"
    subprocess.run(
        ["python3", str(script), "--input-dir", str(source_dir), "--output-dir", str(output)],
        cwd=str(root),
        check=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh Hoop Explorer enriched player exports.")
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--season", type=int, default=2026, help="Card season, e.g. 2027 for 2026-27.")
    parser.add_argument("--json-year", type=int, help="Hoop Explorer JSON year; defaults to season - 1.")
    parser.add_argument("--export-url", default=os.getenv("HOOP_EXPLORER_EXPORT_URL", ""))
    args = parser.parse_args()

    if not args.export_url:
        raise RuntimeError(
            "Missing Hoop Explorer direct export URL. Set HOOP_EXPLORER_EXPORT_URL "
            "or pass --export-url; /Exports is an instructions page, not the archive."
        )
    root = Path(args.project_root).resolve()
    json_year = args.json_year or args.season - 1
    manual = root / "player_cards_pipeline" / "data" / "manual" / "enriched_players"
    source_dir = manual / f"hoop_explorer_{json_year}_source"
    expected = manual / "by_script_season" / (
        f"players_all_Men_scriptSeason_{args.season}_fromJsonYear_{json_year}.json"
    )

    with tempfile.TemporaryDirectory(prefix="hoop_explorer_export_") as temp_dir:
        archive = Path(temp_dir) / "export.zip"
        extracted = Path(temp_dir) / "extracted"
        extracted.mkdir()
        download(args.export_url, archive)
        if not zipfile.is_zipfile(archive):
            raise RuntimeError(
                "Hoop Explorer URL did not return a zip archive. Use the direct export download URL "
                "from the Exports page, not the page URL itself."
            )
        with zipfile.ZipFile(archive) as bundle:
            bundle.extractall(extracted)
        files = source_files(extracted, json_year)
        verify(files)
        copy_sources(files, source_dir, json_year)
        combine(root, source_dir)

    if not expected.exists():
        raise RuntimeError(f"Expected combined output not found: {expected}")
    print(f"Hoop Explorer season {args.season} refresh complete: {expected}", flush=True)


if __name__ == "__main__":
    main()
