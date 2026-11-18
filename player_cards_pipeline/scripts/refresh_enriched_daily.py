#!/usr/bin/env python3
"""Refresh Hoop Explorer enriched player exports for a card season.

The Exports page documents the downloads but is not itself a download
endpoint. The unattended job accepts the direct export URL through
``--export-url`` or ``HOOP_EXPLORER_EXPORT_URL``. Both tiered ZIP exports and
the flat ``players_all_Men_<year>_All.csv`` export are supported.
"""

from __future__ import annotations

import argparse
import csv
import io
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


def number(value: str):
    try:
        if value == "" or value is None:
            return None
        n = float(value)
        return int(n) if n.is_integer() else n
    except (TypeError, ValueError):
        return value


def csv_player(row: dict[str, str], json_year: int) -> dict:
    """Adapt the flat export to the nested shape used by the card builder."""
    player = {
        "key": row.get("player_name", ""),
        "_id": f"{row.get('roster.ncaa_id', '')}_Men_{json_year}_all",
        "code": row.get("player_code", ""),
        "team": row.get("team", ""),
        "conf": row.get("conf", ""),
        "year": row.get("year", f"{json_year}/{str(json_year + 1)[-2:]}"),
        "posClass": row.get("posClass", ""),
        "roster": {
            "ncaa_id": number(row.get("roster.ncaa_id", "")),
            "number": number(row.get("roster.number", "")),
            "height": row.get("roster.height", ""),
            "year_class": row.get("roster.year_class", ""),
            "pos": row.get("roster.pos", ""),
            "origin": row.get("roster.origin", ""),
        },
        "off_team_poss": {"value": number(row.get("off_poss", ""))},
        "off_adj_rapm": {"value": number(row.get("off_adj_rapm", ""))},
        "def_adj_rapm": {"value": number(row.get("def_adj_rapm", ""))},
        "net_pts": {
            "o": number(row.get("off_adj_rapm", "")),
            "d": number(row.get("def_adj_rapm", "")),
            "value": number(row.get("adj_rapm_margin", "")),
        },
        "off": {},
        "def": {},
        "style": {},
        "source_format": "hoop_explorer_csv",
    }

    for key, value in row.items():
        if key.startswith("off_"):
            player["off"][key] = {"value": number(value)}
        elif key.startswith("def_"):
            player["def"][key] = {"value": number(value)}

    style_names = {
        "rim_attack": "Rim Attack",
        "perimeter_sniper": "Perimeter Sniper",
        "dribble_jumper": "Dribble Jumper",
        "mid_range": "Mid-Range",
        "pnr_passer": "PnR Passer",
        "big_cut_roll": "Big Cut & Roll",
        "pick_pop": "Pick & Pop",
        "post_up": "Post-Up",
        "perimeter_cut": "Backdoor Cut",
        "transition": "Transition",
    }
    for slug, label in style_names.items():
        ppp = row.get(f"off_style_{slug}_ppp", "")
        volume = row.get(f"off_style_{slug}_usg", "")
        player["style"][label] = {
            "adj_pts": {"value": number(ppp)},
            "possPctUsg": {"value": number(volume)},
        }
    return player


def convert_csv(csv_bytes: bytes, output_root: Path, json_year: int) -> list[Path]:
    rows = list(csv.DictReader(io.StringIO(csv_bytes.decode("utf-8-sig", errors="replace"))))
    if not rows:
        raise RuntimeError("Hoop Explorer CSV contained no player rows.")
    required = {"player_name", "team", "conf", "tier", "year"}
    missing = sorted(required - set(rows[0]))
    if missing:
        raise RuntimeError(f"Hoop Explorer CSV is missing required columns: {', '.join(missing)}")

    output_root.mkdir(parents=True, exist_ok=True)
    files: list[Path] = []
    for tier in ("Low", "Medium", "High"):
        players = [csv_player(row, json_year) for row in rows if row.get("tier") == tier]
        if not players:
            continue
        path = output_root / f"players_all_Men_{json_year}_{tier}.json"
        path.write_text(
            json.dumps({
                "lastUpdated": rows[0].get("year", ""),
                "json_year": str(json_year),
                "script_season": str(json_year + 1),
                "confs": sorted({row.get("conf", "") for row in rows if row.get("conf")}),
                "players": players,
            }),
            encoding="utf-8",
        )
        files.append(path)
    return files


def copy_sources(files: list[Path], destination: Path, json_year: int) -> None:
    destination.mkdir(parents=True, exist_ok=True)
    for old in destination.glob(f"players_all_Men_{json_year}_*.json"):
        old.unlink()
    for source in files:
        shutil.copy2(source, destination / source.name)


def verify(files: list[Path], source_format: str = "json") -> None:
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
    print(f"Hoop Explorer {source_format} check: files={len(files)} net_pts_files={with_net_pts}", flush=True)


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
        if zipfile.is_zipfile(archive):
            with zipfile.ZipFile(archive) as bundle:
                bundle.extractall(extracted)
            files = source_files(extracted, json_year)
            verify(files)
        else:
            files = convert_csv(archive.read_bytes(), extracted, json_year)
            verify(files, source_format="CSV")
        copy_sources(files, source_dir, json_year)
        combine(root, source_dir)

    if not expected.exists():
        raise RuntimeError(f"Expected combined output not found: {expected}")
    print(f"Hoop Explorer season {args.season} refresh complete: {expected}", flush=True)


if __name__ == "__main__":
    main()
