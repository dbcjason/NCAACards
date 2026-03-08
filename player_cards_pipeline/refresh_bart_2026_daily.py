#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
from pathlib import Path
from urllib.request import Request, urlopen

ADVGAMES_HEADERS = [
    'numdate', 'datetext', 'opstyle', 'quality', 'win1', 'opponent', 'muid', 'win2',
    'Min_per', 'ORtg', 'Usage', 'eFG', 'TS_per', 'ORB_per', 'DRB_per', 'AST_per', 'TO_per',
    'dunksmade', 'dunksatt', 'rimmade', 'rimatt', 'midmade', 'midatt', 'twoPM', 'twoPA',
    'TPM', 'TPA', 'FTM', 'FTA', 'bpm_rd', 'Obpm', 'Dbpm', 'bpm_net', 'pts', 'ORB', 'DRB',
    'AST', 'TOV', 'STL', 'BLK', 'stl_per', 'blk_per', 'PF', 'possessions', 'bpm', 'sbpm',
    'loc', 'tt', 'pp', 'inches', 'cls', 'pid', 'year'
]


def fetch_bytes(url: str) -> bytes:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "*/*"})
    with urlopen(req, timeout=120) as resp:
        return resp.read()


def fetch_text(url: str) -> str:
    return fetch_bytes(url).decode("utf-8", errors="replace")


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if not path.exists():
        return [], []
    with path.open(newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        rows = list(r)
        return list(r.fieldnames or []), rows


def write_csv(path: Path, header: list[str], rows: list[dict[str, str]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in header})


def refresh_advstats_2026(bt_dir: Path) -> None:
    url = "https://barttorvik.com/getadvstats.php?year=2026&csv=1"
    txt = fetch_text(url)
    parsed = list(csv.reader(txt.splitlines()))
    if not parsed:
        raise RuntimeError("Empty advstats response for 2026")

    h26 = parsed[0]
    rows26 = parsed[1:]
    h26_out = h26 + (["bt_fetch_year"] if "bt_fetch_year" not in h26 else [])

    out_2026 = bt_dir / "bt_advstats_2026.csv"
    out_2010_2026 = bt_dir / "bt_advstats_2010_2026.csv"
    base_2010_2025 = bt_dir / "bt_advstats_2010_2025.csv"

    rows26_dict: list[dict[str, str]] = []
    for r in rows26:
        rr = r + [""] * max(0, len(h26) - len(r))
        d = {h26[i]: rr[i] for i in range(len(h26))}
        d["bt_fetch_year"] = "2026"
        rows26_dict.append(d)

    write_csv(out_2026, h26_out, rows26_dict)

    base_h, base_rows = read_csv(base_2010_2025)
    if not base_h:
        # Fall back to just 2026 if no base history available.
        write_csv(out_2010_2026, h26_out, rows26_dict)
        return

    header = list(base_h)
    for k in h26_out:
        if k not in header:
            header.append(k)

    merged = []
    for r in base_rows:
        if r.get("bt_fetch_year", "") == "2026":
            continue
        merged.append(r)
    merged.extend(rows26_dict)

    write_csv(out_2010_2026, header, merged)


def refresh_playerstat_2026(bt_dir: Path) -> None:
    raw_dir = bt_dir / "raw_playerstat_json"
    ensure_dir(raw_dir)

    url = "https://barttorvik.com/2026_pbp_playerstat_array.json"
    raw = fetch_text(url)
    (raw_dir / "2026_pbp_playerstat_array.json").write_text(raw, encoding="utf-8")

    arr = json.loads(raw)
    csv_out = bt_dir / "bt_playerstat_2026.csv"
    header = [
        "year", "pid", "player", "team",
        "rim_made", "rim_miss", "rim_assisted",
        "mid_made", "mid_miss", "mid_assisted",
        "three_made", "three_miss", "three_assisted",
        "dunks_made", "dunks_miss", "dunks_assisted",
    ]
    rows: list[dict[str, str]] = []
    for it in arr:
        if not isinstance(it, list) or len(it) < 15:
            continue
        rows.append({
            "year": "2026",
            "pid": str(it[0]),
            "player": str(it[1]),
            "team": str(it[2]),
            "rim_made": str(it[3]),
            "rim_miss": str(it[4]),
            "rim_assisted": str(it[5]),
            "mid_made": str(it[6]),
            "mid_miss": str(it[7]),
            "mid_assisted": str(it[8]),
            "three_made": str(it[9]),
            "three_miss": str(it[10]),
            "three_assisted": str(it[11]),
            "dunks_made": str(it[12]),
            "dunks_miss": str(it[13]),
            "dunks_assisted": str(it[14]),
        })
    write_csv(csv_out, header, rows)


def refresh_advgames_2026(bt_dir: Path) -> None:
    raw_dir = bt_dir / "raw_advgames_json"
    out_dir = bt_dir / "advgames_labeled"
    ensure_dir(raw_dir)
    ensure_dir(out_dir)

    urls = [
        "https://barttorvik.com/2026_all_advgames.json.gz",
        "https://barttorvik.com/2026_all_advgames.json",
    ]

    payload: bytes | None = None
    used_url = ""
    for u in urls:
        try:
            payload = fetch_bytes(u)
            used_url = u
            if payload:
                break
        except Exception:
            continue
    if not payload:
        raise RuntimeError("Could not fetch 2026 all_advgames JSON")

    if used_url.endswith('.gz'):
        txt = gzip.decompress(payload).decode("utf-8", errors="replace")
    else:
        txt = payload.decode("utf-8", errors="replace")

    (raw_dir / "2026_all_advgames.json").write_text(txt, encoding="utf-8")

    arr = json.loads(txt)
    rows: list[dict[str, str]] = []
    for it in arr:
        if not isinstance(it, list):
            continue
        row = {}
        for i, h in enumerate(ADVGAMES_HEADERS):
            row[h] = str(it[i]) if i < len(it) and it[i] is not None else ""
        rows.append(row)

    write_csv(out_dir / "2026_all_advgames_labeled.csv", ADVGAMES_HEADERS, rows)


def main() -> None:
    ap = argparse.ArgumentParser(description="Refresh 2026 Bart datasets used by player cards.")
    ap.add_argument("--project-root", required=True)
    args = ap.parse_args()

    root = Path(args.project_root)
    bt_dir = root / "player_cards_pipeline" / "data" / "bt"
    ensure_dir(bt_dir)

    refresh_advstats_2026(bt_dir)
    refresh_playerstat_2026(bt_dir)
    refresh_advgames_2026(bt_dir)

    print("Bart 2026 refresh complete")


if __name__ == "__main__":
    main()
