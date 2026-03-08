#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from urllib.request import Request, urlopen


def fetch_text(url: str) -> str:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "text/csv,application/json,*/*"})
    with urlopen(req, timeout=120) as resp:
        return resp.read().decode("utf-8", errors="replace")


def parse_csv_text(text: str) -> tuple[list[str], list[list[str]]]:
    rows = list(csv.reader(text.splitlines()))
    if not rows:
        return [], []
    return rows[0], rows[1:]


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch Bart Torvik history (advstats CSV + playerstat JSON) for 2010..2025.")
    ap.add_argument("--year-start", type=int, default=2010)
    ap.add_argument("--year-end", type=int, default=2025)
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    bt_dir = out_dir / "data" / "bt"
    raw_dir = bt_dir / "raw_playerstat_json"
    bt_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)

    adv_out = bt_dir / f"bt_advstats_{args.year_start}_{args.year_end}.csv"
    playerstat_out = bt_dir / f"bt_playerstat_{args.year_start}_{args.year_end}.csv"

    adv_header: list[str] = []
    adv_rows: list[list[str]] = []

    playerstat_header = [
        "year", "pid", "player", "team",
        "rim_made", "rim_miss", "rim_assisted",
        "mid_made", "mid_miss", "mid_assisted",
        "three_made", "three_miss", "three_assisted",
        "dunks_made", "dunks_miss", "dunks_assisted",
    ]
    playerstat_rows: list[list[str]] = []

    for year in range(args.year_start, args.year_end + 1):
        # 1) Advstats CSV
        csv_url = f"https://barttorvik.com/getadvstats.php?year={year}&csv=1"
        try:
            text = fetch_text(csv_url)
            h, rows = parse_csv_text(text)
            if h and not adv_header:
                adv_header = h + ["bt_fetch_year"]
            if h:
                for r in rows:
                    rr = r + [""] * max(0, len(h) - len(r))
                    adv_rows.append(rr[:len(h)] + [str(year)])
            print(f"[ok] advstats {year} rows={len(rows)}")
        except Exception as e:
            print(f"[warn] advstats {year} failed: {e}")

        # 2) Playerstat JSON array
        json_url = f"https://barttorvik.com/{year}_pbp_playerstat_array.json"
        try:
            raw = fetch_text(json_url)
            (raw_dir / f"{year}_pbp_playerstat_array.json").write_text(raw, encoding="utf-8")
            arr = json.loads(raw)
            kept = 0
            for item in arr:
                if not isinstance(item, list) or len(item) < 15:
                    continue
                row = [
                    str(year), str(item[0]), str(item[1]), str(item[2]),
                    str(item[3]), str(item[4]), str(item[5]),
                    str(item[6]), str(item[7]), str(item[8]),
                    str(item[9]), str(item[10]), str(item[11]),
                    str(item[12]), str(item[13]), str(item[14]),
                ]
                playerstat_rows.append(row)
                kept += 1
            print(f"[ok] playerstat {year} rows={kept}")
        except Exception as e:
            print(f"[warn] playerstat {year} failed: {e}")

    if adv_header:
        with adv_out.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(adv_header)
            w.writerows(adv_rows)
        print(f"wrote {adv_out} rows={len(adv_rows)}")

    with playerstat_out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(playerstat_header)
        w.writerows(playerstat_rows)
    print(f"wrote {playerstat_out} rows={len(playerstat_rows)}")


if __name__ == "__main__":
    main()
