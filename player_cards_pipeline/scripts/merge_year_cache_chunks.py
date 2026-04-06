#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def init_target_db(path: Path, season: str, min_games: int) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    conn.execute("CREATE TABLE IF NOT EXISTS card_cache (cache_key TEXT PRIMARY KEY, payload_json TEXT NOT NULL)")
    meta = {
        "schema_version": "1",
        "season": str(season),
        "min_games": str(min_games),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    for k, v in meta.items():
        conn.execute("INSERT OR REPLACE INTO metadata(key, value) VALUES(?, ?)", (k, v))
    conn.commit()
    return conn


def main() -> None:
    ap = argparse.ArgumentParser(description="Merge chunked season cache sqlite files into a single year sqlite.")
    ap.add_argument("--project-root", default=".")
    ap.add_argument("--year", required=True)
    ap.add_argument("--chunk-tags", required=True, help="Comma-separated chunk tags, e.g. chunk001_060,chunk061_120")
    ap.add_argument("--min-games", type=int, default=5)
    ap.add_argument("--cache-dir", default="player_cards_pipeline/data/cache/card_sections")
    ap.add_argument("--manifest-dir", default="player_cards_pipeline/data/cache/card_sections_manifest")
    args = ap.parse_args()

    root = Path(args.project_root).resolve()
    year = str(args.year).strip()
    tags = [t.strip() for t in str(args.chunk_tags).split(",") if t.strip()]
    if not tags:
        raise SystemExit("No chunk tags supplied")

    cache_dir = root / args.cache_dir
    manifest_dir = root / args.manifest_dir
    target_db = cache_dir / f"{year}.sqlite"
    target_manifest = manifest_dir / f"{year}.json"

    conn = init_target_db(target_db, year, args.min_games)
    merged_manifest: dict[str, str] = {}
    total_rows = 0

    with conn:
        for tag in tags:
            db_path = cache_dir / f"{year}_{tag}.sqlite"
            man_path = manifest_dir / f"{year}_{tag}.json"
            if not db_path.exists():
                print(f"[merge] missing db: {db_path}")
                continue
            src = sqlite3.connect(str(db_path))
            try:
                rows = src.execute("SELECT cache_key, payload_json FROM card_cache").fetchall()
                conn.executemany(
                    "INSERT OR REPLACE INTO card_cache(cache_key, payload_json) VALUES(?, ?)",
                    rows,
                )
                total_rows += len(rows)
            finally:
                src.close()

            if man_path.exists():
                try:
                    m = json.loads(man_path.read_text(encoding="utf-8"))
                    if isinstance(m, dict):
                        merged_manifest.update({str(k): str(v) for k, v in m.items()})
                except Exception:
                    pass
            print(f"[merge] merged tag={tag}")

    if merged_manifest:
        target_manifest.parent.mkdir(parents=True, exist_ok=True)
        target_manifest.write_text(json.dumps(merged_manifest, ensure_ascii=True, sort_keys=True, indent=2), encoding="utf-8")
        print(f"[merge] wrote manifest={target_manifest} keys={len(merged_manifest)}")
    print(f"[merge] wrote db={target_db} rows_seen={total_rows}")


if __name__ == "__main__":
    main()
