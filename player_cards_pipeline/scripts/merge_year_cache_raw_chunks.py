#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser(description="Merge raw chunk player lists + source-hash maps.")
    ap.add_argument("--project-root", default=".")
    ap.add_argument("--year", required=True)
    ap.add_argument("--chunk-tags", required=True, help="Comma-separated tags")
    ap.add_argument("--raw-dir", default="player_cards_pipeline/data/cache/raw_chunks")
    args = ap.parse_args()

    root = Path(args.project_root).resolve()
    year = str(args.year).strip()
    tags = [t.strip() for t in str(args.chunk_tags).split(",") if t.strip()]
    if not tags:
        raise SystemExit("No chunk tags provided")

    raw_dir = root / args.raw_dir
    raw_dir.mkdir(parents=True, exist_ok=True)

    seen_keys: set[str] = set()
    merged_lines: list[str] = []
    merged_hashes: dict[str, str] = {}

    for tag in tags:
        players_file = raw_dir / f"{year}_{tag}_players.jsonl"
        hashes_file = raw_dir / f"{year}_{tag}_hashes.json"
        if players_file.exists():
            for line in players_file.read_text(encoding="utf-8").splitlines():
                t = line.strip()
                if not t:
                    continue
                row = json.loads(t)
                k = f"{str(row.get('player','')).strip().lower()}|{str(row.get('team','')).strip().lower()}|{str(row.get('season','')).strip()}"
                if k in seen_keys:
                    continue
                seen_keys.add(k)
                merged_lines.append(json.dumps(row, ensure_ascii=True))
        if hashes_file.exists():
            h = json.loads(hashes_file.read_text(encoding="utf-8"))
            if isinstance(h, dict):
                for k, v in h.items():
                    merged_hashes[str(k)] = str(v)
        print(f"[raw-merge] merged tag={tag}")

    out_players = raw_dir / f"{year}_merged_players.jsonl"
    out_hashes = raw_dir / f"{year}_merged_hashes.json"
    out_players.write_text("\n".join(merged_lines) + ("\n" if merged_lines else ""), encoding="utf-8")
    out_hashes.write_text(json.dumps(merged_hashes, ensure_ascii=True, sort_keys=True, indent=2), encoding="utf-8")
    print(f"[raw-merge] wrote players={out_players} count={len(merged_lines)}")
    print(f"[raw-merge] wrote hashes={out_hashes} keys={len(merged_hashes)}")


if __name__ == "__main__":
    main()
