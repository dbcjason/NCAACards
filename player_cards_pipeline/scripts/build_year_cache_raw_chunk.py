#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import hashlib
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import cbb_player_cards_v1.build_player_card as bpc


def stable_hash_payload(parts: dict[str, Any]) -> str:
    raw = json.dumps(parts, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def parse_year(y: str) -> str:
    return bpc.norm_season(str(y or "").strip())


def rel_to_pipeline(project_root: Path, rel: str) -> Path:
    return project_root / "player_cards_pipeline" / rel


def build_bt_indexes(bt_rows: list[dict[str, str]]):
    exact: dict[tuple[str, str, str], dict[str, str]] = {}
    fallback: dict[tuple[str, str], dict[str, str]] = {}
    for r in bt_rows:
        p = bpc.norm_player_name(bpc.bt_get(r, ["player_name"]))
        t = bpc.norm_team(bpc.bt_get(r, ["team"]))
        y = bpc.norm_season(bpc.bt_get(r, ["year"]))
        if not p or not y:
            continue
        if p and t and y:
            exact.setdefault((p, t, y), r)
        fallback.setdefault((p, y), r)
    return exact, fallback


def build_enriched_indexes(enriched_rows: list[dict[str, Any]]):
    exact: dict[tuple[str, str, str], dict[str, Any]] = {}
    fallback: dict[tuple[str, str], dict[str, Any]] = {}
    for r in enriched_rows:
        p = bpc.norm_player_name(r.get("key", ""))
        t = bpc.norm_team(r.get("team", ""))
        y = bpc.norm_season(r.get("year", ""))
        if not p or not y:
            continue
        if p and t and y:
            exact.setdefault((p, t, y), r)
        fallback.setdefault((p, y), r)
    return exact, fallback


def main() -> None:
    ap = argparse.ArgumentParser(description="Build raw chunk inputs (player list + source hashes) for cache.")
    ap.add_argument("--project-root", default=".")
    ap.add_argument("--year", required=True)
    ap.add_argument("--team-start", type=int, required=True)
    ap.add_argument("--team-end", type=int, required=True)
    ap.add_argument("--chunk-tag", required=True)
    ap.add_argument("--out-dir", default="player_cards_pipeline/data/cache/raw_chunks")
    args = ap.parse_args()

    root = Path(args.project_root).resolve()
    year = parse_year(args.year)
    out_dir = root / args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    settings = json.loads((root / "player_cards_pipeline" / "config" / "settings.json").read_text(encoding="utf-8"))
    bt_csv = rel_to_pipeline(root, settings["bt_advstats_csv"])
    _h, bt_rows = bpc.read_csv_rows(bt_csv)
    if not bt_rows:
        raise RuntimeError(f"No BT rows loaded from {bt_csv}")
    bpc.inject_enriched_fields_into_bt_rows(bt_rows)
    players_all = bpc.build_player_pool_from_bt(bt_rows)

    season_players_all = [p for p in players_all if bpc.norm_season(p.season) == year]
    season_players_all = sorted(season_players_all, key=lambda p: (bpc.norm_team(p.team), bpc.norm_player_name(p.player)))

    team_keys = sorted({bpc.norm_team(p.team) for p in season_players_all if (p.team or "").strip()})
    total_teams = len(team_keys)
    ts = max(1, min(total_teams, int(args.team_start)))
    te = max(ts, min(total_teams, int(args.team_end)))
    active_team_keys = set(team_keys[ts - 1 : te])
    season_players = [p for p in season_players_all if bpc.norm_team(p.team) in active_team_keys]

    adv_map = settings.get("advgames_csv_by_year", {}) or {}
    adv_rows: list[dict[str, str]] = []
    rel = adv_map.get(year)
    if rel:
        p = rel_to_pipeline(root, rel)
        if p.exists():
            _ah, adv_rows = bpc.read_csv_rows(p)

    enriched_rows = bpc.load_enriched_players_for_script_season(year) or []
    bt_exact_idx, bt_fallback_idx = build_bt_indexes(bt_rows)
    en_exact_idx, en_fallback_idx = build_enriched_indexes(enriched_rows)

    hashes: dict[str, str] = {}
    players_out = out_dir / f"{year}_{args.chunk_tag}_players.jsonl"
    hashes_out = out_dir / f"{year}_{args.chunk_tag}_hashes.json"

    lines: list[str] = []
    for i, target in enumerate(season_players, start=1):
        ck = bpc.card_cache_key(target.player, target.team, target.season)
        pk = bpc.norm_player_name(target.player)
        tk = bpc.norm_team(target.team)
        yk = bpc.norm_season(target.season)
        bt_row = bt_exact_idx.get((pk, tk, yk)) or bt_fallback_idx.get((pk, yk), {})
        en_row = en_exact_idx.get((pk, tk, yk)) or en_fallback_idx.get((pk, yk), {})
        hashes[ck] = stable_hash_payload(
            {
                "bt": bt_row,
                "adv_count": len(adv_rows),
                "enriched": en_row,
                "season": target.season,
                "player": target.player,
                "team": target.team,
            }
        )
        lines.append(json.dumps({"player": target.player, "team": target.team, "season": target.season}, ensure_ascii=True))
        if i % 100 == 0 or i == len(season_players):
            print(f"[raw-chunk] {year} {args.chunk_tag}: {i}/{len(season_players)}")

    players_out.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    hashes_out.write_text(json.dumps(hashes, ensure_ascii=True, sort_keys=True, indent=2), encoding="utf-8")
    print(
        f"[raw-chunk] {year} {args.chunk_tag}: teams={len(active_team_keys)}/{total_teams} "
        f"players={len(season_players)} players_file={players_out} hashes_file={hashes_out}"
    )


if __name__ == "__main__":
    main()
