#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from collections import defaultdict

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import cbb_player_cards_v1.build_player_card as bpc


BT_SECTION_SPECS: dict[str, list[tuple[str, str, bool, int]]] = {
    "Impact": [
        ("BPM", "bpm", False, 1),
        ("RAPM", "rapm", False, 1),
        ("Net Pts", "net_pts", False, 1),
        ("On/Off NetR", "onoff_net_rating", False, 1),
    ],
    "Scoring": [
        ("Usage", "usg", False, 1),
        ("TS%", "ts_per", True, 1),
        ("2P%", "twop_per", True, 1),
        ("Dunks/100", "dunks_100_bt", False, 2),
        ("Rim Att/100", "rim_att_100_bt", False, 1),
        ("Rim%", "rim_pct", True, 1),
        ("Mid%", "mid_pct", True, 1),
        ("3P%", "tp_per", True, 1),
        ("3PA/100", "threepa100", False, 1),
        ("FTA/100", "fta100_bt", False, 1),
        ("FT%", "ft_per", True, 1),
        ("FTr", "ftr", False, 1),
    ],
    "Playmaking": [
        ("AST%", "ast_per", True, 1),
        ("TO%", "to_per", True, 1),
        ("A/TO", "ast_tov", False, 2),
        ("Rim Ast/100", "rim_assists_100_btposs", False, 2),
    ],
    "Defense": [
        ("STL%", "stl_per", True, 1),
        ("BLK%", "blk_per", True, 1),
        ("DBPM", "dbpm", False, 1),
    ],
    "Rebounding": [
        ("OREB%", "orb_per", True, 1),
        ("DREB%", "drb_per", True, 1),
    ],
}

GRADE_CATEGORIES: list[tuple[str, list[str]]] = [
    ("Impact", ["bpm", "rapm", "net_pts", "onoff_net_rating"]),
    ("Scoring", ["usg", "ts_per", "twop_per", "dunksmade", "rim_pct", "mid_pct", "tp_per", "threepa100", "ft_per", "ftr"]),
    ("Playmaking", ["ast_per", "to_per", "ast_tov", "rim_assists_100_btposs"]),
    ("Defense", ["stl_per", "blk_per", "dbpm"]),
    ("Rebounding", ["orb_per", "drb_per"]),
]

PER_GAME_KEYS = ["ppg", "rpg", "apg", "spg", "bpg", "fg_pct", "tp_pct", "ft_pct"]


def parse_years(spec: str) -> list[str]:
    out: list[int] = []
    for part in (spec or "").split(","):
        p = part.strip()
        if not p:
            continue
        if "-" in p:
            a, b = p.split("-", 1)
            start, end = int(a), int(b)
            step = 1 if end >= start else -1
            out.extend(range(start, end + step, step))
        else:
            out.append(int(p))
    return [str(y) for y in sorted(set(out))]


def settings_paths(project_root: Path) -> dict[str, Any]:
    settings_path = project_root / "player_cards_pipeline" / "config" / "settings.json"
    return json.loads(settings_path.read_text(encoding="utf-8"))


def rel_to_pipeline(project_root: Path, rel: str) -> Path:
    return project_root / "player_cards_pipeline" / rel


def init_or_open_db(path: Path, season: str, min_games: int) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    conn.execute("CREATE TABLE IF NOT EXISTS card_cache (cache_key TEXT PRIMARY KEY, payload_json TEXT NOT NULL)")
    meta = {
        "schema_version": str(bpc.CACHE_SCHEMA_VERSION),
        "season": str(season),
        "min_games": str(min_games),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    for k, v in meta.items():
        conn.execute("INSERT OR REPLACE INTO metadata(key, value) VALUES(?, ?)", (k, v))
    conn.commit()
    return conn


def stable_hash_payload(parts: dict[str, Any]) -> str:
    raw = json.dumps(parts, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def find_bt_row_for_target(target: bpc.PlayerGameStats, bt_rows: list[dict[str, str]]) -> dict[str, str]:
    pk = bpc.norm_player_name(target.player)
    tk = bpc.norm_team(target.team)
    yk = bpc.norm_season(target.season)
    for r in bt_rows:
        if (
            bpc.norm_player_name(bpc.bt_get(r, ["player_name"])) == pk
            and bpc.norm_team(bpc.bt_get(r, ["team"])) == tk
            and bpc.norm_season(bpc.bt_get(r, ["year"])) == yk
        ):
            return r
    return {}


def find_enriched_row_for_target(target: bpc.PlayerGameStats, enriched_rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not enriched_rows:
        return {}
    pk = bpc.norm_player_name(target.player)
    tk = bpc.norm_team(target.team)
    yk = bpc.norm_season(target.season)
    for p in enriched_rows:
        if (
            bpc.norm_player_name(p.get("key", "")) == pk
            and bpc.norm_team(p.get("team", "")) == tk
            and bpc.norm_season(p.get("year", "")) == yk
        ):
            return p
    for p in enriched_rows:
        if bpc.norm_player_name(p.get("key", "")) == pk and bpc.norm_season(p.get("year", "")) == yk:
            return p
    return {}


def build_bt_indexes(
    bt_rows: list[dict[str, str]],
) -> tuple[dict[tuple[str, str, str], dict[str, str]], dict[tuple[str, str], dict[str, str]]]:
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


def build_enriched_indexes(
    enriched_rows: list[dict[str, Any]],
) -> tuple[dict[tuple[str, str, str], dict[str, Any]], dict[tuple[str, str], dict[str, Any]]]:
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


def compute_source_hash(
    target: bpc.PlayerGameStats,
    bt_exact_idx: dict[tuple[str, str, str], dict[str, str]],
    bt_fallback_idx: dict[tuple[str, str], dict[str, str]],
    adv_rows: list[dict[str, str]],
    enriched_exact_idx: dict[tuple[str, str, str], dict[str, Any]],
    enriched_fallback_idx: dict[tuple[str, str], dict[str, Any]],
) -> str:
    pk = bpc.norm_player_name(target.player)
    tk = bpc.norm_team(target.team)
    yk = bpc.norm_season(target.season)
    bt_row = bt_exact_idx.get((pk, tk, yk)) or bt_fallback_idx.get((pk, yk), {})
    enriched_row = enriched_exact_idx.get((pk, tk, yk)) or enriched_fallback_idx.get((pk, yk), {})
    return stable_hash_payload(
        {
            "bt": bt_row,
            "adv_count": len(adv_rows),
            "enriched": enriched_row,
            "season": target.season,
            "player": target.player,
            "team": target.team,
        }
    )


def bt_row_key(r: dict[str, str]) -> tuple[str, str, str]:
    return (
        bpc.norm_player_name(bpc.bt_get(r, ["player_name"])),
        bpc.norm_team(bpc.bt_get(r, ["team"])),
        bpc.norm_season(bpc.bt_get(r, ["year"])),
    )


def player_key(p: bpc.PlayerGameStats) -> tuple[str, str, str]:
    return (
        bpc.norm_player_name(p.player),
        bpc.norm_team(p.team),
        bpc.norm_season(p.season),
    )


def _valid(v: float | None) -> bool:
    return v is not None and math.isfinite(v)


def build_fast_percentile_context(
    bt_rows: list[dict[str, str]],
    players_all: list[bpc.PlayerGameStats],
    season: str,
    min_games: int,
) -> dict[str, Any]:
    ys = bpc.norm_season(season)
    season_bt_rows = [r for r in bt_rows if bpc.norm_season(bpc.bt_get(r, ["year"])) == ys]
    pos_rows: dict[str, list[dict[str, str]]] = defaultdict(list)
    for r in season_bt_rows:
        pos_rows[bpc.bt_row_position_bucket(r)].append(r)

    metric_keys: set[str] = set()
    for section in BT_SECTION_SPECS.values():
        for _label, key, _is_pct, _digits in section:
            metric_keys.add(key)
    for _label, keys in GRADE_CATEGORIES:
        for key in keys:
            metric_keys.add(key)

    metric_vals_by_pos: dict[str, dict[str, list[float]]] = {}
    for pos, rows in pos_rows.items():
        metric_map: dict[str, list[float]] = {}
        for key in metric_keys:
            vals: list[float] = []
            for r in rows:
                v = bpc.bt_metric_value(r, key)
                if _valid(v):
                    vals.append(float(v))
            metric_map[key] = vals
        metric_vals_by_pos[pos] = metric_map

    # Precompute category score distributions (same semantics as bt_category_percentile).
    cat_scores_by_pos: dict[str, dict[str, dict[str, Any]]] = {}
    for pos, rows in pos_rows.items():
        by_cat: dict[str, dict[str, Any]] = {}
        metric_map = metric_vals_by_pos.get(pos, {})
        for cat_label, cat_keys in GRADE_CATEGORIES:
            row_scores: list[float] = []
            row_score_by_key: dict[tuple[str, str, str], float] = {}
            for r in rows:
                pcts: list[float] = []
                for key in cat_keys:
                    vals = metric_map.get(key, [])
                    if not vals:
                        continue
                    v = bpc.bt_metric_value(r, key)
                    if not _valid(v):
                        continue
                    p = bpc.percentile(float(v), vals)
                    if key == "to_per":
                        p = 100.0 - p
                    pcts.append(p)
                if not pcts:
                    continue
                s = sum(pcts) / len(pcts)
                row_scores.append(s)
                row_score_by_key[bt_row_key(r)] = s
            by_cat[cat_label] = {"scores": row_scores, "by_key": row_score_by_key}
        cat_scores_by_pos[pos] = by_cat

    # Per-game percentile cohorts by position.
    pos_by_player_key: dict[tuple[str, str, str], str] = {}
    for r in season_bt_rows:
        pos_by_player_key[bt_row_key(r)] = bpc.bt_row_position_bucket(r)

    season_players = [p for p in players_all if bpc.norm_season(p.season) == ys and p.games >= min_games]
    per_game_vals_by_pos: dict[str, dict[str, list[float]]] = {}
    # Default/all-position fallback
    all_vals: dict[str, list[float]] = {k: [] for k in PER_GAME_KEYS}
    for p in season_players:
        all_vals["ppg"].append(p.ppg)
        all_vals["rpg"].append(p.rpg)
        all_vals["apg"].append(p.apg)
        all_vals["spg"].append(p.spg)
        all_vals["bpg"].append(p.bpg)
        all_vals["fg_pct"].append(p.fg_pct)
        all_vals["tp_pct"].append(p.tp_pct)
        all_vals["ft_pct"].append(p.ft_pct)
    per_game_vals_by_pos["ALL"] = all_vals

    for pos in ("G", "F", "C"):
        vals: dict[str, list[float]] = {k: [] for k in PER_GAME_KEYS}
        for p in season_players:
            if pos_by_player_key.get(player_key(p)) != pos:
                continue
            vals["ppg"].append(p.ppg)
            vals["rpg"].append(p.rpg)
            vals["apg"].append(p.apg)
            vals["spg"].append(p.spg)
            vals["bpg"].append(p.bpg)
            vals["fg_pct"].append(p.fg_pct)
            vals["tp_pct"].append(p.tp_pct)
            vals["ft_pct"].append(p.ft_pct)
        per_game_vals_by_pos[pos] = vals

    return {
        "season": ys,
        "pos_rows": pos_rows,
        "metric_vals_by_pos": metric_vals_by_pos,
        "cat_scores_by_pos": cat_scores_by_pos,
        "per_game_vals_by_pos": per_game_vals_by_pos,
    }


def build_grade_boxes_html_fast(
    target: bpc.PlayerGameStats,
    target_row: dict[str, str] | None,
    ctx: dict[str, Any],
) -> str:
    if not target_row:
        return "".join(
            f'<div class="grade-chip"><div class="grade-k">{label}</div><div class="grade-v">--</div></div>'
            for label, _keys in GRADE_CATEGORIES
        )
    pos = bpc.bt_row_position_bucket(target_row)
    rk = player_key(target)
    by_cat = ctx.get("cat_scores_by_pos", {}).get(pos, {})
    chips: list[str] = []
    for cat_label, _keys in GRADE_CATEGORIES:
        c = by_cat.get(cat_label, {})
        score = c.get("by_key", {}).get(rk)
        vals = c.get("scores", [])
        pct = bpc.percentile(score, vals) if _valid(score) and vals else None
        grade = bpc.grade_from_percentile(pct)
        chips.append(
            f'<div class="grade-chip"><div class="grade-k">{cat_label}</div><div class="grade-v">{grade}</div></div>'
        )
    return "".join(chips)


def build_per_game_percentiles_fast(
    target: bpc.PlayerGameStats,
    target_row: dict[str, str] | None,
    ctx: dict[str, Any],
) -> dict[str, float | None]:
    pos = bpc.bt_row_position_bucket(target_row) if target_row else "ALL"
    vals = ctx.get("per_game_vals_by_pos", {}).get(pos) or ctx.get("per_game_vals_by_pos", {}).get("ALL", {})
    target_vals = {
        "ppg": target.ppg,
        "rpg": target.rpg,
        "apg": target.apg,
        "spg": target.spg,
        "bpg": target.bpg,
        "fg_pct": target.fg_pct,
        "tp_pct": target.tp_pct,
        "ft_pct": target.ft_pct,
    }
    out: dict[str, float | None] = {}
    for k in PER_GAME_KEYS:
        cohort = vals.get(k, [])
        out[k] = bpc.percentile_safe(target_vals[k], cohort)
    return out


def build_bt_percentile_html_fast(
    target: bpc.PlayerGameStats,
    target_row: dict[str, str] | None,
    ctx: dict[str, Any],
    adv_rows: list[dict[str, str]],
) -> str:
    if not target_row:
        return '<div class="panel" style="margin-top:14px;"><h3>Advanced Percentiles</h3><div class="shot-meta">No matching Bart Torvik row found for this player/team/season.</div></div>'

    pos = bpc.bt_row_position_bucket(target_row)
    metric_vals = ctx.get("metric_vals_by_pos", {}).get(pos, {})

    def section_rows(rows: list[tuple[str, str, bool, int]]) -> str:
        out = ""
        for label, key, is_pct, digits in rows:
            value = bpc.bt_metric_value(target_row, key)
            cohort_vals = metric_vals.get(key, [])
            pct = bpc.percentile(value, cohort_vals) if _valid(value) and cohort_vals else None
            if label == "STL%":
                stl_val = bpc.bt_display_stl_pct(value)
                out += bpc.bt_row_html(label, stl_val, pct, is_percent=is_pct, digits=1, truncate=True)
            elif label == "BLK%":
                blk_val = bpc.bt_display_blk_pct(value)
                out += bpc.bt_row_html(label, blk_val, pct, is_percent=False, digits=1, truncate=True)
            else:
                out += bpc.bt_row_html(label, value, pct, is_percent=is_pct, digits=digits)
        return out

    impact_html = section_rows(BT_SECTION_SPECS["Impact"])
    scoring_html = section_rows(BT_SECTION_SPECS["Scoring"])
    playmaking_html = section_rows(BT_SECTION_SPECS["Playmaking"])
    defense_html = section_rows(BT_SECTION_SPECS["Defense"])
    rebounding_html = section_rows(BT_SECTION_SPECS["Rebounding"])

    return f"""
      <div class="panel" style="margin-top:14px;">
        <h3>Advanced Percentiles</h3>
        <div class="shot-meta">Season: {target.season}</div>
        <div class="section-grid">
          <div class="section-card"><h4>Impact</h4>{impact_html}{bpc.build_bpm_trend_svg(target, adv_rows)}</div>
          <div class="section-card"><h4>Scoring</h4>{scoring_html}</div>
          <div class="section-card">
            <h4>Playmaking</h4>
            {playmaking_html}
            <h4 style="margin-top:4px;">Defense</h4>
            {defense_html}
            <h4 style="margin-top:4px;">Rebounding</h4>
            {rebounding_html}
          </div>
        </div>
      </div>
"""


def build_sections_payload(
    target: bpc.PlayerGameStats,
    bt_rows: list[dict[str, str]],
    players_all: list[bpc.PlayerGameStats],
    adv_rows: list[dict[str, str]],
    bt_playerstat_rows: list[dict[str, Any]],
    bio_lookup: dict[tuple[str, str, str], dict[str, str]],
    rsci_map: dict[str, int],
    min_games: int,
    include_player_comparisons: bool,
    include_draft_projection: bool,
    fast_ctx: dict[str, Any] | None = None,
    bt_row_idx: dict[tuple[str, str, str], dict[str, str]] | None = None,
) -> dict[str, Any]:
    target_row = (bt_row_idx or {}).get(player_key(target))
    if fast_ctx:
        bt_percentiles_html = build_bt_percentile_html_fast(target, target_row, fast_ctx, adv_rows)
        grade_boxes_html = build_grade_boxes_html_fast(target, target_row, fast_ctx)
        per_game_pcts = build_per_game_percentiles_fast(target, target_row, fast_ctx)
    else:
        bt_percentiles_html = bpc.build_bt_percentile_html(target, bt_rows, adv_rows, [])
        grade_boxes_html = bpc.build_grade_boxes_html(target, bt_rows)
        per_game_pcts = bpc.build_per_game_percentiles(players_all, target, min_games, bt_rows=bt_rows)
    self_creation_html = bpc.build_self_creation_html(target, bt_rows, bt_playerstat_rows, [], pbp_games_map={})
    playstyles_html = bpc.build_playstyles_html(target, bt_rows)
    team_impact_html = bpc.build_team_impact_html(target, bt_rows)
    shot_diet_html = bpc.build_shot_diet_html(target, bt_rows)
    player_comparisons_html = (
        bpc.build_player_comparisons_html(target, bt_rows, bio_lookup, top_n=5)
        if include_player_comparisons
        else ""
    )
    draft_projection_html = (
        bpc.build_draft_projection_html(target, bt_rows, bio_lookup, rsci_map)
        if include_draft_projection
        else ""
    )
    bt_fgm, bt_fga = bpc.bt_fg_totals_for_target(target, bt_rows)

    _act_pps, _exp_pps, pps_oe, pps_oe_pct = bpc.pps_over_expected_from_enriched(target)
    if pps_oe is not None:
        if pps_oe_pct is not None:
            p_rank = max(1, min(99, int(round(pps_oe_pct))))
            pps_line = f"Points per Shot Over Expectation: {pps_oe:+.1f}% ({bpc.ordinal(p_rank)} Percentile)"
        else:
            pps_line = f"Points per Shot Over Expectation: {pps_oe:+.1f}% (Percentile N/A)"
    else:
        pps_line = "Points per Shot Over Expectation: N/A"

    return {
        "bt_percentiles_html": bt_percentiles_html,
        "grade_boxes_html": grade_boxes_html,
        "self_creation_html": self_creation_html,
        "playstyles_html": playstyles_html,
        "team_impact_html": team_impact_html,
        "shot_diet_html": shot_diet_html,
        "player_comparisons_html": player_comparisons_html,
        "draft_projection_html": draft_projection_html,
        "pps_line": pps_line,
        "bt_fgm": bt_fgm,
        "bt_fga": bt_fga,
        "per_game_pcts": per_game_pcts,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Incrementally update per-season card section sqlite cache.")
    ap.add_argument("--project-root", default=".")
    ap.add_argument("--years", required=True, help="Years spec, e.g. 2026 or 2024-2026")
    ap.add_argument("--min-games", type=int, default=5)
    ap.add_argument("--out-dir", default="player_cards_pipeline/data/cache/card_sections")
    ap.add_argument(
        "--manifest-dir",
        default="player_cards_pipeline/data/cache/card_sections_manifest",
        help="Stores source-hash manifest used for incremental updates.",
    )
    ap.add_argument(
        "--checkpoint-every",
        type=int,
        default=25,
        help="Write manifest + commit sqlite progress every N players (resume safety).",
    )
    ap.add_argument(
        "--team-start",
        type=int,
        default=0,
        help="1-based inclusive team index start for chunked runs (0 = all teams).",
    )
    ap.add_argument(
        "--team-end",
        type=int,
        default=0,
        help="1-based inclusive team index end for chunked runs (0 = all teams).",
    )
    ap.add_argument(
        "--chunk-tag",
        default="",
        help="Optional chunk tag. When set, writes year_{chunk-tag}.sqlite/json so multiple chunks can run in parallel safely.",
    )
    ap.add_argument(
        "--players-jsonl",
        default="",
        help="Optional JSONL file with target rows: {player,team,season}. Restricts cache build to this subset.",
    )
    ap.add_argument(
        "--precomputed-hashes",
        default="",
        help="Optional JSON file mapping cache_key -> source_hash (from raw chunk stage).",
    )
    ap.add_argument(
        "--skip-stale-delete",
        action="store_true",
        help="Skip stale-key deletion. Recommended for subset/chunked merge workflows.",
    )
    ap.add_argument(
        "--include-player-comparisons",
        action="store_true",
        help="Also precompute player-comparison similarity HTML into cache (slower).",
    )
    ap.add_argument(
        "--include-draft-projection",
        action="store_true",
        help="Also precompute draft-projection HTML into cache (slower).",
    )
    args = ap.parse_args()

    project_root = Path(args.project_root).resolve()
    years = parse_years(args.years)
    if not years:
        raise SystemExit("No years parsed from --years")

    settings = settings_paths(project_root)
    bt_csv = rel_to_pipeline(project_root, settings["bt_advstats_csv"])
    _h, bt_rows = bpc.read_csv_rows(bt_csv)
    if not bt_rows:
        raise RuntimeError(f"No BT rows loaded from {bt_csv}")
    bpc.inject_enriched_fields_into_bt_rows(bt_rows)
    players_all = bpc.build_player_pool_from_bt(bt_rows)
    bt_exact_idx, bt_fallback_idx = build_bt_indexes(bt_rows)

    bio_lookup: dict[tuple[str, str, str], dict[str, str]] = {}
    bio_rel = settings.get("bio_csv", "")
    if bio_rel:
        bio_path = rel_to_pipeline(project_root, bio_rel)
        if bio_path.exists():
            bio_lookup = bpc.load_bio_lookup(bio_path)

    rsci_path = project_root / "player_cards_pipeline" / "data" / "manual" / "rsci" / "rsci_rankings.csv"
    rsci_map = bpc.load_rsci_rankings(rsci_path) if rsci_path.exists() else {}

    adv_rows_by_year: dict[str, list[dict[str, str]]] = {}
    adv_map = settings.get("advgames_csv_by_year", {}) or {}
    subset_targets: dict[tuple[str, str, str], bool] = {}
    subset_count = 0
    if args.players_jsonl:
        p = Path(args.players_jsonl).resolve()
        if not p.exists():
            raise SystemExit(f"--players-jsonl not found: {p}")
        for line in p.read_text(encoding="utf-8").splitlines():
            t = line.strip()
            if not t:
                continue
            row = json.loads(t)
            subset_targets[
                (
                    bpc.norm_player_name(str(row.get("player", ""))),
                    bpc.norm_team(str(row.get("team", ""))),
                    bpc.norm_season(str(row.get("season", ""))),
                )
            ] = True
            subset_count += 1
        print(f"[cache-incr] subset file loaded: {p} rows={subset_count}", flush=True)

    precomputed_hashes: dict[str, str] = {}
    if args.precomputed_hashes:
        hp = Path(args.precomputed_hashes).resolve()
        if not hp.exists():
            raise SystemExit(f"--precomputed-hashes not found: {hp}")
        loaded = json.loads(hp.read_text(encoding="utf-8"))
        if isinstance(loaded, dict):
            precomputed_hashes = {str(k): str(v) for k, v in loaded.items()}

    for y in years:
        rel = adv_map.get(y)
        if not rel:
            adv_rows_by_year[y] = []
            continue
        p = rel_to_pipeline(project_root, rel)
        if p.exists():
            _ah, rows = bpc.read_csv_rows(p)
            adv_rows_by_year[y] = rows
        else:
            adv_rows_by_year[y] = []

    out_dir = project_root / args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_dir = project_root / args.manifest_dir
    manifest_dir.mkdir(parents=True, exist_ok=True)

    for y in years:
        ys = bpc.norm_season(y)
        adv_rows = adv_rows_by_year.get(ys, [])
        fast_ctx = build_fast_percentile_context(
            bt_rows=bt_rows,
            players_all=players_all,
            season=ys,
            min_games=args.min_games,
        )
        season_players_all = [p for p in players_all if bpc.norm_season(p.season) == ys]
        season_players_all = sorted(
            season_players_all,
            key=lambda p: (bpc.norm_team(p.team), bpc.norm_player_name(p.player)),
        )
        all_team_keys = sorted({bpc.norm_team(p.team) for p in season_players_all if (p.team or "").strip()})
        total_teams = len(all_team_keys)
        team_start = int(args.team_start or 0)
        team_end = int(args.team_end or 0)
        chunked = team_start > 0 and team_end > 0
        if chunked:
            team_start = max(1, min(total_teams, team_start))
            team_end = max(team_start, min(total_teams, team_end))
            active_team_keys = set(all_team_keys[team_start - 1 : team_end])
        else:
            active_team_keys = set(all_team_keys)
            team_start, team_end = 1, total_teams

        season_players = [p for p in season_players_all if bpc.norm_team(p.team) in active_team_keys]
        if args.players_jsonl:
            season_players = [
                p
                for p in season_players
                if (
                    bpc.norm_player_name(p.player),
                    bpc.norm_team(p.team),
                    bpc.norm_season(p.season),
                )
                in subset_targets
            ]
        enriched_rows = bpc.load_enriched_players_for_script_season(ys) or []
        enriched_exact_idx, enriched_fallback_idx = build_enriched_indexes(enriched_rows)

        bt_playerstat_rows: list[dict[str, Any]] = []
        local_ps = project_root / "player_cards_pipeline" / "data" / "bt" / "raw_playerstat_json" / f"{ys}_pbp_playerstat_array.json"
        if local_ps.exists():
            try:
                bt_playerstat_rows = bpc.load_bt_playerstat_rows_from_source(str(local_ps))
            except Exception:
                bt_playerstat_rows = []
        bt_row_idx: dict[tuple[str, str, str], dict[str, str]] = {}
        for r in bt_rows:
            rk = bt_row_key(r)
            if rk[2] == ys and rk[0] and rk[1]:
                bt_row_idx[rk] = r

        chunk_tag = str(args.chunk_tag or "").strip()
        suffix = f"_{chunk_tag}" if chunk_tag else ""
        out_db = out_dir / f"{ys}{suffix}.sqlite"
        manifest_path = manifest_dir / f"{ys}{suffix}.json"
        prior_manifest: dict[str, str] = {}
        if manifest_path.exists():
            try:
                prior_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except Exception:
                prior_manifest = {}

        conn = init_or_open_db(out_db, ys, args.min_games)
        active_keys: set[str] = set()  # keys present in this chunk based on current source
        new_manifest: dict[str, str] = dict(prior_manifest) if chunked else {}
        built = 0
        skipped = 0
        print(
            f"[cache-incr] {ys}: players={len(season_players)} teams={len(active_team_keys)} "
            f"team_range={team_start}-{team_end}/{total_teams} chunked={chunked}"
        )

        checkpoint_every = max(1, int(args.checkpoint_every))
        with conn:
            for i, target in enumerate(season_players, start=1):
                ck = bpc.card_cache_key(target.player, target.team, target.season)
                active_keys.add(ck)
                src_hash = precomputed_hashes.get(ck)
                if not src_hash:
                    src_hash = compute_source_hash(
                        target=target,
                        bt_exact_idx=bt_exact_idx,
                        bt_fallback_idx=bt_fallback_idx,
                        adv_rows=adv_rows,
                        enriched_exact_idx=enriched_exact_idx,
                        enriched_fallback_idx=enriched_fallback_idx,
                    )
                new_manifest[ck] = src_hash

                if prior_manifest.get(ck) == src_hash:
                    row = conn.execute("SELECT 1 FROM card_cache WHERE cache_key=?", (ck,)).fetchone()
                    if row:
                        skipped += 1
                        continue

                payload = build_sections_payload(
                    target=target,
                    bt_rows=bt_rows,
                    players_all=players_all,
                    adv_rows=adv_rows,
                    bt_playerstat_rows=bt_playerstat_rows,
                    bio_lookup=bio_lookup,
                    rsci_map=rsci_map,
                    min_games=args.min_games,
                    include_player_comparisons=args.include_player_comparisons,
                    include_draft_projection=args.include_draft_projection,
                    fast_ctx=fast_ctx,
                    bt_row_idx=bt_row_idx,
                )
                conn.execute(
                    "INSERT OR REPLACE INTO card_cache(cache_key, payload_json) VALUES(?, ?)",
                    (ck, json.dumps(payload, ensure_ascii=True)),
                )
                built += 1
                if i % 50 == 0 or i == len(season_players):
                    print(f"[cache-incr] {ys}: {i}/{len(season_players)} built={built} skipped={skipped}", flush=True)

                # Periodic checkpoints so interrupted runs can resume without redoing all work.
                if (i % checkpoint_every) == 0:
                    conn.commit()
                    manifest_path.write_text(
                        json.dumps(new_manifest, ensure_ascii=True, sort_keys=True, indent=2),
                        encoding="utf-8",
                    )
                    print(
                        f"[cache-incr] {ys}: checkpoint {i}/{len(season_players)} "
                        f"manifest_keys={len(new_manifest)}",
                        flush=True,
                    )

            # Remove stale keys no longer present in season player pool.
            if not args.skip_stale_delete:
                stale_rows = conn.execute("SELECT cache_key FROM card_cache").fetchall()
                stale_keys: list[str] = []
                for r in stale_rows:
                    k = str(r[0])
                    parts = k.split("|")
                    if len(parts) != 3:
                        continue
                    team_k = parts[1]
                    if team_k not in active_team_keys:
                        continue
                    if k not in active_keys:
                        stale_keys.append(k)
                if stale_keys:
                    conn.executemany("DELETE FROM card_cache WHERE cache_key=?", [(k,) for k in stale_keys])
                    for k in stale_keys:
                        new_manifest.pop(k, None)
                    print(f"[cache-incr] {ys}: removed stale={len(stale_keys)}")

        conn.close()
        manifest_path.write_text(json.dumps(new_manifest, ensure_ascii=True, sort_keys=True, indent=2), encoding="utf-8")
        print(f"[cache-incr] {ys}: wrote db={out_db} manifest={manifest_path} built={built} skipped={skipped}")


if __name__ == "__main__":
    main()
