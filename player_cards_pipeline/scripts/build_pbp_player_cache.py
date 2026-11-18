#!/usr/bin/env python3
"""Build compact player shot and on/off caches from a CBBD plays CSV.

The raw play file is intentionally kept separate from the card build.  This
script converts it once per refresh into small, card-friendly CSVs.
"""
from __future__ import annotations

import argparse
import csv
import math
import re
from collections import defaultdict
from pathlib import Path


def truth(v: str) -> bool:
    return str(v or "").strip().lower() in {"1", "true", "yes", "y"}


def norm(v: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(v or "").lower())


def num(v: str) -> float:
    try:
        return float(str(v or "").replace(",", "").strip())
    except ValueError:
        return 0.0


def on_floor(row: dict[str, str]) -> dict[str, set[str]]:
    teams: dict[str, set[str]] = defaultdict(set)
    for i in range(20):
        name = row.get(f"onFloor[{i}].name", "").strip()
        team = row.get(f"onFloor[{i}].team", "").strip()
        if name and team:
            teams[norm(team)].add(name)
    return dict(teams)


def shot_kind(row: dict[str, str]) -> str:
    return str(row.get("shotInfo.range", "") or "").strip().lower()


def is_shot(row: dict[str, str]) -> bool:
    return bool(row.get("shotInfo.location.x", "").strip()) and bool(row.get("shootingPlay", "").strip())


def empty_stats() -> dict[str, float]:
    return defaultdict(float)


def event_stats(row: dict[str, str]) -> dict[str, float]:
    s = empty_stats()
    text = str(row.get("playText", "") or "").lower()
    team = norm(row.get("team", ""))
    if is_shot(row):
        kind = shot_kind(row)
        if kind == "three_pointer":
            s["fga"] += 1
            s["fgm"] += 1 if truth(row.get("shotInfo.made")) else 0
            s["3pa"] += 1
            s["3pm"] += 1 if truth(row.get("shotInfo.made")) else 0
        elif kind in {"rim", "jumper", "two_pointer"}:
            s["fga"] += 1
            s["fgm"] += 1 if truth(row.get("shotInfo.made")) else 0
            s["2pa"] += 1
            if kind == "rim":
                s["rim_att"] += 1
                s["rim_made"] += 1 if truth(row.get("shotInfo.made")) else 0
    if "free throw" in text:
        s["fta"] += 1
    if "turnover" in text:
        s["to"] += 1
    if "offensive rebound" in text:
        s["orb"] += 1
    if "defensive rebound" in text:
        s["drb"] += 1
    score = num(row.get("scoreValue", ""))
    if truth(row.get("scoringPlay")) and score:
        s["pts"] += score
    return s


def add(dst: dict[str, float], src: dict[str, float]) -> None:
    for k, v in src.items():
        dst[k] += v


def metric_rows(agg: dict[tuple[str, str, str], dict[str, float]]) -> list[dict[str, str]]:
    out = []
    for (player, team, side), s in sorted(agg.items()):
        s = defaultdict(float, s)
        poss = s["fga"] - s["orb"] + s["to"] + 0.475 * s["fta"]
        s["opp_poss"] = s["opp_fga"] - s["opp_orb"] + s["opp_to"] + 0.475 * s["opp_fta"]
        poss = max(poss, 1e-9)
        def pct(n: float, d: float) -> str:
            return f"{n / d:.6f}" if d else ""
        row = {
            "player": player, "team": team, "season": "", "side": side,
            "on.off_efg.old_value": pct(s["fgm"] + 0.5 * s["3pm"], s["fga"]),
            "on.off_to.value": pct(s["to"], poss),
            "on.off_2prim.value": pct(s["rim_made"], s["rim_att"]),
            "on.off_2primr.value": pct(s["rim_att"], s["fga"]),
            "on.off_ftr.value": pct(s["fta"], s["fga"]),
            "on.off_3pr.value": pct(s["3pa"], s["fga"]),
            "on.def_efg.old_value": pct(s["opp_fgm"] + 0.5 * s["opp_3pm"], s["opp_fga"]),
            "on.def_to.value": pct(s["opp_to"], s["opp_poss"]),
            "on.def_2prim.value": pct(s["opp_rim_made"], s["opp_rim_att"]),
            "on.def_2primr.value": pct(s["opp_rim_att"], s["opp_fga"]),
            "on.def_ftr.value": pct(s["opp_fta"], s["opp_fga"]),
            "on.def_3pr.value": pct(s["opp_3pa"], s["opp_fga"]),
            "on.off_orb.value": pct(s["orb"], s["orb"] + s["opp_drb"]),
            "on.def_orb.value": pct(s["opp_orb"], s["opp_orb"] + s["drb"]),
        }
        # The renderer uses the same field names for both sides.  Copy the
        # calculated off-court row into the off.* namespace below.
        prefix = "on." if side == "on" else "off."
        row = {k.replace("on.", prefix, 1) if k.startswith("on.") else k: v for k, v in row.items()}
        out.append(row)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--plays-csv", required=True)
    ap.add_argument("--season", required=True)
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    shots_path = out_dir / f"{args.season}_shots.csv"
    impact_path = out_dir / f"{args.season}_team_impact.csv"

    on_agg: dict[tuple[str, str, str], dict[str, float]] = defaultdict(empty_stats)
    totals: dict[tuple[str, str], dict[str, float]] = defaultdict(empty_stats)
    shot_fields = ["player", "team", "season", "gameId", "x", "y", "made", "range"]
    with open(args.plays_csv, newline="", encoding="utf-8") as f, open(shots_path, "w", newline="", encoding="utf-8") as sf:
        rd = csv.DictReader(f)
        sw = csv.DictWriter(sf, fieldnames=shot_fields)
        sw.writeheader()
        for row in rd:
            teams = on_floor(row)
            event_team = norm(row.get("team", ""))
            if not teams or not event_team:
                continue
            if is_shot(row):
                sw.writerow({
                    "player": row.get("shotInfo.shooter.name", ""),
                    "team": row.get("team", ""), "season": args.season,
                    "gameId": row.get("gameId", ""), "x": row.get("shotInfo.location.x", ""),
                    "y": row.get("shotInfo.location.y", ""), "made": row.get("shotInfo.made", ""),
                    "range": row.get("shotInfo.range", ""),
                })
            ev = event_stats(row)
            for team_key, players in teams.items():
                if not players:
                    continue
                team_total = totals[(team_key, "team")]
                if event_team == team_key:
                    add(team_total, ev)
                else:
                    opp_ev = {f"opp_{k}": v for k, v in ev.items()}
                    add(team_total, opp_ev)
                for player in players:
                    key = (player, team_key, "on")
                    if event_team == team_key:
                        add(on_agg[key], ev)
                    else:
                        add(on_agg[key], {f"opp_{k}": v for k, v in ev.items()})

    # Derive off-court totals by subtracting the on-court event counts from
    # the team totals.  The output has the same dotted keys as the card's
    # existing nested JSON source, so the renderer needs no special markup.
    rows = []
    all_keys = set()
    for s in totals.values(): all_keys.update(s)
    for (player, team, _), on_s in sorted(on_agg.items()):
        total = totals[(team, "team")]
        for side, src in (("on", on_s), ("off", {k: total[k] - on_s[k] for k in all_keys})):
            rec = {"player": player, "team": team, "season": args.season, "side": side}
            # Convert opponent event counters into the formula input expected
            # by metric_rows, then calculate one row and rename its prefix.
            calc = metric_rows({(player, team, side): src})[0]
            rec.update(calc)
            rec["player"] = player
            rec["team"] = team
            rec["season"] = args.season
            rec["side"] = side
            rows.append(rec)
    fields = sorted({k for row in rows for k in row})
    with impact_path.open("w", newline="", encoding="utf-8") as f:
        wr = csv.DictWriter(f, fieldnames=fields)
        wr.writeheader(); wr.writerows(rows)
    print(f"shots={shots_path} impact={impact_path} rows={len(rows)}")


if __name__ == "__main__":
    main()
