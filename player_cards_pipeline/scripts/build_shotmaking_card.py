#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import re
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TEMPLATE = ROOT / "player_cards_pipeline/templates/shotmaking_profile_template.html"
BT_ADV_ALL = ROOT / "player_cards_pipeline/data/bt/bt_advstats_2010_2026.csv"
FALLBACK_TEMPLATE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Shotmaking Profile</title>
<style>
body{margin:0;background:#0a0a0a;color:#f5f5f5;font-family:Segoe UI,Arial,sans-serif}.wrap{max-width:1180px;margin:18px auto;padding:16px}
.card{border:2px solid #3b3b3b;border-radius:12px;background:#000;padding:16px}.panel{border:1px solid #3b3b3b;border-radius:10px;background:#151515;padding:12px}
.basic-strip{display:grid;grid-template-columns:repeat(8,minmax(92px,1fr));gap:8px}.basic-chip{border:1px solid #3b3b3b;border-radius:8px;background:#202020;padding:8px;text-align:center}
.profile-grid{display:grid;grid-template-columns:386px max-content 390px;gap:14px;margin-top:14px;align-items:start}.bar-wrap{position:relative;height:12px;border-radius:999px;background:#2a2a2a;overflow:hidden}
.bar-fill{height:100%;border-radius:999px;background:linear-gradient(90deg,#9de7b7,#22c55e)}.bar-fill.drafted{background:#6ab7ff}.bar-fill.avg{background:#f5f5f5}
.pct-badge{position:absolute;top:50%;transform:translate(-50%,-50%);width:22px;height:22px;border-radius:50%;display:grid;place-items:center;color:#fff;background:#111;border:1px solid #3b3b3b;font-size:10px;font-weight:900}
.metric-row{display:grid;grid-template-columns:44px 190px 48px;gap:6px;align-items:center;min-height:20px}.zone-head{display:grid;grid-template-columns:44px 190px 48px;gap:6px;color:#d0d0d0;font-size:11px;font-weight:800;text-transform:uppercase}
.zone-head .zone-title{grid-column:1/3;white-space:nowrap}.zone-head .unit{grid-column:3;text-align:right}.metric-note{margin-top:8px;color:#d0d0d0;font-size:10px;line-height:1.3}
</style></head><body><div class="wrap"><article class="card">
<h1>Cameron Boozer</h1>
<div class="sub">Duke | 2026 | Position: PF | Age: 18.9 | Height: 6'9" | Statistical Height: 6'9", +0.00 in | RSCI: 2nd</div>
<section class="panel" style="margin-top:14px;"><h2>Basic Shooting Stats</h2><div class="basic-strip">
<div class="basic-chip"><div class="k">PPG</div><div class="v">22.5</div><div class="p">-- %tile</div></div><div class="basic-chip"><div class="k">TS%</div><div class="v">65.3</div><div class="p">-- %tile</div></div>
<div class="basic-chip"><div class="k">Usage</div><div class="v">31.2</div><div class="p">-- %tile</div></div><div class="basic-chip"><div class="k">Rim%</div><div class="v">64.5</div><div class="p">-- %tile</div></div>
<div class="basic-chip"><div class="k">Mid%</div><div class="v">42.3</div><div class="p">-- %tile</div></div><div class="basic-chip"><div class="k">2P%</div><div class="v">61.5</div><div class="p">-- %tile</div></div>
<div class="basic-chip"><div class="k">3P%</div><div class="v">39.1</div><div class="p">-- %tile</div></div><div class="basic-chip"><div class="k">FT%</div><div class="v">78.9</div><div class="p">-- %tile</div></div></div></section>
<div class="profile-grid"><section class="panel"><h2>Shot Chart</h2><div class="shot-meta">Attempts: 522 | Made: 318 | FG%: 60.9%</div><div class="shot-meta">Points per Shot Over Expectation: +15.1% (92nd Percentile)</div></section>
<section class="panel"><h3>PPS by Shot Type</h3><div id="ppsBars"></div><div class="metric-note" id="ppsNote"></div></section><section class="panel"><h2>Shot Diet</h2></section></div>
<script>
const shotBins = [];
const zones = [];
const selfCreation = [];
function metricRow(value,pct,fillClass,diff,digits){const row=document.createElement("div");const diffText=diff==null?"":`${diff>=0?"+":""}${diff.toFixed(digits)}`;row.className="metric-row";row.innerHTML=`<div style="font-weight:800;text-align:right">${value.toFixed(digits)}</div><div class="bar-wrap"><div class="bar-fill ${fillClass}" style="width:${Math.max(2,Math.min(100,pct))}%"></div><span class="pct-badge" style="left:${Math.max(4,Math.min(96,pct))}%">${Math.round(pct)}</span></div><div style="font-weight:800;text-align:right">${diffText}</div>`;return row;}
function zoneBlock(z){const b=document.createElement("div");b.innerHTML=`<div class="zone-head"><span class="zone-title">${z.label}</span><span class="unit">PPS</span></div>`;b.appendChild(metricRow(z.pps,z.ppsPct,"",null,3));b.appendChild(metricRow(z.drafted,z.draftedPpsPct,"drafted",z.pps-z.drafted,3));b.appendChild(metricRow(z.all,50,"avg",z.pps-z.all,3));return b;}
const ppsHost=document.getElementById("ppsBars");zones.forEach(z=>ppsHost.appendChild(zoneBlock(z)));
document.getElementById("ppsNote").textContent="Expected outputs are based on how the average PF and average drafted PF would expect to perform if given Cameron Boozer's exact shot diet.";
</script></article></div></body></html>"""


def norm_name(s: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", (s or "").lower())).strip()


def norm_team(s: str) -> str:
    s = (s or "").lower().replace("st.", "state").replace("st ", "state ")
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", s)).strip()


def canonical_pos(pos_raw: str) -> str:
    s = (pos_raw or "").upper().replace("S-", "")
    if "PG" in s or "SG" in s or "WG" in s or "CG" in s or s == "G":
        return "G"
    if "SF" in s or "PF" in s or "WF" in s or s == "F":
        return "F"
    if "C" in s:
        return "C"
    return "F"


def group_from_pos_freqs(row: dict[str, Any]) -> str:
    pf = row.get("posFreqs") or {}
    if isinstance(pf, dict) and pf:
        vals = {
            "pg": float(pf.get("pg") or 0.0),
            "sg": float(pf.get("sg") or 0.0),
            "sf": float(pf.get("sf") or 0.0),
            "pf": float(pf.get("pf") or 0.0),
            "c": float(pf.get("c") or 0.0),
        }
        top = max(vals.items(), key=lambda kv: kv[1])[0]
        if top in {"pg", "sg"}:
            return "G"
        if top in {"sf", "pf"}:
            return "F"
        return "C"
    return canonical_pos(row.get("posClass") or row.get("position") or "")


def has_exact_shot_coords(row: dict[str, Any]) -> bool:
    info = (((row.get("shotInfo") or {}).get("data") or {}).get("info")) or []
    if not isinstance(info, list) or not info:
        return False
    for rec in info:
        if not isinstance(rec, list) or len(rec) < 4:
            continue
        try:
            x = float(rec[0])
            y = float(rec[1])
            att = float(rec[3])
        except Exception:
            continue
        if math.isfinite(x) and math.isfinite(y) and math.isfinite(att) and att > 0:
            return True
    return False


def region(x: float, y: float) -> str:
    d = math.hypot(float(x), float(y))
    if d <= 4.5:
        return "rim"
    if d >= 22.0:
        return "three"
    return "mid"


def percentile(v: float, vals: list[float]) -> float | None:
    arr = sorted(x for x in vals if isinstance(x, (int, float)) and math.isfinite(x))
    if not arr or not math.isfinite(v):
        return None
    lt = sum(1 for x in arr if x < v)
    eq = sum(1 for x in arr if x == v)
    return ((lt + 0.5 * eq) / len(arr)) * 100.0


def ordinal(n: int) -> str:
    if 11 <= (n % 100) <= 13:
        return f"{n}th"
    tail = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{tail}"


def add_stat(m: dict[str, dict[str, float]], key: str, pts: float, att: float) -> None:
    row = m.setdefault(key, {"pts": 0.0, "att": 0.0})
    row["pts"] += float(pts)
    row["att"] += float(att)


def _rows_from_json_file(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    d = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(d, dict):
        return list(d.get("players") or [])
    if isinstance(d, list):
        return list(d)
    return []


def load_enriched_for_season(season: int) -> list[dict[str, Any]]:
    json_year = season - 1
    by_script = (
        ROOT
        / "player_cards_pipeline/data/manual/enriched_players/by_script_season"
        / f"players_all_Men_scriptSeason_{season}_fromJsonYear_{json_year}.json"
    )
    by_json = (
        ROOT
        / "player_cards_pipeline/data/manual/enriched_players/by_json_year"
        / f"players_all_Men_{json_year}_combined.json"
    )
    rows = _rows_from_json_file(by_script)
    if not rows:
        rows = _rows_from_json_file(by_json)
    return rows


def load_enriched_all_players_2019_2026() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for season in range(2019, 2027):
        rows.extend(load_enriched_for_season(season))
    return rows


def load_drafted_keys() -> tuple[set[str], set[str]]:
    candidates = [
        ROOT / "player_cards_pipeline/data/manual/rsci/drafted_players.csv",
        Path("/Users/henryhalverson/Downloads/Master Doc - drafted players.csv"),
    ]
    by_name_team: set[str] = set()
    by_year_name_team: set[str] = set()
    drafted_csv = next((p for p in candidates if p.exists()), None)
    if drafted_csv is None:
        return by_year_name_team, by_name_team
    with drafted_csv.open("r", encoding="utf-8-sig", newline="") as f:
        rd = csv.DictReader(f)
        for r in rd:
            year = (r.get("Year") or "").strip()
            name = norm_name(r.get("Player") or "")
            team = norm_team(r.get("Team") or "")
            if name:
                if team:
                    by_name_team.add(f"{name}|{team}")
                if year:
                    by_year_name_team.add(f"{year}|{name}|{team}")
    return by_year_name_team, by_name_team


def find_target(players: list[dict[str, Any]], player: str, team: str) -> dict[str, Any] | None:
    np = norm_name(player)
    nt = norm_team(team)
    for p in players:
        key = p.get("key") or ""
        key_name = " ".join(x.strip() for x in key.split(",")[::-1]).strip()
        if norm_name(key_name) == np and norm_team(p.get("team") or "") == nt:
            return p
    for p in players:
        key = p.get("key") or ""
        key_name = " ".join(x.strip() for x in key.split(",")[::-1]).strip()
        if norm_name(key_name) == np:
            return p
    return None


def actual_region(row: dict[str, Any]) -> dict[str, dict[str, float]]:
    out = {k: {"pts": 0.0, "att": 0.0} for k in ("overall", "rim", "mid", "three")}
    info = (((row.get("shotInfo") or {}).get("data") or {}).get("info")) or []
    for rec in info:
        if not isinstance(rec, list) or len(rec) < 4:
            continue
        x, y, pts, att = float(rec[0]), float(rec[1]), float(rec[2]), float(rec[3])
        if att <= 0:
            continue
        rg = region(x, y)
        out[rg]["pts"] += pts
        out[rg]["att"] += att
        out["overall"]["pts"] += pts
        out["overall"]["att"] += att
    return out


def build_rates(rows: list[dict[str, Any]]) -> tuple[dict[str, dict[str, float]], dict[str, dict[str, float]]]:
    by_bin: dict[str, dict[str, float]] = {}
    by_region: dict[str, dict[str, float]] = {}
    for p in rows:
        data = (p.get("shotInfo") or {}).get("data") or {}
        keys = data.get("keys") or []
        info = data.get("info") or []
        n = min(len(keys), len(info))
        for i in range(n):
            rec = info[i]
            if not isinstance(rec, list) or len(rec) < 4:
                continue
            x, y, pts, att = float(rec[0]), float(rec[1]), float(rec[2]), float(rec[3])
            if att <= 0:
                continue
            add_stat(by_bin, str(keys[i]), pts, att)
            rg = region(x, y)
            add_stat(by_region, rg, pts, att)
            add_stat(by_region, "overall", pts, att)
    return by_bin, by_region


def rate_for(by_bin: dict[str, dict[str, float]], by_region: dict[str, dict[str, float]], key: str, rg: str) -> float | None:
    b = by_bin.get(str(key))
    if b and b["att"] > 0:
        return b["pts"] / b["att"]
    r = by_region.get(rg)
    if r and r["att"] > 0:
        return r["pts"] / r["att"]
    o = by_region.get("overall")
    if o and o["att"] > 0:
        return o["pts"] / o["att"]
    return None


def expected_region(target: dict[str, Any], by_bin: dict[str, dict[str, float]], by_region: dict[str, dict[str, float]]) -> dict[str, dict[str, float]]:
    out = {k: {"pts": 0.0, "att": 0.0} for k in ("overall", "rim", "mid", "three")}
    data = (target.get("shotInfo") or {}).get("data") or {}
    keys = data.get("keys") or []
    info = data.get("info") or []
    n = min(len(keys), len(info))
    for i in range(n):
        rec = info[i]
        if not isinstance(rec, list) or len(rec) < 4:
            continue
        x, y, att = float(rec[0]), float(rec[1]), float(rec[3])
        if att <= 0:
            continue
        rg = region(x, y)
        rate = rate_for(by_bin, by_region, str(keys[i]), rg)
        if rate is None:
            continue
        out[rg]["pts"] += rate * att
        out[rg]["att"] += att
        out["overall"]["pts"] += rate * att
        out["overall"]["att"] += att
    return out


def diet_from_row(row: dict[str, Any]) -> dict[str, float]:
    counts = {"rim": 0.0, "mid": 0.0, "three": 0.0}
    info = (((row.get("shotInfo") or {}).get("data") or {}).get("info")) or []
    for rec in info:
        if not isinstance(rec, list) or len(rec) < 4:
            continue
        x, y, att = float(rec[0]), float(rec[1]), float(rec[3])
        if att <= 0:
            continue
        counts[region(x, y)] += att
    total = max(1e-9, counts["rim"] + counts["mid"] + counts["three"])
    return {k: (counts[k] / total * 100.0) for k in ("rim", "mid", "three")}


def diet_from_rows(rows: list[dict[str, Any]]) -> dict[str, float]:
    counts = {"rim": 0.0, "mid": 0.0, "three": 0.0}
    for row in rows:
        info = (((row.get("shotInfo") or {}).get("data") or {}).get("info")) or []
        for rec in info:
            if not isinstance(rec, list) or len(rec) < 4:
                continue
            x, y, att = float(rec[0]), float(rec[1]), float(rec[3])
            if att <= 0:
                continue
            counts[region(x, y)] += att
    total = max(1e-9, counts["rim"] + counts["mid"] + counts["three"])
    return {k: (counts[k] / total * 100.0) for k in ("rim", "mid", "three")}


def build_diet_block_html(title: str, diet: dict[str, float]) -> str:
    rim = diet.get("rim", 0.0)
    mid = diet.get("mid", 0.0)
    three = diet.get("three", 0.0)
    return f"""
            <div>
              <div class="diet-row-title"><span>{title}</span></div>
              <div class="shotdiet-bar">
                <span class="shotdiet-seg shotdiet-rim" style="width:{rim:.1f}%"></span>
                <span class="shotdiet-seg shotdiet-nonrim" style="width:{mid:.1f}%"></span>
                <span class="shotdiet-seg shotdiet-three" style="width:{three:.1f}%"></span>
              </div>
              <div class="shotdiet-key">
                <span><i class="shotdiet-dot shotdiet-rim"></i>Rim {rim:.1f}%</span>
                <span><i class="shotdiet-dot shotdiet-nonrim"></i>Non-Rim 2 {mid:.1f}%</span>
                <span><i class="shotdiet-dot shotdiet-three"></i>3PA {three:.1f}%</span>
              </div>
            </div>"""


def parse_bt_row(player: str, team: str) -> dict[str, str] | None:
    bt = ROOT / "player_cards_pipeline/data/bt/bt_advstats_2026.csv"
    with bt.open("r", encoding="utf-8-sig", newline="") as f:
        rd = csv.DictReader(f)
        np = norm_name(player)
        nt = norm_team(team)
        for r in rd:
            if norm_name(r.get("player_name") or "") == np and norm_team(r.get("team") or "") == nt:
                return r
    return None


_BT_MIN_PCT_CACHE: dict[tuple[str, str, str], float] | None = None


def _norm_season_key(v: str) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    if "/" in s:
        return s.split("/")[0].strip()
    m = re.search(r"(20\d{2})", s)
    return m.group(1) if m else s


def load_bt_min_pct_cache() -> dict[tuple[str, str, str], float]:
    global _BT_MIN_PCT_CACHE
    if _BT_MIN_PCT_CACHE is not None:
        return _BT_MIN_PCT_CACHE
    out: dict[tuple[str, str, str], float] = {}
    if not BT_ADV_ALL.exists():
        _BT_MIN_PCT_CACHE = out
        return out
    with BT_ADV_ALL.open("r", encoding="utf-8-sig", newline="") as f:
        rd = csv.DictReader(f)
        for r in rd:
            season = _norm_season_key(r.get("season", ""))
            name = norm_name(r.get("player_name", ""))
            team = norm_team(r.get("team", ""))
            if not season or not name or not team:
                continue
            try:
                min_pct = float(r.get("Min%", "") or 0.0)
            except Exception:
                continue
            out[(season, name, team)] = min_pct
    _BT_MIN_PCT_CACHE = out
    return out


def min_pct_ok(row: dict[str, Any], min_pct_cut: float = 20.0) -> bool:
    yv = row.get("year")
    if isinstance(yv, dict):
        season = _norm_season_key(str(yv.get("value") or ""))
    else:
        season = _norm_season_key(str(yv or ""))
    name = norm_name(" ".join(x.strip() for x in (row.get("key") or "").split(",")[::-1]).strip())
    team = norm_team(row.get("team") or "")
    if not season or not name or not team:
        return False
    min_map = load_bt_min_pct_cache()
    v = min_map.get((season, name, team))
    return (v is not None) and (v >= min_pct_cut)


def build(args: argparse.Namespace) -> str:
    season_i = int(args.season)
    season_players = load_enriched_for_season(season_i)
    target = find_target(season_players, args.player, args.team or "")
    if not target:
        raise SystemExit(f"Could not find enriched row for player={args.player} team={args.team}")
    players_all = load_enriched_all_players_2019_2026()
    pos = group_from_pos_freqs(target)
    key_name = " ".join(x.strip() for x in (target.get("key") or "").split(",")[::-1]).strip()
    team = target.get("team") or args.team or ""
    same = [
        p
        for p in players_all
        if group_from_pos_freqs(p) == pos
        and has_exact_shot_coords(p)
        and min_pct_ok(p, 20.0)
    ]
    drafted_year_keys, drafted_name_team_keys = load_drafted_keys()
    drafted = []
    for p in same:
        name = " ".join(x.strip() for x in (p.get("key") or "").split(",")[::-1]).strip()
        nn = norm_name(name)
        nt = norm_team(p.get("team") or "")
        yv = p.get("year")
        if isinstance(yv, dict):
            yr = str(yv.get("value") or "").strip()
        else:
            yr = str(yv or "").strip()
        token_y = f"{yr}|{nn}|{nt}" if yr else ""
        token_nt = f"{nn}|{nt}"
        if (token_y and token_y in drafted_year_keys) or token_nt in drafted_name_team_keys:
            drafted.append(p)
    # Keep drafted cohort strictly to players identified as drafted; do not
    # backfill with all-position players when the drafted cohort is empty.
    same_wo_target = [p for p in same if not (norm_name(" ".join(x.strip() for x in (p.get("key") or "").split(",")[::-1])) == norm_name(key_name) and norm_team(p.get("team") or "") == norm_team(team))]
    all_bin, all_region = build_rates(same_wo_target)
    dr_bin, dr_region = build_rates(drafted)
    act = actual_region(target)
    exp_all = expected_region(target, all_bin, all_region)
    exp_dr = expected_region(target, dr_bin, dr_region)
    poss = float(((target.get("off_team_poss") or {}).get("value")) or 0.0)
    if poss <= 0:
        poss = 1.0

    zones = []
    for rg, label in (("overall", "Overall"), ("rim", "Rim"), ("mid", "Mid-range"), ("three", "Three")):
        pps = act[rg]["pts"] / max(1e-9, act[rg]["att"])
        all_pps = exp_all[rg]["pts"] / max(1e-9, exp_all[rg]["att"])
        dr_pps = exp_dr[rg]["pts"] / max(1e-9, exp_dr[rg]["att"])
        pps_idx = round(max(1, min(99, 50 + 2 * ((pps - all_pps) / max(1e-9, all_pps) * 100))))
        dr_pps_idx = round(max(1, min(99, 50 + 2 * ((dr_pps - all_pps) / max(1e-9, all_pps) * 100))))
        p100 = act[rg]["pts"] / poss * 100.0
        all100 = exp_all[rg]["pts"] / poss * 100.0
        dr100 = exp_dr[rg]["pts"] / poss * 100.0
        p100_idx = round(max(1, min(99, 50 + 2 * ((p100 - all100) / max(1e-9, all100) * 100))))
        dr100_idx = round(max(1, min(99, 50 + 2 * ((dr100 - all100) / max(1e-9, all100) * 100))))
        zones.append(
            {
                "key": rg,
                "label": label,
                "pps": round(pps, 3),
                "all": round(all_pps, 3),
                "drafted": round(dr_pps, 3),
                "ppsPct": pps_idx,
                "draftedPpsPct": dr_pps_idx,
                "pts100": round(p100, 2),
                "all100": round(all100, 2),
                "drafted100": round(dr100, 2),
                "ptsPct": p100_idx,
                "draftedPtsPct": dr100_idx,
            }
        )

    bt = parse_bt_row(key_name, team) or {}
    basic = [
        ("PPG", float(bt.get("pts") or 0.0)),
        ("TS%", float(bt.get("TS_per") or 0.0)),
        ("Usage", float(bt.get("usg") or 0.0)),
        ("Rim%", float(bt.get("rimmade/(rimmade+rimmiss)") or 0.0) * 100.0),
        ("Mid%", float(bt.get("midmade/(midmade+midmiss)") or 0.0) * 100.0),
        ("2P%", float(bt.get("twoP_per") or 0.0) * 100.0),
        ("3P%", float(bt.get("TP_per") or 0.0) * 100.0),
        ("FT%", float(bt.get("FT_per") or 0.0) * 100.0),
    ]

    sc_path = ROOT / "player_cards_pipeline/data/bt/self_creation_by_year/self_creation_cache_2026.csv"
    self_rows: list[dict[str, Any]] = []
    if sc_path.exists():
        with sc_path.open("r", encoding="utf-8-sig", newline="") as f:
            rd = list(csv.DictReader(f))
        row = next((r for r in rd if norm_name(r.get("player_name") or "") == norm_name(key_name) and norm_team(r.get("team") or "") == norm_team(team)), None)
        if row:
            def pct_for(k: str, v: float) -> int:
                vals = [float(r.get(k) or 0.0) for r in rd]
                return int(round(percentile(v, vals) or 50))
            for label, key in [
                ("UAsst'd Dunks/100", "unassisted_dunks_100"),
                ("UAsst'd Rim FGM/100", "unassisted_rim_makes_100"),
                ("UAsst'd Mid FGM/100", "unassisted_mid_makes_100"),
                ("UAsst'd 3PM/100", "unassisted_3pm_100"),
                ("Unassisted Pts/100", "unassisted_points_100"),
            ]:
                v = float(row.get(key) or 0.0)
                self_rows.append({"label": label, "value": round(v, 2), "pct": pct_for(key, v)})

    attempted = int(round(act["overall"]["att"]))
    made = int(round((act["rim"]["pts"] / 2.0) + (act["mid"]["pts"] / 2.0) + (act["three"]["pts"] / 3.0)))
    fg = (made / attempted * 100.0) if attempted else 0.0
    oe = ((zones[0]["pps"] - zones[0]["all"]) / max(1e-9, zones[0]["all"]) * 100.0)

    grade_vals = []
    for p in same:
        ar = actual_region(p)
        er = expected_region(p, all_bin, all_region)
        if ar["overall"]["att"] <= 0 or er["overall"]["att"] <= 0:
            continue
        ap = ar["overall"]["pts"] / ar["overall"]["att"]
        ep = er["overall"]["pts"] / er["overall"]["att"]
        if ep > 0:
            grade_vals.append(((ap - ep) / ep) * 100.0)
    grade = int(round(percentile(oe, grade_vals) or 50))

    data = ((target.get("shotInfo") or {}).get("data") or {})
    info = data.get("info") or []
    shot_bins = [[round(float(r[0]), 4), round(float(r[1]), 4), float(r[2]), float(r[3])] for r in info if isinstance(r, list) and len(r) >= 4]

    if DEFAULT_TEMPLATE.exists():
        html = DEFAULT_TEMPLATE.read_text(encoding="utf-8")
    else:
        html = FALLBACK_TEMPLATE
    html = re.sub(r"<title>.*?</title>", f"<title>{key_name} Shotmaking Profile</title>", html, count=1, flags=re.S)
    html = html.replace("<h1>Cameron Boozer</h1>", f"<h1>{key_name}</h1>")
    ht_raw = bt.get("ht", "6-8")
    ht_fmt = ht_raw.replace("-", "'") + '"'
    age_val = float(((target.get("age") or {}).get("value")) or 0.0)
    subtitle = (
        f"{team} | {args.season} | Position: {pos} | Age: {age_val:.1f} | "
        f"Height: {ht_fmt} | Statistical Height: {ht_fmt}, +0.00 in | "
        f"RSCI: {ordinal(int(float(bt.get('Rec Rank', 50) or 50)))}"
    )
    html = re.sub(
        r"Duke \| 2026 \| Position: PF \| Age: [^|]+ \| Height: [^|]+ \| Statistical Height: [^|]+ \| RSCI: [^<]+",
        subtitle,
        html,
    )
    html = re.sub(r'<div class="v">\d+</div>', f'<div class="v">{grade}</div>', html, count=1)

    chips = "\n".join(
        f'          <div class="basic-chip"><div class="k">{k}</div><div class="v">{v:.1f}</div><div class="p">-- %tile</div></div>'
        for k, v in basic
    )
    html = re.sub(r'<div class="basic-strip">[\s\S]*?</div>\s*</section>', f'<div class="basic-strip">\n{chips}\n        </div>\n      </section>', html)
    html = html.replace("Cameron Boozer", key_name).replace("Drafted PFs", f"Drafted {pos}s").replace("All PFs", f"All {pos}s")
    html = html.replace("Drafted PF Avg", f"Drafted {pos} Avg").replace("All PF Avg", f"All {pos} Avg")
    html = re.sub(r"Attempts: \d+ \| Made: \d+ \| FG%: [\d.]+%", f"Attempts: {attempted} | Made: {made} | FG%: {fg:.1f}%", html)
    html = re.sub(r"Points per Shot Over Expectation: [^<]+", f"Points per Shot Over Expectation: {oe:+.1f}% ({ordinal(grade)} Percentile)", html)
    html = re.sub(r"const shotBins = \[[\s\S]*?\];\n\n    const zones =", f"const shotBins = {json.dumps(shot_bins)};\n\n    const zones =", html)
    html = re.sub(r"const zones = \[[\s\S]*?\n    \];\n\n    const selfCreation =", f"const zones = {json.dumps(zones, indent=6)};\n\n    const selfCreation =", html)
    if self_rows:
        html = re.sub(r"const selfCreation = \[[\s\S]*?\n    \];\n\n    function seeded", f"const selfCreation = {json.dumps(self_rows, indent=6)};\n\n    function seeded", html)
    note = f"Expected outputs are based on how the average {pos} and average drafted {pos} would expect to perform if given {key_name}'s exact shot diet."
    html = re.sub(
        r"Expected outputs are based on how the average [A-Z]{1,2} and average drafted [A-Z]{1,2} would expect to perform if given [^']+'s exact shot diet\.",
        note,
        html,
    )

    player_diet = diet_from_row(target)
    drafted_diet = diet_from_rows(drafted) if drafted else diet_from_rows(same)
    all_diet = diet_from_rows(same_wo_target if same_wo_target else same)
    diet_stack_html = (
        '<div class="diet-stack">'
        + build_diet_block_html(key_name, player_diet)
        + build_diet_block_html(f"Drafted {pos} Avg", drafted_diet)
        + build_diet_block_html(f"All {pos} Avg", all_diet)
        + "\n          </div>"
    )
    html = re.sub(r'<div class="diet-stack">[\s\S]*?</div>\s*</section>', f"{diet_stack_html}\n          </section>", html, count=1)
    return html


def main() -> None:
    ap = argparse.ArgumentParser(description="Build single-player shotmaking profile card HTML.")
    ap.add_argument("--season", required=True, help="Season year, e.g. 2026")
    ap.add_argument("--player", required=True, help="Player name")
    ap.add_argument("--team", default="", help="Team name for disambiguation")
    ap.add_argument("--out-html", required=True, help="Output HTML path")
    args = ap.parse_args()
    html = build(args)
    out = Path(args.out_html)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
