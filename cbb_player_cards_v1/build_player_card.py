#!/usr/bin/env python3
"""Generate a college basketball player card (HTML) from CBBD-style CSV data.

v1 goals:
- Bio block (name/team/position/class/height/age if available)
- Small per-game strip near top
- Percentile bars from cohort in plays dataset
- Shot chart from shot location x/y in plays dataset

Primary input is a plays CSV with columns similar to:
  participants[0].name, team, season,
  scoringPlay, scoreValue,
  playType, shotInfo.shooter.name, shotInfo.made,
  shotInfo.location.x, shotInfo.location.y
"""

from __future__ import annotations

import argparse
import csv
import difflib
import html
import json
import math
import re
from collections import defaultdict
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen


PLAY_TYPES_FT = {"MadeFreeThrow"}
PLAY_TYPES_REBOUND = {"Defensive Rebound", "Offensive Rebound", "Dead Ball Rebound"}

BIO_ALIAS_MAP = {
    "player": ["Player", "player", "player_name", "Name", "name"],
    "team": ["Team", "team", "team_name", "School", "school"],
    "year": ["Year", "year", "season", "Season"],
    "class": ["Class", "class", "Year In School", "year_in_school"],
    "height": ["Height", "height", "roster.height", "HEIGHT_WO_SHOES_FT_IN", "HEIGHT_W_SHOES_FT_IN"],
    "age": ["DD Age", "Age", "age"],
    "position": ["Role", "POSITION", "Position", "position", "roster.pos", "posClass"],
    "conference": ["Conference", "conference", "Conf", "conf"],
    "dob": ["DOB", "dob", "Birthdate", "birthdate", "Birthday", "birthday", "Date of Birth"],
}


@dataclass
class PlayerGameStats:
    player: str
    team: str
    season: str
    games: int
    points: int
    rebounds: int
    assists: int
    steals: int
    blocks: int
    fgm: int
    fga: int
    tpm: int
    tpa: int
    ftm: int
    fta: int

    @property
    def ppg(self) -> float:
        return self.points / self.games if self.games else 0.0

    @property
    def rpg(self) -> float:
        return self.rebounds / self.games if self.games else 0.0

    @property
    def apg(self) -> float:
        return self.assists / self.games if self.games else 0.0

    @property
    def spg(self) -> float:
        return self.steals / self.games if self.games else 0.0

    @property
    def bpg(self) -> float:
        return self.blocks / self.games if self.games else 0.0

    @property
    def fg_pct(self) -> float:
        return (100.0 * self.fgm / self.fga) if self.fga else 0.0

    @property
    def tp_pct(self) -> float:
        return (100.0 * self.tpm / self.tpa) if self.tpa else 0.0

    @property
    def ft_pct(self) -> float:
        return (100.0 * self.ftm / self.fta) if self.fta else 0.0


def norm_text(v: Any) -> str:
    if v is None:
        return ""
    return " ".join(str(v).strip().lower().split())


def norm_team(v: Any) -> str:
    s = norm_text(v)
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def norm_player_name(v: Any) -> str:
    s = str(v or "").strip()
    if "," in s:
        last, first = s.split(",", 1)
        s = f"{first.strip()} {last.strip()}".strip()
    return norm_text(s)


def norm_season(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    # 2024/25 -> 2025
    m = re.match(r"^\s*(20\d{2})\s*/\s*(\d{2})\s*$", s)
    if m:
        return f"20{m.group(2)}"
    # 2024-25 -> 2025
    m = re.match(r"^\s*(20\d{2})\s*-\s*(\d{2})\s*$", s)
    if m:
        return f"20{m.group(2)}"
    m = re.search(r"(20\d{2})", s)
    return m.group(1) if m else norm_text(s)


def to_bool(v: Any) -> bool:
    s = norm_text(v)
    return s in {"true", "1", "yes", "y"}


def to_float(v: Any) -> float | None:
    if v is None:
        return None
    s = str(v).strip().replace(",", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def read_csv_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        rows = list(csv.reader(f))
    if not rows:
        return [], []

    # Handle files with two header rows (e.g., master scouting docs).
    if len(rows) >= 2 and "Player" in rows[1] and rows[0].count("") > (len(rows[0]) * 0.5):
        header = rows[1]
        data_rows = rows[2:]
    else:
        header = rows[0]
        data_rows = rows[1:]

    out: list[dict[str, str]] = []
    for r in data_rows:
        if len(r) < len(header):
            r = r + [""] * (len(header) - len(r))
        out.append({header[i]: r[i] for i in range(len(header))})
    return header, out


def find_col(header: list[str], aliases: list[str]) -> str | None:
    hset = {h: norm_text(h) for h in header}
    alias_norm = {norm_text(a) for a in aliases}
    for col, normed in hset.items():
        if normed in alias_norm:
            return col
    return None


def load_bio_lookup(path: Path) -> dict[tuple[str, str, str], dict[str, str]]:
    def load_trank_fixed_lookup() -> dict[tuple[str, str, str], dict[str, str]]:
        lookup: dict[tuple[str, str, str], dict[str, str]] = {}
        with path.open("r", newline="", encoding="utf-8-sig") as f:
            rows = list(csv.reader(f))
        if not rows:
            return lookup
        for r in rows:
            if len(r) < 67:
                continue
            player = r[0].strip()
            team = r[1].strip()
            year = r[31].strip()  # 1-based col 32
            if not player or not team or not year:
                continue
            key = key_player_team_season(player, team, year)
            if key in lookup:
                continue
            lookup[key] = {
                "class": r[25].strip(),      # 1-based col 26
                "height": r[26].strip(),     # 1-based col 27
                "age": "",
                "position": r[64].strip(),   # 1-based col 65
                "conference": r[2].strip(),  # 1-based col 3
                "dob": r[66].strip(),        # 1-based col 67
            }
        return lookup

    header, rows = read_csv_rows(path)
    if not header:
        return load_trank_fixed_lookup()
    col_player = find_col(header, BIO_ALIAS_MAP["player"])
    col_team = find_col(header, BIO_ALIAS_MAP["team"])
    col_year = find_col(header, BIO_ALIAS_MAP["year"])
    if not col_player or not col_team or not col_year:
        return load_trank_fixed_lookup()

    bio_cols = {
        "class": find_col(header, BIO_ALIAS_MAP["class"]),
        "height": find_col(header, BIO_ALIAS_MAP["height"]),
        "age": find_col(header, BIO_ALIAS_MAP["age"]),
        "position": find_col(header, BIO_ALIAS_MAP["position"]),
        "conference": find_col(header, BIO_ALIAS_MAP["conference"]),
        "dob": find_col(header, BIO_ALIAS_MAP["dob"]),
    }

    lookup: dict[tuple[str, str, str], dict[str, str]] = {}
    for row in rows:
        key = key_player_team_season(
            row.get(col_player, ""),
            row.get(col_team, ""),
            row.get(col_year, ""),
        )
        if key in lookup:
            continue
        lookup[key] = {
            k: (row.get(v, "") if v else "")
            for k, v in bio_cols.items()
        }
    return lookup


def key_player_team_season(player: str, team: str, season: str) -> tuple[str, str, str]:
    return norm_player_name(player), norm_text(team), norm_season(season)


def _season_from_row(row: dict[str, str], season_hint: str = "") -> str:
    s = str(row.get("season", "")).strip()
    if s:
        return norm_season(s)
    if season_hint:
        return norm_season(season_hint)
    d = str(row.get("date", "")).strip()
    m = re.match(r"^\s*(\d{4})-(\d{2})-\d{2}\s*$", d)
    if m:
        y = int(m.group(1))
        mo = int(m.group(2))
        # NCAA season year label: Jan-Jun -> same year, Jul-Dec -> next year.
        return str(y if mo <= 6 else y + 1)
    return ""


def _resolve_side_team(row: dict[str, str], side_key: str) -> str:
    raw = str(row.get(side_key, "")).strip()
    side = norm_text(raw)
    if side == "home":
        return str(row.get("home", "")).strip()
    if side == "away":
        return str(row.get("away", "")).strip()
    # Older feeds provide actual team names in shot_team/action_team.
    if raw and side not in {"na", "none", "nan"}:
        return raw
    return ""


def _team_from_row(row: dict[str, str]) -> str:
    team = str(row.get("team", "")).strip()
    if team and norm_text(team) not in {"na", "none", "nan"}:
        return team
    t = _resolve_side_team(row, "shot_team")
    if t:
        return t
    t = _resolve_side_team(row, "action_team")
    if t:
        return t
    t = str(row.get("possession_before", "")).strip()
    if t and norm_text(t) not in {"na", "none", "nan"}:
        return t
    return ""


def _shot_made_from_row(row: dict[str, str]) -> bool:
    made = row.get("shotInfo.made")
    if made is not None and str(made).strip() != "":
        return to_bool(made)
    return norm_text(row.get("shot_outcome", "")) == "made"


def _shot_range_from_row(row: dict[str, str]) -> str:
    rng = norm_text(row.get("shotInfo.range", ""))
    if rng in {"rim", "jumper", "three_pointer"}:
        return rng

    if to_bool(row.get("three_pt", "")):
        return "three_pointer"

    sx = to_float(row.get("shot_x"))
    sy = to_float(row.get("shot_y"))
    if sx is not None and sy is not None:
        # ncaahoopR shot coords are in feet; hoops near x=+/-41.75, y=0.
        d1 = math.hypot(sx - 41.75, sy)
        d2 = math.hypot(sx + 41.75, sy)
        if min(d1, d2) <= 4.5:
            return "rim"
    return "jumper"


def _shot_loc_from_row(row: dict[str, str]) -> tuple[float | None, float | None]:
    x = to_float(row.get("shotInfo.location.x"))
    y = to_float(row.get("shotInfo.location.y"))
    if x is not None and y is not None:
        return x, y

    sx = to_float(row.get("shot_x"))
    sy = to_float(row.get("shot_y"))
    if sx is None or sy is None:
        return None, None

    # Convert roughly from feet coords (-47..47, -25..25) to CBBD 0..940 / 0..500 scale.
    full_x = (sx + 47.0) * 10.0
    full_y = (sy + 25.0) * 10.0
    return full_x, full_y


def _desc_rebound_player(desc: str) -> str:
    m = re.match(r"^(.*?) (Offensive|Defensive) Rebound\.", desc.strip())
    return m.group(1).strip() if m else ""


def _desc_steal_player(desc: str) -> str:
    m = re.match(r"^(.*?) Steal\.", desc.strip())
    return m.group(1).strip() if m else ""


def _desc_block_player(desc: str) -> str:
    s = desc.strip()
    m = re.match(r"^(.*?) Block\.", s)
    if m:
        return m.group(1).strip()
    m = re.search(r"Block by (.*?)\.", s)
    return m.group(1).strip() if m else ""


def build_player_stats(
    plays_rows: list[dict[str, str]],
    season_hint: str = "",
) -> tuple[dict[tuple[str, str, str], PlayerGameStats], dict[tuple[str, str, str], set[str]]]:
    stats: dict[tuple[str, str, str], dict[str, Any]] = {}
    games_by_player: dict[tuple[str, str, str], set[str]] = {}

    for row in plays_rows:
        season = _season_from_row(row, season_hint)
        team = _team_from_row(row)
        shooter = (row.get("shotInfo.shooter.name", "") or row.get("shooter", "")).strip()
        participant_0 = row.get("participants[0].name", "").strip()
        if not team:
            continue

        game_id = (
            str(row.get("gameId", "")).strip()
            or str(row.get("gameSourceId", "")).strip()
            or str(row.get("id", "")).strip()
            or str(row.get("game_id", "")).strip()
        )
        play_type = (row.get("playType", "") or "").strip()
        shot_range = _shot_range_from_row(row)
        shot_is_tracked_attempt = shot_range in {"rim", "jumper", "three_pointer"}
        made = _shot_made_from_row(row)
        description = str(row.get("description", "") or "")

        def get_bucket(player_name: str) -> dict[str, Any]:
            player_key = key_player_team_season(player_name, team, season)
            if game_id:
                games_by_player.setdefault(player_key, set()).add(game_id)
            return stats.setdefault(
                player_key,
                {
                    "player": player_name,
                    "team": team,
                    "season": season,
                    "points": 0,
                    "rebounds": 0,
                    "assists": 0,
                    "steals": 0,
                    "blocks": 0,
                    "fgm": 0,
                    "fga": 0,
                    "tpm": 0,
                    "tpa": 0,
                    "ftm": 0,
                    "fta": 0,
                },
            )

        # Rebounds/steals/blocks from CBBD event types or ncaahoopR descriptions.
        if play_type in PLAY_TYPES_REBOUND and participant_0:
            bucket = get_bucket(participant_0)
            bucket["rebounds"] += 1
        if play_type == "Steal" and participant_0:
            bucket = get_bucket(participant_0)
            bucket["steals"] += 1
        if play_type == "Block Shot" and participant_0:
            bucket = get_bucket(participant_0)
            bucket["blocks"] += 1
        if not play_type:
            rb = _desc_rebound_player(description)
            if rb:
                bucket = get_bucket(rb)
                bucket["rebounds"] += 1
            st = _desc_steal_player(description)
            if st:
                bucket = get_bucket(st)
                bucket["steals"] += 1
            blk = _desc_block_player(description)
            if blk:
                bucket = get_bucket(blk)
                bucket["blocks"] += 1

        # Field-goal attempts/makes and points should be credited to shooter only.
        if shooter and shot_is_tracked_attempt and (_shot_loc_from_row(row)[0] is not None or row.get("shot_outcome") is not None):
            bucket = get_bucket(shooter)
            bucket["fga"] += 1
            if made:
                bucket["fgm"] += 1

            if shot_range == "three_pointer":
                bucket["tpa"] += 1
                if made:
                    bucket["tpm"] += 1

            score_value = to_float(row.get("scoreValue"))
            if score_value is None:
                score_value = to_float(row.get("score_value"))
            scoring_play = to_bool(row.get("scoringPlay")) or to_bool(row.get("scoring_play")) or made
            if scoring_play and score_value is not None and math.isfinite(score_value):
                bucket["points"] += int(round(score_value))
            elif made:
                bucket["points"] += 3 if shot_range == "three_pointer" else 2

            assister = (row.get("shotInfo.assistedBy.name", "") or row.get("assist", "")).strip()
            if made and assister:
                assist_bucket = get_bucket(assister)
                assist_bucket["assists"] += 1

        # Free-throw attempts/makes and points should also be shooter-only.
        is_ft_event = (play_type in PLAY_TYPES_FT) or to_bool(row.get("free_throw"))
        if shooter and is_ft_event:
            bucket = get_bucket(shooter)
            bucket["fta"] += 1
            if made:
                bucket["ftm"] += 1
                bucket["points"] += 1

    out: dict[tuple[str, str, str], PlayerGameStats] = {}
    for key, v in stats.items():
        games = len(games_by_player.get(key, set()))
        if games <= 0:
            games = 1
        out[key] = PlayerGameStats(
            player=v["player"],
            team=v["team"],
            season=v["season"],
            games=games,
            points=v["points"],
            rebounds=v["rebounds"],
            assists=v["assists"],
            steals=v["steals"],
            blocks=v["blocks"],
            fgm=v["fgm"],
            fga=v["fga"],
            tpm=v["tpm"],
            tpa=v["tpa"],
            ftm=v["ftm"],
            fta=v["fta"],
        )
    return out, games_by_player


def percentile(value: float, cohort: list[float]) -> float:
    if not cohort:
        return 0.0
    less = sum(1 for x in cohort if x < value)
    equal = sum(1 for x in cohort if x == value)
    return 100.0 * (less + 0.5 * equal) / len(cohort)


def percentile_safe(value: float | None, cohort: list[float]) -> float | None:
    if value is None:
        return None
    vals = [x for x in cohort if x is not None and math.isfinite(x)]
    if not vals:
        return None
    return percentile(value, vals)


def collect_shots(
    plays_rows: list[dict[str, str]],
    player: str,
    team: str,
    season: str,
    season_hint: str = "",
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    np, nt, ns = norm_text(player), norm_text(team), norm_text(season)
    for row in plays_rows:
        rp = norm_text(row.get("shotInfo.shooter.name", "") or row.get("shooter", "") or row.get("participants[0].name", ""))
        rt = norm_text(_team_from_row(row))
        rs = norm_text(_season_from_row(row, season_hint))
        if (rp, rt, rs) != (np, nt, ns):
            continue
        x, y = _shot_loc_from_row(row)
        if x is None or y is None:
            continue
        made = _shot_made_from_row(row)
        shot_range = _shot_range_from_row(row)
        out.append(
            {
                "x": x,
                "y": y,
                "made": made,
                "range": shot_range,
            }
        )
    return out


def _fold_half_court(full_x: float, full_y: float) -> tuple[float, float]:
    court_len = 940.0
    court_wid = 500.0
    half_len = court_len / 2.0
    x2 = max(0.0, min(court_len, full_x))
    y2 = max(0.0, min(court_wid, full_y))
    x_half = min(x2, court_len - x2)
    return x_half, y2


def _shot_zone(shot: dict[str, Any]) -> str:
    rng = shot.get("range", "")
    xh, yy = _fold_half_court(float(shot["x"]), float(shot["y"]))
    if rng == "rim":
        return "Rim"
    if rng == "jumper":
        if xh <= 125 and 160 <= yy <= 340:
            return "Paint"
        return "Midrange"
    if rng == "three_pointer":
        if yy <= 65 or yy >= 435:
            return "Corner 3"
        if yy < 200:
            return "Wing 3 Left"
        if yy > 300:
            return "Wing 3 Right"
        return "Top 3"
    return "Other"


def _zone_pct_map(shots: list[dict[str, Any]]) -> dict[str, tuple[int, int, float]]:
    counts: dict[str, list[int]] = {}
    for s in shots:
        z = _shot_zone(s)
        made, att = counts.setdefault(z, [0, 0])
        counts[z] = [made + (1 if s.get("made") else 0), att + 1]
    out: dict[str, tuple[int, int, float]] = {}
    for z, (m, a) in counts.items():
        out[z] = (m, a, (100.0 * m / a) if a else 0.0)
    return out


def _color_for_delta(delta: float) -> str:
    # Requested mapping: red = good, blue = bad.
    m = min(1.0, abs(delta) / 15.0)
    if delta >= 0:
        r = int(130 + 120 * m)
        g = int(40 + 30 * (1.0 - m))
        b = int(50 + 40 * (1.0 - m))
    else:
        r = int(35 + 25 * (1.0 - m))
        g = int(80 + 50 * (1.0 - m))
        b = int(140 + 110 * m)
    return f"rgb({r},{g},{b})"


def shot_svg(
    shots: list[dict[str, Any]],
    season_shots: list[dict[str, Any]],
    width: int = 460,
    height: int = 300,
) -> str:
    # NCAA half-court geometry in CBBD's 94x50-ft coordinate scale (10 units per foot).
    court_len = 940.0
    court_wid = 500.0
    half_len = court_len / 2.0
    margin = 20.0

    def map_x(full_y: float) -> float:
        y2 = max(0.0, min(court_wid, full_y))
        return margin + y2 * (width - 2 * margin) / court_wid

    def map_y(full_x: float) -> float:
        x2 = max(0.0, min(court_len, full_x))
        x_half = min(x2, court_len - x2)
        return margin + x_half * (height - 2 * margin) / half_len

    def pt(full_x: float, full_y: float) -> tuple[float, float]:
        return map_x(full_y), map_y(full_x)

    points = []
    for s in shots:
        x = float(s["x"])
        y = float(s["y"])
        made = bool(s.get("made"))
        fill = "#2dd4bf" if made else "#f97316"
        points.append(
            f'<circle cx="{map_x(y):.1f}" cy="{map_y(x):.1f}" r="4.2" fill="{fill}" fill-opacity="0.8" />'
        )

    # Core court anchors (units where 10 = 1 foot).
    hoop_x = 40.0
    hoop_y = 250.0
    lane_x = 190.0
    lane_y_min = 190.0
    lane_y_max = 310.0
    ft_r = 60.0
    restricted_r = 40.0
    three_r = 221.46  # 22' 1.75"
    corner_y_min = 30.0
    corner_y_max = 470.0
    three_join_x = hoop_x + max(0.0, (three_r * three_r - (hoop_y - corner_y_min) ** 2) ** 0.5)

    ox1, oy1 = pt(0.0, 0.0)
    ox2, oy2 = pt(half_len, court_wid)
    lx1, ly1 = pt(0.0, lane_y_min)
    lx2, ly2 = pt(lane_x, lane_y_max)
    hx, hy = pt(hoop_x, hoop_y)
    bb1x, bb1y = pt(40.0 - 7.5, 220.0)
    bb2x, bb2y = pt(40.0 - 7.5, 280.0)
    ftcx, ftcy = pt(lane_x, hoop_y)
    c1x1, c1y1 = pt(0.0, corner_y_min)
    c1x2, c1y2 = pt(three_join_x, corner_y_min)
    c2x1, c2y1 = pt(0.0, corner_y_max)
    c2x2, c2y2 = pt(three_join_x, corner_y_max)
    arc_points: list[str] = []
    for i in range(81):
        yy = corner_y_min + (corner_y_max - corner_y_min) * (i / 80.0)
        dx = math.sqrt(max(0.0, three_r * three_r - (yy - hoop_y) ** 2))
        xx = hoop_x + dx
        px, py = pt(xx, yy)
        arc_points.append(f"{px:.1f},{py:.1f}")
    three_arc_polyline = " ".join(arc_points)

    px_per_unit_y = (width - 2 * margin) / court_wid
    px_per_unit_x = (height - 2 * margin) / half_len
    rr_x = restricted_r * px_per_unit_y
    rr_y = restricted_r * px_per_unit_x
    ft_rx = ft_r * px_per_unit_y
    ft_ry = ft_r * px_per_unit_x

    court = f"""
<rect x="{ox1:.1f}" y="{oy1:.1f}" width="{ox2-ox1:.1f}" height="{oy2-oy1:.1f}" fill="#0b1020" stroke="#2a385f" stroke-width="2"/>
<rect x="{lx1:.1f}" y="{ly1:.1f}" width="{lx2-lx1:.1f}" height="{ly2-ly1:.1f}" fill="none" stroke="#35507f" stroke-width="2"/>
<line x1="{bb1x:.1f}" y1="{bb1y:.1f}" x2="{bb2x:.1f}" y2="{bb2y:.1f}" stroke="#35507f" stroke-width="2"/>
<ellipse cx="{hx:.1f}" cy="{hy:.1f}" rx="6.0" ry="6.0" fill="none" stroke="#35507f" stroke-width="2"/>
<path d="M {map_x(hoop_y-restricted_r):.1f} {hy:.1f} A {rr_x:.1f} {rr_y:.1f} 0 0 1 {map_x(hoop_y+restricted_r):.1f} {hy:.1f}" fill="none" stroke="#35507f" stroke-width="2"/>
<ellipse cx="{ftcx:.1f}" cy="{ftcy:.1f}" rx="{ft_rx:.1f}" ry="{ft_ry:.1f}" fill="none" stroke="#35507f" stroke-width="2"/>
<line x1="{c1x1:.1f}" y1="{c1y1:.1f}" x2="{c1x2:.1f}" y2="{c1y2:.1f}" stroke="#35507f" stroke-width="2"/>
<line x1="{c2x1:.1f}" y1="{c2y1:.1f}" x2="{c2x2:.1f}" y2="{c2y2:.1f}" stroke="#35507f" stroke-width="2"/>
<polyline points="{three_arc_polyline}" fill="none" stroke="#35507f" stroke-width="2"/>
<line x1="{ox1:.1f}" y1="{oy2:.1f}" x2="{ox2:.1f}" y2="{oy2:.1f}" stroke="#35507f" stroke-width="2"/>
"""
    return f"""
<svg viewBox="0 0 {width} {height}" width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">
  {court}
  {''.join(points)}
</svg>
"""


def fmt(v: float, digits: int = 1) -> str:
    return f"{v:.{digits}f}"


def fmt_percent_source_value(v: float) -> float:
    # Bart percent fields can be on 0..1 or 0..100 scales.
    return v * 100.0 if 0.0 <= v <= 1.0 else v


def parse_date_maybe(v: str) -> datetime | None:
    s = (v or "").strip()
    if not s:
        return None
    for fmt_s in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(s, fmt_s)
        except ValueError:
            continue
    return None


def age_on_june25_for_season(dob_raw: str, season: str) -> str:
    dob = parse_date_maybe(dob_raw)
    if dob is None:
        return "N/A"
    m = re.search(r"(20\d{2})", str(season))
    if not m:
        return "N/A"
    ref_year = int(m.group(1))
    ref = datetime(ref_year, 6, 25)
    years = (ref - dob).days / 365.2425
    if years <= 0:
        return "N/A"
    return f"{years:.1f}"


def adv_bar(metric: str, value: float | None, pct: float | None, digits: int = 2) -> str:
    if value is None:
        return ""
    pct_num = 0.0 if pct is None else pct
    pct_lbl = "-" if pct is None else f"{pct:.0f}"
    return f"""
<div class="metric-row">
  <div class="metric-label">{html.escape(metric)}</div>
  <div class="metric-val">{value:.{digits}f}</div>
  <div class="bar-wrap"><div class="bar-fill" style="width:{pct_num:.1f}%"></div></div>
  <div class="metric-pct">{pct_lbl}</div>
</div>
"""


def lookup_row(rows: list[dict[str, str]], col_player: str, col_team: str, col_year: str, player: str, team: str, season: str) -> dict[str, str] | None:
    k = key_player_team_season(player, team, season)
    for row in rows:
        rk = key_player_team_season(row.get(col_player, ""), row.get(col_team, ""), row.get(col_year, ""))
        if rk == k:
            return row
    return None


def collect_numeric_column(rows: list[dict[str, str]], col: str) -> list[float]:
    out: list[float] = []
    for r in rows:
        v = to_float(r.get(col))
        if v is not None and math.isfinite(v):
            out.append(v)
    return out


def normalize_pct_maybe(v: float) -> float:
    # Some style percentile fields are on 0..1 scale.
    return v * 100.0 if 0.0 <= v <= 1.0 else v


def format_height(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return "N/A"
    if "'" in s or '"' in s:
        return s
    if re.fullmatch(r"\d+\s*-\s*\d+", s):
        a, b = re.split(r"\s*-\s*", s)
        return f"{int(a)}'{int(b)}\""
    v = to_float(s)
    if v is None:
        return s
    inches = int(round(v))
    if 48 <= inches <= 96:
        return f"{inches // 12}'{inches % 12}\""
    return s


def build_advanced_html(
    target: PlayerGameStats,
    lebron_rows: list[dict[str, str]],
    rim_rows: list[dict[str, str]],
    style_rows: list[dict[str, str]],
) -> str:
    blocks: list[str] = []

    # LEBRON block
    if lebron_rows:
        lr = lookup_row(lebron_rows, "Player", "Team", "Year", target.player, target.team, target.season)
        if lr:
            def lval(col: str) -> float | None:
                return to_float(lr.get(col))

            metrics = [
                ("LEBRON", lval("LEBRON"), collect_numeric_column(lebron_rows, "LEBRON"), 2),
                ("O-LEBRON", lval("O-LEBRON"), collect_numeric_column(lebron_rows, "O-LEBRON"), 2),
                ("D-LEBRON", lval("D-LEBRON"), collect_numeric_column(lebron_rows, "D-LEBRON"), 2),
                ("BPM", lval("BPM"), collect_numeric_column(lebron_rows, "BPM"), 1),
                ("TS", lval("TS"), collect_numeric_column(lebron_rows, "TS"), 1),
                ("Usg", lval("Usg"), collect_numeric_column(lebron_rows, "Usg"), 1),
                ("PRPG!", lval("PRPG!"), collect_numeric_column(lebron_rows, "PRPG!"), 1),
            ]
            body = ""
            for label, v, cohort, digits in metrics:
                body += adv_bar(label, v, percentile_safe(v, cohort), digits)
            if body:
                blocks.append(f'<div class="panel"><h3>Advanced: LEBRON Model</h3>{body}</div>')

    # Rimfluence block
    if rim_rows:
        rr = lookup_row(rim_rows, "player_name", "team_name", "year", target.player, target.team, target.season)
        if rr:
            def rval(col: str) -> float | None:
                return to_float(rr.get(col))

            metrics = [
                ("Rimfluence", rval("Rimfluence"), collect_numeric_column(rim_rows, "Rimfluence"), 2),
                ("Rimfluence z", rval("Rimfluence_z"), collect_numeric_column(rim_rows, "Rimfluence_z"), 2),
                ("Off Pts/100 Poss", rval("off_pts_per100poss"), collect_numeric_column(rim_rows, "off_pts_per100poss"), 1),
                ("Def Pts Saved/100", rval("def_pts_saved_per100poss"), collect_numeric_column(rim_rows, "def_pts_saved_per100poss"), 1),
            ]
            body = ""
            for label, v, cohort, digits in metrics:
                body += adv_bar(label, v, percentile_safe(v, cohort), digits)
            if body:
                blocks.append(f'<div class="panel"><h3>Advanced: Rimfluence</h3>{body}</div>')

    # Style + play type block from master/style sheet
    if style_rows:
        sr = lookup_row(style_rows, "Player", "Team", "Year", target.player, target.team, target.season)
        if sr:
            def sval(col: str) -> float | None:
                return to_float(sr.get(col))

            style_specs = [
                ("Rim Attack PPP", "Rim Attack PPP", "pctile_off_style_rim_attack_pct"),
                ("Attack & Kick PPP", "Attack & Kick PPP", "pctile_off_style_attack_kick_pct"),
                ("Transition PPP", "Transition PPP", "transition_pct"),
                ("PNR Passer PPP", "PNR Passer PPP", "pctile_off_style_pnr_passer_pct"),
                ("PnR Roller PPP", "PnR Roller PPP", "pctile_off_style_big_cut_roll_pct"),
                ("Post Up PPP", "Post Up PPP", "pctile_off_style_post_up_pct"),
            ]
            body = ""
            for label, raw_col, pct_col in style_specs:
                raw_v = sval(raw_col)
                pct_v = sval(pct_col)
                if pct_v is not None:
                    pct_v = normalize_pct_maybe(pct_v)
                # Fallback percentile if style percentile field missing.
                if pct_v is None and raw_v is not None:
                    pct_v = percentile_safe(raw_v, collect_numeric_column(style_rows, raw_col))
                body += adv_bar(label, raw_v, pct_v, 2)
            if body:
                blocks.append(f'<div class="panel"><h3>Advanced: Style + Play Types</h3>{body}</div>')

    if not blocks:
        return ""

    return f"""
      <div class="row" style="margin-top:14px;">
        {''.join(blocks[:2])}
      </div>
      {'<div class="row" style="margin-top:14px;">' + ''.join(blocks[2:4]) + '</div>' if len(blocks) > 2 else ''}
"""


def bt_get(row: dict[str, str], aliases: list[str]) -> str:
    alias_norm = {norm_text(a) for a in aliases}
    for k, v in row.items():
        if norm_text(k) in alias_norm:
            return v
    return ""


def bt_num(row: dict[str, str], aliases: list[str]) -> float | None:
    return to_float(bt_get(row, aliases))


def bt_find_target_row(rows: list[dict[str, str]], target: PlayerGameStats) -> dict[str, str] | None:
    np = norm_text(target.player)
    nt = norm_team(target.team)
    ny = norm_text(target.season)

    by_name_year = []
    for r in rows:
        rp = norm_text(bt_get(r, ["player_name"]))
        rt = norm_team(bt_get(r, ["team"]))
        ry = norm_text(bt_get(r, ["year"]))
        if rp == np and ry == ny:
            by_name_year.append(r)
            if rt == nt:
                return r
    return by_name_year[0] if by_name_year else None


def bt_cohort_for_year(rows: list[dict[str, str]], season: str) -> list[dict[str, str]]:
    ys = norm_text(season)
    cohort = [r for r in rows if norm_text(bt_get(r, ["year"])) == ys]
    return cohort if cohort else rows


def pbp_find_target_row(rows: list[dict[str, str]], target: PlayerGameStats) -> dict[str, str] | None:
    np = norm_text(target.player)
    nt = norm_team(target.team)
    ny = norm_text(target.season)
    for r in rows:
        rp = norm_text(r.get("player", ""))
        rt = norm_team(r.get("team", ""))
        ry = norm_text(r.get("season", ""))
        if rp == np and rt == nt and ry == ny:
            return r
    return None


def pbp_cohort_for_year(rows: list[dict[str, str]], season: str) -> list[dict[str, str]]:
    ys = norm_text(season)
    cohort = [r for r in rows if norm_text(r.get("season", "")) == ys]
    return cohort if cohort else rows


def pbp_metric_percentile(
    target_row: dict[str, str] | None,
    cohort_rows: list[dict[str, str]],
    key: str,
) -> tuple[float | None, float | None]:
    if not target_row:
        return None, None

    def pbp_metric_value(row: dict[str, str], metric_key: str) -> float | None:
        if metric_key == "unassisted_points_100":
            r = to_float(row.get("unassisted_rim_makes_100", ""))
            m = to_float(row.get("unassisted_mid_makes_100", ""))
            t = to_float(row.get("unassisted_3pm_100", ""))
            if r is None or m is None or t is None:
                return None
            return (2.0 * r) + (2.0 * m) + (3.0 * t)
        return to_float(row.get(metric_key, ""))

    val = pbp_metric_value(target_row, key)
    vals: list[float] = []
    for r in cohort_rows:
        v = pbp_metric_value(r, key)
        if v is not None and math.isfinite(v):
            vals.append(v)
    if val is None or not vals:
        return val, None
    return val, percentile(val, vals)


def load_bt_playerstat_rows_from_source(source: str) -> list[dict[str, Any]]:
    if not source:
        return []
    if source.startswith("http://") or source.startswith("https://"):
        req = Request(
            source,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json,text/plain,*/*",
            },
            method="GET",
        )
        with urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    else:
        raw = Path(source).read_text(encoding="utf-8")

    arr = json.loads(raw)
    out: list[dict[str, Any]] = []
    for r in arr:
        if not isinstance(r, list) or len(r) < 15:
            continue
        out.append(
            {
                "pid": r[0],
                "player": str(r[1]),
                "team": str(r[2]),
                "rim_made": float(r[3]),
                "rim_miss": float(r[4]),
                "rim_assisted": float(r[5]),
                "mid_made": float(r[6]),
                "mid_miss": float(r[7]),
                "mid_assisted": float(r[8]),
                "three_made": float(r[9]),
                "three_miss": float(r[10]),
                "three_assisted": float(r[11]),
                "dunks_made": float(r[12]),
                "dunks_miss": float(r[13]),
                "dunks_assisted": float(r[14]),
            }
        )
    return out


def bt_playerstat_metrics_from_row(ps_row: dict[str, Any], possessions: float | None) -> dict[str, float] | None:
    if possessions is None or possessions <= 0:
        return None
    un_rim = max(0.0, float(ps_row.get("rim_made", 0.0)) - float(ps_row.get("rim_assisted", 0.0)))
    un_mid = max(0.0, float(ps_row.get("mid_made", 0.0)) - float(ps_row.get("mid_assisted", 0.0)))
    un_3 = max(0.0, float(ps_row.get("three_made", 0.0)) - float(ps_row.get("three_assisted", 0.0)))
    un_dunks = max(0.0, float(ps_row.get("dunks_made", 0.0)) - float(ps_row.get("dunks_assisted", 0.0)))
    mul = 100.0 / float(possessions)
    return {
        "unassisted_dunks_100": un_dunks * mul,
        "unassisted_rim_makes_100": un_rim * mul,
        "unassisted_mid_makes_100": un_mid * mul,
        "unassisted_3pm_100": un_3 * mul,
        "unassisted_points_100": ((2.0 * un_rim) + (2.0 * un_mid) + (3.0 * un_3)) * mul,
    }


def find_bt_playerstat_row(
    rows: list[dict[str, Any]],
    player: str,
    team: str,
) -> dict[str, Any] | None:
    np = norm_player_name(player)
    nt = norm_team(team)
    exact = [r for r in rows if norm_player_name(r.get("player", "")) == np and norm_team(r.get("team", "")) == nt]
    if exact:
        return exact[0]
    by_name = [r for r in rows if norm_player_name(r.get("player", "")) == np]
    if not by_name:
        return None
    if len(by_name) == 1:
        return by_name[0]
    scored = sorted(
        ((difflib.SequenceMatcher(None, nt, norm_team(r.get("team", ""))).ratio(), r) for r in by_name),
        key=lambda x: x[0],
        reverse=True,
    )
    return scored[0][1] if scored and scored[0][0] >= 0.55 else by_name[0]


def build_pbp_off_possessions_map(pbp_rows: list[dict[str, str]]) -> dict[tuple[str, str, str], float]:
    out: dict[tuple[str, str, str], float] = defaultdict(float)
    for r in pbp_rows:
        p = norm_player_name(r.get("player", ""))
        t = norm_team(r.get("team", ""))
        y = norm_season(r.get("season", ""))
        poss = to_float(r.get("off_possessions", ""))
        if not p or not t or not y or poss is None or not math.isfinite(poss):
            continue
        out[(p, t, y)] = float(poss)
    return dict(out)


def bt_metric_value(row: dict[str, str], key: str) -> float | None:
    if key == "net_rating":
        ortg = bt_num(row, ["ORtg"])
        drtg = bt_num(row, ["drtg", "DRtg", " drtg"])
        if ortg is None or drtg is None:
            return None
        return ortg - drtg
    if key == "rim_pct":
        return bt_num(row, ["rimmade/(rimmade+rimmiss)", " rimmade/(rimmade+rimmiss)"])
    if key == "mid_pct":
        return bt_num(row, ["midmade/(midmade+midmiss)", " midmade/(midmade+midmiss)"])
    key_aliases = {
        "bpm": ["bpm", " bpm"],
        "obpm": ["obpm", " obpm"],
        "dbpm": ["dbpm", " dbpm"],
        "usg": ["usg"],
        "ts_per": ["TS_per"],
        "twop_per": ["twoP_per"],
        "dunksmade": ["dunksmade", " dunksmade"],
        "tp_per": ["TP_per"],
        "threepa100": ["3p/100?"],
        "ft_per": ["FT_per"],
        "ftr": ["ftr"],
        "ast_per": ["AST_per"],
        "to_per": ["TO_per"],
        "ast_tov": ["ast/tov", " ast/tov"],
        "stl_per": ["stl_per"],
        "blk_per": ["blk_per"],
        "orb_per": ["ORB_per"],
        "drb_per": ["DRB_per"],
        "possessions": ["possessions", " possessions"],
    }
    aliases = key_aliases.get(key, [key])
    return bt_num(row, aliases)


def bt_metric_percentile(
    target_row: dict[str, str],
    cohort_rows: list[dict[str, str]],
    key: str,
) -> tuple[float | None, float | None]:
    val = bt_metric_value(target_row, key)
    vals: list[float] = []
    for r in cohort_rows:
        v = bt_metric_value(r, key)
        if v is not None and math.isfinite(v):
            vals.append(v)
    if val is None or not vals:
        return val, None
    p = percentile(val, vals)
    if key == "to_per":
        p = 100.0 - p
    return val, p


def bt_row_html(
    label: str,
    value: float | None,
    pct: float | None,
    is_percent: bool = False,
    digits: int = 2,
    scale: float = 1.0,
    truncate: bool = False,
) -> str:
    if value is None:
        return ""
    shown = fmt_percent_source_value(value) if is_percent else value
    shown = shown * scale
    if truncate:
        factor = 10 ** digits
        shown = math.trunc(shown * factor) / factor
    else:
        shown = round(shown, digits)
    return adv_bar(label, shown, pct, digits=digits)


def build_bpm_trend_svg(target: PlayerGameStats, adv_rows: list[dict[str, str]]) -> str:
    if not adv_rows:
        return '<div class="shot-meta">No per-game BPM file loaded.</div>'
    np = norm_player_name(target.player)
    nt = norm_text(target.team)
    ys = norm_season(target.season)

    points_raw: list[tuple[int, str, float]] = []
    for r in adv_rows:
        if norm_player_name(r.get("pp", "")) != np:
            continue
        if norm_text(r.get("tt", "")) != nt:
            continue
        if norm_season(r.get("year", "")) != ys:
            continue
        nd = (r.get("numdate", "") or "").strip()
        bpm = to_float(r.get("bpm", ""))
        if not nd or bpm is None:
            continue
        try:
            ndi = int(nd)
        except ValueError:
            continue
        points_raw.append((ndi, r.get("datetext", ""), float(bpm)))

    points_raw.sort(key=lambda x: x[0])
    if len(points_raw) < 2:
        return '<div class="shot-meta">Not enough game-level BPM points for chart.</div>'

    w, h = 330, 130
    ml, mr, mt, mb = 38, 10, 10, 24
    xs = [i for i in range(len(points_raw))]
    ys_v = [p[2] for p in points_raw]
    ymin, ymax = min(ys_v), max(ys_v)
    if abs(ymax - ymin) < 1e-9:
        ymax = ymin + 1.0
    pad = 0.08 * (ymax - ymin)
    ymin -= pad
    ymax += pad

    def xpx(i: int) -> float:
        span = max(1, len(points_raw) - 1)
        return ml + i * (w - ml - mr) / span

    def ypx(v: float) -> float:
        return mt + (ymax - v) * (h - mt - mb) / (ymax - ymin)

    path = " ".join(
        ("M" if i == 0 else "L") + f" {xpx(i):.1f} {ypx(v):.1f}"
        for i, (_, _, v) in enumerate(points_raw)
    )

    # Show more small date labels across the axis.
    n = len(points_raw)
    tick_target = 7
    tick_idx = sorted({int(round(i * (n - 1) / (tick_target - 1))) for i in range(tick_target)})
    x_ticks = "".join(
        f'<text x="{xpx(i):.1f}" y="{h-8}" text-anchor="middle" font-size="9" fill="#9db2d6">{html.escape(points_raw[i][1] or str(points_raw[i][0]))}</text>'
        for i in tick_idx
    )
    y_vals = [ymin + k * (ymax - ymin) / 4.0 for k in range(5)]
    y_ticks = "".join(
        f'<text x="12" y="{ypx(v)+3:.1f}" text-anchor="start" font-size="9" fill="#9db2d6">{v:.1f}</text>'
        for v in y_vals
    )
    y_grid = "".join(
        f'<line x1="{ml}" y1="{ypx(v):.1f}" x2="{w-mr}" y2="{ypx(v):.1f}" stroke="#223453" stroke-width="0.8" stroke-dasharray="2 2"/>'
        for v in y_vals
    )
    dots = "".join(
        f'<circle cx="{xpx(i):.1f}" cy="{ypx(v):.1f}" r="2.4" fill="#40c7ff" />'
        for i, (_, _, v) in enumerate(points_raw)
    )
    return f"""
<div class="trend-wrap">
<svg viewBox="0 0 {w} {h}" width="{w}" height="{h}" xmlns="http://www.w3.org/2000/svg">
  <rect x="{ml}" y="{mt}" width="{w-ml-mr}" height="{h-mt-mb}" fill="#0b1020" stroke="#2a385f" stroke-width="1"/>
  {y_grid}
  <line x1="{ml}" y1="{ypx(0):.1f}" x2="{w-mr}" y2="{ypx(0):.1f}" stroke="#35507f" stroke-width="1" stroke-dasharray="3 3"/>
  <path d="{path}" fill="none" stroke="#40c7ff" stroke-width="2"/>
  {dots}
  {x_ticks}
  {y_ticks}
  <text x="{w/2:.1f}" y="{h-1}" text-anchor="middle" font-size="9" fill="#9db2d6">Date</text>
  <text x="6" y="{h/2:.1f}" text-anchor="start" font-size="9" fill="#9db2d6" transform="rotate(-90 6 {h/2:.1f})">BPM</text>
</svg>
</div>
"""


def grade_from_percentile(p: float | None) -> str:
    if p is None:
        return "--"
    if p >= 97:
        return "A+"
    if p >= 93:
        return "A"
    if p >= 90:
        return "A-"
    if p >= 87:
        return "B+"
    if p >= 83:
        return "B"
    if p >= 80:
        return "B-"
    if p >= 77:
        return "C+"
    if p >= 73:
        return "C"
    if p >= 70:
        return "C-"
    if p >= 67:
        return "D+"
    if p >= 63:
        return "D"
    if p >= 60:
        return "D-"
    return "F"


def bt_category_percentile(
    target_row: dict[str, str],
    cohort_rows: list[dict[str, str]],
    metric_keys: list[str],
) -> float | None:
    vals_by_key: dict[str, list[float]] = {}
    for key in metric_keys:
        vals: list[float] = []
        for r in cohort_rows:
            v = bt_metric_value(r, key)
            if v is not None and math.isfinite(v):
                vals.append(v)
        if vals:
            vals_by_key[key] = vals
    if not vals_by_key:
        return None

    def row_score(r: dict[str, str]) -> float | None:
        pcts: list[float] = []
        for key in metric_keys:
            vals = vals_by_key.get(key)
            if not vals:
                continue
            v = bt_metric_value(r, key)
            if v is None or not math.isfinite(v):
                continue
            p = percentile(v, vals)
            if key == "to_per":
                p = 100.0 - p
            pcts.append(p)
        if not pcts:
            return None
        return sum(pcts) / len(pcts)

    target_score = row_score(target_row)
    if target_score is None:
        return None
    cohort_scores = [s for s in (row_score(r) for r in cohort_rows) if s is not None]
    if not cohort_scores:
        return None
    return percentile(target_score, cohort_scores)


def build_grade_boxes_html(target: PlayerGameStats, bt_rows: list[dict[str, str]]) -> str:
    categories: list[tuple[str, list[str]]] = [
        ("Impact", ["bpm", "obpm", "dbpm", "net_rating"]),
        ("Scoring", ["usg", "ts_per", "twop_per", "dunksmade", "rim_pct", "mid_pct", "tp_per", "threepa100", "ft_per", "ftr"]),
        ("Playmaking", ["ast_per", "to_per", "ast_tov"]),
        ("Defense", ["stl_per", "blk_per", "dbpm"]),
        ("Rebounding", ["orb_per", "drb_per"]),
    ]
    if not bt_rows:
        return "".join(
            f'<div class="grade-chip"><div class="grade-k">{html.escape(label)}</div><div class="grade-v">--</div></div>'
            for label, _ in categories
        )

    target_row = bt_find_target_row(bt_rows, target)
    if not target_row:
        return "".join(
            f'<div class="grade-chip"><div class="grade-k">{html.escape(label)}</div><div class="grade-v">--</div></div>'
            for label, _ in categories
        )
    cohort = bt_cohort_for_year(bt_rows, target.season)
    chips = []
    for label, keys in categories:
        p = bt_category_percentile(target_row, cohort, keys)
        g = grade_from_percentile(p)
        chips.append(
            f'<div class="grade-chip"><div class="grade-k">{html.escape(label)}</div><div class="grade-v">{g}</div></div>'
        )
    return "".join(chips)


def build_bt_percentile_html(
    target: PlayerGameStats,
    bt_rows: list[dict[str, str]],
    adv_rows: list[dict[str, str]],
    pbp_rows: list[dict[str, str]],
) -> str:
    if not bt_rows:
        return '<div class="panel" style="margin-top:14px;"><h3>Advanced Percentiles</h3><div class="shot-meta">No Bart Torvik CSV loaded.</div></div>'

    target_row = bt_find_target_row(bt_rows, target)
    if not target_row:
        return '<div class="panel" style="margin-top:14px;"><h3>Advanced Percentiles</h3><div class="shot-meta">No matching Bart Torvik row found for this player/team/season.</div></div>'

    cohort = bt_cohort_for_year(bt_rows, target.season)
    pbp_target = pbp_find_target_row(pbp_rows, target) if pbp_rows else None
    pbp_cohort = pbp_cohort_for_year(pbp_rows, target.season) if pbp_rows else []

    sections = {
        "Impact": [
            ("BPM", "bpm", False, 1),
            ("OBPM", "obpm", False, 1),
            ("DBPM", "dbpm", False, 1),
            ("Net Rating", "net_rating", False, 1),
        ],
        "Scoring": [
            ("Usage", "usg", False, 1),
            ("TS%", "ts_per", True, 1),
            ("2P%", "twop_per", True, 1),
            ("Dunks/100", "pbp_dunks_100", False, 2),
            ("Rim Att/100", "pbp_rim_att_100", False, 2),
            ("Rim%", "rim_pct", True, 1),
            ("Mid%", "mid_pct", True, 1),
            ("3P%", "tp_per", True, 1),
            ("3PA/100", "threepa100", False, 2),
            ("FTA/100", "pbp_fta_100", False, 2),
            ("FT%", "ft_per", True, 1),
            ("FTr", "ftr", False, 1),
        ],
        "Playmaking": [
            ("AST%", "ast_per", True, 1),
            ("TO%", "to_per", True, 1),
            ("A/TO", "ast_tov", False, 2),
            ("Rim Ast/100", "pbp_rim_assists_100", False, 2),
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

    def section_rows(rows: list[tuple[str, str, bool, int]]) -> str:
        rows_html = ""
        for label, key, is_pct, digits in rows:
            if key.startswith("pbp_"):
                pbp_key = key.replace("pbp_", "")
                value, pct = pbp_metric_percentile(pbp_target, pbp_cohort, pbp_key)
                rows_html += bt_row_html(label, value, pct, is_percent=False, digits=digits)
                continue
            value, pct = bt_metric_percentile(target_row, cohort, key)
            if label == "BLK%":
                # Shift BLK% display two decimals left.
                rows_html += bt_row_html(label, value, pct, is_percent=is_pct, digits=1, scale=0.01, truncate=True)
            else:
                rows_html += bt_row_html(label, value, pct, is_percent=is_pct, digits=digits)
        return rows_html

    impact_html = section_rows(sections["Impact"])
    scoring_html = section_rows(sections["Scoring"])
    playmaking_html = section_rows(sections["Playmaking"])
    defense_html = section_rows(sections["Defense"])
    rebounding_html = section_rows(sections["Rebounding"])

    return f"""
      <div class="panel" style="margin-top:14px;">
        <h3>Advanced Percentiles</h3>
        <div class="shot-meta">Season: {html.escape(target.season)}</div>
        <div class="section-grid">
          <div class="section-card"><h4>Impact</h4>{impact_html}{build_bpm_trend_svg(target, adv_rows)}</div>
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


def build_self_creation_html(
    target: PlayerGameStats,
    bt_rows: list[dict[str, str]],
    bt_playerstat_rows: list[dict[str, Any]],
    pbp_rows: list[dict[str, str]],
) -> str:
    if not bt_playerstat_rows:
        return '<div class="panel"><h3>Self Creation</h3><div class="shot-meta">No Bart playerstat JSON loaded.</div></div>'
    target_bt = bt_find_target_row(bt_rows, target) if bt_rows else None
    target_ps = find_bt_playerstat_row(bt_playerstat_rows, target.player, target.team)
    if not target_bt or not target_ps:
        return '<div class="panel"><h3>Self Creation</h3><div class="shot-meta">No matching player/team/season in Bart playerstat JSON.</div></div>'

    pbp_poss_map = build_pbp_off_possessions_map(pbp_rows)
    target_poss = pbp_poss_map.get((norm_player_name(target.player), norm_team(target.team), norm_season(target.season)))
    if target_poss is None and target_bt:
        # Fallback when pbp metrics are unavailable in the runtime environment.
        target_poss = bt_metric_value(target_bt, "possessions")
    target_metrics = bt_playerstat_metrics_from_row(target_ps, target_poss)
    if not target_metrics:
        return '<div class="panel"><h3>Self Creation</h3><div class="shot-meta">Missing possessions for self-creation rate normalization.</div></div>'

    cohort_bt = bt_cohort_for_year(bt_rows, target.season)
    metric_vals: dict[str, list[float]] = defaultdict(list)
    for r in cohort_bt:
        ps = find_bt_playerstat_row(bt_playerstat_rows, bt_get(r, ["player_name"]), bt_get(r, ["team"]))
        if not ps:
            continue
        poss = pbp_poss_map.get(
            (
                norm_player_name(bt_get(r, ["player_name"])),
                norm_team(bt_get(r, ["team"])),
                norm_season(bt_get(r, ["year"])),
            )
        )
        if poss is None:
            poss = bt_metric_value(r, "possessions")
        m = bt_playerstat_metrics_from_row(ps, poss)
        if not m:
            continue
        for k, v in m.items():
            if math.isfinite(v):
                metric_vals[k].append(v)

    rows_html = ""
    specs = [
        ("UAsst'd Dunks/100", "unassisted_dunks_100"),
        ("UAsst'd Rim FGM/100", "unassisted_rim_makes_100"),
        ("UAsst'd Mid FGM/100", "unassisted_mid_makes_100"),
        ("UAsst'd 3PM/100", "unassisted_3pm_100"),
        ("Unassisted Pts/100", "unassisted_points_100"),
    ]
    for label, key in specs:
        value = target_metrics.get(key)
        cohort = metric_vals.get(key, [])
        pct = percentile(value, cohort) if value is not None and cohort else None
        rows_html += bt_row_html(label, value, pct, is_percent=False, digits=2)

    return f"""
      <div class="panel">
        <h3>Self Creation</h3>
        {rows_html}
      </div>
"""


def build_shot_diet_html(target: PlayerGameStats, bt_rows: list[dict[str, str]]) -> str:
    if not bt_rows:
        return '<div class="panel"><h3>Shot Diet</h3><div class="shot-meta">No Bart Torvik CSV loaded.</div></div>'

    row = bt_find_target_row(bt_rows, target)
    if not row:
        return '<div class="panel"><h3>Shot Diet</h3><div class="shot-meta">No matching Bart Torvik row found for this player/team/season.</div></div>'

    rim_att = bt_num(row, ["rimatt", " rimatt", "rimmade+rimmiss", " rimmade+rimmiss"])
    if rim_att is None:
        rm = bt_num(row, ["rimmade", " rimmade"]) or 0.0
        rmiss = bt_num(row, ["rimmiss", " rimmiss"]) or 0.0
        rim_att = rm + rmiss

    mid_att = bt_num(row, ["midatt", " midatt", "midmade+midmiss", " midmade+midmiss"])
    if mid_att is None:
        mm = bt_num(row, ["midmade", " midmade"]) or 0.0
        mmiss = bt_num(row, ["midmiss", " midmiss"]) or 0.0
        mid_att = mm + mmiss

    three_att = bt_num(row, ["TPA", " TPA", "tpa", " tpa"]) or 0.0
    total = rim_att + mid_att + three_att
    if total <= 0:
        return '<div class="panel"><h3>Shot Diet</h3><div class="shot-meta">No attempt data available.</div></div>'

    rim_pct = 100.0 * rim_att / total
    mid_pct = 100.0 * mid_att / total
    three_pct = 100.0 * three_att / total

    return f"""
      <div class="panel">
        <h3>Shot Diet</h3>
        <div class="shotdiet-bar">
          <div class="shotdiet-seg shotdiet-rim" style="width:{rim_pct:.2f}%"></div>
          <div class="shotdiet-seg shotdiet-mid" style="width:{mid_pct:.2f}%"></div>
          <div class="shotdiet-seg shotdiet-three" style="width:{three_pct:.2f}%"></div>
        </div>
        <div class="shotdiet-legend">
          <div class="shotdiet-key"><span class="shotdiet-dot shotdiet-rim"></span> Rim ({rim_pct:.1f}%)</div>
          <div class="shotdiet-key"><span class="shotdiet-dot shotdiet-mid"></span> Non-Rim 2 ({mid_pct:.1f}%)</div>
          <div class="shotdiet-key"><span class="shotdiet-dot shotdiet-three"></span> 3PA ({three_pct:.1f}%)</div>
        </div>
      </div>
"""


def _height_to_inches(raw: str) -> float | None:
    s = (raw or "").strip()
    if not s:
        return None
    m = re.match(r"^\s*(\d+)\s*'\s*(\d+)\s*\"?\s*$", s)
    if m:
        return float(int(m.group(1)) * 12 + int(m.group(2)))
    m = re.match(r"^\s*(\d+)\s*-\s*(\d+)\s*$", s)
    if m:
        return float(int(m.group(1)) * 12 + int(m.group(2)))
    v = to_float(s)
    if v is None:
        return None
    if 48 <= v <= 96:
        return float(v)
    return None


def _bio_age_height_for_row(row: dict[str, str], bio_lookup: dict[tuple[str, str, str], dict[str, str]]) -> tuple[float | None, float | None]:
    player = bt_get(row, ["player_name"])
    team = bt_get(row, ["team"])
    season = bt_get(row, ["year"])
    bk = key_player_team_season(player, team, season)
    bio = bio_lookup.get(bk, {})

    age_val: float | None = None
    if bio:
        age_s = age_on_june25_for_season(bio.get("dob", ""), season)
        if age_s != "N/A":
            age_val = to_float(age_s)
        if age_val is None:
            age_val = to_float(bio.get("age", ""))

    height_val: float | None = None
    if bio:
        height_val = _height_to_inches(bio.get("height", ""))
    if height_val is None:
        height_val = bt_num(row, ["inches", " inches"])

    return age_val, height_val


def build_player_comparisons_html(
    target: PlayerGameStats,
    bt_rows: list[dict[str, str]],
    bio_lookup: dict[tuple[str, str, str], dict[str, str]],
    top_n: int = 5,
) -> str:
    if not bt_rows:
        return '<div class="panel"><h3>Player Comparisons</h3><div class="shot-meta">No Bart Torvik CSV loaded.</div></div>'
    target_row = bt_find_target_row(bt_rows, target)
    if not target_row:
        return '<div class="panel"><h3>Player Comparisons</h3><div class="shot-meta">No matching Bart row for comparisons.</div></div>'

    metric_keys = [
        "bpm", "obpm", "dbpm", "net_rating",
        "usg", "ts_per", "twop_per", "dunksmade", "rim_pct", "mid_pct", "tp_per", "threepa100", "ft_per", "ftr",
        "ast_per", "to_per", "ast_tov",
        "stl_per", "blk_per", "orb_per", "drb_per",
    ]

    # Build per-season cohorts once.
    by_year: dict[str, list[dict[str, str]]] = defaultdict(list)
    for r in bt_rows:
        by_year[norm_season(bt_get(r, ["year"]))].append(r)

    def build_pct_lookup(items: list[tuple[int, float]]) -> dict[int, float]:
        # Percentile with midrank tie handling in O(n log n).
        if not items:
            return {}
        n = len(items)
        s = sorted(items, key=lambda x: x[1])
        out: dict[int, float] = {}
        i = 0
        while i < n:
            j = i + 1
            while j < n and s[j][1] == s[i][1]:
                j += 1
            p = 100.0 * (i + 0.5 * (j - i)) / n
            for k in range(i, j):
                out[s[k][0]] = p
            i = j
        return out

    # Precompute metric percentile lookup maps by year/key.
    metric_pct_map: dict[tuple[str, str], dict[int, float]] = {}
    for year, rows in by_year.items():
        for key in metric_keys:
            vals: list[tuple[int, float]] = []
            for r in rows:
                v = bt_metric_value(r, key)
                if v is None or not math.isfinite(v):
                    continue
                vals.append((id(r), float(v)))
            mp = build_pct_lookup(vals)
            if key == "to_per":
                mp = {rk: 100.0 - pv for rk, pv in mp.items()}
            metric_pct_map[(year, key)] = mp

    def metric_pct_for_row(r: dict[str, str], key: str) -> float | None:
        year = norm_season(bt_get(r, ["year"]))
        return metric_pct_map.get((year, key), {}).get(id(r))

    age_by_row: dict[int, float] = {}
    hgt_by_row: dict[int, float] = {}
    for r in bt_rows:
        age_v, h_v = _bio_age_height_for_row(r, bio_lookup)
        if age_v is not None and math.isfinite(age_v):
            age_by_row[id(r)] = age_v
        if h_v is not None and math.isfinite(h_v):
            hgt_by_row[id(r)] = h_v

    age_pct_map: dict[str, dict[int, float]] = {}
    hgt_pct_map: dict[str, dict[int, float]] = {}
    for year, rows in by_year.items():
        age_items = [(id(r), age_by_row[id(r)]) for r in rows if id(r) in age_by_row]
        hgt_items = [(id(r), hgt_by_row[id(r)]) for r in rows if id(r) in hgt_by_row]
        age_pct_map[year] = build_pct_lookup(age_items)
        hgt_pct_map[year] = build_pct_lookup(hgt_items)

    def age_pct_for_row(r: dict[str, str]) -> float | None:
        year = norm_season(bt_get(r, ["year"]))
        return age_pct_map.get(year, {}).get(id(r))

    def hgt_pct_for_row(r: dict[str, str]) -> float | None:
        year = norm_season(bt_get(r, ["year"]))
        return hgt_pct_map.get(year, {}).get(id(r))

    target_vec: dict[str, float] = {}
    for k in metric_keys:
        p = metric_pct_for_row(target_row, k)
        if p is not None:
            target_vec[k] = p
    tp_age = age_pct_for_row(target_row)
    tp_hgt = hgt_pct_for_row(target_row)
    if tp_age is not None:
        target_vec["age_pct"] = tp_age
    if tp_hgt is not None:
        target_vec["height_pct"] = tp_hgt

    if len(target_vec) < 8:
        return '<div class="panel"><h3>Player Comparisons</h3><div class="shot-meta">Not enough data to compute comparisons.</div></div>'

    def similarity(other: dict[str, str]) -> float | None:
        # Exclude exact same player-season.
        if (
            norm_player_name(bt_get(other, ["player_name"])) == norm_player_name(target.player)
            and norm_team(bt_get(other, ["team"])) == norm_team(target.team)
            and norm_season(bt_get(other, ["year"])) == norm_season(target.season)
        ):
            return None

        keys = list(metric_keys)
        ov: dict[str, float] = {}
        for k in keys:
            tv = target_vec.get(k)
            if tv is None:
                continue
            pv = metric_pct_for_row(other, k)
            if pv is None:
                continue
            ov[k] = pv

        if "age_pct" in target_vec:
            pv = age_pct_for_row(other)
            if pv is not None:
                ov["age_pct"] = pv
        if "height_pct" in target_vec:
            pv = hgt_pct_for_row(other)
            if pv is not None:
                ov["height_pct"] = pv

        shared = [k for k in ov if k in target_vec]
        if len(shared) < 8:
            return None

        # Percentile-space similarity: 100 - average absolute percentile gap.
        diffs = [abs(float(target_vec[k]) - float(ov[k])) for k in shared]
        score = 100.0 - (sum(diffs) / len(diffs))
        return max(0.0, min(100.0, score))

    ranked: list[tuple[float, dict[str, str]]] = []
    for r in bt_rows:
        s = similarity(r)
        if s is None:
            continue
        ranked.append((s, r))
    ranked.sort(key=lambda x: x[0], reverse=True)
    top = ranked[:top_n]
    if not top:
        return '<div class="panel"><h3>Player Comparisons</h3><div class="shot-meta">No comparable players found.</div></div>'

    rows_html = ""
    for score, r in top:
        pname = bt_get(r, ["player_name"]) or "Unknown"
        pyear = bt_get(r, ["year"]) or "?"
        rows_html += f'<div class="comp-row"><span class="comp-name">{html.escape(pname)}</span><span class="comp-year">{html.escape(str(pyear))}</span><span class="comp-score">{score:.1f}</span></div>'

    return f"""
      <div class="panel">
        <h3>Player Comparisons</h3>
        <div class="comp-table">
          {rows_html}
        </div>
      </div>
"""


def render_card(
    stats: PlayerGameStats,
    bio: dict[str, str],
    shots: list[dict[str, Any]],
    season_shots: list[dict[str, Any]],
    per_game_pcts: dict[str, float | None],
    grade_boxes_html: str,
    bt_percentiles_html: str,
    self_creation_html: str,
    shot_diet_html: str,
    player_comparisons_html: str,
    advanced_html: str,
    out_path: Path,
) -> None:
    name = stats.player
    team = stats.team
    season = stats.season

    age = age_on_june25_for_season(bio.get("dob", ""), season)
    if age == "N/A":
        age = bio.get("age", "") or "N/A"
    height = format_height(bio.get("height", ""))
    position = bio.get("position", "") or "N/A"
    subtitle = f"{team} | {season} | Position: {position} | Age: {age} | Height: {height}"

    # Use full event-derived FG totals for header stats, not only plotted (x/y) shots.
    shot_makes = stats.fgm
    shot_att = stats.fga
    shot_pct = (100.0 * shot_makes / shot_att) if shot_att else 0.0

    html_doc = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>{html.escape(name)} - Player Card</title>
<style>
:root {{
  --bg: #0b1220;
  --panel: #111a2e;
  --line: #22406c;
  --text: #dbe7ff;
  --muted: #9db2d6;
  --accent: #40c7ff;
  --bar: #2dd4bf;
}}
body {{
  margin: 0;
  background: radial-gradient(circle at 20% 0%, #1b2d51 0%, var(--bg) 45%);
  color: var(--text);
  font-family: "Segoe UI", Arial, sans-serif;
}}
.wrap {{
  max-width: 1100px;
  margin: 18px auto;
  padding: 16px;
}}
.card {{
  border: 2px solid var(--line);
  border-radius: 12px;
  background: rgba(13, 22, 38, 0.95);
  padding: 16px;
}}
.title {{
  font-size: 44px;
  line-height: 1;
  font-weight: 800;
  color: var(--accent);
  margin: 0 0 8px 0;
}}
.title-row {{
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
}}
.grade-strip {{
  display: grid;
  grid-template-columns: repeat(5, minmax(96px, 1fr));
  gap: 8px;
  min-width: 560px;
}}
.grade-chip {{
  border: 1px solid var(--line);
  border-radius: 8px;
  padding: 6px 8px;
  text-align: center;
  background: #0e1729;
}}
.grade-k {{
  color: var(--muted);
  font-size: 11px;
  line-height: 1.1;
}}
.grade-v {{
  font-size: 22px;
  font-weight: 800;
  line-height: 1.1;
  color: var(--accent);
}}
.sub {{
  color: var(--muted);
  margin-bottom: 12px;
  font-size: 15px;
}}
.row {{
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 14px;
}}
.panel {{
  border: 1px solid var(--line);
  border-radius: 10px;
  padding: 12px;
  background: var(--panel);
}}
.panel h3 {{
  margin: 0 0 4px 0;
  font-size: 14px;
}}
.section-grid {{
  display: grid;
  grid-template-columns: repeat(3, minmax(210px, 1fr));
  gap: 10px;
}}
.section-card {{
  border: 1px solid var(--line);
  border-radius: 8px;
  padding: 8px;
  background: #0e1729;
}}
.section-card h4 {{
  margin: 0 0 4px 0;
  font-size: 12px;
  color: #bcd1f5;
  letter-spacing: 0.1px;
}}
.kv {{
  display: grid;
  grid-template-columns: repeat(3, minmax(120px, 1fr));
  gap: 8px;
  font-size: 14px;
}}
.stat-strip {{
  margin-top: 10px;
  display: grid;
  grid-template-columns: repeat(8, 1fr);
  gap: 8px;
}}
.shot-wrap {{
  display: flex;
  justify-content: flex-start;
  gap: 12px;
  align-items: stretch;
}}
.shot-panel {{
  min-width: 0;
}}
.shot-panel svg {{
  display: block;
  margin: 0 auto;
}}
.shot-chart-col {{
  flex: 0 0 33%;
  min-width: 320px;
}}
.chip {{
  border: 1px solid var(--line);
  border-radius: 8px;
  padding: 8px;
  text-align: center;
  background: #0e1729;
}}
.chip .k {{
  color: var(--muted);
  font-size: 12px;
}}
.chip .v {{
  font-size: 20px;
  font-weight: 700;
}}
.chip .p {{
  margin-top: 3px;
  color: var(--muted);
  font-size: 10px;
  line-height: 1;
}}
.metric-row {{
  display: grid;
  grid-template-columns: 72px 58px 1fr 34px;
  gap: 6px;
  align-items: center;
  margin-bottom: 5px;
}}
.metric-label {{
  color: var(--muted);
  font-size: 12px;
}}
.metric-val {{
  font-weight: 700;
  font-size: 12px;
}}
.bar-wrap {{
  height: 12px;
  border-radius: 999px;
  background: #1e2e4d;
  overflow: hidden;
}}
.bar-fill {{
  height: 12px;
  background: linear-gradient(90deg, #60a5fa, var(--bar));
}}
.metric-pct {{
  text-align: right;
  font-weight: 700;
  font-size: 12px;
}}
.shot-meta {{
  font-size: 13px;
  color: var(--muted);
  margin-bottom: 8px;
}}
.trend-wrap {{
  margin-top: 8px;
}}
.shotdiet-bar {{
  width: 100%;
  height: 16px;
  border-radius: 999px;
  overflow: hidden;
  background: #1e2e4d;
  border: 1px solid #2a385f;
}}
.shotdiet-seg {{
  height: 100%;
  display: inline-block;
  vertical-align: top;
}}
.shotdiet-rim {{
  background: #2dd4bf;
}}
.shotdiet-mid {{
  background: #f97316;
}}
.shotdiet-three {{
  background: #60a5fa;
}}
.shotdiet-legend {{
  margin-top: 8px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 10px;
  font-size: 12px;
  color: var(--muted);
}}
.shotdiet-key {{
  display: flex;
  align-items: center;
  gap: 7px;
  white-space: nowrap;
}}
.shotdiet-dot {{
  width: 9px;
  height: 9px;
  border-radius: 999px;
  display: inline-block;
}}
.right-wrap {{
  flex: 1 1 auto;
  display: flex;
  flex-direction: column;
  margin-top: 14px;
}}
.right-top {{
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 12px;
  align-items: start;
}}
.right-bottom {{
  width: calc(50% - 6px);
  margin-top: auto;
}}
.comp-table {{
  display: grid;
  gap: 6px;
}}
.comp-row {{
  display: grid;
  grid-template-columns: 1fr 42px 42px;
  gap: 8px;
  font-size: 12px;
  align-items: center;
  border: 1px solid #233552;
  border-radius: 7px;
  padding: 6px 8px;
  background: #0e1729;
}}
.comp-name {{
  font-weight: 600;
  color: var(--text);
}}
.comp-year {{
  color: var(--muted);
  text-align: right;
}}
.comp-score {{
  color: var(--accent);
  text-align: right;
  font-weight: 700;
}}
@media (max-width: 920px) {{
  .title-row {{ flex-direction: column; }}
  .grade-strip {{ min-width: 0; width: 100%; grid-template-columns: repeat(2, minmax(130px, 1fr)); }}
  .row {{ grid-template-columns: 1fr; }}
  .stat-strip {{ grid-template-columns: repeat(3, 1fr); }}
  .section-grid {{ grid-template-columns: 1fr; }}
  .shot-panel {{ width: 100%; min-width: 0; }}
  .shot-chart-col {{ flex: 1 1 auto; min-width: 0; }}
  .right-wrap {{ width: 100%; margin-top: 14px; }}
  .right-top {{ grid-template-columns: 1fr; }}
  .right-bottom {{ width: 100%; }}
}}
</style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <div class="title-row">
        <h1 class="title">{html.escape(name)}</h1>
        <div class="grade-strip">{grade_boxes_html}</div>
      </div>
      <div class="sub">{html.escape(subtitle)}</div>

      <div class="panel">
        <h3>Per Game</h3>
        <div class="stat-strip">
          <div class="chip"><div class="k">PPG</div><div class="v">{fmt(stats.ppg)}</div><div class="p">{(f"{per_game_pcts['ppg']:.0f}%" if per_game_pcts.get('ppg') is not None else "")}</div></div>
          <div class="chip"><div class="k">RPG</div><div class="v">{fmt(stats.rpg)}</div><div class="p">{(f"{per_game_pcts['rpg']:.0f}%" if per_game_pcts.get('rpg') is not None else "")}</div></div>
          <div class="chip"><div class="k">APG</div><div class="v">{fmt(stats.apg)}</div><div class="p">{(f"{per_game_pcts['apg']:.0f}%" if per_game_pcts.get('apg') is not None else "")}</div></div>
          <div class="chip"><div class="k">SPG</div><div class="v">{fmt(stats.spg)}</div><div class="p">{(f"{per_game_pcts['spg']:.0f}%" if per_game_pcts.get('spg') is not None else "")}</div></div>
          <div class="chip"><div class="k">BPG</div><div class="v">{fmt(stats.bpg)}</div><div class="p">{(f"{per_game_pcts['bpg']:.0f}%" if per_game_pcts.get('bpg') is not None else "")}</div></div>
          <div class="chip"><div class="k">FG%</div><div class="v">{fmt(stats.fg_pct)}</div><div class="p">{(f"{per_game_pcts['fg_pct']:.0f}%" if per_game_pcts.get('fg_pct') is not None else "")}</div></div>
          <div class="chip"><div class="k">3P%</div><div class="v">{fmt(stats.tp_pct)}</div><div class="p">{(f"{per_game_pcts['tp_pct']:.0f}%" if per_game_pcts.get('tp_pct') is not None else "")}</div></div>
          <div class="chip"><div class="k">FT%</div><div class="v">{fmt(stats.ft_pct)}</div><div class="p">{(f"{per_game_pcts['ft_pct']:.0f}%" if per_game_pcts.get('ft_pct') is not None else "")}</div></div>
        </div>
      </div>

      {bt_percentiles_html}

      <div class="shot-wrap">
        <div class="panel shot-panel shot-chart-col" style="margin-top:14px;">
          <h3>Shot Chart</h3>
          <div class="shot-meta">Attempts: {shot_att} | Made: {shot_makes} | FG%: {fmt(shot_pct)}%</div>
          {shot_svg(shots, season_shots, width=355, height=250)}
        </div>
        <div class="right-wrap">
          <div class="right-top">
            {self_creation_html}
            {player_comparisons_html}
          </div>
          <div class="right-bottom">
            {shot_diet_html}
          </div>
        </div>
      </div>
      {advanced_html}
    </div>
  </div>
</body>
</html>
"""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html_doc, encoding="utf-8")

def choose_player(
    players: list[PlayerGameStats],
    player: str,
    team: str | None,
    season: str | None,
) -> PlayerGameStats:
    np = norm_text(player)
    nt = norm_text(team or "")
    ns = norm_text(season or "")

    candidates = [p for p in players if norm_text(p.player) == np]
    if nt:
        candidates = [
            p
            for p in candidates
            if nt in norm_text(p.team) or norm_text(p.team) in nt
        ]
    if ns:
        candidates = [p for p in candidates if norm_text(p.season) == ns]
    if not candidates:
        raise RuntimeError("No player match found with the supplied player/team/season filters.")
    if len(candidates) > 1:
        candidates = sorted(candidates, key=lambda x: (x.team, x.season))
    return candidates[0]


def build_per_game_percentiles(
    players: list[PlayerGameStats],
    target: PlayerGameStats,
    min_games: int,
) -> dict[str, float | None]:
    cohort = [p for p in players if norm_text(p.season) == norm_text(target.season) and p.games >= min_games]
    if not cohort:
        cohort = [p for p in players if p.games >= min_games]
    metrics = {
        "ppg": [p.ppg for p in cohort],
        "rpg": [p.rpg for p in cohort],
        "apg": [p.apg for p in cohort],
        "spg": [p.spg for p in cohort],
        "bpg": [p.bpg for p in cohort],
        "fg_pct": [p.fg_pct for p in cohort],
        "tp_pct": [p.tp_pct for p in cohort],
        "ft_pct": [p.ft_pct for p in cohort],
    }
    return {
        "ppg": percentile_safe(target.ppg, metrics["ppg"]),
        "rpg": percentile_safe(target.rpg, metrics["rpg"]),
        "apg": percentile_safe(target.apg, metrics["apg"]),
        "spg": percentile_safe(target.spg, metrics["spg"]),
        "bpg": percentile_safe(target.bpg, metrics["bpg"]),
        "fg_pct": percentile_safe(target.fg_pct, metrics["fg_pct"]),
        "tp_pct": percentile_safe(target.tp_pct, metrics["tp_pct"]),
        "ft_pct": percentile_safe(target.ft_pct, metrics["ft_pct"]),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Build a college basketball player card HTML.")
    ap.add_argument("--plays-csv", required=True, help="Path to CBBD plays CSV (regular/fullseason).")
    ap.add_argument("--player", required=True, help="Player name.")
    ap.add_argument("--team", default="", help="Optional team filter.")
    ap.add_argument("--season", default="", help="Optional season filter (e.g. 2025).")
    ap.add_argument("--bio-csv", default="", help="Optional CSV for bio/class/height/age/position.")
    ap.add_argument("--bt-csv", default="", help="Optional Bart Torvik advanced stats CSV.")
    ap.add_argument("--lebron-csv", default="", help="Optional CSV with LEBRON/O-LEBRON/D-LEBRON/BPM.")
    ap.add_argument("--rimfluence-csv", default="", help="Optional Rimfluence model output CSV.")
    ap.add_argument("--style-csv", default="", help="Optional style/playtype CSV (e.g., master sheet).")
    ap.add_argument("--advgames-csv", default="", help="Optional per-game labeled advgames CSV for BPM trend.")
    ap.add_argument("--pbp-metrics-csv", default="", help="Optional player metrics CSV derived from ncaahoopR pbp logs.")
    ap.add_argument("--bt-playerstat-json", default="", help="Optional Bart playerstat JSON file path or URL.")
    ap.add_argument(
        "--bt-playerstat-url-template",
        default="https://barttorvik.com/{year}_pbp_playerstat_array.json",
        help="Bart playerstat URL template; {year} is replaced with target season year.",
    )
    ap.add_argument("--out-html", required=True, help="Output HTML path.")
    ap.add_argument("--min-games", type=int, default=5, help="Min games for percentile cohort.")
    args = ap.parse_args()

    _, plays_rows = read_csv_rows(Path(args.plays_csv))
    if not plays_rows:
        raise RuntimeError("Plays CSV had no rows.")

    stats_map, _ = build_player_stats(plays_rows, season_hint=args.season or "")
    players = list(stats_map.values())
    if not players:
        raise RuntimeError("Could not build player stats from plays data.")

    target = choose_player(players, args.player, args.team or None, args.season or None)

    bio_lookup: dict[tuple[str, str, str], dict[str, str]] = {}
    if args.bio_csv:
        bio_lookup = load_bio_lookup(Path(args.bio_csv))
    bio = bio_lookup.get(
        key_player_team_season(target.player, target.team, target.season),
        {"class": "", "height": "", "age": "", "position": "", "conference": "", "dob": ""},
    )

    # Optional advanced sources.
    bt_rows: list[dict[str, str]] = []
    lebron_rows: list[dict[str, str]] = []
    rim_rows: list[dict[str, str]] = []
    style_rows: list[dict[str, str]] = []
    adv_rows: list[dict[str, str]] = []
    pbp_rows: list[dict[str, str]] = []
    bt_playerstat_rows: list[dict[str, Any]] = []

    if args.bt_csv:
        _, bt_rows = read_csv_rows(Path(args.bt_csv))
    if args.lebron_csv:
        _, lebron_rows = read_csv_rows(Path(args.lebron_csv))
    if args.rimfluence_csv:
        _, rim_rows = read_csv_rows(Path(args.rimfluence_csv))
    if args.style_csv:
        _, style_rows = read_csv_rows(Path(args.style_csv))
    if args.advgames_csv:
        _, adv_rows = read_csv_rows(Path(args.advgames_csv))
    if args.pbp_metrics_csv:
        _, pbp_rows = read_csv_rows(Path(args.pbp_metrics_csv))

    if args.bt_playerstat_json:
        bt_playerstat_rows = load_bt_playerstat_rows_from_source(args.bt_playerstat_json)
    else:
        bt_ps_url = args.bt_playerstat_url_template.format(year=norm_season(target.season))
        try:
            bt_playerstat_rows = load_bt_playerstat_rows_from_source(bt_ps_url)
        except Exception:
            bt_playerstat_rows = []

    bt_percentiles_html = build_bt_percentile_html(target, bt_rows, adv_rows, pbp_rows)
    grade_boxes_html = build_grade_boxes_html(target, bt_rows)
    self_creation_html = build_self_creation_html(target, bt_rows, bt_playerstat_rows, pbp_rows)
    shot_diet_html = build_shot_diet_html(target, bt_rows)
    player_comparisons_html = build_player_comparisons_html(target, bt_rows, bio_lookup, top_n=5)
    advanced_html = build_advanced_html(target, lebron_rows, rim_rows, style_rows)

    shots = collect_shots(plays_rows, target.player, target.team, target.season, season_hint=args.season or "")
    season_shots: list[dict[str, Any]] = []
    for row in plays_rows:
        if norm_text(_season_from_row(row, args.season or "")) != norm_text(target.season):
            continue
        x, y = _shot_loc_from_row(row)
        rng = _shot_range_from_row(row)
        if x is None or y is None or rng not in {"rim", "jumper", "three_pointer"}:
            continue
        season_shots.append(
            {
                "x": x,
                "y": y,
                "made": _shot_made_from_row(row),
                "range": rng,
            }
        )
    per_game_pcts = build_per_game_percentiles(players, target, args.min_games)
    render_card(
        target,
        bio,
        shots,
        season_shots,
        per_game_pcts,
        grade_boxes_html,
        bt_percentiles_html,
        self_creation_html,
        shot_diet_html,
        player_comparisons_html,
        advanced_html,
        Path(args.out_html),
    )

    print(f"Wrote card: {args.out_html}")
    print(f"Player: {target.player} | Team: {target.team} | Season: {target.season}")
    if bt_rows:
        bt_cohort = bt_cohort_for_year(bt_rows, target.season)
        print(f"Bart Torvik cohort size: {len(bt_cohort)}")
    print(f"Shot points plotted: {len(shots)}")
    if shots:
        xs = [float(s["x"]) for s in shots]
        ys = [float(s["y"]) for s in shots]
        print(f"Shot x range: {min(xs):.1f}..{max(xs):.1f} | y range: {min(ys):.1f}..{max(ys):.1f}")


if __name__ == "__main__":
    main()
