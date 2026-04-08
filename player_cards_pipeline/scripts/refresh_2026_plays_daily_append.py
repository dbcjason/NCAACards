#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import difflib
import json
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo


BASE_URL = "https://api.collegebasketballdata.com"
DEFAULT_API_KEY = "SXMeiEWTsy0KNablQhQVQBL7LhVcVnubACJUUAoeT/xWHKo+kV0fAxjAjaHEc6Ph"


def log(msg: str) -> None:
    print(msg, flush=True)


def to_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ("data", "results", "items", "rows"):
            value = payload.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
        return [payload]
    return []


def norm(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    return re.sub(r"[^a-z0-9]+", "", text)


def alias_variants(name: str) -> list[str]:
    out = {name.strip(), name.strip().replace(".", "")}
    s = name.strip()
    rules = [
        (" St.", " State"),
        ("Cal St.", "Cal State"),
        ("FIU", "Florida International"),
        ("LIU", "LIU Brooklyn"),
        ("Albany", "UAlbany"),
        ("Appalachian St.", "App State"),
        ("Illinois Chicago", "UIC"),
        ("IU Indy", "IU Indianapolis"),
        ("Loyola MD", "Loyola Maryland"),
        ("Miami FL", "Miami"),
        ("Mississippi", "Ole Miss"),
        ("Nebraska Omaha", "Omaha"),
        ("Penn", "Pennsylvania"),
        ("Saint Francis", "St. Francis (PA)"),
        ("Southeastern Louisiana", "SE Louisiana"),
        ("Tennessee Martin", "UT Martin"),
        ("USC Upstate", "South Carolina Upstate"),
        ("Seattle", "Seattle U"),
        ("Queens", "Queens University"),
        ("San Jose St.", "San Jose State"),
        ("Louisiana Monroe", "UL Monroe"),
        ("Sam Houston St.", "Sam Houston"),
        ("Nicholls St.", "Nicholls"),
        ("McNeese St.", "McNeese"),
        ("Grambling St.", "Grambling"),
        ("Texas A&M Corpus Chris", "Texas A&M-Corpus Christi"),
        ("Long Beach St.", "Long Beach State"),
        ("Cal Baptist", "California Baptist"),
        ("Connecticut", "UConn"),
        ("UMKC", "Kansas City"),
    ]
    for a, b in rules:
        if a in s:
            out.add(s.replace(a, b))
    if s.startswith("Saint "):
        out.add(s.replace("Saint ", "St. "))
    if s.startswith("St. "):
        out.add(s.replace("St. ", "Saint "))
    return list(out)


class Client:
    def __init__(self, api_key: str, sleep_sec: float, timeout_sec: int, max_requests: int) -> None:
        self.api_key = api_key
        self.sleep_sec = sleep_sec
        self.timeout_sec = timeout_sec
        self.max_requests = max_requests
        self.request_count = 0

    def get(self, path: str, params: dict[str, Any]) -> tuple[int, Any]:
        if self.request_count >= self.max_requests:
            raise RuntimeError(f"Request budget exceeded (max_requests={self.max_requests}).")
        query = urlencode({k: v for k, v in params.items() if v is not None and v != ""})
        url = f"{BASE_URL}{path}" + (f"?{query}" if query else "")
        req = Request(
            url,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Accept": "application/json",
                "User-Agent": "cbbd-plays-daily-append/1.0",
            },
            method="GET",
        )
        status = 0
        body: Any = None
        try:
            with urlopen(req, timeout=self.timeout_sec) as resp:
                status = int(resp.status)
                payload = resp.read().decode("utf-8", errors="replace")
                body = json.loads(payload) if payload else None
        except HTTPError as exc:
            status = int(exc.code)
            payload = exc.read().decode("utf-8", errors="replace")
            try:
                body = json.loads(payload) if payload else None
            except Exception:
                body = {"error_text": payload[:1000]}
        except URLError as exc:
            body = {"error": str(exc)}
        except Exception as exc:  # noqa: BLE001
            body = {"error": str(exc)}
        finally:
            self.request_count += 1
            if self.sleep_sec > 0:
                time.sleep(self.sleep_sec)
        return status, body


def season_label(year: int) -> str:
    return f"{year - 1}-{year}"


def pick_game_id(game: dict[str, Any]) -> int | None:
    for key in ("id", "gameId", "game_id"):
        value = game.get(key)
        if value is None:
            continue
        try:
            return int(value)
        except Exception:
            continue
    return None


def pick_team_candidates(game: dict[str, Any]) -> list[str]:
    out: list[str] = []
    fields = (
        "homeTeam",
        "home_team",
        "homeSchool",
        "home_school",
        "home",
        "awayTeam",
        "away_team",
        "awaySchool",
        "away_school",
        "away",
        "team",
        "opponent",
    )
    for key in fields:
        value = game.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            out.append(text)
    return out


def infer_season_type(game: dict[str, Any]) -> str:
    raw = str(game.get("seasonType") or game.get("season_type") or "").strip().lower()
    if "post" in raw or "tour" in raw:
        return "postseason"
    return "regular"


def discover_teams(client: Client, year: int) -> list[dict[str, Any]]:
    status, payload = client.get("/teams", {"season": year})
    if status != 200:
        return []
    out: list[dict[str, Any]] = []
    for row in to_records(payload):
        out.append(
            {
                "team_id": row.get("id"),
                "team_name": row.get("school") or row.get("team") or row.get("name"),
            }
        )
    return out


def map_teams(raw_team_names: set[str], discovered: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    by_norm: dict[str, dict[str, Any]] = {}
    for team in discovered:
        key = norm(team.get("team_name"))
        if key and key not in by_norm:
            by_norm[key] = team
    norm_keys = list(by_norm.keys())
    matched: list[dict[str, Any]] = []
    unmatched: list[str] = []
    for raw_name in sorted(raw_team_names):
        team = None
        for variant in alias_variants(raw_name):
            team = by_norm.get(norm(variant))
            if team is not None:
                break
        if team is None:
            close = difflib.get_close_matches(norm(raw_name), norm_keys, n=1, cutoff=0.86)
            if close:
                team = by_norm[close[0]]
        if team is None:
            unmatched.append(raw_name)
        else:
            matched.append(team)
    deduped: list[dict[str, Any]] = []
    seen = set()
    for team in matched:
        key = str(team.get("team_id") or team.get("team_name"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(team)
    return deduped, unmatched


def existing_headers(path: Path) -> list[str]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open(newline="", encoding="utf-8", errors="replace") as handle:
        reader = csv.reader(handle)
        hdr = next(reader, [])
        return [h for h in hdr if h is not None]


def append_csv(path: Path, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    hdr = existing_headers(path)
    if not hdr:
        hdr = sorted({k for row in rows for k in row.keys()})
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=hdr)
            writer.writeheader()
            for row in rows:
                writer.writerow({k: row.get(k) for k in hdr})
        return len(rows)
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=hdr)
        for row in rows:
            writer.writerow({k: row.get(k) for k in hdr})
    return len(rows)


def parse_args() -> argparse.Namespace:
    now_et = datetime.now(ZoneInfo("America/New_York"))
    default_day = (now_et - timedelta(days=1)).date().isoformat()
    ap = argparse.ArgumentParser(description="Append yesterday's CBBD plays into 2026 output tables.")
    ap.add_argument("--project-root", default=".")
    ap.add_argument("--season", type=int, default=2026)
    ap.add_argument("--date", default=default_day, help="YYYY-MM-DD target date (ET).")
    ap.add_argument("--force", action="store_true", help="Re-ingest date even if already recorded in manifest.")
    ap.add_argument("--sleep-sec", type=float, default=0.05)
    ap.add_argument("--timeout-sec", type=int, default=60)
    ap.add_argument("--max-requests", type=int, default=2000)
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    project_root = Path(args.project_root).resolve()
    out = project_root / "cbbd_seasons" / season_label(args.season)
    tables = out / "tables"
    manifest_dir = out / "manifest"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / "plays_daily_ingest.json"
    api_key = os.getenv("CBBD_API_KEY", DEFAULT_API_KEY).strip()
    if not api_key:
        raise RuntimeError("Missing CBBD_API_KEY")

    ingest_key = f"{args.season}:{args.date}"
    manifest = {"ingested_dates": [], "history": []}
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            manifest = {"ingested_dates": [], "history": []}
    done = set(manifest.get("ingested_dates", []) or [])
    if ingest_key in done and not args.force:
        log(f"[plays-daily] already ingested {ingest_key}; skipping (use --force to rerun)")
        return

    client = Client(
        api_key=api_key,
        sleep_sec=args.sleep_sec,
        timeout_sec=args.timeout_sec,
        max_requests=args.max_requests,
    )

    status_games, payload_games = client.get(
        "/games",
        {"season": args.season, "startDateRange": args.date, "endDateRange": args.date},
    )
    games = to_records(payload_games) if status_games == 200 else []
    if not games:
        log(f"[plays-daily] no games found for {args.date}")
        return
    log(f"[plays-daily] games={len(games)} date={args.date}")

    game_type_by_id: dict[int, str] = {}
    raw_teams: set[str] = set()
    for game in games:
        gid = pick_game_id(game)
        if gid is None:
            continue
        game_type_by_id[gid] = infer_season_type(game)
        for team_name in pick_team_candidates(game):
            raw_teams.add(team_name)
    if not game_type_by_id:
        log("[plays-daily] no game IDs present; exiting")
        return

    discovered = discover_teams(client, args.season)
    matched_teams, unmatched = map_teams(raw_teams, discovered)
    log(
        f"[plays-daily] raw_teams={len(raw_teams)} matched={len(matched_teams)} "
        f"unmatched={len(unmatched)} discovered={len(discovered)}"
    )
    if not matched_teams:
        log("[plays-daily] no matched teams; exiting")
        return

    regular_rows: list[dict[str, Any]] = []
    postseason_rows: list[dict[str, Any]] = []
    unknown_rows: list[dict[str, Any]] = []
    target_game_ids = set(game_type_by_id.keys())

    for index, team in enumerate(matched_teams, start=1):
        team_id = team.get("team_id")
        team_name = str(team.get("team_name") or "")
        status, payload = client.get("/plays/team", {"season": args.season, "team": team_id if team_id else team_name})
        plays = to_records(payload) if status == 200 else []
        for row in plays:
            gid = row.get("gameId")
            try:
                gid_int = int(gid) if gid is not None else None
            except Exception:
                gid_int = None
            if gid_int is None or gid_int not in target_game_ids:
                continue
            out_row = dict(row)
            out_row["__team_id"] = team_id
            out_row["__team_name"] = team_name
            out_row["__ingest_date"] = args.date
            st = game_type_by_id.get(gid_int, "unknown")
            if st == "regular":
                out_row["__season_type"] = "regular"
                regular_rows.append(out_row)
            elif st == "postseason":
                out_row["__season_type"] = "postseason"
                postseason_rows.append(out_row)
            else:
                out_row["__season_type"] = "unknown"
                unknown_rows.append(out_row)
        if index == 1 or index % 25 == 0 or index == len(matched_teams):
            log(
                f"[plays-daily] team {index}/{len(matched_teams)} requests={client.request_count} "
                f"reg_rows={len(regular_rows)} post_rows={len(postseason_rows)}"
            )

    full_rows = [*regular_rows, *postseason_rows, *unknown_rows]
    wrote_regular = append_csv(tables / "plays_regular.csv", regular_rows)
    wrote_post = append_csv(tables / "plays_postseason.csv", postseason_rows)
    wrote_unknown = append_csv(tables / "plays_daily_unknown_game_map.csv", unknown_rows)
    wrote_full = append_csv(tables / "plays_fullseason.csv", full_rows)

    done.add(ingest_key)
    manifest["ingested_dates"] = sorted(done)
    history = manifest.get("history", []) or []
    history.append(
        {
            "ingest_key": ingest_key,
            "season": args.season,
            "date": args.date,
            "games": len(games),
            "target_game_ids": len(target_game_ids),
            "matched_teams": len(matched_teams),
            "unmatched_teams": unmatched,
            "rows_appended": {
                "plays_regular": wrote_regular,
                "plays_postseason": wrote_post,
                "plays_daily_unknown_game_map": wrote_unknown,
                "plays_fullseason": wrote_full,
            },
            "request_count": client.request_count,
            "finished_utc": datetime.utcnow().isoformat() + "Z",
        }
    )
    manifest["history"] = history[-60:]
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=True, indent=2), encoding="utf-8")
    log(
        f"[plays-daily] appended regular={wrote_regular} postseason={wrote_post} "
        f"unknown={wrote_unknown} full={wrote_full} requests={client.request_count}"
    )


if __name__ == "__main__":
    main()
