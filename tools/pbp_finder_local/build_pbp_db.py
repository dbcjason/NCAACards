#!/usr/bin/env python3
import argparse
import csv
import os
import sqlite3
import time
from glob import glob
from typing import Optional

EVENTS = [
    "Field goal attempt",
    "Field goal miss",
    "Field goal make",
    "Assist",
    "Offensive rebound",
    "Defensive rebound",
    "Steal",
    "Block",
    "Turnover",
]


def norm_bool(raw: str) -> Optional[int]:
    t = (raw or "").strip().lower()
    if t == "true":
        return 1
    if t == "false":
        return 0
    return None


def map_shot_location(raw: str) -> Optional[str]:
    t = (raw or "").strip().lower()
    if t == "rim":
        return "Rim"
    if t == "three_pointer":
        return "Three"
    if t in {"jumper", "mid_range", "non_rim"}:
        return "Mid Range"
    return None


def clean_name(raw: str) -> str:
    return (raw or "").strip()


def to_int(raw: str) -> Optional[int]:
    try:
        return int(raw)
    except Exception:
        return None


def choose_actor(row: dict) -> str:
    return clean_name(row.get("participants[0].name", ""))


def choose_shooter(row: dict) -> str:
    shooter = clean_name(row.get("shotInfo.shooter.name", ""))
    if shooter:
        return shooter
    return choose_actor(row)


def build_rows_from_play(row: dict):
    out = []

    play_type = (row.get("playType") or "").strip()
    if not play_type:
        return out

    team = (row.get("team") or "").strip()
    opp = (row.get("opponent") or "").strip()
    game_id = (row.get("gameId") or "").strip()
    game_date = (row.get("gameStartDate") or "").strip()[:10]
    clock = (row.get("clock") or "").strip()
    period = (row.get("period") or "").strip()
    home_score = to_int((row.get("homeScore") or "").strip())
    away_score = to_int((row.get("awayScore") or "").strip())
    play_text = (row.get("playText") or "").strip()

    base = {
        "raw_play_id": (row.get("id") or "").strip(),
        "game_id": game_id,
        "game_date": game_date,
        "team": team,
        "opponent": opp,
        "clock": clock,
        "period": period,
        "home_score": home_score,
        "away_score": away_score,
        "play_text": play_text,
    }

    shooting = ((row.get("shootingPlay") or "").strip().lower() == "true")
    shot_range_raw = (row.get("shotInfo.range") or "").strip()
    shot_location = map_shot_location(shot_range_raw)
    if play_type == "DunkShot":
        shot_location = "Dunk"

    # Field goal events (exclude free throws)
    if shooting and shot_range_raw.lower() != "free_throw":
        shooter = choose_shooter(row)
        if shooter:
            made = norm_bool(row.get("shotInfo.made", ""))
            assisted = norm_bool(row.get("shotInfo.assisted", ""))
            assisted_by = clean_name(row.get("shotInfo.assistedBy.name", ""))
            fg_base = {
                **base,
                "player": shooter,
                "shot_location": shot_location,
                "assisted": assisted,
                "assisted_by": assisted_by if assisted == 1 else "",
                "recipient": "",
            }
            out.append({**fg_base, "event": "Field goal attempt"})
            if made == 1:
                out.append({**fg_base, "event": "Field goal make"})
                if assisted == 1 and assisted_by:
                    out.append(
                        {
                            **base,
                            "event": "Assist",
                            "player": assisted_by,
                            "shot_location": shot_location,
                            "assisted": 1,
                            "assisted_by": assisted_by,
                            "recipient": shooter,
                        }
                    )
            elif made == 0:
                out.append({**fg_base, "event": "Field goal miss"})
        return out

    actor = choose_actor(row)
    if "Offensive Rebound" in play_type and actor:
        out.append(
            {
                **base,
                "event": "Offensive rebound",
                "player": actor,
                "shot_location": "",
                "assisted": None,
                "assisted_by": "",
                "recipient": "",
            }
        )
    elif "Defensive Rebound" in play_type and actor:
        out.append(
            {
                **base,
                "event": "Defensive rebound",
                "player": actor,
                "shot_location": "",
                "assisted": None,
                "assisted_by": "",
                "recipient": "",
            }
        )
    elif play_type == "Steal" and actor:
        out.append(
            {
                **base,
                "event": "Steal",
                "player": actor,
                "shot_location": "",
                "assisted": None,
                "assisted_by": "",
                "recipient": "",
            }
        )
    elif play_type in {"Block Shot", "Blocked Shot", "Block"} and actor:
        out.append(
            {
                **base,
                "event": "Block",
                "player": actor,
                "shot_location": "",
                "assisted": None,
                "assisted_by": "",
                "recipient": "",
            }
        )
    elif "Turnover" in play_type and actor:
        out.append(
            {
                **base,
                "event": "Turnover",
                "player": actor,
                "shot_location": "",
                "assisted": None,
                "assisted_by": "",
                "recipient": "",
            }
        )

    return out


def init_db(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.executescript(
        """
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        DROP TABLE IF EXISTS pbp_events;
        CREATE TABLE pbp_events (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          raw_play_id TEXT,
          game_id TEXT,
          game_date TEXT,
          team TEXT,
          opponent TEXT,
          event TEXT,
          player TEXT,
          shot_location TEXT,
          assisted INTEGER,
          assisted_by TEXT,
          recipient TEXT,
          home_score INTEGER,
          away_score INTEGER,
          clock TEXT,
          period TEXT,
          play_text TEXT
        );
        """
    )
    conn.commit()


def finalize_db(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.executescript(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_pbp_dedupe_natural
          ON pbp_events(
            game_id,
            period,
            clock,
            team,
            player,
            event,
            IFNULL(shot_location,''),
            IFNULL(assisted_by,''),
            IFNULL(recipient,''),
            IFNULL(play_text,'')
          );
        CREATE INDEX IF NOT EXISTS idx_pbp_event ON pbp_events(event);
        CREATE INDEX IF NOT EXISTS idx_pbp_player ON pbp_events(player);
        CREATE INDEX IF NOT EXISTS idx_pbp_assisted_by ON pbp_events(assisted_by);
        CREATE INDEX IF NOT EXISTS idx_pbp_shot_loc ON pbp_events(shot_location);
        CREATE INDEX IF NOT EXISTS idx_pbp_date ON pbp_events(game_date);
        CREATE INDEX IF NOT EXISTS idx_pbp_event_player ON pbp_events(event, player);
        CREATE INDEX IF NOT EXISTS idx_pbp_event_assist ON pbp_events(event, assisted);
        """
    )
    conn.commit()


def parse_args():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(os.path.dirname(script_dir))
    ap = argparse.ArgumentParser(description="Build local play-by-play finder SQLite DB for 2026 men")
    ap.add_argument(
        "--source",
        default="",
        help="Optional single source CSV path (if omitted, script auto-discovers all shards)",
    )
    ap.add_argument(
        "--source-dir",
        default=os.path.join(repo_root, "cbbd_seasons", "2025-2026", "tables"),
        help="Directory containing play-by-play CSV shards",
    )
    ap.add_argument(
        "--out-db",
        default=os.path.join(script_dir, "pbp_2026_men.sqlite"),
        help="Output SQLite path",
    )
    return ap.parse_args()

def discover_sources(source_dir: str) -> list[str]:
    source_dir = os.path.abspath(source_dir)
    if not os.path.isdir(source_dir):
        return []

    def nonzero(paths: list[str]) -> list[str]:
        return [p for p in sorted(paths) if os.path.isfile(p) and os.path.getsize(p) > 0]

    # Prefer shard files over pre-combined files to avoid partial/old combined snapshots.
    regular_files = nonzero(glob(os.path.join(source_dir, "plays_regular_chunk*.csv")))

    postseason_chunk = nonzero(glob(os.path.join(source_dir, "plays_postseason_chunk*.csv")))
    postseason_unknown = nonzero(glob(os.path.join(source_dir, "plays_postseason_unknown_game_map*.csv")))

    sources: list[str] = []

    if regular_files:
        sources.extend(regular_files)
    else:
        sources.extend(nonzero(glob(os.path.join(source_dir, "plays_regular*.csv"))))

    # Postseason chunk files are sometimes zero-byte placeholders. Prefer unknown-game-map shards if present.
    if postseason_unknown:
        sources.extend(postseason_unknown)
    elif postseason_chunk:
        sources.extend(postseason_chunk)
    else:
        sources.extend(nonzero(glob(os.path.join(source_dir, "plays_postseason*.csv"))))

    # Remove duplicates while preserving order.
    seen = set()
    uniq: list[str] = []
    for s in sources:
        if s in seen:
            continue
        seen.add(s)
        uniq.append(s)
    return uniq


def main():
    args = parse_args()
    sources: list[str] = []
    if args.source:
        if not os.path.exists(args.source):
            raise SystemExit(f"Source file not found: {args.source}")
        sources = [os.path.abspath(args.source)]
    else:
        sources = discover_sources(args.source_dir)
        if not sources:
            raise SystemExit(f"No source shards found in: {args.source_dir}")

    started = time.time()
    conn = sqlite3.connect(args.out_db)
    init_db(conn)

    insert_sql = (
        "INSERT OR IGNORE INTO pbp_events (raw_play_id, game_id, game_date, team, opponent, event, player, shot_location, assisted, assisted_by, recipient, home_score, away_score, clock, period, play_text) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )

    buf = []
    read_rows = 0
    written_rows = 0
    source_rows = 0

    for src in sources:
        source_rows += 1
        print(f"[source {source_rows}/{len(sources)}] {src}")
        with open(src, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                read_rows += 1
                events = build_rows_from_play(row)
                for e in events:
                    if not e.get("player"):
                        continue
                    if e.get("event") not in EVENTS:
                        continue
                    buf.append(
                        (
                            e.get("raw_play_id", ""),
                            e.get("game_id", ""),
                            e.get("game_date", ""),
                            e.get("team", ""),
                            e.get("opponent", ""),
                            e.get("event", ""),
                            e.get("player", ""),
                            e.get("shot_location", "") or "",
                            e.get("assisted", None),
                            e.get("assisted_by", ""),
                            e.get("recipient", ""),
                            e.get("home_score", None),
                            e.get("away_score", None),
                            e.get("clock", ""),
                            e.get("period", ""),
                            e.get("play_text", ""),
                        )
                    )
                if len(buf) >= 10000:
                    conn.executemany(insert_sql, buf)
                    conn.commit()
                    written_rows += len(buf)
                    buf.clear()
                    if read_rows % 200000 < 10000:
                        print(f"processed={read_rows:,} inserted={written_rows:,}")

    if buf:
        conn.executemany(insert_sql, buf)
        conn.commit()
        written_rows += len(buf)

    finalize_db(conn)
    conn.close()

    elapsed = time.time() - started
    print(
        f"done sources={len(sources)} source_rows={read_rows:,} inserted={written_rows:,} "
        f"elapsed_sec={elapsed:.1f} db={args.out_db}"
    )


if __name__ == "__main__":
    main()
