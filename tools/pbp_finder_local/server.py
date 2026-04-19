#!/usr/bin/env python3
import json
import os
import sqlite3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlencode, urlparse

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "pbp_2026_men.sqlite")
INDEX_PATH = os.path.join(BASE_DIR, "index.html")

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


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def json_response(handler: BaseHTTPRequestHandler, payload, status=200):
    body = json.dumps(payload).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def parse_bool(v: str):
    t = (v or "").strip().lower()
    if t == "yes":
        return 1
    if t == "no":
        return 0
    return None


def build_where(params):
    where = ["1=1"]
    args = []

    player = (params.get("player", [""])[0] or "").strip()
    event = (params.get("event", [""])[0] or "").strip()
    shot_location = (params.get("shot_location", [""])[0] or "").strip()
    assisted = parse_bool((params.get("assisted", [""])[0] or "").strip())
    assisted_by = (params.get("assisted_by", [""])[0] or "").strip()

    if player:
        where.append("player = ?")
        args.append(player)
    if event and event in EVENTS:
        where.append("event = ?")
        args.append(event)
    if shot_location:
        where.append("shot_location = ?")
        args.append(shot_location)

    if event in {"Field goal attempt", "Field goal miss", "Field goal make"}:
        if assisted is not None:
            where.append("assisted = ?")
            args.append(assisted)
        if assisted == 1 and assisted_by:
            where.append("assisted_by = ?")
            args.append(assisted_by)
    elif event == "Assist":
        if assisted_by:
            where.append("player = ?")
            args.append(assisted_by)

    return where, args


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        params = parse_qs(parsed.query)

        if path == "/":
            self.serve_index()
            return

        if path == "/api/health":
            json_response(self, {"ok": os.path.exists(DB_PATH), "db": DB_PATH})
            return

        if path == "/api/events":
            json_response(self, {"events": EVENTS})
            return

        if path == "/api/players":
            self.api_players(params)
            return

        if path == "/api/assist-options":
            self.api_assist_options(params)
            return

        if path == "/api/search":
            self.api_search(params)
            return

        self.send_response(404)
        self.end_headers()

    def serve_index(self):
        if not os.path.exists(INDEX_PATH):
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b"Missing index.html")
            return
        with open(INDEX_PATH, "rb") as f:
            body = f.read()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def api_players(self, params):
        q = (params.get("q", [""])[0] or "").strip()
        event = (params.get("event", [""])[0] or "").strip()

        sql = "SELECT player, COUNT(*) AS n FROM pbp_events WHERE player <> ''"
        args = []
        if event in EVENTS:
            sql += " AND event = ?"
            args.append(event)
        if q:
            sql += " AND player LIKE ?"
            args.append(f"%{q}%")
        sql += " GROUP BY player ORDER BY n DESC, player ASC LIMIT 100"

        with get_conn() as conn:
            rows = conn.execute(sql, args).fetchall()
        json_response(self, {"players": [r["player"] for r in rows]})

    def api_assist_options(self, params):
        player = (params.get("player", [""])[0] or "").strip()
        event = (params.get("event", [""])[0] or "").strip()
        assisted = parse_bool((params.get("assisted", [""])[0] or "").strip())

        if not player:
            json_response(self, {"assist_by": []})
            return

        with get_conn() as conn:
            if event in {"Field goal attempt", "Field goal miss", "Field goal make"}:
                # Anchor assist suggestions to player's primary team in this dataset.
                primary_team_row = conn.execute(
                    """
                    SELECT team, COUNT(*) AS n
                    FROM pbp_events
                    WHERE event IN ('Field goal attempt','Field goal miss','Field goal make')
                      AND player = ?
                      AND team <> ''
                    GROUP BY team
                    ORDER BY n DESC, team ASC
                    LIMIT 1
                    """,
                    [player],
                ).fetchone()
                primary_team = primary_team_row["team"] if primary_team_row else ""

                sql = (
                    "SELECT assisted_by, COUNT(*) AS n "
                    "FROM pbp_events "
                    "WHERE event IN ('Field goal attempt','Field goal miss','Field goal make') "
                    "AND player = ? AND assisted_by <> ''"
                )
                args = [player]
                if primary_team:
                    sql += " AND team = ?"
                    args.append(primary_team)
                if assisted is not None:
                    sql += " AND assisted = ?"
                    args.append(assisted)
                sql += " GROUP BY assisted_by ORDER BY n DESC, assisted_by LIMIT 100"
                rows = conn.execute(sql, args).fetchall()
                names = [r["assisted_by"] for r in rows]
            elif event == "Assist":
                sql = (
                    "SELECT DISTINCT player FROM pbp_events WHERE event = 'Assist' AND player LIKE ? ORDER BY player LIMIT 100"
                )
                rows = conn.execute(sql, ["%%"]).fetchall()
                names = [r["player"] for r in rows]
            else:
                names = []

        json_response(self, {"assist_by": names})

    def api_search(self, params):
        page = max(1, int((params.get("page", ["1"])[0] or "1")))
        page_size = min(500, max(25, int((params.get("page_size", ["100"])[0] or "100"))))
        offset = (page - 1) * page_size

        where, args = build_where(params)
        where_sql = " AND ".join(where)

        count_sql = f"SELECT COUNT(*) AS c FROM pbp_events WHERE {where_sql}"
        data_sql = (
            "SELECT game_date, team, opponent, home_score, away_score, clock, period, player, event, shot_location, "
            "assisted, assisted_by, recipient, play_text "
            f"FROM pbp_events WHERE {where_sql} "
            "ORDER BY game_date DESC, game_id DESC, id DESC LIMIT ? OFFSET ?"
        )

        with get_conn() as conn:
            total = conn.execute(count_sql, args).fetchone()["c"]
            rows = conn.execute(data_sql, [*args, page_size, offset]).fetchall()

        payload_rows = []
        for r in rows:
            payload_rows.append(
                {
                    "date": r["game_date"],
                    "matchup": f"{r['team']} vs {r['opponent']}" if r["team"] and r["opponent"] else "",
                    "score": f"{r['home_score']}-{r['away_score']}" if r["home_score"] is not None and r["away_score"] is not None else "",
                    "clock": r["clock"] or "",
                    "quarter": f"P{r['period']}" if (r["period"] or "") != "" else "",
                    "team": r["team"] or "",
                    "player": r["player"] or "",
                    "event": r["event"] or "",
                    "shot_location": r["shot_location"] or "",
                    "assisted": "Yes" if r["assisted"] == 1 else ("No" if r["assisted"] == 0 else ""),
                    "assisted_by": r["assisted_by"] or "",
                    "recipient": r["recipient"] or "",
                    "play_text": r["play_text"] or "",
                }
            )

        json_response(
            self,
            {
                "total": total,
                "page": page,
                "page_size": page_size,
                "rows": payload_rows,
            },
        )


def main():
    if not os.path.exists(DB_PATH):
        raise SystemExit(
            f"Missing DB at {DB_PATH}. Run: python3 {os.path.join(BASE_DIR, 'build_pbp_db.py')}"
        )

    host = "127.0.0.1"
    port = 8765
    print(f"Play-by-Play Finder running at http://{host}:{port}")
    httpd = ThreadingHTTPServer((host, port), Handler)
    httpd.serve_forever()


if __name__ == "__main__":
    main()
