#!/usr/bin/env python3
from __future__ import annotations

import json
import time
import io
import zipfile
import base64
import re
import http.client
import os
import subprocess
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import urllib.response
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import csv

import streamlit as st
import pandas as pd
from scripts import roster_simulator as roster_sim


DEFAULT_WORKFLOW_FILE = "build_player_card.yml"
DEFAULT_REF = "main"
ROOT = Path(__file__).resolve().parent


def _detect_paths() -> tuple[Path, Path]:
    # Supports launching from either:
    # - repo root: /app/player_cards_pipeline/action_runner_app.py
    # - pipeline root only: /app/action_runner_app.py
    candidates = [ROOT, ROOT.parent, ROOT.parent.parent]
    for c in candidates:
        if (c / "cbb_player_cards_v1" / "build_player_card.py").exists() and (c / "player_cards_pipeline").exists():
            return c, c / "player_cards_pipeline"
    # Fallback to current layout assumptions.
    if (ROOT / "data").exists():
        return ROOT.parent, ROOT
    return ROOT, ROOT / "player_cards_pipeline"


PROJECT_ROOT, PIPELINE_ROOT = _detect_paths()


def _cfg(name: str, default: str = "") -> str:
    # Railway/other hosts usually provide env vars; local Streamlit may use secrets.toml.
    env_v = str(os.getenv(name, "")).strip()
    if env_v:
        return env_v
    try:
        return str(st.secrets.get(name, default)).strip()
    except Exception:
        return str(default).strip()


@st.cache_data(show_spinner=False)
def _load_settings() -> dict[str, Any]:
    p = PIPELINE_ROOT / "config" / "settings.json"
    return json.loads(p.read_text(encoding="utf-8"))


def _resolve_pipeline_path(rel_or_abs: str) -> Path:
    p = Path(rel_or_abs)
    if p.is_absolute():
        return p
    return (PIPELINE_ROOT / rel_or_abs).resolve()


def _build_card_html_local(
    *,
    year: str,
    player: str,
    team: str,
    transfer_up: bool,
    destination_conference: str,
) -> tuple[bool, str, str]:
    settings = _load_settings()
    script = (PROJECT_ROOT / "cbb_player_cards_v1" / "build_player_card.py").resolve()
    if not script.exists():
        return False, "", f"Missing builder script: {script}"

    bt_csv = _resolve_pipeline_path(settings.get("bt_advstats_csv", ""))
    if not bt_csv.exists():
        return False, "", f"Missing BT CSV: {bt_csv}"

    with tempfile.NamedTemporaryFile(prefix=f"card_{slugify(player)}_{year}_", suffix=".html", delete=False) as tf:
        out_html = Path(tf.name)

    cmd = [
        "python3",
        str(script),
        "--player",
        str(player),
        "--season",
        str(year),
        "--bt-csv",
        str(bt_csv),
        "--bt-playerstat-url-template",
        str(settings.get("bt_playerstat_url_template", "https://barttorvik.com/{year}_pbp_playerstat_array.json")),
        "--out-html",
        str(out_html),
    ]
    if team:
        cmd += ["--team", team]

    bio_csv = str(settings.get("bio_csv", "")).strip()
    if bio_csv:
        bp = _resolve_pipeline_path(bio_csv)
        if bp.exists():
            cmd += ["--bio-csv", str(bp)]

    adv_rel = (settings.get("advgames_csv_by_year", {}) or {}).get(str(year), "")
    if adv_rel:
        ap = _resolve_pipeline_path(adv_rel)
        if ap.exists():
            cmd += ["--advgames-csv", str(ap)]

    pbp_rel = (settings.get("pbp_metrics_csv_by_year", {}) or {}).get(str(year), "")
    if pbp_rel:
        pp = _resolve_pipeline_path(pbp_rel)
        if pp.exists():
            cmd += ["--pbp-metrics-csv", str(pp)]

    if transfer_up:
        cmd += ["--transfer-up"]
        if destination_conference:
            cmd += ["--destination-conference", destination_conference]

    try:
        res = subprocess.run(cmd, capture_output=True, text=True, check=False)
    except Exception as e:
        return False, "", f"Local build failed to start: {e}"

    if res.returncode != 0:
        err = (res.stdout or "") + "\n" + (res.stderr or "")
        return False, "", err.strip() or f"Build failed with code {res.returncode}"

    try:
        html_text = out_html.read_text(encoding="utf-8")
        return True, html_text, ""
    except Exception as e:
        return False, "", f"Built but could not read output HTML: {e}"
    finally:
        try:
            out_html.unlink(missing_ok=True)
        except Exception:
            pass


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso(ts: str) -> datetime:
    # GitHub timestamps are like 2026-03-14T19:12:52Z
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts)


def _norm_year(raw: str) -> str:
    s = str(raw or "").strip()
    if not s:
        return ""
    if "/" in s:
        tail = s.split("/")[-1]
        if len(tail) == 2 and tail.isdigit():
            return f"20{tail}"
    if "-" in s:
        tail = s.split("-")[-1]
        if len(tail) == 2 and tail.isdigit():
            return f"20{tail}"
    return s


def load_team_player_index() -> dict[str, dict[str, list[str]]]:
    files = [
        PIPELINE_ROOT / "data" / "bt" / "bt_advstats_2010_2025.csv",
        PIPELINE_ROOT / "data" / "bt" / "bt_advstats_2019_2025.csv",
        PIPELINE_ROOT / "data" / "bt" / "bt_advstats_2026.csv",
        PIPELINE_ROOT / "data" / "bt" / "bt_advstats_2010_2026.csv",
    ]
    idx: dict[str, dict[str, set[str]]] = {}
    for p in files:
        if not p.exists():
            continue
        with p.open("r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                y = _norm_year(row.get("year", ""))
                if y not in {"2021", "2022", "2023", "2024", "2025", "2026"}:
                    continue
                team = (row.get("team") or "").strip()
                player = (row.get("player_name") or "").strip()
                if not team or not player:
                    continue
                idx.setdefault(y, {}).setdefault(team, set()).add(player)

    out: dict[str, dict[str, list[str]]] = {}
    for y, team_map in idx.items():
        out[y] = {t: sorted(list(players)) for t, players in team_map.items()}
    return out


def load_player_conference_index() -> tuple[dict[str, list[dict[str, str]]], list[str]]:
    files = [
        PIPELINE_ROOT / "data" / "bt" / "bt_advstats_2010_2025.csv",
        PIPELINE_ROOT / "data" / "bt" / "bt_advstats_2019_2025.csv",
        PIPELINE_ROOT / "data" / "bt" / "bt_advstats_2026.csv",
        PIPELINE_ROOT / "data" / "bt" / "bt_advstats_2010_2026.csv",
    ]
    by_year: dict[str, dict[tuple[str, str], dict[str, str]]] = {}
    confs: set[str] = set()
    allowed_years = {"2021", "2022", "2023", "2024", "2025", "2026"}

    for p in files:
        if not p.exists():
            continue
        with p.open("r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                y = _norm_year(row.get("year", ""))
                if y not in allowed_years:
                    continue
                player = (row.get("player_name") or "").strip()
                team = (row.get("team") or "").strip()
                conf = (row.get("conf") or "").strip()
                if not player or not team:
                    continue
                if conf:
                    confs.add(conf)
                ymap = by_year.setdefault(y, {})
                key = (player, team)
                if key not in ymap:
                    ymap[key] = {
                        "player": player,
                        "team": team,
                        "conf": conf,
                    }

    out_year: dict[str, list[dict[str, str]]] = {}
    for y, items in by_year.items():
        out_year[y] = sorted(
            items.values(),
            key=lambda r: ((r.get("team") or "").lower(), (r.get("player") or "").lower()),
        )
    return out_year, sorted(confs)


def github_api(
    *,
    method: str,
    owner: str,
    repo: str,
    token: str,
    path: str,
    query: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
) -> tuple[int, Any]:
    base = f"https://api.github.com/repos/{owner}/{repo}{path}"
    if query:
        base += "?" + urllib.parse.urlencode(query)

    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")

    def _do_req(auth_value: str) -> tuple[int, Any]:
        req = urllib.request.Request(
            base,
            data=data,
            method=method,
            headers={
                "Authorization": auth_value,
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "NCAACards-ActionRunner",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=45) as resp:
                status = resp.status
                try:
                    raw_bytes = resp.read()
                except http.client.IncompleteRead as e:
                    raw_bytes = e.partial or b""
                raw = raw_bytes.decode("utf-8", errors="replace")
                if not raw:
                    return status, None
                try:
                    return status, json.loads(raw)
                except Exception:
                    return status, raw
        except urllib.error.HTTPError as e:
            raw = ""
            if e.fp:
                try:
                    raw = e.read().decode("utf-8", errors="replace")
                except http.client.IncompleteRead as ie:
                    raw = (ie.partial or b"").decode("utf-8", errors="replace")
            try:
                body = json.loads(raw) if raw else {}
            except Exception:
                body = {"raw": raw}
            return int(e.code or 0), body

    code, body = _do_req(f"Bearer {token}")
    if code == 401:
        code, body = _do_req(f"token {token}")
    return code, body


def github_viewer_login(owner: str, repo: str, token: str) -> str:
    code, body = github_api(
        method="GET",
        owner=owner,
        repo=repo,
        token=token,
        path="",
    )
    # Repo endpoint returns owner info; use that as a stable actor fallback.
    if code == 200 and isinstance(body, dict):
        o = body.get("owner")
        if isinstance(o, dict):
            return str(o.get("login") or "").strip()
    return ""


def dispatch_build(
    owner: str,
    repo: str,
    token: str,
    workflow_file: str,
    ref: str,
    *,
    year: str,
    player: str,
    team: str,
    output_filename: str,
    commit_to_repo: bool,
    transfer_up: bool = False,
    destination_conference: str = "",
) -> tuple[bool, str]:
    payload = {
        "ref": ref,
        "inputs": {
            "year": str(year),
            "player": player,
            "team": team,
            "output_filename": output_filename,
            "commit_to_repo": bool(commit_to_repo),
            "transfer_up": bool(transfer_up),
            "destination_conference": destination_conference,
        },
    }
    code, body = github_api(
        method="POST",
        owner=owner,
        repo=repo,
        token=token,
        path=f"/actions/workflows/{workflow_file}/dispatches",
        payload=payload,
    )
    if code == 204:
        return True, "Workflow dispatched"
    return False, f"Dispatch failed ({code}): {body}"


def slugify(s: str) -> str:
    s2 = re.sub(r"[^A-Za-z0-9]+", "_", (s or "").strip()).strip("_").lower()
    return s2 or "player"


def get_repo_file_bytes(owner: str, repo: str, token: str, path: str, ref: str) -> tuple[bytes | None, str]:
    enc_path = urllib.parse.quote(path, safe="/")
    code, body = github_api(
        method="GET",
        owner=owner,
        repo=repo,
        token=token,
        path=f"/contents/{enc_path}",
        query={"ref": ref},
    )
    if code != 200 or not isinstance(body, dict):
        return None, f"http {code}"
    if str(body.get("type") or "") != "file":
        return None, "not a file"
    content = str(body.get("content") or "")
    encoding = str(body.get("encoding") or "")
    if not content:
        return None, "empty content"
    try:
        if encoding == "base64":
            return base64.b64decode(content.encode("ascii"), validate=False), ""
        return content.encode("utf-8"), ""
    except Exception:
        return None, "decode failed"


def list_dispatch_runs(
    owner: str,
    repo: str,
    token: str,
    workflow_file: str,
    *,
    per_page: int = 20,
) -> list[dict[str, Any]]:
    code, body = github_api(
        method="GET",
        owner=owner,
        repo=repo,
        token=token,
        path=f"/actions/workflows/{workflow_file}/runs",
        query={"event": "workflow_dispatch", "per_page": per_page},
    )
    if code != 200 or not isinstance(body, dict):
        return []
    runs = body.get("workflow_runs", [])
    if not isinstance(runs, list):
        return []
    return [r for r in runs if isinstance(r, dict)]


def find_run_id_for_dispatch(
    owner: str,
    repo: str,
    token: str,
    workflow_file: str,
    *,
    after_ts: str,
    actor_login: str = "",
    tries: int = 8,
    sleep_sec: float = 1.25,
) -> int | None:
    for _ in range(max(1, tries)):
        runs = list_dispatch_runs(owner, repo, token, workflow_file, per_page=30)
        for r in runs:
            created_at = str(r.get("created_at") or "")
            if created_at:
                try:
                    if _parse_iso(created_at) < _parse_iso(after_ts):
                        continue
                except Exception:
                    pass
            if actor_login:
                ta = r.get("triggering_actor")
                tal = str(ta.get("login") if isinstance(ta, dict) else "").strip().lower()
                if tal and tal != actor_login.strip().lower():
                    continue
            rid = r.get("id")
            if isinstance(rid, int):
                return rid
            try:
                return int(str(rid))
            except Exception:
                continue
        time.sleep(sleep_sec)
    return None


def get_artifacts(owner: str, repo: str, token: str, run_id: int) -> list[dict[str, Any]]:
    code, body = github_api(
        method="GET",
        owner=owner,
        repo=repo,
        token=token,
        path=f"/actions/runs/{run_id}/artifacts",
    )
    if code != 200 or not isinstance(body, dict):
        return []
    arts = body.get("artifacts", [])
    if not isinstance(arts, list):
        return []
    return [a for a in arts if isinstance(a, dict)]


def _http_fetch_bytes(url: str, token: str, *, accept: str, with_auth: bool) -> tuple[int, bytes, str]:
    def _single_fetch(auth_value: str | None) -> tuple[int, bytes, str]:
        headers = {
            "Accept": accept,
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "NCAACards-ActionRunner",
        }
        if auth_value:
            headers["Authorization"] = auth_value
        req = urllib.request.Request(url, method="GET", headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                try:
                    data = resp.read()
                except http.client.IncompleteRead as e:
                    data = e.partial or b""
                status = int(getattr(resp, "status", 200) or 200)
                location = str(getattr(resp, "url", "") or "")
                return status, data, location
        except urllib.error.HTTPError as e:
            body = b""
            try:
                body = e.read()
            except http.client.IncompleteRead as ie:
                body = ie.partial or b""
            except Exception:
                pass
            location = str(e.headers.get("Location", "") if e.headers else "")
            return int(e.code or 0), body, location
        except Exception:
            return 0, b"", ""

    if with_auth:
        code, data, location = _single_fetch(f"Bearer {token}")
        if code == 401:
            code, data, location = _single_fetch(f"token {token}")
        return code, data, location

    return _single_fetch(None)


def download_artifact_zip(owner: str, repo: str, token: str, artifact_id: int, archive_url: str = "") -> tuple[bytes | None, str]:
    api_url = f"https://api.github.com/repos/{owner}/{repo}/actions/artifacts/{artifact_id}/zip"
    urls = [u for u in [archive_url, api_url] if u]
    accepts = ["application/octet-stream", "application/zip", "*/*", "application/vnd.github+json"]
    last_err = "unknown download error"

    for u in urls:
        for accept in accepts:
            code, data, loc = _http_fetch_bytes(u, token, accept=accept, with_auth=True)
            if data[:2] == b"PK":
                return data, ""

            # Sometimes GitHub returns a redirect URL that must be fetched without auth header.
            if loc and loc != u:
                _c2, d2, _loc2 = _http_fetch_bytes(loc, token, accept="*/*", with_auth=False)
                if d2[:2] == b"PK":
                    return d2, ""
                last_err = f"http {code} -> redirected non-zip"
            else:
                last_err = f"http {code} ({len(data)} bytes)"

    return None, last_err


def extract_first_html(zip_bytes: bytes) -> tuple[str, bytes] | None:
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
            html_names = [n for n in zf.namelist() if n.lower().endswith((".html", ".htm"))]
            if not html_names:
                return None
            target = html_names[0]
            return target, zf.read(target)
    except Exception:
        return None


def run_progress(status: str, conclusion: str) -> tuple[int, str]:
    s = (status or "").strip().lower()
    c = (conclusion or "").strip().lower()
    if s == "queued":
        return 20, "Queued"
    if s == "in_progress":
        return 65, "Running"
    if s == "completed":
        if c == "success":
            return 100, "Completed"
        return 100, f"Completed ({conclusion or 'unknown'})"
    if s:
        return 35, s.replace("_", " ").title()
    return 0, "Not started"


def run_matches_request(run: dict[str, Any], year: str, player: str, after_ts: str | None) -> bool:
    title = str(run.get("display_title") or "")
    if year not in title and player.lower() not in title.lower():
        # display_title often contains actor; fallback to only timestamp gate.
        pass
    if after_ts:
        ra = str(run.get("created_at") or "")
        if ra:
            try:
                if _parse_iso(ra) < _parse_iso(after_ts):
                    return False
            except Exception:
                pass
    return True


ROSTER_BT_CSV_CANDIDATES = [
    PIPELINE_ROOT / "data" / "bt" / "bt_advstats_2010_2026.csv",
    PIPELINE_ROOT / "data" / "bt" / "bt_advstats_2010_2025.csv",
    PIPELINE_ROOT / "data" / "bt" / "bt_advstats_2019_2025.csv",
]


@st.cache_resource(show_spinner=False)
def load_roster_core():
    mod = roster_sim.load_module(PROJECT_ROOT)
    loaded_files = []
    bt_rows = []
    seen = set()
    for p in ROSTER_BT_CSV_CANDIDATES:
        if not p.exists():
            continue
        rows = roster_sim.read_bt_rows(p)
        if not rows:
            continue
        loaded_files.append(p)
        for r in rows:
            k = (
                mod.norm_player_name(mod.bt_get(r, ["player_name"])),
                mod.norm_team(mod.bt_get(r, ["team"])),
                mod.norm_season(mod.bt_get(r, ["year"])),
            )
            if not k[0] or not k[1] or not k[2]:
                continue
            if int(k[2]) < 2021:
                continue
            if k in seen:
                continue
            seen.add(k)
            bt_rows.append(r)
    return mod, bt_rows, loaded_files


def roster_seasons(mod, bt_rows):
    years = sorted(
        {
            int(mod.norm_season(mod.bt_get(r, ["year"])))
            for r in bt_rows
            if mod.norm_season(mod.bt_get(r, ["year"])).isdigit()
            and int(mod.norm_season(mod.bt_get(r, ["year"]))) >= 2021
        },
        reverse=True,
    )
    return years


def roster_teams(mod, bt_rows, season):
    teams = sorted(
        {
            (mod.bt_get(r, ["team"]) or "").strip()
            for r in bt_rows
            if mod.norm_season(mod.bt_get(r, ["year"])) == str(season)
        }
    )
    return [t for t in teams if t]


def roster_rows_for_team(mod, bt_rows, season, team):
    out = []
    for r in bt_rows:
        if mod.norm_season(mod.bt_get(r, ["year"])) != str(season):
            continue
        t = (mod.bt_get(r, ["team"]) or "").strip()
        if mod.norm_team(t) != mod.norm_team(team):
            continue
        name = (mod.bt_get(r, ["player_name"]) or "").strip()
        if not name:
            continue
        m = mod._row_transfer_metrics(r)
        out.append(
            {
                "player": name,
                "team": t,
                "season": season,
                "conf": (mod.bt_get(r, ["conf", "conference"]) or "").strip(),
                "mpg": float(m.get("mpg", 20.0) or 20.0),
            }
        )
    out.sort(key=lambda x: x["player"])
    return out


def roster_candidate_pool(mod, bt_rows, season, exclude_team):
    rows = []
    for r in bt_rows:
        if mod.norm_season(mod.bt_get(r, ["year"])) != str(season):
            continue
        team = (mod.bt_get(r, ["team"]) or "").strip()
        if mod.norm_team(team) == mod.norm_team(exclude_team):
            continue
        name = (mod.bt_get(r, ["player_name"]) or "").strip()
        if not name:
            continue
        m = mod._row_transfer_metrics(r)
        rows.append(
            {
                "player": name,
                "team": team,
                "season": season,
                "conf": (mod.bt_get(r, ["conf", "conference"]) or "").strip(),
                "mpg": float(m.get("mpg", 15.0) or 15.0),
                "label": f"{name} ({team})",
            }
        )
    rows.sort(key=lambda x: x["label"])
    return rows


def _render_static_table(df: pd.DataFrame):
    st.markdown(df.to_html(index=False, escape=False), unsafe_allow_html=True)


def render_card_tab(owner: str, repo: str, token: str, workflow_file: str, ref: str):
    index = load_team_player_index()
    _, all_confs = load_player_conference_index()
    years = [y for y in ["2021", "2022", "2023", "2024", "2025", "2026"] if y in index]
    if not years:
        years = ["2021", "2022", "2023", "2024", "2025", "2026"]

    if "card_requests" not in st.session_state:
        st.session_state.card_requests = []

    conf_choices = [""] + (all_confs if all_confs else [])

    top_cols = st.columns([1, 2, 2, 1.5, 1.2, 2])
    with top_cols[0]:
        year = st.selectbox("Season", years, index=len(years) - 1, key="card_year")
    teams = sorted(index.get(year, {}).keys())
    with top_cols[1]:
        team = st.selectbox("Team", teams, key="card_team") if teams else st.selectbox("Team", [""], key="card_team_empty")
    players = index.get(year, {}).get(team, []) if team else []
    with top_cols[2]:
        player = st.selectbox("Player", players, key="card_player") if players else st.selectbox("Player", [""], key="card_player_empty")
    with top_cols[3]:
        projection_mode = st.selectbox("Projection Mode", ["NBA Draft", "Transfer"], key="proj_mode_1")
    with top_cols[4]:
        compare = st.checkbox("Compare", value=False, key="compare_mode")
    with top_cols[5]:
        destination_conf = st.selectbox(
            "Destination Conference",
            conf_choices,
            key="transfer_dest_conf",
            disabled=(projection_mode != "Transfer"),
        )

    second_player_cfg = None
    if compare:
        row2 = st.columns([1, 2, 2, 1.5, 1.2, 2])
        with row2[0]:
            year2 = st.selectbox("Season", years, index=len(years) - 1, key="card_year_2")
        teams2 = sorted(index.get(year2, {}).keys())
        with row2[1]:
            team2 = st.selectbox("Team", teams2, key="card_team_2") if teams2 else st.selectbox("Team", [""], key="card_team_empty_2")
        players2 = index.get(year2, {}).get(team2, []) if team2 else []
        with row2[2]:
            player2 = st.selectbox("Player", players2, key="card_player_2") if players2 else st.selectbox("Player", [""], key="card_player_empty_2")
        with row2[3]:
            projection_mode_2 = st.selectbox("Projection Mode", ["NBA Draft", "Transfer"], key="proj_mode_2")
        with row2[4]:
            st.markdown("&nbsp;")
        with row2[5]:
            destination_conf_2 = st.selectbox(
                "Destination Conference",
                conf_choices,
                key="transfer_dest_conf_2",
                disabled=(projection_mode_2 != "Transfer"),
            )
        second_player_cfg = {
            "year": year2.strip(),
            "team": team2.strip(),
            "player": player2.strip(),
            "transfer_up": projection_mode_2 == "Transfer",
            "destination_conf": destination_conf_2.strip(),
        }

    action_cols = st.columns([1, 1, 1.5])
    with action_cols[0]:
        run_btn = st.button("Run Card Build", type="primary")
    with action_cols[1]:
        refresh_btn = st.button("Refresh Status")
    with action_cols[2]:
        auto_refresh = st.checkbox("Auto-refresh run status", value=True)
    download_html = st.checkbox("Show HTML download option", value=False)
    backend_mode = st.selectbox("Build Backend", ["Local (render on screen)", "GitHub Actions"], index=0)

    if "local_cards" not in st.session_state:
        st.session_state.local_cards = []

    if run_btn:
        builds = [
            {
                "year": year.strip(),
                "team": team.strip(),
                "player": player.strip(),
                "transfer_up": projection_mode == "Transfer",
                "destination_conf": destination_conf.strip(),
            }
        ]
        if second_player_cfg:
            builds.append(second_player_cfg)

        for b in builds:
            if not b["year"] or not b["player"]:
                st.error("Please choose season/team/player for each selected card.")
                st.stop()
            if b["transfer_up"] and not b["destination_conf"]:
                st.error(f"Please choose a destination conference for {b['player']} in Transfer mode.")
                st.stop()

        if backend_mode.startswith("Local"):
            local_out: list[dict[str, Any]] = []
            for i, b in enumerate(builds):
                ok, html_text, err = _build_card_html_local(
                    year=b["year"],
                    player=b["player"],
                    team=b["team"],
                    transfer_up=bool(b["transfer_up"]),
                    destination_conference=b["destination_conf"],
                )
                local_out.append(
                    {
                        "label": f"{b['player']} ({b['year']})",
                        "ok": ok,
                        "html": html_text,
                        "error": err,
                        "download_name": f"{slugify(b['player'])}_{b['year']}.html",
                    }
                )
            st.session_state.local_cards = local_out
            st.session_state.card_requests = []
        else:
            st.session_state.local_cards = []

        if backend_mode.startswith("Local"):
            pass
        else:
            actor = github_viewer_login(owner, repo, token)
            new_requests: list[dict[str, Any]] = []
            ts_base = int(time.time())
            for i, b in enumerate(builds, start=1):
                out_name = f"streamlit_cards/{b['year']}/{slugify(b['player'])}_{ts_base}_{i}.html"
                out_repo_path = f"player_cards_pipeline/output/{out_name}"
                ts = _iso_now()
                ok, msg = dispatch_build(
                    owner,
                    repo,
                    token,
                    workflow_file,
                    ref,
                    year=b["year"],
                    player=b["player"],
                    team=b["team"],
                    output_filename=out_name,
                    commit_to_repo=True,
                    transfer_up=bool(b["transfer_up"]),
                    destination_conference=b["destination_conf"],
                )
                if not ok:
                    st.error(f"{b['player']}: {msg}")
                    continue
                rid = find_run_id_for_dispatch(
                    owner,
                    repo,
                    token,
                    workflow_file,
                    after_ts=ts,
                    actor_login=actor,
                )
                new_requests.append(
                    {
                        "label": f"{b['player']} ({b['year']})",
                        "year": b["year"],
                        "player": b["player"],
                        "run_id": rid,
                        "after_ts": ts,
                        "actor": actor,
                        "out_repo_path": out_repo_path,
                        "status": "queued",
                        "conclusion": "",
                    }
                )
            st.session_state.card_requests = new_requests

    local_cards = st.session_state.get("local_cards", [])
    if local_cards and backend_mode.startswith("Local"):
        display_cols = st.columns(2) if (compare and len(local_cards) >= 2) else None
        for i, card in enumerate(local_cards):
            label = card.get("label", "Card")
            ok = bool(card.get("ok"))
            html_text = str(card.get("html") or "")
            err = str(card.get("error") or "")
            dl_name = str(card.get("download_name") or f"card_{i+1}.html")
            if display_cols is not None and i < len(display_cols):
                ctx = display_cols[i]
            else:
                ctx = st
            with ctx:
                st.caption(label)
                if ok and html_text:
                    st.components.v1.html(html_text, height=1700, scrolling=True)
                    if download_html:
                        st.download_button(
                            label="Download HTML",
                            data=html_text.encode("utf-8"),
                            file_name=dl_name,
                            mime="text/html",
                            key=f"local_html_dl_{i}",
                        )
                else:
                    st.error(err or "Local build failed.")
        return

    requests = st.session_state.get("card_requests", [])
    if refresh_btn or requests:
        any_active = False
        updated: list[dict[str, Any]] = []
        for req in requests:
            run_id = req.get("run_id")
            if not run_id:
                run_id = find_run_id_for_dispatch(
                    owner,
                    repo,
                    token,
                    workflow_file,
                    after_ts=str(req.get("after_ts") or ""),
                    actor_login=str(req.get("actor") or ""),
                    tries=1,
                    sleep_sec=0.25,
                )
            status = str(req.get("status") or "")
            conclusion = str(req.get("conclusion") or "")
            if run_id:
                code, body = github_api(
                    method="GET",
                    owner=owner,
                    repo=repo,
                    token=token,
                    path=f"/actions/runs/{int(run_id)}",
                )
                if code == 200 and isinstance(body, dict):
                    status = str(body.get("status") or "")
                    conclusion = str(body.get("conclusion") or "")
            req["run_id"] = run_id
            req["status"] = status
            req["conclusion"] = conclusion
            if status in {"queued", "in_progress"}:
                any_active = True
            updated.append(req)
        st.session_state.card_requests = updated
        requests = updated

        card_cols = st.columns(2) if (compare and len(requests) >= 2) else None

        def render_one(req: dict[str, Any], idx: int):
            st.caption(req.get("label", "Card"))
            rid = req.get("run_id")
            status = str(req.get("status") or "")
            conclusion = str(req.get("conclusion") or "")
            if rid:
                st.write(f"Run ID: `{rid}`")
            pct, label = run_progress(status, conclusion)
            st.progress(pct, text=f"{label} ({pct}%)")

            if status == "completed" and conclusion == "success":
                out_repo_path = str(req.get("out_repo_path") or "").strip()
                html_bytes = None
                err = "not found"
                for _ in range(8):
                    html_bytes, err = get_repo_file_bytes(owner, repo, token, out_repo_path, ref)
                    if html_bytes:
                        break
                    time.sleep(1.5)
                if html_bytes:
                    html_text = html_bytes.decode("utf-8", errors="replace")
                    st.components.v1.html(html_text, height=1700, scrolling=True)
                    if download_html:
                        st.download_button(
                            label="Download HTML",
                            data=html_bytes,
                            file_name=Path(out_repo_path).name,
                            mime="text/html",
                            key=f"html_dl_{rid}_{idx}",
                        )
                else:
                    st.caption(f"Built, but HTML is not readable yet ({err}).")
            elif status == "completed":
                st.caption("Run failed; no HTML output available.")

        for i, req in enumerate(requests):
            if card_cols is not None and i < len(card_cols):
                with card_cols[i]:
                    render_one(req, i)
            else:
                render_one(req, i)

        if auto_refresh and any_active:
            time.sleep(4)
            st.rerun()


def render_roster_tab():
    try:
        mod, bt_rows, bt_paths = load_roster_core()
    except FileNotFoundError:
        st.error(
            "Roster simulator requires full repo checkout (missing cbb_player_cards_v1/build_player_card.py). "
            "In Railway, set Root Directory to repo root and start command to "
            "`streamlit run player_cards_pipeline/action_runner_app.py --server.port 8080 --server.address 0.0.0.0`."
        )
        return
    if not bt_paths:
        st.error("No BT CSV found for roster simulator.")
        return

    years = roster_seasons(mod, bt_rows)
    if not years:
        st.error("No seasons found in BT CSV (2021+).")
        return

    c1, c2, c3 = st.columns([1, 1.2, 1.2])
    with c1:
        season = st.selectbox("Season", years, index=0, key="r_season")
    teams = roster_teams(mod, bt_rows, season)
    with c2:
        team = st.selectbox("Base Team", teams, key="r_team") if teams else st.selectbox("Base Team", [""], key="r_team_empty")
    base_rows = roster_rows_for_team(mod, bt_rows, season, team)
    if not base_rows:
        st.error("No roster rows for selected team/season.")
        return
    base_conf = base_rows[0]["conf"] if base_rows else ""
    with c3:
        dest_conf = st.text_input("Destination Conference (for added players)", value=base_conf, key="r_dest_conf")

    st.subheader("1) Exclude Current Players")
    base_names = [r["player"] for r in base_rows]
    exclude_names = st.multiselect("Select players to exclude", options=base_names, default=[], key="r_exclude")
    keep_rows = [r for r in base_rows if r["player"] not in set(exclude_names)]

    st.subheader("2) Add Incoming Players")
    pool = roster_candidate_pool(mod, bt_rows, season, team)
    add_labels = st.multiselect(
        "Select players to add",
        options=[r["label"] for r in pool],
        default=[],
        key="r_add",
        help="All available players for the selected season (excluding current base team).",
    )
    label_to_row = {r["label"]: r for r in pool}
    add_rows = [label_to_row[l] for l in add_labels if l in label_to_row]

    st.subheader("3) Minutes / Final Roster")
    merged = []
    for r in keep_rows:
        merged.append(
            {
                "player": r["player"],
                "team": r["team"],
                "season": r["season"],
                "added": False,
                "minutes": round(max(0.0, r["mpg"]), 1),
                "destination_conference": "",
            }
        )
    for r in add_rows:
        merged.append(
            {
                "player": r["player"],
                "team": r["team"],
                "season": r["season"],
                "added": True,
                "minutes": round(max(0.0, r["mpg"]), 1),
                "destination_conference": dest_conf,
            }
        )
    if not merged:
        st.warning("No players selected after exclusions/additions.")
        return

    edited = st.data_editor(
        pd.DataFrame(merged),
        hide_index=True,
        use_container_width=True,
        disabled=["player", "team", "season", "added"],
        key="r_editor",
        column_config={
            "minutes": st.column_config.NumberColumn(min_value=0.0, max_value=40.0, step=0.5),
            "destination_conference": st.column_config.TextColumn(help="Only used when added=True"),
        },
    )

    interaction_model = st.checkbox(
        "Enable Interaction Model",
        value=True,
        key="r_interaction",
        help="Rebalances projected usage/efficiency across selected players based on creation and spacing context.",
    )
    export_html = st.checkbox(
        "Also generate HTML report",
        value=False,
        key="r_export_html",
    )

    if st.button("Generate Team Fit Report", type="primary", key="r_generate"):
        progress = st.progress(0, text="Starting team-fit simulation...")
        inputs: list[roster_sim.InputPlayer] = []
        for _, row in edited.iterrows():
            inputs.append(
                roster_sim.InputPlayer(
                    player=str(row["player"]),
                    team=str(row["team"]),
                    season=int(row["season"]),
                    minutes=float(row["minutes"]),
                    destination_conference=str(row["destination_conference"] or ""),
                )
            )
        progress.progress(10, text="Prepared roster inputs")
        history_examples = roster_sim.build_transfer_examples(mod, bt_rows)
        progress.progress(25, text="Built transfer history examples")

        resolved: list[roster_sim.ResolvedPlayer] = []
        missing: list[roster_sim.InputPlayer] = []
        total_inputs = max(1, len(inputs))
        for idx, p in enumerate(inputs, start=1):
            bt_row = roster_sim.find_bt_row(mod, bt_rows, p)
            if bt_row is None:
                missing.append(p)
                continue
            projected, transfer_applied = roster_sim.project_transfer_metrics(mod, bt_row, p.destination_conference, history_examples)
            src_conf = mod._conference_key(mod.bt_get(bt_row, ["conf", "conference"]))
            resolved.append(
                roster_sim.ResolvedPlayer(
                    inp=p,
                    bt_row=bt_row,
                    projected=projected,
                    source_conf=src_conf,
                    transfer_applied=transfer_applied,
                )
            )
            if idx % 5 == 0 or idx == total_inputs:
                pct = 25 + int(35 * (idx / total_inputs))
                progress.progress(min(60, pct), text=f"Matching/projecting players ({idx}/{total_inputs})")

        if not resolved:
            progress.progress(100, text="No matched players")
            st.error("No players matched in BT data. Check names/teams.")
            return

        progress.progress(70, text="Aggregating team summaries")
        pace_scale = roster_sim.estimate_pace_scale(mod, bt_rows, season)
        team_summary, total_minutes = roster_sim.aggregate_team(resolved, interaction_model=interaction_model, pace_scale=pace_scale)
        current_players = roster_sim.build_current_team_players(mod, bt_rows, season, team)
        current_summary, _ = roster_sim.aggregate_team(current_players, interaction_model=False, pace_scale=pace_scale)
        league_team_summaries = roster_sim.build_season_team_summaries(mod, bt_rows, season)
        edited_player_metrics = roster_sim.projected_player_metrics(resolved, interaction_model=interaction_model)
        current_player_metrics = roster_sim.projected_player_metrics(current_players, interaction_model=False)
        in_rows, out_rows = roster_sim.build_in_out_rows(
            mod=mod,
            base_team=team,
            edited_players=resolved,
            edited_metrics=edited_player_metrics,
            current_players=current_players,
            current_metrics=current_player_metrics,
        )
        progress.progress(88, text="Preparing in-app tables")

        # Build In/Out.
        base_norm = mod.norm_team(team)
        selected_base_keys = {
            (str(p.inp.player).strip().lower(), mod.norm_team(p.inp.team))
            for p in resolved
            if mod.norm_team(p.inp.team) == base_norm
        }
        in_data = []
        for p, m in zip(resolved, edited_player_metrics):
            is_in = p.transfer_applied or mod.norm_team(p.inp.team) != base_norm
            if not is_in:
                continue
            in_data.append(
                {
                    "Player": p.inp.player,
                    "Team": p.inp.team,
                    "Season": p.inp.season,
                    "MPG": round(float(m.get("mpg", 0.0)), 1),
                    "PPG": round(float(m.get("ppg", 0.0)), 1),
                    "RPG": round(float(m.get("rpg", 0.0)), 1),
                    "APG": round(float(m.get("apg", 0.0)), 1),
                    "SPG": round(float(m.get("spg", 0.0)), 1),
                    "BPG": round(float(m.get("bpg", 0.0)), 1),
                    "FG%": round(float(m.get("fg_pct", 0.0)), 1),
                    "3P%": round(float(m.get("tp_pct", 0.0)), 1),
                    "FT%": round(float(m.get("ft_pct", 0.0)), 1),
                }
            )
        out_data = []
        for p, m in zip(current_players, current_player_metrics):
            key = (str(p.inp.player).strip().lower(), mod.norm_team(p.inp.team))
            if key in selected_base_keys:
                continue
            out_data.append(
                {
                    "Player": p.inp.player,
                    "Team": p.inp.team,
                    "Season": p.inp.season,
                    "MPG": round(float(m.get("mpg", 0.0)), 1),
                    "PPG": round(float(m.get("ppg", 0.0)), 1),
                    "RPG": round(float(m.get("rpg", 0.0)), 1),
                    "APG": round(float(m.get("apg", 0.0)), 1),
                    "SPG": round(float(m.get("spg", 0.0)), 1),
                    "BPG": round(float(m.get("bpg", 0.0)), 1),
                    "FG%": round(float(m.get("fg_pct", 0.0)), 1),
                    "3P%": round(float(m.get("tp_pct", 0.0)), 1),
                    "FT%": round(float(m.get("ft_pct", 0.0)), 1),
                }
            )

        proj_rows = []
        for key, label in roster_sim.TEAM_DISPLAY_METRICS:
            cur = current_summary.get(key)
            new = team_summary.get(key)
            delta = (new - cur) if (cur is not None and new is not None) else None
            pool_vals = [s[key] for s in league_team_summaries.values() if key in s]
            low_is_better = key in {"def_rating"}
            cur_rank = roster_sim.metric_rank(cur, pool_vals, lower_is_better=low_is_better)
            new_rank = roster_sim.metric_rank(new, pool_vals, lower_is_better=low_is_better)
            proj_rows.append(
                {
                    "Metric": label,
                    "Current Team": None if cur is None else round(float(cur), 2),
                    "Current Rank": cur_rank,
                    "Edited Roster": None if new is None else round(float(new), 2),
                    "Edited Rank": new_rank,
                    "Delta": None if delta is None else round(float(delta), 2),
                }
            )
        progress.progress(100, text="Completed")

        st.success("Team fit simulation completed.")
        st.subheader("In")
        if in_data:
            _render_static_table(pd.DataFrame(in_data))
        else:
            st.info("No added players selected.")

        st.subheader("Out")
        if out_data:
            _render_static_table(pd.DataFrame(out_data))
        else:
            st.info("No removed players.")

        st.subheader("Team Projection: Current vs Edited")
        _render_static_table(pd.DataFrame(proj_rows))

        if export_html:
            out_dir = PIPELINE_ROOT / "output" / "roster_simulator"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_name = f"{team.replace(' ', '_').lower()}_{season}_roster_sim_{int(time.time())}.html"
            out_path = out_dir / out_name
            roster_sim.render_html(
                out_path=out_path,
                season=season,
                players=resolved,
                edited_summary=team_summary,
                total_minutes=total_minutes,
                current_summary=current_summary,
                base_team=team,
                league_team_summaries=league_team_summaries,
                in_rows=in_rows,
                out_rows=out_rows,
                interaction_model=interaction_model,
            )
            st.caption(f"HTML export: {out_path}")
            html_bytes = out_path.read_bytes()
            st.download_button(
                label="Download HTML Report",
                data=html_bytes,
                file_name=out_path.name,
                mime="text/html",
                type="secondary",
                key="r_download_html",
            )

        if missing:
            st.warning(f"Missing matches: {len(missing)}")
            for m in missing[:20]:
                st.write(f"- {m.player} ({m.team}, {m.season})")


def main() -> None:
    st.set_page_config(page_title="NCAAM Tools", layout="wide")
    st.markdown(
        """
<style>
.stTabs [data-baseweb="tab-list"] { gap: 1rem; }
.stTabs [data-baseweb="tab"] { font-weight: 700; }
table { width: 100% !important; border-collapse: collapse !important; table-layout: fixed !important; }
table th { padding: 8px 10px !important; font-size: 0.9rem !important; white-space: nowrap !important; text-align: center !important; }
table td { padding: 8px 10px !important; font-size: 0.9rem !important; white-space: nowrap !important; text-align: center !important; overflow: hidden !important; text-overflow: ellipsis !important; }
table tbody td:first-child { text-align: left !important; }
</style>
        """,
        unsafe_allow_html=True,
    )

    owner = _cfg("GITHUB_OWNER")
    repo = _cfg("GITHUB_REPO")
    token = _cfg("GITHUB_TOKEN")
    workflow_file = _cfg("GITHUB_WORKFLOW_FILE", DEFAULT_WORKFLOW_FILE) or DEFAULT_WORKFLOW_FILE
    ref = _cfg("GITHUB_REF", DEFAULT_REF) or DEFAULT_REF

    if not owner or not repo or not token:
        st.error("Missing config vars: GITHUB_OWNER, GITHUB_REPO, GITHUB_TOKEN")
        st.code(
            """
GITHUB_OWNER = "dbcjason"
GITHUB_REPO = "NCAACards"
GITHUB_TOKEN = "ghp_..."
GITHUB_WORKFLOW_FILE = "build_player_card.yml"
GITHUB_REF = "main"
            """.strip()
        )
        st.stop()

    tab_cards, tab_roster = st.tabs(["Player Profiles", "Roster Construction"])
    with tab_cards:
        render_card_tab(owner, repo, token, workflow_file, ref)
    with tab_roster:
        render_roster_tab()


if __name__ == "__main__":
    main()
