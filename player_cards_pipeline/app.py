#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
from pathlib import Path

import streamlit as st


ROOT = Path(__file__).resolve().parent
SETTINGS_PATH = ROOT / "config" / "settings.json"
CARD_SCRIPT = ROOT.parent / "cbb_player_cards_v1" / "build_player_card.py"
DEFAULT_OUT_DIR = ROOT / "output"


def load_settings() -> dict:
    return json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))


def read_csv_rows(path: Path):
    import csv
    with path.open(newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        return list(r)


def norm_year(y: str) -> str:
    y = str(y).strip()
    if "/" in y:
        return "20" + y.split("/")[-1][-2:]
    if "-" in y and len(y.split("-")[-1]) == 2:
        return "20" + y.split("-")[-1]
    return y


def main() -> None:
    st.set_page_config(page_title="CBB Player Card Runner", layout="wide")
    st.title("CBB Player Card Runner")

    settings = load_settings()

    bt_csv = (ROOT / settings["bt_advstats_csv"]).resolve()
    if not bt_csv.exists():
        st.error(f"Missing BT advstats CSV: {bt_csv}")
        st.stop()

    bt_rows = read_csv_rows(bt_csv)
    years = sorted({norm_year(r.get("year", "")) for r in bt_rows if r.get("year")}, reverse=True)

    year = st.selectbox("Year", years)
    rows_y = [r for r in bt_rows if norm_year(r.get("year", "")) == year]
    players = sorted({r.get("player_name", "").strip() for r in rows_y if r.get("player_name", "").strip()})
    player = st.selectbox("Player", players)

    teams_for_player = sorted({r.get("team", "").strip() for r in rows_y if r.get("player_name", "").strip() == player and r.get("team", "").strip()})
    team = st.selectbox("Team", teams_for_player) if teams_for_player else ""

    default_out = str((DEFAULT_OUT_DIR / f"{player.lower().replace(' ', '_')}_{year}.html").resolve())
    out_html = st.text_input("Output HTML", value=default_out)

    if st.button("Run"):
        plays_map = settings.get("plays_csv_by_year", {})
        advgames_map = settings.get("advgames_csv_by_year", {})
        pbp_map = settings.get("pbp_metrics_csv_by_year", {})

        plays_csv = (ROOT / plays_map.get(year, "")).resolve() if plays_map.get(year) else None
        advgames_csv = (ROOT / advgames_map.get(year, "")).resolve() if advgames_map.get(year) else None
        pbp_csv = (ROOT / pbp_map.get(year, "")).resolve() if pbp_map.get(year) else None

        if not plays_csv or not plays_csv.exists():
            st.error(f"Missing plays CSV for {year}: {plays_csv}")
            st.stop()

        cmd = [
            "python3", str(CARD_SCRIPT),
            "--plays-csv", str(plays_csv),
            "--player", player,
            "--season", year,
            "--bt-csv", str(bt_csv),
            "--bt-playerstat-url-template", settings.get("bt_playerstat_url_template", "https://barttorvik.com/{year}_pbp_playerstat_array.json"),
            "--out-html", out_html,
        ]
        if team:
            cmd += ["--team", team]

        bio_csv = settings.get("bio_csv", "")
        if bio_csv and Path(bio_csv).exists():
            cmd += ["--bio-csv", bio_csv]
        if advgames_csv and advgames_csv.exists():
            cmd += ["--advgames-csv", str(advgames_csv)]
        if pbp_csv and pbp_csv.exists():
            cmd += ["--pbp-metrics-csv", str(pbp_csv)]

        try:
            res = subprocess.run(cmd, check=True, capture_output=True, text=True)
            st.success("Card generated")
            st.code(res.stdout)
            out_path = Path(out_html).resolve()
            if out_path.exists():
                st.markdown(f"Output: `{out_path}`")
        except subprocess.CalledProcessError as e:
            st.error("Build failed")
            st.code((e.stdout or "") + "\n" + (e.stderr or ""))


if __name__ == "__main__":
    main()
