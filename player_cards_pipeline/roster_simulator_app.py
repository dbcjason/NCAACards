#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from scripts import roster_simulator as sim

ROOT = Path(__file__).resolve().parents[1]
BT_CSV_CANDIDATES = [
    ROOT / "player_cards_pipeline" / "data" / "bt" / "bt_advstats_2010_2026.csv",
    ROOT / "player_cards_pipeline" / "data" / "bt" / "bt_advstats_2010_2025.csv",
    ROOT / "player_cards_pipeline" / "data" / "bt" / "bt_advstats_2019_2025.csv",
]
OUTPUT_DIR = ROOT / "player_cards_pipeline" / "output" / "roster_simulator"


@st.cache_resource(show_spinner=False)
def load_core():
    mod = sim.load_module(ROOT)
    loaded_files = []
    bt_rows = []
    seen = set()
    for p in BT_CSV_CANDIDATES:
        if not p.exists():
            continue
        rows = sim.read_bt_rows(p)
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
            if k in seen:
                continue
            seen.add(k)
            bt_rows.append(r)
    return mod, bt_rows, loaded_files


def season_values(mod, bt_rows):
    years = sorted(
        {
            int(mod.norm_season(mod.bt_get(r, ["year"])))
            for r in bt_rows
            if mod.norm_season(mod.bt_get(r, ["year"])).isdigit()
            and int(mod.norm_season(mod.bt_get(r, ["year"]))) >= 2019
        },
        reverse=True,
    )
    return years


def team_values(mod, bt_rows, season):
    teams = sorted(
        {
            (mod.bt_get(r, ["team"]) or "").strip()
            for r in bt_rows
            if mod.norm_season(mod.bt_get(r, ["year"])) == str(season)
        }
    )
    return [t for t in teams if t]


def rows_for_team_season(mod, bt_rows, season, team):
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
                "uid": f"{name}|{t}|{season}",
                "player": name,
                "team": t,
                "season": season,
                "conf": (mod.bt_get(r, ["conf", "conference"]) or "").strip(),
                "mpg": float(m.get("mpg", 20.0) or 20.0),
                "row": r,
            }
        )
    out.sort(key=lambda x: x["player"])
    return out


def candidate_pool(mod, bt_rows, season, exclude_team):
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
                "uid": f"{name}|{team}|{season}",
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


def main():
    st.set_page_config(page_title="Men Roster Simulator", layout="wide")
    st.title("Men Roster Simulator")
    st.caption("UCLA preloaded. Exclude current roster players, add incoming players, set minutes, then generate in-app team-fit tables.")

    mod, bt_rows, bt_paths = load_core()
    if bt_paths:
        shown = ", ".join(str(p.name) for p in bt_paths)
        st.caption(f"BT sources: {shown} | merged rows: {len(bt_rows)}")
    else:
        st.error("No BT CSV found. Expected one of: bt_advstats_2010_2026.csv, bt_advstats_2010_2025.csv, bt_advstats_2019_2025.csv")
        return
    years = season_values(mod, bt_rows)
    if not years:
        st.error("No seasons found in BT CSV.")
        return

    default_year = years[0]
    col1, col2, col3 = st.columns([1, 1.2, 1.2])
    with col1:
        season = st.selectbox("Season", years, index=0)

    teams = team_values(mod, bt_rows, season)
    ucla_idx = next((i for i, t in enumerate(teams) if mod.norm_team(t) == mod.norm_team("UCLA")), 0)
    with col2:
        team = st.selectbox("Base Team", teams, index=ucla_idx)

    base_rows = rows_for_team_season(mod, bt_rows, season, team)
    if not base_rows:
        st.error("No roster rows for selected team/season.")
        return

    base_conf = base_rows[0]["conf"] if base_rows else ""
    with col3:
        dest_conf = st.text_input("Destination Conference (for added players)", value=base_conf)

    st.subheader("1) Exclude Current Players")
    base_names = [r["player"] for r in base_rows]
    exclude_names = st.multiselect("Select players to exclude", options=base_names, default=[])
    keep_rows = [r for r in base_rows if r["player"] not in set(exclude_names)]

    st.subheader("2) Add Incoming Players")
    pool = candidate_pool(mod, bt_rows, season, team)

    add_labels = st.multiselect(
        "Select players to add",
        options=[r["label"] for r in pool],
        default=[],
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

    df = pd.DataFrame(merged)
    edited = st.data_editor(
        df,
        hide_index=True,
        use_container_width=True,
        disabled=["player", "team", "season", "added"],
        column_config={
            "minutes": st.column_config.NumberColumn(min_value=0.0, max_value=40.0, step=0.5),
            "destination_conference": st.column_config.TextColumn(help="Only used when added=True"),
        },
    )

    interaction_model = st.checkbox(
        "Enable Interaction Model",
        value=True,
        help="Rebalances projected usage/efficiency across selected players based on creation and spacing context.",
    )
    export_html = st.checkbox(
        "Also generate HTML report",
        value=False,
    )
    out_name = st.text_input(
        "Output HTML filename (used only if export is enabled)",
        value=f"{team.replace(' ', '_').lower()}_{season}_roster_sim.html",
        disabled=not export_html,
    )

    if st.button("Generate Team Fit Report", type="primary"):
        progress_text = st.empty()
        progress = st.progress(0, text="Starting team-fit simulation...")
        inputs: list[sim.InputPlayer] = []
        for _, row in edited.iterrows():
            inputs.append(
                sim.InputPlayer(
                    player=str(row["player"]),
                    team=str(row["team"]),
                    season=int(row["season"]),
                    minutes=float(row["minutes"]),
                    destination_conference=str(row["destination_conference"] or ""),
                )
            )
        progress.progress(10, text="Prepared roster inputs")

        history_examples = sim.build_transfer_examples(mod, bt_rows)
        progress.progress(25, text="Built transfer history examples")
        resolved: list[sim.ResolvedPlayer] = []
        missing: list[sim.InputPlayer] = []

        total_inputs = max(1, len(inputs))
        for idx, p in enumerate(inputs, start=1):
            bt_row = sim.find_bt_row(mod, bt_rows, p)
            if bt_row is None:
                missing.append(p)
                continue
            projected, transfer_applied = sim.project_transfer_metrics(mod, bt_row, p.destination_conference, history_examples)
            src_conf = mod._conference_key(mod.bt_get(bt_row, ["conf", "conference"]))
            resolved.append(
                sim.ResolvedPlayer(
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
        pace_scale = sim.estimate_pace_scale(mod, bt_rows, season)
        team_summary, total_minutes = sim.aggregate_team(
            resolved,
            interaction_model=interaction_model,
            pace_scale=pace_scale,
        )
        current_players = sim.build_current_team_players(mod, bt_rows, season, team)
        current_summary, _ = sim.aggregate_team(
            current_players,
            interaction_model=False,
            pace_scale=pace_scale,
        )
        league_team_summaries = sim.build_season_team_summaries(mod, bt_rows, season)
        edited_player_metrics = sim.projected_player_metrics(resolved, interaction_model=interaction_model)
        current_player_metrics = sim.projected_player_metrics(current_players, interaction_model=False)
        in_rows, out_rows = sim.build_in_out_rows(
            mod=mod,
            base_team=team,
            edited_players=resolved,
            edited_metrics=edited_player_metrics,
            current_players=current_players,
            current_metrics=current_player_metrics,
        )
        progress.progress(88, text="Preparing in-app tables")

        # In/Out tables rendered directly in app.
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
                    "From Team": p.inp.team,
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
        for key, label in sim.TEAM_DISPLAY_METRICS:
            cur = current_summary.get(key)
            new = team_summary.get(key)
            delta = (new - cur) if (cur is not None and new is not None) else None
            pool = [s[key] for s in league_team_summaries.values() if key in s]
            low_is_better = key in {"def_rating"}
            cur_rank = sim.metric_rank(cur, pool, lower_is_better=low_is_better)
            new_rank = sim.metric_rank(new, pool, lower_is_better=low_is_better)
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
            st.table(pd.DataFrame(in_data))
        else:
            st.info("No added players selected.")

        st.subheader("Out")
        if out_data:
            st.table(pd.DataFrame(out_data))
        else:
            st.info("No removed players.")

        st.subheader("Team Projection: Current vs Edited")
        st.table(pd.DataFrame(proj_rows))

        if export_html:
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            out_path = OUTPUT_DIR / out_name
            sim.render_html(
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
            )

        if missing:
            st.warning(f"Missing matches: {len(missing)}")
            for m in missing[:20]:
                st.write(f"- {m.player} ({m.team}, {m.season})")


if __name__ == "__main__":
    main()
