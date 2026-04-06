#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import pickle
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, pstdev
from typing import Any


def _num(v: Any, default: float = 0.0) -> float:
    if v is None:
        return default
    s = str(v).strip().replace("%", "")
    if not s:
        return default
    try:
        out = float(s)
        if math.isfinite(out):
            return out
    except Exception:
        pass
    return default


def _pct(v: Any) -> float:
    x = _num(v, 0.0)
    return x * 100.0 if 0.0 <= x <= 1.0 else x


def _safe_div(a: float, b: float) -> float:
    if abs(b) < 1e-9:
        return 0.0
    return a / b


@dataclass
class TeamSeason:
    season: int
    team: str
    rows: list[dict[str, str]]


BASE_FEATURES = [
    "mp", "pts", "treb", "ast", "stl", "blk",
    "usg", "TS_per", "eFG", "AST_per", "TO_per",
    "ORB_per", "DRB_per", "ftr", "TP_per",
    "bpm", "gbpm", "dgbpm", "ORtg", "drtg",
]

TARGET_KEYS = ("off_rating", "def_rating", "net_rating")


def read_bt_rows(bt_csv: Path) -> list[dict[str, str]]:
    with bt_csv.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def valid_target(off: float, deff: float) -> bool:
    # Filter clearly bad/misaligned rows from historical csv alignment issues.
    return 70.0 <= off <= 140.0 and 70.0 <= deff <= 140.0


def group_team_seasons(rows: list[dict[str, str]], min_season: int, max_season: int) -> list[TeamSeason]:
    grouped: dict[tuple[int, str], list[dict[str, str]]] = defaultdict(list)
    for r in rows:
        y = int(_num(r.get("year"), 0))
        t = str(r.get("team", "")).strip()
        if not t or y < min_season or y > max_season:
            continue
        grouped[(y, t)].append(r)
    out: list[TeamSeason] = []
    for (y, t), rr in grouped.items():
        out.append(TeamSeason(season=y, team=t, rows=rr))
    out.sort(key=lambda x: (x.season, x.team))
    return out


def weighted_mean(items: list[tuple[float, float]]) -> float:
    num = 0.0
    den = 0.0
    for v, w in items:
        if w <= 0:
            continue
        num += v * w
        den += w
    return _safe_div(num, den)


def build_features(ts: TeamSeason) -> dict[str, float]:
    rows = ts.rows
    # Use minutes weighted profile + concentration terms.
    weights = [max(0.0, _num(r.get("mp"), 0.0)) for r in rows]
    wsum = sum(weights)
    if wsum <= 0:
        wsum = float(len(rows))
        weights = [1.0] * len(rows)

    f: dict[str, float] = {}
    f["n_players"] = float(len(rows))
    f["minutes_sum"] = float(sum(_num(r.get("mp"), 0.0) for r in rows))

    # Weighted means for core features.
    for k in BASE_FEATURES:
        vals: list[tuple[float, float]] = []
        for r, w in zip(rows, weights):
            if k in {"TS_per", "eFG", "ftr", "TP_per"}:
                v = _pct(r.get(k))
            else:
                v = _num(r.get(k), 0.0)
            vals.append((v, w))
        f[f"wm_{k.lower()}"] = weighted_mean(vals)

    # Usage concentration and top-end creator/scorer pressure.
    usage = sorted((_num(r.get("usg"), 0.0) for r in rows), reverse=True)
    f["usg_top1"] = usage[0] if usage else 0.0
    f["usg_top3_sum"] = sum(usage[:3]) if usage else 0.0
    f["usg_std"] = pstdev(usage) if len(usage) > 1 else 0.0

    # Shooting spread and playmaking spread.
    ts_vals = [_pct(r.get("TS_per")) for r in rows]
    ast_vals = [_num(r.get("AST_per"), 0.0) for r in rows]
    f["ts_std"] = pstdev(ts_vals) if len(ts_vals) > 1 else 0.0
    f["ast_std"] = pstdev(ast_vals) if len(ast_vals) > 1 else 0.0

    # Approximate per-40 totals from per-game + mpg to keep scale consistent.
    def per40(per_game: float, mpg: float) -> float:
        return 40.0 * _safe_div(per_game, max(1e-6, mpg))

    p40_pts = []
    p40_ast = []
    p40_reb = []
    for r in rows:
        mpg = max(1e-6, _num(r.get("mp"), 0.0))
        p40_pts.append(per40(_num(r.get("pts"), 0.0), mpg))
        p40_ast.append(per40(_num(r.get("ast"), 0.0), mpg))
        p40_reb.append(per40(_num(r.get("treb"), 0.0), mpg))
    f["mean_pts40"] = mean(p40_pts) if p40_pts else 0.0
    f["mean_ast40"] = mean(p40_ast) if p40_ast else 0.0
    f["mean_reb40"] = mean(p40_reb) if p40_reb else 0.0

    # Explicit interaction terms (model can still learn more nonlinearity).
    f["int_usg_x_ts"] = f.get("wm_usg", 0.0) * f.get("wm_ts_per", 0.0) / 100.0
    f["int_ast_x_tp"] = f.get("wm_ast_per", 0.0) * f.get("wm_tp_per", 0.0) / 100.0
    f["int_blk_x_drb"] = f.get("wm_blk", 0.0) * f.get("wm_drb_per", 0.0) / 100.0
    f["int_orb_x_ftr"] = f.get("wm_orb_per", 0.0) * f.get("wm_ftr", 0.0) / 100.0
    f["int_offdef_gap"] = f.get("wm_ortg", 0.0) - f.get("wm_drtg", 0.0)

    return f


def build_targets(ts: TeamSeason) -> dict[str, float] | None:
    # Team-level adjusted targets exist on player rows; use robust median.
    off_vals = [_num(r.get("adjoe"), float("nan")) for r in ts.rows]
    def_vals = [_num(r.get("adrtg"), float("nan")) for r in ts.rows]
    off_vals = [v for v in off_vals if math.isfinite(v)]
    def_vals = [v for v in def_vals if math.isfinite(v)]
    if not off_vals or not def_vals:
        return None
    off = sorted(off_vals)[len(off_vals) // 2]
    deff = sorted(def_vals)[len(def_vals) // 2]
    if not valid_target(off, deff):
        return None
    return {
        "off_rating": off,
        "def_rating": deff,
        "net_rating": off - deff,
    }


def build_dataset(
    bt_rows: list[dict[str, str]],
    min_season: int,
    max_season: int,
    min_players: int,
) -> tuple[list[dict[str, float]], list[dict[str, float]], list[dict[str, Any]]]:
    team_seasons = group_team_seasons(bt_rows, min_season=min_season, max_season=max_season)
    x_rows: list[dict[str, float]] = []
    y_rows: list[dict[str, float]] = []
    meta_rows: list[dict[str, Any]] = []
    for ts in team_seasons:
        if len(ts.rows) < min_players:
            continue
        target = build_targets(ts)
        if target is None:
            continue
        feats = build_features(ts)
        x_rows.append(feats)
        y_rows.append(target)
        meta_rows.append({"season": ts.season, "team": ts.team})
    return x_rows, y_rows, meta_rows


def rmse(y_true: list[float], y_pred: list[float]) -> float:
    if not y_true:
        return 0.0
    return math.sqrt(sum((a - b) ** 2 for a, b in zip(y_true, y_pred)) / len(y_true))


def r2(y_true: list[float], y_pred: list[float]) -> float:
    if not y_true:
        return 0.0
    mu = sum(y_true) / len(y_true)
    ss_tot = sum((v - mu) ** 2 for v in y_true)
    if ss_tot <= 1e-12:
        return 0.0
    ss_res = sum((a - b) ** 2 for a, b in zip(y_true, y_pred))
    return 1.0 - (ss_res / ss_tot)


def main() -> None:
    ap = argparse.ArgumentParser(description="Train team interaction model (gradient boosted regression) for roster simulator.")
    ap.add_argument("--project-root", default=".")
    ap.add_argument(
        "--bt-csv",
        default="player_cards_pipeline/data/bt/bt_advstats_2010_2026.csv",
        help="BT player-season csv path (relative to project root by default).",
    )
    ap.add_argument("--min-season", type=int, default=2011)
    ap.add_argument("--max-season", type=int, default=2025)
    ap.add_argument("--min-players", type=int, default=7)
    ap.add_argument("--holdout-seasons", default="2024,2025", help="Comma-separated seasons for holdout evaluation.")
    ap.add_argument(
        "--out-model",
        default="player_cards_pipeline/data/models/team_interaction_gbr_v1.pkl",
    )
    ap.add_argument(
        "--out-report",
        default="player_cards_pipeline/data/models/team_interaction_gbr_v1_report.json",
    )
    args = ap.parse_args()

    try:
        import numpy as np  # noqa: F401
        from sklearn.ensemble import HistGradientBoostingRegressor
    except Exception as e:
        raise SystemExit(
            "Missing training deps. Install with: python3 -m pip install numpy scikit-learn\n"
            f"Detail: {e}"
        )

    root = Path(args.project_root).resolve()
    bt_csv = (root / args.bt_csv).resolve() if not Path(args.bt_csv).is_absolute() else Path(args.bt_csv)
    out_model = (root / args.out_model).resolve() if not Path(args.out_model).is_absolute() else Path(args.out_model)
    out_report = (root / args.out_report).resolve() if not Path(args.out_report).is_absolute() else Path(args.out_report)
    out_model.parent.mkdir(parents=True, exist_ok=True)
    out_report.parent.mkdir(parents=True, exist_ok=True)

    bt_rows = read_bt_rows(bt_csv)
    x_rows, y_rows, meta_rows = build_dataset(
        bt_rows=bt_rows,
        min_season=args.min_season,
        max_season=args.max_season,
        min_players=args.min_players,
    )
    if not x_rows:
        raise SystemExit("No valid team-season rows built for training.")

    feature_names = sorted({k for r in x_rows for k in r.keys()})
    X = [[float(r.get(k, 0.0)) for k in feature_names] for r in x_rows]
    Y = [[float(y.get(t, 0.0)) for t in TARGET_KEYS] for y in y_rows]

    holdout = {int(s.strip()) for s in args.holdout_seasons.split(",") if s.strip()}
    train_idx = [i for i, m in enumerate(meta_rows) if int(m["season"]) not in holdout]
    test_idx = [i for i, m in enumerate(meta_rows) if int(m["season"]) in holdout]
    if not train_idx or not test_idx:
        raise SystemExit("Need both train and holdout rows. Adjust --holdout-seasons or season range.")

    models: dict[str, Any] = {}
    preds = {"train": {}, "test": {}}
    metrics = {"train": {}, "test": {}}

    for t_i, t_name in enumerate(TARGET_KEYS):
        y_all = [row[t_i] for row in Y]
        y_train = [y_all[i] for i in train_idx]
        y_test = [y_all[i] for i in test_idx]
        X_train = [X[i] for i in train_idx]
        X_test = [X[i] for i in test_idx]

        model = HistGradientBoostingRegressor(
            learning_rate=0.05,
            max_depth=6,
            max_iter=350,
            min_samples_leaf=8,
            l2_regularization=0.1,
            random_state=42,
        )
        model.fit(X_train, y_train)
        models[t_name] = model

        pred_train = list(model.predict(X_train))
        pred_test = list(model.predict(X_test))

        preds["train"][t_name] = pred_train
        preds["test"][t_name] = pred_test
        metrics["train"][t_name] = {
            "rmse": rmse(y_train, pred_train),
            "r2": r2(y_train, pred_train),
        }
        metrics["test"][t_name] = {
            "rmse": rmse(y_test, pred_test),
            "r2": r2(y_test, pred_test),
        }

    bundle = {
        "model_type": "hist_gradient_boosting_regressor",
        "target_keys": list(TARGET_KEYS),
        "feature_names": feature_names,
        "models": models,
        "train_idx": train_idx,
        "test_idx": test_idx,
        "meta_rows": meta_rows,
        "version": "v1",
    }
    with out_model.open("wb") as f:
        pickle.dump(bundle, f, protocol=pickle.HIGHEST_PROTOCOL)

    report = {
        "model_path": str(out_model),
        "rows_total": len(meta_rows),
        "rows_train": len(train_idx),
        "rows_holdout": len(test_idx),
        "holdout_seasons": sorted(holdout),
        "metrics": metrics,
        "sample_holdout_rows": [meta_rows[i] for i in test_idx[:25]],
    }
    out_report.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"[team-interaction] rows total={len(meta_rows)} train={len(train_idx)} holdout={len(test_idx)}")
    print(f"[team-interaction] model -> {out_model}")
    print(f"[team-interaction] report -> {out_report}")
    print(json.dumps(metrics["test"], indent=2))


if __name__ == "__main__":
    main()
