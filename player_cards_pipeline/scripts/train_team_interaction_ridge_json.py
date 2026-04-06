#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from pathlib import Path
from statistics import median, pstdev
from typing import Any

FEATURE_ORDER = [
    "n_players",
    "minutes_sum",
    "wm_pts",
    "wm_reb",
    "wm_ast",
    "wm_stl",
    "wm_blk",
    "wm_ortg",
    "wm_drtg",
    "wm_usg",
    "wm_ts",
    "wm_efg",
    "wm_tp",
    "wm_astp",
    "wm_top",
    "wm_orbp",
    "wm_drbp",
    "wm_bpm",
    "wm_gbpm",
    "wm_dgbpm",
    "usg_top1",
    "usg_top3_sum",
    "usg_std",
    "ts_std",
    "ast_std",
    "int_usg_x_ts",
    "int_ast_x_tp",
    "int_blk_x_drb",
    "int_orb_x_to",
    "int_offdef_gap",
]

METRIC_KEYS = ["net", "off", "def", "ast100", "tov100", "stl100", "blk100", "reb100", "oreb", "fg", "tp", "ts"]


def _num(v: Any, default: float = 0.0) -> float:
    if v is None:
        return default
    s = str(v).strip().replace("%", "")
    if not s:
        return default
    try:
        x = float(s)
        if math.isfinite(x):
            return x
    except Exception:
        pass
    return default


def _pct(v: Any) -> float:
    x = _num(v, 0.0)
    return x * 100.0 if 0.0 <= x <= 1.0 else x


def _safe_div(a: float, b: float) -> float:
    return 0.0 if abs(b) < 1e-9 else a / b


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def train_ridge_gd(X: list[list[float]], y: list[float], lam: float = 0.08, iters: int = 450, lr: float = 0.05) -> dict[str, Any]:
    n = len(X)
    d = len(X[0]) if n else 0
    mean_x = [0.0] * d
    std_x = [1.0] * d
    for j in range(d):
        mean_x[j] = sum(X[i][j] for i in range(n)) / max(1, n)
    for j in range(d):
        v = sum((X[i][j] - mean_x[j]) ** 2 for i in range(n)) / max(1, n)
        std_x[j] = math.sqrt(v) or 1.0
    Xn = [[(r[j] - mean_x[j]) / std_x[j] for j in range(d)] for r in X]
    bias = sum(y) / max(1, len(y))
    w = [0.0] * d
    for _ in range(iters):
        grad_w = [0.0] * d
        grad_b = 0.0
        for i in range(n):
            pred = bias + sum(w[j] * Xn[i][j] for j in range(d))
            err = pred - y[i]
            grad_b += err
            for j in range(d):
                grad_w[j] += err * Xn[i][j]
        scale = 2.0 / max(1, n)
        grad_b *= scale
        for j in range(d):
            grad_w[j] = grad_w[j] * scale + 2.0 * lam * w[j]
            w[j] -= lr * grad_w[j]
        bias -= lr * grad_b
    return {"meanX": mean_x, "stdX": std_x, "weights": w, "bias": bias}


def weighted_mean(vals: list[tuple[float, float]]) -> float:
    num = 0.0
    den = 0.0
    for v, w in vals:
        if w <= 0:
            continue
        num += v * w
        den += w
    return _safe_div(num, den)


def build_features(rows: list[dict[str, str]]) -> dict[str, float]:
    mins = [max(0.0, _num(r.get("mp"), 0.0)) for r in rows]
    if sum(mins) <= 0:
        mins = [1.0] * len(rows)

    def wm(key: str, pct: bool = False) -> float:
        vals: list[tuple[float, float]] = []
        for r, w in zip(rows, mins):
            v = _pct(r.get(key)) if pct else _num(r.get(key), 0.0)
            vals.append((v, w))
        return weighted_mean(vals)

    f: dict[str, float] = {}
    f["n_players"] = float(len(rows))
    f["minutes_sum"] = sum(_num(r.get("mp"), 0.0) for r in rows)
    f["wm_pts"] = wm("pts")
    f["wm_reb"] = wm("treb")
    f["wm_ast"] = wm("ast")
    f["wm_stl"] = wm("stl")
    f["wm_blk"] = wm("blk")
    f["wm_ortg"] = wm("ORtg")
    f["wm_drtg"] = wm("drtg")
    f["wm_usg"] = wm("usg", pct=True)
    f["wm_ts"] = wm("TS_per", pct=True)
    f["wm_efg"] = wm("eFG", pct=True)
    f["wm_tp"] = wm("TP_per", pct=True)
    f["wm_astp"] = wm("AST_per")
    f["wm_top"] = wm("TO_per")
    f["wm_orbp"] = wm("ORB_per")
    f["wm_drbp"] = wm("DRB_per")
    f["wm_bpm"] = wm("bpm")
    f["wm_gbpm"] = wm("gbpm")
    f["wm_dgbpm"] = wm("dgbpm")

    usgs = sorted((_pct(r.get("usg")) for r in rows), reverse=True)
    ts_vals = [_pct(r.get("TS_per")) for r in rows]
    ast_vals = [_num(r.get("AST_per")) for r in rows]
    f["usg_top1"] = usgs[0] if usgs else 0.0
    f["usg_top3_sum"] = sum(usgs[:3]) if usgs else 0.0
    f["usg_std"] = pstdev(usgs) if len(usgs) > 1 else 0.0
    f["ts_std"] = pstdev(ts_vals) if len(ts_vals) > 1 else 0.0
    f["ast_std"] = pstdev(ast_vals) if len(ast_vals) > 1 else 0.0
    f["int_usg_x_ts"] = (f["wm_usg"] * f["wm_ts"]) / 100.0
    f["int_ast_x_tp"] = (f["wm_astp"] * f["wm_tp"]) / 100.0
    f["int_blk_x_drb"] = (f["wm_blk"] * f["wm_drbp"]) / 100.0
    f["int_orb_x_to"] = (f["wm_orbp"] * f["wm_top"]) / 100.0
    f["int_offdef_gap"] = f["wm_ortg"] - f["wm_drtg"]
    return f


def build_metric_targets(rows: list[dict[str, str]]) -> dict[str, float] | None:
    adjoe_vals = [_num(r.get("adjoe"), float("nan")) for r in rows]
    adrtg_vals = [_num(r.get("adrtg"), float("nan")) for r in rows]
    adjoe_vals = [v for v in adjoe_vals if 70.0 <= v <= 140.0]
    adrtg_vals = [v for v in adrtg_vals if 70.0 <= v <= 140.0]
    if not adjoe_vals or not adrtg_vals:
        return None
    off = float(median(adjoe_vals))
    deff = float(median(adrtg_vals))

    mins = [max(0.0, _num(r.get("mp"), 0.0)) for r in rows]
    if sum(mins) <= 0:
        mins = [1.0] * len(rows)

    def wavg(key: str, pct: bool = False) -> float:
        vals = []
        for r, w in zip(rows, mins):
            vals.append((_pct(r.get(key)) if pct else _num(r.get(key)), w))
        return weighted_mean(vals)

    two_pm = sum(_num(r.get("twoPM")) * m for r, m in zip(rows, mins))
    two_pa = sum(_num(r.get("twoPA")) * m for r, m in zip(rows, mins))
    tpm = sum(_num(r.get("TPM")) * m for r, m in zip(rows, mins))
    tpa = sum(_num(r.get("TPA")) * m for r, m in zip(rows, mins))
    ftm = sum(_num(r.get("FTM")) * m for r, m in zip(rows, mins))
    fta = sum(_num(r.get("FTA")) * m for r, m in zip(rows, mins))
    ast = sum(_num(r.get("ast")) * m for r, m in zip(rows, mins))
    stl = sum(_num(r.get("stl")) * m for r, m in zip(rows, mins))
    blk = sum(_num(r.get("blk")) * m for r, m in zip(rows, mins))
    oreb = sum(_num(r.get("oreb")) * m for r, m in zip(rows, mins))
    dreb = sum(_num(r.get("dreb")) * m for r, m in zip(rows, mins))
    pts = (2.0 * two_pm) + (3.0 * tpm) + ftm
    fga = two_pa + tpa
    fgm = two_pm + tpm
    fg = _safe_div(fgm, fga) * 100.0 if fga > 0 else 0.0
    tp = _safe_div(tpm, tpa) * 100.0 if tpa > 0 else 0.0
    ts = _safe_div(pts, 2.0 * (fga + (0.44 * fta))) * 100.0 if (fga + 0.44 * fta) > 0 else 0.0
    ppg = wavg("pts")
    poss = _safe_div(ppg, off / 100.0) if off > 1e-6 else 0.0
    ast100 = (_safe_div(ast, poss) * 100.0) if poss > 0 else wavg("AST_per")
    stl100 = (_safe_div(stl, poss) * 100.0) if poss > 0 else wavg("stl_per")
    blk100 = (_safe_div(blk, poss) * 100.0) if poss > 0 else wavg("blk_per")
    reb100 = (_safe_div((oreb + dreb), poss) * 100.0) if poss > 0 else (wavg("ORB_per") + wavg("DRB_per"))

    return {
        "net": off - deff,
        "off": off,
        "def": deff,
        "ast100": ast100,
        "tov100": wavg("TO_per"),
        "stl100": stl100,
        "blk100": blk100,
        "reb100": reb100,
        "oreb": wavg("ORB_per"),
        "fg": fg,
        "tp": tp,
        "ts": ts,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Train deploy-safe ridge model JSON for roster simulator.")
    ap.add_argument("--project-root", default=".")
    ap.add_argument("--bt-csv", default="player_cards_pipeline/data/bt/bt_advstats_2010_2026.csv")
    ap.add_argument("--min-season", type=int, default=2011)
    ap.add_argument("--max-season", type=int, default=2025)
    ap.add_argument("--min-players", type=int, default=7)
    ap.add_argument("--out-json", default="player_cards_pipeline/data/models/team_interaction_ridge_v1.json")
    args = ap.parse_args()

    root = Path(args.project_root).resolve()
    bt_csv = (root / args.bt_csv).resolve() if not Path(args.bt_csv).is_absolute() else Path(args.bt_csv)
    out_json = (root / args.out_json).resolve() if not Path(args.out_json).is_absolute() else Path(args.out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)

    rows = read_rows(bt_csv)
    grouped: dict[tuple[int, str], list[dict[str, str]]] = defaultdict(list)
    for r in rows:
        season = int(_num(r.get("year"), 0))
        team = str(r.get("team", "")).strip()
        if not team or season < args.min_season or season > args.max_season:
            continue
        grouped[(season, team)].append(r)

    X: list[list[float]] = []
    y: dict[str, list[float]] = {k: [] for k in METRIC_KEYS}
    for _, rr in grouped.items():
        if len(rr) < args.min_players:
            continue
        feats = build_features(rr)
        targets = build_metric_targets(rr)
        if not targets:
            continue
        X.append([feats.get(k, 0.0) for k in FEATURE_ORDER])
        for mk in METRIC_KEYS:
            y[mk].append(float(targets.get(mk, 0.0)))

    if len(X) < 50:
        raise SystemExit(f"Not enough team-seasons to train: {len(X)}")

    models = {mk: train_ridge_gd(X, y[mk]) for mk in METRIC_KEYS}
    payload = {
        "version": "team_interaction_ridge_v1",
        "feature_order": FEATURE_ORDER,
        "metric_keys": METRIC_KEYS,
        "rows_total": len(X),
        "train_range": [args.min_season, args.max_season],
        "models": models,
    }
    out_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"[team-interaction-ridge] rows={len(X)}")
    print(f"[team-interaction-ridge] model -> {out_json}")


if __name__ == "__main__":
    main()

