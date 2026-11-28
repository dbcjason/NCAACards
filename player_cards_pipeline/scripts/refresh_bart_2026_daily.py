#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import re
import subprocess
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

ADVGAMES_HEADERS = [
    'numdate', 'datetext', 'opstyle', 'quality', 'win1', 'opponent', 'muid', 'win2',
    'Min_per', 'ORtg', 'Usage', 'eFG', 'TS_per', 'ORB_per', 'DRB_per', 'AST_per', 'TO_per',
    'dunksmade', 'dunksatt', 'rimmade', 'rimatt', 'midmade', 'midatt', 'twoPM', 'twoPA',
    'TPM', 'TPA', 'FTM', 'FTA', 'bpm_rd', 'Obpm', 'Dbpm', 'bpm_net', 'pts', 'ORB', 'DRB',
    'AST', 'TOV', 'STL', 'BLK', 'stl_per', 'blk_per', 'PF', 'possessions', 'bpm', 'sbpm',
    'loc', 'tt', 'pp', 'inches', 'cls', 'pid', 'year'
]

BT_ADV_HEADERS = [
    "player_name", "team", "conf", "GP", "Min_per", "ORtg", "usg", "eFG", "TS_per", "ORB_per", "DRB_per",
    "AST_per", "TO_per", "FTM", "FTA", "FT_per", "twoPM", "twoPA", "twoP_per", "TPM", "TPA", "TP_per",
    "blk_per", "stl_per", "ftr", "yr", "ht", "num", "porpag", "adjoe", "pfr", "year", "pid", "type",
    "Rec Rank", "ast/tov", "rimmade", "rimmade+rimmiss", "midmade", "midmade+midmiss",
    "rimmade/(rimmade+rimmiss)", "midmade/(midmade+midmiss)", "dunksmade", "dunksmiss+dunksmade",
    "dunksmade/(dunksmade+dunksmiss)", "pick", "drtg", "adrtg", "dporpag", "stops", "bpm", "obpm", "dbpm",
    "gbpm", "mp", "ogbpm", "dgbpm", "oreb", "dreb", "treb", "ast", "stl", "blk", "pts", "role", "3p/100?", "dob",
]


def google_translate_proxy_url(url: str) -> str:
    """Return a text-proxy URL for Bart when a runner IP is blocked."""
    parsed = urlsplit(url)
    if parsed.netloc.lower() != "barttorvik.com":
        return url
    query = parsed.query
    if query:
        query += "&"
    query += "_x_tr_sl=auto&_x_tr_tl=en&_x_tr_hl=en"
    return f"https://barttorvik-com.translate.goog{parsed.path}?{query}"


def fetch_candidates(url: str) -> list[str]:
    """Return ordinary endpoint variants, with the public proxy last."""
    url = normalize_url(url)
    candidates = [url]
    parsed = urlsplit(url)
    if parsed.netloc.lower() == "barttorvik.com":
        candidates.append(url.replace("https://barttorvik.com/", "https://www.barttorvik.com/", 1))
        candidates.append(google_translate_proxy_url(url))
    return list(dict.fromkeys(candidates))


def normalize_url(url: str) -> str:
    """Strip Markdown link syntax accidentally introduced by copied configs/logs."""
    value = str(url).strip()
    match = re.fullmatch(r"\[([^\]]+)\]\((https?://[^)]+)\)", value)
    if match:
        return match.group(2).strip()
    return value


def normalize_header_value(value: str) -> str:
    """Remove Markdown link wrappers from copied header/config values too."""
    return normalize_url(value)


def fetch_with_curl(url: str, headers: dict[str, str], timeout: int) -> bytes:
    """Fetch one URL with curl and reject empty/HTML error bodies."""
    url = normalize_url(url)
    curl_headers = []
    for key, value in headers.items():
        curl_headers.extend(["-H", f"{key}: {normalize_header_value(value)}"])
    result = subprocess.run(
        [
            "curl", "--fail", "--location", "--silent", "--show-error",
            "--compressed", "--connect-timeout", "30", "--max-time", str(timeout),
            *curl_headers, url,
        ],
        capture_output=True,
        check=True,
    )
    body = result.stdout
    if not body:
        raise RuntimeError(f"Empty response body for {url}")
    sample = body[:512].lstrip().lower()
    if sample.startswith((b"<!doctype html", b"<html", b"<head")):
        raise RuntimeError(f"HTML response returned where data was expected for {url}")
    return body


def fetch_bytes(url: str, retries: int = 6, timeout: int = 120) -> bytes:
    url = normalize_url(url)
    # Bart's CDN advertises a short cache lifetime but can still serve an
    # older edge response to GitHub runners. Give each daily refresh a unique
    # query token so the downloaded season reflects the current endpoint.
    cache_token = str(int(time.time()))
    separator = "&" if "?" in url else "?"
    url = f"{url}{separator}_bt_refresh={cache_token}"
    # Bart occasionally returns transient 403/5xx from anti-bot/proxy layers.
    # Retry with browser-like headers and exponential backoff.
    header_sets = [
        {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
            "Accept": "text/csv,application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://barttorvik.com/",
            "Connection": "keep-alive",
        },
        {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.8",
            "Referer": "https://barttorvik.com/",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        },
    ]

    last_err: Exception | None = None
    for attempt in range(1, max(1, retries) + 1):
        headers = header_sets[(attempt - 1) % len(header_sets)]
        for candidate in fetch_candidates(url):
            try:
                # Bart's CDN can reject urllib's request fingerprint. Try curl
                # first, then urllib, before moving to the next endpoint.
                return fetch_with_curl(candidate, headers, timeout)
            except (FileNotFoundError, subprocess.CalledProcessError, RuntimeError) as err:
                last_err = err
                try:
                    req = Request(candidate, headers=headers)
                    with urlopen(req, timeout=timeout) as resp:
                        data = resp.read()
                        if data:
                            sample = data[:512].lstrip().lower()
                            if sample.startswith((b"<!doctype html", b"<html", b"<head")):
                                raise RuntimeError(f"HTML response returned for {candidate}")
                            return data
                        raise RuntimeError(f"Empty response body for {candidate}")
                except HTTPError as e:
                    last_err = e
                    if e.code not in (403, 429, 500, 502, 503, 504):
                        raise
                except (URLError, TimeoutError, RuntimeError) as e:
                    last_err = e
            except HTTPError as e:
                last_err = e
                if e.code not in (403, 429, 500, 502, 503, 504):
                    raise

        if attempt < retries:
            sleep_s = min(45.0, 1.6 ** attempt)
            print(
                f"[bart-fetch] retry {attempt}/{retries} url={url} err={type(last_err).__name__}: {last_err}; sleep={sleep_s:.1f}s",
                flush=True,
            )
            time.sleep(sleep_s)

    raise RuntimeError(f"Failed to fetch after {retries} attempts: {url} ({last_err})")


def fetch_text(url: str) -> str:
    return fetch_bytes(url).decode("utf-8", errors="replace")


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if not path.exists():
        return [], []
    with path.open(newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        rows = list(r)
        return list(r.fieldnames or []), rows


def write_csv(path: Path, header: list[str], rows: list[dict[str, str]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in header})


def refresh_advstats(bt_dir: Path, year: int, history_start: int = 2010) -> None:
    url = f"https://barttorvik.com/getadvstats.php?year={year}&csv=1"
    txt = fetch_text(url)
    parsed = [r for r in csv.reader(txt.splitlines()) if r]
    if not parsed:
        raise RuntimeError(f"Empty advstats response for {year}")

    # getadvstats.php returns data rows only (no header row).
    h26 = list(BT_ADV_HEADERS)
    rows26 = parsed
    h26_out = h26 + (["bt_fetch_year"] if "bt_fetch_year" not in h26 else [])

    out_year = bt_dir / f"bt_advstats_{year}.csv"
    out_history = bt_dir / f"bt_advstats_{history_start}_{year}.csv"
    base_history = bt_dir / f"bt_advstats_{history_start}_{year - 1}.csv"
    trank_dir = bt_dir / "trank_by_year"
    ensure_dir(trank_dir)
    trank_year = trank_dir / f"trank_{year}.csv"
    trank_history = bt_dir / f"trank_{history_start}_{year}.csv"
    trank_base = bt_dir / f"trank_{history_start}_{year - 1}.csv"

    # A Bart CDN edge can occasionally return an older in-season snapshot.
    # Never replace a newer cached season with one containing fewer games.
    if out_year.exists():
        old_h, old_rows = read_csv(out_year)

        def season_progress(rows: list[dict[str, str]]) -> tuple[float, float, int]:
            gps = []
            for row in rows:
                try:
                    gps.append(float(row.get("GP", "")))
                except (TypeError, ValueError):
                    continue
            return (sum(gps), max(gps, default=0.0), len(rows))

        incoming_rows = [
            {BT_ADV_HEADERS[i]: value for i, value in enumerate(row[:len(BT_ADV_HEADERS)])}
            for row in parsed
        ]
        if old_rows and season_progress(incoming_rows) < season_progress(old_rows):
            print(
                f"[bart-refresh] keeping newer cached {year} advstats "
                f"({season_progress(old_rows)} > {season_progress(incoming_rows)})",
                flush=True,
            )
            parsed = [[row.get(h, "") for h in BT_ADV_HEADERS] for row in old_rows]

    # A Bart CDN edge can occasionally return an older in-season snapshot.
    # Never replace a newer cached season with one containing fewer games.
    if out_year.exists():
        old_h, old_rows = read_csv(out_year)

        def season_progress(rows: list[dict[str, str]]) -> tuple[float, float, int]:
            gps = []
            for row in rows:
                try:
                    gps.append(float(row.get("GP", "")))
                except (TypeError, ValueError):
                    continue
            return (sum(gps), max(gps, default=0.0), len(rows))

        incoming_rows = [
            {BT_ADV_HEADERS[i]: value for i, value in enumerate(row[:len(BT_ADV_HEADERS)])}
            for row in parsed
        ]
        if old_rows and season_progress(incoming_rows) < season_progress(old_rows):
            print(
                f"[bart-refresh] keeping newer cached {year} advstats "
                f"({season_progress(old_rows)} > {season_progress(incoming_rows)})",
                flush=True,
            )
            parsed = [[row.get(h, "") for h in BT_ADV_HEADERS] for row in old_rows]

    rows26_dict: list[dict[str, str]] = []
    rows26_trank: list[dict[str, str]] = []
    for r in rows26:
        rr = r + [""] * max(0, len(h26) - len(r))
        d = {h26[i]: rr[i] for i in range(len(h26))}
        d["bt_fetch_year"] = str(year)
        rows26_dict.append(d)
        td = dict(d)
        td["trank_year"] = str(year)
        rows26_trank.append(td)

    write_csv(out_year, h26_out, rows26_dict)
    trank_header = list(h26)
    if "trank_year" not in trank_header:
        trank_header.append("trank_year")
    write_csv(trank_year, trank_header, rows26_trank)

    base_h, base_rows = read_csv(base_history)
    if not base_h:
        # Fall back to just 2026 if no base history available.
        write_csv(out_history, h26_out, rows26_dict)
        return

    header = list(base_h)
    for k in h26_out:
        if k not in header:
            header.append(k)

    merged = []
    for r in base_rows:
        if r.get("bt_fetch_year", "") == str(year):
            continue
        merged.append(r)
    merged.extend(rows26_dict)

    write_csv(out_history, header, merged)

    # Maintain a trank combined file with explicit source year.
    th, tr = read_csv(trank_base)
    if not th:
        # Backfill from bt_advstats_2010_2025.csv if needed.
        bh, br = read_csv(base_history)
        if bh:
            th = list(bh)
            if "trank_year" not in th:
                th.append("trank_year")
            tr = []
            for row in br:
                d = dict(row)
                d["trank_year"] = row.get("year", "") or row.get("bt_fetch_year", "")
                tr.append(d)

    if not th:
        write_csv(trank_history, trank_header, rows26_trank)
        return

    merged_trank_header = list(th)
    for c in trank_header:
        if c not in merged_trank_header:
            merged_trank_header.append(c)

    tr_no_year = [r for r in tr if (r.get("trank_year", "") or r.get("year", "")) != str(year)]
    tr_no_year.extend(rows26_trank)
    write_csv(trank_history, merged_trank_header, tr_no_year)


def refresh_playerstat(bt_dir: Path, year: int) -> None:
    raw_dir = bt_dir / "raw_playerstat_json"
    ensure_dir(raw_dir)

    url = f"https://barttorvik.com/{year}_pbp_playerstat_array.json"
    raw = fetch_text(url)
    (raw_dir / f"{year}_pbp_playerstat_array.json").write_text(raw, encoding="utf-8")

    arr = json.loads(raw)
    csv_out = bt_dir / f"bt_playerstat_{year}.csv"
    header = [
        "year", "pid", "player", "team",
        "rim_made", "rim_miss", "rim_assisted",
        "mid_made", "mid_miss", "mid_assisted",
        "three_made", "three_miss", "three_assisted",
        "dunks_made", "dunks_miss", "dunks_assisted",
    ]
    rows: list[dict[str, str]] = []
    for it in arr:
        if not isinstance(it, list) or len(it) < 15:
            continue
        rows.append({
            "year": str(year),
            "pid": str(it[0]),
            "player": str(it[1]),
            "team": str(it[2]),
            "rim_made": str(it[3]),
            "rim_miss": str(it[4]),
            "rim_assisted": str(it[5]),
            "mid_made": str(it[6]),
            "mid_miss": str(it[7]),
            "mid_assisted": str(it[8]),
            "three_made": str(it[9]),
            "three_miss": str(it[10]),
            "three_assisted": str(it[11]),
            "dunks_made": str(it[12]),
            "dunks_miss": str(it[13]),
            "dunks_assisted": str(it[14]),
        })
    write_csv(csv_out, header, rows)


def refresh_advgames(bt_dir: Path, year: int) -> None:
    raw_dir = bt_dir / "raw_advgames_json"
    out_dir = bt_dir / "advgames_labeled"
    ensure_dir(raw_dir)
    ensure_dir(out_dir)

    urls = [
        f"https://barttorvik.com/{year}_all_advgames.json.gz",
        f"https://barttorvik.com/{year}_all_advgames.json",
    ]

    payload: bytes | None = None
    used_url = ""
    for u in urls:
        try:
            payload = fetch_bytes(u)
            used_url = u
            if payload:
                break
        except Exception:
            continue
    if not payload:
        raise RuntimeError(f"Could not fetch {year} all_advgames JSON")

    # Bart can serve plain JSON from the .gz URL; inspect the actual bytes.
    if payload[:2] == b"\x1f\x8b":
        txt = gzip.decompress(payload).decode("utf-8", errors="replace")
    else:
        txt = payload.decode("utf-8", errors="replace")

    (raw_dir / f"{year}_all_advgames.json").write_text(txt, encoding="utf-8")

    arr = json.loads(txt)
    rows: list[dict[str, str]] = []
    for it in arr:
        if not isinstance(it, list):
            continue
        row = {}
        for i, h in enumerate(ADVGAMES_HEADERS):
            row[h] = str(it[i]) if i < len(it) and it[i] is not None else ""
        rows.append(row)

    write_csv(out_dir / f"{year}_all_advgames_labeled.csv", ADVGAMES_HEADERS, rows)


def main() -> None:
    ap = argparse.ArgumentParser(description="Refresh Bart datasets used by player cards.")
    ap.add_argument("--project-root", required=True)
    ap.add_argument("--year", type=int, default=2026, help="Bart season year, e.g. 2027 for 2026-27.")
    ap.add_argument("--history-start", type=int, default=2010)
    args = ap.parse_args()

    root = Path(args.project_root)
    bt_dir = root / "player_cards_pipeline" / "data" / "bt"
    ensure_dir(bt_dir)

    failures: list[tuple[str, Exception]] = []
    for label, fn in (
        ("advstats", lambda: refresh_advstats(bt_dir, args.year, args.history_start)),
        ("playerstat", lambda: refresh_playerstat(bt_dir, args.year)),
        ("advgames", lambda: refresh_advgames(bt_dir, args.year)),
    ):
        try:
            fn()
        except Exception as exc:
            # A temporary Bart block must not prevent Hoop Explorer and other
            # independent daily jobs from running; existing files remain intact.
            failures.append((label, exc))
            print(f"[bart-refresh] WARNING {label} skipped: {exc}", flush=True)

    if failures:
        print(
            "[bart-refresh] completed with warnings; existing Bart cache was preserved "
            "for failed datasets",
            flush=True,
        )
    else:
        print(f"Bart {args.year} refresh complete")


if __name__ == "__main__":
    main()
