# NCAACards

Standalone project for college basketball player cards.

## What is included
- Card builder: `cbb_player_cards_v1/build_player_card.py`
- Daily/automation workflows:
  - `.github/workflows/cbbd_2026_plays_every_other_day.yml`
  - `.github/workflows/refresh_2027_data_daily.yml`
  - `.github/workflows/refresh_2027_plays_daily.yml`
  - `.github/workflows/build_player_card.yml`
  - `.github/workflows/bt_bootstrap_once.yml`
- Config: `player_cards_pipeline/config/settings.json`
- One-time Bart history files (local bootstrap):
  - `player_cards_pipeline/data/bt/bt_advstats_2010_2025.csv`
  - `player_cards_pipeline/data/bt/bt_advstats_2010_2026.csv`
  - `player_cards_pipeline/data/bt/bt_playerstat_2010_2025.csv`
  - `player_cards_pipeline/data/bt/raw_playerstat_json/*.json`
- Manual yearly PBP assets from ncaahoopR:
  - `player_cards_pipeline/data/manual/plays_by_year/<year>/plays_<year>.csv.gz`
  - `player_cards_pipeline/data/manual/pbp_metrics/<year>/pbp_player_metrics_<year>.csv`

## Local bootstrap scripts
- `player_cards_pipeline/scripts/bootstrap_ncaahoopr_manual_data.py`
- `player_cards_pipeline/scripts/bootstrap_bt_playerstat_json_from_downloads.py`
- `player_cards_pipeline/scripts/fetch_bt_advgames_by_year.py`

## Notes
- Historical ncaahoopR schema differs by year. In this build, metric files for 2010-2022 are mostly header-only; raw yearly `plays_<year>.csv.gz` files are present.
- `build_player_card.yml` supports `.csv.gz` for `plays_csv_by_year` and auto-unzips during the run.
- The 2027 daily workflow expects the direct Hoop Explorer export archive in the
  GitHub Actions secret `HOOP_EXPLORER_EXPORT_URL_2027`. The `/Exports` page is
  documentation and cannot be used as the archive URL itself.
- The 2027 plays workflow appends CBBD data into `cbbd_seasons/2026-2027/`.
- Large raw/manual datasets are ignored by default in `.gitignore` to avoid GitHub size issues.
