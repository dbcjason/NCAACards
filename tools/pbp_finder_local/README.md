# Local Play-by-Play Finder (Men 2026)

This is a local-only query tool (not deployed to the live site).

## Files
- `build_pbp_db.py` -> builds SQLite from men 2025-26 play-by-play CSV shards
- `server.py` -> local web server + query API
- `index.html` -> local UI
- `pbp_2026_men.sqlite` -> generated query database (ignored by git)

## Build Database
From repo root:

```bash
python3 tools/pbp_finder_local/build_pbp_db.py \
  --source-dir "/path/to/cbbd_seasons/2025-2026/tables" \
  --out-db "tools/pbp_finder_local/pbp_2026_men.sqlite"
```

If omitted, `--source-dir` and `--out-db` use the script defaults.

## Start Local App
From repo root:

```bash
python3 tools/pbp_finder_local/server.py
```

Then open: `http://127.0.0.1:8765`

## Included Filters
- Player (searchable)
- Event
  - Field goal attempt
  - Field goal miss
  - Field goal make
  - Assist
  - Offensive rebound
  - Defensive rebound
  - Steal
  - Block
  - Turnover
- Shot location: Rim, Mid Range, Three
- Assisted?: Yes/No (shown only for FG events)
- Assisted By: searchable; appears when relevant

## Output Columns
- Date
- Teams
- Score at play time
- Time remaining (`clock`)
- Quarter/Period (`period`)
- Player
- Event
- Shot location
- Assisted
- Assisted By
- Recipient (for assist events)
- Raw play text

## Notes
- `Assist` events are generated from made assisted field goals.
- Free throws are excluded from FG event filters.
