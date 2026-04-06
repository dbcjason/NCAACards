# Railway Setup

This repo is set up to use Railway for payload generation without moving the website off Vercel.

## Recommended Railway services

Create these services from the `dbcjason/NCAACards` repo:

1. `ncaam-nightly`
2. `ncaam-worker`

Use the repo root as the service root directory.

## Build command

Set this in Railway service settings:

```bash
python -m pip install --upgrade pip && python -m pip install pandas
```

## Start commands

### Nightly cron service

Use:

```bash
python3 player_cards_pipeline/scripts/railway_run_payload_job.py
```

Suggested variables:

- `YEARS=2026`
- `MIN_GAMES=5`
- `INCREMENTAL=true`
- optional `LIMIT=50` for smoke tests

Historical bootstrap:

- `YEARS=2019-2025`
- `INCREMENTAL=false`

### Persistent warm worker

Use:

```bash
python3 player_cards_pipeline/scripts/railway_warm_worker.py
```

Suggested variables:

- `WARM_SEASON=2026`
- `HEARTBEAT_SECONDS=300`

This worker keeps one Python process alive so the season caches stay warm for repeated builds.

## Volumes

Attach a Railway volume and mount it at:

```text
/data
```

Recommended later uses:

- sqlite card caches
- intermediate shard manifests
- transient warm-cache artifacts

## Notes

- Keep the frontend on Vercel.
- Use Railway for heavy payload generation.
- Use Supabase for metadata/indexing.
- Keep GitHub as payload storage for now.
