# WTCOC Match Sync

Manual CLI for fetching WTCOC data, checking the mapping, and optionally upserting into `auth-server` `matches` / `duels`.

Run from `auth-server` root:

```bash
python3 run_sync_wtcoc_matches.py --db-path ./data/auth.sqlite --tournament-id WTCOC-2026
```

Filter a single WTCOC match:

```bash
python3 run_sync_wtcoc_matches.py --match-id 1
```

Apply into DB:

```bash
python3 run_sync_wtcoc_matches.py --db-path ./data/auth.sqlite --tournament-id WTCOC-2026 --apply
```

Current scope:

- fetches `calendar` and optionally `playoff`
- compares WTCOC team names against local `teams`
- reports what is ready to map into `matches` / `duels`
- can upsert mapped rows into SQLite with `--apply`
