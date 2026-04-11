# WTCOC Match Sync

Manual CLI for fetching WTCOC data and checking how well it maps into `auth-server` `matches` / `duels`.

Run from `auth-server` root:

```bash
python3 run_sync_wtcoc_matches.py --db-path ./data/auth.sqlite --tournament-id WTCOC-2026
```

Filter a single WTCOC match:

```bash
python3 run_sync_wtcoc_matches.py --match-id 1
```

Current scope:

- fetches `calendar` and optionally `playoff`
- compares WTCOC team names against local `associations`
- reports what is ready to map into `matches` / `duels`
- highlights missing data that still blocks a safe DB upsert
