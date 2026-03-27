# Auth Server Match Updates

Python module for updating duel results from BGA using the `auth-server` SQLite database.

It reads:
- `duels`
- `duel_formats`
- `profiles`

It writes:
- `duels.dw1`
- `duels.dw2`
- `duels.status`
- `games`

It persists sync errors into:
- `duels.results_last_error`

## Install

```bash
cd /home/carcassonne-gg/auth-server
pip3 install -r update_matches/requirements.txt
```

## Required env

```env
BGA_EMAIL=...
BGA_PASSWORD=...
CHROME_BINARY_PATH=/usr/bin/chromium
CHROMEDRIVER_PATH=/usr/bin/chromedriver
```

## Run

From `auth-server` root:

```bash
python3 run_update_matches.py
```

Examples:

```bash
python3 run_update_matches.py --targets ongoing --limit 10
```

```bash
python3 run_update_matches.py --targets empty_finished
```

Manual test for one match:

```bash
python3 run_update_matches.py --match-id 20250330UKRPRT
```

## Selection rules

- `ongoing`: `duels.status = 'Planned'`, start time already passed, players assigned
- `empty_finished`: `duels.status = 'Done'` and `dw1` or `dw2` is empty
- manual test mode: `--match-id <match_id>` ignores automatic selection and loads all duels of that match

## Result rules

- `dw1` / `dw2` updated from BGA
- duel status becomes `Done` when one side reaches `games_to_win`
- `games` rows are upserted by `bga_table_id`
