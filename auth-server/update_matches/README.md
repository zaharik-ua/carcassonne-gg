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
- `duels.results_checked_at`
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
BGA_EMAIL_2=...
BGA_PASSWORD_2=...
BGA_EMAIL_3=...
BGA_PASSWORD_3=...
CHROME_BINARY_PATH=/usr/bin/chromium
CHROMEDRIVER_PATH=/usr/bin/chromedriver
```

Notes:

- `BGA_EMAIL` / `BGA_PASSWORD` are the primary account.
- `BGA_EMAIL_2` / `BGA_PASSWORD_2`, `BGA_EMAIL_3` / `BGA_PASSWORD_3`, and so on are optional reserve accounts.
- when BGA starts returning empty tables, the updater refreshes the HTTP session and rotates to the next configured account automatically.
- repeated empty-table responses are counted inside `duels.results_last_error`; after `BGA_EMPTY_TABLES_ERROR_LIMIT` attempts the duel is marked `Error`.
- if a batch hits a mass empty-table anomaly, the updater skips the whole batch without updating duel rows, so the same duels will be retried on the next run.

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
python3 run_update_matches.py --targets finished_pending
```

Manual test for one match:

```bash
python3 run_update_matches.py --match-id 20250330UKRPRT
```

## Selection rules

- `finished_pending`: duel already ended, status is not `Done`, `Error`, or `No Show`
- `ongoing`: duel already started, not yet ended, status is not `Done`, `Error`, or `No Show`
- manual test mode: `--match-id <match_id>` ignores automatic selection and loads all duels of that match
- automatic runs prioritize the least recently checked duels via `duels.results_checked_at`

## Result rules

- `dw1` / `dw2` updated from BGA
- empty `tables=[]` from BGA is treated as a temporary fetch failure, not as a valid `0:0` result
- repeated empty-table responses eventually escalate the duel to `Error`
- duel status becomes:
  - `Done` when one side exactly reaches `games_to_win` and the other is still below
  - `In progress` while the duel is still inside its play window and no winner is determined yet
  - `Error` when the play window is over and no valid winner is determined
- match status becomes:
  - `Done` when all non-deleted duels of the match are `Done`
  - `In progress` while the current time is inside the combined duel time window of the match and not all duels are `Done`
  - `Planned` otherwise
- `games` rows are upserted by `bga_table_id`

## Logging

- manual run: logs go to console with timestamps
- systemd run: logs are appended to `/var/log/carcassonne/update-duels.log`
- when BGA returns empty tables, the updater logs the active account label and rotates to the next configured account before retrying
- recommended inspection:

```bash
tail -f /var/log/carcassonne/update-duels.log
```
