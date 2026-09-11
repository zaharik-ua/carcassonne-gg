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
- `game_replays`

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
- reserve accounts are used only when the current BGA account/session fails to return a valid response.

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

## Manual replay command

Replay import is intentionally manual-only while this feature is a PoC.
`run_update_matches.py` and `update-duels.timer` never create or retry
`game_replays` records.

The replay script accepts the exact primary key from `games.id`, reads that
row's `bga_table_id`, logs in with the configured BGA server account, and stores
the raw BGA archive plus normalized tile/meeple events in `game_replays`.

From the `auth-server` directory:

```bash
python3 get_game_replay.py '<games.id>'
```

To fetch an already stored replay again:

```bash
python3 get_game_replay.py '<games.id>' --force
```

The manual command tries the primary account first and automatically switches
through every configured `BGA_EMAIL_N` / `BGA_PASSWORD_N` reserve account when
BGA rejects the replay request (for example, after `limit (replay)`).

The database path is read from `AUTH_SQLITE_PATH`, then `DB_PATH`, and otherwise
defaults to `auth-server/data/auth.sqlite`. It can also be provided explicitly:

```bash
python3 get_game_replay.py '<games.id>' --db-path /absolute/path/to/auth.sqlite
```

Raw BGA logs are parsed in memory and are not stored. `events_json` contains
ordered `pickTile`, `playTile`, and `playPartisan` events. `players_json`
contains player ids, names, BGA color hex values, and normalized meeple color
names (`black`, `blue`, `green`, `red`, or `yellow`). `carcassonne_lab_url`
contains the encoded CarcassonneLab replay URL when all required moves and
player colors were found.

The replay import also stores compact derived history data:

- `board_stats_json`: final board `width` and `height`, including the starting
  tile;
- `meeple_stats_json`: placements, recovered meeples, and meeples remaining on
  the board, both in total and per player;
- `scoring_json`: scoring events and totals for fields, cities, roads, and
  monasteries (including BGA `cloister`/`abbey` aliases);
- `player_time_json`: total game duration and active turn time per player,
  derived from notification `time` values. `newActivePlayer` timestamps are
  preferred, followed by `gameStateChange.active_player`; `playTile`
  timestamps are the final fallback.

The ordered tile/meeple events can reconstruct intermediate board states.
`board_stats_json` stores only the final dimensions.

Failures are saved as `status = 'error'` with `last_error`.

## Selection rules

- `finished_pending`: duel already ended, status is not `Done`, `Error`, or `No Show`, and it is still eligible for result sync
- `ongoing`: duel already started, not yet ended, status is not `Done`, `Error`, or `No Show`, and it is still eligible for result sync
- manual test mode: `--match-id <match_id>` ignores automatic target selection but still skips protected duels
- protected duels are never synced from BGA: deleted duels and statuses `Cancelled`, `Draft`, `Requested new time`, `Removed`
- automatic runs prioritize the least recently checked duels via `duels.results_checked_at`

## Result rules

- `dw1` / `dw2` updated from BGA
- duel status becomes:
  - `Done` when one side exactly reaches `games_to_win` and the other is still below
  - `In progress` while the duel is still inside its play window and no winner is determined yet
  - `Error` when the play window is over and no valid winner is determined
- match status becomes:
  - `Done` when all non-deleted duels of the match are `Done`
  - `In progress` while the current time is inside the combined duel time window of the match and not all duels are `Done`
  - `Planned` otherwise
- when a match changes to `Done`, existing team and player standings for its tournament are recalculated; tournaments without standings are skipped
- `games` rows are upserted by `bga_table_id`
- before writing either results or errors, the script re-checks the duel and skips it if it became protected during the run

## Logging

- manual run: logs go to console with timestamps
- systemd run: logs are appended to `/var/log/carcassonne/update-duels.log`
- recommended inspection:

```bash
tail -f /var/log/carcassonne/update-duels.log
```
