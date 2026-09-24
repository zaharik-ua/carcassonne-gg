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
BGA_EMAIL_4=...
BGA_PASSWORD_4=...
BGA_REPLAY_TOTAL_LIMIT=80
BGA_REPLAY_FRESH_RESERVE=50
BGA_REPLAY_HISTORICAL_LIMIT=30
BGA_REPLAY_MAX_TOTAL_LIMIT=100
BGA_REPLAY_COOLDOWN_HOURS=24
CHROME_BINARY_PATH=/usr/bin/chromium
CHROMEDRIVER_PATH=/usr/bin/chromedriver
```

Notes:

- Accounts 1–3 form the normal shared replay-account pool. Account 4 is a
  standby: the replay gateway can select it only while all configured accounts
  1–3 have an active cooldown. Exhausting their local request budgets does not
  unlock account 4.
- Before every `logs.html` request, the gateway chooses an available account
  with the lowest rolling-24h usage; ties use round-robin ordering.
- During an HTTP-session refresh, Selenium waits up to 10 seconds for
  `bgaConfig.requestToken`. A missing token triggers up to three complete
  refresh attempts before the replay is deferred as a temporary error.
- The replay budget defaults to total `80`, historical `30`, and a protected
  fresh/manual reserve of `50` per account. The values are configurable through
  the variables above.

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

## Automatic replay scheduling for new ranked games

When `run_update_matches.py` receives a BGA game that is not yet present in
`games` and the parent duel has `ranking = 1`, it creates a persistent replay
queue entry in the same database transaction. The entry has `status = pending`,
`retry_reason = initial`, `queue_class = fresh`, and `next_attempt_at` five
minutes after it was queued. Creating the game performs no BGA replay HTTP
request. Updating an existing game does not enqueue its replay again.

The update summary reports newly queued entries as `replays_scheduled` and
already available entries as `replays_ready`.

## Replay queue worker

Run due fresh work manually from the `auth-server` directory:

```bash
python3 retry_pending_game_replays.py --queue-class fresh --limit 3
```

Historical work uses the same engine and state machine:

```bash
python3 retry_pending_game_replays.py --queue-class historical --limit 3
```

Requested historical archives have a dedicated execution mode. It leaves the
stored queue class and request budget as `historical` and ignores ordinary
historical backlog:

```bash
python3 retry_pending_game_replays.py --queue-class archive-follow-up --limit 3
```

The worker takes a non-blocking `flock` next to the SQLite database and also
leases each selected row. This prevents overlapping runs from processing the
same game; an expired lease can be reclaimed. A run performs at most three
`logs.html` requests. Historical runs additionally perform at most one request
per available account and stop before each request whenever fresh work is due.

The `initial`, `archive`, and `colors` transitions use one request per attempt.
An absent archive is requested once and checked again after two minutes by
default. The delay is stored in the `bga_replay_archive_retry_minutes` system
setting and can be changed in `Admin` → `System Settings` without restarting
the replay worker. A
fallback replay is immediately marked ready and receives one color refresh
after 15 minutes. A second fallback response keeps the stored replay unchanged
and ends automatic retries.

Old games are not queued implicitly. Internal callers can idempotently add one
explicit historical game without making an HTTP request:

```python
from update_matches.replay_worker import enqueue_historical_game_replay

enqueue_historical_game_replay(
    "data/auth.sqlite",
    "GAME_ID",
    historical_batch_id="batch-2026-09",
)
```

### One-off existing replay backfill

After deploying the replay queue schema, preview the one-off cleanup/backfill:

```bash
./.venv/bin/python backfill_existing_game_replays.py
```

The command reports changes without writing by default. Apply it only after a
production SQLite backup:

```bash
./.venv/bin/python backfill_existing_game_replays.py \
  --apply \
  --batch-id legacy-ready-backfill
```

The transaction deletes `error` replay rows, normalizes every existing `ready`
row and schedules ready fallback-color rows as due `historical` color refreshes.
The existing `color_source` is authoritative: `fallback` rows are scheduled and
`bga` rows remain final. The script reads but never updates `color_source`, and
it never reschedules a fallback row whose single color refresh has already run.
Derived values recoverable from normalized events and players are rebuilt.
Existing scoring and player-time JSON are preserved because those values cannot
be reconstructed exactly after legacy raw logs have been removed.

### systemd timers

The repository contains a shared template service and three independent timers:

- `systemd/bga-replay-worker@.service`;
- `systemd/bga-replay-fresh.timer` — every two minutes;
- `systemd/bga-replay-archive-follow-up.timer` — every minute, but only for due
  historical rows whose BGA archive has already been requested;
- `systemd/bga-replay-historical.timer` — every 30 minutes;
- `systemd/bga-replay-worker.logrotate`.

Install the units without enabling the timers first:

```bash
sudo cp systemd/bga-replay-worker@.service /etc/systemd/system/
sudo cp systemd/bga-replay-fresh.timer /etc/systemd/system/
sudo cp systemd/bga-replay-archive-follow-up.timer /etc/systemd/system/
sudo cp systemd/bga-replay-historical.timer /etc/systemd/system/
sudo cp systemd/bga-replay-worker.logrotate /etc/logrotate.d/bga-replay-worker
sudo systemctl daemon-reload
sudo systemctl disable --now \
  bga-replay-fresh.timer \
  bga-replay-archive-follow-up.timer \
  bga-replay-historical.timer
```

Preview the due queue and rolling request usage without contacting BGA:

```bash
sqlite3 data/auth.sqlite "
SELECT queue_class, status, retry_reason, COUNT(*) AS due
FROM game_replays
WHERE retry_reason IN ('initial','archive','colors')
  AND next_attempt_at IS NOT NULL
  AND datetime(next_attempt_at) <= datetime('now')
GROUP BY queue_class, status, retry_reason
ORDER BY queue_class, retry_reason;
"

sqlite3 data/auth.sqlite "
SELECT account_label, request_class, COUNT(*) AS attempts_24h
FROM bga_replay_requests
WHERE endpoint = '/archive/archive/logs.html'
  AND datetime(attempted_at) >= datetime('now', '-24 hours')
GROUP BY account_label, request_class
ORDER BY account_label, request_class;
"
```

Perform the first smoke test with one request and inspect its JSON output and
the database rows before enabling a timer:

```bash
./.venv/bin/python retry_pending_game_replays.py \
  --queue-class fresh --limit 1
```

After checking queue transitions, budget rows, and logs, enable only fresh:

```bash
sudo systemctl enable --now bga-replay-fresh.timer
sudo systemctl list-timers --all bga-replay-fresh.timer
tail -n 100 /var/log/carcassonne/bga-replay-worker.log
```

Enable the archive follow-up lane and historical separately only after fresh
has run successfully:

```bash
sudo systemctl enable --now \
  bga-replay-archive-follow-up.timer \
  bga-replay-historical.timer
sudo systemctl list-timers --all \
  bga-replay-fresh.timer \
  bga-replay-archive-follow-up.timer \
  bga-replay-historical.timer
```

Timer frequency does not grant request capacity. Every service invocation still
uses `--limit 3`, the worker state machine, the shared lock, fresh priority, and
the persistent rolling budget. The archive follow-up lane does not change
`game_replays.queue_class`: its BGA requests remain `historical` for budgets and
metrics, and it never processes untouched historical backlog. All lanes write
to `/var/log/carcassonne/bga-replay-worker.log`.

### Admin monitoring and overrides

Global admins can open `BGA Replay Queue` in `gg-html/admin.html`. The section
shows due/scheduled queue totals, ready/error/manual-required replay counts, and
rolling-24h attempts and successful replays for `fresh`, `historical`, and
`manual` per account. Account rows also show cooldown, effective limits,
`historical_available_now`, and capacity currently protected from historical
work.

The protected API is:

```text
GET    /admin/bga-replay-budget
POST   /admin/bga-replay-budget/overrides
DELETE /admin/bga-replay-budget/overrides/{id}
```

An override can target one or all configured accounts, has a required expiry
and reason, and can change the historical boost, total limit, or both. The total
limit must be above the base total and no higher than the configured maximum;
the historical boost is dynamically bounded by the effective total. Saving a
new override revokes the previous active row for each selected account instead
of stacking values. Revoked and expired rows remain visible in the audit
history, and changes are also written to the general admin audit trail.

Before confirmation, the UI shows the resulting total/historical limits,
historical capacity available now, and fresh reserve for every selected
account. It warns when fresh work exists, the total is raised above its base, or
the protected fresh reserve is reduced. Refreshing this section performs no BGA
request.

### Automated replay regression suite

Stage 9 is covered by isolated Python and Node tests. They use temporary SQLite
databases and injected/mocked replay responses; the automated suite does not
authenticate with BGA or spend the live replay quota.

Run the Python replay and match-update tests from `auth-server`:

```bash
python3 -m unittest discover -s update_matches -t . -p 'test_*.py'
```

Run the Node schema, public-query, and admin tests in an environment with the
project's Node dependencies installed:

```bash
npm test
```

The workflow scenarios are covered as follows:

- scheduling and the five-minute delay: `test_sqlite_repository.py`,
  `test_service.py`, `test_replay_worker.py`;
- ready/fallback colors, archive retry, no polling, cache reuse, and cooldown:
  `test_game_replay.py`, `test_replay_worker.py`;
- immediate public availability of ready fallback data:
  `src/bga-replay-public.test.js`;
- per-run, rolling, concurrent, historical-reserve, account-selection, expiry,
  revoke, and override-attribution rules: `test_replay_budget.py` and
  `test_replay_worker.py`;
- exact-duel manual selection: `test_duel_game_replay_cli.py`;
- admin rolling metrics, override boundaries, replacement, revoke, and route
  protection: `src/bga-replay-admin.test.js`.

The complete suite should run before deployment and before enabling either
systemd timer. A real BGA smoke test is separate and remains limited to one
explicit request during staged rollout.

## Manual replay command

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

Each invocation performs at most one `logs.html` request for a game. If BGA has
not prepared the replay yet, the command requests the archive once and exits
without polling. A later invocation checks `logs.html` again but does not repeat
`requestTableArchive.html` when `archive_requested_at` is already stored.

The manual command uses the same persistent budget and shared account pool as
automatic replay processing. One invocation selects one account and never
cascades the same game through the remaining accounts. A replay-limit response
stops the current operation immediately and puts the selected account into a
24-hour cooldown.

Every `logs.html` attempt is reserved before HTTP and recorded in
`bga_replay_requests`, including failed and rate-limited attempts. Archive
preparation requests are audited in the same table under their own endpoint but
do not consume the `logs.html` budget. `fresh` and `manual` requests can use the
effective total limit. `historical` requests must also fit within the historical
limit and cannot consume the protected reserve.

The gateway supports one active expiring override per account. An override can
increase the historical allowance within the effective total and can separately
raise the total limit from its base value up to the configured maximum of 100.
Replacing an override revokes the previous row instead of stacking values; all
rows remain available for audit.

The database path is read from `AUTH_SQLITE_PATH`, then `DB_PATH`, and otherwise
defaults to `auth-server/data/auth.sqlite`. It can also be provided explicitly:

```bash
python3 get_game_replay.py '<games.id>' --db-path /absolute/path/to/auth.sqlite
```

To process selected replay states for every active game in every active duel of
one match, use the match primary key. Pending, error, and fallback rows require
explicit flags:

```bash
cd /home/carcassonne-gg/auth-server
./.venv/bin/python get_match_game_replays.py 'MATCH_ID' \
  --include-pending --include-errors --include-fallback --max-requests 3
```

The equivalent exact-duel command never selects games from a neighboring duel:

```bash
./.venv/bin/python get_duel_game_replays.py 'DUEL_ID' \
  --include-pending --max-requests 3
```

Ready replays with BGA colors are reused without another request. `--force`
selects all active games. Both batch commands default to three requests and
accept `--max-failed-games N`; each selected game receives at most one
`logs.html` request. A replay-limit response stops the entire command without
trying the same game through another account. Their JSON summaries distinguish
cached, deferred, failed, and remaining games.

Raw BGA logs are parsed in memory and are not stored. `events_json` contains
ordered `pickTile`, `playTile`, and `playPartisan` events. `players_json`
contains player ids, names, BGA color hex values, and normalized meeple color
names (`black`, `blue`, `green`, `red`, or `yellow`). `carcassonne_lab_url`
contains the encoded CarcassonneLab replay URL when all required moves and
player ids were found. If either player's BGA color is missing, unsupported, or
duplicates the other color, both players receive the fallback pair `red` and
`green` in first-move order. The same fallback is written to `players_json`,
`events_json`, `meeple_stats_json`, and the CarcassonneLab URL, while
`color_source = 'fallback'` preserves its provenance.

A forced refresh keeps an existing ready replay available while the BGA request
is in progress. When complete BGA colors arrive, the normalized players,
events, meeple statistics, URL, and `color_source` are replaced together. A
failed refresh leaves the ready fallback data unchanged.

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

Failures are saved as `status = 'error'` with a classified `last_error`. Every
BGA error includes the endpoint that produced it.

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
