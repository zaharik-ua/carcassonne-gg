# Update Matches Migration

Self-contained module for moving match result updates from Google Sheets to a server-side job backed by a database.

What is included:
- BGA authenticated HTTP session handling via Selenium
- Batch match result fetching from BGA
- Server-side update service that reads matches from a repository and writes update results back
- Repository interface for plugging into another project's DB layer
- JSON-file repository example for local testing
- CLI script for scheduled execution

What is intentionally not included:
- Concrete DB/ORM code from the other project
- Flask endpoints
- Google Sheets code

## Expected Match Input

Each match to update should provide:

- `match_id`: internal DB identifier
- `player0`: BGA nickname of first player
- `player1`: BGA nickname of second player
- `player0_id`: optional BGA numeric id
- `player1_id`: optional BGA numeric id
- `game_id`: BGA game id
- `start_date`: unix timestamp in seconds
- `end_date`: optional unix timestamp in seconds
- `gtw`: wins needed to finish the series, default `2`
- `stat`: optional boolean, default `false`

## Expected Persisted Output

For successful updates:
- `wins0`
- `wins1`
- `flags`
- `players_url`
- `table_urls`
- `tables_json`
- `last_error = null`
- `updated_at`

For failed updates:
- `last_error`
- `updated_at`

## Integration Contract

Implement `MatchRepository` from [update_matches/repository.py](/Users/sergeyzakharenko/Documents/visualstudio/carcassonne-server-helper/update_matches_migration/update_matches/repository.py).

Minimal integration steps in the target project:

1. Copy this folder into the target project.
2. Implement a repository adapter for that project's DB.
3. Wire that adapter into `run_update_matches.py`.
4. Configure cron/systemd/Celery/worker scheduler to run the script.

## Environment

Required:
- `BGA_EMAIL`
- `BGA_PASSWORD`

Optional:
- `BGA_HTTP_WORKERS=6`
- `BGA_HTTP_TIMEOUT_SECONDS=20`
- `BGA_HTTP_MIN_INTERVAL_MS=250`
- `BGA_TOKEN_TTL_SECONDS=86400`
- `BGA_PLAYER_ID_CACHE_FILE=.cache/player_id_cache.json`
- `MATCH_UPDATE_BATCH_SIZE=20`
- `MATCH_UPDATE_TARGETS=ongoing,empty_finished`
- `MATCH_UPDATE_LIMIT=100`

Selenium / Chrome:
- `CHROMEDRIVER_PATH`
- `CHROME_BINARY_PATH`
- `DRIVER_IDLE_SECONDS`
- `CHROME_STARTUP_TIMEOUT`
- `DRIVER_ACQUIRE_TIMEOUT`
- `SELENIUM_MAX_QUEUE`
- `CHROME_PAGE_LOAD_TIMEOUT`

## Local Smoke Test With JSON Repository

Create a JSON file with `matches` array and point the CLI to it:

```json
{
  "matches": [
    {
      "match_id": 1,
      "target": "ongoing",
      "player0": "player_a",
      "player1": "player_b",
      "game_id": 1,
      "start_date": 1735689600,
      "end_date": 1736294400,
      "gtw": 2,
      "stat": false
    }
  ]
}
```

Run:

```bash
python run_update_matches.py --repository json --json-path ./matches.json
```

## Suggested DB Adapter Responsibilities

- Select matches that need update for a target:
  - `ongoing`
  - `empty_finished`
- Convert DB rows to `MatchUpdateRequest`
- Persist success result fields
- Persist error text
- Ensure already-updated rows stop matching the selection query

## Scheduling

Examples:

```bash
*/2 * * * * cd /path/to/project/update_matches_migration && /path/to/venv/bin/python run_update_matches.py
```

```bash
0 * * * * cd /path/to/project/update_matches_migration && /path/to/venv/bin/python run_update_matches.py --targets ongoing
```
