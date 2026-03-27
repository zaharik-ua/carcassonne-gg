# Migration Folder

Self-contained code bundles for moving Google Sheets driven BGA logic into another server project.

Included flows:
- `update_matches`: update match results from BGA and persist them into a DB
- `update_player_elo`: update current player Elo from BGA and persist it into a DB

What is intentionally not included:
- concrete DB/ORM code from the target project
- Flask endpoints
- Google Sheets code

## Player Elo Migration

This flow replaces the behavior from `google_scritps/updatePlayersElo.gs`, but runs on the server side and writes into a repository adapter instead of a sheet.

Included files:
- `run_update_player_elo.py`
- `update_player_elo/bga_client.py`
- `update_player_elo/service.py`
- `update_player_elo/repository.py`
- `update_player_elo/db_repository_template.py`
- `update_player_elo/json_repository.py`
- `update_player_elo/models.py`
- `update_player_elo/cli.py`

### Expected Player Input

Each player row should provide:
- `player_id`: internal DB identifier
- `bga_player_id`: numeric BGA player id

Optional fields can stay in the row and will be preserved by the JSON test adapter.

### Expected Persisted Output

For successful updates:
- `elo`
- `elo_raw`
- `elo_url`
- `last_error = null`
- `updated_at`

For failed updates:
- `last_error`
- `updated_at`

### Integration Contract

Implement `PlayerEloRepository` from [repository.py](/Users/sergeyzakharenko/Documents/visualstudio/carcassonne-server-helper/migration_folder/update_player_elo/repository.py).

Minimal integration steps in the target project:

1. Copy `migration_folder/update_player_elo` and `migration_folder/run_update_player_elo.py`.
2. Replace [db_repository_template.py](/Users/sergeyzakharenko/Documents/visualstudio/carcassonne-server-helper/migration_folder/update_player_elo/db_repository_template.py) with the real DB adapter.
3. Wire that adapter into [cli.py](/Users/sergeyzakharenko/Documents/visualstudio/carcassonne-server-helper/migration_folder/update_player_elo/cli.py) or call the service directly from your worker.
4. Configure cron/systemd/Celery/worker scheduler to run the script.

### Environment

Optional:
- `PLAYER_ELO_BATCH_SIZE=80`
- `PLAYER_ELO_HTTP_TIMEOUT_SECONDS=20`
- `PLAYER_ELO_MIN_INTERVAL_MS=250`
- `PLAYER_ELO_LIMIT=100`
- `PLAYER_ELO_JSON_PATH=./players.json`

### Local Smoke Test With JSON Repository

Create a JSON file with `players` array:

```json
{
  "players": [
    {
      "player_id": 1,
      "bga_player_id": 85016225,
      "needs_update": true
    }
  ]
}
```

Run:

```bash
python run_update_player_elo.py --repository json --json-path ./players.json
```

## Match Result Migration

This is the existing server-side match update bundle.

Included files:
- `run_update_matches.py`
- `update_matches/...`

### Expected Match Input

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

### Expected Persisted Output

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

### Match Integration Contract

Implement `MatchRepository` from [repository.py](/Users/sergeyzakharenko/Documents/visualstudio/carcassonne-server-helper/migration_folder/update_matches/repository.py).

Minimal integration steps in the target project:

1. Copy `migration_folder/update_matches` and `migration_folder/run_update_matches.py`.
2. Implement a repository adapter for that project's DB.
3. Wire that adapter into [cli.py](/Users/sergeyzakharenko/Documents/visualstudio/carcassonne-server-helper/migration_folder/update_matches/cli.py).
4. Configure cron/systemd/Celery/worker scheduler to run the script.

### Match Environment

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

### Match Local Smoke Test With JSON Repository

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
