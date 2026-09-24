const SUPPORTED_BGA_COLOR_HEXES = new Set([
  "000000",
  "0000ff",
  "008000",
  "ff0000",
  "ffa500",
]);

const GAME_REPLAY_ADDITIONAL_COLUMNS = [
  ["carcassonne_lab_url", "TEXT"],
  ["board_stats_json", "TEXT"],
  ["meeple_stats_json", "TEXT"],
  ["scoring_json", "TEXT"],
  ["player_time_json", "TEXT"],
  ["next_attempt_at", "TEXT"],
  ["retry_reason", "TEXT"],
  ["queue_class", "TEXT NOT NULL DEFAULT 'fresh'"],
  ["queued_at", "TEXT"],
  ["historical_batch_id", "TEXT"],
  ["history_request_count", "INTEGER NOT NULL DEFAULT 0"],
  ["color_refresh_count", "INTEGER NOT NULL DEFAULT 0"],
  ["color_source", "TEXT"],
  ["archive_requested_at", "TEXT"],
  ["last_account_label", "TEXT"],
  ["lease_owner", "TEXT"],
  ["lease_until", "TEXT"],
];

const LEGACY_GAME_REPLAY_COLUMNS = [
  "logs_json",
  "event_count",
  "tile_count",
  "meeple_count",
  "archive_requested",
];

function quoteSqlIdentifier(identifier) {
  return `"${String(identifier || "").replaceAll('"', '""')}"`;
}

function dbAll(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => {
      if (error) reject(error);
      else resolve(rows || []);
    });
  });
}

function dbRun(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function onRun(error) {
      if (error) reject(error);
      else resolve(this);
    });
  });
}

function dbExec(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (error) => {
      if (error) reject(error);
      else resolve();
    });
  });
}

function parseJsonArray(value) {
  if (Array.isArray(value)) return value;
  if (typeof value !== "string" || !value) return null;
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function normalizeText(value) {
  const text = String(value ?? "").trim();
  return text || null;
}

function normalizeColorHex(value) {
  const color = String(value ?? "").trim().toLowerCase().replace(/^#/, "");
  return /^[0-9a-f]{6}$/.test(color) ? color : null;
}

export function inferExistingReplayColorSource(playersJson, eventsJson) {
  const players = parseJsonArray(playersJson);
  const events = parseJsonArray(eventsJson);
  if (!players || !events) return "fallback";

  const movingPlayerIds = [];
  for (const event of events) {
    if (!event || typeof event !== "object" || event.type !== "playTile") continue;
    const playerId = normalizeText(event.player_id);
    if (playerId && !movingPlayerIds.includes(playerId)) {
      movingPlayerIds.push(playerId);
    }
  }
  if (movingPlayerIds.length !== 2) return "fallback";

  const playersById = new Map();
  for (const player of players) {
    if (!player || typeof player !== "object") continue;
    const playerId = normalizeText(player.player_id);
    if (playerId) playersById.set(playerId, player);
  }

  const colors = [];
  for (const playerId of movingPlayerIds) {
    const colorHex = normalizeColorHex(playersById.get(playerId)?.color_hex);
    if (!SUPPORTED_BGA_COLOR_HEXES.has(colorHex)) return "fallback";
    colors.push(colorHex);
  }
  return new Set(colors).size === 2 ? "bga" : "fallback";
}

export async function ensureGameReplaysSchema(db) {
  await dbExec(db, `
    CREATE TABLE IF NOT EXISTS game_replays (
      game_id TEXT PRIMARY KEY,
      bga_table_id TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      events_json TEXT,
      players_json TEXT,
      carcassonne_lab_url TEXT,
      board_stats_json TEXT,
      meeple_stats_json TEXT,
      scoring_json TEXT,
      player_time_json TEXT,
      fetched_at TEXT,
      last_attempt_at TEXT,
      last_error TEXT,
      next_attempt_at TEXT,
      retry_reason TEXT,
      queue_class TEXT NOT NULL DEFAULT 'fresh',
      queued_at TEXT,
      historical_batch_id TEXT,
      history_request_count INTEGER NOT NULL DEFAULT 0,
      color_refresh_count INTEGER NOT NULL DEFAULT 0,
      color_source TEXT,
      archive_requested_at TEXT,
      last_account_label TEXT,
      lease_owner TEXT,
      lease_until TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (game_id) REFERENCES games(id)
        ON UPDATE CASCADE ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS bga_replay_budget_overrides (
      id INTEGER PRIMARY KEY,
      account_label TEXT NOT NULL,
      extra_historical_limit INTEGER NOT NULL DEFAULT 0,
      total_limit_override INTEGER,
      starts_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      expires_at TEXT NOT NULL,
      created_by TEXT NOT NULL,
      reason TEXT,
      revoked_at TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS bga_replay_requests (
      id INTEGER PRIMARY KEY,
      account_label TEXT NOT NULL,
      bga_table_id TEXT NOT NULL,
      endpoint TEXT NOT NULL,
      request_class TEXT NOT NULL,
      budget_override_id INTEGER,
      attempted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      outcome TEXT,
      error TEXT
    );

    CREATE TABLE IF NOT EXISTS bga_replay_account_state (
      account_label TEXT PRIMARY KEY,
      cooldown_until TEXT,
      last_limit_at TEXT,
      last_selected_at TEXT,
      last_error TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_bga_replay_requests_budget
      ON bga_replay_requests(account_label, endpoint, attempted_at, request_class);

    CREATE INDEX IF NOT EXISTS idx_bga_replay_requests_class_budget
      ON bga_replay_requests(account_label, endpoint, request_class, attempted_at);

    CREATE INDEX IF NOT EXISTS idx_bga_replay_budget_overrides_active
      ON bga_replay_budget_overrides(account_label, revoked_at, expires_at, starts_at);
  `);

  const columns = await dbAll(db, "PRAGMA table_info(game_replays)");
  const columnNames = new Set(columns.map((column) => column.name));
  for (const [columnName, definition] of GAME_REPLAY_ADDITIONAL_COLUMNS) {
    if (columnNames.has(columnName)) continue;
    await dbRun(
      db,
      `ALTER TABLE game_replays ADD COLUMN ${quoteSqlIdentifier(columnName)} ${definition}`
    );
    columnNames.add(columnName);
  }
  if (columnNames.has("archive_requested")) {
    await dbRun(
      db,
      `
        UPDATE game_replays
        SET archive_requested_at = COALESCE(
          archive_requested_at,
          last_attempt_at,
          updated_at,
          created_at,
          CURRENT_TIMESTAMP
        )
        WHERE archive_requested = 1
          AND archive_requested_at IS NULL
      `
    );
  }
  for (const columnName of LEGACY_GAME_REPLAY_COLUMNS) {
    if (!columnNames.has(columnName)) continue;
    await dbRun(
      db,
      `ALTER TABLE game_replays DROP COLUMN ${quoteSqlIdentifier(columnName)}`
    );
    columnNames.delete(columnName);
  }

  await dbExec(db, `
    CREATE UNIQUE INDEX IF NOT EXISTS idx_game_replays_bga_table_id
      ON game_replays(bga_table_id);

    CREATE INDEX IF NOT EXISTS idx_game_replays_due
      ON game_replays(queue_class, status, retry_reason, next_attempt_at);
  `);

  const readyRows = await dbAll(
    db,
    `
      SELECT game_id, players_json, events_json
      FROM game_replays
      WHERE status = 'ready'
        AND (color_source IS NULL OR trim(color_source) = '')
    `
  );
  // Record provenance without placing legacy replays in the automatic queue.
  for (const row of readyRows) {
    await dbRun(
      db,
      "UPDATE game_replays SET color_source = ? WHERE game_id = ?",
      [inferExistingReplayColorSource(row.players_json, row.events_json), row.game_id]
    );
  }
}
