function runAsync(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function onRun(error) {
      if (error) {
        reject(error);
        return;
      }
      resolve(this);
    });
  });
}

function getAsync(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (error, row) => {
      if (error) {
        reject(error);
        return;
      }
      resolve(row || null);
    });
  });
}

function allAsync(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => {
      if (error) {
        reject(error);
        return;
      }
      resolve(rows || []);
    });
  });
}

function stringifyJson(value) {
  if (value === undefined || value === null) return null;
  return JSON.stringify(value);
}

function parseJson(value, fallback) {
  const raw = String(value || "").trim();
  if (!raw) return fallback;
  try {
    return JSON.parse(raw);
  } catch (_error) {
    return fallback;
  }
}

function normalizeCounter(value) {
  return Math.max(0, Number(value) || 0);
}

function normalizeRunRow(row, batches) {
  if (!row) return null;
  const normalizedBatches = (Array.isArray(batches) ? batches : []).map((batch) => ({
    type: "batch",
    batch: Math.max(1, Number(batch?.batch_number) || 1),
    requested: normalizeCounter(batch?.requested),
    processed: normalizeCounter(batch?.processed),
    updated: normalizeCounter(batch?.updated),
    removed: normalizeCounter(batch?.removed),
    unchanged: normalizeCounter(batch?.unchanged),
    failed: normalizeCounter(batch?.failed),
    results: parseJson(batch?.results_json, []),
  }));
  const batchesProcessed = normalizedBatches.reduce(
    (total, batch) => total + normalizeCounter(batch.processed),
    0
  );

  return {
    id: String(row.id || ""),
    script_id: String(row.script_id || ""),
    status: String(row.status || ""),
    started_at: row.started_at || null,
    finished_at: row.finished_at || null,
    options: parseJson(row.options_json, {}),
    candidate_count: normalizeCounter(row.candidate_count),
    batches: normalizedBatches,
    processed: Math.max(normalizeCounter(row.processed), batchesProcessed),
    summary: parseJson(row.summary_json, null),
    error: row.error || null,
    requested_by_user_id: row.requested_by_user_id == null
      ? null
      : Number(row.requested_by_user_id),
  };
}

export function createAdminScriptRunStore(db) {
  if (!db) throw new TypeError("SQLite database is required");
  let schemaPromise = null;

  const ensureSchema = () => {
    if (schemaPromise) return schemaPromise;
    schemaPromise = (async () => {
      await runAsync(db, `
        CREATE TABLE IF NOT EXISTS admin_script_runs (
          id TEXT PRIMARY KEY,
          script_id TEXT NOT NULL,
          status TEXT NOT NULL,
          started_at TEXT NOT NULL,
          finished_at TEXT,
          options_json TEXT,
          candidate_count INTEGER NOT NULL DEFAULT 0,
          processed INTEGER NOT NULL DEFAULT 0,
          summary_json TEXT,
          error TEXT,
          requested_by_user_id INTEGER,
          created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
      `);
      await runAsync(db, `
        CREATE TABLE IF NOT EXISTS admin_script_run_batches (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          run_id TEXT NOT NULL,
          batch_number INTEGER NOT NULL,
          requested INTEGER NOT NULL DEFAULT 0,
          processed INTEGER NOT NULL DEFAULT 0,
          updated INTEGER NOT NULL DEFAULT 0,
          removed INTEGER NOT NULL DEFAULT 0,
          unchanged INTEGER NOT NULL DEFAULT 0,
          failed INTEGER NOT NULL DEFAULT 0,
          results_json TEXT,
          created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          UNIQUE(run_id, batch_number),
          FOREIGN KEY (run_id) REFERENCES admin_script_runs(id) ON DELETE CASCADE
        )
      `);
      await runAsync(
        db,
        "CREATE INDEX IF NOT EXISTS idx_admin_script_runs_latest ON admin_script_runs(script_id, started_at DESC)"
      );
      await runAsync(
        db,
        "CREATE INDEX IF NOT EXISTS idx_admin_script_run_batches_run ON admin_script_run_batches(run_id, batch_number)"
      );
    })().catch((error) => {
      schemaPromise = null;
      throw error;
    });
    return schemaPromise;
  };

  const createRun = async (run) => {
    await ensureSchema();
    await runAsync(
      db,
      `
        INSERT INTO admin_script_runs (
          id,
          script_id,
          status,
          started_at,
          finished_at,
          options_json,
          candidate_count,
          processed,
          summary_json,
          error,
          requested_by_user_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
      [
        String(run?.id || ""),
        String(run?.script_id || ""),
        String(run?.status || "running"),
        run?.started_at || new Date().toISOString(),
        run?.finished_at || null,
        stringifyJson(run?.options || {}),
        normalizeCounter(run?.candidate_count),
        normalizeCounter(run?.processed),
        stringifyJson(run?.summary),
        run?.error || null,
        Number.isInteger(Number(run?.requested_by_user_id)) && Number(run?.requested_by_user_id) > 0
          ? Number(run.requested_by_user_id)
          : null,
      ]
    );
  };

  const saveRun = async (run) => {
    await ensureSchema();
    await runAsync(
      db,
      `
        UPDATE admin_script_runs
        SET
          status = ?,
          finished_at = ?,
          options_json = ?,
          candidate_count = ?,
          processed = ?,
          summary_json = ?,
          error = ?,
          updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
      `,
      [
        String(run?.status || "failed"),
        run?.finished_at || null,
        stringifyJson(run?.options || {}),
        normalizeCounter(run?.candidate_count),
        normalizeCounter(run?.processed),
        stringifyJson(run?.summary),
        run?.error || null,
        String(run?.id || ""),
      ]
    );
  };

  const saveBatch = async (runId, batch) => {
    await ensureSchema();
    await runAsync(
      db,
      `
        INSERT INTO admin_script_run_batches (
          run_id,
          batch_number,
          requested,
          processed,
          updated,
          removed,
          unchanged,
          failed,
          results_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id, batch_number) DO UPDATE SET
          requested = excluded.requested,
          processed = excluded.processed,
          updated = excluded.updated,
          removed = excluded.removed,
          unchanged = excluded.unchanged,
          failed = excluded.failed,
          results_json = excluded.results_json,
          updated_at = CURRENT_TIMESTAMP
      `,
      [
        String(runId || ""),
        Math.max(1, Number(batch?.batch) || 1),
        normalizeCounter(batch?.requested),
        normalizeCounter(batch?.processed),
        normalizeCounter(batch?.updated),
        normalizeCounter(batch?.removed),
        normalizeCounter(batch?.unchanged),
        normalizeCounter(batch?.failed),
        stringifyJson(Array.isArray(batch?.results) ? batch.results : []),
      ]
    );
  };

  const loadRun = async (runRow) => {
    if (!runRow) return null;
    const batches = await allAsync(
      db,
      `
        SELECT *
        FROM admin_script_run_batches
        WHERE run_id = ?
        ORDER BY batch_number ASC
      `,
      [runRow.id]
    );
    return normalizeRunRow(runRow, batches);
  };

  const loadLatestRun = async (scriptId) => {
    await ensureSchema();
    const row = await getAsync(
      db,
      `
        SELECT *
        FROM admin_script_runs
        WHERE script_id = ?
        ORDER BY datetime(started_at) DESC, rowid DESC
        LIMIT 1
      `,
      [String(scriptId || "")]
    );
    return loadRun(row);
  };

  const interruptRunningRuns = async (scriptId, message) => {
    await ensureSchema();
    await runAsync(
      db,
      `
        UPDATE admin_script_runs
        SET
          status = 'interrupted',
          finished_at = COALESCE(finished_at, CURRENT_TIMESTAMP),
          error = COALESCE(NULLIF(error, ''), ?),
          updated_at = CURRENT_TIMESTAMP
        WHERE script_id = ?
          AND status = 'running'
      `,
      [
        String(message || "The server stopped before the maintenance script finished."),
        String(scriptId || ""),
      ]
    );
  };

  return {
    ensureSchema,
    createRun,
    saveRun,
    saveBatch,
    loadLatestRun,
    interruptRunningRuns,
  };
}
