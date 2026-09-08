import test from "node:test";
import assert from "node:assert/strict";
import sqlite3 from "sqlite3";

import { createAdminScriptRunStore } from "./admin-script-run-store.js";

function closeDatabase(db) {
  return new Promise((resolve, reject) => {
    db.close((error) => {
      if (error) reject(error);
      else resolve();
    });
  });
}

test("persists maintenance script runs and batches across store instances", async () => {
  const db = new sqlite3.Database(":memory:");
  const store = createAdminScriptRunStore(db);
  const run = {
    id: "run-1",
    script_id: "profile-bga-data",
    status: "running",
    started_at: "2026-09-08T10:00:00.000Z",
    finished_at: null,
    options: {
      batch_size: 20,
      stop_after_consecutive_failures: 5,
      include_removed: false,
      bga_data_updated_before: "2026-09-08T09:00:00.000Z",
    },
    candidate_count: 40,
    processed: 0,
    summary: null,
    error: null,
    requested_by_user_id: 7,
  };

  try {
    await store.createRun(run);
    await store.saveBatch(run.id, {
      batch: 1,
      requested: 20,
      processed: 20,
      updated: 12,
      removed: 1,
      unchanged: 6,
      failed: 1,
      results: [{ player_id: "123", ok: false, message: "BGA request failed" }],
    });
    await store.saveRun({
      ...run,
      status: "completed_with_errors",
      finished_at: "2026-09-08T10:05:00.000Z",
      processed: 20,
      summary: {
        updated: 12,
        removed: 1,
        unchanged: 6,
        failed: 1,
      },
      error: "1 player update failed.",
    });

    const reloadedStore = createAdminScriptRunStore(db);
    const latest = await reloadedStore.loadLatestRun("profile-bga-data");

    assert.equal(latest.id, "run-1");
    assert.equal(latest.status, "completed_with_errors");
    assert.equal(latest.processed, 20);
    assert.equal(latest.requested_by_user_id, 7);
    assert.deepEqual(latest.options, run.options);
    assert.deepEqual(latest.summary, {
      updated: 12,
      removed: 1,
      unchanged: 6,
      failed: 1,
    });
    assert.deepEqual(latest.batches, [{
      type: "batch",
      batch: 1,
      requested: 20,
      processed: 20,
      updated: 12,
      removed: 1,
      unchanged: 6,
      failed: 1,
      results: [{ player_id: "123", ok: false, message: "BGA request failed" }],
    }]);
  } finally {
    await closeDatabase(db);
  }
});

test("marks a persisted running job as interrupted after server restart", async () => {
  const db = new sqlite3.Database(":memory:");
  const store = createAdminScriptRunStore(db);

  try {
    await store.createRun({
      id: "run-interrupted",
      script_id: "profile-bga-data",
      status: "running",
      started_at: "2026-09-08T11:00:00.000Z",
      options: {},
      candidate_count: 100,
      processed: 20,
    });
    await store.interruptRunningRuns("profile-bga-data", "Server restarted.");

    const latest = await store.loadLatestRun("profile-bga-data");
    assert.equal(latest.status, "interrupted");
    assert.equal(latest.error, "Server restarted.");
    assert.ok(latest.finished_at);
  } finally {
    await closeDatabase(db);
  }
});
