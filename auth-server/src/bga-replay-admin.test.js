import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import sqlite3 from "sqlite3";

import {
  getConfiguredReplayAccountLabels,
  getReplayBudgetLimits,
  loadReplayBudgetAdminState,
  normalizeReplayBudgetOverrideInput,
  registerBgaReplayAdminRoutes,
  replaceReplayBudgetOverrides,
  revokeReplayBudgetOverride,
} from "./bga-replay-admin.js";
import { ensureGameReplaysSchema } from "./bga-replay-schema.js";

function exec(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (error) => (error ? reject(error) : resolve()));
  });
}

function all(db, sql) {
  return new Promise((resolve, reject) => {
    db.all(sql, (error, rows) => (error ? reject(error) : resolve(rows || [])));
  });
}

function close(db) {
  return new Promise((resolve) => db.close(resolve));
}

const ENV = {
  BGA_EMAIL: "primary@example.com",
  BGA_PASSWORD: "secret",
  BGA_EMAIL_2: "reserve2@example.com",
  BGA_PASSWORD_2: "secret-2",
  BGA_EMAIL_3: "reserve3@example.com",
  BGA_PASSWORD_3: "secret-3",
  BGA_EMAIL_4: "reserve4@example.com",
  BGA_PASSWORD_4: "secret-4",
  BGA_EMAIL_5: "reserve5@example.com",
  BGA_PASSWORD_5: "secret-5",
  BGA_REPLAY_TOTAL_LIMIT: "80",
  BGA_REPLAY_FRESH_RESERVE: "50",
  BGA_REPLAY_HISTORICAL_LIMIT: "30",
  BGA_REPLAY_MAX_TOTAL_LIMIT: "100",
};

async function createDatabase(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => close(db));
  await exec(db, `
    CREATE TABLE games (
      id TEXT PRIMARY KEY,
      bga_table_id TEXT,
      deleted_at TEXT
    );
  `);
  await ensureGameReplaysSchema(db);
  return db;
}

test("uses the same masked labels and configurable limits as the replay gateway", () => {
  assert.deepEqual(getConfiguredReplayAccountLabels(ENV), [
    "primary:pr***@example.com",
    "reserve2:re***@example.com",
    "reserve3:re***@example.com",
    "reserve4:re***@example.com",
    "reserve5:re***@example.com",
  ]);
  assert.deepEqual(getReplayBudgetLimits(ENV), {
    total_limit: 80,
    historical_limit: 30,
    fresh_reserve: 50,
    max_total_limit: 100,
    window_hours: 24,
  });
});

test("activates standby tiers only after every earlier configured tier is cooling down", async (t) => {
  const db = await createDatabase(t);
  const now = new Date("2026-09-24T12:00:00Z");
  const labels = getConfiguredReplayAccountLabels(ENV);
  const firstStandbyLabel = labels[3];
  const secondStandbyLabel = labels[4];

  for (const accountLabel of labels.slice(0, 3)) {
    await new Promise((resolve, reject) => {
      db.run(`
        INSERT INTO bga_replay_account_state (account_label, cooldown_until)
        VALUES (?, '2026-09-24 13:00:00')
      `, [accountLabel], (error) => (error ? reject(error) : resolve()));
    });
  }

  let state = await loadReplayBudgetAdminState({ db, env: ENV, now });
  let firstStandby = state.accounts.find((item) => item.account_label === firstStandbyLabel);
  let secondStandby = state.accounts.find((item) => item.account_label === secondStandbyLabel);
  assert.equal(firstStandby.standby_level, 1);
  assert.equal(firstStandby.standby_eligible, true);
  assert.equal(secondStandby.standby_level, 2);
  assert.equal(secondStandby.standby_eligible, false);

  await new Promise((resolve, reject) => {
    db.run(`
      INSERT INTO bga_replay_account_state (account_label, cooldown_until)
      VALUES (?, '2026-09-24 13:00:00')
    `, [firstStandbyLabel], (error) => (error ? reject(error) : resolve()));
  });

  state = await loadReplayBudgetAdminState({ db, env: ENV, now });
  secondStandby = state.accounts.find((item) => item.account_label === secondStandbyLabel);
  assert.equal(secondStandby.standby_eligible, true);
});

test("activates the first standby when accounts 1-3 have zero available", async (t) => {
  const db = await createDatabase(t);
  const now = new Date("2026-09-24T12:00:00Z");
  const env = {
    ...ENV,
    BGA_REPLAY_TOTAL_LIMIT: "1",
    BGA_REPLAY_FRESH_RESERVE: "0",
    BGA_REPLAY_HISTORICAL_LIMIT: "1",
    BGA_REPLAY_MAX_TOTAL_LIMIT: "1",
  };
  const labels = getConfiguredReplayAccountLabels(env);

  for (const [index, accountLabel] of labels.slice(0, 3).entries()) {
    await new Promise((resolve, reject) => {
      db.run(`
        INSERT INTO bga_replay_requests (
          account_label, bga_table_id, endpoint, request_class, attempted_at, outcome
        ) VALUES (?, ?, '/archive/archive/logs.html', 'manual',
          '2026-09-24 11:00:00', 'success')
      `, [accountLabel, `primary-budget-${index}`], (error) => (
        error ? reject(error) : resolve()
      ));
    });
  }

  const state = await loadReplayBudgetAdminState({ db, env, now });
  const firstStandby = state.accounts.find((item) => item.account_label === labels[3]);
  const secondStandby = state.accounts.find((item) => item.account_label === labels[4]);
  assert.equal(firstStandby.standby_eligible, true);
  assert.equal(secondStandby.standby_eligible, false);
});

test("activates the second standby when every earlier account is cooling down or has zero available", async (t) => {
  const db = await createDatabase(t);
  const now = new Date("2026-09-24T12:00:00Z");
  const env = {
    ...ENV,
    BGA_REPLAY_TOTAL_LIMIT: "1",
    BGA_REPLAY_FRESH_RESERVE: "0",
    BGA_REPLAY_HISTORICAL_LIMIT: "1",
    BGA_REPLAY_MAX_TOTAL_LIMIT: "1",
  };
  const labels = getConfiguredReplayAccountLabels(env);

  for (const accountLabel of labels.slice(0, 3)) {
    await new Promise((resolve, reject) => {
      db.run(`
        INSERT INTO bga_replay_account_state (account_label, cooldown_until)
        VALUES (?, '2026-09-24 13:00:00')
      `, [accountLabel], (error) => (error ? reject(error) : resolve()));
    });
  }
  await new Promise((resolve, reject) => {
    db.run(`
      INSERT INTO bga_replay_requests (
        account_label, bga_table_id, endpoint, request_class, attempted_at, outcome
      ) VALUES (?, 'standby-budget', '/archive/archive/logs.html', 'manual',
        '2026-09-24 11:00:00', 'success')
    `, [labels[3]], (error) => (error ? reject(error) : resolve()));
  });

  const state = await loadReplayBudgetAdminState({ db, env, now });
  const secondStandby = state.accounts.find((item) => item.account_label === labels[4]);
  assert.equal(secondStandby.standby_eligible, true);
});

test("validates dynamic override limits, accounts, expiry and reason", () => {
  const limits = getReplayBudgetLimits(ENV);
  const now = new Date("2026-09-24T12:00:00Z");
  const basePayload = {
    account_labels: ["account-a"],
    expires_at: "2026-09-24T14:00:00Z",
    reason: "Reviewed catch-up",
  };
  const normalized = normalizeReplayBudgetOverrideInput({
    ...basePayload,
    extra_historical_limit: 20,
    total_limit_override: 100,
  }, {
    availableAccountLabels: ["account-a", "account-b"],
    limits,
    now,
  });
  assert.equal(normalized.extra_historical_limit, 20);
  assert.equal(normalized.total_limit_override, 100);
  assert.equal(normalized.starts_at, "2026-09-24 12:00:00.000");

  for (const extraHistoricalLimit of [10, 20, 50]) {
    const preset = normalizeReplayBudgetOverrideInput({
      ...basePayload,
      extra_historical_limit: extraHistoricalLimit,
    }, { availableAccountLabels: ["account-a"], limits, now });
    assert.equal(preset.extra_historical_limit, extraHistoricalLimit);
    assert.equal(preset.total_limit_override, null);
  }
  const maximumWithTotalOverride = normalizeReplayBudgetOverrideInput({
    ...basePayload,
    extra_historical_limit: 70,
    total_limit_override: 100,
  }, { availableAccountLabels: ["account-a"], limits, now });
  assert.equal(maximumWithTotalOverride.extra_historical_limit, 70);

  for (const totalLimitOverride of [81, 100]) {
    const boundary = normalizeReplayBudgetOverrideInput({
      ...basePayload,
      extra_historical_limit: 0,
      total_limit_override: totalLimitOverride,
    }, { availableAccountLabels: ["account-a"], limits, now });
    assert.equal(boundary.total_limit_override, totalLimitOverride);
  }
  for (const totalLimitOverride of [-1, 80, 101, 81.5]) {
    assert.throws(() => normalizeReplayBudgetOverrideInput({
      ...basePayload,
      extra_historical_limit: 0,
      total_limit_override: totalLimitOverride,
    }, { availableAccountLabels: ["account-a"], limits, now }), /integer|between/);
  }
  assert.throws(() => normalizeReplayBudgetOverrideInput({
    ...basePayload,
    extra_historical_limit: 71,
    total_limit_override: 100,
  }, { availableAccountLabels: ["account-a"], limits, now }), /between 0 and 70/);
  assert.throws(() => normalizeReplayBudgetOverrideInput({
    ...basePayload,
    extra_historical_limit: -1,
  }, { availableAccountLabels: ["account-a"], limits, now }), /between 0 and 50/);
  assert.throws(() => normalizeReplayBudgetOverrideInput({
    ...basePayload,
    extra_historical_limit: 1.5,
  }, { availableAccountLabels: ["account-a"], limits, now }), /integer/);
  assert.throws(() => normalizeReplayBudgetOverrideInput({
    account_labels: ["unknown"],
    extra_historical_limit: 10,
    expires_at: "2026-09-24T14:00:00Z",
    reason: "Unknown",
  }, { availableAccountLabels: ["account-a"], limits, now }), /Unknown replay account/);
  assert.throws(() => normalizeReplayBudgetOverrideInput({
    account_labels: ["account-a"],
    extra_historical_limit: 10,
    expires_at: "2026-09-24T11:00:00Z",
    reason: "Expired",
  }, { availableAccountLabels: ["account-a"], limits, now }), /future/);
});

test("builds rolling account metrics, queue summary and effective availability", async (t) => {
  const db = await createDatabase(t);
  const now = new Date("2026-09-24T12:00:00Z");
  const account = getConfiguredReplayAccountLabels(ENV)[0];
  const reserveAccount = getConfiguredReplayAccountLabels(ENV)[1];
  await exec(db, `
    INSERT INTO games VALUES ('fresh-due', '101', NULL);
    INSERT INTO games VALUES ('historical-scheduled', '102', NULL);
    INSERT INTO games VALUES ('fallback', '103', NULL);

    INSERT INTO game_replays (
      game_id, bga_table_id, status, retry_reason, queue_class,
      queued_at, next_attempt_at
    ) VALUES
      ('fresh-due', '101', 'pending', 'initial', 'fresh',
       '2026-09-24 10:00:00', '2026-09-24 11:00:00'),
      ('historical-scheduled', '102', 'pending', 'initial', 'historical',
       '2026-09-24 10:30:00', '2026-09-24 13:00:00');
    INSERT INTO game_replays (
      game_id, bga_table_id, status, retry_reason, queue_class,
      color_source, color_refresh_count
    ) VALUES ('fallback', '103', 'ready', NULL, 'fresh', 'fallback', 1);
  `);
  await new Promise((resolve, reject) => {
    const rows = [
      [account, "101", "fresh", "ready", null],
      [account, "102", "historical", "temporary_error", null],
      [account, "103", "manual", "ready", 1],
    ];
    const statement = db.prepare(`
      INSERT INTO bga_replay_requests (
        account_label, bga_table_id, endpoint, request_class,
        outcome, budget_override_id, attempted_at
      ) VALUES (?, ?, '/archive/archive/logs.html', ?, ?, ?, '2026-09-24 11:30:00')
    `);
    rows.forEach((row) => statement.run(row));
    statement.finalize((error) => (error ? reject(error) : resolve()));
  });
  await exec(db, `
    INSERT INTO bga_replay_budget_overrides (
      account_label, extra_historical_limit, total_limit_override,
      starts_at, expires_at, created_by, reason
    ) VALUES
      (
        '${account}', 20, 100,
        '2026-09-24 11:00:00', '2026-09-24 14:00:00', 'admin', 'catch up'
      ),
      (
        '${reserveAccount}', 20, NULL,
        '2026-09-24 11:00:00', '2026-09-24 14:00:00', 'admin', 'boost only'
      );
    INSERT INTO bga_replay_account_state (
      account_label, cooldown_until, last_limit_at, last_error
    ) VALUES (
      '${account}', '2026-09-24 13:00:00', '2026-09-24 12:00:00', 'limit'
    );
  `);

  const state = await loadReplayBudgetAdminState({ db, env: ENV, now });
  const primary = state.accounts.find((item) => item.account_label === account);
  assert.deepEqual(primary.usage.fresh, { attempts: 1, successful: 1 });
  assert.deepEqual(primary.usage.historical, { attempts: 1, successful: 0 });
  assert.deepEqual(primary.usage.manual, { attempts: 1, successful: 1 });
  assert.equal(primary.limits.effective_total_limit, 100);
  assert.equal(primary.limits.effective_historical_limit, 50);
  assert.equal(primary.limits.historical_available_now, 47);
  assert.equal(primary.limits.protected_non_historical_available_now, 50);
  assert.equal(primary.cooldown.active, true);
  const reserve = state.accounts.find((item) => item.account_label === reserveAccount);
  assert.equal(reserve.limits.effective_total_limit, 80);
  assert.equal(reserve.limits.effective_historical_limit, 50);
  assert.equal(reserve.limits.effective_fresh_reserve, 30);
  assert.equal(state.queue.fresh.due, 1);
  assert.equal(state.queue.historical.scheduled, 1);
  assert.equal(state.queue.ready_fallback, 1);
  assert.equal(state.queue.manual_required, 1);
});

test("replacement revokes active rows while retaining history and supports revoke", async (t) => {
  const db = await createDatabase(t);
  const first = {
    account_labels: ["account-a", "account-b"],
    extra_historical_limit: 10,
    total_limit_override: null,
    starts_at: "2026-09-24 12:00:00.000",
    expires_at: "2026-09-24 14:00:00.000",
    reason: "first",
  };
  const created = await replaceReplayBudgetOverrides(db, first, "admin@example.com");
  assert.equal(created.overrides.length, 2);

  const replacement = await replaceReplayBudgetOverrides(db, {
    ...first,
    account_labels: ["account-a"],
    extra_historical_limit: 20,
    starts_at: "2026-09-24 12:30:00.000",
    reason: "replacement",
  }, "admin@example.com");
  assert.equal(replacement.previous_overrides.length, 1);
  const rows = await all(db, `
    SELECT account_label, extra_historical_limit, revoked_at
    FROM bga_replay_budget_overrides
    ORDER BY id
  `);
  assert.equal(rows.length, 3);
  assert.ok(rows[0].revoked_at);
  assert.equal(rows[1].revoked_at, null);
  assert.equal(rows[2].extra_historical_limit, 20);

  const revoked = await revokeReplayBudgetOverride(
    db,
    replacement.overrides[0].id,
    new Date("2026-09-24T13:00:00Z")
  );
  assert.equal(revoked.after.revoked_at, "2026-09-24 13:00:00.000");
});

test("registers global-admin routes and exposes the BGA Replay Queue in admin.html", () => {
  const routes = [];
  const app = {};
  ["get", "post", "delete"].forEach((method) => {
    app[method] = (path, ...handlers) => routes.push({ method, path, handlers });
  });
  const requireAdmin = () => {};
  registerBgaReplayAdminRoutes(app, {
    db: {},
    requireAdmin,
    logger: { error() {} },
  });
  assert.deepEqual(
    routes.map((route) => [route.method, route.path]),
    [
      ["get", "/admin/bga-replay-budget"],
      ["post", "/admin/bga-replay-budget/overrides"],
      ["delete", "/admin/bga-replay-budget/overrides/:id"],
    ]
  );
  routes.forEach((route) => assert.equal(route.handlers[0], requireAdmin));

  const adminHtml = readFileSync(new URL("../../gg-html/admin.html", import.meta.url), "utf8");
  assert.match(adminHtml, /title: "BGA Replay Queue"/);
  assert.match(adminHtml, /historical_available_now/);
  assert.match(adminHtml, /protected_non_historical_available_now/);
  assert.match(adminHtml, /cooldown or 0 available/);
  assert.match(adminHtml, /Apply this replay budget override/);
  assert.match(adminHtml, /Fresh work exists/);
  assert.match(adminHtml, /method: "DELETE"/);
});
