import { ensureGameReplaysSchema } from "./bga-replay-schema.js";

const LOGS_ENDPOINT = "/archive/archive/logs.html";
const REQUEST_CLASSES = ["fresh", "historical", "manual"];

function dbAll(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => {
      if (error) reject(error);
      else resolve(rows || []);
    });
  });
}

function dbGet(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (error, row) => {
      if (error) reject(error);
      else resolve(row || null);
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

function envInteger(env, name, fallback) {
  const parsed = Number(String(env?.[name] ?? fallback).trim());
  return Number.isInteger(parsed) ? parsed : fallback;
}

function normalizeText(value) {
  const text = String(value ?? "").trim();
  return text || null;
}

function strictInteger(value, fieldName) {
  if (typeof value === "boolean" || value === null || value === undefined || value === "") {
    throw new Error(`${fieldName} must be an integer`);
  }
  const text = String(value).trim();
  if (!/^-?\d+$/.test(text)) throw new Error(`${fieldName} must be an integer`);
  const parsed = Number(text);
  if (!Number.isSafeInteger(parsed)) throw new Error(`${fieldName} must be an integer`);
  return parsed;
}

function asDate(value, fieldName = "date") {
  const date = value instanceof Date ? new Date(value.getTime()) : new Date(value);
  if (Number.isNaN(date.getTime())) throw new Error(`${fieldName} must be a valid date/time`);
  return date;
}

function sqliteTimestamp(value) {
  return asDate(value).toISOString().replace("T", " ").replace("Z", "");
}

function timestampIsActive(value, now) {
  if (!value) return false;
  const normalized = String(value).includes("T")
    ? String(value)
    : `${String(value).replace(" ", "T")}Z`;
  const timestamp = Date.parse(normalized);
  return Number.isFinite(timestamp) && timestamp > now.getTime();
}

function replayStandbyLevel(value) {
  const match = /^reserve([45]):/.exec(String(value || "").trim());
  return match ? Number(match[1]) - 3 : 0;
}

function timestampHasStarted(value, now) {
  if (!value) return false;
  const normalized = String(value).includes("T")
    ? String(value)
    : `${String(value).replace(" ", "T")}Z`;
  const timestamp = Date.parse(normalized);
  return Number.isFinite(timestamp) && timestamp <= now.getTime();
}

function maskEmail(email) {
  const raw = String(email || "").trim();
  const at = raw.indexOf("@");
  if (at < 0) return raw.length > 2 ? `${raw.slice(0, 2)}***` : "***";
  const local = raw.slice(0, at);
  const domain = raw.slice(at + 1);
  const visible = local.length > 2 ? local.slice(0, 2) : local.slice(0, 1);
  return `${visible}***@${domain}`;
}

export function getReplayBudgetLimits(env = process.env) {
  const totalLimit = Math.max(1, envInteger(env, "BGA_REPLAY_TOTAL_LIMIT", 80));
  const historicalLimit = Math.min(
    totalLimit,
    Math.max(0, envInteger(env, "BGA_REPLAY_HISTORICAL_LIMIT", 30))
  );
  const freshReserve = Math.min(
    totalLimit,
    Math.max(0, envInteger(env, "BGA_REPLAY_FRESH_RESERVE", 50))
  );
  const maxTotalLimit = Math.max(
    totalLimit,
    envInteger(env, "BGA_REPLAY_MAX_TOTAL_LIMIT", 100)
  );
  return {
    total_limit: totalLimit,
    historical_limit: historicalLimit,
    fresh_reserve: freshReserve,
    max_total_limit: maxTotalLimit,
    window_hours: 24,
  };
}

export function getConfiguredReplayAccountLabels(env = process.env) {
  const labels = [];
  const primaryEmail = normalizeText(env?.BGA_EMAIL);
  const primaryPassword = normalizeText(env?.BGA_PASSWORD);
  if (primaryEmail && primaryPassword) {
    labels.push(`primary:${maskEmail(primaryEmail)}`);
  }

  Object.keys(env || {})
    .map((key) => {
      const match = /^BGA_EMAIL_(\d+)$/.exec(key);
      return match ? [Number(match[1]), key] : null;
    })
    .filter(Boolean)
    .sort((left, right) => left[0] - right[0])
    .forEach(([index, key]) => {
      const email = normalizeText(env[key]);
      const password = normalizeText(env[`BGA_PASSWORD_${index}`]);
      if (email && password) labels.push(`reserve${index}:${maskEmail(email)}`);
    });
  return labels;
}

function calculateEffectiveLimits(baseLimits, activeOverride) {
  if (!activeOverride) {
    return {
      total_limit: baseLimits.total_limit,
      historical_limit: baseLimits.historical_limit,
      fresh_reserve: baseLimits.fresh_reserve,
      override_id: null,
      extra_historical_limit: 0,
    };
  }
  const requestedTotal = Number(activeOverride.total_limit_override);
  const totalLimit = Number.isInteger(requestedTotal)
    && requestedTotal > baseLimits.total_limit
    && requestedTotal <= baseLimits.max_total_limit
    ? requestedTotal
    : baseLimits.total_limit;
  const extraHistorical = Math.max(0, Number(activeOverride.extra_historical_limit) || 0);
  const historicalLimit = Math.min(baseLimits.historical_limit + extraHistorical, totalLimit);
  return {
    total_limit: totalLimit,
    historical_limit: historicalLimit,
    fresh_reserve: totalLimit - historicalLimit,
    override_id: Number(activeOverride.id),
    extra_historical_limit: extraHistorical,
  };
}

function emptyUsage() {
  return {
    fresh: { attempts: 0, successful: 0 },
    historical: { attempts: 0, successful: 0 },
    manual: { attempts: 0, successful: 0 },
    total_attempts: 0,
    total_successful: 0,
    override_attempts: 0,
    replay_limits: 0,
    errors: 0,
  };
}

export async function loadReplayBudgetAdminState({
  db,
  env = process.env,
  now = new Date(),
}) {
  if (!db) throw new Error("db is required");
  const currentTime = asDate(now);
  const currentTimestamp = sqliteTimestamp(currentTime);
  const baseLimits = getReplayBudgetLimits(env);
  await ensureGameReplaysSchema(db);

  const [
    knownRows,
    usageRows,
    accountStateRows,
    overrideRows,
    activeOverrideRows,
    queueRows,
    queueTotalsRow,
  ] = await Promise.all([
    dbAll(
      db,
      `
        SELECT account_label FROM bga_replay_requests
        UNION SELECT account_label FROM bga_replay_account_state
        UNION SELECT account_label FROM bga_replay_budget_overrides
      `
    ),
    dbAll(
      db,
      `
        SELECT
          account_label,
          request_class,
          COUNT(*) AS attempts,
          SUM(CASE WHEN outcome = 'ready' THEN 1 ELSE 0 END) AS successful,
          SUM(CASE WHEN budget_override_id IS NOT NULL THEN 1 ELSE 0 END) AS override_attempts,
          SUM(CASE WHEN outcome = 'replay_limit' THEN 1 ELSE 0 END) AS replay_limits,
          SUM(CASE
            WHEN outcome NOT IN ('ready', 'reserved', 'replay_limit') THEN 1
            ELSE 0
          END) AS errors
        FROM bga_replay_requests
        WHERE endpoint = ?
          AND datetime(attempted_at) > datetime(?, '-24 hours')
        GROUP BY account_label, request_class
      `,
      [LOGS_ENDPOINT, currentTimestamp]
    ),
    dbAll(db, "SELECT * FROM bga_replay_account_state"),
    dbAll(
      db,
      `
        SELECT *
        FROM bga_replay_budget_overrides
        ORDER BY datetime(created_at) DESC, id DESC
        LIMIT 100
      `
    ),
    dbAll(
      db,
      `
        SELECT *
        FROM bga_replay_budget_overrides
        WHERE revoked_at IS NULL
          AND datetime(starts_at) <= datetime(?)
          AND datetime(expires_at) > datetime(?)
        ORDER BY datetime(starts_at) DESC, id DESC
      `,
      [currentTimestamp, currentTimestamp]
    ),
    dbAll(
      db,
      `
        SELECT
          COALESCE(gr.queue_class, 'fresh') AS queue_class,
          SUM(CASE WHEN datetime(gr.next_attempt_at) <= datetime(?) THEN 1 ELSE 0 END) AS due,
          SUM(CASE WHEN datetime(gr.next_attempt_at) > datetime(?) THEN 1 ELSE 0 END) AS scheduled,
          MIN(gr.queued_at) AS oldest_queued_at
        FROM game_replays gr
        JOIN games g ON g.id = gr.game_id
        WHERE gr.retry_reason IN ('initial', 'archive', 'colors')
          AND gr.next_attempt_at IS NOT NULL
          AND gr.status IN ('pending', 'ready', 'fetching', 'error')
          AND trim(COALESCE(g.deleted_at, '')) = ''
        GROUP BY COALESCE(gr.queue_class, 'fresh')
      `,
      [currentTimestamp, currentTimestamp]
    ),
    dbGet(
      db,
      `
        SELECT
          SUM(CASE WHEN gr.status = 'ready' AND gr.color_source = 'bga' THEN 1 ELSE 0 END)
            AS ready_bga,
          SUM(CASE WHEN gr.status = 'ready' AND gr.color_source = 'fallback' THEN 1 ELSE 0 END)
            AS ready_fallback,
          SUM(CASE WHEN gr.status = 'error' THEN 1 ELSE 0 END) AS errors,
          SUM(CASE
            WHEN gr.retry_reason IS NULL
             AND (gr.status = 'error' OR (gr.status = 'ready' AND gr.color_source = 'fallback'))
            THEN 1 ELSE 0 END) AS manual_required
        FROM game_replays gr
        JOIN games g ON g.id = gr.game_id
        WHERE trim(COALESCE(g.deleted_at, '')) = ''
      `
    ),
  ]);

  const configuredLabels = getConfiguredReplayAccountLabels(env);
  const allLabels = Array.from(new Set([
    ...configuredLabels,
    ...knownRows.map((row) => normalizeText(row.account_label)).filter(Boolean),
  ]));
  const usageByAccount = new Map(allLabels.map((label) => [label, emptyUsage()]));
  usageRows.forEach((row) => {
    const label = normalizeText(row.account_label);
    const requestClass = normalizeText(row.request_class);
    if (!label || !REQUEST_CLASSES.includes(requestClass)) return;
    if (!usageByAccount.has(label)) usageByAccount.set(label, emptyUsage());
    const usage = usageByAccount.get(label);
    const attempts = Number(row.attempts) || 0;
    const successful = Number(row.successful) || 0;
    usage[requestClass] = { attempts, successful };
    usage.total_attempts += attempts;
    usage.total_successful += successful;
    usage.override_attempts += Number(row.override_attempts) || 0;
    usage.replay_limits += Number(row.replay_limits) || 0;
    usage.errors += Number(row.errors) || 0;
  });

  const stateByAccount = new Map(
    accountStateRows.map((row) => [normalizeText(row.account_label), row])
  );
  const configuredLabelsByLevel = new Map();
  configuredLabels.forEach((label) => {
    const level = replayStandbyLevel(label);
    if (!configuredLabelsByLevel.has(level)) configuredLabelsByLevel.set(level, []);
    configuredLabelsByLevel.get(level).push(label);
  });
  const standbyEligibility = new Map();
  [1, 2].forEach((level) => {
    const guardLabels = Array.from(configuredLabelsByLevel.entries())
      .filter(([candidateLevel]) => candidateLevel < level)
      .flatMap(([, labels]) => labels);
    standbyEligibility.set(
      level,
      guardLabels.length > 0 && guardLabels.every((label) => (
        timestampIsActive(stateByAccount.get(label)?.cooldown_until, currentTime)
      ))
    );
  });
  const activeOverrideByAccount = new Map();
  activeOverrideRows.forEach((row) => {
    const label = normalizeText(row.account_label);
    if (!label || activeOverrideByAccount.has(label)) return;
    activeOverrideByAccount.set(label, row);
  });

  const accounts = allLabels.map((accountLabel) => {
    const usage = usageByAccount.get(accountLabel) || emptyUsage();
    const activeOverride = activeOverrideByAccount.get(accountLabel) || null;
    const effective = calculateEffectiveLimits(baseLimits, activeOverride);
    const totalAvailable = Math.max(0, effective.total_limit - usage.total_attempts);
    const historicalAvailable = Math.max(
      0,
      Math.min(
        effective.historical_limit - usage.historical.attempts,
        effective.total_limit - effective.fresh_reserve - usage.total_attempts
      )
    );
    const accountState = stateByAccount.get(accountLabel) || {};
    const cooldownActive = timestampIsActive(accountState.cooldown_until, currentTime);
    const standbyLevel = replayStandbyLevel(accountLabel);
    return {
      account_label: accountLabel,
      configured: configuredLabels.includes(accountLabel),
      standby: standbyLevel > 0,
      standby_level: standbyLevel,
      standby_eligible: standbyLevel > 0 && standbyEligibility.get(standbyLevel) === true,
      usage,
      limits: {
        base_total_limit: baseLimits.total_limit,
        base_historical_limit: baseLimits.historical_limit,
        base_fresh_reserve: baseLimits.fresh_reserve,
        effective_total_limit: effective.total_limit,
        effective_historical_limit: effective.historical_limit,
        effective_fresh_reserve: effective.fresh_reserve,
        total_available_now: totalAvailable,
        historical_available_now: historicalAvailable,
        protected_non_historical_available_now: Math.max(0, totalAvailable - historicalAvailable),
      },
      cooldown: {
        active: cooldownActive,
        until: accountState.cooldown_until || null,
        last_limit_at: accountState.last_limit_at || null,
        last_error: accountState.last_error || null,
      },
      active_override: activeOverride ? {
        id: Number(activeOverride.id),
        extra_historical_limit: Number(activeOverride.extra_historical_limit) || 0,
        total_limit_override: activeOverride.total_limit_override == null
          ? null
          : Number(activeOverride.total_limit_override),
        starts_at: activeOverride.starts_at,
        expires_at: activeOverride.expires_at,
        created_by: activeOverride.created_by,
        reason: activeOverride.reason,
      } : null,
    };
  });

  const queue = {
    fresh: { due: 0, scheduled: 0, oldest_queued_at: null },
    historical: { due: 0, scheduled: 0, oldest_queued_at: null },
    ready_bga: Number(queueTotalsRow?.ready_bga) || 0,
    ready_fallback: Number(queueTotalsRow?.ready_fallback) || 0,
    errors: Number(queueTotalsRow?.errors) || 0,
    manual_required: Number(queueTotalsRow?.manual_required) || 0,
  };
  queueRows.forEach((row) => {
    const queueClass = normalizeText(row.queue_class);
    if (!["fresh", "historical"].includes(queueClass)) return;
    queue[queueClass] = {
      due: Number(row.due) || 0,
      scheduled: Number(row.scheduled) || 0,
      oldest_queued_at: row.oldest_queued_at || null,
    };
  });

  return {
    generated_at: currentTime.toISOString(),
    window_hours: 24,
    base_limits: baseLimits,
    accounts,
    queue,
    overrides: overrideRows.map((row) => ({
      id: Number(row.id),
      account_label: row.account_label,
      extra_historical_limit: Number(row.extra_historical_limit) || 0,
      total_limit_override: row.total_limit_override == null ? null : Number(row.total_limit_override),
      starts_at: row.starts_at,
      expires_at: row.expires_at,
      created_by: row.created_by,
      reason: row.reason,
      revoked_at: row.revoked_at || null,
      created_at: row.created_at,
      active: !row.revoked_at
        && timestampHasStarted(row.starts_at, currentTime)
        && timestampIsActive(row.expires_at, currentTime),
    })),
  };
}

export function normalizeReplayBudgetOverrideInput(payload, {
  availableAccountLabels,
  limits,
  now = new Date(),
}) {
  const allowedLabels = new Set(
    (availableAccountLabels || []).map(normalizeText).filter(Boolean)
  );
  const requestedLabels = Array.isArray(payload?.account_labels)
    ? payload.account_labels
    : [];
  const accountLabels = Array.from(new Set(requestedLabels.map(normalizeText).filter(Boolean)));
  if (!accountLabels.length) throw new Error("Select at least one replay account");
  const unknownLabels = accountLabels.filter((label) => !allowedLabels.has(label));
  if (unknownLabels.length) throw new Error(`Unknown replay account: ${unknownLabels[0]}`);

  const extraHistoricalLimit = strictInteger(
    payload?.extra_historical_limit ?? 0,
    "extra_historical_limit"
  );
  const totalRaw = payload?.total_limit_override;
  const totalLimitOverride = totalRaw === null || totalRaw === undefined || String(totalRaw).trim() === ""
    ? null
    : strictInteger(totalRaw, "total_limit_override");
  if (
    totalLimitOverride !== null
    && (totalLimitOverride <= limits.total_limit || totalLimitOverride > limits.max_total_limit)
  ) {
    throw new Error(
      `total_limit_override must be between ${limits.total_limit + 1} and ${limits.max_total_limit}`
    );
  }
  const effectiveTotal = totalLimitOverride || limits.total_limit;
  const maxExtra = effectiveTotal - limits.historical_limit;
  if (extraHistoricalLimit < 0 || extraHistoricalLimit > maxExtra) {
    throw new Error(`extra_historical_limit must be between 0 and ${maxExtra}`);
  }
  if (extraHistoricalLimit === 0 && totalLimitOverride === null) {
    throw new Error("Set a historical boost or a total-limit override");
  }

  const reason = normalizeText(payload?.reason);
  if (!reason) throw new Error("reason is required");
  if (reason.length > 500) throw new Error("reason must be 500 characters or fewer");
  const currentTime = asDate(now);
  const expiresAt = asDate(payload?.expires_at, "expires_at");
  if (expiresAt.getTime() <= currentTime.getTime()) {
    throw new Error("expires_at must be in the future");
  }
  return {
    account_labels: accountLabels,
    extra_historical_limit: extraHistoricalLimit,
    total_limit_override: totalLimitOverride,
    starts_at: sqliteTimestamp(currentTime),
    expires_at: sqliteTimestamp(expiresAt),
    reason,
  };
}

export async function replaceReplayBudgetOverrides(db, input, createdBy) {
  const actor = normalizeText(createdBy);
  if (!actor) throw new Error("created_by is required");
  await ensureGameReplaysSchema(db);
  const placeholders = input.account_labels.map(() => "?").join(", ");
  await dbRun(db, "BEGIN IMMEDIATE TRANSACTION");
  try {
    const previousOverrides = await dbAll(
      db,
      `
        SELECT * FROM bga_replay_budget_overrides
        WHERE account_label IN (${placeholders})
          AND revoked_at IS NULL
          AND datetime(expires_at) > datetime(?)
        ORDER BY id
      `,
      [...input.account_labels, input.starts_at]
    );
    await dbRun(
      db,
      `
        UPDATE bga_replay_budget_overrides
        SET revoked_at = ?
        WHERE account_label IN (${placeholders})
          AND revoked_at IS NULL
          AND datetime(expires_at) > datetime(?)
      `,
      [input.starts_at, ...input.account_labels, input.starts_at]
    );
    const overrideIds = [];
    for (const accountLabel of input.account_labels) {
      const result = await dbRun(
        db,
        `
          INSERT INTO bga_replay_budget_overrides (
            account_label, extra_historical_limit, total_limit_override,
            starts_at, expires_at, created_by, reason
          ) VALUES (?, ?, ?, ?, ?, ?, ?)
        `,
        [
          accountLabel,
          input.extra_historical_limit,
          input.total_limit_override,
          input.starts_at,
          input.expires_at,
          actor,
          input.reason,
        ]
      );
      overrideIds.push(Number(result.lastID));
    }
    const newPlaceholders = overrideIds.map(() => "?").join(", ");
    const newOverrides = await dbAll(
      db,
      `SELECT * FROM bga_replay_budget_overrides WHERE id IN (${newPlaceholders}) ORDER BY id`,
      overrideIds
    );
    await dbRun(db, "COMMIT");
    return { previous_overrides: previousOverrides, overrides: newOverrides };
  } catch (error) {
    await dbRun(db, "ROLLBACK").catch(() => {});
    throw error;
  }
}

export async function revokeReplayBudgetOverride(db, overrideId, now = new Date()) {
  const normalizedId = Number(overrideId);
  if (!Number.isInteger(normalizedId) || normalizedId <= 0) {
    throw new Error("Invalid replay budget override id");
  }
  await ensureGameReplaysSchema(db);
  const before = await dbGet(
    db,
    "SELECT * FROM bga_replay_budget_overrides WHERE id = ? LIMIT 1",
    [normalizedId]
  );
  if (!before) return null;
  if (!before.revoked_at) {
    await dbRun(
      db,
      "UPDATE bga_replay_budget_overrides SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL",
      [sqliteTimestamp(now), normalizedId]
    );
  }
  return {
    before,
    after: await dbGet(
      db,
      "SELECT * FROM bga_replay_budget_overrides WHERE id = ? LIMIT 1",
      [normalizedId]
    ),
  };
}

function auditEvent(logAuditEvent, entry) {
  if (typeof logAuditEvent !== "function") return;
  logAuditEvent(entry, () => {});
}

function actorLabel(user) {
  return normalizeText(user?.email)
    || normalizeText(user?.player_id ?? user?.bga_id)
    || (Number.isInteger(Number(user?.id)) ? `user:${Number(user.id)}` : null);
}

export function registerBgaReplayAdminRoutes(app, {
  db,
  requireAdmin,
  env = process.env,
  getAuditActor = () => ({}),
  logAuditEvent = null,
  logger = console,
  now = () => new Date(),
}) {
  if (!app || !db || typeof requireAdmin !== "function") {
    throw new Error("app, db and requireAdmin are required");
  }

  app.get("/admin/bga-replay-budget", requireAdmin, async (_req, res) => {
    try {
      const state = await loadReplayBudgetAdminState({ db, env, now: now() });
      return res.json({ ok: true, ...state });
    } catch (error) {
      logger.error?.("Failed to load BGA replay budget", error);
      return res.status(500).json({ ok: false, message: "Failed to load BGA replay budget" });
    }
  });

  app.post("/admin/bga-replay-budget/overrides", requireAdmin, async (req, res) => {
    let state;
    let input;
    const requestNow = now();
    try {
      state = await loadReplayBudgetAdminState({ db, env, now: requestNow });
    } catch (error) {
      logger.error?.("Failed to load BGA replay budget before override", error);
      return res.status(500).json({ ok: false, message: "Failed to load BGA replay budget" });
    }
    try {
      input = normalizeReplayBudgetOverrideInput(req.body || {}, {
        availableAccountLabels: state.accounts
          .filter((account) => account.configured)
          .map((account) => account.account_label),
        limits: state.base_limits,
        now: requestNow,
      });
    } catch (error) {
      return res.status(400).json({ ok: false, message: error?.message || "Invalid override" });
    }

    try {
      const result = await replaceReplayBudgetOverrides(db, input, actorLabel(req.user));
      auditEvent(logAuditEvent, {
        ...getAuditActor(req.user),
        event_type: "bga_replay_budget_override.replaced",
        entity_type: "bga_replay_budget_override",
        action: "replace",
        record_id: result.overrides.map((item) => item.id).join(","),
        changes: {
          active_overrides: {
            old: result.previous_overrides,
            new: result.overrides,
          },
        },
        metadata: {
          account_labels: input.account_labels,
          reason: input.reason,
        },
      });
      const nextState = await loadReplayBudgetAdminState({ db, env, now: now() });
      return res.status(201).json({ ok: true, ...nextState });
    } catch (error) {
      logger.error?.("Failed to replace BGA replay budget overrides", error);
      return res.status(500).json({ ok: false, message: "Failed to save replay budget override" });
    }
  });

  app.delete("/admin/bga-replay-budget/overrides/:id", requireAdmin, async (req, res) => {
    try {
      const result = await revokeReplayBudgetOverride(db, req.params?.id, now());
      if (!result) {
        return res.status(404).json({ ok: false, message: "Replay budget override not found" });
      }
      auditEvent(logAuditEvent, {
        ...getAuditActor(req.user),
        event_type: "bga_replay_budget_override.revoked",
        entity_type: "bga_replay_budget_override",
        action: "revoke",
        record_id: String(result.after.id),
        changes: {
          revoked_at: {
            old: result.before.revoked_at || null,
            new: result.after.revoked_at || null,
          },
        },
        metadata: {
          account_label: result.after.account_label,
          reason: result.after.reason,
        },
      });
      const state = await loadReplayBudgetAdminState({ db, env, now: now() });
      return res.json({ ok: true, ...state });
    } catch (error) {
      if (/Invalid replay budget override id/.test(String(error?.message || ""))) {
        return res.status(400).json({ ok: false, message: error.message });
      }
      logger.error?.("Failed to revoke BGA replay budget override", error);
      return res.status(500).json({ ok: false, message: "Failed to revoke replay budget override" });
    }
  });

  return { registered: true };
}
