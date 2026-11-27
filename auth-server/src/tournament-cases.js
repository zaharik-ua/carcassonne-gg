// tournament_cases: problems, complaints, requests and feedback players send to tournament admins.
// The type says what the case is, the category what it is about; the UI shows only their labels.

export const TOURNAMENT_CASE_TYPES = new Set(["problem", "complaint", "request", "feedback", "other"]);
export const TOURNAMENT_CASE_STATUSES = new Set(["open", "in_progress", "resolved", "closed"]);
export const TOURNAMENT_CASE_PRIORITIES = new Set(["low", "normal", "high", "urgent"]);
export const OPEN_TOURNAMENT_CASE_STATUSES = ["open", "in_progress"];
export const CASE_DETAILS_MAX_LENGTH = 5000;
// Between the texts of repeated submissions of one case; the dashboard shows them as separate parts.
export const CASE_DETAILS_SEPARATOR = "\n--\n";

// Categories of each type. A type without categories stores category = NULL.
// complaint/no_show is a no-show report (Rivals and CCL); reported_player_id holds the player who did not show up.
export const TOURNAMENT_CASE_CATEGORIES = {
  problem: ["scheduling", "not_played", "incomplete", "result_dispute"],
  complaint: ["conduct", "no_show"],
  request: ["general"],
  feedback: [],
  other: [],
};

export const TOURNAMENT_CASE_TYPE_LABELS = {
  problem: "Match problem",
  complaint: "Complaint",
  request: "Request",
  feedback: "Feedback",
  other: "Other",
};

export const TOURNAMENT_CASE_CATEGORY_LABELS = {
  scheduling: "Scheduling the match time",
  not_played: "Match was not played",
  incomplete: "Match not finished",
  result_dispute: "Wrong match result",
  conduct: "Rule violation or opponent's behaviour",
  no_show: "No-show",
  general: "Other question for admins",
};

// What a player may choose in a case without a match (the tournament Rules page).
export const NON_MATCH_CASE_CHOICES = [
  { case_type: "request", category: "general" },
  { case_type: "complaint", category: "conduct" },
  { case_type: "feedback", category: null },
];

// Case types and categories about a match (GET /tournament-cases/choices).
export const MATCH_CASE_CHOICES = [
  ...TOURNAMENT_CASE_CATEGORIES.problem.map((category) => ({ case_type: "problem", category })),
  { case_type: "complaint", category: "conduct" },
  { case_type: "request", category: "general" },
  { case_type: "feedback", category: null },
];

const text = (value) => String(value ?? "").trim();

function dbGet(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (error, row) => (error ? reject(error) : resolve(row || null)));
  });
}

function dbAll(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows || [])));
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
    db.exec(sql, (error) => (error ? reject(error) : resolve()));
  });
}

const TABLE_SQL = (name) => `
  CREATE TABLE IF NOT EXISTS ${name} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    case_type TEXT NOT NULL DEFAULT 'problem',
    category TEXT,
    status TEXT NOT NULL DEFAULT 'open',
    priority TEXT NOT NULL DEFAULT 'normal',
    details TEXT NOT NULL DEFAULT '',
    submitted_by_user_id INTEGER,
    submitted_by_player_id TEXT NOT NULL,
    responsible_user_id INTEGER,
    reported_player_id TEXT,
    match_id TEXT,
    duel_id TEXT,
    tournament_id TEXT,
    challenge_period_id TEXT,
    related_entity_type TEXT,
    related_entity_id TEXT,
    resolution TEXT,
    resolved_at TEXT,
    deleted_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (submitted_by_user_id) REFERENCES users(id) ON DELETE SET NULL,
    FOREIGN KEY (submitted_by_player_id) REFERENCES profiles(id) ON DELETE RESTRICT,
    FOREIGN KEY (responsible_user_id) REFERENCES users(id) ON DELETE SET NULL,
    FOREIGN KEY (reported_player_id) REFERENCES profiles(id) ON DELETE SET NULL,
    FOREIGN KEY (match_id) REFERENCES matches(id) ON DELETE SET NULL,
    FOREIGN KEY (duel_id) REFERENCES duels(id) ON DELETE SET NULL,
    FOREIGN KEY (tournament_id) REFERENCES tournaments(id) ON DELETE SET NULL,
    FOREIGN KEY (challenge_period_id) REFERENCES challenge_periods(id) ON DELETE SET NULL,
    CHECK (case_type IN ('problem', 'complaint', 'request', 'feedback', 'other')),
    CHECK (status IN ('open', 'in_progress', 'resolved', 'closed')),
    CHECK (priority IN ('low', 'normal', 'high', 'urgent'))
  )
`;

const INDEXES_SQL = `
  CREATE INDEX IF NOT EXISTS idx_tournament_cases_status_priority
    ON tournament_cases(status, priority, created_at DESC);
  CREATE INDEX IF NOT EXISTS idx_tournament_cases_submitted_by
    ON tournament_cases(submitted_by_player_id, created_at DESC);
  CREATE INDEX IF NOT EXISTS idx_tournament_cases_responsible
    ON tournament_cases(responsible_user_id, status, created_at DESC);
  CREATE INDEX IF NOT EXISTS idx_tournament_cases_duel
    ON tournament_cases(duel_id, created_at DESC);
  CREATE INDEX IF NOT EXISTS idx_tournament_cases_match
    ON tournament_cases(match_id, created_at DESC);
  CREATE INDEX IF NOT EXISTS idx_tournament_cases_tournament
    ON tournament_cases(tournament_id, created_at DESC);
  CREATE INDEX IF NOT EXISTS idx_tournament_cases_challenge_period
    ON tournament_cases(challenge_period_id, created_at DESC);
  CREATE INDEX IF NOT EXISTS idx_tournament_cases_related_entity
    ON tournament_cases(related_entity_type, related_entity_id, created_at DESC);
`;

const KEPT_COLUMNS = [
  "id", "case_type", "category", "status", "priority", "details",
  "submitted_by_user_id", "submitted_by_player_id", "responsible_user_id", "reported_player_id",
  "match_id", "duel_id", "tournament_id", "challenge_period_id",
  "related_entity_type", "related_entity_id",
  "resolution", "resolved_at", "deleted_at", "created_at", "updated_at",
];

// The first version had subject and no 'feedback' type; an interim version also dropped
// reported_player_id. SQLite cannot drop a column or change a CHECK in place, so the table is rebuilt:
// subject is dropped without moving its data (details already holds the text), reported_player_id is
// kept, or added back empty.
async function needsRebuild(db) {
  const table = await dbGet(db, "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'tournament_cases'");
  if (!table) return false;
  const columns = new Set((await dbAll(db, "PRAGMA table_info(tournament_cases)")).map((column) => column.name));
  return columns.has("subject") || !columns.has("reported_player_id") || !String(table.sql || "").includes("'feedback'");
}

async function rebuildTable(db) {
  const columns = new Set((await dbAll(db, "PRAGMA table_info(tournament_cases)")).map((column) => column.name));
  const copied = KEPT_COLUMNS.filter((column) => columns.has(column)).join(", ");
  await dbExec(db, "BEGIN IMMEDIATE TRANSACTION");
  try {
    // The match id sync trigger updates this table; ensureMatchIdReferenceSync re-creates it after the schema.
    await dbExec(db, `
      DROP TRIGGER IF EXISTS trg_matches_sync_references_after_id_update;
      DROP TABLE IF EXISTS tournament_cases__rebuilt;
      ${TABLE_SQL("tournament_cases__rebuilt")};
      INSERT INTO tournament_cases__rebuilt (${copied}) SELECT ${copied} FROM tournament_cases;
      DROP TABLE tournament_cases;
      ALTER TABLE tournament_cases__rebuilt RENAME TO tournament_cases;
    `);
    await dbExec(db, "COMMIT");
  } catch (error) {
    await dbExec(db, "ROLLBACK").catch(() => {});
    throw error;
  }
}

export async function ensureTournamentCasesSchema(db) {
  if (await needsRebuild(db)) await rebuildTable(db);
  await dbExec(db, `${TABLE_SQL("tournament_cases")};\n${INDEXES_SQL}`);
}

// { case_type, category } when the pair is one of `choices`, else { error }.
export function normalizeCaseChoice(input, choices) {
  const caseType = text(input?.case_type).toLowerCase();
  const category = text(input?.category).toLowerCase() || null;
  const choice = choices.find((entry) => entry.case_type === caseType && (entry.category || null) === category);
  if (!choice) return { error: caseType ? "Choose a valid case category" : "Choose a case type" };
  return { case_type: choice.case_type, category: choice.category || null };
}

// Trimmed details, or { error } when empty or too long.
export function normalizeCaseDetails(value) {
  const details = text(value);
  if (!details) return { error: "Details are required" };
  if (details.length > CASE_DETAILS_MAX_LENGTH) {
    return { error: `Details must be ${CASE_DETAILS_MAX_LENGTH.toLocaleString("en-US")} characters or fewer` };
  }
  return { value: details };
}

export async function loadTournamentCase(db, id) {
  return dbGet(db, "SELECT * FROM tournament_cases WHERE id = ? LIMIT 1", [id]);
}

// Creates a case. For a case about a duel, the player's open case of the same category on that
// duel takes the new details instead (one open case per duel, player and category).
// Returns { case, appended, before }; call it inside the caller's transaction.
export async function createOrAppendTournamentCase(db, fields) {
  const duelId = text(fields.duel_id) || null;
  const category = fields.category || null;
  const playerId = text(fields.submitted_by_player_id);
  if (duelId) {
    const existing = await dbGet(
      db,
      `
        SELECT * FROM tournament_cases
        WHERE deleted_at IS NULL
          AND duel_id = ?
          AND submitted_by_player_id = ?
          AND COALESCE(category, '') = COALESCE(?, '')
          AND status IN (${OPEN_TOURNAMENT_CASE_STATUSES.map(() => "?").join(", ")})
        ORDER BY id DESC
        LIMIT 1
      `,
      [duelId, playerId, category, ...OPEN_TOURNAMENT_CASE_STATUSES]
    );
    if (existing) {
      const joined = [text(existing.details), fields.details].filter(Boolean).join(CASE_DETAILS_SEPARATOR);
      await dbRun(
        db,
        "UPDATE tournament_cases SET details = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        [joined, existing.id]
      );
      return { case: await loadTournamentCase(db, existing.id), appended: true, before: existing };
    }
  }
  const result = await dbRun(
    db,
    `
      INSERT INTO tournament_cases (
        case_type, category, status, priority, details,
        submitted_by_user_id, submitted_by_player_id, reported_player_id,
        match_id, duel_id, tournament_id, challenge_period_id,
        created_at, updated_at
      )
      VALUES (?, ?, 'open', 'normal', ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    `,
    [
      fields.case_type, category, fields.details,
      fields.submitted_by_user_id ?? null, playerId, text(fields.reported_player_id) || null,
      text(fields.match_id) || null, duelId, text(fields.tournament_id) || null,
      text(fields.challenge_period_id) || null,
    ]
  );
  return { case: await loadTournamentCase(db, result.lastID), appended: false, before: null };
}

// What the API returns about a case the player just sent.
export function formatSubmittedCase(row) {
  if (!row) return null;
  return {
    id: row.id,
    case_type: row.case_type,
    category: row.category || null,
    type_label: TOURNAMENT_CASE_TYPE_LABELS[row.case_type] || row.case_type,
    category_label: row.category ? TOURNAMENT_CASE_CATEGORY_LABELS[row.category] || row.category : null,
    status: row.status,
    details: row.details,
    reported_player_id: row.reported_player_id || null,
    tournament_id: row.tournament_id || null,
    duel_id: row.duel_id || null,
    created_at: row.created_at,
    updated_at: row.updated_at,
  };
}

export function caseAuditEntry(caseResult, metadata = {}) {
  return {
    event_type: caseResult.appended ? "tournament_case.updated" : "tournament_case.created",
    entity_type: "tournament_case",
    action: caseResult.appended ? "update" : "create",
    record_id: String(caseResult.case?.id ?? ""),
    changes: caseResult.appended
      ? { details: { old: caseResult.before?.details ?? null, new: caseResult.case?.details ?? null } }
      : Object.fromEntries(Object.entries(caseResult.case || {}).map(([key, value]) => [key, { old: null, new: value }])),
    metadata,
  };
}

const actorPlayerId = (user) => text(user?.player_id ?? user?.bga_id);
const actorUserId = (user) => (Number.isInteger(Number(user?.id)) && Number(user?.id) > 0 ? Number(user.id) : null);

// Global admins manage every case; tournament admins (role = admin in tournament_access_users)
// manage the cases of their tournaments. Cases without a tournament (Rivals) are for global admins.
export async function canManageTournamentCase(db, user, row) {
  if (!row) return false;
  if (Number(user?.admin) === 1) return true;
  const userId = actorUserId(user);
  const tournamentId = text(row.tournament_id);
  if (!userId || !tournamentId) return false;
  const access = await dbGet(
    db,
    `
      SELECT 1 FROM tournament_access_users
      WHERE tournament_entity_type = 'tournament'
        AND user_id = ?
        AND lower(trim(role)) = 'admin'
        AND upper(trim(tournament_id)) = upper(trim(?))
      LIMIT 1
    `,
    [userId, tournamentId]
  );
  return !!access;
}

function caseChangeAuditEntry(before, after, action) {
  const changes = {};
  Object.keys(after || {}).forEach((key) => {
    if ((before?.[key] ?? null) !== (after?.[key] ?? null)) changes[key] = { old: before?.[key] ?? null, new: after[key] ?? null };
  });
  return {
    event_type: `tournament_case.${action === "delete" ? "deleted" : "updated"}`,
    entity_type: "tournament_case",
    action,
    record_id: String(after?.id ?? before?.id ?? ""),
    changes,
    metadata: { tournament_id: before?.tournament_id || null },
  };
}

// POST /tournaments/:tournamentId/cases — a case without a match, from the tournament Rules page.
// POST /tournament-cases/:caseId/close — an admin closes a case (status = closed).
// DELETE /tournament-cases/:caseId — an admin soft-deletes a case (deleted_at is set).
export function registerTournamentCaseRoutes(app, {
  db,
  requireAuthenticated,
  requireTrustedJsonRequest = (_req, _res, next) => next(),
  getAuditActor = () => ({}),
  logAuditEvent = null,
  logger = console,
}) {
  if (!app || !db || typeof requireAuthenticated !== "function") {
    throw new Error("app, db and requireAuthenticated are required");
  }

  app.get("/tournament-cases/choices", (_req, res) => {
    const label = (choice) => ({
      ...choice,
      type_label: TOURNAMENT_CASE_TYPE_LABELS[choice.case_type],
      category_label: choice.category ? TOURNAMENT_CASE_CATEGORY_LABELS[choice.category] : null,
    });
    res.json({ ok: true, non_match: NON_MATCH_CASE_CHOICES.map(label), match: MATCH_CASE_CHOICES.map(label) });
  });

  app.post("/tournaments/:tournamentId/cases", requireAuthenticated, requireTrustedJsonRequest, async (req, res) => {
    try {
      const playerId = actorPlayerId(req.user);
      if (!playerId) {
        return res.status(403).json({ ok: false, code: "profile_required", message: "Link your BGA profile to contact the admins" });
      }
      const tournament = await dbGet(
        db,
        "SELECT id FROM tournaments WHERE upper(trim(id)) = upper(trim(?)) LIMIT 1",
        [text(req.params.tournamentId)]
      );
      if (!tournament) return res.status(404).json({ ok: false, message: "Tournament not found" });
      const choice = normalizeCaseChoice(req.body, NON_MATCH_CASE_CHOICES);
      if (choice.error) return res.status(400).json({ ok: false, message: choice.error });
      const details = normalizeCaseDetails(req.body?.details);
      if (details.error) return res.status(400).json({ ok: false, message: details.error });

      const created = await createOrAppendTournamentCase(db, {
        ...choice,
        details: details.value,
        submitted_by_user_id: actorUserId(req.user),
        submitted_by_player_id: playerId,
        tournament_id: tournament.id,
      });
      if (typeof logAuditEvent === "function") {
        logAuditEvent({ ...getAuditActor(req.user), ...caseAuditEntry(created, { tournament_id: tournament.id }) }, () => {});
      }
      return res.status(201).json({ ok: true, case: formatSubmittedCase(created.case), appended: created.appended });
    } catch (error) {
      logger.error?.("Failed to create tournament case", error);
      return res.status(500).json({ ok: false, message: "Failed to send the case" });
    }
  });

  // The case an admin may change, or null after sending the error response.
  async function loadManagedCase(req, res) {
    const caseId = Number(req.params?.caseId);
    if (!Number.isInteger(caseId) || caseId <= 0) {
      res.status(400).json({ ok: false, message: "Invalid case id" });
      return null;
    }
    const row = await loadTournamentCase(db, caseId);
    if (!row || row.deleted_at) {
      res.status(404).json({ ok: false, message: "Case not found" });
      return null;
    }
    if (!(await canManageTournamentCase(db, req.user, row))) {
      res.status(403).json({ ok: false, message: "Only admins of this tournament can change the case" });
      return null;
    }
    return row;
  }

  const logChange = (req, before, after, action) => {
    if (typeof logAuditEvent === "function") {
      logAuditEvent({ ...getAuditActor(req.user), ...caseChangeAuditEntry(before, after, action) }, () => {});
    }
  };

  app.post("/tournament-cases/:caseId/close", requireAuthenticated, requireTrustedJsonRequest, async (req, res) => {
    try {
      const before = await loadManagedCase(req, res);
      if (!before) return undefined;
      if (before.status === "closed") return res.json({ ok: true, case: formatSubmittedCase(before) });
      await dbRun(
        db,
        `
          UPDATE tournament_cases
          SET status = 'closed', resolved_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
          WHERE id = ? AND deleted_at IS NULL
        `,
        [before.id]
      );
      const after = await loadTournamentCase(db, before.id);
      logChange(req, before, after, "update");
      return res.json({ ok: true, case: formatSubmittedCase(after) });
    } catch (error) {
      logger.error?.("Failed to close tournament case", error);
      return res.status(500).json({ ok: false, message: "Failed to close the case" });
    }
  });

  app.delete("/tournament-cases/:caseId", requireAuthenticated, requireTrustedJsonRequest, async (req, res) => {
    try {
      const before = await loadManagedCase(req, res);
      if (!before) return undefined;
      await dbRun(
        db,
        "UPDATE tournament_cases SET deleted_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND deleted_at IS NULL",
        [before.id]
      );
      const after = await loadTournamentCase(db, before.id);
      logChange(req, before, after, "delete");
      return res.json({ ok: true, id: before.id });
    } catch (error) {
      logger.error?.("Failed to delete tournament case", error);
      return res.status(500).json({ ok: false, message: "Failed to delete the case" });
    }
  });
}
