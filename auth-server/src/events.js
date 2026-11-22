// events: schedule entries shown on the main page next to matches (stream nights, draws, deadlines...).
// Global admins manage every event, may leave the tournament empty and mark main events; tournament
// admins manage only events of the tournaments they administer, so they must pick one of them.
import { tournamentTypeSql } from "./tournament-classification.js";

export const EVENT_TITLE_MAX_LENGTH = 120;
export const EVENT_LINK_MAX_LENGTH = 500;
// The public feed keeps events this long after they start; the page shows today's and upcoming ones.
export const PUBLIC_EVENTS_LOOKBACK_DAYS = 2;

const EVENT_AUDIT_FIELDS = ["title", "time_utc", "tournament_id", "link", "icon", "main_event"];

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

export async function ensureEventsSchema(db) {
  await dbExec(db, `
    CREATE TABLE IF NOT EXISTS events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      title TEXT NOT NULL,
      time_utc TEXT NOT NULL,
      tournament_id TEXT,
      link TEXT,
      icon TEXT,
      main_event INTEGER NOT NULL DEFAULT 0 CHECK (main_event IN (0, 1)),
      created_by_user_id INTEGER,
      created_by TEXT,
      updated_by TEXT,
      deleted_by TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      deleted_at TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_events_deleted_time_utc ON events(deleted_at, time_utc);
    CREATE INDEX IF NOT EXISTS idx_events_tournament_id ON events(tournament_id COLLATE NOCASE);
  `);
}

export function isGlobalAdmin(user) {
  return Number(user?.admin) === 1;
}

// Lower-cased ids of the tournaments the user administers; global admins get null (all of them).
export async function loadManagedTournamentIds(db, user) {
  if (isGlobalAdmin(user)) return null;
  const userId = Number(user?.id);
  if (!Number.isInteger(userId) || userId <= 0) return new Set();
  const rows = await dbAll(
    db,
    `
      SELECT DISTINCT lower(trim(tournament_id)) AS tournament_id
      FROM tournament_access_users
      WHERE tournament_entity_type = 'tournament'
        AND user_id = ?
        AND lower(trim(role)) = 'admin'
        AND trim(COALESCE(tournament_id, '')) <> ''
    `,
    [userId]
  );
  return new Set(rows.map((row) => text(row.tournament_id)).filter(Boolean));
}

export function canManageEvent(managedTournamentIds, event) {
  if (managedTournamentIds === null) return true;
  const tournamentId = text(event?.tournament_id).toLowerCase();
  return !!tournamentId && managedTournamentIds.has(tournamentId);
}

function isTruthyFlag(value) {
  if (value === true || value === 1) return true;
  return ["1", "true", "yes", "on"].includes(text(value).toLowerCase());
}

export function normalizeEventTime(value) {
  const raw = text(value);
  if (!raw) return null;
  const timestamp = Date.parse(raw);
  if (!Number.isFinite(timestamp)) return null;
  return new Date(timestamp).toISOString();
}

export function normalizeEventLink(value) {
  const raw = text(value);
  if (!raw) return { value: null };
  if (raw.length > EVENT_LINK_MAX_LENGTH) {
    return { error: `Link must be ${EVENT_LINK_MAX_LENGTH} characters or fewer` };
  }
  try {
    const url = new URL(raw);
    if (url.protocol !== "https:" && url.protocol !== "http:") throw new Error("protocol");
  } catch (_error) {
    return { error: "Link must be a valid http(s) URL" };
  }
  return { value: raw };
}

// { values } ready to store, or { status, error }. `before` is the stored event on update.
export async function validateEventInput(db, body, managedTournamentIds, before = null) {
  const source = body && typeof body === "object" ? body : {};
  const has = (key) => Object.prototype.hasOwnProperty.call(source, key);

  const title = has("title") ? text(source.title) : text(before?.title);
  if (!title) return { status: 400, error: "Event name is required" };
  if (title.length > EVENT_TITLE_MAX_LENGTH) {
    return { status: 400, error: `Event name must be ${EVENT_TITLE_MAX_LENGTH} characters or fewer` };
  }

  const timeUtc = has("time_utc") ? normalizeEventTime(source.time_utc) : text(before?.time_utc);
  if (!timeUtc) return { status: 400, error: "Date and time (UTC) are required" };

  let tournamentId = has("tournament_id") ? text(source.tournament_id) || null : text(before?.tournament_id) || null;
  if (tournamentId) {
    const tournament = await dbGet(
      db,
      "SELECT id FROM tournaments WHERE upper(trim(id)) = upper(trim(?)) LIMIT 1",
      [tournamentId]
    );
    if (!tournament) return { status: 400, error: "Tournament not found" };
    tournamentId = text(tournament.id);
  }
  if (managedTournamentIds !== null) {
    if (!tournamentId) return { status: 400, error: "Choose one of your tournaments" };
    if (!managedTournamentIds.has(tournamentId.toLowerCase())) {
      return { status: 403, error: "You are not an admin of this tournament" };
    }
  }

  const link = has("link") ? normalizeEventLink(source.link) : { value: text(before?.link) || null };
  if (link.error) return { status: 400, error: link.error };

  const icon = has("icon") ? text(source.icon) || null : text(before?.icon) || null;
  if (icon && icon !== text(before?.icon)) {
    const iconRow = await dbGet(db, "SELECT id FROM icons WHERE trim(icon_link) = ? LIMIT 1", [icon]);
    if (!iconRow) return { status: 400, error: "Choose an icon from the catalog" };
  }

  // Only global admins mark main events; for tournament admins the stored value stays as it is.
  const storedMainEvent = Number(before?.main_event) === 1 ? 1 : 0;
  const mainEvent = managedTournamentIds === null && has("main_event")
    ? (isTruthyFlag(source.main_event) ? 1 : 0)
    : storedMainEvent;

  return {
    values: {
      title,
      time_utc: timeUtc,
      tournament_id: tournamentId,
      link: link.value,
      icon,
      main_event: mainEvent,
    },
  };
}

const EVENT_SELECT_SQL = `
  SELECT
    e.id,
    e.title,
    e.time_utc,
    e.tournament_id,
    e.link,
    e.icon,
    COALESCE(e.main_event, 0) AS main_event,
    e.created_by,
    e.updated_by,
    e.created_at,
    e.updated_at,
    t.name AS tournament_name,
    t.short_title AS tournament_short_title,
    t.logo AS tournament_logo,
    t.link AS tournament_link,
    CASE
      WHEN t.id IS NULL THEN NULL
      WHEN lower(${tournamentTypeSql("t")}) IN ('team', 'teams') THEN 'TEAM'
      WHEN lower(${tournamentTypeSql("t")}) IN ('individual', 'individuals') THEN 'Individual'
      ELSE COALESCE(${tournamentTypeSql("t")}, 'TEAM')
    END AS tournament_type
  FROM events e
  LEFT JOIN tournaments t
    ON upper(trim(COALESCE(t.id, ''))) = upper(trim(COALESCE(e.tournament_id, '')))
`;

export async function loadEvent(db, id) {
  return dbGet(db, `${EVENT_SELECT_SQL} WHERE e.id = ? AND e.deleted_at IS NULL LIMIT 1`, [id]);
}

function serializeEvent(row) {
  if (!row) return null;
  return { ...row, main_event: Number(row.main_event) === 1 };
}

function auditRecord(row) {
  if (!row) return {};
  return {
    title: row.title ?? null,
    time_utc: row.time_utc ?? null,
    tournament_id: row.tournament_id ?? null,
    link: row.link ?? null,
    icon: row.icon ?? null,
    main_event: Number(row.main_event) === 1 ? 1 : 0,
  };
}

function buildChanges(before, after) {
  const previous = auditRecord(before);
  const next = auditRecord(after);
  const changes = {};
  EVENT_AUDIT_FIELDS.forEach((field) => {
    const oldValue = before ? previous[field] ?? null : null;
    const newValue = after ? next[field] ?? null : null;
    if (oldValue !== newValue) changes[field] = { old: oldValue, new: newValue };
  });
  return changes;
}

function parseEventId(value) {
  const id = Number(value);
  return Number.isInteger(id) && id > 0 ? id : null;
}

export function registerEventRoutes(app, {
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

  const actorPlayerId = (user) => text(user?.player_id ?? user?.bga_id) || null;
  const audit = (req, action, eventId, changes) => {
    if (typeof logAuditEvent !== "function" || !Object.keys(changes).length) return;
    logAuditEvent({
      ...getAuditActor(req.user),
      event_type: `event.${action}d`,
      entity_type: "event",
      action,
      record_id: String(eventId),
      changes,
    }, () => {});
  };
  // Managed tournament ids, or null after answering 403 for users without access to the section.
  const loadAccess = async (req, res) => {
    const managed = await loadManagedTournamentIds(db, req.user);
    if (managed !== null && managed.size === 0) {
      res.status(403).json({ ok: false, message: "Events are available to global and tournament admins" });
      return undefined;
    }
    return managed;
  };

  app.get("/events", requireAuthenticated, async (req, res) => {
    try {
      const managed = await loadAccess(req, res);
      if (managed === undefined) return undefined;
      const rows = await dbAll(
        db,
        `${EVENT_SELECT_SQL}
          WHERE e.deleted_at IS NULL
          ORDER BY datetime(e.time_utc) DESC, e.id DESC`
      );
      return res.json({
        ok: true,
        is_admin: managed === null,
        events: rows.filter((row) => canManageEvent(managed, row)).map(serializeEvent),
      });
    } catch (error) {
      logger.error("Failed to load events", error);
      return res.status(500).json({ ok: false, message: "Failed to load events" });
    }
  });

  app.get("/events/options", requireAuthenticated, async (req, res) => {
    try {
      const managed = await loadAccess(req, res);
      if (managed === undefined) return undefined;
      const rows = await dbAll(
        db,
        `
          SELECT id, name, short_title, logo
          FROM tournaments
          WHERE trim(COALESCE(id, '')) <> ''
          ORDER BY lower(COALESCE(NULLIF(trim(name), ''), short_title, id)) ASC
        `
      );
      const tournaments = managed === null
        ? rows
        : rows.filter((row) => managed.has(text(row.id).toLowerCase()));
      return res.json({ ok: true, is_admin: managed === null, tournaments });
    } catch (error) {
      logger.error("Failed to load event options", error);
      return res.status(500).json({ ok: false, message: "Failed to load event options" });
    }
  });

  app.post("/events", requireAuthenticated, requireTrustedJsonRequest, async (req, res) => {
    try {
      const managed = await loadAccess(req, res);
      if (managed === undefined) return undefined;
      const result = await validateEventInput(db, req.body, managed);
      if (result.error) return res.status(result.status).json({ ok: false, message: result.error });
      const { values } = result;
      const insert = await dbRun(
        db,
        `
          INSERT INTO events (title, time_utc, tournament_id, link, icon, main_event, created_by_user_id, created_by, updated_by)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        `,
        [
          values.title, values.time_utc, values.tournament_id, values.link, values.icon, values.main_event,
          Number.isInteger(Number(req.user?.id)) ? Number(req.user.id) : null,
          actorPlayerId(req.user), actorPlayerId(req.user),
        ]
      );
      const created = await loadEvent(db, insert.lastID);
      audit(req, "create", insert.lastID, buildChanges(null, created));
      return res.status(201).json({ ok: true, event: serializeEvent(created) });
    } catch (error) {
      logger.error("Failed to create event", error);
      return res.status(500).json({ ok: false, message: "Failed to create event" });
    }
  });

  app.patch("/events/:id", requireAuthenticated, requireTrustedJsonRequest, async (req, res) => {
    try {
      const managed = await loadAccess(req, res);
      if (managed === undefined) return undefined;
      const eventId = parseEventId(req.params.id);
      if (!eventId) return res.status(400).json({ ok: false, message: "Invalid event id" });
      const before = await loadEvent(db, eventId);
      if (!before) return res.status(404).json({ ok: false, message: "Event not found" });
      if (!canManageEvent(managed, before)) return res.status(403).json({ ok: false, message: "Forbidden" });
      const result = await validateEventInput(db, req.body, managed, before);
      if (result.error) return res.status(result.status).json({ ok: false, message: result.error });
      const { values } = result;
      await dbRun(
        db,
        `
          UPDATE events
          SET title = ?, time_utc = ?, tournament_id = ?, link = ?, icon = ?, main_event = ?,
              updated_by = ?, updated_at = CURRENT_TIMESTAMP
          WHERE id = ? AND deleted_at IS NULL
        `,
        [
          values.title, values.time_utc, values.tournament_id, values.link, values.icon, values.main_event,
          actorPlayerId(req.user), eventId,
        ]
      );
      const updated = await loadEvent(db, eventId);
      audit(req, "update", eventId, buildChanges(before, updated));
      return res.json({ ok: true, event: serializeEvent(updated) });
    } catch (error) {
      logger.error("Failed to update event", error);
      return res.status(500).json({ ok: false, message: "Failed to update event" });
    }
  });

  app.delete("/events/:id", requireAuthenticated, async (req, res) => {
    try {
      const managed = await loadAccess(req, res);
      if (managed === undefined) return undefined;
      const eventId = parseEventId(req.params.id);
      if (!eventId) return res.status(400).json({ ok: false, message: "Invalid event id" });
      const before = await loadEvent(db, eventId);
      if (!before) return res.status(404).json({ ok: false, message: "Event not found" });
      if (!canManageEvent(managed, before)) return res.status(403).json({ ok: false, message: "Forbidden" });
      await dbRun(
        db,
        "UPDATE events SET deleted_at = CURRENT_TIMESTAMP, deleted_by = ? WHERE id = ? AND deleted_at IS NULL",
        [actorPlayerId(req.user), eventId]
      );
      audit(req, "delete", eventId, buildChanges(before, null));
      return res.json({ ok: true });
    } catch (error) {
      logger.error("Failed to delete event", error);
      return res.status(500).json({ ok: false, message: "Failed to delete event" });
    }
  });

  // Main page feed: events from the last days and upcoming ones, oldest first.
  app.get("/public/events", async (_req, res) => {
    try {
      const rows = await dbAll(
        db,
        `${EVENT_SELECT_SQL}
          WHERE e.deleted_at IS NULL
            AND datetime(e.time_utc) >= datetime('now', ?)
            AND (t.id IS NULL OR COALESCE(t.is_test, 0) = 0)
          ORDER BY datetime(e.time_utc) ASC, e.id ASC`,
        [`-${PUBLIC_EVENTS_LOOKBACK_DAYS} days`]
      );
      return res.json({
        ok: true,
        events: rows.map((row) => ({
          id: row.id,
          title: row.title,
          time_utc: row.time_utc,
          link: row.link,
          icon: row.icon,
          main_event: Number(row.main_event) === 1,
          tournament_id: row.tournament_id,
          tournament_name: row.tournament_name,
          tournament_short_title: row.tournament_short_title,
          tournament_logo: row.tournament_logo,
          tournament_link: row.tournament_link,
          tournament_type: row.tournament_type,
        })),
      });
    } catch (error) {
      logger.error("Failed to load public events", error);
      return res.status(500).json({ ok: false, message: "Failed to load events" });
    }
  });
}
