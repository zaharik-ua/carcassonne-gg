import { loadCclTournament, resolveCclAssociation } from "./ccl-routes.js";

// Self-registration requests of players for a tournament (CCL first, other tournaments later).
// Admins review them and invite the player; the row itself never creates a slot.
const TOURNAMENT_REGISTRATIONS_SQL = `
  CREATE TABLE IF NOT EXISTS tournament_registrations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tournament_id TEXT NOT NULL COLLATE NOCASE,
    association_id TEXT COLLATE NOCASE,
    country_name TEXT,
    bga_nickname TEXT NOT NULL CHECK (trim(bga_nickname) <> ''),
    real_name TEXT NOT NULL CHECK (trim(real_name) <> ''),
    contact TEXT NOT NULL CHECK (trim(contact) <> ''),
    user_id INTEGER,
    player_id TEXT,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected')),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- A known association, or a country that has no association yet.
    CHECK (association_id IS NOT NULL OR trim(COALESCE(country_name, '')) <> ''),
    FOREIGN KEY (tournament_id) REFERENCES tournaments(id) ON DELETE CASCADE
  );
  -- One request per BGA account in a tournament.
  CREATE UNIQUE INDEX IF NOT EXISTS idx_tournament_registrations_nickname
    ON tournament_registrations(tournament_id COLLATE NOCASE, bga_nickname COLLATE NOCASE);
  CREATE INDEX IF NOT EXISTS idx_tournament_registrations_status
    ON tournament_registrations(tournament_id COLLATE NOCASE, status);
`;

// Same canonical association id as resolveAssociationId in server.js.
const ASSOCIATION_ID_SQL = "COALESCE(NULLIF(trim(a.code), ''), printf('ASSOCIATION_%s', a.rowid))";
const FIELD_LIMITS = { bga_nickname: 64, real_name: 100, contact: 200, country_name: 80 };

const text = (value) => String(value ?? "").trim().replace(/\s+/g, " ");
const idKey = (value) => text(value).toUpperCase();

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

function requestError(status, message, extra = {}) {
  return Object.assign(new Error(message), { status, extra });
}

export async function ensureTournamentRegistrationsSchema(db) {
  await dbExec(db, TOURNAMENT_REGISTRATIONS_SQL);
}

// Associations with their captains (profiles.team_captain = 1), keyed by canonical id.
async function loadAssociationsWithCaptains(db) {
  const [associations, captains] = await Promise.all([
    dbAll(db, `
      SELECT ${ASSOCIATION_ID_SQL} AS id, a.code, a.name, a.flag
      FROM associations a
      WHERE trim(COALESCE(a.name, '')) <> ''
      ORDER BY a.name COLLATE NOCASE ASC
    `),
    dbAll(db, `
      SELECT trim(id) AS id, bga_nickname, name, avatar, association
      FROM profiles
      WHERE COALESCE(team_captain, 0) = 1
        AND deleted_at IS NULL
        AND trim(COALESCE(association, '')) <> ''
      ORDER BY bga_nickname COLLATE NOCASE ASC
    `),
  ]);
  const list = associations.map((row) => ({
    id: text(row.id),
    name: row.name,
    flag: row.flag || null,
    captains: [],
  }));
  // profiles.association holds a code or a name.
  const byKey = new Map();
  associations.forEach((row, index) => {
    [row.code, row.id].forEach((key) => text(key) && byKey.set(idKey(key), list[index]));
    byKey.set(text(row.name).toLowerCase(), list[index]);
  });
  captains.forEach((profile) => {
    const association = byKey.get(idKey(profile.association)) || byKey.get(text(profile.association).toLowerCase());
    association?.captains.push({
      player_id: profile.id,
      bga_nickname: profile.bga_nickname || null,
      name: profile.name || null,
      avatar: profile.avatar || null,
    });
  });
  return list;
}

async function viewerCaptainAssociationId(db, user) {
  if (Number(user?.team_captain) !== 1) return null;
  return (await resolveCclAssociation(db, user?.association))?.id || null;
}

export function normalizeRegistrationInput(body) {
  const value = {};
  for (const [field, limit] of Object.entries(FIELD_LIMITS)) {
    value[field] = text(body?.[field]);
    if (value[field].length > limit) return { error: `${field} must be at most ${limit} characters` };
  }
  value.association_id = text(body?.association_id);
  if (!value.bga_nickname) return { error: "BGA account is required" };
  if (!value.real_name) return { error: "Real name is required" };
  if (!value.contact) return { error: "Contact information is required" };
  if (!value.association_id && !value.country_name) return { error: "Country is required" };
  return { value };
}

export function registerTournamentRegistrationRoutes(app, {
  db,
  requireTrustedJsonRequest = (_req, _res, next) => next(),
  getAuditActor = () => ({}),
  logAuditEvent = null,
  logger = console,
}) {
  if (!app || !db) throw new Error("app and db are required");

  const handle = (failureMessage, handler) => async (req, res) => {
    try {
      return await handler(req, res);
    } catch (error) {
      const status = Number(error?.status) || 500;
      if (status >= 500) logger.error?.(failureMessage, error);
      return res.status(status).json({
        ok: false,
        message: status >= 500 ? failureMessage : error.message,
        ...(error?.extra || {}),
      });
    }
  };

  const loadTournament = async (tournamentId) => {
    const tournament = await loadCclTournament(db, tournamentId);
    if (!tournament) throw requestError(404, "CCL tournament not found");
    return tournament;
  };

  // Public: the registration form decides per country whether captains register its players.
  app.get("/public/ccl/:tournamentId/registration", handle("Failed to load CCL registration", async (req, res) => {
    const tournament = await loadTournament(req.params.tournamentId);
    const [associations, captainAssociationId] = await Promise.all([
      loadAssociationsWithCaptains(db),
      viewerCaptainAssociationId(db, req.user),
    ]);
    return res.json({
      ok: true,
      tournament: { id: tournament.id, name: tournament.name },
      associations,
      viewer: {
        authenticated: Boolean(req.user),
        player_id: text(req.user?.player_id) || null,
        captain_association_id: captainAssociationId,
      },
    });
  }));

  // Public: players of countries without captains register themselves.
  app.post(
    "/public/ccl/:tournamentId/registrations",
    requireTrustedJsonRequest,
    handle("Failed to save CCL registration", async (req, res) => {
      const tournament = await loadTournament(req.params.tournamentId);
      const input = normalizeRegistrationInput(req.body);
      if (input.error) throw requestError(400, input.error);
      const { value } = input;

      // A typed country that is a known association counts as that association.
      let association = null;
      if (value.association_id || value.country_name) {
        association = await resolveCclAssociation(db, value.association_id || value.country_name);
        if (value.association_id && !association) throw requestError(400, `Association not found: ${value.association_id}`);
      }
      if (association) {
        const withCaptains = (await loadAssociationsWithCaptains(db))
          .find((row) => idKey(row.id) === idKey(association.id));
        if (withCaptains?.captains.length) {
          throw requestError(409, `Players from ${association.name} are registered by the association captains`, {
            association_id: association.id,
            captains: withCaptains.captains,
          });
        }
      }

      let result;
      try {
        result = await dbRun(
          db,
          `
            INSERT INTO tournament_registrations (
              tournament_id, association_id, country_name, bga_nickname, real_name, contact, user_id, player_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
          `,
          [
            tournament.id,
            association?.id || null,
            association ? null : value.country_name,
            value.bga_nickname,
            value.real_name,
            value.contact,
            Number.isInteger(Number(req.user?.id)) ? Number(req.user.id) : null,
            text(req.user?.player_id) || null,
          ]
        );
      } catch (error) {
        if (String(error?.code) === "SQLITE_CONSTRAINT" && /UNIQUE/i.test(String(error?.message))) {
          throw requestError(409, "This BGA account is already registered for the tournament");
        }
        throw error;
      }

      if (typeof logAuditEvent === "function") {
        logAuditEvent({
          ...getAuditActor(req.user),
          event_type: "tournament_registration.created",
          entity_type: "tournament_registration",
          action: "create",
          record_id: String(result.lastID),
          changes: {
            association_id: { old: null, new: association?.id || null },
            country_name: { old: null, new: association ? null : value.country_name },
            bga_nickname: { old: null, new: value.bga_nickname },
          },
          metadata: { tournament_id: tournament.id },
        }, () => {});
      }
      return res.status(201).json({
        ok: true,
        registration: {
          id: result.lastID,
          tournament_id: tournament.id,
          association_id: association?.id || null,
          association_name: association?.name || null,
          country_name: association ? null : value.country_name,
          bga_nickname: value.bga_nickname,
          status: "pending",
        },
      });
    })
  );
}
