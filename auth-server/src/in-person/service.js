import { randomUUID } from "node:crypto";
import { getPlayoffPreview } from "./constants.js";
import { TOURNAMENT_ENTITY_TYPES } from "./schema.js";
import {
  conflictError,
  InPersonError,
  normalizeAdminUserIds,
  normalizeCityInput,
  normalizeOptionalText,
  normalizeTournamentInput,
  normalizeText,
  notFoundError,
  validationError,
} from "./validation.js";

function dbAll(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows || [])));
  });
}

function dbGet(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (error, row) => (error ? reject(error) : resolve(row || null)));
  });
}

function dbRun(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function onRun(error) {
      if (error) reject(error);
      else resolve({ changes: Number(this?.changes || 0), lastID: this?.lastID });
    });
  });
}

function mapDatabaseError(error) {
  if (error instanceof InPersonError) return error;
  const message = String(error?.message || "");
  if (message.includes("in_person_tournaments.slug")) {
    return conflictError("DUPLICATE_SLUG", "An in-person tournament with this slug already exists");
  }
  if (
    message.includes("idx_cities_active_association_name_en")
    || (message.includes("cities.association_id") && message.includes("cities.name_en"))
  ) {
    return conflictError("DUPLICATE_CITY", "An active city with this English name already exists in the association");
  }
  return error;
}

function serializeCity(row) {
  if (!row) return null;
  return {
    id: row.id,
    association_id: row.association_id,
    association_name: row.association_name || null,
    association_flag: row.association_flag || null,
    name_en: row.name_en,
    name_local: row.name_local || null,
    archived: !!row.archived_at,
    archived_at: row.archived_at || null,
    created_at: row.created_at,
    updated_at: row.updated_at,
  };
}

function serializeTournament(row, admins = []) {
  if (!row) return null;
  return {
    id: row.id,
    slug: row.slug,
    name_en: row.name_en,
    name_local: row.name_local || null,
    scope: row.scope,
    association_id: row.association_id || null,
    association_name: row.association_name || null,
    association_flag: row.association_flag || null,
    local_subtype: row.local_subtype || null,
    qualifier_city_id: row.qualifier_city_id || null,
    qualifier_city_name_en: row.qualifier_city_name_en || null,
    qualifier_city_name_local: row.qualifier_city_name_local || null,
    start_date: row.start_date,
    end_date: row.end_date,
    organizer_name: row.organizer_name,
    organizer_url: row.organizer_url || null,
    rules_url: row.rules_url || null,
    swiss_rounds_count: Number(row.swiss_rounds_count),
    playoff_first_round: row.playoff_first_round,
    playoff_preview: getPlayoffPreview(row.playoff_first_round),
    draw_mode: row.draw_mode,
    swiss_tiebreak_profile: row.swiss_tiebreak_profile,
    status: row.status,
    revision: Number(row.revision),
    published_at: row.published_at || null,
    completed_at: row.completed_at || null,
    cancelled_at: row.cancelled_at || null,
    has_started_swiss: Number(row.has_started_swiss) === 1,
    admins,
    admin_user_ids: admins.map((admin) => admin.user_id),
    created_at: row.created_at,
    updated_at: row.updated_at,
  };
}

const TOURNAMENT_SELECT = `
  SELECT
    t.*,
    a.name AS association_name,
    a.flag AS association_flag,
    c.name_en AS qualifier_city_name_en,
    c.name_local AS qualifier_city_name_local,
    CASE WHEN EXISTS (
      SELECT 1
      FROM in_person_rounds r
      WHERE r.tournament_id = t.id
        AND r.stage = 'swiss'
        AND r.status <> 'cancelled'
    ) THEN 1 ELSE 0 END AS has_started_swiss
  FROM in_person_tournaments t
  LEFT JOIN associations a
    ON upper(trim(a.code)) = upper(trim(t.association_id))
  LEFT JOIN cities c ON c.id = t.qualifier_city_id
`;

const CITY_SELECT = `
  SELECT
    c.*,
    a.name AS association_name,
    a.flag AS association_flag
  FROM cities c
  JOIN associations a
    ON upper(trim(a.code)) = upper(trim(c.association_id))
`;

export function createInPersonService({ db, idFactory = randomUUID } = {}) {
  if (!db) throw new Error("db is required");
  let mutationQueue = Promise.resolve();

  function enqueueMutation(task) {
    const result = mutationQueue.then(task, task);
    mutationQueue = result.catch(() => {});
    return result;
  }

  async function transaction(task) {
    await dbRun(db, "BEGIN IMMEDIATE TRANSACTION");
    try {
      const result = await task();
      await dbRun(db, "COMMIT");
      return result;
    } catch (error) {
      try {
        await dbRun(db, "ROLLBACK");
      } catch {
        // Preserve the original error.
      }
      throw mapDatabaseError(error);
    }
  }

  async function findAssociation(associationId) {
    const row = await dbGet(
      db,
      `SELECT code, name, flag FROM associations WHERE upper(trim(code)) = upper(trim(?)) LIMIT 1`,
      [associationId]
    );
    if (!row) {
      throw validationError("UNKNOWN_ASSOCIATION", "association_id must reference an existing association", {
        field: "association_id",
      });
    }
    return row;
  }

  async function validateCityRelations(input, current = null, { requireActive = false } = {}) {
    const association = await findAssociation(input.association_id);
    if (
      current
      && normalizeText(current.association_id).toUpperCase() !== normalizeText(association.code).toUpperCase()
    ) {
      const reference = await dbGet(
        db,
        `
          SELECT 1 AS used
          FROM in_person_tournaments
          WHERE qualifier_city_id = ?
          UNION ALL
          SELECT 1 AS used
          FROM in_person_participants
          WHERE city_id = ?
          LIMIT 1
        `,
        [current.id, current.id]
      );
      if (reference) {
        throw conflictError(
          "CITY_ASSOCIATION_LOCKED",
          "The association cannot be changed because this city is already in use"
        );
      }
    }
    if (requireActive && current?.archived_at) {
      throw conflictError("CITY_ARCHIVED", "An archived city cannot be selected for new data");
    }
    return { ...input, association_id: association.code };
  }

  async function validateTournamentRelations(input, current = null, { requireActiveCity = false } = {}) {
    let association = null;
    if (input.association_id) {
      association = await findAssociation(input.association_id);
      input = { ...input, association_id: association.code };
    }
    if (input.qualifier_city_id) {
      const city = await dbGet(db, `SELECT * FROM cities WHERE id = ? LIMIT 1`, [input.qualifier_city_id]);
      if (!city) {
        throw validationError("UNKNOWN_QUALIFIER_CITY", "qualifier_city_id must reference an existing city", {
          field: "qualifier_city_id",
        });
      }
      const sameCurrentCity = current && current.qualifier_city_id === city.id;
      if (city.archived_at && (requireActiveCity || !sameCurrentCity)) {
        throw conflictError("CITY_ARCHIVED", "An archived city cannot be selected for a tournament");
      }
      if (normalizeText(city.association_id).toUpperCase() !== normalizeText(association?.code).toUpperCase()) {
        throw validationError(
          "QUALIFIER_CITY_ASSOCIATION_MISMATCH",
          "Qualifier city must belong to the tournament association",
          { field: "qualifier_city_id" }
        );
      }
    }
    return input;
  }

  async function validateAdminUsers(userIds) {
    if (!userIds.length) return [];
    const rows = await dbAll(
      db,
      `SELECT id FROM users WHERE id IN (${userIds.map(() => "?").join(", ")})`,
      userIds
    );
    const found = new Set(rows.map((row) => Number(row.id)));
    const missing = userIds.filter((userId) => !found.has(userId));
    if (missing.length) {
      throw validationError("UNKNOWN_ADMIN_USER", "Every tournament admin must reference an existing user", {
        user_ids: missing,
      });
    }
    return userIds;
  }

  async function replaceAdminsInTransaction(tournamentId, userIds) {
    await validateAdminUsers(userIds);
    await dbRun(
      db,
      `
        DELETE FROM tournament_access_users
        WHERE tournament_entity_type = ? AND tournament_id = ?
      `,
      [TOURNAMENT_ENTITY_TYPES.IN_PERSON_TOURNAMENT, tournamentId]
    );
    for (const userId of userIds) {
      await dbRun(
        db,
        `
          INSERT INTO tournament_access_users (
            tournament_entity_type, tournament_id, user_id, role, created_at, updated_at
          ) VALUES (?, ?, ?, 'admin', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        `,
        [TOURNAMENT_ENTITY_TYPES.IN_PERSON_TOURNAMENT, tournamentId, userId]
      );
    }
  }

  async function loadAdmins(tournamentId = null) {
    const params = [TOURNAMENT_ENTITY_TYPES.IN_PERSON_TOURNAMENT];
    let filter = "";
    if (tournamentId) {
      filter = "AND tau.tournament_id = ?";
      params.push(tournamentId);
    }
    return dbAll(
      db,
      `
        SELECT
          tau.tournament_id,
          tau.user_id,
          'admin' AS role,
          u.name,
          u.email,
          p.bga_nickname
        FROM tournament_access_users tau
        JOIN users u ON u.id = tau.user_id
        LEFT JOIN profiles p ON p.id = u.bga_id
        WHERE tau.tournament_entity_type = ?
          ${filter}
        ORDER BY
          lower(COALESCE(NULLIF(trim(u.name), ''), NULLIF(trim(p.bga_nickname), ''), NULLIF(trim(u.email), ''), CAST(u.id AS TEXT))),
          u.id
      `,
      params
    ).then((rows) => rows.map((row) => ({
      tournament_id: row.tournament_id,
      user_id: Number(row.user_id),
      role: "admin",
      name: row.name || null,
      email: row.email || null,
      bga_nickname: row.bga_nickname || null,
    })));
  }

  async function getTournamentRow(tournamentId) {
    return dbGet(db, `${TOURNAMENT_SELECT} WHERE t.id = ? LIMIT 1`, [tournamentId]);
  }

  async function requireTournamentRow(tournamentId) {
    const normalizedId = normalizeText(tournamentId);
    if (!normalizedId) throw validationError("INVALID_TOURNAMENT_ID", "Invalid in-person tournament id");
    const row = await getTournamentRow(normalizedId);
    if (!row) throw notFoundError("TOURNAMENT_NOT_FOUND", "In-person tournament not found");
    return row;
  }

  async function getTournament(tournamentId) {
    const row = await requireTournamentRow(tournamentId);
    const admins = await loadAdmins(row.id);
    return serializeTournament(row, admins);
  }

  async function listTournaments() {
    const [rows, admins] = await Promise.all([
      dbAll(db, `${TOURNAMENT_SELECT} ORDER BY t.start_date DESC, lower(t.name_en), t.id`),
      loadAdmins(),
    ]);
    const adminsByTournament = new Map();
    admins.forEach((admin) => {
      const list = adminsByTournament.get(admin.tournament_id) || [];
      list.push(admin);
      adminsByTournament.set(admin.tournament_id, list);
    });
    return rows.map((row) => serializeTournament(row, adminsByTournament.get(row.id) || []));
  }

  async function listCities({ associationId = null, includeArchived = false } = {}) {
    const filters = [];
    const params = [];
    if (normalizeOptionalText(associationId)) {
      filters.push("upper(trim(c.association_id)) = upper(trim(?))");
      params.push(normalizeText(associationId));
    }
    if (!includeArchived) filters.push("c.archived_at IS NULL");
    const where = filters.length ? `WHERE ${filters.join(" AND ")}` : "";
    const rows = await dbAll(
      db,
      `${CITY_SELECT} ${where} ORDER BY lower(a.name), lower(c.name_en), c.id`,
      params
    );
    return rows.map(serializeCity);
  }

  async function getCity(cityId) {
    const row = await dbGet(db, `${CITY_SELECT} WHERE c.id = ? LIMIT 1`, [normalizeText(cityId)]);
    if (!row) throw notFoundError("CITY_NOT_FOUND", "City not found");
    return serializeCity(row);
  }

  async function createCity(payload) {
    const input = normalizeCityInput(payload);
    return enqueueMutation(() => transaction(async () => {
      const validated = await validateCityRelations(input);
      const cityId = `city_${idFactory()}`;
      await dbRun(
        db,
        `
          INSERT INTO cities (
            id, association_id, name_en, name_local, created_at, updated_at
          ) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        `,
        [cityId, validated.association_id, validated.name_en, validated.name_local]
      );
      return getCity(cityId);
    }));
  }

  async function updateCity(cityId, payload) {
    return enqueueMutation(() => transaction(async () => {
      const current = await dbGet(db, `SELECT * FROM cities WHERE id = ? LIMIT 1`, [normalizeText(cityId)]);
      if (!current) throw notFoundError("CITY_NOT_FOUND", "City not found");
      const input = await validateCityRelations(normalizeCityInput(payload, current), current);
      await dbRun(
        db,
        `
          UPDATE cities
          SET association_id = ?, name_en = ?, name_local = ?, updated_at = CURRENT_TIMESTAMP
          WHERE id = ?
        `,
        [input.association_id, input.name_en, input.name_local, current.id]
      );
      return getCity(current.id);
    }));
  }

  async function setCityArchived(cityId, archived) {
    return enqueueMutation(() => transaction(async () => {
      const current = await dbGet(db, `SELECT * FROM cities WHERE id = ? LIMIT 1`, [normalizeText(cityId)]);
      if (!current) throw notFoundError("CITY_NOT_FOUND", "City not found");
      if (!archived) {
        await validateCityRelations(normalizeCityInput(current, current), current);
      }
      await dbRun(
        db,
        `
          UPDATE cities
          SET archived_at = ${archived ? "COALESCE(archived_at, CURRENT_TIMESTAMP)" : "NULL"},
              updated_at = CURRENT_TIMESTAMP
          WHERE id = ?
        `,
        [current.id]
      );
      return getCity(current.id);
    }));
  }

  async function createTournament(payload) {
    const input = normalizeTournamentInput(payload);
    const adminUserIds = normalizeAdminUserIds(payload?.admin_user_ids ?? payload?.admins) || [];
    return enqueueMutation(() => transaction(async () => {
      const validated = await validateTournamentRelations(input, null, { requireActiveCity: true });
      const tournamentId = `ipt_${idFactory()}`;
      await dbRun(
        db,
        `
          INSERT INTO in_person_tournaments (
            id, slug, name_en, name_local, scope, association_id, local_subtype,
            qualifier_city_id, start_date, end_date, organizer_name, organizer_url,
            rules_url, swiss_rounds_count, playoff_first_round, draw_mode,
            swiss_tiebreak_profile, status, revision, created_at, updated_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'draft', 1,
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        `,
        [
          tournamentId,
          validated.slug,
          validated.name_en,
          validated.name_local,
          validated.scope,
          validated.association_id,
          validated.local_subtype,
          validated.qualifier_city_id,
          validated.start_date,
          validated.end_date,
          validated.organizer_name,
          validated.organizer_url,
          validated.rules_url,
          validated.swiss_rounds_count,
          validated.playoff_first_round,
          validated.draw_mode,
          validated.swiss_tiebreak_profile,
        ]
      );
      await replaceAdminsInTransaction(tournamentId, adminUserIds);
      return getTournament(tournamentId);
    }));
  }

  async function updateTournament(tournamentId, payload) {
    return enqueueMutation(() => transaction(async () => {
      const current = await requireTournamentRow(tournamentId);
      if (payload?.id !== undefined && normalizeText(payload.id) !== current.id) {
        throw validationError("TOURNAMENT_ID_IMMUTABLE", "Tournament id cannot be changed");
      }
      if (["cancelled", "completed"].includes(current.status)) {
        throw conflictError("TOURNAMENT_READ_ONLY", "Cancelled or completed tournaments are read-only");
      }
      const input = await validateTournamentRelations(normalizeTournamentInput(payload, current), current);
      if (current.published_at && input.slug !== current.slug) {
        throw conflictError("SLUG_IMMUTABLE", "slug cannot be changed after publication");
      }
      if (
        Number(current.has_started_swiss) === 1
        && (
          Number(input.swiss_rounds_count) !== Number(current.swiss_rounds_count)
          || input.playoff_first_round !== current.playoff_first_round
        )
      ) {
        throw conflictError(
          "FORMAT_LOCKED",
          "Swiss rounds count and playoff first round cannot be changed after Swiss starts"
        );
      }
      const adminUserIds = normalizeAdminUserIds(payload?.admin_user_ids ?? payload?.admins);
      await dbRun(
        db,
        `
          UPDATE in_person_tournaments
          SET slug = ?, name_en = ?, name_local = ?, scope = ?, association_id = ?,
              local_subtype = ?, qualifier_city_id = ?, start_date = ?, end_date = ?,
              organizer_name = ?, organizer_url = ?, rules_url = ?, swiss_rounds_count = ?,
              playoff_first_round = ?, draw_mode = ?, swiss_tiebreak_profile = ?,
              revision = revision + 1, updated_at = CURRENT_TIMESTAMP
          WHERE id = ?
        `,
        [
          input.slug,
          input.name_en,
          input.name_local,
          input.scope,
          input.association_id,
          input.local_subtype,
          input.qualifier_city_id,
          input.start_date,
          input.end_date,
          input.organizer_name,
          input.organizer_url,
          input.rules_url,
          input.swiss_rounds_count,
          input.playoff_first_round,
          input.draw_mode,
          input.swiss_tiebreak_profile,
          current.id,
        ]
      );
      if (adminUserIds !== undefined) {
        await replaceAdminsInTransaction(current.id, adminUserIds);
      }
      return getTournament(current.id);
    }));
  }

  async function publishTournament(tournamentId) {
    return enqueueMutation(() => transaction(async () => {
      const current = await requireTournamentRow(tournamentId);
      if (current.status === "registration" && current.published_at) return getTournament(current.id);
      if (current.status !== "draft") {
        throw conflictError("INVALID_TOURNAMENT_STATUS", "Only a draft tournament can be published");
      }
      const input = await validateTournamentRelations(normalizeTournamentInput(current, current), current, {
        requireActiveCity: true,
      });
      void input;
      await dbRun(
        db,
        `
          UPDATE in_person_tournaments
          SET status = 'registration', published_at = CURRENT_TIMESTAMP,
              revision = revision + 1, updated_at = CURRENT_TIMESTAMP
          WHERE id = ?
        `,
        [current.id]
      );
      return getTournament(current.id);
    }));
  }

  async function cancelTournament(tournamentId) {
    return enqueueMutation(() => transaction(async () => {
      const current = await requireTournamentRow(tournamentId);
      if (current.status === "cancelled") return getTournament(current.id);
      const swissRound = await dbGet(
        db,
        `
          SELECT id
          FROM in_person_rounds
          WHERE tournament_id = ? AND stage = 'swiss' AND status <> 'cancelled'
          LIMIT 1
        `,
        [current.id]
      );
      if (swissRound || ["swiss", "playoff", "completed"].includes(current.status)) {
        throw conflictError(
          "TOURNAMENT_ALREADY_STARTED",
          "The tournament cannot be cancelled after the first Swiss round starts"
        );
      }
      await dbRun(
        db,
        `
          UPDATE in_person_tournaments
          SET status = 'cancelled', cancelled_at = CURRENT_TIMESTAMP,
              revision = revision + 1, updated_at = CURRENT_TIMESTAMP
          WHERE id = ?
        `,
        [current.id]
      );
      return getTournament(current.id);
    }));
  }

  async function listTournamentAdmins(tournamentId) {
    const tournament = await requireTournamentRow(tournamentId);
    return loadAdmins(tournament.id);
  }

  async function replaceTournamentAdmins(tournamentId, rawUserIds) {
    const userIds = normalizeAdminUserIds(rawUserIds);
    return enqueueMutation(() => transaction(async () => {
      const tournament = await requireTournamentRow(tournamentId);
      await replaceAdminsInTransaction(tournament.id, userIds || []);
      return loadAdmins(tournament.id);
    }));
  }

  async function addTournamentAdmin(tournamentId, rawUserId) {
    const [userId] = normalizeAdminUserIds([rawUserId]);
    return enqueueMutation(() => transaction(async () => {
      const tournament = await requireTournamentRow(tournamentId);
      await validateAdminUsers([userId]);
      await dbRun(
        db,
        `
          INSERT INTO tournament_access_users (
            tournament_entity_type, tournament_id, user_id, role, created_at, updated_at
          ) VALUES (?, ?, ?, 'admin', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
          ON CONFLICT (tournament_entity_type, tournament_id, user_id)
          DO UPDATE SET role = 'admin', updated_at = CURRENT_TIMESTAMP
        `,
        [TOURNAMENT_ENTITY_TYPES.IN_PERSON_TOURNAMENT, tournament.id, userId]
      );
      return (await loadAdmins(tournament.id)).find((admin) => admin.user_id === userId);
    }));
  }

  async function removeTournamentAdmin(tournamentId, rawUserId) {
    const [userId] = normalizeAdminUserIds([rawUserId]);
    return enqueueMutation(() => transaction(async () => {
      const tournament = await requireTournamentRow(tournamentId);
      const result = await dbRun(
        db,
        `
          DELETE FROM tournament_access_users
          WHERE tournament_entity_type = ? AND tournament_id = ? AND user_id = ?
        `,
        [TOURNAMENT_ENTITY_TYPES.IN_PERSON_TOURNAMENT, tournament.id, userId]
      );
      if (!result.changes) throw notFoundError("TOURNAMENT_ADMIN_NOT_FOUND", "Tournament admin not found");
      return { user_id: userId };
    }));
  }

  return {
    addTournamentAdmin,
    archiveCity: (cityId) => setCityArchived(cityId, true),
    cancelTournament,
    createCity,
    createTournament,
    getCity,
    getTournament,
    listCities,
    listTournamentAdmins,
    listTournaments,
    publishTournament,
    removeTournamentAdmin,
    replaceTournamentAdmins,
    restoreCity: (cityId) => setCityArchived(cityId, false),
    updateCity,
    updateTournament,
  };
}
