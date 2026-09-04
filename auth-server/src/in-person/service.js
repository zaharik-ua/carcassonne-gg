import { randomUUID } from "node:crypto";
import { getPlayoffPreview } from "./constants.js";
import {
  calculateSwissStandings,
  InPersonEngineError,
  pairFirstSwissRound,
  pairNextSwissRound,
  validateMatchResult,
} from "./engine.js";
import { TOURNAMENT_ENTITY_TYPES } from "./schema.js";
import {
  conflictError,
  InPersonError,
  normalizeAdminUserIds,
  normalizeCityInput,
  normalizeDrawNumber,
  normalizeOptionalText,
  normalizeParticipantInput,
  normalizeRequiredBoolean,
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
  if (message.includes("in_person_participants.tournament_id, in_person_participants.draw_number")) {
    return conflictError("DRAW_NUMBER_TAKEN", "This draw number is already assigned in the tournament");
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
    icon_url: row.icon_url || null,
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

function serializeOrganizerTournament(row) {
  const tournament = serializeTournament(row, []);
  delete tournament.admins;
  delete tournament.admin_user_ids;
  return { ...tournament, access_role: "admin" };
}

function serializeParticipant(row) {
  if (!row) return null;
  return {
    id: row.id,
    tournament_id: row.tournament_id,
    name_en: row.name_en,
    name_local: row.name_local || null,
    bga_nickname: row.bga_nickname || null,
    association_id: row.association_id || null,
    association_name: row.association_name || null,
    association_flag: row.association_flag || null,
    city_id: row.city_id || null,
    city_name_en: row.city_name_en || null,
    city_name_local: row.city_name_local || null,
    city_icon_url: row.city_icon_url || null,
    city_association_id: row.city_association_id || null,
    status: row.status,
    draw_number: row.draw_number == null ? null : Number(row.draw_number),
    checked_in_at: row.checked_in_at || null,
    withdrawn_at: row.withdrawn_at || null,
    disqualified_at: row.disqualified_at || null,
    status_reason: row.status_reason || null,
    is_late_entry: Number(row.is_late_entry) === 1,
    late_entry_mode: row.late_entry_mode || null,
    has_matches: Number(row.has_matches) === 1,
    created_at: row.created_at,
    updated_at: row.updated_at,
  };
}

function serializeSwissMatch(row) {
  if (!row) return null;
  return {
    id: row.id,
    round_id: row.round_id,
    table_number: row.table_number == null ? null : Number(row.table_number),
    participant_a_id: row.participant_a_id || null,
    participant_a_name_en: row.participant_a_name_en || null,
    participant_a_name_local: row.participant_a_name_local || null,
    participant_b_id: row.participant_b_id || null,
    participant_b_name_en: row.participant_b_name_en || null,
    participant_b_name_local: row.participant_b_name_local || null,
    starting_participant_id: row.starting_participant_id || null,
    status: row.status,
    is_bye: Number(row.is_bye) === 1,
    result_type: row.result_type || null,
    points_a: row.points_a == null ? null : Number(row.points_a),
    points_b: row.points_b == null ? null : Number(row.points_b),
    winner_participant_id: row.winner_participant_id || null,
    loser_participant_id: row.loser_participant_id || null,
    finish_reason: row.finish_reason || null,
    admin_note: row.admin_note || null,
    revision: Number(row.revision),
    created_at: row.created_at,
    updated_at: row.updated_at,
  };
}

function serializeSwissRound(row, matches = []) {
  if (!row) return null;
  const activeMatches = matches.filter((match) => match.status !== "cancelled");
  const completedMatches = activeMatches.filter((match) => match.status === "completed");
  return {
    id: row.id,
    tournament_id: row.tournament_id,
    round_number: Number(row.round_number),
    status: row.status,
    revision: Number(row.revision),
    published_at: row.published_at || null,
    completed_at: row.completed_at || null,
    created_at: row.created_at,
    updated_at: row.updated_at,
    progress: {
      completed: completedMatches.length,
      total: activeMatches.length,
      missing_table_numbers: activeMatches
        .filter((match) => match.status !== "completed" && match.table_number != null)
        .map((match) => Number(match.table_number)),
    },
    matches: activeMatches,
  };
}

function serializeSwissStanding(row) {
  if (!row) return null;
  return {
    participant_id: row.participant_id,
    participant_name_en: row.participant_name_en || null,
    participant_name_local: row.participant_name_local || null,
    bga_nickname: row.bga_nickname || null,
    position: Number(row.position),
    wins: Number(row.wins),
    buchholz: Number(row.buchholz),
    solkoff1: Number(row.solkoff1),
    solkoff2: Number(row.solkoff2),
    vp_difference: Number(row.vp_difference),
    sonneborn_berger: Number(row.sonneborn_berger),
    bye_count: Number(row.bye_count),
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

const PARTICIPANT_SELECT = `
  SELECT
    p.*,
    a.name AS association_name,
    a.flag AS association_flag,
    c.name_en AS city_name_en,
    c.name_local AS city_name_local,
    c.icon_url AS city_icon_url,
    c.association_id AS city_association_id,
    CASE WHEN EXISTS (
      SELECT 1
      FROM in_person_matches m
      WHERE m.participant_a_id = p.id OR m.participant_b_id = p.id
    ) THEN 1 ELSE 0 END AS has_matches
  FROM in_person_participants p
  LEFT JOIN associations a
    ON upper(trim(a.code)) = upper(trim(p.association_id))
  LEFT JOIN cities c ON c.id = p.city_id
`;

const SWISS_MATCH_SELECT = `
  SELECT
    m.*,
    pa.name_en AS participant_a_name_en,
    pa.name_local AS participant_a_name_local,
    pb.name_en AS participant_b_name_en,
    pb.name_local AS participant_b_name_local
  FROM in_person_matches m
  LEFT JOIN in_person_participants pa ON pa.id = m.participant_a_id
  LEFT JOIN in_person_participants pb ON pb.id = m.participant_b_id
`;

export function createInPersonService({ db, idFactory = randomUUID, faultInjector = null } = {}) {
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

  async function injectFault(point, context = {}) {
    if (typeof faultInjector === "function") await faultInjector(point, context);
  }

  function throwEngineError(error, { conflict = false } = {}) {
    if (!(error instanceof InPersonEngineError)) throw error;
    if (conflict) throw conflictError(error.code, error.message, error.details);
    throw validationError(error.code, error.message, error.details);
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

  async function listAccessibleTournaments(user) {
    const userId = Number(user?.id);
    if (!Number.isInteger(userId) || userId <= 0) return [];
    const params = [];
    let accessFilter = "t.status <> 'cancelled'";
    if (Number(user?.admin) !== 1 && user?.isAdmin !== true) {
      accessFilter += `
        AND EXISTS (
          SELECT 1
          FROM tournament_access_users tau
          WHERE tau.tournament_entity_type = 'in_person_tournament'
            AND tau.tournament_id = t.id
            AND tau.user_id = ?
            AND lower(trim(tau.role)) = 'admin'
        )
      `;
      params.push(userId);
    }
    const rows = await dbAll(
      db,
      `${TOURNAMENT_SELECT} WHERE ${accessFilter}
       ORDER BY t.start_date DESC, lower(t.name_en), t.id`,
      params
    );
    return rows.map(serializeOrganizerTournament);
  }

  async function loadParticipantRows(tournamentId) {
    return dbAll(
      db,
      `${PARTICIPANT_SELECT}
       WHERE p.tournament_id = ?
       ORDER BY
         CASE p.status
           WHEN 'checked_in' THEN 0
           WHEN 'registered' THEN 1
           WHEN 'withdrawn' THEN 2
           ELSE 3
         END,
         CASE WHEN p.draw_number IS NULL THEN 1 ELSE 0 END,
         p.draw_number,
         lower(p.name_en),
         p.id`,
      [tournamentId]
    );
  }

  async function getParticipantRow(tournamentId, participantId) {
    return dbGet(
      db,
      `${PARTICIPANT_SELECT} WHERE p.tournament_id = ? AND p.id = ? LIMIT 1`,
      [tournamentId, normalizeText(participantId)]
    );
  }

  async function requireParticipantRow(tournamentId, participantId) {
    const row = await getParticipantRow(tournamentId, participantId);
    if (!row) throw notFoundError("PARTICIPANT_NOT_FOUND", "Participant not found");
    return row;
  }

  function participantCounters(participants) {
    const activeRoster = participants.filter((participant) => (
      participant.status === "registered" || participant.status === "checked_in"
    ));
    const checkedIn = participants.filter((participant) => participant.status === "checked_in");
    const withdrawnDisqualified = participants.filter((participant) => (
      participant.status === "withdrawn" || participant.status === "disqualified"
    ));
    return {
      total: participants.length,
      registered: activeRoster.length,
      awaiting_check_in: participants.filter((participant) => participant.status === "registered").length,
      checked_in: checkedIn.length,
      without_draw_number: checkedIn.filter((participant) => participant.draw_number == null).length,
      withdrawn_disqualified: withdrawnDisqualified.length,
    };
  }

  function firstRoundReadiness(tournament, participants) {
    const counters = participantCounters(participants);
    const missingDrawNumbers = participants
      .filter((participant) => participant.status === "checked_in" && participant.draw_number == null)
      .map((participant) => ({ id: participant.id, name_en: participant.name_en }));
    const minimumCheckedIn = Number(getPlayoffPreview(tournament.playoff_first_round)?.participant_count || 0);
    const issues = [];
    if (tournament.status !== "check_in") {
      issues.push({ code: "CHECK_IN_NOT_STARTED", message: "Start check-in before forming the first round." });
    }
    if (tournament.has_started_swiss) {
      issues.push({ code: "FIRST_ROUND_ALREADY_CREATED", message: "The first Swiss round already exists." });
    }
    if (counters.checked_in < minimumCheckedIn) {
      issues.push({
        code: "NOT_ENOUGH_CHECKED_IN",
        message: `At least ${minimumCheckedIn} checked-in participants are required by the playoff configuration.`,
        required: minimumCheckedIn,
        actual: counters.checked_in,
      });
    }
    if (missingDrawNumbers.length) {
      issues.push({
        code: "MISSING_DRAW_NUMBERS",
        message: `${missingDrawNumbers.length} checked-in participant(s) have no draw number.`,
        participants: missingDrawNumbers,
      });
    }
    return {
      ready: issues.length === 0,
      locked: !!tournament.has_started_swiss,
      minimum_checked_in: minimumCheckedIn,
      checked_in: counters.checked_in,
      missing_draw_numbers: missingDrawNumbers,
      issues,
    };
  }

  async function getParticipantsOverview(tournamentId) {
    const tournamentRow = await requireTournamentRow(tournamentId);
    const participants = (await loadParticipantRows(tournamentRow.id)).map(serializeParticipant);
    const tournament = serializeOrganizerTournament(tournamentRow);
    return {
      tournament,
      participants,
      counters: participantCounters(participants),
      readiness: firstRoundReadiness(tournament, participants),
    };
  }

  async function validateParticipantRelations(input, tournament, current = null) {
    if (tournament.scope === "international") {
      const association = await findAssociation(input.association_id);
      return { ...input, association_id: association.code, city_id: null };
    }
    const city = await dbGet(db, `SELECT * FROM cities WHERE id = ? LIMIT 1`, [input.city_id]);
    if (!city) {
      throw validationError("UNKNOWN_PARTICIPANT_CITY", "city_id must reference an existing city", {
        field: "city_id",
      });
    }
    const sameCurrentCity = current && current.city_id === city.id;
    if (city.archived_at && !sameCurrentCity) {
      throw conflictError("CITY_ARCHIVED", "An archived city cannot be selected for a participant");
    }
    if (
      normalizeText(city.association_id).toUpperCase()
      !== normalizeText(tournament.association_id).toUpperCase()
    ) {
      throw validationError(
        "PARTICIPANT_CITY_ASSOCIATION_MISMATCH",
        "Participant city must belong to the local tournament association",
        { field: "city_id" }
      );
    }
    return { ...input, association_id: null, city_id: city.id };
  }

  function normalizeDuplicateValue(value) {
    return normalizeText(value).toLocaleLowerCase("en").replace(/\s+/g, " ");
  }

  async function validateParticipantDuplicate(
    tournamentId,
    input,
    { excludeParticipantId = null, confirmed = false } = {}
  ) {
    const rows = await loadParticipantRows(tournamentId);
    const normalizedName = normalizeDuplicateValue(input.name_en);
    const normalizedBga = normalizeDuplicateValue(input.bga_nickname);
    const candidates = rows
      .filter((row) => row.id !== excludeParticipantId)
      .map((row) => {
        const reasons = [];
        if (normalizedName && normalizeDuplicateValue(row.name_en) === normalizedName) {
          reasons.push("name_en");
        }
        if (normalizedBga && normalizeDuplicateValue(row.bga_nickname) === normalizedBga) {
          reasons.push("bga_nickname");
        }
        return reasons.length ? { ...serializeParticipant(row), duplicate_fields: reasons } : null;
      })
      .filter(Boolean);
    if (candidates.length && !confirmed) {
      throw conflictError(
        "DUPLICATE_PARTICIPANT",
        "A possible duplicate participant already exists. Confirm only for a genuine namesake.",
        { candidates, confirmation_field: "confirm_duplicate" }
      );
    }
  }

  async function touchTournament(tournamentId) {
    await dbRun(
      db,
      `UPDATE in_person_tournaments
       SET revision = revision + 1, updated_at = CURRENT_TIMESTAMP
       WHERE id = ?`,
      [tournamentId]
    );
  }

  function assertParticipantMutationsAllowed(tournament) {
    if (["cancelled", "completed"].includes(tournament.status)) {
      throw conflictError("TOURNAMENT_READ_ONLY", "Cancelled or completed tournaments are read-only");
    }
  }

  async function createParticipant(tournamentId, payload) {
    return enqueueMutation(() => transaction(async () => {
      const tournament = await requireTournamentRow(tournamentId);
      assertParticipantMutationsAllowed(tournament);
      if (Number(tournament.has_started_swiss) === 1) {
        throw conflictError(
          "LATE_ENTRY_REQUIRED",
          "After the first Swiss round is created, use the dedicated late participant operation"
        );
      }
      const input = await validateParticipantRelations(
        normalizeParticipantInput(payload, null, tournament),
        tournament
      );
      await validateParticipantDuplicate(tournament.id, input, {
        confirmed: payload?.confirm_duplicate === true,
      });
      const participantId = `ipp_${idFactory()}`;
      await dbRun(
        db,
        `
          INSERT INTO in_person_participants (
            id, tournament_id, name_en, name_local, bga_nickname,
            association_id, city_id, status, created_at, updated_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, 'registered', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        `,
        [
          participantId,
          tournament.id,
          input.name_en,
          input.name_local,
          input.bga_nickname,
          input.association_id,
          input.city_id,
        ]
      );
      await touchTournament(tournament.id);
      return serializeParticipant(await requireParticipantRow(tournament.id, participantId));
    }));
  }

  async function updateParticipant(tournamentId, participantId, payload) {
    return enqueueMutation(() => transaction(async () => {
      const tournament = await requireTournamentRow(tournamentId);
      assertParticipantMutationsAllowed(tournament);
      const current = await requireParticipantRow(tournament.id, participantId);
      if (payload?.id !== undefined && normalizeText(payload.id) !== current.id) {
        throw validationError("PARTICIPANT_ID_IMMUTABLE", "Participant id cannot be changed");
      }
      const input = await validateParticipantRelations(
        normalizeParticipantInput(payload, current, tournament),
        tournament,
        current
      );
      await validateParticipantDuplicate(tournament.id, input, {
        excludeParticipantId: current.id,
        confirmed: payload?.confirm_duplicate === true,
      });
      await dbRun(
        db,
        `
          UPDATE in_person_participants
          SET name_en = ?, name_local = ?, bga_nickname = ?, association_id = ?, city_id = ?,
              updated_at = CURRENT_TIMESTAMP
          WHERE id = ? AND tournament_id = ?
        `,
        [
          input.name_en,
          input.name_local,
          input.bga_nickname,
          input.association_id,
          input.city_id,
          current.id,
          tournament.id,
        ]
      );
      await touchTournament(tournament.id);
      return serializeParticipant(await requireParticipantRow(tournament.id, current.id));
    }));
  }

  async function deleteParticipant(tournamentId, participantId) {
    return enqueueMutation(() => transaction(async () => {
      const tournament = await requireTournamentRow(tournamentId);
      assertParticipantMutationsAllowed(tournament);
      const current = await requireParticipantRow(tournament.id, participantId);
      if (Number(tournament.has_started_swiss) === 1 || Number(current.has_matches) === 1) {
        throw conflictError(
          "PARTICIPANT_DELETE_LOCKED",
          "A participant cannot be deleted after the first round or after appearing in a match"
        );
      }
      await dbRun(
        db,
        `DELETE FROM in_person_participants WHERE id = ? AND tournament_id = ?`,
        [current.id, tournament.id]
      );
      await touchTournament(tournament.id);
      return { id: current.id };
    }));
  }

  async function startCheckIn(tournamentId) {
    return enqueueMutation(() => transaction(async () => {
      const tournament = await requireTournamentRow(tournamentId);
      assertParticipantMutationsAllowed(tournament);
      if (tournament.status === "check_in") return serializeOrganizerTournament(tournament);
      if (tournament.status !== "registration") {
        throw conflictError(
          "INVALID_TOURNAMENT_STATUS",
          "Publish the tournament before starting check-in"
        );
      }
      const roster = await dbGet(
        db,
        `
          SELECT COUNT(*) AS count
          FROM in_person_participants
          WHERE tournament_id = ? AND status IN ('registered', 'checked_in')
        `,
        [tournament.id]
      );
      if (Number(roster?.count || 0) < 2) {
        throw conflictError(
          "NOT_ENOUGH_REGISTERED_PARTICIPANTS",
          "At least two registered participants are required to start check-in"
        );
      }
      await dbRun(
        db,
        `
          UPDATE in_person_tournaments
          SET status = 'check_in', revision = revision + 1, updated_at = CURRENT_TIMESTAMP
          WHERE id = ?
        `,
        [tournament.id]
      );
      return serializeOrganizerTournament(await requireTournamentRow(tournament.id));
    }));
  }

  async function setParticipantCheckIn(tournamentId, participantId, payload) {
    const checkedIn = normalizeRequiredBoolean(payload?.checked_in, "checked_in");
    const requestedDrawNumber = normalizeDrawNumber(payload?.draw_number);
    return enqueueMutation(() => transaction(async () => {
      const tournament = await requireTournamentRow(tournamentId);
      assertParticipantMutationsAllowed(tournament);
      if (tournament.status !== "check_in") {
        throw conflictError("CHECK_IN_NOT_STARTED", "Start tournament check-in before confirming participants");
      }
      if (Number(tournament.has_started_swiss) === 1) {
        throw conflictError(
          "CHECK_IN_LOCKED",
          "Check-in and draw numbers are locked after the first Swiss round is created"
        );
      }
      const current = await requireParticipantRow(tournament.id, participantId);
      if (["withdrawn", "disqualified"].includes(current.status)) {
        throw conflictError(
          "PARTICIPANT_INACTIVE",
          "A withdrawn or disqualified participant cannot be checked in"
        );
      }
      const drawNumber = checkedIn
        ? (requestedDrawNumber === undefined ? current.draw_number : requestedDrawNumber)
        : null;
      if (drawNumber != null) {
        const duplicate = await dbGet(
          db,
          `
            SELECT id, name_en
            FROM in_person_participants
            WHERE tournament_id = ? AND draw_number = ? AND id <> ?
            LIMIT 1
          `,
          [tournament.id, drawNumber, current.id]
        );
        if (duplicate) {
          throw conflictError("DRAW_NUMBER_TAKEN", "This draw number is already assigned", {
            draw_number: drawNumber,
            participant: { id: duplicate.id, name_en: duplicate.name_en },
          });
        }
      }
      await dbRun(
        db,
        `
          UPDATE in_person_participants
          SET status = ?, draw_number = ?,
              checked_in_at = CASE
                WHEN ? = 1 THEN COALESCE(checked_in_at, CURRENT_TIMESTAMP)
                ELSE NULL
              END,
              updated_at = CURRENT_TIMESTAMP
          WHERE id = ? AND tournament_id = ?
        `,
        [checkedIn ? "checked_in" : "registered", drawNumber, checkedIn ? 1 : 0, current.id, tournament.id]
      );
      await touchTournament(tournament.id);
      return serializeParticipant(await requireParticipantRow(tournament.id, current.id));
    }));
  }

  async function loadSwissMatchRows(roundId) {
    const rows = await dbAll(
      db,
      `${SWISS_MATCH_SELECT}
       WHERE m.round_id = ? AND m.status <> 'cancelled'
       ORDER BY
         CASE WHEN m.table_number IS NULL THEN 1 ELSE 0 END,
         m.table_number,
         m.id`,
      [roundId]
    );
    return rows.map(serializeSwissMatch);
  }

  async function loadSwissRounds(tournamentId) {
    const rows = await dbAll(
      db,
      `
        SELECT *
        FROM in_person_rounds
        WHERE tournament_id = ? AND stage = 'swiss' AND status <> 'cancelled'
        ORDER BY round_number, id
      `,
      [tournamentId]
    );
    const rounds = [];
    for (const row of rows) {
      rounds.push(serializeSwissRound(row, await loadSwissMatchRows(row.id)));
    }
    return rounds;
  }

  async function requireSwissRoundRow(tournamentId, roundId) {
    const row = await dbGet(
      db,
      `
        SELECT *
        FROM in_person_rounds
        WHERE id = ? AND tournament_id = ? AND stage = 'swiss'
        LIMIT 1
      `,
      [normalizeText(roundId), tournamentId]
    );
    if (!row) throw notFoundError("SWISS_ROUND_NOT_FOUND", "Swiss round not found");
    return row;
  }

  async function loadLatestSwissStandings(tournamentId) {
    const revisionRow = await dbGet(
      db,
      `
        SELECT MAX(s.revision) AS revision
        FROM in_person_standings s
        JOIN in_person_rounds r ON r.id = s.source_completed_round_id
        WHERE s.tournament_id = ?
          AND r.stage = 'swiss'
          AND r.status = 'completed'
      `,
      [tournamentId]
    );
    const revision = Number(revisionRow?.revision || 0);
    if (!revision) {
      return { revision: 0, source_completed_round_id: null, calculated_at: null, rows: [] };
    }
    const rows = await dbAll(
      db,
      `
        SELECT
          s.*,
          p.name_en AS participant_name_en,
          p.name_local AS participant_name_local,
          p.bga_nickname
        FROM in_person_standings s
        JOIN in_person_participants p ON p.id = s.participant_id
        WHERE s.tournament_id = ? AND s.revision = ?
        ORDER BY s.position, s.participant_id
      `,
      [tournamentId, revision]
    );
    return {
      revision,
      source_completed_round_id: rows[0]?.source_completed_round_id || null,
      calculated_at: rows[0]?.calculated_at || null,
      rows: rows.map(serializeSwissStanding),
    };
  }

  function assertSwissRoundSequence(rounds) {
    rounds.forEach((round, index) => {
      if (Number(round.round_number) !== index + 1) {
        throw conflictError(
          "INVALID_SWISS_ROUND_SEQUENCE",
          "Active Swiss rounds must be sequential without gaps"
        );
      }
    });
    if (rounds.filter((round) => round.status !== "completed").length > 1) {
      throw conflictError(
        "MULTIPLE_ACTIVE_SWISS_ROUNDS",
        "Only one incomplete Swiss round may exist"
      );
    }
  }

  function normalizeRequestedRoundNumber(value, fallback) {
    if (value === undefined || value === null || value === "") return fallback;
    const roundNumber = Number(value);
    if (!Number.isInteger(roundNumber) || roundNumber <= 0) {
      throw validationError("INVALID_ROUND_NUMBER", "round_number must be a positive integer", {
        field: "round_number",
      });
    }
    return roundNumber;
  }

  function enrichPairingPlan(plan, participants) {
    const participantById = new Map(participants.map((participant) => [participant.id, participant]));
    return {
      ...plan,
      matches: plan.matches.map((match) => {
        const participantA = participantById.get(match.participant_a_id);
        const participantB = participantById.get(match.participant_b_id);
        return {
          ...match,
          participant_a_name_en: participantA?.name_en || null,
          participant_a_name_local: participantA?.name_local || null,
          participant_b_name_en: participantB?.name_en || null,
          participant_b_name_local: participantB?.name_local || null,
        };
      }),
    };
  }

  async function buildSwissPairingPlan(tournamentId, requestedRoundNumber) {
    const tournamentRow = await requireTournamentRow(tournamentId);
    const tournament = serializeOrganizerTournament(tournamentRow);
    if (["draft", "registration", "playoff", "completed", "cancelled"].includes(tournament.status)) {
      throw conflictError(
        "INVALID_TOURNAMENT_STATUS",
        "Swiss pairing is available only during check-in or the Swiss stage"
      );
    }
    const [participantRows, rounds, standings] = await Promise.all([
      loadParticipantRows(tournament.id),
      loadSwissRounds(tournament.id),
      loadLatestSwissStandings(tournament.id),
    ]);
    assertSwissRoundSequence(rounds);
    const expectedRoundNumber = rounds.length + 1;
    const roundNumber = normalizeRequestedRoundNumber(requestedRoundNumber, expectedRoundNumber);
    if (roundNumber !== expectedRoundNumber) {
      throw conflictError(
        "STALE_ROUND_PREVIEW",
        `Round ${roundNumber} is no longer the next Swiss round`,
        { expected_round_number: expectedRoundNumber }
      );
    }
    if (roundNumber > Number(tournament.swiss_rounds_count)) {
      throw conflictError("SWISS_ROUNDS_COMPLETE", "All configured Swiss rounds are complete");
    }
    const incompleteRound = rounds.find((round) => round.status !== "completed");
    if (incompleteRound) {
      throw conflictError(
        "SWISS_ROUND_IN_PROGRESS",
        `Complete round ${incompleteRound.round_number} before generating the next round`,
        { round_id: incompleteRound.id, round_number: incompleteRound.round_number }
      );
    }

    const participants = participantRows.map(serializeParticipant);
    let plan;
    try {
      if (roundNumber === 1) {
        const readiness = firstRoundReadiness(tournament, participants);
        if (!readiness.ready) {
          throw conflictError(
            "FIRST_ROUND_NOT_READY",
            "The first Swiss round cannot be formed yet",
            { readiness }
          );
        }
        plan = pairFirstSwissRound({ participants });
      } else {
        const previousRound = rounds[rounds.length - 1];
        if (!previousRound || previousRound.status !== "completed") {
          throw conflictError(
            "PREVIOUS_ROUND_INCOMPLETE",
            "Complete the current Swiss round before generating the next round"
          );
        }
        if (standings.source_completed_round_id !== previousRound.id) {
          throw conflictError(
            "STANDINGS_NOT_CURRENT",
            "Recalculate standings for the completed round before generating the next round"
          );
        }
        plan = pairNextSwissRound({
          participants,
          standings: standings.rows,
          rounds,
        });
      }
    } catch (error) {
      throwEngineError(error, { conflict: true });
    }
    return {
      round_number: roundNumber,
      tournament_revision: Number(tournament.revision),
      ...enrichPairingPlan(plan, participants),
    };
  }

  async function getSwissOverview(tournamentId) {
    const tournamentRow = await requireTournamentRow(tournamentId);
    const [rounds, standings] = await Promise.all([
      loadSwissRounds(tournamentRow.id),
      loadLatestSwissStandings(tournamentRow.id),
    ]);
    assertSwissRoundSequence(rounds);
    const currentRound = rounds.length ? rounds[rounds.length - 1] : null;
    const completedRounds = rounds.filter((round) => round.status === "completed").length;
    const swissRoundsCount = Number(tournamentRow.swiss_rounds_count);
    const nextRoundNumber = rounds.length + 1;
    return {
      tournament: serializeOrganizerTournament(tournamentRow),
      rounds,
      current_round: currentRound,
      standings,
      progress: currentRound?.progress || {
        completed: 0,
        total: 0,
        missing_table_numbers: [],
      },
      completed_rounds: completedRounds,
      swiss_rounds_count: swissRoundsCount,
      next_round_number: nextRoundNumber <= swissRoundsCount ? nextRoundNumber : null,
      can_generate_first_round: rounds.length === 0 && tournamentRow.status === "check_in",
      can_generate_next_round: !!currentRound
        && currentRound.status === "completed"
        && nextRoundNumber <= swissRoundsCount,
      swiss_complete: completedRounds >= swissRoundsCount,
    };
  }

  async function previewSwissRound(tournamentId, payload = {}) {
    return buildSwissPairingPlan(tournamentId, payload?.round_number);
  }

  async function confirmSwissRound(tournamentId, payload = {}) {
    return enqueueMutation(async () => {
      const outcome = await transaction(async () => {
        const tournament = await requireTournamentRow(tournamentId);
        const rounds = await loadSwissRounds(tournament.id);
        assertSwissRoundSequence(rounds);
        const incompleteRound = rounds.find((round) => round.status !== "completed");
        const fallbackRoundNumber = incompleteRound?.round_number || rounds.length + 1;
        const roundNumber = normalizeRequestedRoundNumber(payload?.round_number, fallbackRoundNumber);
        const existingRound = rounds.find((round) => round.round_number === roundNumber);
        if (existingRound) return { created: false, round_id: existingRound.id };

        const plan = await buildSwissPairingPlan(tournament.id, roundNumber);
        const roundId = `ipr_${idFactory()}`;
        await dbRun(
          db,
          `
            INSERT INTO in_person_rounds (
              id, tournament_id, stage, round_number, status, revision, created_at, updated_at
            ) VALUES (?, ?, 'swiss', ?, 'draft', 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
          `,
          [roundId, tournament.id, roundNumber]
        );
        await injectFault("swiss_round_after_insert", { tournament_id: tournament.id, round_id: roundId });

        for (let index = 0; index < plan.matches.length; index += 1) {
          const match = plan.matches[index];
          const matchId = `ipm_${idFactory()}`;
          await dbRun(
            db,
            `
              INSERT INTO in_person_matches (
                id, round_id, table_number, participant_a_id, participant_b_id,
                starting_participant_id, status, is_bye, result_type,
                winner_participant_id, loser_participant_id, revision,
                created_at, updated_at
              ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            `,
            [
              matchId,
              roundId,
              match.table_number,
              match.participant_a_id,
              match.participant_b_id,
              match.starting_participant_id,
              match.status,
              match.is_bye ? 1 : 0,
              match.result_type,
              match.winner_participant_id,
              match.loser_participant_id,
            ]
          );
          await injectFault("swiss_round_after_match_insert", {
            tournament_id: tournament.id,
            round_id: roundId,
            match_id: matchId,
            match_index: index,
          });
        }
        await dbRun(
          db,
          `
            UPDATE in_person_tournaments
            SET status = 'swiss', revision = revision + 1, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
          `,
          [tournament.id]
        );
        return { created: true, round_id: roundId };
      });
      const overview = await getSwissOverview(tournamentId);
      return { ...overview, ...outcome };
    });
  }

  async function publishSwissRound(tournamentId, roundId) {
    return enqueueMutation(async () => {
      const outcome = await transaction(async () => {
        const tournament = await requireTournamentRow(tournamentId);
        const round = await requireSwissRoundRow(tournament.id, roundId);
        if (["published", "completed"].includes(round.status)) {
          return { published: false, round_id: round.id };
        }
        if (round.status !== "draft") {
          throw conflictError("INVALID_ROUND_STATUS", "Only a draft Swiss round can be published");
        }
        await dbRun(
          db,
          `
            UPDATE in_person_rounds
            SET status = 'published', published_at = CURRENT_TIMESTAMP,
                revision = revision + 1, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
          `,
          [round.id]
        );
        await touchTournament(tournament.id);
        return { published: true, round_id: round.id };
      });
      return { ...(await getSwissOverview(tournamentId)), ...outcome };
    });
  }

  function canonicalResultEquals(match, canonical, startingParticipantId) {
    const fields = [
      "status",
      "result_type",
      "points_a",
      "points_b",
      "winner_participant_id",
      "loser_participant_id",
      "finish_reason",
      "admin_note",
    ];
    return String(match.starting_participant_id || "") === String(startingParticipantId || "")
      && fields.every((field) => (match[field] ?? null) === (canonical[field] ?? null));
  }

  async function saveSwissMatchResult(tournamentId, matchId, payload = {}) {
    return enqueueMutation(async () => {
      const outcome = await transaction(async () => {
        const tournament = await requireTournamentRow(tournamentId);
        const match = await dbGet(
          db,
          `
            SELECT m.*, r.status AS round_status, r.round_number
            FROM in_person_matches m
            JOIN in_person_rounds r ON r.id = m.round_id
            WHERE m.id = ? AND r.tournament_id = ? AND r.stage = 'swiss'
            LIMIT 1
          `,
          [normalizeText(matchId), tournament.id]
        );
        if (!match) throw notFoundError("SWISS_MATCH_NOT_FOUND", "Swiss match not found");
        if (match.status === "cancelled") {
          throw conflictError("MATCH_CANCELLED", "A cancelled match cannot receive a result");
        }
        if (Number(match.is_bye) === 1) {
          throw conflictError("BYE_RESULT_LOCKED", "A system bye result cannot be edited");
        }
        const startingParticipantId = normalizeText(
          payload?.starting_participant_id ?? match.starting_participant_id
        );
        let canonical;
        try {
          canonical = validateMatchResult(
            { ...match, starting_participant_id: startingParticipantId },
            payload
          );
        } catch (error) {
          throwEngineError(error);
        }
        if (canonicalResultEquals(match, canonical, startingParticipantId)) {
          return { changed: false, match_id: match.id };
        }
        if (match.round_status === "draft") {
          throw conflictError("ROUND_NOT_PUBLISHED", "Publish the Swiss round before entering results");
        }
        if (match.round_status === "completed") {
          const laterRound = await dbGet(
            db,
            `
              SELECT id
              FROM in_person_rounds
              WHERE tournament_id = ? AND stage = 'swiss' AND status <> 'cancelled'
                AND round_number > ?
              LIMIT 1
            `,
            [tournament.id, match.round_number]
          );
          if (laterRound) {
            throw conflictError(
              "RESULT_LOCKED",
              "A completed result is locked after the next Swiss round is formed"
            );
          }
        } else if (match.round_status !== "published") {
          throw conflictError("INVALID_ROUND_STATUS", "Results can be saved only for a published round");
        }
        await dbRun(
          db,
          `
            UPDATE in_person_matches
            SET starting_participant_id = ?, status = ?, is_bye = ?, result_type = ?,
                points_a = ?, points_b = ?, winner_participant_id = ?, loser_participant_id = ?,
                finish_reason = ?, admin_note = ?, revision = revision + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
          `,
          [
            startingParticipantId,
            canonical.status,
            canonical.is_bye ? 1 : 0,
            canonical.result_type,
            canonical.points_a,
            canonical.points_b,
            canonical.winner_participant_id,
            canonical.loser_participant_id,
            canonical.finish_reason,
            canonical.admin_note,
            match.id,
          ]
        );
        if (match.round_status === "completed") {
          await dbRun(
            db,
            `
              UPDATE in_person_rounds
              SET status = 'published', completed_at = NULL,
                  revision = revision + 1, updated_at = CURRENT_TIMESTAMP
              WHERE id = ?
            `,
            [match.round_id]
          );
        }
        await touchTournament(tournament.id);
        return { changed: true, match_id: match.id };
      });
      const overview = await getSwissOverview(tournamentId);
      const match = overview.rounds
        .flatMap((round) => round.matches)
        .find((entry) => entry.id === outcome.match_id) || null;
      return { ...overview, ...outcome, match };
    });
  }

  async function completeSwissRound(tournamentId, roundId) {
    return enqueueMutation(async () => {
      const outcome = await transaction(async () => {
        const tournament = await requireTournamentRow(tournamentId);
        const round = await requireSwissRoundRow(tournament.id, roundId);
        if (round.status === "completed") {
          return { completed: false, round_id: round.id };
        }
        if (round.status !== "published") {
          throw conflictError("ROUND_NOT_PUBLISHED", "Publish the Swiss round before completing it");
        }
        const priorIncomplete = await dbGet(
          db,
          `
            SELECT id, round_number
            FROM in_person_rounds
            WHERE tournament_id = ? AND stage = 'swiss' AND status <> 'cancelled'
              AND round_number < ? AND status <> 'completed'
            LIMIT 1
          `,
          [tournament.id, round.round_number]
        );
        if (priorIncomplete) {
          throw conflictError(
            "PREVIOUS_ROUND_INCOMPLETE",
            `Complete round ${priorIncomplete.round_number} first`
          );
        }
        const matches = await loadSwissMatchRows(round.id);
        const incompleteMatches = matches.filter((match) => match.status !== "completed");
        if (incompleteMatches.length) {
          throw conflictError(
            "ROUND_RESULTS_INCOMPLETE",
            "Every active table needs a valid result before the round can be completed",
            {
              missing_table_numbers: incompleteMatches
                .filter((match) => match.table_number != null)
                .map((match) => match.table_number),
            }
          );
        }
        try {
          matches.forEach((match) => validateMatchResult(match, match));
        } catch (error) {
          throwEngineError(error);
        }
        await dbRun(
          db,
          `
            UPDATE in_person_rounds
            SET status = 'completed', completed_at = CURRENT_TIMESTAMP,
                revision = revision + 1, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
          `,
          [round.id]
        );
        await injectFault("swiss_round_after_complete", {
          tournament_id: tournament.id,
          round_id: round.id,
        });

        const [participantRows, completedRounds, revisionRow] = await Promise.all([
          loadParticipantRows(tournament.id),
          loadSwissRounds(tournament.id),
          dbGet(
            db,
            `SELECT COALESCE(MAX(revision), 0) AS revision
             FROM in_person_standings WHERE tournament_id = ?`,
            [tournament.id]
          ),
        ]);
        let standings;
        try {
          standings = calculateSwissStandings({
            participants: participantRows.map(serializeParticipant),
            rounds: completedRounds,
          });
        } catch (error) {
          throwEngineError(error);
        }
        const standingsRevision = Number(revisionRow?.revision || 0) + 1;
        for (let index = 0; index < standings.length; index += 1) {
          const standing = standings[index];
          await dbRun(
            db,
            `
              INSERT INTO in_person_standings (
                tournament_id, revision, source_completed_round_id, participant_id,
                position, wins, buchholz, solkoff1, solkoff2, vp_difference,
                sonneborn_berger, bye_count, calculated_at
              ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            `,
            [
              tournament.id,
              standingsRevision,
              round.id,
              standing.participant_id,
              standing.position,
              standing.wins,
              standing.buchholz,
              standing.solkoff1,
              standing.solkoff2,
              standing.vp_difference,
              standing.sonneborn_berger,
              standing.bye_count,
            ]
          );
          await injectFault("swiss_standings_after_insert", {
            tournament_id: tournament.id,
            round_id: round.id,
            standings_revision: standingsRevision,
            standing_index: index,
          });
        }
        await touchTournament(tournament.id);
        return {
          completed: true,
          round_id: round.id,
          standings_revision: standingsRevision,
        };
      });
      return { ...(await getSwissOverview(tournamentId)), ...outcome };
    });
  }

  async function listParticipantCities(tournamentId) {
    const tournament = await requireTournamentRow(tournamentId);
    if (tournament.scope !== "local") return [];
    return listCities({ associationId: tournament.association_id, includeArchived: false });
  }

  async function validateExistingParticipantLocations(tournamentId, input) {
    const rows = await dbAll(
      db,
      `
        SELECT p.id, p.name_en, p.association_id, p.city_id, c.association_id AS city_association_id
        FROM in_person_participants p
        LEFT JOIN cities c ON c.id = p.city_id
        WHERE p.tournament_id = ?
      `,
      [tournamentId]
    );
    const conflicts = rows.filter((row) => {
      if (input.scope === "international") {
        return !row.association_id || !!row.city_id;
      }
      return !!row.association_id
        || !row.city_id
        || normalizeText(row.city_association_id).toUpperCase()
          !== normalizeText(input.association_id).toUpperCase();
    });
    if (conflicts.length) {
      throw conflictError(
        "PARTICIPANT_LOCATIONS_CONFLICT",
        "Tournament scope or association conflicts with existing participant locations",
        { participants: conflicts.map((row) => ({ id: row.id, name_en: row.name_en })) }
      );
    }
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
            id, association_id, name_en, name_local, icon_url, created_at, updated_at
          ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        `,
        [
          cityId,
          validated.association_id,
          validated.name_en,
          validated.name_local,
          validated.icon_url,
        ]
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
          SET association_id = ?, name_en = ?, name_local = ?, icon_url = ?,
              updated_at = CURRENT_TIMESTAMP
          WHERE id = ?
        `,
        [input.association_id, input.name_en, input.name_local, input.icon_url, current.id]
      );
      return getCity(current.id);
    }));
  }

  async function createParticipantCity(tournamentId, payload) {
    return enqueueMutation(() => transaction(async () => {
      const tournament = await requireTournamentRow(tournamentId);
      assertParticipantMutationsAllowed(tournament);
      if (tournament.scope !== "local" || !tournament.association_id) {
        throw conflictError(
          "CITY_CREATION_LOCAL_ONLY",
          "Participant cities can be created only for a local tournament"
        );
      }
      const input = normalizeCityInput({
        ...(payload || {}),
        association_id: tournament.association_id,
      });
      const validated = await validateCityRelations(input);
      const cityId = `city_${idFactory()}`;
      await dbRun(
        db,
        `
          INSERT INTO cities (
            id, association_id, name_en, name_local, icon_url, created_at, updated_at
          ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        `,
        [
          cityId,
          validated.association_id,
          validated.name_en,
          validated.name_local,
          validated.icon_url,
        ]
      );
      return getCity(cityId);
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
      await validateExistingParticipantLocations(current.id, input);
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
    completeSwissRound,
    confirmSwissRound,
    createCity,
    createParticipantCity,
    createParticipant,
    createTournament,
    deleteParticipant,
    getCity,
    getParticipantsOverview,
    getSwissOverview,
    getTournament,
    listAccessibleTournaments,
    listCities,
    listParticipantCities,
    listTournamentAdmins,
    listTournaments,
    previewSwissRound,
    publishTournament,
    publishSwissRound,
    removeTournamentAdmin,
    replaceTournamentAdmins,
    restoreCity: (cityId) => setCityArchived(cityId, false),
    saveSwissMatchResult,
    setParticipantCheckIn,
    startCheckIn,
    updateCity,
    updateParticipant,
    updateTournament,
  };
}
