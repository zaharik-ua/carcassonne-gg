import { randomUUID } from "node:crypto";
import { getPlayoffPreview } from "./constants.js";
import {
  calculateSwissStandings,
  InPersonEngineError,
  pairFirstSwissRound,
  pairNextSwissRound,
  validateMatchResult,
} from "./engine.js";
import {
  buildPlayoffBracket,
  getPlayoffRoundLabel,
  InPersonPlayoffError,
} from "./playoff.js";
import {
  publicPlayoffPlacements,
  serializePublicMatch,
  serializePublicParticipant,
  serializePublicRound,
  serializePublicStanding,
  serializePublicTournament,
} from "./public.js";
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

function serializePlayoffMatch(row) {
  if (!row) return null;
  return {
    ...serializeSwissMatch(row),
    bracket_position: row.bracket_position == null ? null : Number(row.bracket_position),
    next_match_for_winner_id: row.next_match_for_winner_id || null,
    next_match_for_winner_slot: row.next_match_for_winner_slot || null,
    next_match_for_loser_id: row.next_match_for_loser_id || null,
    next_match_for_loser_slot: row.next_match_for_loser_slot || null,
  };
}

function serializePlayoffRound(row, matches = []) {
  if (!row) return null;
  const activeMatches = matches.filter((match) => match.status !== "cancelled");
  const completedMatches = activeMatches.filter((match) => match.status === "completed");
  const tableNumbers = activeMatches.map((match) => match.table_number);
  const missingParticipants = activeMatches.filter((match) => (
    !match.participant_a_id || !match.participant_b_id
  ));
  const missingTables = activeMatches.filter((match) => match.table_number == null);
  const duplicateTables = tableNumbers.filter((tableNumber, index) => (
    tableNumber != null && tableNumbers.indexOf(tableNumber) !== index
  ));
  const streamingTables = activeMatches.filter((match) => match.table_number === 1);
  const publishBlockers = [];
  if (missingParticipants.length) {
    publishBlockers.push({
      code: "PLAYOFF_PARTICIPANTS_PENDING",
      message: "Both participants must be known for every match.",
      match_ids: missingParticipants.map((match) => match.id),
    });
  }
  if (missingTables.length) {
    publishBlockers.push({
      code: "PLAYOFF_TABLES_INCOMPLETE",
      message: "Every match needs a positive table number.",
      match_ids: missingTables.map((match) => match.id),
    });
  }
  if (duplicateTables.length) {
    publishBlockers.push({
      code: "DUPLICATE_PLAYOFF_TABLE",
      message: "Table numbers must be unique within the round.",
      table_numbers: [...new Set(duplicateTables)],
    });
  }
  if (
    row.round_key === "bronze_medal_match"
    && activeMatches.some((match) => match.table_number !== 2)
  ) {
    publishBlockers.push({
      code: "BRONZE_TABLE_REQUIRED",
      message: "The Bronze medal match must use table 2.",
    });
  } else if (row.round_key !== "bronze_medal_match" && streamingTables.length !== 1) {
    publishBlockers.push({
      code: "STREAMING_TABLE_REQUIRED",
      message: "Exactly one match in the round must use streaming table 1.",
    });
  }
  return {
    id: row.id,
    tournament_id: row.tournament_id,
    round_key: row.round_key,
    round_label: getPlayoffRoundLabel(row.round_key),
    round_order: Number(row.round_order),
    status: row.status,
    revision: Number(row.revision),
    published_at: row.published_at || null,
    completed_at: row.completed_at || null,
    created_at: row.created_at,
    updated_at: row.updated_at,
    progress: { completed: completedMatches.length, total: activeMatches.length },
    can_publish: row.status === "draft" && publishBlockers.length === 0,
    publish_blockers: publishBlockers,
    matches: activeMatches,
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

const PLAYOFF_MATCH_SELECT = SWISS_MATCH_SELECT;

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

  function normalizeRequiredReason(value, field = "reason") {
    const reason = normalizeOptionalText(value);
    if (!reason) {
      throw validationError("REASON_REQUIRED", `${field} is required`, { field });
    }
    return reason;
  }

  function normalizeActorUserId(user) {
    const userId = Number(user?.id ?? user);
    return Number.isInteger(userId) && userId > 0 ? userId : null;
  }

  function normalizeLateEntryMode(value) {
    const mode = normalizeText(value).toLowerCase();
    if (!["late_bye", "pair_with_bye"].includes(mode)) {
      throw validationError(
        "INVALID_LATE_ENTRY_MODE",
        "mode must be late_bye or pair_with_bye",
        { field: "mode" }
      );
    }
    return mode;
  }

  function normalizePositiveTableNumber(value) {
    const tableNumber = Number(value);
    if (!Number.isInteger(tableNumber) || tableNumber <= 0) {
      throw validationError(
        "INVALID_TABLE_NUMBER",
        "table_number must be a positive integer",
        { field: "table_number" }
      );
    }
    return tableNumber;
  }

  function normalizeLateEntryStarter(value) {
    const starter = normalizeText(value).toLowerCase();
    if (!["late_participant", "bye_participant"].includes(starter)) {
      throw validationError(
        "INVALID_LATE_ENTRY_STARTER",
        "starting_participant must be late_participant or bye_participant",
        { field: "starting_participant" }
      );
    }
    return starter;
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
      can_reopen_current_round: !!currentRound
        && currentRound.status === "completed"
        && tournamentRow.status === "swiss",
      can_add_late_participant: rounds.length === 1
        && currentRound?.round_number === 1
        && tournamentRow.status === "swiss",
      can_cancel_current_round: !!currentRound && tournamentRow.status === "swiss",
      swiss_complete: completedRounds >= swissRoundsCount,
    };
  }

  async function previewSwissRound(tournamentId, payload = {}) {
    return buildSwissPairingPlan(tournamentId, payload?.round_number);
  }

  async function confirmSwissRound(tournamentId, payload = {}) {
    const publishImmediately = payload?.publish === true;
    return enqueueMutation(async () => {
      const outcome = await transaction(async () => {
        const tournament = await requireTournamentRow(tournamentId);
        const rounds = await loadSwissRounds(tournament.id);
        assertSwissRoundSequence(rounds);
        const incompleteRound = rounds.find((round) => round.status !== "completed");
        const fallbackRoundNumber = incompleteRound?.round_number || rounds.length + 1;
        const roundNumber = normalizeRequestedRoundNumber(payload?.round_number, fallbackRoundNumber);
        const existingRound = rounds.find((round) => round.round_number === roundNumber);
        if (existingRound) {
          if (publishImmediately && existingRound.status === "draft") {
            await dbRun(
              db,
              `
                UPDATE in_person_rounds
                SET status = 'published', published_at = CURRENT_TIMESTAMP,
                    revision = revision + 1, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
              `,
              [existingRound.id]
            );
            await touchTournament(tournament.id);
            return { created: false, published: true, round_id: existingRound.id };
          }
          return {
            created: false,
            published: false,
            round_id: existingRound.id,
          };
        }

        const plan = await buildSwissPairingPlan(tournament.id, roundNumber);
        const roundId = `ipr_${idFactory()}`;
        await dbRun(
          db,
          `
            INSERT INTO in_person_rounds (
              id, tournament_id, stage, round_number, status, revision,
              published_at, created_at, updated_at
            ) VALUES (?, ?, 'swiss', ?, ?, 1,
              CASE WHEN ? = 1 THEN CURRENT_TIMESTAMP ELSE NULL END,
              CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
          `,
          [
            roundId,
            tournament.id,
            roundNumber,
            publishImmediately ? "published" : "draft",
            publishImmediately ? 1 : 0,
          ]
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
        return {
          created: true,
          published: publishImmediately,
          round_id: roundId,
        };
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

  async function reopenSwissRound(tournamentId, roundId) {
    return enqueueMutation(async () => {
      const outcome = await transaction(async () => {
        const tournament = await requireTournamentRow(tournamentId);
        if (tournament.status !== "swiss") {
          throw conflictError(
            "INVALID_TOURNAMENT_STATUS",
            "A completed Swiss round can be reopened only during the Swiss stage"
          );
        }
        const requestedRound = await requireSwissRoundRow(tournament.id, roundId);
        const rounds = await loadSwissRounds(tournament.id);
        assertSwissRoundSequence(rounds);
        const lastRound = rounds[rounds.length - 1];
        if (!lastRound || lastRound.id !== requestedRound.id) {
          throw conflictError(
            "NEXT_ROUND_ALREADY_FORMED",
            "Cancel the later Swiss round before reopening this completed round",
            lastRound ? {
              last_round_id: lastRound.id,
              last_round_number: lastRound.round_number,
            } : null
          );
        }
        if (lastRound.status === "published") {
          return {
            reopened: false,
            round_id: lastRound.id,
            round_number: Number(lastRound.round_number),
          };
        }
        if (lastRound.status !== "completed") {
          throw conflictError(
            "ROUND_NOT_COMPLETED",
            "Only the last completed Swiss round can be reopened"
          );
        }
        await dbRun(
          db,
          `
            UPDATE in_person_rounds
            SET status = 'published', completed_at = NULL,
                revision = revision + 1, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
          `,
          [lastRound.id]
        );
        await injectFault("swiss_round_after_reopen", {
          tournament_id: tournament.id,
          round_id: lastRound.id,
        });
        await touchTournament(tournament.id);
        return {
          reopened: true,
          round_id: lastRound.id,
          round_number: Number(lastRound.round_number),
        };
      });
      return { ...(await getSwissOverview(tournamentId)), ...outcome };
    });
  }

  async function previewSwissRoundCancellation(tournamentId, roundId) {
    const tournament = await requireTournamentRow(tournamentId);
    if (tournament.status !== "swiss") {
      throw conflictError(
        "INVALID_TOURNAMENT_STATUS",
        "Swiss rollback is available only during the Swiss stage"
      );
    }
    const rounds = await loadSwissRounds(tournament.id);
    assertSwissRoundSequence(rounds);
    if (!rounds.length) {
      throw conflictError("NO_SWISS_ROUND", "There is no active Swiss round to cancel");
    }
    const lastRound = rounds[rounds.length - 1];
    if (lastRound.id !== normalizeText(roundId)) {
      throw conflictError(
        "NOT_LAST_SWISS_ROUND",
        "Only the last active Swiss round can be cancelled",
        { last_round_id: lastRound.id, last_round_number: lastRound.round_number }
      );
    }
    const previousRevision = await dbGet(
      db,
      `
        SELECT MAX(s.revision) AS revision
        FROM in_person_standings s
        JOIN in_person_rounds r ON r.id = s.source_completed_round_id
        WHERE s.tournament_id = ?
          AND r.stage = 'swiss'
          AND r.status = 'completed'
          AND r.round_number < ?
      `,
      [tournament.id, lastRound.round_number]
    );
    const resultMatches = lastRound.matches.filter((match) => match.status === "completed");
    return {
      round: lastRound,
      completed_results: resultMatches,
      completed_results_count: resultMatches.length,
      previous_standings_revision: Number(previousRevision?.revision || 0),
      returns_to_check_in: lastRound.round_number === 1,
      confirmation_required: true,
    };
  }

  async function cancelSwissRound(tournamentId, roundId, payload = {}, actor = null) {
    const cancellationReason = normalizeRequiredReason(payload?.reason, "reason");
    const actorUserId = normalizeActorUserId(actor);
    return enqueueMutation(async () => {
      const outcome = await transaction(async () => {
        const tournament = await requireTournamentRow(tournamentId);
        const requestedRound = await requireSwissRoundRow(tournament.id, roundId);
        if (requestedRound.status === "cancelled") {
          return {
            cancelled: false,
            round_id: requestedRound.id,
            round_number: Number(requestedRound.round_number),
          };
        }
        if (tournament.status !== "swiss") {
          throw conflictError(
            "INVALID_TOURNAMENT_STATUS",
            "Swiss rollback is available only during the Swiss stage"
          );
        }
        const rounds = await loadSwissRounds(tournament.id);
        assertSwissRoundSequence(rounds);
        const lastRound = rounds[rounds.length - 1];
        if (!lastRound || lastRound.id !== requestedRound.id) {
          throw conflictError(
            "NOT_LAST_SWISS_ROUND",
            "Only the last active Swiss round can be cancelled",
            lastRound ? {
              last_round_id: lastRound.id,
              last_round_number: lastRound.round_number,
            } : null
          );
        }

        await dbRun(
          db,
          `
            UPDATE in_person_matches
            SET status = 'cancelled',
                is_bye = CASE WHEN is_bye = 1 THEN 0 ELSE is_bye END,
                cancelled_at = CURRENT_TIMESTAMP,
                cancelled_by_user_id = ?, cancellation_reason = ?,
                revision = revision + 1, updated_at = CURRENT_TIMESTAMP
            WHERE round_id = ? AND status <> 'cancelled'
          `,
          [actorUserId, cancellationReason, requestedRound.id]
        );
        await injectFault("swiss_cancellation_after_matches", {
          tournament_id: tournament.id,
          round_id: requestedRound.id,
        });
        await dbRun(
          db,
          `
            UPDATE in_person_rounds
            SET status = 'cancelled', cancelled_at = CURRENT_TIMESTAMP,
                cancelled_by_user_id = ?, cancellation_reason = ?,
                revision = revision + 1, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
          `,
          [actorUserId, cancellationReason, requestedRound.id]
        );
        const remainingRoundCount = rounds.length - 1;
        await dbRun(
          db,
          `
            UPDATE in_person_tournaments
            SET status = ?, revision = revision + 1, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
          `,
          [remainingRoundCount ? "swiss" : "check_in", tournament.id]
        );
        await injectFault("swiss_cancellation_after_round", {
          tournament_id: tournament.id,
          round_id: requestedRound.id,
        });
        return {
          cancelled: true,
          round_id: requestedRound.id,
          round_number: Number(requestedRound.round_number),
        };
      });
      return { ...(await getSwissOverview(tournamentId)), ...outcome };
    });
  }

  async function findParticipantSwissResolution(tournamentId, participantId) {
    const row = await dbGet(
      db,
      `
        SELECT
          m.*,
          r.id AS active_round_id,
          r.round_number AS active_round_number,
          r.status AS active_round_status,
          pa.name_en AS participant_a_name_en,
          pa.name_local AS participant_a_name_local,
          pb.name_en AS participant_b_name_en,
          pb.name_local AS participant_b_name_local
        FROM in_person_matches m
        JOIN in_person_rounds r ON r.id = m.round_id
        LEFT JOIN in_person_participants pa ON pa.id = m.participant_a_id
        LEFT JOIN in_person_participants pb ON pb.id = m.participant_b_id
        WHERE r.tournament_id = ?
          AND r.stage = 'swiss'
          AND r.status IN ('draft', 'published')
          AND m.status <> 'cancelled'
          AND (m.participant_a_id = ? OR m.participant_b_id = ?)
        ORDER BY r.round_number DESC, m.id
        LIMIT 1
      `,
      [tournamentId, participantId, participantId]
    );
    if (!row || row.status === "completed" || Number(row.is_bye) === 1) return null;
    const match = serializeSwissMatch(row);
    const opponentId = row.participant_a_id === participantId
      ? row.participant_b_id
      : row.participant_a_id;
    if (row.active_round_status === "draft") {
      return {
        type: "cancel_draft_round",
        round_id: row.active_round_id,
        round_number: Number(row.active_round_number),
        match,
      };
    }
    return {
      type: "technical_result",
      round_id: row.active_round_id,
      round_number: Number(row.active_round_number),
      match,
      suggested_winner_participant_id: opponentId,
    };
  }

  async function findParticipantPlayoffResolution(tournamentId, participantId) {
    const row = await dbGet(
      db,
      `
        SELECT
          m.*,
          r.id AS active_round_id,
          r.round_key AS active_round_key,
          r.status AS active_round_status,
          pa.name_en AS participant_a_name_en,
          pa.name_local AS participant_a_name_local,
          pb.name_en AS participant_b_name_en,
          pb.name_local AS participant_b_name_local
        FROM in_person_matches m
        JOIN in_person_rounds r ON r.id = m.round_id
        LEFT JOIN in_person_participants pa ON pa.id = m.participant_a_id
        LEFT JOIN in_person_participants pb ON pb.id = m.participant_b_id
        WHERE r.tournament_id = ?
          AND r.stage = 'playoff'
          AND r.status IN ('draft', 'published')
          AND m.status = 'scheduled'
          AND m.participant_a_id IS NOT NULL
          AND m.participant_b_id IS NOT NULL
          AND (m.participant_a_id = ? OR m.participant_b_id = ?)
        ORDER BY r.round_order, m.bracket_position, m.id
        LIMIT 1
      `,
      [tournamentId, participantId, participantId]
    );
    if (!row) return null;
    const match = serializePlayoffMatch(row);
    const opponentId = row.participant_a_id === participantId
      ? row.participant_b_id
      : row.participant_a_id;
    return {
      type: "technical_result",
      stage: "playoff",
      round_id: row.active_round_id,
      round_key: row.active_round_key,
      round_label: getPlayoffRoundLabel(row.active_round_key),
      match,
      suggested_winner_participant_id: opponentId,
    };
  }

  async function setParticipantInactive(tournamentId, participantId, payload = {}) {
    const status = normalizeText(payload?.status).toLowerCase();
    if (!["withdrawn", "disqualified"].includes(status)) {
      throw validationError(
        "INVALID_PARTICIPANT_STATUS",
        "status must be withdrawn or disqualified",
        { field: "status" }
      );
    }
    const reason = normalizeRequiredReason(payload?.reason, "reason");
    return enqueueMutation(async () => {
      const outcome = await transaction(async () => {
        const tournament = await requireTournamentRow(tournamentId);
        assertParticipantMutationsAllowed(tournament);
        if (!["swiss", "playoff"].includes(tournament.status)) {
          throw conflictError(
            "TOURNAMENT_STAGE_NOT_ACTIVE",
            "A participant can be withdrawn or disqualified only during Swiss or playoff"
          );
        }
        const current = await requireParticipantRow(tournament.id, participantId);
        const unchanged = current.status === status && current.status_reason === reason;
        if (!unchanged) {
          await dbRun(
            db,
            `
              UPDATE in_person_participants
              SET status = ?, draw_number = NULL, status_reason = ?,
                  withdrawn_at = CASE WHEN ? = 'withdrawn' THEN CURRENT_TIMESTAMP ELSE NULL END,
                  disqualified_at = CASE WHEN ? = 'disqualified' THEN CURRENT_TIMESTAMP ELSE NULL END,
                  updated_at = CURRENT_TIMESTAMP
              WHERE id = ? AND tournament_id = ?
            `,
            [status, reason, status, status, current.id, tournament.id]
          );
          await touchTournament(tournament.id);
        }
        return {
          changed: !unchanged,
          participant_id: current.id,
          status,
          tournament_stage: tournament.status,
        };
      });
      const participant = serializeParticipant(
        await requireParticipantRow(tournamentId, outcome.participant_id)
      );
      const resolution = outcome.tournament_stage === "playoff"
        ? await findParticipantPlayoffResolution(tournamentId, outcome.participant_id)
        : await findParticipantSwissResolution(tournamentId, outcome.participant_id);
      if (resolution?.type === "technical_result") {
        resolution.suggested_result = {
          starting_participant_id: resolution.match.starting_participant_id
            || resolution.match.participant_a_id,
          result_type: "technical",
          winner_participant_id: resolution.suggested_winner_participant_id,
          finish_reason: outcome.status === "withdrawn" ? "withdrawal" : "disqualification",
        };
      }
      return { ...outcome, participant, resolution };
    });
  }

  async function buildLateParticipantPreview(tournamentId, payload = {}) {
    const tournament = await requireTournamentRow(tournamentId);
    if (tournament.status !== "swiss") {
      throw conflictError(
        "INVALID_TOURNAMENT_STATUS",
        "A late participant can be added only during the Swiss stage"
      );
    }
    const rounds = await loadSwissRounds(tournament.id);
    assertSwissRoundSequence(rounds);
    if (rounds.length !== 1 || rounds[0].round_number !== 1) {
      throw conflictError(
        "LATE_ENTRY_WINDOW_CLOSED",
        "A late participant can be added only while round 1 is the last active Swiss round"
      );
    }
    const round = rounds[0];
    const input = await validateParticipantRelations(
      normalizeParticipantInput(payload, null, tournament),
      tournament
    );
    await validateParticipantDuplicate(tournament.id, input, {
      confirmed: payload?.confirm_duplicate === true,
    });
    const drawNumber = normalizeDrawNumber(payload?.draw_number) ?? null;
    if (drawNumber != null) {
      const duplicate = await dbGet(
        db,
        `SELECT id, name_en FROM in_person_participants
         WHERE tournament_id = ? AND draw_number = ? LIMIT 1`,
        [tournament.id, drawNumber]
      );
      if (duplicate) {
        throw conflictError("DRAW_NUMBER_TAKEN", "This draw number is already assigned", {
          draw_number: drawNumber,
          participant: { id: duplicate.id, name_en: duplicate.name_en },
        });
      }
    }

    const byeMatches = round.matches
      .filter((match) => match.is_bye)
      .sort((left, right) => left.id.localeCompare(right.id));
    const mode = byeMatches.length ? "pair_with_bye" : "late_bye";
    const requestedMode = normalizeOptionalText(payload?.mode);
    if (requestedMode && normalizeLateEntryMode(requestedMode) !== mode) {
      throw conflictError(
        "LATE_ENTRY_MODE_REQUIRED",
        mode === "pair_with_bye"
          ? "The late participant must be paired with the active first-round bye recipient"
          : "A late participant receives a bye because the first round has no active bye",
        { required_mode: mode }
      );
    }
    const preview = {
      tournament_revision: Number(tournament.revision),
      round: {
        id: round.id,
        round_number: round.round_number,
        status: round.status,
        revision: round.revision,
      },
      participant: { ...input, draw_number: drawNumber },
      mode,
      reopens_completed_round: round.status === "completed",
      confirmation_required: true,
    };
    if (mode === "late_bye") {
      return {
        ...preview,
        change: { type: "create_late_bye" },
      };
    }

    const requestedByeMatchId = normalizeOptionalText(payload?.bye_match_id);
    const byeMatch = requestedByeMatchId
      ? byeMatches.find((match) => match.id === requestedByeMatchId)
      : byeMatches[0];
    if (!byeMatch) {
      throw conflictError(
        "FIRST_ROUND_BYE_NOT_FOUND",
        "pair_with_bye requires an active bye in the first Swiss round"
      );
    }
    const tableNumber = normalizePositiveTableNumber(payload?.table_number);
    const occupiedTable = round.matches.find((match) => match.table_number === tableNumber);
    if (occupiedTable) {
      throw conflictError("TABLE_NUMBER_TAKEN", "This table number is already used in the round", {
        table_number: tableNumber,
        match_id: occupiedTable.id,
      });
    }
    const startingParticipant = normalizeLateEntryStarter(payload?.starting_participant);
    return {
      ...preview,
      bye_match: byeMatch,
      table_number: tableNumber,
      starting_participant: startingParticipant,
      change: {
        type: "replace_bye_with_match",
        bye_participant_id: byeMatch.participant_a_id,
        bye_participant_name_en: byeMatch.participant_a_name_en,
      },
    };
  }

  async function previewLateParticipant(tournamentId, payload = {}) {
    return buildLateParticipantPreview(tournamentId, payload);
  }

  async function confirmLateParticipant(tournamentId, payload = {}, actor = null) {
    const actorUserId = normalizeActorUserId(actor);
    return enqueueMutation(async () => {
      const outcome = await transaction(async () => {
        const preview = await buildLateParticipantPreview(tournamentId, payload);
        if (
          payload?.expected_round_revision !== undefined
          && Number(payload.expected_round_revision) !== Number(preview.round.revision)
        ) {
          throw conflictError(
            "LATE_ENTRY_PREVIEW_STALE",
            "The first round changed after the late-entry preview. Preview it again."
          );
        }
        const participantId = `ipp_${idFactory()}`;
        const participant = preview.participant;
        await dbRun(
          db,
          `
            INSERT INTO in_person_participants (
              id, tournament_id, name_en, name_local, bga_nickname,
              association_id, city_id, status, draw_number, checked_in_at,
              is_late_entry, late_entry_mode, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'checked_in', ?, CURRENT_TIMESTAMP,
              1, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
          `,
          [
            participantId,
            tournamentId,
            participant.name_en,
            participant.name_local,
            participant.bga_nickname,
            participant.association_id,
            participant.city_id,
            participant.draw_number,
            preview.mode,
          ]
        );

        let matchId;
        if (preview.mode === "pair_with_bye") {
          await dbRun(
            db,
            `
              UPDATE in_person_matches
              SET status = 'cancelled', is_bye = 0, cancelled_at = CURRENT_TIMESTAMP,
                  cancelled_by_user_id = ?,
                  cancellation_reason = 'Replaced by late participant match',
                  revision = revision + 1, updated_at = CURRENT_TIMESTAMP
              WHERE id = ? AND round_id = ? AND status <> 'cancelled'
            `,
            [actorUserId, preview.bye_match.id, preview.round.id]
          );
          matchId = `ipm_${idFactory()}`;
          const startingParticipantId = preview.starting_participant === "late_participant"
            ? participantId
            : preview.bye_match.participant_a_id;
          await dbRun(
            db,
            `
              INSERT INTO in_person_matches (
                id, round_id, table_number, participant_a_id, participant_b_id,
                starting_participant_id, status, is_bye, revision,
                created_at, updated_at
              ) VALUES (?, ?, ?, ?, ?, ?, 'scheduled', 0, 1,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            `,
            [
              matchId,
              preview.round.id,
              preview.table_number,
              preview.bye_match.participant_a_id,
              participantId,
              startingParticipantId,
            ]
          );
        } else {
          matchId = `ipm_${idFactory()}`;
          await dbRun(
            db,
            `
              INSERT INTO in_person_matches (
                id, round_id, table_number, participant_a_id, participant_b_id,
                starting_participant_id, status, is_bye, result_type,
                winner_participant_id, loser_participant_id, revision,
                created_at, updated_at
              ) VALUES (?, ?, NULL, ?, NULL, NULL, 'completed', 1, 'bye', ?, NULL, 1,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            `,
            [matchId, preview.round.id, participantId, participantId]
          );
        }
        await injectFault("late_participant_after_match", {
          tournament_id: tournamentId,
          participant_id: participantId,
          match_id: matchId,
        });
        await dbRun(
          db,
          `
            UPDATE in_person_rounds
            SET status = CASE WHEN status = 'completed' THEN 'published' ELSE status END,
                completed_at = CASE WHEN status = 'completed' THEN NULL ELSE completed_at END,
                revision = revision + 1, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
          `,
          [preview.round.id]
        );
        await touchTournament(tournamentId);
        return {
          participant_id: participantId,
          match_id: matchId,
          mode: preview.mode,
          round_id: preview.round.id,
        };
      });
      const [participantRow, overview] = await Promise.all([
        requireParticipantRow(tournamentId, outcome.participant_id),
        getSwissOverview(tournamentId),
      ]);
      const match = overview.current_round?.matches.find((entry) => entry.id === outcome.match_id) || null;
      return {
        ...overview,
        ...outcome,
        participant: serializeParticipant(participantRow),
        match,
        created: true,
      };
    });
  }

  async function loadPlayoffMatchRows(roundId) {
    const rows = await dbAll(
      db,
      `${PLAYOFF_MATCH_SELECT}
       WHERE m.round_id = ? AND m.status <> 'cancelled'
       ORDER BY m.bracket_position, m.id`,
      [roundId]
    );
    return rows.map(serializePlayoffMatch);
  }

  async function loadPlayoffRounds(tournamentId) {
    const rows = await dbAll(
      db,
      `
        SELECT *
        FROM in_person_rounds
        WHERE tournament_id = ? AND stage = 'playoff' AND status <> 'cancelled'
        ORDER BY round_order, round_key, id
      `,
      [tournamentId]
    );
    const rounds = [];
    for (const row of rows) {
      rounds.push(serializePlayoffRound(row, await loadPlayoffMatchRows(row.id)));
    }
    return rounds;
  }

  async function requirePlayoffRoundRow(tournamentId, roundId) {
    const row = await dbGet(
      db,
      `SELECT * FROM in_person_rounds
       WHERE id = ? AND tournament_id = ? AND stage = 'playoff' LIMIT 1`,
      [normalizeText(roundId), tournamentId]
    );
    if (!row) throw notFoundError("PLAYOFF_ROUND_NOT_FOUND", "Playoff round not found");
    return row;
  }

  async function requirePlayoffMatchRow(tournamentId, matchId) {
    const row = await dbGet(
      db,
      `
        SELECT m.*, r.tournament_id, r.round_key, r.round_order,
               r.status AS round_status
        FROM in_person_matches m
        JOIN in_person_rounds r ON r.id = m.round_id
        WHERE m.id = ? AND r.tournament_id = ? AND r.stage = 'playoff'
        LIMIT 1
      `,
      [normalizeText(matchId), tournamentId]
    );
    if (!row) throw notFoundError("PLAYOFF_MATCH_NOT_FOUND", "Playoff match not found");
    return row;
  }

  function playoffPlacements(rounds) {
    const finalMatch = rounds.find((round) => round.round_key === "final")?.matches?.[0];
    const bronzeMatch = rounds.find((round) => (
      round.round_key === "bronze_medal_match"
    ))?.matches?.[0];
    if (finalMatch?.status !== "completed" || bronzeMatch?.status !== "completed") return null;
    return {
      first: finalMatch.winner_participant_id,
      second: finalMatch.loser_participant_id,
      third: bronzeMatch.winner_participant_id,
      fourth: bronzeMatch.loser_participant_id,
    };
  }

  async function getPlayoffOverview(tournamentId) {
    const tournamentRow = await requireTournamentRow(tournamentId);
    const [rounds, standings, participantRows, swissRounds] = await Promise.all([
      loadPlayoffRounds(tournamentRow.id),
      loadLatestSwissStandings(tournamentRow.id),
      loadParticipantRows(tournamentRow.id),
      loadSwissRounds(tournamentRow.id),
    ]);
    const expectedSwissRounds = Number(tournamentRow.swiss_rounds_count);
    const lastSwissRound = swissRounds[swissRounds.length - 1] || null;
    const swissComplete = swissRounds.length === expectedSwissRounds
      && swissRounds.every((round) => round.status === "completed")
      && standings.revision > 0
      && standings.source_completed_round_id === lastSwissRound?.id;
    const placements = playoffPlacements(rounds);
    const finalComplete = rounds.find((round) => round.round_key === "final")
      ?.matches?.every((match) => match.status === "completed") || false;
    const bronzeComplete = rounds.find((round) => round.round_key === "bronze_medal_match")
      ?.matches?.every((match) => match.status === "completed") || false;
    const hasCompletedPlayoffMatch = rounds.some((round) => (
      round.matches?.some((match) => match.status === "completed")
    ));
    return {
      tournament: serializeOrganizerTournament(tournamentRow),
      first_round: tournamentRow.playoff_first_round,
      participant_count: Number(getPlayoffPreview(tournamentRow.playoff_first_round)?.participant_count || 0),
      standings,
      eligible_participants: participantRows
        .filter((participant) => !["withdrawn", "disqualified"].includes(participant.status))
        .map(serializeParticipant),
      rounds,
      placements,
      can_start: tournamentRow.status === "swiss" && swissComplete && rounds.length === 0,
      can_reset: tournamentRow.status === "playoff"
        && rounds.length > 0
        && !hasCompletedPlayoffMatch,
      can_complete: tournamentRow.status === "playoff" && finalComplete && bronzeComplete,
      completion_blockers: [
        ...(!finalComplete ? ["Final must be completed."] : []),
        ...(!bronzeComplete ? ["Bronze medal match must be completed."] : []),
      ],
      swiss_complete: swissComplete,
    };
  }

  function normalizePlayoffParticipantIds(payload = {}) {
    if (Array.isArray(payload?.participant_ids)) return payload.participant_ids;
    if (Array.isArray(payload?.slots)) {
      return payload.slots
        .slice()
        .sort((left, right) => Number(left?.slot_number || 0) - Number(right?.slot_number || 0))
        .map((slot) => slot?.participant_id);
    }
    return payload?.participant_ids;
  }

  async function buildPlayoffPreview(tournamentId, payload = {}) {
    const tournament = await requireTournamentRow(tournamentId);
    if (tournament.status !== "swiss") {
      throw conflictError(
        "INVALID_TOURNAMENT_STATUS",
        "The playoff can start only after the Swiss stage"
      );
    }
    const [existingRounds, swissRounds, standings, participantRows] = await Promise.all([
      loadPlayoffRounds(tournament.id),
      loadSwissRounds(tournament.id),
      loadLatestSwissStandings(tournament.id),
      loadParticipantRows(tournament.id),
    ]);
    if (existingRounds.length) {
      throw conflictError("PLAYOFF_ALREADY_CREATED", "The playoff bracket is already created");
    }
    const expectedSwissRounds = Number(tournament.swiss_rounds_count);
    const lastSwissRound = swissRounds[swissRounds.length - 1] || null;
    if (
      swissRounds.length !== expectedSwissRounds
      || swissRounds.some((round) => round.status !== "completed")
    ) {
      throw conflictError(
        "SWISS_NOT_COMPLETE",
        `Complete all ${expectedSwissRounds} Swiss rounds before starting the playoff`
      );
    }
    if (!standings.revision || standings.source_completed_round_id !== lastSwissRound?.id) {
      throw conflictError(
        "STANDINGS_NOT_CURRENT",
        "Final Swiss standings must be rebuilt before starting the playoff"
      );
    }
    let bracket;
    try {
      bracket = buildPlayoffBracket({
        first_round: tournament.playoff_first_round,
        participant_ids: normalizePlayoffParticipantIds(payload),
      });
    } catch (error) {
      if (error instanceof InPersonPlayoffError) {
        throw validationError(error.code, error.message, error.details);
      }
      throw error;
    }
    const participantsById = new Map(participantRows.map((participant) => [participant.id, participant]));
    const unknownParticipantIds = bracket.participant_ids.filter((participantId) => (
      !participantsById.has(participantId)
    ));
    if (unknownParticipantIds.length) {
      throw validationError(
        "UNKNOWN_PLAYOFF_PARTICIPANT",
        "Every playoff slot must reference a participant from this tournament",
        { participant_ids: unknownParticipantIds }
      );
    }
    const inactiveParticipants = bracket.participant_ids
      .map((participantId) => participantsById.get(participantId))
      .filter((participant) => ["withdrawn", "disqualified"].includes(participant.status));
    if (inactiveParticipants.length) {
      throw conflictError(
        "INACTIVE_PLAYOFF_PARTICIPANT",
        "Withdrawn or disqualified participants must be replaced before starting the playoff",
        {
          participants: inactiveParticipants.map((participant) => ({
            id: participant.id,
            name_en: participant.name_en,
            status: participant.status,
          })),
        }
      );
    }
    return {
      ...bracket,
      tournament_revision: Number(tournament.revision),
      standings_revision: Number(standings.revision),
      confirmation_required: true,
      rounds: bracket.rounds.map((round) => ({
        ...round,
        matches: round.matches.map((match) => ({
          ...match,
          participant_a_name_en: participantsById.get(match.participant_a_id)?.name_en || null,
          participant_b_name_en: participantsById.get(match.participant_b_id)?.name_en || null,
        })),
      })),
    };
  }

  async function previewPlayoff(tournamentId, payload = {}) {
    return buildPlayoffPreview(tournamentId, payload);
  }

  async function confirmPlayoff(tournamentId, payload = {}) {
    return enqueueMutation(async () => {
      const outcome = await transaction(async () => {
        const tournament = await requireTournamentRow(tournamentId);
        const existingRounds = await loadPlayoffRounds(tournament.id);
        if (existingRounds.length) {
          return { created: false };
        }
        const preview = await buildPlayoffPreview(tournament.id, payload);
        if (
          payload?.expected_tournament_revision !== undefined
          && Number(payload.expected_tournament_revision) !== preview.tournament_revision
        ) {
          throw conflictError(
            "PLAYOFF_PREVIEW_STALE",
            "The tournament changed after this playoff preview. Preview the bracket again."
          );
        }
        if (
          payload?.expected_standings_revision !== undefined
          && Number(payload.expected_standings_revision) !== preview.standings_revision
        ) {
          throw conflictError(
            "PLAYOFF_PREVIEW_STALE",
            "Swiss standings changed after this playoff preview. Preview the bracket again."
          );
        }
        const roundIds = new Map();
        const matchIds = new Map();
        preview.rounds.forEach((round) => {
          roundIds.set(round.round_key, `ipr_${idFactory()}`);
          round.matches.forEach((match) => matchIds.set(match.key, `ipm_${idFactory()}`));
        });
        for (const round of preview.rounds) {
          const isFirstRound = round.round_key === preview.first_round;
          await dbRun(
            db,
            `
              INSERT INTO in_person_rounds (
                id, tournament_id, stage, round_key, round_order, status,
                revision, published_at, created_at, updated_at
              ) VALUES (?, ?, 'playoff', ?, ?, ?, 1,
                CASE WHEN ? = 1 THEN CURRENT_TIMESTAMP ELSE NULL END,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            `,
            [
              roundIds.get(round.round_key),
              tournament.id,
              round.round_key,
              round.round_order,
              isFirstRound ? "published" : "draft",
              isFirstRound ? 1 : 0,
            ]
          );
        }
        for (const round of preview.rounds) {
          for (const match of round.matches) {
            await dbRun(
              db,
              `
                INSERT INTO in_person_matches (
                  id, round_id, bracket_position, table_number,
                  participant_a_id, participant_b_id, status, is_bye, revision,
                  created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'scheduled', 0, 1,
                  CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
              `,
              [
                matchIds.get(match.key),
                roundIds.get(round.round_key),
                match.bracket_position,
                match.table_number,
                match.participant_a_id,
                match.participant_b_id,
              ]
            );
          }
        }
        for (const round of preview.rounds) {
          for (const match of round.matches) {
            await dbRun(
              db,
              `
                UPDATE in_person_matches
                SET next_match_for_winner_id = ?, next_match_for_winner_slot = ?,
                    next_match_for_loser_id = ?, next_match_for_loser_slot = ?
                WHERE id = ?
              `,
              [
                match.next_match_for_winner_key
                  ? matchIds.get(match.next_match_for_winner_key)
                  : null,
                match.next_match_for_winner_slot,
                match.next_match_for_loser_key
                  ? matchIds.get(match.next_match_for_loser_key)
                  : null,
                match.next_match_for_loser_slot,
                matchIds.get(match.key),
              ]
            );
          }
        }
        await injectFault("playoff_after_bracket_insert", { tournament_id: tournament.id });
        await dbRun(
          db,
          `UPDATE in_person_tournaments
           SET status = 'playoff', revision = revision + 1, updated_at = CURRENT_TIMESTAMP
           WHERE id = ?`,
          [tournament.id]
        );
        return { created: true };
      });
      return { ...(await getPlayoffOverview(tournamentId)), ...outcome };
    });
  }

  function assertPlayoffRoundPublishable(round) {
    if (round.publish_blockers?.length) {
      const firstBlocker = round.publish_blockers[0];
      throw conflictError(firstBlocker.code, firstBlocker.message, {
        blockers: round.publish_blockers,
      });
    }
  }

  async function publishPlayoffRound(tournamentId, roundId) {
    return enqueueMutation(async () => {
      const outcome = await transaction(async () => {
        const tournament = await requireTournamentRow(tournamentId);
        if (tournament.status !== "playoff") {
          throw conflictError("INVALID_TOURNAMENT_STATUS", "The tournament is not in the playoff stage");
        }
        const row = await requirePlayoffRoundRow(tournament.id, roundId);
        const isMedalRound = ["final", "bronze_medal_match"].includes(row.round_key);
        const targetRows = isMedalRound
          ? await dbAll(
            db,
            `SELECT * FROM in_person_rounds
             WHERE tournament_id = ? AND stage = 'playoff'
               AND round_key IN ('final', 'bronze_medal_match')
               AND status <> 'cancelled'
             ORDER BY round_key, id`,
            [tournament.id]
          )
          : [row];
        if (isMedalRound && targetRows.length !== 2) {
          throw conflictError(
            "PLAYOFF_MEDAL_ROUND_INCOMPLETE",
            "Final and Bronze medal match must be published together"
          );
        }
        const draftRows = [];
        for (const targetRow of targetRows) {
          if (["published", "completed"].includes(targetRow.status)) continue;
          if (targetRow.status !== "draft") {
            throw conflictError("INVALID_ROUND_STATUS", "Only a draft playoff round can be published");
          }
          const round = serializePlayoffRound(
            targetRow,
            await loadPlayoffMatchRows(targetRow.id)
          );
          assertPlayoffRoundPublishable(round);
          draftRows.push(targetRow);
        }
        for (const draftRow of draftRows) {
          await dbRun(
            db,
            `UPDATE in_person_rounds
             SET status = 'published', published_at = CURRENT_TIMESTAMP,
                 revision = revision + 1, updated_at = CURRENT_TIMESTAMP
             WHERE id = ?`,
            [draftRow.id]
          );
        }
        if (draftRows.length) await touchTournament(tournament.id);
        return {
          published: draftRows.length > 0,
          round_id: row.id,
          round_ids: targetRows.map((targetRow) => targetRow.id),
        };
      });
      return { ...(await getPlayoffOverview(tournamentId)), ...outcome };
    });
  }

  async function resetPlayoff(tournamentId, payload = {}, actor = null) {
    const reason = normalizeRequiredReason(payload?.reason, "reason");
    const actorUserId = normalizeActorUserId(actor);
    return enqueueMutation(async () => {
      const outcome = await transaction(async () => {
        const tournament = await requireTournamentRow(tournamentId);
        if (tournament.status !== "playoff") {
          throw conflictError(
            "INVALID_TOURNAMENT_STATUS",
            "The playoff bracket can be reset only during the playoff stage"
          );
        }
        const completedMatches = await dbAll(
          db,
          `
            SELECT m.id, m.table_number, r.round_key
            FROM in_person_matches m
            JOIN in_person_rounds r ON r.id = m.round_id
            WHERE r.tournament_id = ?
              AND r.stage = 'playoff'
              AND r.status <> 'cancelled'
              AND m.status = 'completed'
            ORDER BY r.round_order, r.round_key, m.bracket_position, m.id
          `,
          [tournament.id]
        );
        if (completedMatches.length) {
          throw conflictError(
            "PLAYOFF_RESULTS_EXIST",
            "The playoff bracket cannot be reset after a result has been entered",
            {
              matches: completedMatches.map((match) => ({
                id: match.id,
                round_key: match.round_key,
                table_number: match.table_number == null ? null : Number(match.table_number),
              })),
            }
          );
        }
        const activeRound = await dbGet(
          db,
          `SELECT id FROM in_person_rounds
           WHERE tournament_id = ? AND stage = 'playoff' AND status <> 'cancelled'
           LIMIT 1`,
          [tournament.id]
        );
        if (!activeRound) {
          throw conflictError("PLAYOFF_NOT_CREATED", "There is no active playoff bracket to reset");
        }
        const firstRoundMatches = await dbAll(
          db,
          `
            SELECT m.participant_a_id, m.participant_b_id
            FROM in_person_matches m
            JOIN in_person_rounds r ON r.id = m.round_id
            WHERE r.tournament_id = ?
              AND r.stage = 'playoff'
              AND r.round_key = ?
              AND r.status <> 'cancelled'
              AND m.status <> 'cancelled'
            ORDER BY m.bracket_position, m.id
          `,
          [tournament.id, tournament.playoff_first_round]
        );
        const participantIds = firstRoundMatches.flatMap((match) => ([
          match.participant_a_id,
          match.participant_b_id,
        ]));
        await dbRun(
          db,
          `
            UPDATE in_person_matches
            SET status = 'cancelled', cancelled_at = CURRENT_TIMESTAMP,
                cancelled_by_user_id = ?, cancellation_reason = ?,
                revision = revision + 1, updated_at = CURRENT_TIMESTAMP
            WHERE round_id IN (
              SELECT id FROM in_person_rounds
              WHERE tournament_id = ? AND stage = 'playoff' AND status <> 'cancelled'
            )
              AND status <> 'cancelled'
          `,
          [actorUserId, reason, tournament.id]
        );
        await injectFault("playoff_reset_after_matches", { tournament_id: tournament.id });
        await dbRun(
          db,
          `
            UPDATE in_person_rounds
            SET status = 'cancelled', cancelled_at = CURRENT_TIMESTAMP,
                cancelled_by_user_id = ?, cancellation_reason = ?,
                revision = revision + 1, updated_at = CURRENT_TIMESTAMP
            WHERE tournament_id = ? AND stage = 'playoff' AND status <> 'cancelled'
          `,
          [actorUserId, reason, tournament.id]
        );
        await dbRun(
          db,
          `UPDATE in_person_tournaments
           SET status = 'swiss', revision = revision + 1, updated_at = CURRENT_TIMESTAMP
           WHERE id = ?`,
          [tournament.id]
        );
        await injectFault("playoff_reset_after_rounds", { tournament_id: tournament.id });
        return { reset: true, participant_ids: participantIds };
      });
      return { ...(await getPlayoffOverview(tournamentId)), ...outcome };
    });
  }

  async function setPlayoffMatchTable(tournamentId, matchId, payload = {}) {
    const tableNumber = normalizePositiveTableNumber(payload?.table_number);
    return enqueueMutation(async () => {
      const outcome = await transaction(async () => {
        const tournament = await requireTournamentRow(tournamentId);
        if (tournament.status !== "playoff") {
          throw conflictError("INVALID_TOURNAMENT_STATUS", "Playoff tables can be changed only during the playoff");
        }
        const match = await requirePlayoffMatchRow(tournament.id, matchId);
        if (!["draft", "published"].includes(match.round_status) || match.status === "completed") {
          throw conflictError(
            "PLAYOFF_TABLE_LOCKED",
            "A playoff table cannot be changed after this match has a result"
          );
        }
        const currentTableNumber = match.table_number == null ? null : Number(match.table_number);
        const fixedMedalTableNumber = match.round_key === "final"
          ? 1
          : match.round_key === "bronze_medal_match"
            ? 2
            : null;
        if (fixedMedalTableNumber !== null && tableNumber !== fixedMedalTableNumber) {
          throw conflictError(
            "PLAYOFF_MEDAL_TABLE_LOCKED",
            `${getPlayoffRoundLabel(match.round_key)} must use table ${fixedMedalTableNumber}`,
            { round_key: match.round_key, table_number: fixedMedalTableNumber }
          );
        }
        if (currentTableNumber === tableNumber) {
          return { changed: false, match_id: match.id };
        }
        const occupied = await dbGet(
          db,
          `SELECT id, status, table_number FROM in_person_matches
           WHERE round_id = ? AND status <> 'cancelled' AND table_number = ? AND id <> ? LIMIT 1`,
          [match.round_id, tableNumber, match.id]
        );
        if (occupied?.status === "completed") {
          throw conflictError(
            "PLAYOFF_TABLE_LOCKED",
            "The requested table belongs to a match that already has a result",
            { match_id: occupied.id, table_number: tableNumber }
          );
        }
        if (match.round_status === "published" && currentTableNumber === 1 && !occupied) {
          throw conflictError(
            "STREAMING_TABLE_REQUIRED",
            "A published playoff round must keep exactly one streaming table 1"
          );
        }
        await dbRun(db, "UPDATE in_person_matches SET table_number = NULL WHERE id = ?", [match.id]);
        if (occupied) {
          await dbRun(
            db,
            `UPDATE in_person_matches
             SET table_number = ?, revision = revision + 1, updated_at = CURRENT_TIMESTAMP
             WHERE id = ?`,
            [currentTableNumber, occupied.id]
          );
        }
        await dbRun(
          db,
          `UPDATE in_person_matches
           SET table_number = ?, revision = revision + 1, updated_at = CURRENT_TIMESTAMP
           WHERE id = ?`,
          [tableNumber, match.id]
        );
        await injectFault("playoff_after_table_swap", {
          tournament_id: tournament.id,
          match_id: match.id,
          occupied_match_id: occupied?.id || null,
        });
        await touchTournament(tournament.id);
        return { changed: true, match_id: match.id, swapped_match_id: occupied?.id || null };
      });
      return { ...(await getPlayoffOverview(tournamentId)), ...outcome };
    });
  }

  async function completedPlayoffDescendants(tournamentId, rootMatchId) {
    const rows = await dbAll(
      db,
      `
        SELECT m.id, m.status, m.next_match_for_winner_id, m.next_match_for_loser_id,
               r.round_key
        FROM in_person_matches m
        JOIN in_person_rounds r ON r.id = m.round_id
        WHERE r.tournament_id = ? AND r.stage = 'playoff' AND m.status <> 'cancelled'
      `,
      [tournamentId]
    );
    const byId = new Map(rows.map((row) => [row.id, row]));
    const root = byId.get(rootMatchId);
    const queue = [root?.next_match_for_winner_id, root?.next_match_for_loser_id].filter(Boolean);
    const seen = new Set();
    const completed = [];
    while (queue.length) {
      const id = queue.shift();
      if (seen.has(id)) continue;
      seen.add(id);
      const row = byId.get(id);
      if (!row) continue;
      if (row.status === "completed") completed.push({ id: row.id, round_key: row.round_key });
      queue.push(row.next_match_for_winner_id, row.next_match_for_loser_id);
    }
    return completed;
  }

  async function propagatePlayoffParticipant(targetMatchId, targetSlot, participantId) {
    if (!targetMatchId || !targetSlot) return;
    if (!["participant_a", "participant_b"].includes(targetSlot)) {
      throw conflictError("INVALID_PLAYOFF_ROUTE", "The playoff bracket contains an invalid target slot");
    }
    const target = await dbGet(db, "SELECT * FROM in_person_matches WHERE id = ? LIMIT 1", [targetMatchId]);
    if (!target || target.status === "cancelled") {
      throw conflictError("INVALID_PLAYOFF_ROUTE", "The playoff bracket target match is unavailable");
    }
    if (target.status === "completed") {
      throw conflictError(
        "PLAYOFF_DESCENDANT_PLAYED",
        "The result cannot be corrected because a dependent playoff match has already been played",
        { descendant_match_ids: [target.id] }
      );
    }
    const otherSlot = targetSlot === "participant_a" ? "participant_b_id" : "participant_a_id";
    if (target[otherSlot] === participantId) {
      throw conflictError(
        "PLAYOFF_PROPAGATION_CONFLICT",
        "A participant cannot occupy both slots of a playoff match",
        { match_id: target.id, participant_id: participantId }
      );
    }
    const column = `${targetSlot}_id`;
    await dbRun(
      db,
      `UPDATE in_person_matches
       SET ${column} = ?, starting_participant_id = NULL,
           revision = revision + 1, updated_at = CURRENT_TIMESTAMP
       WHERE id = ?`,
      [participantId, target.id]
    );
  }

  async function savePlayoffMatchResult(tournamentId, matchId, payload = {}) {
    return enqueueMutation(async () => {
      const outcome = await transaction(async () => {
        const tournament = await requireTournamentRow(tournamentId);
        if (tournament.status !== "playoff") {
          throw conflictError("INVALID_TOURNAMENT_STATUS", "Playoff results are read-only outside the playoff stage");
        }
        const match = await requirePlayoffMatchRow(tournament.id, matchId);
        if (match.status === "cancelled") {
          throw conflictError("MATCH_CANCELLED", "A cancelled match cannot receive a result");
        }
        if (!match.participant_a_id || !match.participant_b_id) {
          throw conflictError(
            "PLAYOFF_PARTICIPANTS_PENDING",
            "Both playoff participants must be known before entering a result"
          );
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
        if (!["published", "completed"].includes(match.round_status)) {
          throw conflictError("ROUND_NOT_PUBLISHED", "Publish the playoff round before entering results");
        }
        if (match.status === "completed") {
          const playedDescendants = await completedPlayoffDescendants(tournament.id, match.id);
          if (playedDescendants.length) {
            throw conflictError(
              "PLAYOFF_DESCENDANT_PLAYED",
              "The result cannot be corrected because a dependent playoff match has already been played",
              { descendants: playedDescendants }
            );
          }
        }
        await dbRun(
          db,
          `
            UPDATE in_person_matches
            SET starting_participant_id = ?, status = ?, is_bye = 0, result_type = ?,
                points_a = ?, points_b = ?, winner_participant_id = ?, loser_participant_id = ?,
                finish_reason = ?, admin_note = ?, revision = revision + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
          `,
          [
            startingParticipantId,
            canonical.status,
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
        await injectFault("playoff_result_after_match", {
          tournament_id: tournament.id,
          match_id: match.id,
        });
        await propagatePlayoffParticipant(
          match.next_match_for_winner_id,
          match.next_match_for_winner_slot,
          canonical.winner_participant_id
        );
        await propagatePlayoffParticipant(
          match.next_match_for_loser_id,
          match.next_match_for_loser_slot,
          canonical.loser_participant_id
        );
        const remaining = await dbGet(
          db,
          `SELECT COUNT(*) AS count FROM in_person_matches
           WHERE round_id = ? AND status <> 'cancelled' AND status <> 'completed'`,
          [match.round_id]
        );
        if (Number(remaining?.count || 0) === 0) {
          await dbRun(
            db,
            `UPDATE in_person_rounds
             SET status = 'completed', completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP),
                 revision = revision + 1, updated_at = CURRENT_TIMESTAMP
             WHERE id = ?`,
            [match.round_id]
          );
        }
        await touchTournament(tournament.id);
        return { changed: true, match_id: match.id };
      });
      const overview = await getPlayoffOverview(tournamentId);
      const match = overview.rounds
        .flatMap((round) => round.matches)
        .find((entry) => entry.id === outcome.match_id) || null;
      return { ...overview, ...outcome, match };
    });
  }

  async function completePlayoff(tournamentId) {
    return enqueueMutation(async () => {
      const outcome = await transaction(async () => {
        const tournament = await requireTournamentRow(tournamentId);
        if (tournament.status === "completed") return { completed: false };
        if (tournament.status !== "playoff") {
          throw conflictError("INVALID_TOURNAMENT_STATUS", "The tournament is not in the playoff stage");
        }
        const rounds = await loadPlayoffRounds(tournament.id);
        const finalMatch = rounds.find((round) => round.round_key === "final")?.matches?.[0];
        const bronzeMatch = rounds.find((round) => (
          round.round_key === "bronze_medal_match"
        ))?.matches?.[0];
        const missing = [];
        if (finalMatch?.status !== "completed") missing.push("final");
        if (bronzeMatch?.status !== "completed") missing.push("bronze_medal_match");
        if (missing.length) {
          throw conflictError(
            "PLAYOFF_MEDAL_MATCHES_INCOMPLETE",
            "Complete both Final and Bronze medal match before completing the tournament",
            { missing_round_keys: missing }
          );
        }
        await dbRun(
          db,
          `UPDATE in_person_tournaments
           SET status = 'completed', completed_at = CURRENT_TIMESTAMP,
               revision = revision + 1, updated_at = CURRENT_TIMESTAMP
           WHERE id = ?`,
          [tournament.id]
        );
        return { completed: true };
      });
      return { ...(await getPlayoffOverview(tournamentId)), ...outcome };
    });
  }

  async function listPublicTournaments() {
    const rows = await dbAll(
      db,
      `${TOURNAMENT_SELECT}
       WHERE t.published_at IS NOT NULL
         AND t.status NOT IN ('draft', 'cancelled')
       ORDER BY t.start_date DESC, lower(t.name_en), t.id`
    );
    return rows.map(serializePublicTournament);
  }

  async function getPublicTournamentRow(identifier) {
    const normalizedIdentifier = normalizeText(identifier);
    if (!normalizedIdentifier) {
      throw validationError("INVALID_TOURNAMENT_IDENTIFIER", "Tournament id or slug is required");
    }
    const row = await dbGet(
      db,
      `${TOURNAMENT_SELECT}
       WHERE (t.id = ? OR lower(trim(t.slug)) = lower(trim(?)))
         AND t.published_at IS NOT NULL
         AND t.status NOT IN ('draft', 'cancelled')
       LIMIT 1`,
      [normalizedIdentifier, normalizedIdentifier]
    );
    if (!row) throw notFoundError("PUBLIC_TOURNAMENT_NOT_FOUND", "Published tournament not found");
    return row;
  }

  async function loadPublicRounds(tournamentId, stage) {
    const rows = await dbAll(
      db,
      `
        SELECT *
        FROM in_person_rounds
        WHERE tournament_id = ?
          AND stage = ?
          AND status IN ('published', 'completed')
        ORDER BY
          CASE WHEN stage = 'swiss' THEN round_number ELSE round_order END,
          CASE WHEN round_key = 'bronze_medal_match' THEN 0 ELSE 1 END,
          round_key,
          id
      `,
      [tournamentId, stage]
    );
    const rounds = [];
    for (const row of rows) {
      const matchRows = await dbAll(
        db,
        `${PLAYOFF_MATCH_SELECT}
         WHERE m.round_id = ? AND m.status <> 'cancelled'
         ORDER BY
           CASE WHEN m.bracket_position IS NULL THEN 1 ELSE 0 END,
           m.bracket_position,
           CASE WHEN m.table_number IS NULL THEN 1 ELSE 0 END,
           m.table_number,
           m.id`,
        [row.id]
      );
      rounds.push(serializePublicRound(row, matchRows.map(serializePublicMatch)));
    }
    return rounds;
  }

  async function getPublicTournamentAggregate(identifier) {
    const tournamentRow = await getPublicTournamentRow(identifier);
    const [participantRows, swissRounds, playoffRounds, standings] = await Promise.all([
      loadParticipantRows(tournamentRow.id),
      loadPublicRounds(tournamentRow.id, "swiss"),
      loadPublicRounds(tournamentRow.id, "playoff"),
      loadLatestSwissStandings(tournamentRow.id),
    ]);
    return {
      revision: Number(tournamentRow.revision),
      updated_at: tournamentRow.updated_at,
      tournament: serializePublicTournament(tournamentRow),
      players: participantRows.map(serializePublicParticipant),
      swiss: {
        standings: {
          revision: Number(standings.revision || 0),
          calculated_at: standings.calculated_at || null,
          rows: standings.rows.map(serializePublicStanding),
        },
        rounds: swissRounds,
      },
      playoff: {
        rounds: playoffRounds,
        placements: publicPlayoffPlacements(playoffRounds),
      },
    };
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
    cancelSwissRound,
    cancelTournament,
    completePlayoff,
    completeSwissRound,
    confirmLateParticipant,
    confirmPlayoff,
    confirmSwissRound,
    createCity,
    createParticipantCity,
    createParticipant,
    createTournament,
    deleteParticipant,
    getCity,
    getParticipantsOverview,
    getPlayoffOverview,
    getPublicTournamentAggregate,
    getSwissOverview,
    getTournament,
    listAccessibleTournaments,
    listCities,
    listParticipantCities,
    listPublicTournaments,
    listTournamentAdmins,
    listTournaments,
    previewLateParticipant,
    previewPlayoff,
    previewSwissRound,
    previewSwissRoundCancellation,
    publishTournament,
    publishPlayoffRound,
    publishSwissRound,
    reopenSwissRound,
    resetPlayoff,
    removeTournamentAdmin,
    replaceTournamentAdmins,
    restoreCity: (cityId) => setCityArchived(cityId, false),
    saveSwissMatchResult,
    savePlayoffMatchResult,
    setPlayoffMatchTable,
    setParticipantCheckIn,
    setParticipantInactive,
    startCheckIn,
    updateCity,
    updateParticipant,
    updateTournament,
  };
}
