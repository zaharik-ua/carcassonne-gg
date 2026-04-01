import path from "node:path";
import { execFile } from "node:child_process";
import { fileURLToPath } from "node:url";
import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import session from "express-session";
import sqlite3 from "sqlite3";
import passport from "passport";
import { Strategy as GoogleStrategy } from "passport-google-oauth20";
import connectSqlite3 from "connect-sqlite3";

dotenv.config();

const requiredEnv = [
  "SESSION_SECRET",
  "GOOGLE_CLIENT_ID",
  "GOOGLE_CLIENT_SECRET",
  "GOOGLE_CALLBACK_URL",
];

for (const envName of requiredEnv) {
  if (!process.env[envName]) {
    throw new Error(`Missing required env var: ${envName}`);
  }
}

const PORT = Number(process.env.PORT || 3100);
const DEFAULT_FRONTEND_ORIGINS = [
  "http://localhost:8080",
  "http://127.0.0.1:8080",
  "http://localhost:5500",
  "http://127.0.0.1:5500",
  "http://192.168.0.113:5500",
  "https://carcassonne.gg",
  "https://www.carcassonne.gg",
  "https://carcassonne.com.ua",
  "https://www.carcassonne.com.ua",
  "https://carcassonnebelgium.weebly.com",
];
const FRONTEND_ORIGINS = Array.from(
  new Set(
    (process.env.FRONTEND_ORIGIN || "")
      .split(",")
      .map((origin) => origin.trim())
      .filter(Boolean)
      .concat(DEFAULT_FRONTEND_ORIGINS)
  )
);
const PRIMARY_FRONTEND_ORIGIN = FRONTEND_ORIGINS[0];
const DB_PATH = process.env.DB_PATH || "./data/auth.sqlite";
const isProd = process.env.NODE_ENV === "production";
const SITE_BASE_URL = process.env.SITE_BASE_URL || "https://carcassonne.gg";
const cookieSameSite = process.env.COOKIE_SAME_SITE || (isProd ? "none" : "lax");
const cookieSecure = process.env.COOKIE_SECURE
  ? process.env.COOKIE_SECURE === "true"
  : isProd;

if (cookieSameSite === "none" && !cookieSecure) {
  throw new Error("COOKIE_SAME_SITE=none requires COOKIE_SECURE=true");
}

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const dbFullPath = path.resolve(__dirname, "..", DB_PATH);

const db = new sqlite3.Database(dbFullPath);
const DEFAULT_TEAM_TIMEZONES = {
  ARG: "America/Argentina/Buenos_Aires",
  AUS: "Australia/Sydney",
  AUT: "Europe/Vienna",
  BEL: "Europe/Brussels",
  BRA: "America/Sao_Paulo",
  BGR: "Europe/Sofia",
  CAN: "America/Toronto",
  CAT: "Europe/Madrid",
  CHL: "America/Santiago",
  CHN: "Asia/Shanghai",
  COL: "America/Bogota",
  CRI: "America/Costa_Rica",
  CUB: "America/Havana",
  CZE: "Europe/Prague",
  DEU: "Europe/Berlin",
  DNK: "Europe/Copenhagen",
  EGY: "Africa/Cairo",
  ESP: "Europe/Madrid",
  EST: "Europe/Tallinn",
  FIN: "Europe/Helsinki",
  FRA: "Europe/Paris",
  GBR: "Europe/London",
  GRC: "Europe/Athens",
  GTM: "America/Guatemala",
  HKG: "Asia/Hong_Kong",
  HRV: "Europe/Zagreb",
  HUN: "Europe/Budapest",
  IDN: "Asia/Jakarta",
  IND: "Asia/Kolkata",
  ISL: "Atlantic/Reykjavik",
  ISR: "Asia/Jerusalem",
  ITA: "Europe/Rome",
  JPN: "Asia/Tokyo",
  KAZ: "Asia/Almaty",
  KOR: "Asia/Seoul",
  LTU: "Europe/Vilnius",
  LUX: "Europe/Luxembourg",
  LVA: "Europe/Riga",
  MDA: "Europe/Chisinau",
  MEX: "America/Mexico_City",
  MYS: "Asia/Kuala_Lumpur",
  NLD: "Europe/Amsterdam",
  NOR: "Europe/Oslo",
  PER: "America/Lima",
  POL: "Europe/Warsaw",
  PRT: "Europe/Lisbon",
  RCP: "Europe/Moscow",
  ROU: "Europe/Bucharest",
  SGP: "Asia/Singapore",
  SRB: "Europe/Belgrade",
  SVK: "Europe/Bratislava",
  SWE: "Europe/Stockholm",
  THA: "Asia/Bangkok",
  TUR: "Europe/Istanbul",
  TUT: "Europe/Minsk",
  TWN: "Asia/Taipei",
  UKR: "Europe/Kyiv",
  URY: "America/Montevideo",
  USA: "America/New_York",
  VNM: "Asia/Ho_Chi_Minh",
};
const PROFILE_AUDIT_FIELDS = [
  "id",
  "bga_nickname",
  "avatar",
  "name",
  "association",
  "status",
  "email",
  "master_title",
  "master_title_date",
  "team_captain",
  "telegram",
  "whatsapp",
  "discord",
  "instagram",
  "contact_email",
];
const MATCH_AUDIT_FIELDS = [
  "id",
  "tournament_id",
  "time_utc",
  "lineup_type",
  "lineup_deadline_h",
  "lineup_deadline_utc",
  "number_of_duels",
  "team_1",
  "team_2",
  "status",
  "dw1",
  "dw2",
  "gw1",
  "gw2",
];
const FRIENDLY_FIND_AUDIT_FIELDS = [
  "id",
  "team",
  "dates",
  "time_1",
  "time_2",
  "number_of_players",
];
const LINEUP_AUDIT_FIELDS = [
  "id",
  "tournament_id",
  "match_id",
  "duel_number",
  "duel_format",
  "time_utc",
  "custom_time",
  "player_1_id",
  "player_2_id",
  "dw1",
  "dw2",
  "status",
];
const TOURNAMENT_ACCESS_TYPES = {
  OPEN: 1,
  CLOSED: 2,
};
const TOURNAMENT_ACCESS_ROLES = {
  ADMIN: "admin",
  CAPTAIN: "captain",
};
const TOURNAMENT_LINEUP_SIZE_TYPES = {
  FIXED: 1,
  FLEXIBLE: 2,
};

function addColumnIfMissing(columns, tableName, columnName, sqlDefinition) {
  if (columns.some((col) => col.name === columnName)) return;

  db.run(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${sqlDefinition}`, (alterErr) => {
    if (alterErr) {
      console.error(`Failed to add ${columnName} column to ${tableName}`, alterErr);
    }
  });
}

function normalizeEntityId(value) {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function buildAssociationCode(name, usedCodes, fallbackIndex) {
  const base = normalizeEntityId(name) || `ASSOCIATION_${fallbackIndex}`;
  let candidate = base;
  let suffix = 2;
  while (usedCodes.has(candidate)) {
    candidate = `${base}_${suffix}`;
    suffix += 1;
  }
  usedCodes.add(candidate);
  return candidate;
}

function normalizeNullableText(value) {
  const normalized = String(value || "").trim();
  return normalized || null;
}

function normalizePositiveInteger(value) {
  const normalized = Number.parseInt(String(value ?? "").trim(), 10);
  return Number.isInteger(normalized) && normalized > 0 ? normalized : null;
}

function normalizeIntegerOrNull(value) {
  const raw = String(value ?? "").trim();
  if (!raw) return null;
  const normalized = Number.parseInt(raw, 10);
  return Number.isInteger(normalized) ? normalized : null;
}

function loadDuelsByMatchId(matchId, callback) {
  return db.all(
    `
      SELECT
        id,
        tournament_id,
        match_id,
        duel_number,
        duel_format,
        time_utc,
        custom_time,
        player_1_id,
        player_2_id,
        dw1,
        dw2,
        rating_full,
        rating,
        status
      FROM duels
      WHERE match_id = ?
        AND deleted_at IS NULL
      ORDER BY
        CASE WHEN duel_number IS NULL THEN 1 ELSE 0 END ASC,
        duel_number ASC,
        id ASC
    `,
    [matchId],
    (err, rows) => {
      if (err) {
        callback(err);
        return;
      }
      const duels = Array.isArray(rows) ? rows : [];
      const duelIds = duels
        .map((row) => String(row?.id || "").trim())
        .filter(Boolean);
      if (!duelIds.length) {
        callback(null, duels);
        return;
      }
      loadGamesByDuelIds(duelIds, (gamesErr, gameRows) => {
        if (gamesErr) {
          callback(gamesErr);
          return;
        }
        const gamesByDuelId = new Map();
        (Array.isArray(gameRows) ? gameRows : []).forEach((game) => {
          const duelId = String(game?.duel_id || "").trim();
          if (!duelId) return;
          if (!gamesByDuelId.has(duelId)) gamesByDuelId.set(duelId, []);
          gamesByDuelId.get(duelId).push(game);
        });
        callback(null, duels.map((duel) => ({
          ...duel,
          games: gamesByDuelId.get(String(duel?.id || "").trim()) || [],
        })));
      });
    }
  );
}

function loadDuelsByIds(duelIds, callback) {
  const normalizedIds = Array.from(new Set((Array.isArray(duelIds) ? duelIds : [])
    .map((value) => String(value || "").trim())
    .filter(Boolean)));
  if (!normalizedIds.length) {
    callback(null, []);
    return;
  }
  const placeholders = normalizedIds.map(() => "?").join(", ");
  return db.all(
    `
      SELECT
        id,
        tournament_id,
        match_id,
        duel_number,
        duel_format,
        time_utc,
        custom_time,
        player_1_id,
        player_2_id,
        dw1,
        dw2,
        rating_full,
        rating,
        status
      FROM duels
      WHERE id IN (${placeholders})
        AND deleted_at IS NULL
      ORDER BY
        CASE WHEN duel_number IS NULL THEN 1 ELSE 0 END ASC,
        duel_number ASC,
        id ASC
    `,
    normalizedIds,
    (err, rows) => {
      if (err) {
        callback(err);
        return;
      }
      const duels = Array.isArray(rows) ? rows : [];
      if (!duels.length) {
        callback(null, duels);
        return;
      }
      loadGamesByDuelIds(normalizedIds, (gamesErr, gameRows) => {
        if (gamesErr) {
          callback(gamesErr);
          return;
        }
        const gamesByDuelId = new Map();
        (Array.isArray(gameRows) ? gameRows : []).forEach((game) => {
          const duelId = String(game?.duel_id || "").trim();
          if (!duelId) return;
          if (!gamesByDuelId.has(duelId)) gamesByDuelId.set(duelId, []);
          gamesByDuelId.get(duelId).push(game);
        });
        callback(null, duels.map((duel) => ({
          ...duel,
          games: gamesByDuelId.get(String(duel?.id || "").trim()) || [],
        })));
      });
    }
  );
}

function loadGamesByDuelIds(duelIds, callback) {
  const normalizedIds = Array.from(new Set(
    (Array.isArray(duelIds) ? duelIds : [])
      .map((value) => String(value || "").trim())
      .filter(Boolean)
  ));
  if (!normalizedIds.length) {
    callback(null, []);
    return;
  }
  const placeholders = normalizedIds.map(() => "?").join(", ");
  return db.all(
    `
      SELECT
        id,
        duel_id,
        bga_table_id,
        game_number,
        player_1_score,
        player_2_score,
        player_1_rank,
        player_2_rank,
        player_1_clock,
        player_2_clock,
        status
      FROM games
      WHERE trim(COALESCE(duel_id, '')) IN (${placeholders})
      ORDER BY duel_id COLLATE NOCASE ASC, game_number ASC, id ASC
    `,
    normalizedIds,
    callback
  );
}

function dbGetAsync(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(row || null);
    });
  });
}

function dbAllAsync(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(rows || []);
    });
  });
}

function dbRunAsync(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function onRun(err) {
      if (err) {
        reject(err);
        return;
      }
      resolve(this);
    });
  });
}

async function recomputeMatchAggregates(matchId, actorPlayerId = null) {
  const normalizedMatchId = String(matchId || "").trim();
  if (!normalizedMatchId) return;

  const aggregateRow = await dbGetAsync(
    `
      SELECT
        COALESCE(SUM(COALESCE(l.dw1, 0)), 0) AS gw1,
        COALESCE(SUM(COALESCE(l.dw2, 0)), 0) AS gw2,
        COALESCE(SUM(CASE
          WHEN COALESCE(l.status, 'Planned') = 'Done' AND COALESCE(l.dw1, 0) > COALESCE(l.dw2, 0)
          THEN 1 ELSE 0 END), 0) AS dw1,
        COALESCE(SUM(CASE
          WHEN COALESCE(l.status, 'Planned') = 'Done' AND COALESCE(l.dw2, 0) > COALESCE(l.dw1, 0)
          THEN 1 ELSE 0 END), 0) AS dw2,
        COUNT(*) AS total_duels,
        COALESCE(SUM(CASE WHEN COALESCE(l.status, 'Planned') = 'Done' THEN 1 ELSE 0 END), 0) AS done_duels,
        COALESCE(SUM(CASE WHEN COALESCE(l.status, 'Planned') = 'Error' THEN 1 ELSE 0 END), 0) AS error_duels,
        MIN(CASE
          WHEN datetime(l.time_utc) IS NOT NULL THEN unixepoch(l.time_utc)
          ELSE NULL
        END) AS start_ts,
        MAX(CASE
          WHEN datetime(l.time_utc) IS NOT NULL
          THEN unixepoch(l.time_utc) + (COALESCE(df.minutes_to_play, 60) * 60)
          ELSE NULL
        END) AS end_ts
      FROM duels l
      LEFT JOIN duel_formats df
        ON lower(trim(df.format)) = lower(trim(l.duel_format))
      WHERE trim(COALESCE(l.match_id, '')) = trim(?)
        AND l.deleted_at IS NULL
    `,
    [normalizedMatchId]
  );

  const totalDuels = Number(aggregateRow?.total_duels || 0);
  const doneDuels = Number(aggregateRow?.done_duels || 0);
  const errorDuels = Number(aggregateRow?.error_duels || 0);
  const startTs = aggregateRow?.start_ts === null || aggregateRow?.start_ts === undefined
    ? null
    : Number(aggregateRow.start_ts);
  const endTs = aggregateRow?.end_ts === null || aggregateRow?.end_ts === undefined
    ? null
    : Number(aggregateRow.end_ts);
  const nowTs = Math.floor(Date.now() / 1000);

  let nextStatus = "Planned";
  if (errorDuels > 0) {
    nextStatus = "Error";
  } else if (totalDuels > 0 && doneDuels === totalDuels) {
    nextStatus = "Done";
  } else if (startTs !== null && endTs !== null && startTs <= nowTs && nowTs < endTs) {
    nextStatus = "In progress";
  }

  await dbRunAsync(
    `
      UPDATE matches
      SET
        dw1 = ?,
        dw2 = ?,
        gw1 = ?,
        gw2 = ?,
        status = ?,
        updated_by = COALESCE(?, updated_by),
        updated_at = CURRENT_TIMESTAMP
      WHERE id = ?
        AND deleted_at IS NULL
    `,
    [
      Number(aggregateRow?.dw1 || 0),
      Number(aggregateRow?.dw2 || 0),
      Number(aggregateRow?.gw1 || 0),
      Number(aggregateRow?.gw2 || 0),
      nextStatus,
      actorPlayerId,
      normalizedMatchId,
    ]
  );
}

function execFileAsync(file, args = [], options = {}) {
  return new Promise((resolve, reject) => {
    execFile(file, args, options, (error, stdout, stderr) => {
      if (error) {
        error.stdout = stdout;
        error.stderr = stderr;
        reject(error);
        return;
      }
      resolve({ stdout, stderr });
    });
  });
}

function hasNonEmptyValue(value) {
  return String(value ?? "").trim() !== "";
}

function normalizeTournamentAccessType(value) {
  const normalized = Number.parseInt(String(value ?? "").trim(), 10);
  return normalized === TOURNAMENT_ACCESS_TYPES.CLOSED
    ? TOURNAMENT_ACCESS_TYPES.CLOSED
    : TOURNAMENT_ACCESS_TYPES.OPEN;
}

function normalizeTournamentAccessUserIds(value) {
  if (!Array.isArray(value)) return [];
  const unique = new Set();
  value.forEach((entry) => {
    const normalized = Number.parseInt(String(entry ?? "").trim(), 10);
    if (Number.isInteger(normalized) && normalized > 0) unique.add(normalized);
  });
  return Array.from(unique);
}

function normalizeTournamentAccessRole(value) {
  const normalized = String(value || "").trim().toLowerCase();
  return normalized === TOURNAMENT_ACCESS_ROLES.ADMIN
    ? TOURNAMENT_ACCESS_ROLES.ADMIN
    : TOURNAMENT_ACCESS_ROLES.CAPTAIN;
}

function normalizeTournamentAccessUsers(value, fallbackUserIds) {
  const byUserId = new Map();

  const pushUser = (userIdValue, roleValue) => {
    const userId = Number.parseInt(String(userIdValue ?? "").trim(), 10);
    if (!Number.isInteger(userId) || userId <= 0) return;
    byUserId.set(userId, {
      user_id: userId,
      role: normalizeTournamentAccessRole(roleValue),
    });
  };

  if (Array.isArray(value)) {
    value.forEach((entry) => {
      if (entry && typeof entry === "object" && !Array.isArray(entry)) {
        pushUser(entry.user_id ?? entry.userId ?? entry.id, entry.role);
      } else {
        pushUser(entry, TOURNAMENT_ACCESS_ROLES.CAPTAIN);
      }
    });
  } else if (Array.isArray(fallbackUserIds)) {
    fallbackUserIds.forEach((entry) => {
      pushUser(entry, TOURNAMENT_ACCESS_ROLES.CAPTAIN);
    });
  }

  return Array.from(byUserId.values()).sort((a, b) => a.user_id - b.user_id);
}

function normalizeTournamentLineupSizeType(value) {
  const normalized = Number.parseInt(String(value ?? "").trim(), 10);
  return normalized === TOURNAMENT_LINEUP_SIZE_TYPES.FIXED
    ? TOURNAMENT_LINEUP_SIZE_TYPES.FIXED
    : TOURNAMENT_LINEUP_SIZE_TYPES.FLEXIBLE;
}

function normalizeTournamentLineupSize(value) {
  const normalized = Number.parseInt(String(value ?? "").trim(), 10);
  return Number.isInteger(normalized) && normalized > 0 ? normalized : null;
}

function getTournamentLookupVariants(value) {
  const raw = normalizeNullableText(value);
  if (!raw) return [];
  const normalized = normalizeEntityId(raw);
  return Array.from(new Set([raw, normalized].filter(Boolean)));
}

function buildTournamentLookupWhereClause(columnName) {
  return `
    (
      upper(trim(${columnName})) = upper(trim(?))
      OR upper(trim(${columnName})) = upper(trim(?))
    )
  `;
}

function getTournamentAccessUserId(user) {
  const userId = Number(user?.id);
  return Number.isInteger(userId) && userId > 0 ? userId : 0;
}

function loadTournamentAccessForUser(tournamentId, user, done) {
  const [rawTournamentId, normalizedTournamentId] = getTournamentLookupVariants(tournamentId);
  if (!rawTournamentId) {
    done(null, null);
    return;
  }

  const isAdmin = Number(user?.admin) === 1;
  const userId = getTournamentAccessUserId(user);

  db.get(
    `
      SELECT
        t.id,
        t.name,
        t.short_title,
        t.logo,
        t.link,
        COALESCE(t.access_type, ?) AS access_type,
        COALESCE(t.lineup_size_type, ?) AS lineup_size_type,
        t.lineup_size,
        CASE
          WHEN ? = 1 THEN ?
          WHEN ? > 0 THEN (
            SELECT COALESCE(NULLIF(lower(trim(tau.role)), ''), ?)
            FROM tournament_access_users tau
            WHERE upper(trim(tau.tournament_id)) = upper(trim(t.id))
              AND tau.user_id = ?
            LIMIT 1
          )
          ELSE NULL
        END AS access_role,
        CASE
          WHEN ? = 1 THEN 1
          WHEN COALESCE(t.access_type, ?) = ? THEN 1
          WHEN ? > 0 AND EXISTS (
            SELECT 1
            FROM tournament_access_users tau
            WHERE upper(trim(tau.tournament_id)) = upper(trim(t.id))
              AND tau.user_id = ?
          ) THEN 1
          ELSE 0
        END AS has_access
      FROM tournaments t
      WHERE ${buildTournamentLookupWhereClause("t.id")}
      LIMIT 1
    `,
    [
      TOURNAMENT_ACCESS_TYPES.OPEN,
      TOURNAMENT_LINEUP_SIZE_TYPES.FLEXIBLE,
      isAdmin ? 1 : 0,
      TOURNAMENT_ACCESS_ROLES.ADMIN,
      userId,
      TOURNAMENT_ACCESS_ROLES.CAPTAIN,
      userId,
      isAdmin ? 1 : 0,
      TOURNAMENT_ACCESS_TYPES.OPEN,
      TOURNAMENT_ACCESS_TYPES.OPEN,
      userId,
      userId,
      rawTournamentId,
      normalizedTournamentId || rawTournamentId,
    ],
    (err, row) => {
      if (err) {
        done(err);
        return;
      }
      if (!row) {
        done(null, null);
        return;
      }

      done(null, {
        id: row.id,
        name: row.name,
        short_title: row.short_title,
        logo: row.logo,
        link: row.link,
        access_type: normalizeTournamentAccessType(row.access_type),
        lineup_size_type: normalizeTournamentLineupSizeType(row.lineup_size_type),
        lineup_size: normalizeTournamentLineupSize(row.lineup_size),
        access_role: row.access_role ? normalizeTournamentAccessRole(row.access_role) : null,
        has_access: Number(row.has_access) === 1,
      });
    }
  );
}

function canClosedTournamentCaptainAccessMatch(tournament, userAssociation, team1, team2) {
  if (tournament?.access_type !== TOURNAMENT_ACCESS_TYPES.CLOSED) return false;
  if (tournament?.access_role !== TOURNAMENT_ACCESS_ROLES.CAPTAIN) return false;
  const association = String(userAssociation || "").trim().toUpperCase();
  if (!association) return false;
  const normalizedTeam1 = String(team1 || "").trim().toUpperCase();
  const normalizedTeam2 = String(team2 || "").trim().toUpperCase();
  return association === normalizedTeam1 || association === normalizedTeam2;
}

function validateTournamentAccessUserIds(userIds, done) {
  const normalizedUsers = normalizeTournamentAccessUsers(userIds);
  if (!normalizedUsers.length) {
    done(null, []);
    return;
  }

  const normalizedIds = normalizedUsers.map((entry) => entry.user_id);
  const placeholders = normalizedIds.map(() => "?").join(", ");
  db.all(
    `
      SELECT id
      FROM users
      WHERE id IN (${placeholders})
    `,
    normalizedIds,
    (err, rows) => {
      if (err) {
        done(err);
        return;
      }
      const existingIds = new Set((rows || []).map((row) => Number(row?.id)).filter((id) => Number.isInteger(id) && id > 0));
      const invalidIds = normalizedIds.filter((id) => !existingIds.has(id));
      if (invalidIds.length) {
        done(new Error(`Unknown user ids: ${invalidIds.join(", ")}`));
        return;
      }
      done(null, normalizedUsers);
    }
  );
}

function replaceTournamentAccessUsers(tournamentId, userIds, done) {
  const normalizedTournamentId = normalizeNullableText(tournamentId);
  const normalizedUsers = normalizeTournamentAccessUsers(userIds);
  if (!normalizedTournamentId) {
    done(new Error("Tournament id is required"));
    return;
  }

  db.run(
    `
      DELETE FROM tournament_access_users
      WHERE upper(trim(tournament_id)) = upper(trim(?))
    `,
    [normalizedTournamentId],
    (deleteErr) => {
      if (deleteErr) {
        done(deleteErr);
        return;
      }

      if (!normalizedUsers.length) {
        done(null);
        return;
      }

      const stmt = db.prepare(`
        INSERT INTO tournament_access_users (
          tournament_id,
          user_id,
          role,
          created_at,
          updated_at
        )
        VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
      `);

      let pending = normalizedUsers.length;
      let finished = false;
      const finish = (err) => {
        if (finished) return;
        finished = true;
        stmt.finalize(() => done(err || null));
      };

      normalizedUsers.forEach((entry) => {
        stmt.run([normalizedTournamentId, entry.user_id, entry.role], (insertErr) => {
          if (insertErr) {
            finish(insertErr);
            return;
          }
          pending -= 1;
          if (pending === 0) finish(null);
        });
      });
    }
  );
}

function loadTournamentRowById(tournamentId, includeAccessUsers, done) {
  const [rawTournamentId, normalizedTournamentId] = getTournamentLookupVariants(tournamentId);
  if (!rawTournamentId) {
    done(null, null);
    return;
  }

  db.get(
    `
      SELECT
        id,
        name,
        short_title,
        logo,
        link,
        COALESCE(access_type, ?) AS access_type,
        COALESCE(lineup_size_type, ?) AS lineup_size_type,
        lineup_size
      FROM tournaments
      WHERE ${buildTournamentLookupWhereClause("id")}
      LIMIT 1
    `,
    [
      TOURNAMENT_ACCESS_TYPES.OPEN,
      TOURNAMENT_LINEUP_SIZE_TYPES.FLEXIBLE,
      rawTournamentId,
      normalizedTournamentId || rawTournamentId,
    ],
    (err, row) => {
      if (err) {
        done(err);
        return;
      }
      if (!row) {
        done(null, null);
        return;
      }

      const tournament = {
        ...row,
        access_type: normalizeTournamentAccessType(row.access_type),
        lineup_size_type: normalizeTournamentLineupSizeType(row.lineup_size_type),
        lineup_size: normalizeTournamentLineupSize(row.lineup_size),
      };
      if (!includeAccessUsers) {
        done(null, tournament);
        return;
      }

      db.all(
        `
          SELECT
            user_id,
            COALESCE(NULLIF(lower(trim(role)), ''), ?) AS role
          FROM tournament_access_users
          WHERE upper(trim(tournament_id)) = upper(trim(?))
          ORDER BY user_id ASC
        `,
        [TOURNAMENT_ACCESS_ROLES.CAPTAIN, normalizedTournamentId],
        (accessErr, rows) => {
          if (accessErr) {
            done(accessErr);
            return;
          }
          tournament.access_users = (rows || [])
            .map((entry) => ({
              user_id: Number(entry?.user_id),
              role: normalizeTournamentAccessRole(entry?.role),
            }))
            .filter((entry) => Number.isInteger(entry.user_id) && entry.user_id > 0);
          tournament.access_user_ids = tournament.access_users.map((entry) => entry.user_id);
          done(null, tournament);
        }
      );
    }
  );
}

function syncUserBgaIdFromEmail(userId, email, options, done) {
  const callback = typeof done === "function"
    ? done
    : typeof options === "function"
      ? options
      : () => {};
  const config = options && typeof options === "object" ? options : {};
  const normalizedEmail = normalizeNullableText(email);
  const normalizedUserId = Number(userId);

  if (!Number.isInteger(normalizedUserId) || normalizedUserId <= 0 || !normalizedEmail) {
    callback(null, null);
    return;
  }

  db.get(
    `
      SELECT p.id
      FROM profiles p
      WHERE lower(COALESCE(p.email, '')) = lower(?)
        AND trim(COALESCE(p.id, '')) <> ''
        AND p.deleted_at IS NULL
      ORDER BY p.updated_at DESC, p.id ASC
      LIMIT 1
    `,
    [normalizedEmail],
    (selectErr, profileRow) => {
      if (selectErr) {
        callback(selectErr);
        return;
      }

      const profileId = normalizeNullableText(profileRow?.id);
      if (!profileId) {
        callback(null, null);
        return;
      }

      db.run(
        `
          UPDATE users
          SET
            bga_id = NULL,
            updated_at = CURRENT_TIMESTAMP
          WHERE bga_id = ?
            AND id <> ?
        `,
        [profileId, normalizedUserId],
        (clearErr) => {
          if (clearErr) {
            callback(clearErr);
            return;
          }

          loadAuditUserProfileInfo(normalizedUserId, (loadErr, currentUserRow) => {
            if (loadErr) {
              callback(loadErr);
              return;
            }

            const oldBgaId = normalizeNullableText(currentUserRow?.bga_id);
            if (oldBgaId) {
              callback(null, profileId);
              return;
            }

            db.run(
              `
                UPDATE users
                SET
                  bga_id = ?,
                  updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                  AND trim(COALESCE(bga_id, '')) = ''
              `,
              [profileId, normalizedUserId],
              (updateErr) => {
                if (updateErr) {
                  callback(updateErr);
                  return;
                }

                return logUserBgaLinkAudit(
                  {
                    actor: config.actor || getAuditActor(currentUserRow),
                    userId: normalizedUserId,
                    oldBgaId,
                    source: config.source || "login",
                  },
                  () => callback(null, profileId)
                );
              }
            );
          });
        }
      );
    }
  );
}

function syncUsersBgaIdForProfile(profileId, email, options, done) {
  const callback = typeof done === "function"
    ? done
    : typeof options === "function"
      ? options
      : () => {};
  const config = options && typeof options === "object" ? options : {};
  const normalizedProfileId = normalizeNullableText(profileId);
  const normalizedEmail = normalizeNullableText(email);

  if (!normalizedProfileId) {
    callback(null, null);
    return;
  }

  const clearExistingBindings = (targetUserId, callback) => {
    const params = targetUserId
      ? [normalizedProfileId, Number(targetUserId)]
      : [normalizedProfileId];
    const sql = targetUserId
      ? `
          UPDATE users
          SET
            bga_id = NULL,
            updated_at = CURRENT_TIMESTAMP
          WHERE bga_id = ?
            AND id <> ?
        `
      : `
          UPDATE users
          SET
            bga_id = NULL,
            updated_at = CURRENT_TIMESTAMP
          WHERE bga_id = ?
        `;

    db.run(sql, params, callback);
  };

  if (!normalizedEmail) {
    clearExistingBindings(null, (clearErr) => callback(clearErr || null, null));
    return;
  }

  db.get(
    `
      SELECT u.id
        ,u.bga_id
      FROM users u
      WHERE lower(COALESCE(u.email, '')) = lower(?)
      ORDER BY datetime(COALESCE(u.last_login, u.updated_at, u.created_at)) DESC, u.id ASC
      LIMIT 1
    `,
    [normalizedEmail],
    (selectErr, userRow) => {
      if (selectErr) {
        callback(selectErr);
        return;
      }

      const targetUserId = Number(userRow?.id);
      const oldBgaId = normalizeNullableText(userRow?.bga_id);
      if (!Number.isInteger(targetUserId) || targetUserId <= 0) {
        clearExistingBindings(null, (clearErr) => callback(clearErr || null, null));
        return;
      }

      clearExistingBindings(targetUserId, (clearErr) => {
        if (clearErr) {
          callback(clearErr);
          return;
        }

        db.run(
          `
            UPDATE users
            SET
              bga_id = ?,
              updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
          `,
          [normalizedProfileId, targetUserId],
          (updateErr) => {
            if (updateErr) {
              callback(updateErr);
              return;
            }

            return logUserBgaLinkAudit(
              {
                actor: config.actor || null,
                userId: targetUserId,
                oldBgaId,
                source: config.source || "profile_email_change",
              },
              () => callback(null, targetUserId)
            );
          }
        );
      });
    }
  );
}

function ensureUsersSchema() {
  db.all("PRAGMA table_info(users)", (pragmaErr, columns) => {
    if (pragmaErr) {
      console.error("Failed to inspect users schema", pragmaErr);
      return;
    }
    if (!Array.isArray(columns) || columns.length === 0) return;
    addColumnIfMissing(columns, "users", "email", "TEXT");
    addColumnIfMissing(columns, "users", "name", "TEXT");
    addColumnIfMissing(columns, "users", "picture", "TEXT");
    addColumnIfMissing(columns, "users", "bga_id", "TEXT");
    addColumnIfMissing(columns, "users", "admin", "INTEGER NOT NULL DEFAULT 0");
    addColumnIfMissing(columns, "users", "created_at", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP");
    addColumnIfMissing(columns, "users", "updated_at", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP");
    addColumnIfMissing(columns, "users", "last_login", "TEXT");
    db.run(
      "UPDATE users SET bga_id = NULL WHERE trim(COALESCE(bga_id, '')) = ''",
      (normalizeErr) => {
        if (normalizeErr) {
          console.error("Failed to normalize users.bga_id", normalizeErr);
        }
      }
    );
    db.run(
      `
        CREATE UNIQUE INDEX IF NOT EXISTS idx_users_bga_id
        ON users(bga_id)
        WHERE bga_id IS NOT NULL AND trim(bga_id) <> ''
      `,
      (indexErr) => {
        if (indexErr) {
          console.error("Failed to ensure unique index for users.bga_id", indexErr);
        }
      }
    );
  });
}

function ensureAssociationsSchema() {
  db.run(`
    CREATE TABLE IF NOT EXISTS associations (
      association_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
      code TEXT UNIQUE COLLATE NOCASE,
      name TEXT NOT NULL UNIQUE COLLATE NOCASE,
      flag TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
  `, (createErr) => {
    if (createErr) {
      console.error("Failed to ensure associations schema", createErr);
      return;
    }
    db.all("PRAGMA table_info(associations)", (pragmaErr, columns) => {
      if (pragmaErr) {
        console.error("Failed to inspect associations schema", pragmaErr);
        return;
      }
      if (!Array.isArray(columns) || columns.length === 0) return;
      addColumnIfMissing(columns, "associations", "code", "TEXT");
      addColumnIfMissing(columns, "associations", "flag", "TEXT");
      addColumnIfMissing(columns, "associations", "created_at", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP");
      addColumnIfMissing(columns, "associations", "updated_at", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP");
      db.run(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_associations_code ON associations(code COLLATE NOCASE)",
        (indexErr) => {
          if (indexErr) {
            console.error("Failed to ensure unique index for associations.code", indexErr);
          }
        }
      );
      db.run(
        `
          UPDATE associations
          SET flag = NULLIF(trim(flag), '')
          WHERE flag IS NOT NULL
        `,
        (normalizeErr) => {
          if (normalizeErr) {
            console.error("Failed to normalize associations.flag", normalizeErr);
          }
        }
      );
      db.all(
        `
          SELECT rowid AS row_id, code, name
          FROM associations
          ORDER BY rowid ASC
        `,
        (rowsErr, rows) => {
          if (rowsErr) {
            console.error("Failed to backfill association codes", rowsErr);
            return;
          }
          const usedCodes = new Set();
          (rows || []).forEach((row) => {
            const existingCode = normalizeEntityId(row?.code);
            if (existingCode) usedCodes.add(existingCode);
          });
          const rowsToBackfill = (rows || []).filter((row) => !normalizeEntityId(row?.code));
          if (!rowsToBackfill.length) return;
          const stmt = db.prepare("UPDATE associations SET code = ?, updated_at = CURRENT_TIMESTAMP WHERE rowid = ?");
          rowsToBackfill.forEach((row, index) => {
            const code = buildAssociationCode(row?.name, usedCodes, index + 1);
            stmt.run([code, row.row_id], (updateErr) => {
              if (updateErr) {
                console.error(`Failed to backfill association code for row ${row.row_id}`, updateErr);
              }
            });
          });
          stmt.finalize();
        }
      );
    });
  });
}

function rebuildProfilesTableWithoutAdminColumn(done = () => {}) {
  db.exec(
    `
      BEGIN TRANSACTION;
      DROP INDEX IF EXISTS idx_profiles_id;
      ALTER TABLE profiles RENAME TO profiles_admin_legacy;
      CREATE TABLE profiles (
        email TEXT NOT NULL UNIQUE COLLATE NOCASE,
        bga_nickname TEXT,
        name TEXT,
        association TEXT,
        status TEXT NOT NULL DEFAULT 'Active',
        master_title INTEGER NOT NULL DEFAULT 0,
        master_title_date DATE,
        team_captain INTEGER NOT NULL DEFAULT 0,
        telegram TEXT,
        whatsapp TEXT,
        discord TEXT,
        instagram TEXT,
        contact_email TEXT,
        avatar TEXT,
        id TEXT,
        created_by TEXT,
        updated_by TEXT,
        deleted_by TEXT,
        deleted_at TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
      INSERT INTO profiles (
        email,
        bga_nickname,
        name,
        association,
        status,
        master_title,
        master_title_date,
        team_captain,
        telegram,
        whatsapp,
        discord,
        instagram,
        contact_email,
        avatar,
        id,
        created_by,
        updated_by,
        deleted_by,
        deleted_at,
        created_at,
        updated_at
      )
      SELECT
        email,
        bga_nickname,
        name,
        association,
        status,
        master_title,
        master_title_date,
        team_captain,
        telegram,
        whatsapp,
        discord,
        instagram,
        contact_email,
        avatar,
        id,
        created_by,
        updated_by,
        deleted_by,
        deleted_at,
        created_at,
        updated_at
      FROM profiles_admin_legacy;
      CREATE UNIQUE INDEX IF NOT EXISTS idx_profiles_id ON profiles(id);
      DROP TABLE profiles_admin_legacy;
      COMMIT;
    `,
    (err) => {
      if (err) {
        db.exec("ROLLBACK", () => {
          console.error("Failed to rebuild profiles table without admin column", err);
          done(err);
        });
        return;
      }
      done(null);
    }
  );
}

function migrateAdminFlagToUsers() {
  const adminEmail = "z0675006213@gmail.com";

  db.all("PRAGMA table_info(users)", (usersErr, userColumns) => {
    if (usersErr) {
      console.error("Failed to inspect users schema for admin migration", usersErr);
      return;
    }

    const continueWithUsersAdmin = () => {
      db.all("PRAGMA table_info(profiles)", (profilesErr, profileColumns) => {
        if (profilesErr) {
          console.error("Failed to inspect profiles schema for admin migration", profilesErr);
          return;
        }

        const hasProfilesAdmin = Array.isArray(profileColumns) && profileColumns.some((col) => col.name === "admin");
        const setExplicitAdminUser = () => db.run(
          `
            UPDATE users
            SET admin = 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE lower(trim(COALESCE(email, ''))) = lower(?)
          `,
          [adminEmail],
          (setAdminErr) => {
            if (setAdminErr) {
              console.error(`Failed to set admin for ${adminEmail}`, setAdminErr);
            }
          }
        );

        if (!hasProfilesAdmin) {
          setExplicitAdminUser();
          return;
        }

        return db.run(
          `
            UPDATE users
            SET admin = 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE EXISTS (
              SELECT 1
              FROM profiles p
              WHERE trim(COALESCE(p.id, '')) = trim(COALESCE(users.bga_id, ''))
                AND p.deleted_at IS NULL
                AND COALESCE(p.admin, 0) = 1
            )
          `,
          (copyErr) => {
            if (copyErr) {
              console.error("Failed to migrate profiles.admin to users.admin", copyErr);
              setExplicitAdminUser();
              return;
            }

            rebuildProfilesTableWithoutAdminColumn((rebuildErr) => {
              if (rebuildErr) {
                setExplicitAdminUser();
                return;
              }
              setExplicitAdminUser();
            });
          }
        );
      });
    };

    if (Array.isArray(userColumns) && userColumns.some((col) => col.name === "admin")) {
      continueWithUsersAdmin();
      return;
    }

    db.run(
      "ALTER TABLE users ADD COLUMN admin INTEGER NOT NULL DEFAULT 0",
      (alterErr) => {
        if (alterErr && !String(alterErr.message || "").includes("duplicate column name")) {
          console.error("Failed to add admin column to users", alterErr);
          return;
        }
        continueWithUsersAdmin();
      }
    );
  });
}

function ensureProfilesSchema() {
  db.all("PRAGMA table_info(profiles)", (pragmaErr, columns) => {
    if (pragmaErr) {
      console.error("Failed to inspect profiles schema", pragmaErr);
      return;
    }

    const addOrBackfillProfilesColumns = (currentColumns) => {
      addColumnIfMissing(currentColumns, "profiles", "id", "TEXT");
      addColumnIfMissing(currentColumns, "profiles", "bga_nickname", "TEXT");
      addColumnIfMissing(currentColumns, "profiles", "name", "TEXT");
      addColumnIfMissing(currentColumns, "profiles", "association", "TEXT");
      addColumnIfMissing(currentColumns, "profiles", "status", "TEXT NOT NULL DEFAULT 'Active'");
      addColumnIfMissing(currentColumns, "profiles", "master_title", "INTEGER NOT NULL DEFAULT 0");
      addColumnIfMissing(currentColumns, "profiles", "master_title_date", "DATE");
      addColumnIfMissing(currentColumns, "profiles", "team_captain", "INTEGER NOT NULL DEFAULT 0");
      addColumnIfMissing(currentColumns, "profiles", "telegram", "TEXT");
      addColumnIfMissing(currentColumns, "profiles", "whatsapp", "TEXT");
      addColumnIfMissing(currentColumns, "profiles", "discord", "TEXT");
      addColumnIfMissing(currentColumns, "profiles", "instagram", "TEXT");
      addColumnIfMissing(currentColumns, "profiles", "contact_email", "TEXT");
      addColumnIfMissing(currentColumns, "profiles", "avatar", "TEXT");
      addColumnIfMissing(currentColumns, "profiles", "bga_elo", "INTEGER");
      addColumnIfMissing(currentColumns, "profiles", "bga_elo_updated_at", "TEXT");
      addColumnIfMissing(currentColumns, "profiles", "created_by", "TEXT");
      addColumnIfMissing(currentColumns, "profiles", "updated_by", "TEXT");
      addColumnIfMissing(currentColumns, "profiles", "deleted_by", "TEXT");
      addColumnIfMissing(currentColumns, "profiles", "deleted_at", "TEXT");
      addColumnIfMissing(currentColumns, "profiles", "created_at", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP");
      addColumnIfMissing(currentColumns, "profiles", "updated_at", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP");
      db.run(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_profiles_id ON profiles(id)",
        (indexErr) => {
          if (indexErr) {
            console.error("Failed to ensure unique index for profiles.id", indexErr);
          }
        }
      );
      db.run(
        "UPDATE profiles SET status = 'Active' WHERE status IS NULL OR trim(status) = ''",
        (backfillErr) => {
          if (backfillErr) {
            console.error("Failed to backfill profiles.status", backfillErr);
          }
        }
      );
      migrateAdminFlagToUsers();
    };

    const hasLegacyPlayerId = columns.some((col) => col.name === "player_id");
    const hasIdColumn = columns.some((col) => col.name === "id");
    const idColumn = columns.find((col) => col.name === "id");
    const idIsPrimaryKey = Number(idColumn?.pk || 0) === 1;

    if (!hasLegacyPlayerId) {
      addOrBackfillProfilesColumns(columns);
      return;
    }

    const renameLegacyPlayerId = () => {
      db.run("ALTER TABLE profiles RENAME COLUMN player_id TO id", (renamePlayerIdErr) => {
        if (renamePlayerIdErr) {
          console.error("Failed to rename profiles.player_id to profiles.id", renamePlayerIdErr);
          addOrBackfillProfilesColumns(columns);
          return;
        }
        db.all("PRAGMA table_info(profiles)", (refreshErr, refreshedColumns) => {
          if (refreshErr) {
            console.error("Failed to refresh profiles schema after rename", refreshErr);
            return;
          }
          addOrBackfillProfilesColumns(refreshedColumns || []);
        });
      });
    };

    if (hasIdColumn && idIsPrimaryKey) {
      console.error("Legacy profiles schema uses id as primary key while player_id still exists; skipping auto-migration to avoid corrupting profiles.id");
      addOrBackfillProfilesColumns(columns);
      return;
    }

    if (!hasIdColumn) {
      renameLegacyPlayerId();
      return;
    }

    console.error("profiles table already has id column, cannot auto-rename player_id -> id safely");
    addOrBackfillProfilesColumns(columns);
  });
}

function ensureMatchesSchema() {
  db.all("PRAGMA table_info(matches)", (pragmaErr, columns) => {
    if (pragmaErr) {
      console.error("Failed to inspect matches schema", pragmaErr);
      return;
    }
    if (!Array.isArray(columns) || columns.length === 0) return;
    addColumnIfMissing(columns, "matches", "lineup_deadline_h", "INTEGER");
    addColumnIfMissing(columns, "matches", "deleted_at", "TEXT");
    addColumnIfMissing(columns, "matches", "created_by", "TEXT");
    addColumnIfMissing(columns, "matches", "updated_by", "TEXT");
    addColumnIfMissing(columns, "matches", "deleted_by", "TEXT");
    addColumnIfMissing(columns, "matches", "rating", "INTEGER");
  });
}

function ensureDuelsSchema() {
  db.all(
    "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('duels', 'lineups')",
    (tablesErr, tables) => {
      if (tablesErr) {
        console.error("Failed to inspect duels/lineups tables", tablesErr);
        return;
      }

      const tableNames = new Set((tables || []).map((row) => String(row?.name || "").trim().toLowerCase()));
      const finalizeDuelsSchema = () => db.run(`
    CREATE TABLE IF NOT EXISTS duels (
      id TEXT PRIMARY KEY,
      tournament_id TEXT,
      match_id TEXT,
      duel_number INTEGER,
      duel_format TEXT,
      time_utc TEXT,
      custom_time TEXT,
      player_1_id TEXT,
      player_2_id TEXT,
      dw1 INTEGER,
      dw2 INTEGER,
      rating_full REAL,
      rating INTEGER,
      status TEXT,
      results_last_error TEXT,
      results_checked_at TEXT,
      created_by TEXT,
      updated_by TEXT,
      deleted_by TEXT,
      deleted_at TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
  `, (createErr) => {
    if (createErr) {
      console.error("Failed to ensure duels schema", createErr);
      return;
    }
    db.all("PRAGMA table_info(duels)", (pragmaErr, columns) => {
      if (pragmaErr) {
        console.error("Failed to inspect duels schema", pragmaErr);
        return;
      }
      if (!Array.isArray(columns) || columns.length === 0) return;
      addColumnIfMissing(columns, "duels", "duel_number", "INTEGER");
      addColumnIfMissing(columns, "duels", "results_last_error", "TEXT");
      addColumnIfMissing(columns, "duels", "results_checked_at", "TEXT");
      addColumnIfMissing(columns, "duels", "rating_full", "REAL");
      addColumnIfMissing(columns, "duels", "rating", "INTEGER");
      addColumnIfMissing(columns, "duels", "created_by", "TEXT");
      addColumnIfMissing(columns, "duels", "updated_by", "TEXT");
      addColumnIfMissing(columns, "duels", "deleted_by", "TEXT");
      addColumnIfMissing(columns, "duels", "deleted_at", "TEXT");
    });
  });
      if (tableNames.has("lineups") && !tableNames.has("duels")) {
        db.run("ALTER TABLE lineups RENAME TO duels", (renameErr) => {
          if (renameErr) {
            console.error("Failed to rename lineups table to duels", renameErr);
            return;
          }
          finalizeDuelsSchema();
        });
        return;
      }

      finalizeDuelsSchema();
    }
  );
}

function ensureDuelFormatsSchema() {
  db.run(`
    CREATE TABLE IF NOT EXISTS duel_formats (
      format TEXT PRIMARY KEY,
      games_to_win INTEGER NOT NULL,
      minutes_to_play INTEGER NOT NULL
    )
  `, (createErr) => {
    if (createErr) {
      console.error("Failed to ensure duel_formats schema", createErr);
      return;
    }
    db.all("PRAGMA table_info(duel_formats)", (pragmaErr, columns) => {
      if (pragmaErr) {
        console.error("Failed to inspect duel_formats schema", pragmaErr);
        return;
      }
      if (!Array.isArray(columns) || columns.length === 0) return;
      addColumnIfMissing(columns, "duel_formats", "games_to_win", "INTEGER NOT NULL DEFAULT 1");
      addColumnIfMissing(columns, "duel_formats", "minutes_to_play", "INTEGER NOT NULL DEFAULT 60");
    });
  });
}

function ensureGamesSchema() {
  db.run(`
    CREATE TABLE IF NOT EXISTS games (
      id TEXT PRIMARY KEY,
      duel_id TEXT NOT NULL,
      bga_table_id TEXT,
      game_number INTEGER NOT NULL,
      player_1_score INTEGER,
      player_2_score INTEGER,
      player_1_rank INTEGER,
      player_2_rank INTEGER,
      player_1_clock INTEGER NOT NULL DEFAULT 0,
      player_2_clock INTEGER NOT NULL DEFAULT 0,
      status TEXT
    )
  `, (createErr) => {
    if (createErr) {
      console.error("Failed to ensure games schema", createErr);
      return;
    }
    db.all("PRAGMA table_info(games)", (pragmaErr, columns) => {
      if (pragmaErr) {
        console.error("Failed to inspect games schema", pragmaErr);
        return;
      }
      if (!Array.isArray(columns) || columns.length === 0) return;
      if (columns.some((column) => column.name === "lineup_id") && !columns.some((column) => column.name === "duel_id")) {
        db.run("ALTER TABLE games RENAME COLUMN lineup_id TO duel_id", (renameErr) => {
          if (renameErr) {
            console.error("Failed to rename games.lineup_id to games.duel_id", renameErr);
          }
        });
      }
      addColumnIfMissing(columns, "games", "bga_table_id", "TEXT");
      addColumnIfMissing(columns, "games", "game_number", "INTEGER NOT NULL DEFAULT 1");
      addColumnIfMissing(columns, "games", "player_1_score", "INTEGER");
      addColumnIfMissing(columns, "games", "player_2_score", "INTEGER");
      addColumnIfMissing(columns, "games", "player_1_rank", "INTEGER");
      addColumnIfMissing(columns, "games", "player_2_rank", "INTEGER");
      addColumnIfMissing(columns, "games", "player_1_clock", "INTEGER NOT NULL DEFAULT 0");
      addColumnIfMissing(columns, "games", "player_2_clock", "INTEGER NOT NULL DEFAULT 0");
      addColumnIfMissing(columns, "games", "status", "TEXT");
      if (columns.some((column) => column.name === "bga_flags")) {
        db.run("ALTER TABLE games DROP COLUMN bga_flags", (dropErr) => {
          if (dropErr) {
            console.error("Failed to drop games.bga_flags", dropErr);
          }
        });
      }
    });

    db.run(
      "CREATE INDEX IF NOT EXISTS idx_games_duel_id ON games(duel_id)",
      (indexErr) => {
        if (indexErr) console.error("Failed to ensure idx_games_duel_id", indexErr);
      }
    );
    db.run("DROP INDEX IF EXISTS idx_games_lineup_id");
    db.run(
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_games_bga_table_id ON games(bga_table_id)",
      (indexErr) => {
        if (indexErr) console.error("Failed to ensure idx_games_bga_table_id", indexErr);
      }
    );
  });
}

function seedTeamTimezones() {
  const entries = Object.entries(DEFAULT_TEAM_TIMEZONES);
  if (!entries.length) return;
  const stmt = db.prepare(`
    UPDATE teams
    SET timezone = ?
    WHERE upper(id) = ?
      AND (timezone IS NULL OR trim(timezone) = '')
  `);
  entries.forEach(([teamId, timezone]) => {
    stmt.run([timezone, teamId], (err) => {
      if (err) {
        console.error(`Failed to backfill timezone for team ${teamId}`, err);
      }
    });
  });
  stmt.finalize();
}

function ensureTeamsSchema() {
  db.run(`
    CREATE TABLE IF NOT EXISTS teams (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL UNIQUE COLLATE NOCASE,
      logo TEXT,
      flag TEXT,
      type TEXT,
      timezone TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
  `, (createErr) => {
    if (createErr) {
      console.error("Failed to ensure teams schema", createErr);
      return;
    }
    db.all("PRAGMA table_info(teams)", (pragmaErr, columns) => {
      if (pragmaErr) {
        console.error("Failed to inspect teams schema", pragmaErr);
        return;
      }
      if (!Array.isArray(columns) || columns.length === 0) return;
      addColumnIfMissing(columns, "teams", "name", "TEXT");
      addColumnIfMissing(columns, "teams", "logo", "TEXT");
      addColumnIfMissing(columns, "teams", "flag", "TEXT");
      addColumnIfMissing(columns, "teams", "type", "TEXT");
      addColumnIfMissing(columns, "teams", "timezone", "TEXT");
      addColumnIfMissing(columns, "teams", "created_at", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP");
      addColumnIfMissing(columns, "teams", "updated_at", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP");
      db.run(
        `
          UPDATE teams
          SET flag = logo
          WHERE (flag IS NULL OR trim(flag) = '')
            AND trim(COALESCE(logo, '')) <> ''
        `,
        (backfillErr) => {
          if (backfillErr) {
            console.error("Failed to backfill teams.flag from teams.logo", backfillErr);
          }
        }
      );
      seedTeamTimezones();
    });
  });
}

function ensureTournamentsSchema() {
  db.run(`
    CREATE TABLE IF NOT EXISTS tournaments (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      short_title TEXT,
      logo TEXT,
      link TEXT,
      access_type INTEGER NOT NULL DEFAULT 1,
      lineup_size_type INTEGER NOT NULL DEFAULT 2,
      lineup_size INTEGER,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
  `, (createErr) => {
    if (createErr) {
      console.error("Failed to ensure tournaments schema", createErr);
      return;
    }
    db.all("PRAGMA table_info(tournaments)", (pragmaErr, columns) => {
      if (pragmaErr) {
        console.error("Failed to inspect tournaments schema", pragmaErr);
        return;
      }
      if (!Array.isArray(columns) || columns.length === 0) return;
      addColumnIfMissing(columns, "tournaments", "name", "TEXT");
      addColumnIfMissing(columns, "tournaments", "short_title", "TEXT");
      addColumnIfMissing(columns, "tournaments", "logo", "TEXT");
      addColumnIfMissing(columns, "tournaments", "link", "TEXT");
      addColumnIfMissing(columns, "tournaments", "access_type", "INTEGER NOT NULL DEFAULT 1");
      addColumnIfMissing(columns, "tournaments", "lineup_size_type", "INTEGER NOT NULL DEFAULT 2");
      addColumnIfMissing(columns, "tournaments", "lineup_size", "INTEGER");
      addColumnIfMissing(columns, "tournaments", "created_at", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP");
      addColumnIfMissing(columns, "tournaments", "updated_at", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP");
    });
  });
}

function ensureTournamentAccessUsersSchema() {
  db.run(`
    CREATE TABLE IF NOT EXISTS tournament_access_users (
      tournament_id TEXT NOT NULL,
      user_id INTEGER NOT NULL,
      role TEXT NOT NULL DEFAULT 'captain',
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (tournament_id, user_id)
    )
  `, (createErr) => {
    if (createErr) {
      console.error("Failed to ensure tournament_access_users schema", createErr);
      return;
    }
    db.all("PRAGMA table_info(tournament_access_users)", (pragmaErr, columns) => {
      if (pragmaErr) {
        console.error("Failed to inspect tournament_access_users schema", pragmaErr);
        return;
      }
      if (!Array.isArray(columns) || columns.length === 0) return;
      addColumnIfMissing(columns, "tournament_access_users", "role", "TEXT NOT NULL DEFAULT 'captain'");
      addColumnIfMissing(columns, "tournament_access_users", "created_at", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP");
      addColumnIfMissing(columns, "tournament_access_users", "updated_at", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP");
    });
  });
}

function ensureFriendlyFindSchema() {
  db.run(`
    CREATE TABLE IF NOT EXISTS friendly_find (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      team TEXT,
      dates TEXT,
      time_1 TEXT,
      time_2 TEXT,
      number_of_players INTEGER
    )
  `, (createErr) => {
    if (createErr) {
      console.error("Failed to ensure friendly_find schema", createErr);
      return;
    }
    db.all("PRAGMA table_info(friendly_find)", (pragmaErr, columns) => {
      if (pragmaErr) {
        console.error("Failed to inspect friendly_find schema", pragmaErr);
        return;
      }
      if (!Array.isArray(columns) || columns.length === 0) return;
      addColumnIfMissing(columns, "friendly_find", "team", "TEXT");
      addColumnIfMissing(columns, "friendly_find", "dates", "TEXT");
      addColumnIfMissing(columns, "friendly_find", "time_1", "TEXT");
      addColumnIfMissing(columns, "friendly_find", "time_2", "TEXT");
      addColumnIfMissing(columns, "friendly_find", "number_of_players", "INTEGER");
    });
  });
}

function ensureAuditTrailSchema() {
  db.run(`
    CREATE TABLE IF NOT EXISTS audit_trail (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      event_type TEXT NOT NULL,
      entity_type TEXT NOT NULL,
      action TEXT NOT NULL,
      record_id TEXT NOT NULL,
      actor_user_id INTEGER,
      actor_bga_id TEXT,
      actor_bga_nickname TEXT,
      actor_email TEXT,
      changes TEXT,
      metadata TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
  `, (createErr) => {
    if (createErr) {
      console.error("Failed to ensure audit_trail schema", createErr);
      return;
    }
    db.run(
      "CREATE INDEX IF NOT EXISTS idx_audit_trail_created_at ON audit_trail(created_at DESC, id DESC)",
      (indexErr) => {
        if (indexErr) {
          console.error("Failed to ensure audit trail created_at index", indexErr);
        }
      }
    );
  });
}

function safeStringifyJson(value) {
  if (value === undefined) return null;
  try {
    return JSON.stringify(value);
  } catch (_error) {
    return JSON.stringify({ error: "Failed to serialize value" });
  }
}

function parseJsonOrNull(value) {
  const raw = String(value || "").trim();
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch (_error) {
    return raw;
  }
}

function normalizeAuditValue(value) {
  if (value === undefined) return null;
  if (value === null) return null;
  if (Array.isArray(value)) return value.map((entry) => normalizeAuditValue(entry));
  if (value && typeof value === "object") {
    const result = {};
    Object.keys(value).sort().forEach((key) => {
      result[key] = normalizeAuditValue(value[key]);
    });
    return result;
  }
  return value;
}

function auditValuesEqual(left, right) {
  return JSON.stringify(normalizeAuditValue(left)) === JSON.stringify(normalizeAuditValue(right));
}

function buildAuditChanges(before, after, fields) {
  const result = {};
  const keys = Array.isArray(fields) && fields.length
    ? fields
    : Array.from(new Set([
      ...Object.keys(before || {}),
      ...Object.keys(after || {}),
    ]));

  keys.forEach((key) => {
    const previousValue = before ? before[key] : null;
    const nextValue = after ? after[key] : null;
    if (!auditValuesEqual(previousValue, nextValue)) {
      result[key] = {
        old: previousValue ?? null,
        new: nextValue ?? null,
      };
    }
  });

  return result;
}

function buildAuditCreationChanges(after, fields) {
  return buildAuditChanges(null, after || {}, fields);
}

function buildAuditDeletionChanges(before, fields) {
  return buildAuditChanges(before || {}, null, fields);
}

function normalizeLineupAuditRecord(lineup) {
  if (!lineup || typeof lineup !== "object") return {};

  const normalizeInteger = (value) => {
    if (value === null || value === undefined || String(value).trim() === "") return null;
    const parsed = Number(value);
    if (!Number.isFinite(parsed)) return value;
    return Math.trunc(parsed);
  };

  return {
    id: normalizeNullableText(lineup.id),
    tournament_id: normalizeNullableText(lineup.tournament_id),
    match_id: normalizeNullableText(lineup.match_id),
    duel_number: normalizeInteger(lineup.duel_number),
    duel_format: normalizeNullableText(lineup.duel_format),
    time_utc: normalizeNullableText(lineup.time_utc),
    custom_time: normalizeInteger(lineup.custom_time),
    player_1_id: normalizeNullableText(lineup.player_1_id),
    player_2_id: normalizeNullableText(lineup.player_2_id),
    dw1: normalizeInteger(lineup.dw1),
    dw2: normalizeInteger(lineup.dw2),
    status: normalizeNullableText(lineup.status),
  };
}

function buildLineupsAuditChanges(previousLineups, nextLineups) {
  const beforeMap = new Map();
  const afterMap = new Map();

  (Array.isArray(previousLineups) ? previousLineups : []).forEach((lineup) => {
    const normalizedLineup = normalizeLineupAuditRecord(lineup);
    const lineupId = normalizeNullableText(normalizedLineup.id);
    if (lineupId) beforeMap.set(lineupId, normalizedLineup);
  });

  (Array.isArray(nextLineups) ? nextLineups : []).forEach((lineup) => {
    const normalizedLineup = normalizeLineupAuditRecord(lineup);
    const lineupId = normalizeNullableText(normalizedLineup.id);
    if (lineupId) afterMap.set(lineupId, normalizedLineup);
  });

  const lineupIds = Array.from(new Set([
    ...beforeMap.keys(),
    ...afterMap.keys(),
  ])).sort();

  const result = {};
  lineupIds.forEach((lineupId) => {
    const beforeLineup = beforeMap.get(lineupId) || null;
    const afterLineup = afterMap.get(lineupId) || null;
    const lineupChanges = {};

    if (!beforeLineup || !afterLineup) {
      const changedFields = buildAuditChanges(beforeLineup || {}, afterLineup || {});
      Object.keys(changedFields).forEach((field) => {
        if (field === "id") return;
        lineupChanges[field] = {
          old: changedFields[field].old,
          new: changedFields[field].new,
        };
      });
      if (Object.keys(lineupChanges).length) {
        result[lineupId] = lineupChanges;
      }
      return;
    }

    const changedFields = buildAuditChanges(beforeLineup, afterLineup);
    Object.keys(changedFields).forEach((field) => {
      if (field === "id") return;
      lineupChanges[field] = {
        old: changedFields[field].old,
        new: changedFields[field].new,
      };
    });

    if (Object.keys(lineupChanges).length) {
      result[lineupId] = lineupChanges;
    }
  });

  return result;
}

function getAuditActor(user) {
  return {
    actor_user_id: Number.isInteger(Number(user?.id)) ? Number(user.id) : null,
    actor_bga_id: normalizeNullableText(user?.player_id ?? user?.bga_id),
    actor_bga_nickname: normalizeNullableText(user?.bga_nickname),
    actor_email: normalizeNullableText(user?.email),
  };
}

function logAuditEvent(entry, done = () => {}) {
  const actor = entry && typeof entry === "object" ? entry : {};
  db.run(
    `
      INSERT INTO audit_trail (
        event_type,
        entity_type,
        action,
        record_id,
        actor_user_id,
        actor_bga_id,
        actor_bga_nickname,
        actor_email,
        changes,
        metadata
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `,
    [
      normalizeNullableText(actor.event_type) || "unknown",
      normalizeNullableText(actor.entity_type) || "unknown",
      normalizeNullableText(actor.action) || "unknown",
      String(actor.record_id ?? ""),
      Number.isInteger(Number(actor.actor_user_id)) ? Number(actor.actor_user_id) : null,
      normalizeNullableText(actor.actor_bga_id),
      normalizeNullableText(actor.actor_bga_nickname),
      normalizeNullableText(actor.actor_email),
      safeStringifyJson(actor.changes ?? null),
      safeStringifyJson(actor.metadata ?? null),
    ],
    (err) => {
      if (err) {
        console.error("Failed to write audit trail entry", err);
      }
      done(err || null);
    }
  );
}

function loadAuditUserProfileInfo(userId, done) {
  const normalizedUserId = Number(userId);
  if (!Number.isInteger(normalizedUserId) || normalizedUserId <= 0) {
    done(null, null);
    return;
  }

  db.get(
    `
      SELECT
        u.id,
        u.google_id,
        u.email,
        u.name,
        u.bga_id,
        p.bga_nickname
      FROM users u
      LEFT JOIN profiles p
        ON p.id = u.bga_id
       AND p.deleted_at IS NULL
      WHERE u.id = ?
      LIMIT 1
    `,
    [normalizedUserId],
    (err, row) => done(err || null, row || null)
  );
}

function logUserBgaLinkAudit({ actor, userId, oldBgaId, source }, done = () => {}) {
  loadAuditUserProfileInfo(userId, (loadErr, userRow) => {
    if (loadErr || !userRow || !normalizeNullableText(userRow.bga_id)) {
      done(loadErr || null);
      return;
    }

    const changes = buildAuditChanges(
      { bga_id: oldBgaId || null },
      { bga_id: userRow.bga_id, bga_nickname: userRow.bga_nickname || null },
      ["bga_id", "bga_nickname"]
    );

    if (!Object.keys(changes).length) {
      done(null);
      return;
    }

    logAuditEvent(
      {
        ...(actor || {}),
        event_type: "user.bga_linked",
        entity_type: "user",
        action: "link",
        record_id: String(userRow.id),
        changes,
        metadata: {
          source: source || "unknown",
          google_id: userRow.google_id || null,
          user_email: userRow.email || null,
        },
      },
      done
    );
  });
}

db.serialize(() => {
  db.run(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      google_id TEXT NOT NULL UNIQUE,
      email TEXT,
      name TEXT,
      picture TEXT,
      bga_id TEXT,
      admin INTEGER NOT NULL DEFAULT 0,
      last_login TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
  `);
  ensureUsersSchema();

  db.get(
    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'profiles'",
    (profilesCheckErr, profilesTable) => {
      if (profilesCheckErr) {
        console.error("Failed to check profiles table", profilesCheckErr);
        return;
      }

      db.get(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'user_player_links'",
        (linksCheckErr, legacyTable) => {
          if (linksCheckErr) {
            console.error("Failed to check user_player_links table", linksCheckErr);
            return;
          }

          const shouldRenameLegacyTable = !profilesTable && legacyTable;

          if (shouldRenameLegacyTable) {
            db.run("ALTER TABLE user_player_links RENAME TO profiles", (renameErr) => {
              if (renameErr) {
                console.error("Failed to rename user_player_links to profiles", renameErr);
                return;
              }
              ensureProfilesSchema();
            });
            return;
          }

          db.run(`
            CREATE TABLE IF NOT EXISTS profiles (
              email TEXT NOT NULL UNIQUE COLLATE NOCASE,
              bga_nickname TEXT,
              name TEXT,
              association TEXT,
              status TEXT NOT NULL DEFAULT 'Active',
              master_title INTEGER NOT NULL DEFAULT 0,
              master_title_date DATE,
              team_captain INTEGER NOT NULL DEFAULT 0,
              telegram TEXT,
              whatsapp TEXT,
              discord TEXT,
              instagram TEXT,
              contact_email TEXT,
              avatar TEXT,
              id TEXT,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
          `);

          ensureProfilesSchema();

          ensureAssociationsSchema();
          ensureMatchesSchema();
          ensureDuelsSchema();
          ensureDuelFormatsSchema();
          ensureGamesSchema();
          ensureTeamsSchema();
          ensureTournamentsSchema();
          ensureTournamentAccessUsersSchema();
          ensureFriendlyFindSchema();
          ensureAuditTrailSchema();
        }
      );
    }
  );
});

passport.serializeUser((user, done) => {
  done(null, user.id);
});

passport.deserializeUser((id, done) => {
  db.get(
    `
      SELECT
        u.id,
        u.google_id,
        u.email,
        u.name,
        u.picture,
        u.last_login,
        u.bga_id,
        COALESCE(u.admin, 0) AS admin,
        p.id AS player_id,
        p.bga_nickname,
        p.association,
        COALESCE(p.master_title, 0) AS master_title,
        p.master_title_date,
        COALESCE(p.team_captain, 0) AS team_captain,
        p.name AS profile_name,
        p.email AS profile_email
      FROM users u
      LEFT JOIN profiles p
        ON p.id = u.bga_id
       AND p.deleted_at IS NULL
      WHERE u.id = ?
    `,
    [id],
    (err, row) => {
      if (err) return done(err);
      return done(null, row || false);
    }
  );
});

passport.use(
  new GoogleStrategy(
    {
      clientID: process.env.GOOGLE_CLIENT_ID,
      clientSecret: process.env.GOOGLE_CLIENT_SECRET,
      callbackURL: process.env.GOOGLE_CALLBACK_URL,
    },
    (accessToken, refreshToken, profile, done) => {
      const googleId = profile.id;
      const email = profile.emails?.[0]?.value || null;
      const name = profile.displayName || null;
      const picture = profile.photos?.[0]?.value || null;
      const isSeedAdminEmail = String(email || "").trim().toLowerCase() === "z0675006213@gmail.com";

      db.get(
        "SELECT id, bga_id FROM users WHERE google_id = ? LIMIT 1",
        [googleId],
        (lookupErr, existingUserRow) => {
          if (lookupErr) return done(lookupErr);

          db.run(
            `
              INSERT INTO users (google_id, email, name, picture, admin, last_login)
              VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
              ON CONFLICT(google_id)
              DO UPDATE SET
                email = excluded.email,
                name = excluded.name,
                picture = excluded.picture,
                admin = CASE
                  WHEN excluded.admin = 1 THEN 1
                  ELSE COALESCE(users.admin, 0)
                END,
                last_login = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            `,
            [googleId, email, name, picture, isSeedAdminEmail ? 1 : 0],
            (insertErr) => {
              if (insertErr) return done(insertErr);

              db.get(
                "SELECT id, google_id, email, name, picture, bga_id, last_login FROM users WHERE google_id = ?",
                [googleId],
                (selectErr, row) => {
                  if (selectErr) return done(selectErr);
                  if (!row) return done(null, false);

                  const finishWithPotentialLink = (callback) => {
                    if (normalizeNullableText(row.bga_id)) {
                      return loadAuditUserProfileInfo(row.id, (_loadErr, enrichedRow) => {
                        callback(enrichedRow || row);
                      });
                    }

                    return syncUserBgaIdFromEmail(
                      row.id,
                      email,
                      {
                        actor: getAuditActor(row),
                        source: "login",
                      },
                      (syncErr) => {
                        if (syncErr) return done(syncErr);
                        return loadAuditUserProfileInfo(row.id, (_loadErr, enrichedRow) => {
                          callback(enrichedRow || row);
                        });
                      }
                    );
                  };

                  if (existingUserRow) {
                    return finishWithPotentialLink(() => done(null, row));
                  }

                  return finishWithPotentialLink((auditUserRow) => {
                    return logAuditEvent(
                      {
                        ...getAuditActor(auditUserRow),
                        event_type: "user.created",
                        entity_type: "user",
                        action: "create",
                        record_id: String(row.id),
                        metadata: {
                          google_id: row.google_id || null,
                          email: row.email || null,
                          name: row.name || null,
                        },
                      },
                      () => done(null, row)
                    );
                  });
                }
              );
            }
          );
        }
      );
    }
  )
);

const app = express();
const SQLiteStore = connectSqlite3(session);

app.set("trust proxy", 1);
app.use(express.json());
app.use(
  cors({
    origin: (origin, callback) => {
      if (!origin || FRONTEND_ORIGINS.includes(origin)) {
        return callback(null, true);
      }
      return callback(new Error(`CORS blocked for origin: ${origin}`));
    },
    credentials: true,
  })
);

app.use(
  session({
    store: new SQLiteStore({ db: "sessions.sqlite", dir: path.dirname(dbFullPath) }),
    secret: process.env.SESSION_SECRET,
    resave: false,
    saveUninitialized: false,
    cookie: {
      httpOnly: true,
      secure: cookieSecure,
      sameSite: cookieSameSite,
      maxAge: 1000 * 60 * 60 * 24 * 30,
    },
  })
);

app.use(passport.initialize());
app.use(passport.session());
app.use((req, _res, next) => {
  const userId = Number(req.user?.id);
  if (!Number.isInteger(userId) || userId <= 0) {
    next();
    return;
  }
  db.run(
    `
      UPDATE users
      SET
        last_login = CURRENT_TIMESTAMP,
        updated_at = CURRENT_TIMESTAMP
      WHERE id = ?
        AND (
          last_login IS NULL
          OR datetime(last_login) < datetime('now', '-5 minutes')
        )
    `,
    [userId],
    () => next()
  );
});

function requireAdmin(req, res, next) {
  if (!req.user) {
    return res.status(401).json({ ok: false, message: "Unauthorized" });
  }
  if (Number(req.user.admin) !== 1) {
    return res.status(403).json({ ok: false, message: "Forbidden" });
  }
  return next();
}

app.get("/health", (_req, res) => {
  res.json({ ok: true });
});

app.get(
  "/auth/google",
  passport.authenticate("google", {
    scope: ["profile", "email"],
    prompt: "select_account",
  })
);

app.get(
  "/auth/google/callback",
  passport.authenticate("google", {
    failureRedirect: "/auth/failure",
    session: true,
  }),
  (_req, res) => {
    res.type("html").send(`<!doctype html>
<html>
<head><meta charset="utf-8"><title>Auth complete</title></head>
<body>
<script>
  if (window.opener) {
    window.opener.postMessage({ type: "google-auth-success" }, "*");
    window.close();
  } else {
    window.location.href = "${PRIMARY_FRONTEND_ORIGIN}";
  }
</script>
</body>
</html>`);
  }
);

app.get("/auth/failure", (_req, res) => {
  res.status(401).json({ ok: false, message: "Google auth failed" });
});

app.get("/profiles/public", (_req, res, next) => {
  db.all(
    `
      SELECT
        id,
        bga_nickname,
        name,
        association,
        COALESCE(NULLIF(trim(status), ''), 'Active') AS status,
        email,
        created_by,
        COALESCE(master_title, 0) AS master_title,
        master_title_date,
        COALESCE(team_captain, 0) AS team_captain
      FROM profiles
      WHERE trim(COALESCE(id, '')) <> ''
        AND deleted_at IS NULL
    `,
    (err, rows) => {
      if (err) return next(err);
      return res.json({ ok: true, profiles: rows || [] });
    }
  );
});

app.post("/profiles", (req, res) => {
  if (!req.user) {
    return res.status(401).json({ ok: false, message: "Unauthorized" });
  }

  const isAdmin = Number(req.user.admin) === 1;
  const isTeamCaptain = Number(req.user.team_captain) === 1;
  const userAssociation = String(req.user.association || "").trim().toUpperCase();
  const actorPlayerId = String(req.user.player_id || "").trim() || null;
  const payload = req.body && typeof req.body === "object" ? req.body : {};
  const hasNameInPayload = Object.prototype.hasOwnProperty.call(payload, "name");
  const hasTeamCaptainInPayload = Object.prototype.hasOwnProperty.call(payload, "team_captain");

  const playerId = String(payload.id ?? payload.player_id ?? "").trim();
  const bgaNickname = String(payload.bga_nickname || "").trim();
  const association = String(payload.association || "").trim().toUpperCase();
  const name = String(payload.name || "").trim() || null;

  const normalizeText = (value) => {
    const v = String(value ?? "").trim();
    return v === "" ? null : v;
  };

  const normalizeBool = (value) => {
    if (typeof value === "boolean") return value;
    if (typeof value === "number") return value === 1;
    const raw = String(value ?? "").trim().toLowerCase();
    return raw === "1" || raw === "true" || raw === "yes" || raw === "on";
  };

  const profilePatch = {
    name,
    status: normalizeText(payload.status) || "Active",
    master_title: normalizeBool(payload.master_title) ? 1 : 0,
    master_title_date: normalizeText(payload.master_title_date),
    email: normalizeText(payload.email),
    association,
    team_captain: hasTeamCaptainInPayload ? (normalizeBool(payload.team_captain) ? 1 : 0) : 0,
    telegram: normalizeText(payload.telegram),
    whatsapp: normalizeText(payload.whatsapp),
    discord: normalizeText(payload.discord),
    instagram: normalizeText(payload.instagram),
    contact_email: normalizeText(payload.contact_email),
  };

  if (!/^\d{6,9}$/.test(playerId)) {
    return res.status(400).json({ ok: false, message: "id must be 6-9 digits" });
  }
  if (!bgaNickname) {
    return res.status(400).json({ ok: false, message: "bga_nickname is required" });
  }
  if (!association) {
    return res.status(400).json({ ok: false, message: "association is required" });
  }
  if (profilePatch.master_title === 1 && !profilePatch.master_title_date) {
    return res.status(400).json({
      ok: false,
      message: "master_title_date is required when master_title is enabled",
    });
  }
  if (profilePatch.master_title === 0) {
    profilePatch.master_title_date = null;
  }
  if (profilePatch.status !== "Active" && profilePatch.status !== "Inactive") {
    return res.status(400).json({ ok: false, message: "status must be Active or Inactive" });
  }
  const requestedTournamentId = normalizeText(payload.tournament_id);

  const continueWithProfileCreate = () => db.get(
    `
      SELECT id, bga_nickname, deleted_at
      FROM profiles
      WHERE id = ? OR lower(COALESCE(bga_nickname, '')) = lower(?)
      LIMIT 1
    `,
    [playerId, bgaNickname],
    (dupErr, dupRow) => {
      if (dupErr) {
        return res.status(500).json({ ok: false, message: "Failed to validate profile uniqueness" });
      }
      if (dupRow) {
        const deletedAt = String(dupRow.deleted_at || "").trim();
        if (!deletedAt) {
          return res.status(409).json({ ok: false, message: "Profile with this id or bga_nickname already exists" });
        }

        const restorePatch = isAdmin
          ? profilePatch
          : {
              ...profilePatch,
              association,
              team_captain: 0,
            };

        return db.run(
          `
            UPDATE profiles
            SET
              id = ?,
              bga_nickname = ?,
              name = CASE WHEN ? THEN ? ELSE name END,
              association = ?,
              status = ?,
              email = ?,
              master_title = ?,
              master_title_date = ?,
              team_captain = ?,
              telegram = ?,
              whatsapp = ?,
              discord = ?,
              instagram = ?,
              contact_email = ?,
              deleted_at = NULL,
              deleted_by = NULL,
              updated_by = ?,
              updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
          `,
          [
            playerId,
            bgaNickname,
            hasNameInPayload ? 1 : 0,
            restorePatch.name,
            association,
            restorePatch.status,
            restorePatch.email,
            restorePatch.master_title,
            restorePatch.master_title_date,
            restorePatch.team_captain,
            restorePatch.telegram,
            restorePatch.whatsapp,
            restorePatch.discord,
            restorePatch.instagram,
            restorePatch.contact_email,
            actorPlayerId,
            String(dupRow.id || "").trim(),
          ],
          function onRestore(restoreErr) {
            if (restoreErr) {
              return res.status(500).json({ ok: false, message: "Failed to restore profile" });
            }
            if (!this || this.changes === 0) {
              return res.status(404).json({ ok: false, message: "Profile not found" });
            }
            return db.get(
              `
                SELECT
                  id,
                  bga_nickname,
                  name,
                  association,
                  COALESCE(NULLIF(trim(status), ''), 'Active') AS status,
                  email,
                  COALESCE(master_title, 0) AS master_title,
                  master_title_date,
                  COALESCE(team_captain, 0) AS team_captain,
                  created_by,
                  telegram,
                  whatsapp,
                  discord,
                  instagram,
                  contact_email
                FROM profiles
                WHERE id = ?
                  AND deleted_at IS NULL
                LIMIT 1
              `,
              [playerId],
              (selectErr, row) => {
                if (selectErr) {
                  return res.status(500).json({ ok: false, message: "Failed to load restored profile" });
                }
                return logAuditEvent(
                  {
                    ...getAuditActor(req.user),
                    event_type: "profile.created",
                    entity_type: "profile",
                    action: "create",
                    record_id: playerId,
                    changes: buildAuditCreationChanges(row || {}, PROFILE_AUDIT_FIELDS),
                    metadata: { restored: true },
                  },
                  () => res.status(200).json({ ok: true, restored: true, profile: row || null })
                );
              }
            );
          }
        );
      }

      return db.run(
        `
          INSERT INTO profiles (
            id,
            bga_nickname,
            name,
            association,
            status,
            email,
            master_title,
            master_title_date,
            team_captain,
            admin,
            telegram,
            whatsapp,
            discord,
            instagram,
            contact_email,
            created_by,
            updated_by,
            created_at,
            updated_at
          )
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        `,
        [
          playerId,
          bgaNickname,
          profilePatch.name,
          association,
          profilePatch.status,
          profilePatch.email,
          profilePatch.master_title,
          profilePatch.master_title_date,
          isAdmin ? profilePatch.team_captain : 0,
          profilePatch.telegram,
          profilePatch.whatsapp,
          profilePatch.discord,
          profilePatch.instagram,
          profilePatch.contact_email,
          actorPlayerId,
          actorPlayerId,
        ],
        function onInsert(insertErr) {
          if (insertErr) {
            return res.status(500).json({ ok: false, message: "Failed to create profile" });
          }
          return db.get(
            `
              SELECT
                id,
                bga_nickname,
                name,
                association,
                COALESCE(NULLIF(trim(status), ''), 'Active') AS status,
                email,
                COALESCE(master_title, 0) AS master_title,
                master_title_date,
                COALESCE(team_captain, 0) AS team_captain,
                created_by,
                telegram,
                whatsapp,
                discord,
                instagram,
                contact_email
              FROM profiles
              WHERE id = ?
                AND deleted_at IS NULL
              LIMIT 1
            `,
            [playerId],
            (selectErr, row) => {
              if (selectErr) {
                return res.status(500).json({ ok: false, message: "Failed to load created profile" });
              }
              return logAuditEvent(
                {
                  ...getAuditActor(req.user),
                  event_type: "profile.created",
                  entity_type: "profile",
                  action: "create",
                  record_id: playerId,
                  changes: buildAuditCreationChanges(row || {}, PROFILE_AUDIT_FIELDS),
                },
                () => res.status(201).json({ ok: true, profile: row || null })
              );
            }
          );
        }
      );
    }
  );

  if (isAdmin) {
    return continueWithProfileCreate();
  }

  if (isTeamCaptain && userAssociation && association === userAssociation) {
    return continueWithProfileCreate();
  }

  if (!requestedTournamentId) {
    if (!isTeamCaptain || !userAssociation) {
      return res.status(403).json({ ok: false, message: "Forbidden" });
    }
    return res.status(403).json({ ok: false, message: "Captain can create profiles only in own association" });
  }

  return loadTournamentAccessForUser(requestedTournamentId, req.user, (tournamentErr, tournament) => {
    if (tournamentErr) {
      return res.status(500).json({ ok: false, message: "Failed to validate tournament access" });
    }
    if (
      tournament?.access_type === TOURNAMENT_ACCESS_TYPES.CLOSED
      && tournament?.has_access
      && tournament?.access_role === TOURNAMENT_ACCESS_ROLES.ADMIN
    ) {
      return continueWithProfileCreate();
    }
    if (!isTeamCaptain || !userAssociation) {
      return res.status(403).json({ ok: false, message: "Forbidden" });
    }
    return res.status(403).json({ ok: false, message: "Captain can create profiles only in own association" });
  });
});

app.get("/profiles/contacts/:playerId", (req, res) => {
  const requestedPlayerId = String(req.params.playerId || "").trim();
  if (!requestedPlayerId) {
    return res.status(400).json({ ok: false, message: "playerId is required" });
  }

  if (!req.user) {
    return res.status(401).json({ ok: false, message: "Unauthorized" });
  }

  const linkedPlayerId = String(req.user.player_id || "").trim();
  if (!linkedPlayerId) {
    return res.status(403).json({ ok: false, message: "BGA profile is not linked" });
  }

  return db.get(
    `
      SELECT
        telegram,
        whatsapp,
        discord,
        instagram,
        contact_email
      FROM profiles
      WHERE id = ?
        AND deleted_at IS NULL
      LIMIT 1
    `,
    [requestedPlayerId],
    (err, row) => {
      if (err) {
        return res.status(500).json({ ok: false, message: "Failed to load contacts" });
      }
      if (!row) {
        return res.status(404).json({ ok: false, message: "Profile not found" });
      }
      return res.json({
        ok: true,
        contacts: {
          telegram: row.telegram || null,
          whatsapp: row.whatsapp || null,
          discord: row.discord || null,
          instagram: row.instagram || null,
          contact_email: row.contact_email || null,
        },
      });
    }
  );
});

app.get("/associations", (_req, res, next) => {
  db.all(
    `
      SELECT
        COALESCE(NULLIF(trim(code), ''), printf('ASSOCIATION_%s', rowid)) AS id,
        name,
        flag,
        created_at,
        updated_at
      FROM associations
      WHERE trim(COALESCE(name, '')) <> ''
      ORDER BY name COLLATE NOCASE ASC
    `,
    (err, rows) => {
      if (err) return next(err);
      return res.json({ ok: true, associations: rows || [] });
    }
  );
});

app.post("/associations", requireAdmin, (req, res) => {
  const id = normalizeEntityId(req.body?.id);
  const name = String(req.body?.name || "").trim();
  const flag = String(req.body?.flag || "").trim() || null;
  if (!id) {
    return res.status(400).json({ ok: false, message: "id is required" });
  }
  if (!name) {
    return res.status(400).json({ ok: false, message: "name is required" });
  }

  return db.get(
    `
      SELECT rowid
      FROM associations
      WHERE upper(trim(COALESCE(code, ''))) = upper(?)
         OR lower(trim(COALESCE(name, ''))) = lower(?)
      LIMIT 1
    `,
    [id, name],
    (dupErr, dupRow) => {
      if (dupErr) {
        return res.status(500).json({ ok: false, message: "Failed to validate association uniqueness" });
      }
      if (dupRow) {
        return res.status(409).json({ ok: false, message: "Association with this id or name already exists" });
      }

      return db.run(
        `
          INSERT INTO associations (code, name, flag, created_at, updated_at)
          VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        `,
        [id, name, flag],
        function onInsert(err) {
          if (err) {
            if (String(err.message || "").includes("UNIQUE")) {
              return res.status(409).json({ ok: false, message: "Association with this id or name already exists" });
            }
            return res.status(500).json({ ok: false, message: "Failed to create association" });
          }

          return db.get(
            `
              SELECT
                COALESCE(NULLIF(trim(code), ''), printf('ASSOCIATION_%s', rowid)) AS id,
                name,
                flag,
                created_at,
                updated_at
              FROM associations
              WHERE rowid = ?
              LIMIT 1
            `,
            [this.lastID],
            (selectErr, row) => {
              if (selectErr) {
                return res.status(500).json({ ok: false, message: "Failed to load association" });
              }
              return res.json({ ok: true, association: row || null });
            }
          );
        }
      );
    }
  );
});

app.patch("/associations/:id", requireAdmin, (req, res) => {
  const associationId = normalizeEntityId(req.params.id);
  const payloadId = normalizeEntityId(req.body?.id);
  const name = String(req.body?.name || "").trim();
  const flag = String(req.body?.flag || "").trim() || null;
  if (!associationId) {
    return res.status(400).json({ ok: false, message: "Invalid association id" });
  }
  if (payloadId && payloadId !== associationId) {
    return res.status(400).json({ ok: false, message: "Association id cannot be changed" });
  }
  if (!name) {
    return res.status(400).json({ ok: false, message: "name is required" });
  }

  return db.get(
    `
      SELECT rowid
      FROM associations
      WHERE upper(trim(COALESCE(code, ''))) = upper(?)
         OR CAST(rowid AS TEXT) = ?
      LIMIT 1
    `,
    [associationId, req.params.id],
    (rowErr, currentRow) => {
      if (rowErr) {
        return res.status(500).json({ ok: false, message: "Failed to load association" });
      }
      if (!currentRow) {
        return res.status(404).json({ ok: false, message: "Association not found" });
      }

      return db.get(
        `
          SELECT rowid
          FROM associations
          WHERE (
            upper(trim(COALESCE(code, ''))) = upper(?)
            OR lower(trim(COALESCE(name, ''))) = lower(?)
          )
            AND rowid <> ?
          LIMIT 1
        `,
        [associationId, name, currentRow.rowid],
        (dupErr, dupRow) => {
          if (dupErr) {
            return res.status(500).json({ ok: false, message: "Failed to validate association uniqueness" });
          }
          if (dupRow) {
            return res.status(409).json({ ok: false, message: "Association with this id or name already exists" });
          }

          return db.run(
            `
              UPDATE associations
              SET
                code = ?,
                name = ?,
                flag = ?,
                updated_at = CURRENT_TIMESTAMP
              WHERE rowid = ?
            `,
            [associationId, name, flag, currentRow.rowid],
            function onUpdate(err) {
              if (err) {
                if (String(err.message || "").includes("UNIQUE")) {
                  return res.status(409).json({ ok: false, message: "Association with this id or name already exists" });
                }
                return res.status(500).json({ ok: false, message: "Failed to update association" });
              }
              if (!this || this.changes === 0) {
                return res.status(404).json({ ok: false, message: "Association not found" });
              }

              return db.get(
                `
                  SELECT
                    COALESCE(NULLIF(trim(code), ''), printf('ASSOCIATION_%s', rowid)) AS id,
                    name,
                    flag,
                    created_at,
                    updated_at
                  FROM associations
                  WHERE rowid = ?
                  LIMIT 1
                `,
                [currentRow.rowid],
                (selectErr, row) => {
                  if (selectErr) {
                    return res.status(500).json({ ok: false, message: "Failed to load association" });
                  }
                  return res.json({ ok: true, association: row || null });
                }
              );
            }
          );
        }
      );
    }
  );
});

app.delete("/associations/:id", requireAdmin, (req, res) => {
  const associationId = normalizeEntityId(req.params.id);
  if (!associationId) {
    return res.status(400).json({ ok: false, message: "Invalid association id" });
  }

  return db.run(
    `
      DELETE FROM associations
      WHERE upper(trim(COALESCE(code, ''))) = upper(?)
         OR CAST(rowid AS TEXT) = ?
    `,
    [associationId, req.params.id],
    function onDelete(err) {
      if (err) {
        return res.status(500).json({ ok: false, message: "Failed to delete association" });
      }
      if (!this || this.changes === 0) {
        return res.status(404).json({ ok: false, message: "Association not found" });
      }
      return res.json({ ok: true });
    }
  );
});

app.get("/duel-formats", requireAdmin, (_req, res, next) => {
  db.all(
    `
      SELECT
        format,
        games_to_win,
        minutes_to_play
      FROM duel_formats
      ORDER BY format COLLATE NOCASE ASC
    `,
    (err, rows) => {
      if (err) return next(err);
      return res.json({ ok: true, duel_formats: rows || [] });
    }
  );
});

app.post("/duel-formats", requireAdmin, (req, res) => {
  const format = normalizeNullableText(req.body?.format);
  const gamesToWin = normalizePositiveInteger(req.body?.games_to_win);
  const minutesToPlay = normalizePositiveInteger(req.body?.minutes_to_play);

  if (!format) {
    return res.status(400).json({ ok: false, message: "format is required" });
  }
  if (!gamesToWin) {
    return res.status(400).json({ ok: false, message: "games_to_win must be a positive integer" });
  }
  if (!minutesToPlay) {
    return res.status(400).json({ ok: false, message: "minutes_to_play must be a positive integer" });
  }

  return db.get(
    "SELECT format FROM duel_formats WHERE upper(trim(format)) = upper(?) LIMIT 1",
    [format],
    (dupErr, dupRow) => {
      if (dupErr) {
        return res.status(500).json({ ok: false, message: "Failed to validate duel format uniqueness" });
      }
      if (dupRow) {
        return res.status(409).json({ ok: false, message: "Duel format already exists" });
      }

      return db.run(
        `
          INSERT INTO duel_formats (format, games_to_win, minutes_to_play)
          VALUES (?, ?, ?)
        `,
        [format, gamesToWin, minutesToPlay],
        (insertErr) => {
          if (insertErr) {
            if (String(insertErr.message || "").includes("UNIQUE")) {
              return res.status(409).json({ ok: false, message: "Duel format already exists" });
            }
            return res.status(500).json({ ok: false, message: "Failed to create duel format" });
          }

          return db.get(
            `
              SELECT format, games_to_win, minutes_to_play
              FROM duel_formats
              WHERE upper(trim(format)) = upper(?)
              LIMIT 1
            `,
            [format],
            (selectErr, row) => {
              if (selectErr) {
                return res.status(500).json({ ok: false, message: "Failed to load duel format" });
              }
              return res.json({ ok: true, duel_format: row || null });
            }
          );
        }
      );
    }
  );
});

app.patch("/duel-formats/:format", requireAdmin, (req, res) => {
  const format = normalizeNullableText(req.params.format);
  const payloadFormat = normalizeNullableText(req.body?.format);
  const gamesToWin = normalizePositiveInteger(req.body?.games_to_win);
  const minutesToPlay = normalizePositiveInteger(req.body?.minutes_to_play);

  if (!format) {
    return res.status(400).json({ ok: false, message: "Invalid duel format" });
  }
  if (payloadFormat && payloadFormat.toLowerCase() !== format.toLowerCase()) {
    return res.status(400).json({ ok: false, message: "format cannot be changed" });
  }
  if (!gamesToWin) {
    return res.status(400).json({ ok: false, message: "games_to_win must be a positive integer" });
  }
  if (!minutesToPlay) {
    return res.status(400).json({ ok: false, message: "minutes_to_play must be a positive integer" });
  }

  return db.run(
    `
      UPDATE duel_formats
      SET
        games_to_win = ?,
        minutes_to_play = ?
      WHERE upper(trim(format)) = upper(?)
    `,
    [gamesToWin, minutesToPlay, format],
    function onUpdate(err) {
      if (err) {
        return res.status(500).json({ ok: false, message: "Failed to update duel format" });
      }
      if (!this || this.changes === 0) {
        return res.status(404).json({ ok: false, message: "Duel format not found" });
      }

      return db.get(
        `
          SELECT format, games_to_win, minutes_to_play
          FROM duel_formats
          WHERE upper(trim(format)) = upper(?)
          LIMIT 1
        `,
        [format],
        (selectErr, row) => {
          if (selectErr) {
            return res.status(500).json({ ok: false, message: "Failed to load duel format" });
          }
          return res.json({ ok: true, duel_format: row || null });
        }
      );
    }
  );
});

app.get("/tournaments", (req, res, next) => {
  const includeAccessUsers = Number(req.user?.admin) === 1;
  const userId = getTournamentAccessUserId(req.user);
  const visibilitySql = includeAccessUsers
    ? ""
    : `
      WHERE
        COALESCE(t.access_type, ?) = ?
        OR (? > 0 AND EXISTS (
          SELECT 1
          FROM tournament_access_users tau_filter
          WHERE upper(trim(tau_filter.tournament_id)) = upper(trim(t.id))
            AND tau_filter.user_id = ?
        ))
    `;
  db.all(
    `
      SELECT
        t.id,
        t.name,
        t.short_title,
        t.logo,
        t.link,
        COALESCE(t.access_type, ?) AS access_type,
        COALESCE(t.lineup_size_type, ?) AS lineup_size_type,
        t.lineup_size,
        CASE
          WHEN ? = 1 THEN ?
          WHEN ? > 0 THEN (
            SELECT COALESCE(NULLIF(lower(trim(tau_role.role)), ''), ?)
            FROM tournament_access_users tau_role
            WHERE upper(trim(tau_role.tournament_id)) = upper(trim(t.id))
              AND tau_role.user_id = ?
            LIMIT 1
          )
          ELSE NULL
        END AS access_role,
        (
          SELECT GROUP_CONCAT(tau.user_id)
          FROM tournament_access_users tau
          WHERE upper(trim(tau.tournament_id)) = upper(trim(t.id))
        ) AS access_user_ids_csv,
        (
          SELECT GROUP_CONCAT(
            CAST(tau.user_id AS TEXT) || ':' || COALESCE(NULLIF(lower(trim(tau.role)), ''), 'captain'),
            '|'
          )
          FROM tournament_access_users tau
          WHERE upper(trim(tau.tournament_id)) = upper(trim(t.id))
        ) AS access_users_csv
      FROM tournaments t
      ${visibilitySql}
      ORDER BY t.id COLLATE NOCASE ASC
    `,
    includeAccessUsers
      ? [
          TOURNAMENT_ACCESS_TYPES.OPEN,
          TOURNAMENT_LINEUP_SIZE_TYPES.FLEXIBLE,
          1,
          TOURNAMENT_ACCESS_ROLES.ADMIN,
          userId,
          TOURNAMENT_ACCESS_ROLES.CAPTAIN,
          userId,
        ]
      : [
          TOURNAMENT_ACCESS_TYPES.OPEN,
          TOURNAMENT_LINEUP_SIZE_TYPES.FLEXIBLE,
          0,
          TOURNAMENT_ACCESS_ROLES.ADMIN,
          userId,
          TOURNAMENT_ACCESS_ROLES.CAPTAIN,
          userId,
          TOURNAMENT_ACCESS_TYPES.OPEN,
          TOURNAMENT_ACCESS_TYPES.OPEN,
          userId,
          userId,
        ],
    (err, rows) => {
      if (err) return next(err);
      return res.json({
        ok: true,
        tournaments: (rows || []).map((row) => {
          const tournament = {
            id: row.id,
            name: row.name,
            short_title: row.short_title,
            logo: row.logo,
            link: row.link,
            access_type: normalizeTournamentAccessType(row.access_type),
            lineup_size_type: normalizeTournamentLineupSizeType(row.lineup_size_type),
            lineup_size: normalizeTournamentLineupSize(row.lineup_size),
            access_role: row.access_role ? normalizeTournamentAccessRole(row.access_role) : null,
          };
          if (includeAccessUsers) {
            tournament.access_user_ids = String(row.access_user_ids_csv || "")
              .split(",")
              .map((value) => Number.parseInt(value, 10))
              .filter((value) => Number.isInteger(value) && value > 0);
            tournament.access_users = String(row.access_users_csv || "")
              .split("|")
              .map((entry) => {
                const [userIdRaw, roleRaw] = String(entry || "").split(":");
                const userId = Number.parseInt(String(userIdRaw || "").trim(), 10);
                if (!Number.isInteger(userId) || userId <= 0) return null;
                return {
                  user_id: userId,
                  role: normalizeTournamentAccessRole(roleRaw),
                };
              })
              .filter(Boolean);
          }
          return tournament;
        }),
      });
    }
  );
});

app.post("/tournaments", requireAdmin, (req, res) => {
  const id = normalizeNullableText(req.body?.id);
  const name = String(req.body?.name || "").trim();
  const shortTitle = String(req.body?.short_title || "").trim() || null;
  const logo = String(req.body?.logo || "").trim() || null;
  const link = String(req.body?.link || "").trim() || null;
  const accessType = normalizeTournamentAccessType(req.body?.access_type);
  const lineupSizeType = normalizeTournamentLineupSizeType(req.body?.lineup_size_type);
  const lineupSize = lineupSizeType === TOURNAMENT_LINEUP_SIZE_TYPES.FIXED
    ? normalizeTournamentLineupSize(req.body?.lineup_size)
    : null;
  const requestedAccessUsers = normalizeTournamentAccessUsers(req.body?.access_users, req.body?.access_user_ids);

  if (!id) {
    return res.status(400).json({ ok: false, message: "id is required" });
  }
  if (!name) {
    return res.status(400).json({ ok: false, message: "name is required" });
  }
  if (lineupSizeType === TOURNAMENT_LINEUP_SIZE_TYPES.FIXED && !lineupSize) {
    return res.status(400).json({ ok: false, message: "lineup_size is required for fixed lineup size" });
  }

  return db.get(
    `
      SELECT id
      FROM tournaments
      WHERE upper(trim(COALESCE(id, ''))) = upper(?)
      LIMIT 1
    `,
    [id],
    (dupErr, dupRow) => {
      if (dupErr) {
        return res.status(500).json({ ok: false, message: "Failed to validate tournament uniqueness" });
      }
      if (dupRow) {
        return res.status(409).json({ ok: false, message: "Tournament with this id already exists" });
      }

      return validateTournamentAccessUserIds(
        accessType === TOURNAMENT_ACCESS_TYPES.CLOSED ? requestedAccessUsers : [],
        (validateErr, accessUsers) => {
          if (validateErr) {
            return res.status(400).json({ ok: false, message: validateErr.message || "Invalid tournament access users" });
          }

          return db.serialize(() => {
            db.run("BEGIN IMMEDIATE TRANSACTION");
            db.run(
              `
                INSERT INTO tournaments (
                  id,
                  name,
                  short_title,
                  logo,
                  link,
                  access_type,
                  lineup_size_type,
                  lineup_size,
                  created_at,
                  updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
              `,
              [id, name, shortTitle, logo, link, accessType, lineupSizeType, lineupSize],
              (insertErr) => {
                if (insertErr) {
                  db.run("ROLLBACK");
                  if (String(insertErr.message || "").includes("UNIQUE")) {
                    return res.status(409).json({ ok: false, message: "Tournament with this id already exists" });
                  }
                  return res.status(500).json({ ok: false, message: "Failed to create tournament" });
                }

                return replaceTournamentAccessUsers(id, accessUsers, (accessErr) => {
                  if (accessErr) {
                    db.run("ROLLBACK");
                    return res.status(500).json({ ok: false, message: "Failed to save tournament access users" });
                  }

                  return db.run("COMMIT", (commitErr) => {
                    if (commitErr) {
                      db.run("ROLLBACK");
                      return res.status(500).json({ ok: false, message: "Failed to create tournament" });
                    }

                    return loadTournamentRowById(id, true, (selectErr, row) => {
                      if (selectErr) {
                        return res.status(500).json({ ok: false, message: "Failed to load tournament" });
                      }
                      return res.json({ ok: true, tournament: row || null });
                    });
                  });
                });
              }
            );
          });
        }
      );
    }
  );
});

app.patch("/tournaments/:id", requireAdmin, (req, res) => {
  const tournamentId = normalizeNullableText(req.params.id);
  const payloadId = normalizeNullableText(req.body?.id);
  const name = String(req.body?.name || "").trim();
  const shortTitle = String(req.body?.short_title || "").trim() || null;
  const logo = String(req.body?.logo || "").trim() || null;
  const link = String(req.body?.link || "").trim() || null;
  const accessType = normalizeTournamentAccessType(req.body?.access_type);
  const lineupSizeType = normalizeTournamentLineupSizeType(req.body?.lineup_size_type);
  const lineupSize = lineupSizeType === TOURNAMENT_LINEUP_SIZE_TYPES.FIXED
    ? normalizeTournamentLineupSize(req.body?.lineup_size)
    : null;
  const requestedAccessUsers = normalizeTournamentAccessUsers(req.body?.access_users, req.body?.access_user_ids);

  if (!tournamentId) {
    return res.status(400).json({ ok: false, message: "Invalid tournament id" });
  }
  if (payloadId && payloadId !== tournamentId) {
    return res.status(400).json({ ok: false, message: "Tournament id cannot be changed" });
  }
  if (!name) {
    return res.status(400).json({ ok: false, message: "name is required" });
  }
  if (lineupSizeType === TOURNAMENT_LINEUP_SIZE_TYPES.FIXED && !lineupSize) {
    return res.status(400).json({ ok: false, message: "lineup_size is required for fixed lineup size" });
  }

  const [rawTournamentId, normalizedTournamentId] = getTournamentLookupVariants(tournamentId);

  return db.get(
    `
      SELECT id
      FROM tournaments
      WHERE ${buildTournamentLookupWhereClause("id")}
      LIMIT 1
    `,
    [rawTournamentId, normalizedTournamentId || rawTournamentId],
    (rowErr, currentRow) => {
      if (rowErr) {
        return res.status(500).json({ ok: false, message: "Failed to load tournament" });
      }
      if (!currentRow) {
        return res.status(404).json({ ok: false, message: "Tournament not found" });
      }

      return validateTournamentAccessUserIds(
        accessType === TOURNAMENT_ACCESS_TYPES.CLOSED ? requestedAccessUsers : [],
        (validateErr, accessUsers) => {
          if (validateErr) {
            return res.status(400).json({ ok: false, message: validateErr.message || "Invalid tournament access users" });
          }

          return db.serialize(() => {
            db.run("BEGIN IMMEDIATE TRANSACTION");
            db.run(
              `
                UPDATE tournaments
                SET
                  id = ?,
                  name = ?,
                  short_title = ?,
                  logo = ?,
                  link = ?,
                  access_type = ?,
                  lineup_size_type = ?,
                  lineup_size = ?,
                  updated_at = CURRENT_TIMESTAMP
                WHERE ${buildTournamentLookupWhereClause("id")}
              `,
              [
                currentRow.id,
                name,
                shortTitle,
                logo,
                link,
                accessType,
                lineupSizeType,
                lineupSize,
                rawTournamentId,
                normalizedTournamentId || rawTournamentId,
              ],
              function onUpdate(err) {
                if (err) {
                  db.run("ROLLBACK");
                  if (String(err.message || "").includes("UNIQUE")) {
                    return res.status(409).json({ ok: false, message: "Tournament with this id already exists" });
                  }
                  return res.status(500).json({ ok: false, message: "Failed to update tournament" });
                }
                if (!this || this.changes === 0) {
                  db.run("ROLLBACK");
                  return res.status(404).json({ ok: false, message: "Tournament not found" });
                }

                return replaceTournamentAccessUsers(currentRow.id, accessUsers, (accessErr) => {
                  if (accessErr) {
                    db.run("ROLLBACK");
                    return res.status(500).json({ ok: false, message: "Failed to save tournament access users" });
                  }

                  return db.run("COMMIT", (commitErr) => {
                    if (commitErr) {
                      db.run("ROLLBACK");
                      return res.status(500).json({ ok: false, message: "Failed to update tournament" });
                    }

                    return loadTournamentRowById(currentRow.id, true, (selectErr, row) => {
                      if (selectErr) {
                        return res.status(500).json({ ok: false, message: "Failed to load tournament" });
                      }
                      return res.json({ ok: true, tournament: row || null });
                    });
                  });
                });
              }
            );
          });
        }
      );
    }
  );
});

app.get("/teams", (_req, res, next) => {
  db.all(
    `
      SELECT
        id,
        name,
        COALESCE(NULLIF(trim(flag), ''), NULLIF(trim(logo), '')) AS logo,
        COALESCE(NULLIF(trim(flag), ''), NULLIF(trim(logo), '')) AS flag,
        type,
        timezone
      FROM teams
      ORDER BY name COLLATE NOCASE ASC
    `,
    (err, rows) => {
      if (err) return next(err);
      return res.json({ ok: true, teams: rows || [] });
    }
  );
});

app.post("/teams", requireAdmin, (req, res) => {
  const id = normalizeEntityId(req.body?.id);
  const name = String(req.body?.name || "").trim();
  const flag = String(req.body?.flag || "").trim() || null;
  const type = String(req.body?.type || "").trim() || "National";
  const timezone = String(req.body?.timezone || "").trim() || null;
  const allowedTypes = new Set(["National", "Club"]);

  if (!id) {
    return res.status(400).json({ ok: false, message: "id is required" });
  }
  if (!name) {
    return res.status(400).json({ ok: false, message: "name is required" });
  }
  if (!allowedTypes.has(type)) {
    return res.status(400).json({ ok: false, message: "type must be National or Club" });
  }

  return db.get(
    `
      SELECT id
      FROM teams
      WHERE upper(trim(COALESCE(id, ''))) = upper(?)
         OR lower(trim(COALESCE(name, ''))) = lower(?)
      LIMIT 1
    `,
    [id, name],
    (dupErr, dupRow) => {
      if (dupErr) {
        return res.status(500).json({ ok: false, message: "Failed to validate team uniqueness" });
      }
      if (dupRow) {
        return res.status(409).json({ ok: false, message: "Team with this id or name already exists" });
      }

      return db.run(
        `
          INSERT INTO teams (id, name, logo, flag, type, timezone, created_at, updated_at)
          VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        `,
        [id, name, flag, flag, type, timezone],
        (insertErr) => {
          if (insertErr) {
            if (String(insertErr.message || "").includes("UNIQUE")) {
              return res.status(409).json({ ok: false, message: "Team with this id or name already exists" });
            }
            return res.status(500).json({ ok: false, message: "Failed to create team" });
          }

          return db.get(
            `
              SELECT
                id,
                name,
                COALESCE(NULLIF(trim(flag), ''), NULLIF(trim(logo), '')) AS logo,
                COALESCE(NULLIF(trim(flag), ''), NULLIF(trim(logo), '')) AS flag,
                type,
                timezone
              FROM teams
              WHERE upper(trim(id)) = upper(?)
              LIMIT 1
            `,
            [id],
            (selectErr, row) => {
              if (selectErr) {
                return res.status(500).json({ ok: false, message: "Failed to load team" });
              }
              return res.json({ ok: true, team: row || null });
            }
          );
        }
      );
    }
  );
});

app.patch("/teams/:id", requireAdmin, (req, res) => {
  const teamId = normalizeEntityId(req.params.id);
  const payloadId = normalizeEntityId(req.body?.id);
  const name = String(req.body?.name || "").trim();
  const flag = String(req.body?.flag || "").trim() || null;
  const type = String(req.body?.type || "").trim() || "National";
  const timezone = String(req.body?.timezone || "").trim() || null;
  const allowedTypes = new Set(["National", "Club"]);

  if (!teamId) {
    return res.status(400).json({ ok: false, message: "Invalid team id" });
  }
  if (payloadId && payloadId !== teamId) {
    return res.status(400).json({ ok: false, message: "Team id cannot be changed" });
  }
  if (!name) {
    return res.status(400).json({ ok: false, message: "name is required" });
  }
  if (!allowedTypes.has(type)) {
    return res.status(400).json({ ok: false, message: "type must be National or Club" });
  }

  return db.get(
    "SELECT id FROM teams WHERE upper(trim(id)) = upper(?) LIMIT 1",
    [teamId],
    (rowErr, currentRow) => {
      if (rowErr) {
        return res.status(500).json({ ok: false, message: "Failed to load team" });
      }
      if (!currentRow) {
        return res.status(404).json({ ok: false, message: "Team not found" });
      }

      return db.get(
        `
          SELECT id
          FROM teams
          WHERE (
            upper(trim(COALESCE(id, ''))) = upper(?)
            OR lower(trim(COALESCE(name, ''))) = lower(?)
          )
            AND upper(trim(id)) <> upper(?)
          LIMIT 1
        `,
        [teamId, name, teamId],
        (dupErr, dupRow) => {
          if (dupErr) {
            return res.status(500).json({ ok: false, message: "Failed to validate team uniqueness" });
          }
          if (dupRow) {
            return res.status(409).json({ ok: false, message: "Team with this id or name already exists" });
          }

          return db.run(
            `
              UPDATE teams
              SET
                id = ?,
                name = ?,
                logo = ?,
                flag = ?,
                type = ?,
                timezone = ?,
                updated_at = CURRENT_TIMESTAMP
              WHERE upper(trim(id)) = upper(?)
            `,
            [teamId, name, flag, flag, type, timezone, teamId],
            function onUpdate(err) {
              if (err) {
                if (String(err.message || "").includes("UNIQUE")) {
                  return res.status(409).json({ ok: false, message: "Team with this id or name already exists" });
                }
                return res.status(500).json({ ok: false, message: "Failed to update team" });
              }
              if (!this || this.changes === 0) {
                return res.status(404).json({ ok: false, message: "Team not found" });
              }

              return db.get(
                `
                  SELECT
                    id,
                    name,
                    COALESCE(NULLIF(trim(flag), ''), NULLIF(trim(logo), '')) AS logo,
                    COALESCE(NULLIF(trim(flag), ''), NULLIF(trim(logo), '')) AS flag,
                    type,
                    timezone
                  FROM teams
                  WHERE upper(trim(id)) = upper(?)
                  LIMIT 1
                `,
                [teamId],
                (selectErr, row) => {
                  if (selectErr) {
                    return res.status(500).json({ ok: false, message: "Failed to load team" });
                  }
                  return res.json({ ok: true, team: row || null });
                }
              );
            }
          );
        }
      );
    }
  );
});

app.get("/games", requireAdmin, (_req, res, next) => {
  db.all(
    `
      SELECT
        id,
        duel_id,
        bga_table_id,
        game_number,
        player_1_score,
        player_2_score,
        player_1_rank,
        player_2_rank,
        player_1_clock,
        player_2_clock,
        status
      FROM games
      ORDER BY duel_id COLLATE NOCASE ASC, game_number ASC, id ASC
    `,
    (err, rows) => {
      if (err) return next(err);
      return res.json({ ok: true, games: rows || [] });
    }
  );
});

app.post("/games", requireAdmin, (req, res) => {
  const id = normalizeNullableText(req.body?.id);
  const duelId = normalizeNullableText(req.body?.duel_id);
  const bgaTableId = normalizeNullableText(req.body?.bga_table_id);
  const gameNumber = normalizePositiveInteger(req.body?.game_number);
  const player1Score = normalizeIntegerOrNull(req.body?.player_1_score);
  const player2Score = normalizeIntegerOrNull(req.body?.player_2_score);
  const player1Rank = normalizePositiveInteger(req.body?.player_1_rank);
  const player2Rank = normalizePositiveInteger(req.body?.player_2_rank);
  const player1Clock = normalizeIntegerOrNull(req.body?.player_1_clock);
  const player2Clock = normalizeIntegerOrNull(req.body?.player_2_clock);
  const status = normalizeNullableText(req.body?.status);

  if (!id) {
    return res.status(400).json({ ok: false, message: "id is required" });
  }
  if (!duelId) {
    return res.status(400).json({ ok: false, message: "duel_id is required" });
  }
  if (!gameNumber) {
    return res.status(400).json({ ok: false, message: "game_number must be a positive integer" });
  }
  if (hasNonEmptyValue(req.body?.player_1_score) && player1Score === null) {
    return res.status(400).json({ ok: false, message: "player_1_score must be an integer" });
  }
  if (hasNonEmptyValue(req.body?.player_2_score) && player2Score === null) {
    return res.status(400).json({ ok: false, message: "player_2_score must be an integer" });
  }
  if (hasNonEmptyValue(req.body?.player_1_rank) && !player1Rank) {
    return res.status(400).json({ ok: false, message: "player_1_rank must be a positive integer" });
  }
  if (hasNonEmptyValue(req.body?.player_2_rank) && !player2Rank) {
    return res.status(400).json({ ok: false, message: "player_2_rank must be a positive integer" });
  }
  if (hasNonEmptyValue(req.body?.player_1_clock) && player1Clock === null) {
    return res.status(400).json({ ok: false, message: "player_1_clock must be an integer" });
  }
  if (hasNonEmptyValue(req.body?.player_2_clock) && player2Clock === null) {
    return res.status(400).json({ ok: false, message: "player_2_clock must be an integer" });
  }
  if (player1Clock !== null && player1Clock !== 0 && player1Clock !== 1) {
    return res.status(400).json({ ok: false, message: "player_1_clock must be 0 or 1" });
  }
  if (player2Clock !== null && player2Clock !== 0 && player2Clock !== 1) {
    return res.status(400).json({ ok: false, message: "player_2_clock must be 0 or 1" });
  }

  return db.get(
    "SELECT id FROM duels WHERE upper(trim(id)) = upper(?) AND deleted_at IS NULL LIMIT 1",
    [duelId],
    (duelErr, duelRow) => {
      if (duelErr) {
        return res.status(500).json({ ok: false, message: "Failed to validate duel" });
      }
      if (!duelRow) {
        return res.status(400).json({ ok: false, message: "duel_id not found" });
      }

      return db.get(
        `
          SELECT id
          FROM games
          WHERE upper(trim(id)) = upper(?)
             OR (
               ? IS NOT NULL
               AND trim(COALESCE(bga_table_id, '')) <> ''
               AND trim(COALESCE(bga_table_id, '')) = trim(?)
             )
          LIMIT 1
        `,
        [id, bgaTableId, bgaTableId],
        (dupErr, dupRow) => {
          if (dupErr) {
            return res.status(500).json({ ok: false, message: "Failed to validate game uniqueness" });
          }
          if (dupRow) {
            return res.status(409).json({ ok: false, message: "Game with this id or BGA table already exists" });
          }

          return db.run(
            `
              INSERT INTO games (
                id,
                duel_id,
                bga_table_id,
                game_number,
                player_1_score,
                player_2_score,
                player_1_rank,
                player_2_rank,
                player_1_clock,
                player_2_clock,
                status
              )
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `,
            [
              id,
              duelId,
              bgaTableId,
              gameNumber,
              player1Score,
              player2Score,
              player1Rank,
              player2Rank,
              player1Clock ?? 0,
              player2Clock ?? 0,
              status,
            ],
            (insertErr) => {
              if (insertErr) {
                if (String(insertErr.message || "").includes("UNIQUE")) {
                  return res.status(409).json({ ok: false, message: "Game with this id or BGA table already exists" });
                }
                return res.status(500).json({ ok: false, message: "Failed to create game" });
              }

              return db.get(
                `
                  SELECT
                    id,
                    duel_id,
                    bga_table_id,
                    game_number,
                    player_1_score,
                    player_2_score,
                    player_1_rank,
                    player_2_rank,
                    player_1_clock,
                    player_2_clock,
                    status
                  FROM games
                  WHERE upper(trim(id)) = upper(?)
                  LIMIT 1
                `,
                [id],
                (selectErr, row) => {
                  if (selectErr) {
                    return res.status(500).json({ ok: false, message: "Failed to load game" });
                  }
                  return res.json({ ok: true, game: row || null });
                }
              );
            }
          );
        }
      );
    }
  );
});

app.patch("/games/:id", requireAdmin, (req, res) => {
  const gameId = normalizeNullableText(req.params.id);
  const payloadId = normalizeNullableText(req.body?.id);
  const duelId = normalizeNullableText(req.body?.duel_id);
  const bgaTableId = normalizeNullableText(req.body?.bga_table_id);
  const gameNumber = normalizePositiveInteger(req.body?.game_number);
  const player1Score = normalizeIntegerOrNull(req.body?.player_1_score);
  const player2Score = normalizeIntegerOrNull(req.body?.player_2_score);
  const player1Rank = normalizePositiveInteger(req.body?.player_1_rank);
  const player2Rank = normalizePositiveInteger(req.body?.player_2_rank);
  const player1Clock = normalizeIntegerOrNull(req.body?.player_1_clock);
  const player2Clock = normalizeIntegerOrNull(req.body?.player_2_clock);
  const status = normalizeNullableText(req.body?.status);

  if (!gameId) {
    return res.status(400).json({ ok: false, message: "Invalid game id" });
  }
  if (payloadId && payloadId.toLowerCase() !== gameId.toLowerCase()) {
    return res.status(400).json({ ok: false, message: "id cannot be changed" });
  }
  if (!duelId) {
    return res.status(400).json({ ok: false, message: "duel_id is required" });
  }
  if (!gameNumber) {
    return res.status(400).json({ ok: false, message: "game_number must be a positive integer" });
  }
  if (hasNonEmptyValue(req.body?.player_1_score) && player1Score === null) {
    return res.status(400).json({ ok: false, message: "player_1_score must be an integer" });
  }
  if (hasNonEmptyValue(req.body?.player_2_score) && player2Score === null) {
    return res.status(400).json({ ok: false, message: "player_2_score must be an integer" });
  }
  if (hasNonEmptyValue(req.body?.player_1_rank) && !player1Rank) {
    return res.status(400).json({ ok: false, message: "player_1_rank must be a positive integer" });
  }
  if (hasNonEmptyValue(req.body?.player_2_rank) && !player2Rank) {
    return res.status(400).json({ ok: false, message: "player_2_rank must be a positive integer" });
  }
  if (hasNonEmptyValue(req.body?.player_1_clock) && player1Clock === null) {
    return res.status(400).json({ ok: false, message: "player_1_clock must be an integer" });
  }
  if (hasNonEmptyValue(req.body?.player_2_clock) && player2Clock === null) {
    return res.status(400).json({ ok: false, message: "player_2_clock must be an integer" });
  }
  if (player1Clock !== null && player1Clock !== 0 && player1Clock !== 1) {
    return res.status(400).json({ ok: false, message: "player_1_clock must be 0 or 1" });
  }
  if (player2Clock !== null && player2Clock !== 0 && player2Clock !== 1) {
    return res.status(400).json({ ok: false, message: "player_2_clock must be 0 or 1" });
  }

  return db.get(
    "SELECT id FROM duels WHERE upper(trim(id)) = upper(?) AND deleted_at IS NULL LIMIT 1",
    [duelId],
    (duelErr, duelRow) => {
      if (duelErr) {
        return res.status(500).json({ ok: false, message: "Failed to validate duel" });
      }
      if (!duelRow) {
        return res.status(400).json({ ok: false, message: "duel_id not found" });
      }

      return db.get(
        "SELECT id FROM games WHERE upper(trim(id)) = upper(?) LIMIT 1",
        [gameId],
        (rowErr, currentRow) => {
          if (rowErr) {
            return res.status(500).json({ ok: false, message: "Failed to load game" });
          }
          if (!currentRow) {
            return res.status(404).json({ ok: false, message: "Game not found" });
          }

          return db.get(
            `
              SELECT id
              FROM games
              WHERE ? IS NOT NULL
                AND trim(COALESCE(bga_table_id, '')) <> ''
                AND trim(COALESCE(bga_table_id, '')) = trim(?)
                AND upper(trim(id)) <> upper(?)
              LIMIT 1
            `,
            [bgaTableId, bgaTableId, gameId],
            (dupErr, dupRow) => {
              if (dupErr) {
                return res.status(500).json({ ok: false, message: "Failed to validate game uniqueness" });
              }
              if (dupRow) {
                return res.status(409).json({ ok: false, message: "Game with this BGA table already exists" });
              }

              return db.run(
                `
                  UPDATE games
                  SET
                    duel_id = ?,
                    bga_table_id = ?,
                    game_number = ?,
                    player_1_score = ?,
                    player_2_score = ?,
                    player_1_rank = ?,
                    player_2_rank = ?,
                    player_1_clock = ?,
                    player_2_clock = ?,
                    status = ?
                  WHERE upper(trim(id)) = upper(?)
                `,
                [
                  duelId,
                  bgaTableId,
                  gameNumber,
                  player1Score,
                  player2Score,
                  player1Rank,
                  player2Rank,
                  player1Clock ?? 0,
                  player2Clock ?? 0,
                  status,
                  gameId,
                ],
                function onUpdate(err) {
                  if (err) {
                    if (String(err.message || "").includes("UNIQUE")) {
                      return res.status(409).json({ ok: false, message: "Game with this BGA table already exists" });
                    }
                    return res.status(500).json({ ok: false, message: "Failed to update game" });
                  }
                  if (!this || this.changes === 0) {
                    return res.status(404).json({ ok: false, message: "Game not found" });
                  }

                  return db.get(
                    `
                      SELECT
                        id,
                        duel_id,
                        bga_table_id,
                        game_number,
                        player_1_score,
                        player_2_score,
                        player_1_rank,
                        player_2_rank,
                        player_1_clock,
                        player_2_clock,
                        status
                      FROM games
                      WHERE upper(trim(id)) = upper(?)
                      LIMIT 1
                    `,
                    [gameId],
                    (selectErr, row) => {
                      if (selectErr) {
                        return res.status(500).json({ ok: false, message: "Failed to load game" });
                      }
                      return res.json({ ok: true, game: row || null });
                    }
                  );
                }
              );
            }
          );
        }
      );
    }
  );
});

app.get("/users", requireAdmin, (req, res, next) => {
  const allowedPageSizes = new Set([10, 20, 50]);
  const parsedLimit = Number.parseInt(String(req.query.limit || "10"), 10);
  const parsedPage = Number.parseInt(String(req.query.page || "1"), 10);
  const limit = allowedPageSizes.has(parsedLimit) ? parsedLimit : 10;
  const page = Number.isInteger(parsedPage) && parsedPage > 0 ? parsedPage : 1;
  const offset = (page - 1) * limit;

  return db.get(
    "SELECT COUNT(*) AS total FROM users",
    (countErr, countRow) => {
      if (countErr) return next(countErr);
      const total = Number(countRow?.total || 0);
      const totalPages = Math.max(1, Math.ceil(total / limit));
      const safePage = Math.min(page, totalPages);
      const safeOffset = (safePage - 1) * limit;

      return db.all(
        `
          SELECT
            u.id,
            u.google_id,
            u.email,
            u.name,
            u.picture,
            u.bga_id,
            p.bga_nickname,
            u.created_at,
            u.last_login
          FROM users u
          LEFT JOIN profiles p
            ON p.id = u.bga_id
           AND p.deleted_at IS NULL
          ORDER BY datetime(COALESCE(u.last_login, u.updated_at, u.created_at)) DESC, u.id ASC
          LIMIT ?
          OFFSET ?
        `,
        [limit, total > 0 ? safeOffset : offset],
        (err, rows) => {
          if (err) return next(err);
          return res.json({
            ok: true,
            users: rows || [],
            pagination: {
              page: safePage,
              page_size: limit,
              total,
              total_pages: totalPages,
            },
          });
        }
      );
    }
  );
});

app.get("/users/options", requireAdmin, (req, res, next) => {
  db.all(
    `
      SELECT
        u.id,
        u.email,
        u.name,
        p.bga_nickname
      FROM users u
      LEFT JOIN profiles p
        ON p.id = u.bga_id
       AND p.deleted_at IS NULL
      ORDER BY
        lower(COALESCE(NULLIF(trim(u.name), ''), NULLIF(trim(p.bga_nickname), ''), NULLIF(trim(u.email), ''), CAST(u.id AS TEXT))) ASC,
        u.id ASC
    `,
    (err, rows) => {
      if (err) return next(err);
      return res.json({ ok: true, users: rows || [] });
    }
  );
});

app.get("/audit-trail", requireAdmin, (req, res, next) => {
  const allowedPageSizes = new Set([10, 20, 50]);
  const parsedLimit = Number.parseInt(String(req.query.limit || "10"), 10);
  const parsedPage = Number.parseInt(String(req.query.page || "1"), 10);
  const parsedActorUserId = Number.parseInt(String(req.query.actor_user_id || ""), 10);
  const limit = allowedPageSizes.has(parsedLimit) ? parsedLimit : 10;
  const page = Number.isInteger(parsedPage) && parsedPage > 0 ? parsedPage : 1;
  const offset = (page - 1) * limit;
  const notAdminsOnly = String(req.query.not_admins || "").trim() === "1";
  const actorUserId = Number.isInteger(parsedActorUserId) && parsedActorUserId > 0
    ? parsedActorUserId
    : null;
  const auditFilterClauses = [];
  const auditFilterParams = [];

  if (notAdminsOnly) {
    auditFilterClauses.push(`
      NOT EXISTS (
        SELECT 1
        FROM users actor_users
        WHERE actor_users.id = audit_trail.actor_user_id
          AND COALESCE(actor_users.admin, 0) = 1
      )
    `);
  }

  if (actorUserId != null) {
    auditFilterClauses.push("audit_trail.actor_user_id = ?");
    auditFilterParams.push(actorUserId);
  }

  const auditFilterSql = auditFilterClauses.length
    ? `WHERE ${auditFilterClauses.join(" AND ")}`
    : "";

  return db.get(
    `
      SELECT COUNT(*) AS total
      FROM audit_trail
      ${auditFilterSql}
    `,
    auditFilterParams,
    (countErr, countRow) => {
      if (countErr) return next(countErr);
      const total = Number(countRow?.total || 0);
      const totalPages = Math.max(1, Math.ceil(total / limit));
      const safePage = Math.min(page, totalPages);
      const safeOffset = (safePage - 1) * limit;

      return db.all(
        `
          SELECT
            id,
            event_type,
            entity_type,
            action,
            record_id,
            actor_user_id,
            actor_bga_id,
            actor_bga_nickname,
            actor_email,
            changes,
            metadata,
            created_at
          FROM audit_trail
          ${auditFilterSql}
          ORDER BY datetime(created_at) DESC, id DESC
          LIMIT ?
          OFFSET ?
        `,
        [...auditFilterParams, limit, total > 0 ? safeOffset : offset],
        (err, rows) => {
          if (err) return next(err);
          return res.json({
            ok: true,
            records: (rows || []).map((row) => ({
              ...row,
              changes: parseJsonOrNull(row?.changes),
              metadata: parseJsonOrNull(row?.metadata),
            })),
            pagination: {
              page: safePage,
              page_size: limit,
              total,
              total_pages: totalPages,
            },
          });
        }
      );
    }
  );
});

app.get("/duels", (req, res, next) => {
  const matchId = String(req.query.match_id || "").trim();
  const userAssociation = String(req.user?.association || "").trim().toUpperCase();
  if (!matchId) {
    return res.json({ ok: true, duels: [] });
  }

  return db.get(
    `
      SELECT
        id,
        tournament_id,
        team_1,
        team_2
      FROM matches
      WHERE id = ?
        AND deleted_at IS NULL
      LIMIT 1
    `,
    [matchId],
    (matchErr, matchRow) => {
      if (matchErr) return next(matchErr);
      if (!matchRow) {
        return res.status(404).json({ ok: false, message: "Match not found" });
      }

      return loadTournamentAccessForUser(matchRow.tournament_id, req.user, (tournamentErr, tournament) => {
        if (tournamentErr) return next(tournamentErr);
        if (!tournament) {
          return res.status(404).json({ ok: false, message: "Tournament not found" });
        }
        if (!tournament.has_access) {
          return res.status(403).json({ ok: false, message: "Forbidden" });
        }
        if (
          tournament.access_type === TOURNAMENT_ACCESS_TYPES.CLOSED
          && tournament.access_role === TOURNAMENT_ACCESS_ROLES.CAPTAIN
          && !canClosedTournamentCaptainAccessMatch(tournament, userAssociation, matchRow.team_1, matchRow.team_2)
        ) {
          return res.status(403).json({ ok: false, message: "Forbidden" });
        }

        return loadDuelsByMatchId(matchId, (err, rows) => {
          if (err) return next(err);
          return res.json({ ok: true, duels: rows || [] });
        });
      });
    }
  );
});

app.post("/duels/bulk-upsert", (req, res) => {
  if (!req.user) {
    return res.status(401).json({ ok: false, message: "Unauthorized" });
  }

  const payload = req.body && typeof req.body === "object" ? req.body : {};
  const matchId = String(payload.match_id || "").trim();
  const duels = Array.isArray(payload.duels) ? payload.duels : [];
  const isAdmin = Number(req.user.admin) === 1;
  const isTeamCaptain = Number(req.user.team_captain) === 1;
  const userAssociation = String(req.user.association || "").trim().toUpperCase();
  const actorPlayerId = String(req.user.player_id || "").trim() || null;

  const toIntOrNull = (v) => {
    if (v === null || v === undefined || String(v).trim() === "") return null;
    const n = Number(v);
    if (!Number.isFinite(n)) return null;
    return Math.trunc(n);
  };
  const normalizeText = (v) => {
    const s = String(v ?? "").trim();
    return s === "" ? null : s;
  };

  if (!matchId) {
    if (!isAdmin) {
      return res.status(403).json({ ok: false, message: "Forbidden" });
    }

    const sanitized = [];
    for (let index = 0; index < duels.length; index += 1) {
      const item = duels[index];
      const id = String(item?.id || "").trim();
      const player1 = normalizeText(item?.player_1_id);
      const player2 = normalizeText(item?.player_2_id);
      const duelNumberRaw = toIntOrNull(item?.duel_number);
      const duelNumber = Number.isInteger(duelNumberRaw) && duelNumberRaw > 0
        ? duelNumberRaw
        : index + 1;
      if (!id || (!player1 && !player2)) {
        return res.status(400).json({ ok: false, message: "Each duel requires id and at least one player" });
      }
      sanitized.push({
        id,
        tournament_id: normalizeText(item?.tournament_id),
        match_id: null,
        duel_number: duelNumber,
        duel_format: normalizeText(item?.duel_format),
        time_utc: normalizeText(item?.time_utc),
        custom_time: toIntOrNull(item?.custom_time),
        player_1_id: player1,
        player_2_id: player2,
        dw1: toIntOrNull(item?.dw1),
        dw2: toIntOrNull(item?.dw2),
        status: normalizeText(item?.status),
      });
    }

    if (!sanitized.length) {
      return res.json({ ok: true, duels: [] });
    }

    return db.serialize(() => {
      db.run("BEGIN IMMEDIATE TRANSACTION");
      const stmt = db.prepare(`
        INSERT INTO duels (
          id,
          tournament_id,
          match_id,
          duel_number,
          duel_format,
          time_utc,
          custom_time,
          player_1_id,
          player_2_id,
          dw1,
          dw2,
          status,
          created_by,
          updated_by,
          deleted_by,
          deleted_at,
          created_at,
          updated_at
        )
        VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(id) DO UPDATE SET
          tournament_id = excluded.tournament_id,
          match_id = NULL,
          duel_number = excluded.duel_number,
          duel_format = excluded.duel_format,
          time_utc = excluded.time_utc,
          custom_time = excluded.custom_time,
          player_1_id = excluded.player_1_id,
          player_2_id = excluded.player_2_id,
          dw1 = excluded.dw1,
          dw2 = excluded.dw2,
          status = excluded.status,
          updated_by = excluded.updated_by,
          deleted_by = NULL,
          deleted_at = NULL,
          updated_at = CURRENT_TIMESTAMP
      `);

      let failed = false;
      let pending = sanitized.length;
      sanitized.forEach((item) => {
        stmt.run(
          [
            item.id,
            item.tournament_id,
            item.duel_number,
            item.duel_format,
            item.time_utc,
            item.custom_time,
            item.player_1_id,
            item.player_2_id,
            item.dw1,
            item.dw2,
            item.status,
            actorPlayerId,
            actorPlayerId,
          ],
          (insertErr) => {
            if (failed) return;
            if (insertErr) {
              failed = true;
              stmt.finalize(() => {
                db.run("ROLLBACK");
                return res.status(500).json({ ok: false, message: "Failed to save duels" });
              });
              return;
            }
            pending -= 1;
            if (pending === 0) {
              stmt.finalize(() => {
                db.run("COMMIT", (commitErr) => {
                  if (commitErr) {
                    return res.status(500).json({ ok: false, message: "Failed to save duels" });
                  }
                  return loadDuelsByIds(sanitized.map((item) => item.id), (loadErr, rows) => {
                    if (loadErr) {
                      return res.status(500).json({ ok: false, message: "Failed to load saved duels" });
                    }
                    return res.json({ ok: true, duels: rows || [] });
                  });
                });
              });
            }
          }
        );
      });
    });
  }

  return db.get(
    "SELECT tournament_id, team_1, team_2 FROM matches WHERE id = ? AND deleted_at IS NULL LIMIT 1",
    [matchId],
    (matchErr, matchRow) => {
      if (matchErr) {
        return res.status(500).json({ ok: false, message: "Failed to validate match" });
      }
      if (!matchRow) {
        return res.status(404).json({ ok: false, message: "Match not found" });
      }
      const team1 = String(matchRow.team_1 || "").trim().toUpperCase();
      const team2 = String(matchRow.team_2 || "").trim().toUpperCase();

      return loadTournamentAccessForUser(matchRow.tournament_id, req.user, (tournamentErr, tournament) => {
        if (tournamentErr) {
          return res.status(500).json({ ok: false, message: "Failed to validate tournament access" });
        }
        if (!tournament) {
          return res.status(404).json({ ok: false, message: "Tournament not found" });
        }

        const isClosedTournament = tournament.access_type === TOURNAMENT_ACCESS_TYPES.CLOSED;
        const closedCaptainCanAccessMatch = canClosedTournamentCaptainAccessMatch(
          tournament,
          userAssociation,
          team1,
          team2
        );
        const canEdit = isClosedTournament
          ? (
              tournament.has_access
              && (
                tournament.access_role === TOURNAMENT_ACCESS_ROLES.ADMIN
                || closedCaptainCanAccessMatch
              )
            )
          : (isAdmin || (isTeamCaptain && userAssociation && (userAssociation === team1 || userAssociation === team2)));
        if (!canEdit) {
          return res.status(403).json({ ok: false, message: "Forbidden" });
        }

        const sanitized = [];
        for (let index = 0; index < duels.length; index += 1) {
          const item = duels[index];
          const id = String(item?.id || "").trim();
          const player1 = normalizeText(item?.player_1_id);
          const player2 = normalizeText(item?.player_2_id);
          const duelNumberRaw = toIntOrNull(item?.duel_number);
          const duelNumber = Number.isInteger(duelNumberRaw) && duelNumberRaw > 0
            ? duelNumberRaw
            : index + 1;
          if (!id || (!player1 && !player2)) {
            return res.status(400).json({ ok: false, message: "Each duel requires id and at least one player" });
          }
          sanitized.push({
            id,
            tournament_id: normalizeText(item?.tournament_id) || tournament.id,
            match_id: matchId,
            duel_number: duelNumber,
            duel_format: normalizeText(item?.duel_format),
            time_utc: normalizeText(item?.time_utc),
            custom_time: toIntOrNull(item?.custom_time),
            player_1_id: player1,
            player_2_id: player2,
            dw1: toIntOrNull(item?.dw1),
            dw2: toIntOrNull(item?.dw2),
            status: normalizeText(item?.status),
          });
        }

        return loadDuelsByMatchId(matchId, (beforeErr, previousLineups) => {
          if (beforeErr) {
            return res.status(500).json({ ok: false, message: "Failed to load existing duels" });
          }

          return db.serialize(() => {
            db.run("BEGIN IMMEDIATE TRANSACTION");
            db.run(
              `
                UPDATE duels
                SET
                  deleted_at = CURRENT_TIMESTAMP,
                  deleted_by = ?,
                  updated_by = ?,
                  updated_at = CURRENT_TIMESTAMP
                WHERE match_id = ?
                  AND deleted_at IS NULL
              `,
              [actorPlayerId, actorPlayerId, matchId],
              (deleteErr) => {
                if (deleteErr) {
                  db.run("ROLLBACK");
                  return res.status(500).json({ ok: false, message: "Failed to clear old duels" });
                }
                if (!sanitized.length) {
                  return recomputeMatchAggregates(matchId, actorPlayerId)
                    .then(() => dbRunAsync("COMMIT"))
                    .then(() => {
                      const action = previousLineups.length ? "delete" : "update";
                      const eventType = previousLineups.length ? "lineups.deleted_all" : "lineups.updated";
                      const changes = previousLineups.length
                        ? buildAuditDeletionChanges({ lineups: previousLineups || [] }, ["lineups"])
                        : buildAuditChanges({ lineups: previousLineups || [] }, { lineups: [] }, ["lineups"]);

                      return logAuditEvent(
                        {
                          ...getAuditActor(req.user),
                          event_type: eventType,
                          entity_type: "lineups",
                          action,
                          record_id: matchId,
                          changes,
                          metadata: {
                            match_id: matchId,
                            lineups_count: 0,
                            team_1: team1,
                            team_2: team2,
                          },
                        },
                        () => res.json({ ok: true, duels: [] })
                      );
                    })
                    .catch(() => {
                      db.run("ROLLBACK");
                      return res.status(500).json({ ok: false, message: "Failed to save duels" });
                    });
                }

                const stmt = db.prepare(`
                INSERT INTO duels (
                  id,
                  tournament_id,
                  match_id,
                  duel_number,
                  duel_format,
                  time_utc,
                  custom_time,
                  player_1_id,
                  player_2_id,
                  dw1,
                  dw2,
                  status,
                  created_by,
                  updated_by,
                  deleted_by,
                  deleted_at,
                  created_at,
                  updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT(id) DO UPDATE SET
                  tournament_id = excluded.tournament_id,
                  match_id = excluded.match_id,
                  duel_number = excluded.duel_number,
                  duel_format = excluded.duel_format,
                  time_utc = excluded.time_utc,
                  custom_time = excluded.custom_time,
                  player_1_id = excluded.player_1_id,
                  player_2_id = excluded.player_2_id,
                  dw1 = excluded.dw1,
                  dw2 = excluded.dw2,
                  status = excluded.status,
                  updated_by = excluded.updated_by,
                  deleted_by = NULL,
                  deleted_at = NULL,
                  updated_at = CURRENT_TIMESTAMP
              `);

                let failed = false;
                let pending = sanitized.length;
                sanitized.forEach((item) => {
                  stmt.run(
                    [
                      item.id,
                      item.tournament_id,
                      item.match_id,
                      item.duel_number,
                      item.duel_format,
                      item.time_utc,
                      item.custom_time,
                      item.player_1_id,
                      item.player_2_id,
                      item.dw1,
                      item.dw2,
                      item.status,
                      actorPlayerId,
                      actorPlayerId,
                    ],
                    (insertErr) => {
                      if (failed) return;
                      if (insertErr) {
                        failed = true;
                        stmt.finalize(() => {
                          db.run("ROLLBACK");
                          return res.status(500).json({ ok: false, message: "Failed to insert duels" });
                        });
                        return;
                      }
                      pending -= 1;
                      if (pending === 0) {
                        stmt.finalize(() => {
                          recomputeMatchAggregates(matchId, actorPlayerId)
                            .then(() => dbRunAsync("COMMIT"))
                            .then(() => new Promise((resolve, reject) => {
                              loadDuelsByMatchId(matchId, (loadErr, savedDuels) => {
                                if (loadErr) {
                                  reject(loadErr);
                                  return;
                                }
                                resolve(savedDuels || []);
                              });
                            }))
                            .then((savedDuels) => {
                              const action = previousLineups.length ? "update" : "create";
                              const eventType = previousLineups.length ? "lineups.updated" : "lineups.created";
                              const changes = previousLineups.length
                                ? buildLineupsAuditChanges(previousLineups || [], savedDuels || [])
                                : buildAuditCreationChanges({ lineups: savedDuels || [] }, ["lineups"]);

                              return logAuditEvent(
                                {
                                  ...getAuditActor(req.user),
                                  event_type: eventType,
                                  entity_type: "lineups",
                                  action,
                                  record_id: matchId,
                                  changes,
                                  metadata: {
                                    match_id: matchId,
                                    lineups_count: savedDuels.length,
                                    team_1: team1,
                                    team_2: team2,
                                  },
                                },
                                () => res.json({ ok: true, duels: savedDuels || [] })
                              );
                            })
                            .catch(() => {
                              db.run("ROLLBACK");
                              return res.status(500).json({ ok: false, message: "Failed to save duels" });
                            });
                        });
                      }
                    }
                  );
                });
              }
            );
          });
        });
      });
    }
  );
});

app.get("/matches", (req, res, next) => {
  const isAdmin = Number(req.user?.admin) === 1;
  const userId = getTournamentAccessUserId(req.user);
  const userAssociation = String(req.user?.association || "").trim().toUpperCase();
  const visibilitySql = isAdmin
    ? ""
    : `
      AND EXISTS (
        SELECT 1
        FROM tournaments t
        WHERE upper(trim(t.id)) = upper(trim(m.tournament_id))
          AND (
            COALESCE(t.access_type, ?) = ?
            OR (? > 0 AND EXISTS (
              SELECT 1
              FROM tournament_access_users tau
              WHERE upper(trim(tau.tournament_id)) = upper(trim(t.id))
                AND tau.user_id = ?
            ))
          )
      )
    `;
  db.all(
    `
      SELECT
        m.id,
        m.tournament_id,
        m.time_utc,
        m.lineup_type,
        m.lineup_deadline_h,
        m.lineup_deadline_utc,
        m.number_of_duels,
        m.team_1,
        m.team_2,
        m.status,
        m.dw1,
        m.dw2,
        m.gw1,
        m.gw2,
        m.rating,
        (
          SELECT COALESCE(t.access_type, ?)
          FROM tournaments t
          WHERE upper(trim(t.id)) = upper(trim(m.tournament_id))
          LIMIT 1
        ) AS tournament_access_type,
        (
          SELECT COALESCE(NULLIF(lower(trim(tau.role)), ''), ?)
          FROM tournament_access_users tau
          WHERE upper(trim(tau.tournament_id)) = upper(trim(m.tournament_id))
            AND tau.user_id = ?
          LIMIT 1
        ) AS tournament_access_role
      FROM matches m
      WHERE m.deleted_at IS NULL
      ${visibilitySql}
      ORDER BY m.time_utc DESC, m.id ASC
    `,
    isAdmin
      ? [TOURNAMENT_ACCESS_TYPES.OPEN, TOURNAMENT_ACCESS_ROLES.CAPTAIN, userId]
      : [
          TOURNAMENT_ACCESS_TYPES.OPEN,
          TOURNAMENT_ACCESS_ROLES.CAPTAIN,
          userId,
          TOURNAMENT_ACCESS_TYPES.OPEN,
          TOURNAMENT_ACCESS_TYPES.OPEN,
          userId,
          userId,
        ],
    (err, rows) => {
      if (err) return next(err);
      const filteredRows = (rows || []).filter((row) => {
        if (isAdmin) return true;
        const tournamentAccessType = normalizeTournamentAccessType(row?.tournament_access_type);
        if (tournamentAccessType !== TOURNAMENT_ACCESS_TYPES.CLOSED) return true;
        const tournamentAccessRole = row?.tournament_access_role
          ? normalizeTournamentAccessRole(row.tournament_access_role)
          : null;
        if (tournamentAccessRole === TOURNAMENT_ACCESS_ROLES.ADMIN) return true;
        return canClosedTournamentCaptainAccessMatch(
          { access_type: tournamentAccessType, access_role: tournamentAccessRole },
          userAssociation,
          row?.team_1,
          row?.team_2
        );
      }).map((row) => ({
        id: row.id,
        tournament_id: row.tournament_id,
        time_utc: row.time_utc,
        lineup_type: row.lineup_type,
        lineup_deadline_h: row.lineup_deadline_h,
        lineup_deadline_utc: row.lineup_deadline_utc,
        number_of_duels: row.number_of_duels,
        team_1: row.team_1,
        team_2: row.team_2,
        status: row.status,
        dw1: row.dw1,
        dw2: row.dw2,
        gw1: row.gw1,
        gw2: row.gw2,
        rating: row.rating,
      }));
      return res.json({ ok: true, matches: filteredRows });
    }
  );
});

function publicMainPageMatchesHandler(_req, res, next) {
  const recentStartedFilterSql = `
    AND (
      trim(COALESCE(m.time_utc, '')) = ''
      OR datetime(m.time_utc) IS NULL
      OR datetime(m.time_utc) >= datetime('now', '-7 days')
    )
  `;

  return db.all(
    `
      SELECT
        m.id,
        m.tournament_id,
        m.time_utc,
        m.lineup_type,
        m.lineup_deadline_h,
        m.lineup_deadline_utc,
        m.number_of_duels,
        m.team_1,
        m.team_2,
        m.status,
        m.dw1,
        m.dw2,
        m.gw1,
        m.gw2,
        m.rating,
        team1.name AS team_1_name,
        COALESCE(NULLIF(trim(team1.flag), ''), NULLIF(trim(team1.logo), '')) AS team_1_flag,
        team2.name AS team_2_name,
        COALESCE(NULLIF(trim(team2.flag), ''), NULLIF(trim(team2.logo), '')) AS team_2_flag,
        t.name AS tournament_name,
        t.short_title AS tournament_short_title,
        t.logo AS tournament_logo,
        t.link AS tournament_link,
        CASE
          WHEN team1.id IS NOT NULL AND team2.id IS NOT NULL THEN 'TEAM'
          ELSE 'Individual'
        END AS tournament_type
      FROM matches m
      LEFT JOIN tournaments t
        ON upper(trim(COALESCE(t.id, ''))) = upper(trim(COALESCE(m.tournament_id, '')))
      LEFT JOIN teams team1
        ON upper(trim(COALESCE(team1.id, ''))) = upper(trim(COALESCE(m.team_1, '')))
      LEFT JOIN teams team2
        ON upper(trim(COALESCE(team2.id, ''))) = upper(trim(COALESCE(m.team_2, '')))
      WHERE m.deleted_at IS NULL
      ${recentStartedFilterSql}
      ORDER BY datetime(COALESCE(m.time_utc, '1970-01-01 00:00:00')) DESC, m.id ASC
    `,
    [],
    (matchesErr, matchRows) => {
      if (matchesErr) return next(matchesErr);

      const normalizedMatchIds = Array.from(new Set(
        (matchRows || [])
          .map((row) => String(row?.id || "").trim())
          .filter(Boolean)
      ));

      const finalize = (duelRows, gameRows = []) => {
        const gamesByDuelId = new Map();
        (gameRows || []).forEach((row) => {
          const duelId = String(row?.duel_id || "").trim();
          if (!duelId) return;
          if (!gamesByDuelId.has(duelId)) {
            gamesByDuelId.set(duelId, []);
          }
          gamesByDuelId.get(duelId).push({
            id: row.id,
            duel_id: row.duel_id,
            bga_table_id: row.bga_table_id,
            game_number: row.game_number,
            player_1_score: row.player_1_score,
            player_2_score: row.player_2_score,
            player_1_rank: row.player_1_rank,
            player_2_rank: row.player_2_rank,
            player_1_clock: row.player_1_clock,
            player_2_clock: row.player_2_clock,
            status: row.status,
          });
        });

        const tournaments = Array.from(new Map(
          (matchRows || [])
            .map((row) => {
              const tournamentId = String(row?.tournament_id || "").trim();
              if (!tournamentId) return null;
              return [tournamentId, {
                id: tournamentId,
                name: row.tournament_name,
                short_title: row.tournament_short_title,
                logo: row.tournament_logo,
                link: row.tournament_link,
                type: row.tournament_type,
              }];
            })
            .filter(Boolean)
        ).values());

        return db.get(
          `
            SELECT last_success_at
            FROM job_runs
            WHERE job_name = 'update-player-elo-daily'
            LIMIT 1
          `,
          [],
          (jobRunsErr, jobRunRow) => {
            if (jobRunsErr) return next(jobRunsErr);
            return res.json({
              ok: true,
              elo_updated_at: jobRunRow?.last_success_at || null,
              tournaments,
              matches: (matchRows || []).map((row) => ({
                id: row.id,
                tournament_id: row.tournament_id,
                time_utc: row.time_utc,
                lineup_type: row.lineup_type,
                lineup_deadline_h: row.lineup_deadline_h,
                lineup_deadline_utc: row.lineup_deadline_utc,
                number_of_duels: row.number_of_duels,
                team_1: row.team_1,
                team_2: row.team_2,
                team_1_name: row.team_1_name,
                team_1_flag: row.team_1_flag,
                team_2_name: row.team_2_name,
                team_2_flag: row.team_2_flag,
                status: row.status,
                dw1: row.dw1,
                dw2: row.dw2,
                gw1: row.gw1,
                gw2: row.gw2,
                rating: row.rating,
                tournament_name: row.tournament_name,
                tournament_short_title: row.tournament_short_title,
                tournament_logo: row.tournament_logo,
                tournament_link: row.tournament_link,
                tournament_type: row.tournament_type,
              })),
              duels: (duelRows || []).map((row) => ({
                id: row.id,
                tournament_id: row.tournament_id,
                match_id: row.match_id,
                duel_number: row.duel_number,
                duel_format: row.duel_format,
                time_utc: row.time_utc,
                player_1_id: row.player_1_id,
                player_1_name: row.player_1_name,
                player_1_elo: row.player_1_elo,
                player_2_id: row.player_2_id,
                player_2_name: row.player_2_name,
                player_2_elo: row.player_2_elo,
                dw1: row.dw1,
                dw2: row.dw2,
                rating: row.rating,
                status: row.status,
                games: gamesByDuelId.get(String(row.id || "").trim()) || [],
              })),
            });
          }
        );
      };

      if (!normalizedMatchIds.length) {
        return finalize([]);
      }

      const placeholders = normalizedMatchIds.map(() => "?").join(", ");
      return db.all(
        `
          SELECT
            d.id,
            d.tournament_id,
            d.match_id,
            d.duel_number,
            d.duel_format,
            d.time_utc,
            d.player_1_id,
            COALESCE(NULLIF(trim(p1.bga_nickname), ''), trim(d.player_1_id)) AS player_1_name,
            p1.bga_elo AS player_1_elo,
            d.player_2_id,
            COALESCE(NULLIF(trim(p2.bga_nickname), ''), trim(d.player_2_id)) AS player_2_name,
            p2.bga_elo AS player_2_elo,
            d.dw1,
            d.dw2,
            d.rating,
            d.status
          FROM duels d
          LEFT JOIN profiles p1
            ON trim(COALESCE(p1.id, '')) = trim(COALESCE(d.player_1_id, ''))
          LEFT JOIN profiles p2
            ON trim(COALESCE(p2.id, '')) = trim(COALESCE(d.player_2_id, ''))
          WHERE d.deleted_at IS NULL
            AND trim(COALESCE(d.match_id, '')) IN (${placeholders})
            AND (
              trim(COALESCE(d.time_utc, '')) = ''
              OR datetime(d.time_utc) IS NULL
              OR datetime(d.time_utc) >= datetime('now', '-7 days')
            )
          ORDER BY
            CASE WHEN d.duel_number IS NULL THEN 1 ELSE 0 END ASC,
            d.duel_number ASC,
            datetime(COALESCE(d.time_utc, '1970-01-01 00:00:00')) ASC,
            d.id ASC
        `,
        normalizedMatchIds,
        (duelsErr, duelRows) => {
          if (duelsErr) return next(duelsErr);
          const normalizedDuelIds = Array.from(new Set(
            (duelRows || [])
              .map((row) => String(row?.id || "").trim())
              .filter(Boolean)
          ));
          if (!normalizedDuelIds.length) {
            return finalize(duelRows || [], []);
          }
          return loadGamesByDuelIds(normalizedDuelIds, (gamesErr, gameRows) => {
            if (gamesErr) return next(gamesErr);
            return finalize(duelRows || [], gameRows || []);
          });
        }
      );
    }
  );
}

app.get("/public/main-page-matches", publicMainPageMatchesHandler);

app.get("/public/friendly-matches", (_req, res, next) => {
  const tournamentId = "Friendly-Matches";

  return db.get(
    `
      SELECT
        t.id,
        t.name,
        t.short_title,
        t.logo,
        t.link
      FROM tournaments t
      WHERE upper(trim(t.id)) = upper(trim(?))
      LIMIT 1
    `,
    [tournamentId],
    (tournamentErr, tournamentRow) => {
      if (tournamentErr) return next(tournamentErr);
      if (!tournamentRow) {
        return res.json({ ok: true, tournament: null, matches: [], duels: [] });
      }

      return db.all(
        `
          SELECT
            m.id,
            m.tournament_id,
            m.time_utc,
            m.lineup_type,
            m.lineup_deadline_h,
            m.lineup_deadline_utc,
            m.number_of_duels,
            m.team_1,
            m.team_2,
            m.status,
            m.dw1,
            m.dw2,
            m.gw1,
            m.gw2,
            m.rating,
            team1.name AS team_1_name,
            COALESCE(NULLIF(trim(team1.flag), ''), NULLIF(trim(team1.logo), '')) AS team_1_flag,
            team2.name AS team_2_name,
            COALESCE(NULLIF(trim(team2.flag), ''), NULLIF(trim(team2.logo), '')) AS team_2_flag
          FROM matches m
          LEFT JOIN teams team1
            ON upper(trim(COALESCE(team1.id, ''))) = upper(trim(COALESCE(m.team_1, '')))
          LEFT JOIN teams team2
            ON upper(trim(COALESCE(team2.id, ''))) = upper(trim(COALESCE(m.team_2, '')))
          WHERE m.deleted_at IS NULL
            AND upper(trim(COALESCE(m.tournament_id, ''))) = upper(trim(?))
          ORDER BY datetime(COALESCE(m.time_utc, '1970-01-01 00:00:00')) DESC, m.id ASC
        `,
        [tournamentId],
        (matchesErr, matchRows) => {
          if (matchesErr) return next(matchesErr);

          const normalizedMatchIds = Array.from(new Set(
            (matchRows || [])
              .map((row) => String(row?.id || "").trim())
              .filter(Boolean)
          ));

          const finalize = (duelRows, gameRows = []) => {
            const gamesByDuelId = new Map();
            (gameRows || []).forEach((row) => {
              const duelId = String(row?.duel_id || "").trim();
              if (!duelId) return;
              if (!gamesByDuelId.has(duelId)) {
                gamesByDuelId.set(duelId, []);
              }
              gamesByDuelId.get(duelId).push({
                id: row.id,
                duel_id: row.duel_id,
                bga_table_id: row.bga_table_id,
                game_number: row.game_number,
                player_1_score: row.player_1_score,
                player_2_score: row.player_2_score,
                player_1_rank: row.player_1_rank,
                player_2_rank: row.player_2_rank,
                player_1_clock: row.player_1_clock,
                player_2_clock: row.player_2_clock,
                status: row.status,
              });
            });

            return res.json({
              ok: true,
              tournament: {
                id: tournamentRow.id,
                name: tournamentRow.name,
                short_title: tournamentRow.short_title,
                logo: tournamentRow.logo,
                link: tournamentRow.link,
                type: "TEAM",
              },
              matches: (matchRows || []).map((row) => ({
                id: row.id,
                tournament_id: row.tournament_id,
                time_utc: row.time_utc,
                lineup_type: row.lineup_type,
                lineup_deadline_h: row.lineup_deadline_h,
                lineup_deadline_utc: row.lineup_deadline_utc,
                number_of_duels: row.number_of_duels,
                team_1: row.team_1,
                team_2: row.team_2,
                team_1_name: row.team_1_name,
                team_1_flag: row.team_1_flag,
                team_2_name: row.team_2_name,
                team_2_flag: row.team_2_flag,
                status: row.status,
                dw1: row.dw1,
                dw2: row.dw2,
                gw1: row.gw1,
                gw2: row.gw2,
                rating: row.rating,
              })),
              duels: (duelRows || []).map((row) => ({
                id: row.id,
                match_id: row.match_id,
                duel_number: row.duel_number,
                duel_format: row.duel_format,
                time_utc: row.time_utc,
                player_1_id: row.player_1_id,
                player_1_name: row.player_1_name,
                player_1_elo: row.player_1_elo,
                player_2_id: row.player_2_id,
                player_2_name: row.player_2_name,
                player_2_elo: row.player_2_elo,
                dw1: row.dw1,
                dw2: row.dw2,
                rating: row.rating,
                status: row.status,
                games: gamesByDuelId.get(String(row.id || "").trim()) || [],
              })),
            });
          };

          if (!normalizedMatchIds.length) {
            return finalize([]);
          }

          const placeholders = normalizedMatchIds.map(() => "?").join(", ");
          return db.all(
            `
              SELECT
                d.id,
                d.match_id,
                d.duel_number,
                d.duel_format,
                d.time_utc,
                d.player_1_id,
                COALESCE(NULLIF(trim(p1.bga_nickname), ''), trim(d.player_1_id)) AS player_1_name,
                p1.bga_elo AS player_1_elo,
                d.player_2_id,
                COALESCE(NULLIF(trim(p2.bga_nickname), ''), trim(d.player_2_id)) AS player_2_name,
                p2.bga_elo AS player_2_elo,
                d.dw1,
                d.dw2,
                d.rating,
                d.status
              FROM duels d
              LEFT JOIN profiles p1
                ON trim(COALESCE(p1.id, '')) = trim(COALESCE(d.player_1_id, ''))
              LEFT JOIN profiles p2
                ON trim(COALESCE(p2.id, '')) = trim(COALESCE(d.player_2_id, ''))
              WHERE d.deleted_at IS NULL
                AND upper(trim(COALESCE(d.tournament_id, ''))) = upper(trim(?))
                AND trim(COALESCE(d.match_id, '')) IN (${placeholders})
              ORDER BY
                CASE WHEN d.duel_number IS NULL THEN 1 ELSE 0 END ASC,
                d.duel_number ASC,
                datetime(COALESCE(d.time_utc, '1970-01-01 00:00:00')) ASC,
                d.id ASC
            `,
            [tournamentId, ...normalizedMatchIds],
            (duelsErr, duelRows) => {
              if (duelsErr) return next(duelsErr);
              const normalizedDuelIds = Array.from(new Set(
                (duelRows || [])
                  .map((row) => String(row?.id || "").trim())
                  .filter(Boolean)
              ));
              if (!normalizedDuelIds.length) {
                return finalize(duelRows || [], []);
              }
              return loadGamesByDuelIds(normalizedDuelIds, (gamesErr, gameRows) => {
                if (gamesErr) return next(gamesErr);
                return finalize(duelRows || [], gameRows || []);
              });
            }
          );
        }
      );
    }
  );
});

app.post("/matches", (req, res) => {
  if (!req.user) {
    return res.status(401).json({ ok: false, message: "Unauthorized" });
  }

  const isAdmin = Number(req.user.admin) === 1;
  const isTeamCaptain = Number(req.user.team_captain) === 1;
  const userAssociation = String(req.user.association || "").trim().toUpperCase();
  const actorPlayerId = String(req.user.player_id || "").trim() || null;

  const payload = req.body && typeof req.body === "object" ? req.body : {};
  const normalizeCode = (value) => String(value || "").trim().toUpperCase();
  const normalizeText = (value) => {
    const v = String(value ?? "").trim();
    return v === "" ? null : v;
  };
  const parseIntOrNull = (value) => {
    const raw = String(value ?? "").trim();
    if (!raw) return null;
    const n = Number(raw);
    if (!Number.isFinite(n)) return null;
    return Math.trunc(n);
  };
  const parseUtcIsoOrNull = (value) => {
    const raw = String(value ?? "").trim();
    if (!raw) return null;
    const ts = Date.parse(raw);
    if (!Number.isFinite(ts)) return null;
    return new Date(ts).toISOString();
  };
  const computeDeadlineUtc = (timeIso, hours) => {
    const ts = Date.parse(String(timeIso || "").trim());
    const h = Number(hours);
    if (!Number.isFinite(ts) || !Number.isFinite(h) || h <= 0) return null;
    return new Date(ts - h * 60 * 60 * 1000).toISOString();
  };

  const tournamentId = normalizeText(payload.tournament_id) || "Friendly-Matches";
  const team1 = normalizeCode(payload.team_1);
  const team2 = normalizeCode(payload.team_2);
  if (!team1 || !team2) {
    return res.status(400).json({ ok: false, message: "team_1 and team_2 are required" });
  }
  if (team1 === team2) {
    return res.status(400).json({ ok: false, message: "team_1 and team_2 must be different" });
  }
  const timeUtc = parseUtcIsoOrNull(payload.time_utc);
  if (!timeUtc) {
    return res.status(400).json({ ok: false, message: "time_utc is required and must be valid UTC date-time" });
  }

  const lineupTypeRaw = String(payload.lineup_type || "").trim() || "Open";
  const lineupType = lineupTypeRaw === "Closed" ? "Secret" : lineupTypeRaw;
  if (lineupType !== "Open" && lineupType !== "Secret") {
    return res.status(400).json({ ok: false, message: "lineup_type must be Open or Secret" });
  }

  const lineupDeadlineHoursRaw = parseIntOrNull(payload.lineup_deadline_h);
  const allowedDeadlineHours = new Set([6, 12, 24, 48]);
  const lineupDeadlineHours = lineupType === "Open"
    ? null
    : (Number.isInteger(lineupDeadlineHoursRaw) ? lineupDeadlineHoursRaw : 24);
  if (lineupType === "Secret" && !allowedDeadlineHours.has(lineupDeadlineHours)) {
    return res.status(400).json({ ok: false, message: "lineup_deadline_h must be one of 6, 12, 24, 48 for Secret lineup" });
  }
  const lineupDeadlineUtc = lineupType === "Open"
    ? null
    : computeDeadlineUtc(timeUtc, lineupDeadlineHours);
  if (lineupType === "Secret" && !lineupDeadlineUtc) {
    return res.status(400).json({ ok: false, message: "Failed to calculate lineup_deadline_utc" });
  }

  const numberOfDuels = parseIntOrNull(payload.number_of_duels);
  if (!Number.isInteger(numberOfDuels) || numberOfDuels <= 0) {
    return res.status(400).json({ ok: false, message: "number_of_duels must be a positive integer" });
  }

  const status = normalizeText(payload.status) || "Planned";
  if (status !== "Planned" && status !== "Done") {
    return res.status(400).json({ ok: false, message: "status must be Planned or Done" });
  }

  const dw1 = parseIntOrNull(payload.dw1);
  const dw2 = parseIntOrNull(payload.dw2);
  const gw1 = parseIntOrNull(payload.gw1);
  const gw2 = parseIntOrNull(payload.gw2);
  const nonNegative = [dw1, dw2, gw1, gw2].every((v) => v === null || v >= 0);
  if (!nonNegative) {
    return res.status(400).json({ ok: false, message: "dw1/dw2/gw1/gw2 must be empty or non-negative integers" });
  }

  const idFromPayload = normalizeText(payload.id);
  const generatedId = `${timeUtc.slice(0, 10).replaceAll("-", "")}${team1}${team2}`;
  const matchId = idFromPayload || generatedId;

  return loadTournamentAccessForUser(tournamentId, req.user, (tournamentErr, tournament) => {
    if (tournamentErr) {
      return res.status(500).json({ ok: false, message: "Failed to validate tournament access" });
    }
    if (!tournament) {
      return res.status(404).json({ ok: false, message: "Tournament not found" });
    }

    const isClosedTournament = tournament.access_type === TOURNAMENT_ACCESS_TYPES.CLOSED;
    const canManageClosedTournamentMatches = tournament.access_role === TOURNAMENT_ACCESS_ROLES.ADMIN;
    if (isClosedTournament) {
      if (!tournament.has_access || !canManageClosedTournamentMatches) {
        return res.status(403).json({ ok: false, message: "Forbidden" });
      }
    } else {
      if (!isAdmin && !isTeamCaptain) {
        return res.status(403).json({ ok: false, message: "Forbidden" });
      }
      if (!isAdmin && isTeamCaptain && (!userAssociation || team1 !== userAssociation)) {
        return res.status(403).json({ ok: false, message: "Captain can create matches only for own team as team_1" });
      }
    }

    return db.get(
    `
      SELECT id, deleted_at
      FROM matches
      WHERE id = ?
      LIMIT 1
    `,
    [matchId],
      (dupErr, dupRow) => {
      if (dupErr) {
        return res.status(500).json({ ok: false, message: "Failed to validate match uniqueness" });
      }

      if (dupRow) {
        const deletedAt = String(dupRow.deleted_at || "").trim();
        if (!deletedAt) {
          return res.status(409).json({ ok: false, message: "Match id already exists" });
        }

        return db.serialize(() => {
          db.run("BEGIN IMMEDIATE TRANSACTION");
          db.run(
            `
              UPDATE matches
              SET
                tournament_id = ?,
                time_utc = ?,
                lineup_type = ?,
                lineup_deadline_h = ?,
                lineup_deadline_utc = ?,
                number_of_duels = ?,
                team_1 = ?,
                team_2 = ?,
                status = ?,
                dw1 = ?,
                dw2 = ?,
                gw1 = ?,
                gw2 = ?,
                deleted_at = NULL,
                deleted_by = NULL,
                updated_by = ?,
                updated_at = CURRENT_TIMESTAMP
              WHERE id = ?
            `,
            [
              tournamentId,
              timeUtc,
              lineupType,
              lineupDeadlineHours,
              lineupDeadlineUtc,
              numberOfDuels,
              team1,
              team2,
              status,
              dw1,
              dw2,
              gw1,
              gw2,
              actorPlayerId,
              matchId,
            ],
            function onRestoreMatch(restoreErr) {
              if (restoreErr) {
                db.run("ROLLBACK");
                return res.status(500).json({ ok: false, message: "Failed to restore match" });
              }
              if (!this || this.changes === 0) {
                db.run("ROLLBACK");
                return res.status(404).json({ ok: false, message: "Match not found" });
              }

              return db.run("COMMIT", (commitErr) => {
                if (commitErr) {
                  db.run("ROLLBACK");
                  return res.status(500).json({ ok: false, message: "Failed to restore match" });
                }

                return db.get(
                  `
                    SELECT
                      id,
                      tournament_id,
                      time_utc,
                      lineup_type,
                      lineup_deadline_h,
                      lineup_deadline_utc,
                      number_of_duels,
                      team_1,
                      team_2,
                      status,
                      dw1,
                      dw2,
                      gw1,
                      gw2,
                      rating
                    FROM matches
                    WHERE id = ?
                      AND deleted_at IS NULL
                    LIMIT 1
                  `,
                  [matchId],
                  (selectErr, row) => {
                    if (selectErr) {
                      return res.status(500).json({ ok: false, message: "Failed to load restored match" });
                    }
                    return logAuditEvent(
                      {
                        ...getAuditActor(req.user),
                        event_type: "match.created",
                        entity_type: "match",
                        action: "create",
                        record_id: matchId,
                        changes: buildAuditCreationChanges(row || {}, MATCH_AUDIT_FIELDS),
                        metadata: { restored: true },
                      },
                      () => res.status(200).json({ ok: true, restored: true, match: row || null })
                    );
                  }
                );
              });
            }
          );
        });
      }

      return db.run(
        `
          INSERT INTO matches (
            id,
            tournament_id,
            time_utc,
            lineup_type,
            lineup_deadline_h,
            lineup_deadline_utc,
            number_of_duels,
            team_1,
            team_2,
            status,
            dw1,
            dw2,
            gw1,
            gw2,
            created_by,
            updated_by
          )
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `,
        [
          matchId,
          tournamentId,
          timeUtc,
          lineupType,
          lineupDeadlineHours,
          lineupDeadlineUtc,
          numberOfDuels,
          team1,
          team2,
          status,
          dw1,
          dw2,
          gw1,
          gw2,
          actorPlayerId,
          actorPlayerId,
        ],
        function onInsert(insertErr) {
          if (insertErr) {
            if (String(insertErr.message || "").includes("UNIQUE")) {
              return res.status(409).json({ ok: false, message: "Match id already exists" });
            }
            return res.status(500).json({ ok: false, message: "Failed to create match" });
          }

          return db.get(
            `
              SELECT
                id,
                tournament_id,
                time_utc,
                lineup_type,
                lineup_deadline_h,
                lineup_deadline_utc,
                number_of_duels,
                team_1,
                team_2,
                status,
                dw1,
                dw2,
                gw1,
                gw2,
                rating
              FROM matches
              WHERE id = ?
              LIMIT 1
            `,
            [matchId],
            (selectErr, row) => {
              if (selectErr) {
                return res.status(500).json({ ok: false, message: "Failed to load created match" });
              }
              return logAuditEvent(
                {
                  ...getAuditActor(req.user),
                  event_type: "match.created",
                  entity_type: "match",
                  action: "create",
                  record_id: matchId,
                  changes: buildAuditCreationChanges(row || {}, MATCH_AUDIT_FIELDS),
                },
                () => res.status(201).json({ ok: true, match: row || null })
              );
            }
          );
        }
      );
      }
    );
  });
});

app.patch("/matches/:id", (req, res) => {
  if (!req.user) {
    return res.status(401).json({ ok: false, message: "Unauthorized" });
  }

  const matchId = String(req.params.id || "").trim();
  if (!matchId) {
    return res.status(400).json({ ok: false, message: "Match id is required" });
  }

  const isAdmin = Number(req.user.admin) === 1;
  const isTeamCaptain = Number(req.user.team_captain) === 1;
  const userAssociation = String(req.user.association || "").trim().toUpperCase();
  const actorPlayerId = String(req.user.player_id || "").trim() || null;

  const payload = req.body && typeof req.body === "object" ? req.body : {};
  const normalizeCode = (value) => String(value || "").trim().toUpperCase();
  const normalizeText = (value) => {
    const v = String(value ?? "").trim();
    return v === "" ? null : v;
  };
  const parseIntOrNull = (value) => {
    const raw = String(value ?? "").trim();
    if (!raw) return null;
    const n = Number(raw);
    if (!Number.isFinite(n)) return null;
    return Math.trunc(n);
  };
  const parseUtcIsoOrNull = (value) => {
    const raw = String(value ?? "").trim();
    if (!raw) return null;
    const ts = Date.parse(raw);
    if (!Number.isFinite(ts)) return null;
    return new Date(ts).toISOString();
  };
  const computeDeadlineUtc = (timeIso, hours) => {
    const ts = Date.parse(String(timeIso || "").trim());
    const h = Number(hours);
    if (!Number.isFinite(ts) || !Number.isFinite(h) || h <= 0) return null;
    return new Date(ts - h * 60 * 60 * 1000).toISOString();
  };

  return db.get(
    `
      SELECT
        id,
        tournament_id,
        time_utc,
        lineup_type,
        lineup_deadline_h,
        lineup_deadline_utc,
        number_of_duels,
        team_1,
        team_2,
        status,
        dw1,
        dw2,
        gw1,
        gw2,
        rating
      FROM matches
      WHERE id = ?
        AND deleted_at IS NULL
      LIMIT 1
    `,
    [matchId],
    (findErr, existingRow) => {
      if (findErr) {
        return res.status(500).json({ ok: false, message: "Failed to load match" });
      }
      if (!existingRow) {
        return res.status(404).json({ ok: false, message: "Match not found" });
      }

      return loadTournamentAccessForUser(existingRow.tournament_id, req.user, (tournamentErr, tournament) => {
        if (tournamentErr) {
          return res.status(500).json({ ok: false, message: "Failed to validate tournament access" });
        }
        if (!tournament) {
          return res.status(404).json({ ok: false, message: "Tournament not found" });
        }

        const existingTeam1 = normalizeCode(existingRow.team_1);
        const existingTeam2 = normalizeCode(existingRow.team_2);
        const captainCanEdit = !isAdmin
          && isTeamCaptain
          && userAssociation
          && (existingTeam1 === userAssociation || existingTeam2 === userAssociation);
        const canUseOpenTournamentRules = isAdmin || isTeamCaptain;
        const canManageClosedTournamentMatches = tournament.access_role === TOURNAMENT_ACCESS_ROLES.ADMIN;
        const canEdit = tournament.access_type === TOURNAMENT_ACCESS_TYPES.CLOSED
          ? (tournament.has_access && canManageClosedTournamentMatches)
          : (canUseOpenTournamentRules && (isAdmin || captainCanEdit));
        if (!canEdit) {
          return res.status(403).json({ ok: false, message: "Forbidden" });
        }

      const team1 = normalizeCode(payload.team_1);
      const team2 = normalizeCode(payload.team_2);
      if (!team1 || !team2) {
        return res.status(400).json({ ok: false, message: "team_1 and team_2 are required" });
      }
      if (team1 === team2) {
        return res.status(400).json({ ok: false, message: "team_1 and team_2 must be different" });
      }

      const timeUtc = parseUtcIsoOrNull(payload.time_utc);
      if (!timeUtc) {
        return res.status(400).json({ ok: false, message: "time_utc is required and must be valid UTC date-time" });
      }

      const lineupTypeRaw = String(payload.lineup_type || "").trim();
      const lineupType = lineupTypeRaw === "Closed" ? "Secret" : lineupTypeRaw;
      if (lineupType !== "Open" && lineupType !== "Secret") {
        return res.status(400).json({ ok: false, message: "lineup_type must be Open or Secret" });
      }

      const lineupDeadlineHoursRaw = parseIntOrNull(payload.lineup_deadline_h);
      const allowedDeadlineHours = new Set([6, 12, 24, 48]);
      const lineupDeadlineHours = lineupType === "Open"
        ? null
        : (Number.isInteger(lineupDeadlineHoursRaw) ? lineupDeadlineHoursRaw : 24);
      if (lineupType === "Secret" && !allowedDeadlineHours.has(lineupDeadlineHours)) {
        return res.status(400).json({ ok: false, message: "lineup_deadline_h must be one of 6, 12, 24, 48 for Secret lineup" });
      }
      const lineupDeadlineUtc = lineupType === "Open"
        ? null
        : computeDeadlineUtc(timeUtc, lineupDeadlineHours);
      if (lineupType === "Secret" && !lineupDeadlineUtc) {
        return res.status(400).json({ ok: false, message: "Failed to calculate lineup_deadline_utc" });
      }

      const numberOfDuels = parseIntOrNull(payload.number_of_duels);
      if (!Number.isInteger(numberOfDuels) || numberOfDuels <= 0) {
        return res.status(400).json({ ok: false, message: "number_of_duels must be a positive integer" });
      }

      const status = normalizeText(payload.status);
      if (status !== "Planned" && status !== "Done") {
        return res.status(400).json({ ok: false, message: "status must be Planned or Done" });
      }

      const idFromPayload = normalizeText(payload.id);
      const generatedId = `${timeUtc.slice(0, 10).replaceAll("-", "")}${team1}${team2}`;
      const nextMatchId = idFromPayload || generatedId;
      if (!nextMatchId) {
        return res.status(400).json({ ok: false, message: "id is required" });
      }

      const dw1 = parseIntOrNull(payload.dw1);
      const dw2 = parseIntOrNull(payload.dw2);
      const gw1 = parseIntOrNull(payload.gw1);
      const gw2 = parseIntOrNull(payload.gw2);
      const nonNegative = [dw1, dw2, gw1, gw2].every((v) => v === null || v >= 0);
      if (!nonNegative) {
        return res.status(400).json({ ok: false, message: "dw1/dw2/gw1/gw2 must be empty or non-negative integers" });
      }

        return db.run(
        `
          UPDATE matches
          SET
            id = ?,
            tournament_id = ?,
            team_1 = ?,
            team_2 = ?,
            time_utc = ?,
            lineup_type = ?,
            lineup_deadline_h = ?,
            lineup_deadline_utc = ?,
            number_of_duels = ?,
            status = ?,
            dw1 = ?,
            dw2 = ?,
            gw1 = ?,
            gw2 = ?,
            updated_by = ?
          WHERE id = ?
            AND deleted_at IS NULL
        `,
        [
          nextMatchId,
          tournament.id,
          team1,
          team2,
          timeUtc,
          lineupType,
          lineupDeadlineHours,
          lineupDeadlineUtc,
          numberOfDuels,
          status,
          dw1,
          dw2,
          gw1,
          gw2,
          actorPlayerId,
          matchId,
        ],
        function onUpdate(updateErr) {
          if (updateErr) {
            if (String(updateErr.message || "").includes("UNIQUE")) {
              return res.status(409).json({ ok: false, message: "Match id already exists" });
            }
            return res.status(500).json({ ok: false, message: "Failed to update match" });
          }
          if (!this || this.changes === 0) {
            return res.status(404).json({ ok: false, message: "Match not found" });
          }

            return db.get(
              `
                SELECT
                  id,
                  tournament_id,
                  time_utc,
                  lineup_type,
                  lineup_deadline_h,
                  lineup_deadline_utc,
                  number_of_duels,
                  team_1,
                  team_2,
                  status,
                  dw1,
                  dw2,
                  gw1,
                  gw2,
                  rating
                FROM matches
                WHERE id = ?
                LIMIT 1
              `,
              [nextMatchId],
              (selectErr, row) => {
                if (selectErr) {
                  return res.status(500).json({ ok: false, message: "Failed to load updated match" });
                }
                const changes = buildAuditChanges(existingRow || {}, row || {}, MATCH_AUDIT_FIELDS);
                if (!Object.keys(changes).length) {
                  return res.json({ ok: true, match: row || null });
                }
                return logAuditEvent(
                  {
                    ...getAuditActor(req.user),
                    event_type: "match.updated",
                    entity_type: "match",
                    action: "update",
                    record_id: nextMatchId,
                    changes,
                    metadata: { previous_record_id: matchId !== nextMatchId ? matchId : null },
                  },
                  () => res.json({ ok: true, match: row || null })
                );
              }
            );
        }
      );
      });
    }
  );
});

app.delete("/matches/:id", (req, res) => {
  if (!req.user) {
    return res.status(401).json({ ok: false, message: "Unauthorized" });
  }

  const matchId = String(req.params.id || "").trim();
  if (!matchId) {
    return res.status(400).json({ ok: false, message: "Match id is required" });
  }

  const isAdmin = Number(req.user.admin) === 1;
  const isTeamCaptain = Number(req.user.team_captain) === 1;
  const userAssociation = String(req.user.association || "").trim().toUpperCase();
  const actorPlayerId = String(req.user.player_id || "").trim() || null;

  return db.get(
    `
      SELECT
        id,
        tournament_id,
        time_utc,
        lineup_type,
        lineup_deadline_h,
        lineup_deadline_utc,
        number_of_duels,
        team_1,
        team_2,
        status,
        dw1,
        dw2,
        gw1,
        gw2,
        rating
      FROM matches
      WHERE id = ?
        AND deleted_at IS NULL
      LIMIT 1
    `,
    [matchId],
    (findErr, row) => {
      if (findErr) {
        return res.status(500).json({ ok: false, message: "Failed to load match" });
      }
      if (!row) {
        return res.status(404).json({ ok: false, message: "Match not found" });
      }

      return loadTournamentAccessForUser(row.tournament_id, req.user, (tournamentErr, tournament) => {
        if (tournamentErr) {
          return res.status(500).json({ ok: false, message: "Failed to validate tournament access" });
        }
        if (!tournament) {
          return res.status(404).json({ ok: false, message: "Tournament not found" });
        }

        const team1 = String(row.team_1 || "").trim().toUpperCase();
        const team2 = String(row.team_2 || "").trim().toUpperCase();
        const captainCanDelete = !isAdmin
          && isTeamCaptain
          && userAssociation
          && (team1 === userAssociation || team2 === userAssociation);
        const canUseOpenTournamentRules = isAdmin || isTeamCaptain;
        const canManageClosedTournamentMatches = tournament.access_role === TOURNAMENT_ACCESS_ROLES.ADMIN;
        const canDelete = tournament.access_type === TOURNAMENT_ACCESS_TYPES.CLOSED
          ? (tournament.has_access && canManageClosedTournamentMatches)
          : (canUseOpenTournamentRules && (isAdmin || captainCanDelete));
        if (!canDelete) {
          return res.status(403).json({ ok: false, message: "Forbidden" });
        }

        return db.all(
        `
          SELECT
            id,
            tournament_id,
            match_id,
            duel_number,
            duel_format,
            time_utc,
            custom_time,
            player_1_id,
            player_2_id,
            dw1,
            dw2,
            status
          FROM duels
          WHERE match_id = ?
            AND deleted_at IS NULL
          ORDER BY
            CASE WHEN duel_number IS NULL THEN 1 ELSE 0 END ASC,
            duel_number ASC,
            id ASC
        `,
        [matchId],
          (lineupsErr, activeLineups) => {
          if (lineupsErr) {
            return res.status(500).json({ ok: false, message: "Failed to load match duels" });
          }

          return db.serialize(() => {
            db.run("BEGIN IMMEDIATE TRANSACTION");
            db.run(
              `
                UPDATE matches
                SET
                  deleted_at = CURRENT_TIMESTAMP,
                  deleted_by = ?,
                  updated_by = ?,
                  updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                  AND deleted_at IS NULL
              `,
              [actorPlayerId, actorPlayerId, matchId],
              function onDeleteMatch(deleteErr) {
                if (deleteErr) {
                  db.run("ROLLBACK");
                  return res.status(500).json({ ok: false, message: "Failed to delete match" });
                }
                if (!this || this.changes === 0) {
                  db.run("ROLLBACK");
                  return res.status(404).json({ ok: false, message: "Match not found" });
                }

                return db.run(
                  `
                    UPDATE duels
                    SET
                      deleted_at = CURRENT_TIMESTAMP,
                      deleted_by = ?,
                      updated_by = ?,
                      updated_at = CURRENT_TIMESTAMP
                    WHERE match_id = ?
                      AND deleted_at IS NULL
                  `,
                  [actorPlayerId, actorPlayerId, matchId],
                  (deleteLineupsErr) => {
                    if (deleteLineupsErr) {
                      db.run("ROLLBACK");
                      return res.status(500).json({ ok: false, message: "Failed to delete match duels" });
                    }

                    return db.run("COMMIT", (commitErr) => {
                      if (commitErr) {
                        db.run("ROLLBACK");
                        return res.status(500).json({ ok: false, message: "Failed to delete match" });
                      }

                      return logAuditEvent(
                        {
                          ...getAuditActor(req.user),
                          event_type: "match.deleted",
                          entity_type: "match",
                          action: "delete",
                          record_id: matchId,
                          changes: buildAuditDeletionChanges(row || {}, MATCH_AUDIT_FIELDS),
                        },
                        () => {
                          if (!activeLineups.length) {
                            return res.json({ ok: true });
                          }

                          return logAuditEvent(
                            {
                              ...getAuditActor(req.user),
                              event_type: "lineups.deleted_all",
                              entity_type: "lineups",
                              action: "delete",
                              record_id: matchId,
                              changes: buildAuditDeletionChanges({ lineups: activeLineups || [] }, ["lineups"]),
                              metadata: {
                                match_id: matchId,
                                lineups_count: activeLineups.length,
                                team_1: row?.team_1 || null,
                                team_2: row?.team_2 || null,
                              },
                            },
                            () => res.json({ ok: true })
                          );
                        }
                      );
                    });
                  }
                );
              }
            );
          });
        });
      });
    }
  );
});

app.post("/matches/:id/get-results", requireAdmin, async (req, res) => {
  const matchId = String(req.params.id || "").trim();
  if (!matchId) {
    return res.status(400).json({ ok: false, message: "Match id is required" });
  }

  const actorPlayerId = String(req.user?.player_id || "").trim() || null;
  const authServerRoot = path.resolve(__dirname, "..");
  const updateScriptPath = path.resolve(authServerRoot, "run_update_matches.py");
  const pythonBin = String(process.env.PYTHON_BIN || "python3").trim() || "python3";

  const rollbackQuietly = async () => {
    try {
      await dbRunAsync("ROLLBACK");
    } catch (_error) {
      // ignore rollback errors
    }
  };

  try {
    const existingMatch = await dbGetAsync(
      `
        SELECT
          id,
          tournament_id,
          time_utc,
          lineup_type,
          lineup_deadline_h,
          lineup_deadline_utc,
          number_of_duels,
          team_1,
          team_2,
          status,
          dw1,
          dw2,
          gw1,
          gw2,
          rating
        FROM matches
        WHERE id = ?
          AND deleted_at IS NULL
        LIMIT 1
      `,
      [matchId]
    );
    if (!existingMatch) {
      return res.status(404).json({ ok: false, message: "Match not found" });
    }

    const duelRows = await dbAllAsync(
      `
        SELECT id
        FROM duels
        WHERE match_id = ?
          AND deleted_at IS NULL
      `,
      [matchId]
    );
    const duelIds = duelRows
      .map((row) => String(row?.id || "").trim())
      .filter(Boolean);

    await dbRunAsync("BEGIN IMMEDIATE TRANSACTION");
    try {
      await dbRunAsync(
        `
          UPDATE matches
          SET
            dw1 = NULL,
            dw2 = NULL,
            gw1 = NULL,
            gw2 = NULL,
            status = 'Planned',
            updated_by = ?,
            updated_at = CURRENT_TIMESTAMP
          WHERE id = ?
            AND deleted_at IS NULL
        `,
        [actorPlayerId, matchId]
      );

      await dbRunAsync(
        `
          UPDATE duels
          SET
            dw1 = NULL,
            dw2 = NULL,
            status = 'Planned',
            results_last_error = NULL,
            results_checked_at = NULL,
            updated_by = ?,
            updated_at = CURRENT_TIMESTAMP
          WHERE match_id = ?
            AND deleted_at IS NULL
        `,
        [actorPlayerId, matchId]
      );

      if (duelIds.length) {
        const placeholders = duelIds.map(() => "?").join(", ");
        await dbRunAsync(
          `
            DELETE FROM games
            WHERE trim(COALESCE(duel_id, '')) IN (${placeholders})
          `,
          duelIds
        );
      }

      await dbRunAsync("COMMIT");
    } catch (resetError) {
      await rollbackQuietly();
      throw resetError;
    }

    const execResult = await execFileAsync(
      pythonBin,
      [
        updateScriptPath,
        "--db-path",
        dbFullPath,
        "--match-id",
        matchId,
      ],
      {
        cwd: authServerRoot,
        env: process.env,
        timeout: 5 * 60 * 1000,
        maxBuffer: 1024 * 1024 * 10,
      }
    );

    const refreshedMatch = await dbGetAsync(
      `
        SELECT
          id,
          tournament_id,
          time_utc,
          lineup_type,
          lineup_deadline_h,
          lineup_deadline_utc,
          number_of_duels,
          team_1,
          team_2,
          status,
          dw1,
          dw2,
          gw1,
          gw2,
          rating
        FROM matches
        WHERE id = ?
          AND deleted_at IS NULL
        LIMIT 1
      `,
      [matchId]
    );
    const refreshedDuels = await dbAllAsync(
      `
        SELECT
          id,
          dw1,
          dw2,
          status,
          results_last_error,
          results_checked_at
        FROM duels
        WHERE match_id = ?
          AND deleted_at IS NULL
        ORDER BY
          CASE WHEN duel_number IS NULL THEN 1 ELSE 0 END ASC,
          duel_number ASC,
          id ASC
      `,
      [matchId]
    );
    const refreshedDuelIds = refreshedDuels
      .map((row) => String(row?.id || "").trim())
      .filter(Boolean);
    const refreshedGames = refreshedDuelIds.length
      ? await dbAllAsync(
          `
            SELECT id
            FROM games
            WHERE trim(COALESCE(duel_id, '')) IN (${refreshedDuelIds.map(() => "?").join(", ")})
          `,
          refreshedDuelIds
        )
      : [];

    return logAuditEvent(
      {
        ...getAuditActor(req.user),
        event_type: "match.results_refetched",
        entity_type: "match",
        action: "refresh_results",
        record_id: matchId,
        changes: {
          results_refresh: {
            before: {
              status: existingMatch.status ?? null,
              dw1: existingMatch.dw1 ?? null,
              dw2: existingMatch.dw2 ?? null,
              gw1: existingMatch.gw1 ?? null,
              gw2: existingMatch.gw2 ?? null,
            },
            after: {
              status: refreshedMatch?.status ?? null,
              dw1: refreshedMatch?.dw1 ?? null,
              dw2: refreshedMatch?.dw2 ?? null,
              gw1: refreshedMatch?.gw1 ?? null,
              gw2: refreshedMatch?.gw2 ?? null,
            },
          },
        },
        metadata: {
          duels_total: refreshedDuels.length,
          duels_with_errors: refreshedDuels.filter((row) => String(row?.results_last_error || "").trim()).length,
          games_total: refreshedGames.length,
        },
      },
      () => res.json({
        ok: true,
        message: "Results updated.",
        match: refreshedMatch,
        summary: {
          duels_total: refreshedDuels.length,
          duels_done: refreshedDuels.filter((row) => String(row?.status || "").trim() === "Done").length,
          duels_in_progress: refreshedDuels.filter((row) => String(row?.status || "").trim() === "In progress").length,
          duels_error: refreshedDuels.filter((row) => String(row?.results_last_error || "").trim()).length,
          games_total: refreshedGames.length,
        },
        logs: String(execResult.stdout || execResult.stderr || "").trim(),
      })
    );
  } catch (error) {
    console.error(`Failed to refresh match results for ${matchId}`, error);
    const stderr = String(error?.stderr || "").trim();
    const stdout = String(error?.stdout || "").trim();
    return res.status(500).json({
      ok: false,
      message: stderr || stdout || error.message || "Failed to update match results.",
    });
  }
});

app.get("/friendly-find", (_req, res, next) => {
  db.all(
    `
      SELECT
        id,
        team,
        dates,
        time_1,
        time_2,
        number_of_players
      FROM friendly_find
      ORDER BY id DESC
    `,
    (err, rows) => {
      if (err) return next(err);
      return res.json({ ok: true, requests: rows || [] });
    }
  );
});

function parseFriendlyFindPayload(payload) {
  const team = String(payload?.team || "").trim().toUpperCase();
  const time1 = String(payload?.time_1 || "").trim();
  const time2 = String(payload?.time_2 || "").trim();
  const playersAvailable = String(payload?.number_of_players || "").trim();
  const rawDates = Array.isArray(payload?.dates) ? payload.dates : [];

  const normalizeDate = (value) => {
    const raw = String(value || "").trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) return "";
    const ts = Date.parse(`${raw}T00:00:00Z`);
    if (!Number.isFinite(ts)) return "";
    return raw;
  };
  const normalizedDates = Array.from(new Set(rawDates.map(normalizeDate).filter(Boolean))).sort();

  return {
    team,
    time1,
    time2,
    playersAvailable,
    normalizedDates,
  };
}

app.post("/friendly-find", (req, res) => {
  if (!req.user) {
    return res.status(401).json({ ok: false, message: "Unauthorized" });
  }

  const isAdmin = Number(req.user.admin) === 1;
  const isTeamCaptain = Number(req.user.team_captain) === 1;
  const userAssociation = String(req.user.association || "").trim().toUpperCase();
  if (!isAdmin && !isTeamCaptain) {
    return res.status(403).json({ ok: false, message: "Forbidden" });
  }

  const payload = req.body && typeof req.body === "object" ? req.body : {};
  const {
    team,
    time1,
    time2,
    playersAvailable,
    normalizedDates,
  } = parseFriendlyFindPayload(payload);

  if (!team) {
    return res.status(400).json({ ok: false, message: "team is required" });
  }
  if (!isAdmin && (!userAssociation || team !== userAssociation)) {
    return res.status(403).json({ ok: false, message: "Captain can create requests only for own team" });
  }

  if (normalizedDates.length < 1 || normalizedDates.length > 5) {
    return res.status(400).json({ ok: false, message: "dates must contain from 1 to 5 valid dates" });
  }

  const isValidHour = (value) => /^([01]?\d|2[0-3]):00$/.test(String(value || "").trim());
  if (!isValidHour(time1) || !isValidHour(time2)) {
    return res.status(400).json({ ok: false, message: "time_1 and time_2 must be in HH:00 format" });
  }
  if (time1 > time2) {
    return res.status(400).json({ ok: false, message: "time_1 must be earlier than or equal to time_2" });
  }

  if (!playersAvailable) {
    return res.status(400).json({ ok: false, message: "number_of_players is required" });
  }

  return db.run(
    `
      INSERT INTO friendly_find (
        team,
        dates,
        time_1,
        time_2,
        number_of_players
      )
      VALUES (?, ?, ?, ?, ?)
    `,
    [team, JSON.stringify(normalizedDates), time1, time2, playersAvailable],
    function onInsert(insertErr) {
      if (insertErr) {
        return res.status(500).json({ ok: false, message: "Failed to create friendly find request" });
      }

      return db.get(
        `
          SELECT
            id,
            team,
            dates,
            time_1,
            time_2,
            number_of_players
          FROM friendly_find
          WHERE id = ?
          LIMIT 1
        `,
        [this.lastID],
        (selectErr, row) => {
          if (selectErr) {
            return res.status(500).json({ ok: false, message: "Failed to load created friendly find request" });
          }
          return logAuditEvent(
            {
              ...getAuditActor(req.user),
              event_type: "friendly_find.created",
              entity_type: "friendly_find",
              action: "create",
              record_id: String(this.lastID),
              changes: buildAuditCreationChanges(row || {}, FRIENDLY_FIND_AUDIT_FIELDS),
            },
            () => res.status(201).json({ ok: true, request: row || null })
          );
        }
      );
    }
  );
});

app.patch("/friendly-find/:id", (req, res) => {
  if (!req.user) {
    return res.status(401).json({ ok: false, message: "Unauthorized" });
  }

  const requestId = Number(req.params.id);
  if (!Number.isInteger(requestId) || requestId <= 0) {
    return res.status(400).json({ ok: false, message: "Invalid request id" });
  }

  const isAdmin = Number(req.user.admin) === 1;
  const isTeamCaptain = Number(req.user.team_captain) === 1;
  const userAssociation = String(req.user.association || "").trim().toUpperCase();
  if (!isAdmin && !isTeamCaptain) {
    return res.status(403).json({ ok: false, message: "Forbidden" });
  }

  return db.get(
    `
      SELECT
        id,
        team,
        dates,
        time_1,
        time_2,
        number_of_players
      FROM friendly_find
      WHERE id = ?
      LIMIT 1
    `,
    [requestId],
    (findErr, existingRow) => {
      if (findErr) {
        return res.status(500).json({ ok: false, message: "Failed to load friendly find request" });
      }
      if (!existingRow) {
        return res.status(404).json({ ok: false, message: "Friendly find request not found" });
      }

      const existingTeam = String(existingRow.team || "").trim().toUpperCase();
      if (!isAdmin && (!userAssociation || existingTeam !== userAssociation)) {
        return res.status(403).json({ ok: false, message: "Captain can edit requests only for own team" });
      }

      const payload = req.body && typeof req.body === "object" ? req.body : {};
      const {
        team,
        time1,
        time2,
        playersAvailable,
        normalizedDates,
      } = parseFriendlyFindPayload(payload);

      if (!team) {
        return res.status(400).json({ ok: false, message: "team is required" });
      }
      if (!isAdmin && (!userAssociation || team !== userAssociation)) {
        return res.status(403).json({ ok: false, message: "Captain can edit requests only for own team" });
      }
      if (normalizedDates.length < 1 || normalizedDates.length > 5) {
        return res.status(400).json({ ok: false, message: "dates must contain from 1 to 5 valid dates" });
      }

      const isValidHour = (value) => /^([01]?\d|2[0-3]):00$/.test(String(value || "").trim());
      if (!isValidHour(time1) || !isValidHour(time2)) {
        return res.status(400).json({ ok: false, message: "time_1 and time_2 must be in HH:00 format" });
      }
      if (time1 > time2) {
        return res.status(400).json({ ok: false, message: "time_1 must be earlier than or equal to time_2" });
      }
      if (!playersAvailable) {
        return res.status(400).json({ ok: false, message: "number_of_players is required" });
      }

      return db.run(
        `
          UPDATE friendly_find
          SET
            team = ?,
            dates = ?,
            time_1 = ?,
            time_2 = ?,
            number_of_players = ?
          WHERE id = ?
        `,
        [team, JSON.stringify(normalizedDates), time1, time2, playersAvailable, requestId],
        function onUpdate(updateErr) {
          if (updateErr) {
            return res.status(500).json({ ok: false, message: "Failed to update friendly find request" });
          }
          if (!this || this.changes === 0) {
            return res.status(404).json({ ok: false, message: "Friendly find request not found" });
          }
          return db.get(
            `
              SELECT
                id,
                team,
                dates,
                time_1,
                time_2,
                number_of_players
              FROM friendly_find
              WHERE id = ?
              LIMIT 1
            `,
            [requestId],
            (selectErr, row) => {
              if (selectErr) {
                return res.status(500).json({ ok: false, message: "Failed to load updated friendly find request" });
              }
              return logAuditEvent(
                {
                  ...getAuditActor(req.user),
                  event_type: "friendly_find.updated",
                  entity_type: "friendly_find",
                  action: "update",
                  record_id: String(requestId),
                  changes: buildAuditChanges(existingRow || {}, row || {}, FRIENDLY_FIND_AUDIT_FIELDS),
                },
                () => res.json({ ok: true, request: row || null })
              );
            }
          );
        }
      );
    }
  );
});

app.delete("/friendly-find/:id", (req, res) => {
  if (!req.user) {
    return res.status(401).json({ ok: false, message: "Unauthorized" });
  }

  const requestId = Number(req.params.id);
  if (!Number.isInteger(requestId) || requestId <= 0) {
    return res.status(400).json({ ok: false, message: "Invalid request id" });
  }

  const isAdmin = Number(req.user.admin) === 1;
  const isTeamCaptain = Number(req.user.team_captain) === 1;
  const userAssociation = String(req.user.association || "").trim().toUpperCase();
  if (!isAdmin && !isTeamCaptain) {
    return res.status(403).json({ ok: false, message: "Forbidden" });
  }

  return db.get(
    `
      SELECT
        id,
        team,
        dates,
        time_1,
        time_2,
        number_of_players
      FROM friendly_find
      WHERE id = ?
      LIMIT 1
    `,
    [requestId],
    (findErr, existingRow) => {
      if (findErr) {
        return res.status(500).json({ ok: false, message: "Failed to load friendly find request" });
      }
      if (!existingRow) {
        return res.status(404).json({ ok: false, message: "Friendly find request not found" });
      }

      const existingTeam = String(existingRow.team || "").trim().toUpperCase();
      if (!isAdmin && (!userAssociation || existingTeam !== userAssociation)) {
        return res.status(403).json({ ok: false, message: "Captain can delete requests only for own team" });
      }

      return db.run(
        `
          DELETE FROM friendly_find
          WHERE id = ?
        `,
        [requestId],
        function onDelete(deleteErr) {
          if (deleteErr) {
            return res.status(500).json({ ok: false, message: "Failed to delete friendly find request" });
          }
          if (!this || this.changes === 0) {
            return res.status(404).json({ ok: false, message: "Friendly find request not found" });
          }
          return logAuditEvent(
            {
              ...getAuditActor(req.user),
              event_type: "friendly_find.deleted",
              entity_type: "friendly_find",
              action: "delete",
              record_id: String(requestId),
              changes: buildAuditDeletionChanges(existingRow || {}, FRIENDLY_FIND_AUDIT_FIELDS),
            },
            () => res.json({ ok: true })
          );
        }
      );
    }
  );
});

app.get("/auth/me", (req, res) => {
  if (!req.user) {
    return res.status(401).json({ authenticated: false });
  }

  const {
    id,
    google_id,
    email,
    name,
    picture,
    player_id,
    bga_id,
    admin,
    bga_nickname,
    profile_name,
    profile_email,
    association,
    master_title,
    master_title_date,
    team_captain,
  } = req.user;
  const myProfileUrl = player_id
    ? `${SITE_BASE_URL}/player/?id=${encodeURIComponent(player_id)}`
    : `${SITE_BASE_URL}/player/?id=noprofile`;
  const isAdmin = Number(admin) === 1;

  return res.json({
    authenticated: true,
    user: {
      id,
      googleId: google_id,
      email,
      name,
      picture,
      bgaId: bga_id || null,
      playerId: player_id || null,
      isAdmin,
      myProfileUrl,
      adminPanelUrl: isAdmin ? `${SITE_BASE_URL}/admin` : null,
      profile: {
        bgaNickname: bga_nickname || null,
        name: profile_name || null,
        association: association || null,
        email: profile_email || null,
        masterTitle: Number(master_title) === 1,
        masterTitleDate: master_title_date || null,
        teamCaptain: Number(team_captain) === 1,
      },
    },
  });
});

app.get("/auth/can-edit-player/:playerId", (req, res) => {
  if (!req.user) {
    return res.status(401).json({ authenticated: false, canEdit: false });
  }

  const requestedPlayerId = String(req.params.playerId || "").trim();
  const linkedPlayerId = String(req.user.player_id || "").trim();
  const isAdmin = Number(req.user.admin) === 1;
  const isTeamCaptain = Number(req.user.team_captain) === 1;
  const userAssociation = String(req.user.association || "").trim().toLowerCase();

  if (!requestedPlayerId) {
    return res.json({
      authenticated: true,
      canEdit: false,
      isAdmin,
      isTeamCaptain,
      requestedPlayerId,
      linkedPlayerId: linkedPlayerId || null,
    });
  }

  // Rule 1: admin can edit any profile.
  if (isAdmin) {
    return res.json({
      authenticated: true,
      canEdit: true,
      isAdmin,
      isTeamCaptain,
      requestedPlayerId,
      linkedPlayerId: linkedPlayerId || null,
    });
  }

  // Rule 2: owner can edit own profile.
  if (requestedPlayerId === linkedPlayerId) {
    return res.json({
      authenticated: true,
      canEdit: true,
      isAdmin,
      isTeamCaptain,
      requestedPlayerId,
      linkedPlayerId: linkedPlayerId || null,
    });
  }

  // Rule 3: team captain can edit profiles with the same association.
  if (isTeamCaptain && userAssociation) {
    return db.get(
      "SELECT association FROM profiles WHERE id = ? LIMIT 1",
      [requestedPlayerId],
      (targetErr, targetRow) => {
        if (targetErr) return res.status(500).json({ authenticated: true, canEdit: false });
        const targetAssociation = String(targetRow?.association || "").trim().toLowerCase();
        const canEdit = targetAssociation !== "" && targetAssociation === userAssociation;
        return res.json({
          authenticated: true,
          canEdit,
          isAdmin,
          isTeamCaptain,
          requestedPlayerId,
          linkedPlayerId: linkedPlayerId || null,
        });
      }
    );
  }

  return res.json({
    authenticated: true,
    canEdit: false,
    isAdmin,
    isTeamCaptain,
    requestedPlayerId,
    linkedPlayerId: linkedPlayerId || null,
  });
});

app.patch("/profiles/:playerId", (req, res) => {
  if (!req.user) {
    return res.status(401).json({ ok: false, message: "Unauthorized" });
  }

  const requestedPlayerId = String(req.params.playerId || "").trim();
  if (!requestedPlayerId) {
    return res.status(400).json({ ok: false, message: "playerId is required" });
  }

  const linkedPlayerId = String(req.user.player_id || "").trim();
  const actorPlayerId = linkedPlayerId || null;
  const isAdmin = Number(req.user.admin) === 1;
  const isTeamCaptain = Number(req.user.team_captain) === 1;
  const userAssociation = String(req.user.association || "").trim().toLowerCase();

  const payload = req.body && typeof req.body === "object" ? req.body : {};
  const hasAssociationInPayload = Object.prototype.hasOwnProperty.call(payload, "association");
  const hasTeamCaptainInPayload = Object.prototype.hasOwnProperty.call(payload, "team_captain");

  const normalizeText = (value) => {
    const v = String(value ?? "").trim();
    return v === "" ? null : v;
  };

  const normalizeBool = (value) => {
    if (typeof value === "boolean") return value;
    if (typeof value === "number") return value === 1;
    const raw = String(value ?? "").trim().toLowerCase();
    return raw === "1" || raw === "true" || raw === "yes" || raw === "on";
  };

  const profilePatch = {
    name: normalizeText(payload.name),
    status: normalizeText(payload.status) || "Active",
    master_title: normalizeBool(payload.master_title) ? 1 : 0,
    master_title_date: normalizeText(payload.master_title_date),
    email: normalizeText(payload.email),
    association: normalizeText(payload.association),
    team_captain: hasTeamCaptainInPayload ? (normalizeBool(payload.team_captain) ? 1 : 0) : null,
    telegram: normalizeText(payload.telegram),
    whatsapp: normalizeText(payload.whatsapp),
    discord: normalizeText(payload.discord),
    instagram: normalizeText(payload.instagram),
    contact_email: normalizeText(payload.contact_email),
  };

  if (profilePatch.master_title === 1 && !profilePatch.master_title_date) {
    return res.status(400).json({
      ok: false,
      message: "master_title_date is required when master_title is enabled",
    });
  }

  if (profilePatch.master_title === 0) {
    profilePatch.master_title_date = null;
  }

  if (profilePatch.status !== "Active" && profilePatch.status !== "Inactive") {
    return res.status(400).json({ ok: false, message: "status must be Active or Inactive" });
  }

  if (isAdmin && hasAssociationInPayload && !profilePatch.association) {
    return res.status(400).json({ ok: false, message: "association cannot be empty" });
  }

  const ownerCanEdit = requestedPlayerId === linkedPlayerId;
  const decideAndUpdate = (captainCanEdit) => {
    const canEdit = isAdmin || ownerCanEdit || captainCanEdit;
    if (!canEdit) {
      return res.status(403).json({ ok: false, message: "Forbidden" });
    }

    const allowedPatch = isAdmin
      ? profilePatch
      : captainCanEdit
        ? {
            name: profilePatch.name,
            status: profilePatch.status,
            master_title: profilePatch.master_title,
            master_title_date: profilePatch.master_title_date,
            email: profilePatch.email,
            telegram: profilePatch.telegram,
            whatsapp: profilePatch.whatsapp,
            discord: profilePatch.discord,
            instagram: profilePatch.instagram,
            contact_email: profilePatch.contact_email,
          }
        : {
            name: profilePatch.name,
            status: profilePatch.status,
            master_title: profilePatch.master_title,
            master_title_date: profilePatch.master_title_date,
            telegram: profilePatch.telegram,
            whatsapp: profilePatch.whatsapp,
            discord: profilePatch.discord,
            instagram: profilePatch.instagram,
            contact_email: profilePatch.contact_email,
          };

    const sql = isAdmin
      ? `
          UPDATE profiles
          SET
            name = ?,
            status = ?,
            master_title = ?,
            master_title_date = ?,
            email = ?,
            association = COALESCE(?, association),
            team_captain = COALESCE(?, team_captain),
            telegram = ?,
            whatsapp = ?,
            discord = ?,
            instagram = ?,
            contact_email = ?,
            updated_by = ?,
            updated_at = CURRENT_TIMESTAMP
          WHERE id = ?
            AND deleted_at IS NULL
        `
      : captainCanEdit
        ? `
          UPDATE profiles
          SET
            name = ?,
            status = ?,
            master_title = ?,
            master_title_date = ?,
            email = ?,
            telegram = ?,
            whatsapp = ?,
            discord = ?,
            instagram = ?,
            contact_email = ?,
            updated_by = ?,
            updated_at = CURRENT_TIMESTAMP
          WHERE id = ?
            AND deleted_at IS NULL
        `
      : `
          UPDATE profiles
          SET
            name = ?,
            status = ?,
            master_title = ?,
            master_title_date = ?,
            telegram = ?,
            whatsapp = ?,
            discord = ?,
            instagram = ?,
            contact_email = ?,
            updated_by = ?,
            updated_at = CURRENT_TIMESTAMP
          WHERE id = ?
            AND deleted_at IS NULL
        `;

    const params = isAdmin
      ? [
          allowedPatch.name,
          allowedPatch.status,
          allowedPatch.master_title,
          allowedPatch.master_title_date,
          allowedPatch.email,
          allowedPatch.association,
          allowedPatch.team_captain,
          allowedPatch.telegram,
          allowedPatch.whatsapp,
          allowedPatch.discord,
          allowedPatch.instagram,
          allowedPatch.contact_email,
          actorPlayerId,
          requestedPlayerId,
        ]
      : captainCanEdit
        ? [
          allowedPatch.name,
          allowedPatch.status,
          allowedPatch.master_title,
          allowedPatch.master_title_date,
          allowedPatch.email,
          allowedPatch.telegram,
          allowedPatch.whatsapp,
          allowedPatch.discord,
          allowedPatch.instagram,
          allowedPatch.contact_email,
          actorPlayerId,
          requestedPlayerId,
        ]
      : [
          allowedPatch.name,
          allowedPatch.status,
          allowedPatch.master_title,
          allowedPatch.master_title_date,
          allowedPatch.telegram,
          allowedPatch.whatsapp,
          allowedPatch.discord,
          allowedPatch.instagram,
          allowedPatch.contact_email,
          actorPlayerId,
          requestedPlayerId,
        ];

    const runUpdate = () => db.get(
      `
        SELECT
          id,
          bga_nickname,
          name,
          association,
          COALESCE(NULLIF(trim(status), ''), 'Active') AS status,
          email,
          COALESCE(master_title, 0) AS master_title,
          master_title_date,
          COALESCE(team_captain, 0) AS team_captain,
          telegram,
          whatsapp,
          discord,
          instagram,
          contact_email
        FROM profiles
        WHERE id = ?
          AND deleted_at IS NULL
        LIMIT 1
      `,
      [requestedPlayerId],
      (beforeErr, beforeRow) => {
        if (beforeErr) {
          return res.status(500).json({ ok: false, message: "Failed to load profile before update" });
        }
        if (!beforeRow) {
          return res.status(404).json({ ok: false, message: "Profile not found" });
        }

        return db.run(sql, params, function onUpdate(updateErr) {
          if (updateErr) {
            return res.status(500).json({ ok: false, message: "Failed to update profile" });
          }
          if (!this || this.changes === 0) {
            return res.status(404).json({ ok: false, message: "Profile not found" });
          }

          return db.get(
            `
              SELECT
                id,
                bga_nickname,
                name,
                association,
                COALESCE(NULLIF(trim(status), ''), 'Active') AS status,
                email,
                COALESCE(master_title, 0) AS master_title,
                master_title_date,
                COALESCE(team_captain, 0) AS team_captain,
                telegram,
                whatsapp,
                discord,
                instagram,
                contact_email
              FROM profiles
              WHERE id = ?
                AND deleted_at IS NULL
              LIMIT 1
            `,
            [requestedPlayerId],
            (selectErr, row) => {
              if (selectErr) {
                return res.status(500).json({ ok: false, message: "Failed to load updated profile" });
              }
              const changes = buildAuditChanges(beforeRow || {}, row || {}, PROFILE_AUDIT_FIELDS);
              return syncUsersBgaIdForProfile(
                requestedPlayerId,
                row?.email,
                {
                  actor: getAuditActor(req.user),
                  source: "profile_email_change",
                },
                (syncErr) => {
                  if (syncErr) {
                    return res.status(500).json({ ok: false, message: "Profile updated, but failed to sync linked user" });
                  }
                  if (!Object.keys(changes).length) {
                    return res.json({ ok: true, profile: row || null });
                  }
                  return logAuditEvent(
                    {
                      ...getAuditActor(req.user),
                      event_type: "profile.updated",
                      entity_type: "profile",
                      action: "update",
                      record_id: requestedPlayerId,
                      changes,
                    },
                    () => res.json({ ok: true, profile: row || null })
                  );
                }
              );
            }
          );
        });
      }
    );

    const nextEmail = typeof allowedPatch.email === "string" ? String(allowedPatch.email || "").trim() : null;
    if (!nextEmail) {
      return runUpdate();
    }

    return db.get(
      `
        SELECT id
        FROM profiles
        WHERE lower(COALESCE(email, '')) = lower(?)
          AND id <> ?
          AND deleted_at IS NULL
        LIMIT 1
      `,
      [nextEmail, requestedPlayerId],
      (dupEmailErr, dupEmailRow) => {
        if (dupEmailErr) {
          return res.status(500).json({ ok: false, message: "Failed to validate email uniqueness" });
        }
        if (dupEmailRow) {
          return res.status(409).json({ ok: false, message: "Login Email is already used by another profile" });
        }
        return runUpdate();
      }
    );
  };

  if (isTeamCaptain && userAssociation && !isAdmin) {
    if (ownerCanEdit) {
      return decideAndUpdate(true);
    }
    return db.get(
      "SELECT association FROM profiles WHERE id = ? AND deleted_at IS NULL LIMIT 1",
      [requestedPlayerId],
      (targetErr, targetRow) => {
        if (targetErr) {
          return res.status(500).json({ ok: false, message: "Failed to validate access" });
        }
        const targetAssociation = String(targetRow?.association || "").trim().toLowerCase();
        const captainCanEdit = targetAssociation !== "" && targetAssociation === userAssociation;
        return decideAndUpdate(captainCanEdit);
      }
    );
  }

  return decideAndUpdate(false);
});

app.post("/profiles/:playerId/get-bga-data", requireAdmin, async (req, res) => {
  const requestedPlayerId = String(req.params.playerId || "").trim();
  if (!requestedPlayerId) {
    return res.status(400).json({ ok: false, message: "playerId is required" });
  }

  const actor = getAuditActor(req.user);
  const actorPlayerId = String(req.user?.player_id || "").trim() || null;
  const authServerRoot = path.resolve(__dirname, "..");
  const updateScriptPath = path.resolve(authServerRoot, "run_update_profile_bga_data.py");
  const pythonBin = String(process.env.PYTHON_BIN || "python3").trim() || "python3";

  const parseScriptJson = (raw) => {
    const text = String(raw || "").trim();
    if (!text) return null;
    try {
      return JSON.parse(text);
    } catch (_error) {
      return null;
    }
  };

  try {
    const beforeRow = await dbGetAsync(
      `
        SELECT
          id,
          bga_nickname,
          avatar,
          name,
          association,
          COALESCE(NULLIF(trim(status), ''), 'Active') AS status,
          email,
          COALESCE(master_title, 0) AS master_title,
          master_title_date,
          COALESCE(team_captain, 0) AS team_captain,
          telegram,
          whatsapp,
          discord,
          instagram,
          contact_email
        FROM profiles
        WHERE id = ?
          AND deleted_at IS NULL
        LIMIT 1
      `,
      [requestedPlayerId]
    );
    if (!beforeRow) {
      return res.status(404).json({ ok: false, message: "Profile not found" });
    }

    let scriptPayload = null;
    try {
      const { stdout } = await execFileAsync(
        pythonBin,
        [updateScriptPath, "--db-path", dbFullPath, "--player-id", requestedPlayerId],
        { cwd: authServerRoot }
      );
      scriptPayload = parseScriptJson(stdout);
    } catch (error) {
      scriptPayload = parseScriptJson(error?.stdout);
      const message = scriptPayload?.message
        || String(error?.stderr || "").trim()
        || "Failed to get BGA data";
      return res.status(500).json({ ok: false, message });
    }

    const afterRow = await dbGetAsync(
      `
        SELECT
          id,
          bga_nickname,
          avatar,
          name,
          association,
          COALESCE(NULLIF(trim(status), ''), 'Active') AS status,
          email,
          COALESCE(master_title, 0) AS master_title,
          master_title_date,
          COALESCE(team_captain, 0) AS team_captain,
          telegram,
          whatsapp,
          discord,
          instagram,
          contact_email
        FROM profiles
        WHERE id = ?
          AND deleted_at IS NULL
        LIMIT 1
      `,
      [requestedPlayerId]
    );
    if (!afterRow) {
      return res.status(404).json({ ok: false, message: "Profile not found after update" });
    }

    const changes = buildAuditChanges(beforeRow || {}, afterRow || {}, PROFILE_AUDIT_FIELDS);
    const responsePayload = {
      ok: true,
      message: scriptPayload?.updated
        ? "BGA data updated."
        : "BGA data is already up to date.",
      profile: {
        id: afterRow.id,
        bga_nickname: afterRow.bga_nickname || null,
        avatar: afterRow.avatar || null,
      },
      result: scriptPayload || null,
    };

    if (!Object.keys(changes).length) {
      return res.json(responsePayload);
    }

    return logAuditEvent(
      {
        ...actor,
        actor_player_id: actorPlayerId,
        event_type: "profile.updated",
        entity_type: "profile",
        action: "update",
        record_id: requestedPlayerId,
        changes,
        metadata: {
          source: "bga_get_data",
          matched_player_id: scriptPayload?.matched_player_id ?? null,
          source_url: scriptPayload?.source_url ?? null,
        },
      },
      () => res.json(responsePayload)
    );
  } catch (error) {
    console.error("Failed to get BGA profile data", error);
    return res.status(500).json({ ok: false, message: "Failed to get BGA data" });
  }
});

app.get("/profiles/:playerId/delete-check", (req, res) => {
  if (!req.user) {
    return res.status(401).json({ ok: false, message: "Unauthorized" });
  }

  const requestedPlayerId = String(req.params.playerId || "").trim();
  if (!requestedPlayerId) {
    return res.status(400).json({ ok: false, message: "playerId is required" });
  }

  const actorPlayerId = String(req.user.player_id || "").trim() || null;
  const isAdmin = Number(req.user.admin) === 1;

  return db.get(
    `
      SELECT
        id,
        created_by,
        bga_nickname,
        name,
        association,
        COALESCE(NULLIF(trim(status), ''), 'Active') AS status,
        email,
        COALESCE(master_title, 0) AS master_title,
        master_title_date,
        COALESCE(team_captain, 0) AS team_captain,
        telegram,
        whatsapp,
        discord,
        instagram,
        contact_email
      FROM profiles
      WHERE id = ?
        AND deleted_at IS NULL
      LIMIT 1
    `,
    [requestedPlayerId],
    (profileErr, profileRow) => {
      if (profileErr) {
        return res.status(500).json({ ok: false, message: "Failed to load profile" });
      }
      if (!profileRow) {
        return res.status(404).json({ ok: false, message: "Profile not found" });
      }

      const createdBy = String(profileRow.created_by || "").trim();
      if (!isAdmin && (!actorPlayerId || createdBy !== actorPlayerId)) {
        return res.status(403).json({ ok: false, message: "Forbidden" });
      }

      return db.get(
        `
          SELECT id
          FROM duels
          WHERE deleted_at IS NULL
            AND (player_1_id = ? OR player_2_id = ?)
          LIMIT 1
        `,
        [requestedPlayerId, requestedPlayerId],
        (lineupErr, lineupRow) => {
          if (lineupErr) {
            return res.status(500).json({ ok: false, message: "Failed to validate duels" });
          }
          if (lineupRow) {
            return res.json({
              ok: true,
              can_delete: false,
              message: "This player is already assigned to one or more match duels and cannot be deleted.",
            });
          }
          return res.json({ ok: true, can_delete: true });
        }
      );
    }
  );
});

app.delete("/profiles/:playerId", (req, res) => {
  if (!req.user) {
    return res.status(401).json({ ok: false, message: "Unauthorized" });
  }

  const requestedPlayerId = String(req.params.playerId || "").trim();
  if (!requestedPlayerId) {
    return res.status(400).json({ ok: false, message: "playerId is required" });
  }

  const actorPlayerId = String(req.user.player_id || "").trim() || null;
  const isAdmin = Number(req.user.admin) === 1;

  return db.get(
    `
      SELECT
        id,
        created_by,
        bga_nickname,
        association,
        COALESCE(NULLIF(trim(status), ''), 'Active') AS status,
        name,
        email,
        COALESCE(master_title, 0) AS master_title,
        master_title_date,
        COALESCE(team_captain, 0) AS team_captain,
        telegram,
        whatsapp,
        discord,
        instagram,
        contact_email
      FROM profiles
      WHERE id = ?
        AND deleted_at IS NULL
      LIMIT 1
    `,
    [requestedPlayerId],
    (profileErr, profileRow) => {
      if (profileErr) {
        return res.status(500).json({ ok: false, message: "Failed to load profile" });
      }
      if (!profileRow) {
        return res.status(404).json({ ok: false, message: "Profile not found" });
      }

      const createdBy = String(profileRow.created_by || "").trim();
      if (!isAdmin && (!actorPlayerId || createdBy !== actorPlayerId)) {
        return res.status(403).json({ ok: false, message: "Forbidden" });
      }

      return db.get(
        `
          SELECT id
          FROM duels
          WHERE deleted_at IS NULL
            AND (player_1_id = ? OR player_2_id = ?)
          LIMIT 1
        `,
        [requestedPlayerId, requestedPlayerId],
        (lineupErr, lineupRow) => {
          if (lineupErr) {
            return res.status(500).json({ ok: false, message: "Failed to validate duels" });
          }
          if (lineupRow) {
            return res.status(409).json({
              ok: false,
              message: "This player is already assigned to one or more match duels and cannot be deleted.",
            });
          }

          return db.run(
            `
              UPDATE profiles
              SET
                deleted_at = CURRENT_TIMESTAMP,
                deleted_by = ?,
                updated_by = ?,
                updated_at = CURRENT_TIMESTAMP
              WHERE id = ?
                AND deleted_at IS NULL
            `,
            [actorPlayerId, actorPlayerId, requestedPlayerId],
            function onDelete(deleteErr) {
              if (deleteErr) {
                return res.status(500).json({ ok: false, message: "Failed to delete profile" });
              }
              if (!this || this.changes === 0) {
                return res.status(404).json({ ok: false, message: "Profile not found" });
              }
              return logAuditEvent(
                {
                  ...getAuditActor(req.user),
                  event_type: "profile.deleted",
                  entity_type: "profile",
                  action: "delete",
                  record_id: requestedPlayerId,
                  changes: buildAuditDeletionChanges(profileRow || {}, PROFILE_AUDIT_FIELDS),
                },
                () => res.json({ ok: true })
              );
            }
          );
        }
      );
    }
  );
});

app.post("/auth/logout", (req, res, next) => {
  req.logout((logoutErr) => {
    if (logoutErr) return next(logoutErr);
    req.session.destroy((destroyErr) => {
      if (destroyErr) return next(destroyErr);
      res.clearCookie("connect.sid");
      return res.json({ ok: true });
    });
  });
});

app.listen(PORT, () => {
  console.log(`Auth server running on port ${PORT}`);
});
