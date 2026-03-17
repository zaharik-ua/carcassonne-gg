import path from "node:path";
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
const FRONTEND_ORIGINS = (process.env.FRONTEND_ORIGIN || "http://localhost:8080")
  .split(",")
  .map((origin) => origin.trim())
  .filter(Boolean);
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

function ensureProfilesSchema() {
  db.all("PRAGMA table_info(profiles)", (pragmaErr, columns) => {
    if (pragmaErr) {
      console.error("Failed to inspect profiles schema", pragmaErr);
      return;
    }

    const addOrBackfillProfilesColumns = (currentColumns) => {
      addColumnIfMissing(currentColumns, "profiles", "id", "TEXT");
      addColumnIfMissing(currentColumns, "profiles", "admin", "INTEGER NOT NULL DEFAULT 0");
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
    };

    const hasLegacyPlayerId = columns.some((col) => col.name === "player_id");
    const hasIdColumn = columns.some((col) => col.name === "id");
    const hasProfileRowId = columns.some((col) => col.name === "profile_row_id");
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

    if (hasIdColumn && idIsPrimaryKey && !hasProfileRowId) {
      db.run("ALTER TABLE profiles RENAME COLUMN id TO profile_row_id", (renamePkErr) => {
        if (renamePkErr) {
          console.error("Failed to rename legacy profiles.id PK to profile_row_id", renamePkErr);
          addOrBackfillProfilesColumns(columns);
          return;
        }
        renameLegacyPlayerId();
      });
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
  });
}

function ensureLineupsSchema() {
  db.run(`
    CREATE TABLE IF NOT EXISTS lineups (
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
      status TEXT,
      created_by TEXT,
      updated_by TEXT,
      deleted_by TEXT,
      deleted_at TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
  `, (createErr) => {
    if (createErr) {
      console.error("Failed to ensure lineups schema", createErr);
      return;
    }
    db.all("PRAGMA table_info(lineups)", (pragmaErr, columns) => {
      if (pragmaErr) {
        console.error("Failed to inspect lineups schema", pragmaErr);
        return;
      }
      if (!Array.isArray(columns) || columns.length === 0) return;
      addColumnIfMissing(columns, "lineups", "duel_number", "INTEGER");
      addColumnIfMissing(columns, "lineups", "created_by", "TEXT");
      addColumnIfMissing(columns, "lineups", "updated_by", "TEXT");
      addColumnIfMissing(columns, "lineups", "deleted_by", "TEXT");
      addColumnIfMissing(columns, "lineups", "deleted_at", "TEXT");
    });
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
              profile_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
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
              id TEXT,
              admin INTEGER NOT NULL DEFAULT 0,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
          `);

          ensureProfilesSchema();

          ensureAssociationsSchema();
          ensureMatchesSchema();
          ensureLineupsSchema();
          ensureTeamsSchema();
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
        p.id AS player_id,
        COALESCE(p.admin, 0) AS admin,
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

      db.get(
        "SELECT id, bga_id FROM users WHERE google_id = ? LIMIT 1",
        [googleId],
        (lookupErr, existingUserRow) => {
          if (lookupErr) return done(lookupErr);

          db.run(
            `
              INSERT INTO users (google_id, email, name, picture, last_login)
              VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
              ON CONFLICT(google_id)
              DO UPDATE SET
                email = excluded.email,
                name = excluded.name,
                picture = excluded.picture,
                last_login = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            `,
            [googleId, email, name, picture],
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
  if (!isAdmin) {
    if (!isTeamCaptain || !userAssociation) {
      return res.status(403).json({ ok: false, message: "Forbidden" });
    }
    if (association !== userAssociation) {
      return res.status(403).json({ ok: false, message: "Captain can create profiles only in own association" });
    }
  }

  return db.get(
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

app.get("/tournaments", (_req, res, next) => {
  db.all(
    `
      SELECT
        id,
        name,
        short_title,
        logo,
        link
      FROM tournaments
      ORDER BY id COLLATE NOCASE ASC
    `,
    (err, rows) => {
      if (err) return next(err);
      return res.json({ ok: true, tournaments: rows || [] });
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

app.get("/audit-trail", requireAdmin, (req, res, next) => {
  const allowedPageSizes = new Set([10, 20, 50]);
  const parsedLimit = Number.parseInt(String(req.query.limit || "10"), 10);
  const parsedPage = Number.parseInt(String(req.query.page || "1"), 10);
  const limit = allowedPageSizes.has(parsedLimit) ? parsedLimit : 10;
  const page = Number.isInteger(parsedPage) && parsedPage > 0 ? parsedPage : 1;
  const offset = (page - 1) * limit;

  return db.get(
    "SELECT COUNT(*) AS total FROM audit_trail",
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
          ORDER BY datetime(created_at) DESC, id DESC
          LIMIT ?
          OFFSET ?
        `,
        [limit, total > 0 ? safeOffset : offset],
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

app.get("/lineups", (req, res, next) => {
  const matchId = String(req.query.match_id || "").trim();
  if (!matchId) {
    return res.status(400).json({ ok: false, message: "match_id is required" });
  }

  db.all(
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
      FROM lineups
      WHERE match_id = ?
        AND deleted_at IS NULL
      ORDER BY
        CASE WHEN duel_number IS NULL THEN 1 ELSE 0 END ASC,
        duel_number ASC,
        id ASC
    `,
    [matchId],
    (err, rows) => {
      if (err) return next(err);
      return res.json({ ok: true, lineups: rows || [] });
    }
  );
});

app.post("/lineups/bulk-upsert", (req, res) => {
  if (!req.user) {
    return res.status(401).json({ ok: false, message: "Unauthorized" });
  }

  const payload = req.body && typeof req.body === "object" ? req.body : {};
  const matchId = String(payload.match_id || "").trim();
  const lineups = Array.isArray(payload.lineups) ? payload.lineups : [];
  if (!matchId) {
    return res.status(400).json({ ok: false, message: "match_id is required" });
  }

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

  return db.get(
    "SELECT team_1, team_2 FROM matches WHERE id = ? AND deleted_at IS NULL LIMIT 1",
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
      const canEdit = isAdmin || (isTeamCaptain && userAssociation && (userAssociation === team1 || userAssociation === team2));
      if (!canEdit) {
        return res.status(403).json({ ok: false, message: "Forbidden" });
      }

      const sanitized = [];
      for (let index = 0; index < lineups.length; index += 1) {
        const item = lineups[index];
        const id = String(item?.id || "").trim();
        const player1 = normalizeText(item?.player_1_id);
        const player2 = normalizeText(item?.player_2_id);
        const duelNumberRaw = toIntOrNull(item?.duel_number);
        const duelNumber = Number.isInteger(duelNumberRaw) && duelNumberRaw > 0
          ? duelNumberRaw
          : index + 1;
        if (!id || (!player1 && !player2)) {
          return res.status(400).json({ ok: false, message: "Each lineup requires id and at least one player" });
        }
        sanitized.push({
          id,
          tournament_id: normalizeText(item?.tournament_id),
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
          FROM lineups
          WHERE match_id = ?
            AND deleted_at IS NULL
          ORDER BY
            CASE WHEN duel_number IS NULL THEN 1 ELSE 0 END ASC,
            duel_number ASC,
            id ASC
        `,
        [matchId],
        (beforeErr, previousLineups) => {
          if (beforeErr) {
            return res.status(500).json({ ok: false, message: "Failed to load existing lineups" });
          }

          return db.serialize(() => {
            db.run("BEGIN IMMEDIATE TRANSACTION");
            db.run(
              `
                UPDATE lineups
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
                  return res.status(500).json({ ok: false, message: "Failed to clear old lineups" });
                }
                if (!sanitized.length) {
                  return db.run("COMMIT", (commitErr) => {
                    if (commitErr) return res.status(500).json({ ok: false, message: "Failed to save lineups" });

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
                        metadata: { match_id: matchId, lineups_count: 0 },
                      },
                      () => res.json({ ok: true, lineups: [] })
                    );
                  });
                }

                const stmt = db.prepare(`
                INSERT INTO lineups (
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
                          return res.status(500).json({ ok: false, message: "Failed to insert lineups" });
                        });
                        return;
                      }
                      pending -= 1;
                      if (pending === 0) {
                        stmt.finalize(() => {
                          db.run("COMMIT", (commitErr) => {
                            if (commitErr) return res.status(500).json({ ok: false, message: "Failed to save lineups" });

                            const action = previousLineups.length ? "update" : "create";
                            const eventType = previousLineups.length ? "lineups.updated" : "lineups.created";
                            const changes = previousLineups.length
                              ? buildAuditChanges({ lineups: previousLineups || [] }, { lineups: sanitized || [] }, ["lineups"])
                              : buildAuditCreationChanges({ lineups: sanitized || [] }, ["lineups"]);

                            return logAuditEvent(
                              {
                                ...getAuditActor(req.user),
                                event_type: eventType,
                                entity_type: "lineups",
                                action,
                                record_id: matchId,
                                changes,
                                metadata: { match_id: matchId, lineups_count: sanitized.length },
                              },
                              () => res.json({ ok: true, lineups: sanitized })
                            );
                          });
                        });
                      }
                    }
                  );
                });
              }
            );
          });
        }
      );
    }
  );
});

app.get("/matches", (_req, res, next) => {
  db.all(
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
        gw2
      FROM matches
      WHERE deleted_at IS NULL
      ORDER BY time_utc DESC, id ASC
    `,
    (err, rows) => {
      if (err) return next(err);
      return res.json({ ok: true, matches: rows || [] });
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
  if (!isAdmin && !isTeamCaptain) {
    return res.status(403).json({ ok: false, message: "Forbidden" });
  }

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
  if (!isAdmin && isTeamCaptain && (!userAssociation || team1 !== userAssociation)) {
    return res.status(403).json({ ok: false, message: "Captain can create matches only for own team as team_1" });
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
                      gw2
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
                gw2
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
  if (!isAdmin && !isTeamCaptain) {
    return res.status(403).json({ ok: false, message: "Forbidden" });
  }

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
        gw2
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

      const existingTeam1 = normalizeCode(existingRow.team_1);
      const existingTeam2 = normalizeCode(existingRow.team_2);
      const captainCanEdit = !isAdmin
        && isTeamCaptain
        && userAssociation
        && (existingTeam1 === userAssociation || existingTeam2 === userAssociation);
      if (!isAdmin && !captainCanEdit) {
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
                gw2
              FROM matches
              WHERE id = ?
              LIMIT 1
            `,
            [nextMatchId],
            (selectErr, row) => {
              if (selectErr) {
                return res.status(500).json({ ok: false, message: "Failed to load updated match" });
              }
              return logAuditEvent(
                {
                  ...getAuditActor(req.user),
                  event_type: "match.updated",
                  entity_type: "match",
                  action: "update",
                  record_id: nextMatchId,
                  changes: buildAuditChanges(existingRow || {}, row || {}, MATCH_AUDIT_FIELDS),
                  metadata: { previous_record_id: matchId !== nextMatchId ? matchId : null },
                },
                () => res.json({ ok: true, match: row || null })
              );
            }
          );
        }
      );
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
  if (!isAdmin && !isTeamCaptain) {
    return res.status(403).json({ ok: false, message: "Forbidden" });
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
        gw2
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

      const team1 = String(row.team_1 || "").trim().toUpperCase();
      const team2 = String(row.team_2 || "").trim().toUpperCase();
      const captainCanDelete = !isAdmin
        && isTeamCaptain
        && userAssociation
        && (team1 === userAssociation || team2 === userAssociation);
      if (!isAdmin && !captainCanDelete) {
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
          FROM lineups
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
            return res.status(500).json({ ok: false, message: "Failed to load match lineups" });
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
                    UPDATE lineups
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
                      return res.status(500).json({ ok: false, message: "Failed to delete match lineups" });
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
                              metadata: { match_id: matchId, lineups_count: activeLineups.length },
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
        }
      );
    }
  );
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
          FROM lineups
          WHERE deleted_at IS NULL
            AND (player_1_id = ? OR player_2_id = ?)
          LIMIT 1
        `,
        [requestedPlayerId, requestedPlayerId],
        (lineupErr, lineupRow) => {
          if (lineupErr) {
            return res.status(500).json({ ok: false, message: "Failed to validate lineups" });
          }
          if (lineupRow) {
            return res.json({
              ok: true,
              can_delete: false,
              message: "This player is already assigned to one or more match lineups and cannot be deleted.",
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
      SELECT id, created_by
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
          FROM lineups
          WHERE deleted_at IS NULL
            AND (player_1_id = ? OR player_2_id = ?)
          LIMIT 1
        `,
        [requestedPlayerId, requestedPlayerId],
        (lineupErr, lineupRow) => {
          if (lineupErr) {
            return res.status(500).json({ ok: false, message: "Failed to validate lineups" });
          }
          if (lineupRow) {
            return res.status(409).json({
              ok: false,
              message: "This player is already assigned to one or more match lineups and cannot be deleted.",
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
