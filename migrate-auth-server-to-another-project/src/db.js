import fs from "node:fs";
import path from "node:path";
import sqlite3 from "sqlite3";
import { config } from "./config.js";

fs.mkdirSync(path.dirname(config.DB_PATH), { recursive: true });

export const db = new sqlite3.Database(config.DB_PATH);

export function dbGet(sql, params = []) {
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

export function dbAll(sql, params = []) {
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

export function dbRun(sql, params = []) {
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

function quoteSqlIdentifier(identifier) {
  return `"${String(identifier || "").replaceAll('"', '""')}"`;
}

export async function addColumnIfMissing(tableName, columnName, sqlDefinition) {
  const columns = await dbAll(`PRAGMA table_info(${quoteSqlIdentifier(tableName)})`);
  if (columns.some((column) => column.name === columnName)) return;
  await dbRun(
    `ALTER TABLE ${quoteSqlIdentifier(tableName)} ADD COLUMN ${quoteSqlIdentifier(columnName)} ${sqlDefinition}`
  );
}

export async function ensureCoreSchema() {
  await dbRun(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      google_id TEXT NOT NULL UNIQUE,
      email TEXT,
      name TEXT,
      picture TEXT,
      admin INTEGER NOT NULL DEFAULT 0,
      last_login TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
  `);

  await addColumnIfMissing("users", "email", "TEXT");
  await addColumnIfMissing("users", "name", "TEXT");
  await addColumnIfMissing("users", "picture", "TEXT");
  await addColumnIfMissing("users", "admin", "INTEGER NOT NULL DEFAULT 0");
  await addColumnIfMissing("users", "last_login", "TEXT");
  await addColumnIfMissing("users", "created_at", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP");
  await addColumnIfMissing("users", "updated_at", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP");

  await dbRun(`
    CREATE TABLE IF NOT EXISTS profiles (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL UNIQUE,
      display_name TEXT,
      avatar_url TEXT,
      bio TEXT,
      location TEXT,
      website_url TEXT,
      metadata_json TEXT NOT NULL DEFAULT '{}',
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    )
  `);

  await addColumnIfMissing("profiles", "display_name", "TEXT");
  await addColumnIfMissing("profiles", "avatar_url", "TEXT");
  await addColumnIfMissing("profiles", "bio", "TEXT");
  await addColumnIfMissing("profiles", "location", "TEXT");
  await addColumnIfMissing("profiles", "website_url", "TEXT");
  await addColumnIfMissing("profiles", "metadata_json", "TEXT NOT NULL DEFAULT '{}'");
  await addColumnIfMissing("profiles", "created_at", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP");
  await addColumnIfMissing("profiles", "updated_at", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP");

  await dbRun("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_google_id ON users(google_id)");
  await dbRun("CREATE UNIQUE INDEX IF NOT EXISTS idx_profiles_user_id ON profiles(user_id)");
}

export async function loadUserWithProfile(userId) {
  return dbGet(
    `
      SELECT
        u.id,
        u.google_id,
        u.email,
        u.name,
        u.picture,
        COALESCE(u.admin, 0) AS admin,
        u.last_login,
        p.id AS profile_id,
        p.display_name,
        p.avatar_url,
        p.bio,
        p.location,
        p.website_url,
        p.metadata_json
      FROM users u
      LEFT JOIN profiles p
        ON p.user_id = u.id
      WHERE u.id = ?
      LIMIT 1
    `,
    [userId]
  );
}
