import path from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import dotenv from "dotenv";
import sqlite3 from "sqlite3";
import { updatePlannedTournamentGgRatings } from "./planned-tournament-gg-ratings.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
dotenv.config({ path: path.join(root, ".env") });
const { values } = parseArgs({ options: {
  "db-path": { type: "string" },
  "dry-run": { type: "boolean", default: false },
} });
const dbPath = path.resolve(root, values["db-path"] || process.env.DB_PATH || process.env.AUTH_SQLITE_PATH || "data/auth.sqlite");
let db;
try {
  // Refuse to create an empty database if the path is wrong.
  db = await new Promise((resolve, reject) => {
    const connection = new sqlite3.Database(dbPath, sqlite3.OPEN_READWRITE,
      (error) => error ? reject(error) : resolve(connection));
  });
  db.configure("busyTimeout", 5000);
  const result = await updatePlannedTournamentGgRatings(db, { dryRun: values["dry-run"] });
  console.log(JSON.stringify({ ok: true, db_path: dbPath, ...result }, null, 2));
} catch (error) {
  console.error("Failed to update planned tournament GG ratings:", error.message);
  process.exitCode = 1;
} finally {
  if (db) await new Promise((resolve) => db.close(resolve));
}
