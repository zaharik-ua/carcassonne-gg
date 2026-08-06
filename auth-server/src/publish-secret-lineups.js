import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import sqlite3 from "sqlite3";
import {
  ensureSecretLineupsSchema,
  publishDueSecretLineups,
} from "./secret-lineups.js";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const authServerRoot = path.resolve(__dirname, "..");
const configuredDbPath = process.env.DB_PATH || process.env.AUTH_SQLITE_PATH || "./data/auth.sqlite";
const dbPath = path.isAbsolute(configuredDbPath)
  ? configuredDbPath
  : path.resolve(authServerRoot, configuredDbPath);
const db = new sqlite3.Database(dbPath);
db.configure("busyTimeout", 5000);

try {
  await ensureSecretLineupsSchema(db);
  const results = await publishDueSecretLineups(db);
  const publishedCount = results.filter((result) => result?.published).length;
  console.log(JSON.stringify({
    ok: true,
    checked: results.length,
    published: publishedCount,
  }));
} catch (error) {
  console.error("Failed to publish due secret lineups", error);
  process.exitCode = 1;
} finally {
  await new Promise((resolve) => db.close(() => resolve()));
}
