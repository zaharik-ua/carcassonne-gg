import path from "node:path";
import { fileURLToPath } from "node:url";
import { readFile, writeFile } from "node:fs/promises";
import { parseArgs } from "node:util";
import dotenv from "dotenv";
import sqlite3 from "sqlite3";

import { applyCclImportPlan, buildCclImportPlan, parseCclAssociationMap, readCclSourceRows } from "./ccl-import.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
dotenv.config({ path: path.join(root, ".env") });

const DEFAULT_SOURCE = "https://zaharik-ua.github.io/carcassonne-gg/json-data/tournaments-open.json";
const USAGE = `Dry-run: node src/import-ccl-slots.js [--db-path PATH] [--source FILE_OR_URL] [--source-tournament CCL-2027Q]
                                     [--tournament CCL-2027] [--map "Country=CODE" ...] [--report REPORT.json]
Apply:   node src/import-ccl-slots.js [--db-path PATH] --apply --plan REPORT.json [--backup-dir DIR]`;

function openDatabase(filename, { readOnly }) {
  return new Promise((resolve, reject) => {
    const mode = readOnly ? sqlite3.OPEN_READONLY : sqlite3.OPEN_READWRITE;
    const db = new sqlite3.Database(filename, mode, (error) => (error ? reject(error) : resolve(db)));
  });
}

async function loadSource(source) {
  if (/^https?:\/\//i.test(source)) {
    const response = await fetch(source);
    if (!response.ok) throw new Error(`Failed to fetch ${source} (${response.status})`);
    return response.json();
  }
  return JSON.parse(await readFile(path.resolve(source), "utf8"));
}

let db;
try {
  const { values } = parseArgs({ options: {
    "db-path": { type: "string" }, "source": { type: "string" },
    "source-tournament": { type: "string", default: "CCL-2027Q" }, "tournament": { type: "string", default: "CCL-2027" },
    "map": { type: "string", multiple: true, default: [] }, "report": { type: "string" }, "plan": { type: "string" },
    "backup-dir": { type: "string" }, "apply": { type: "boolean", default: false }, "help": { type: "boolean", default: false },
  } });
  if (values.help) {
    console.log(USAGE);
  } else {
    if (values.apply && !values.plan) throw new Error("--apply requires a reviewed --plan report");
    if (!values.apply && values.plan) throw new Error("--plan is used with --apply; generate a new report with --report");
    const filename = path.resolve(root, values["db-path"] || process.env.DB_PATH || "data/auth.sqlite");
    db = await openDatabase(filename, { readOnly: !values.apply });
    // The running server holds its own connection; wait for its writes instead of failing.
    await new Promise((resolve, reject) => db.run("PRAGMA busy_timeout = 10000", (error) => (error ? reject(error) : resolve())));
    if (values.apply) {
      const plan = JSON.parse(await readFile(path.resolve(values.plan), "utf8"));
      const result = await applyCclImportPlan(db, plan, {
        backupDir: values["backup-dir"] ? path.resolve(values["backup-dir"]) : undefined,
      });
      console.log(JSON.stringify({ ok: true, db_path: filename, ...result }, null, 2));
    } else {
      const source = values.source || DEFAULT_SOURCE;
      const { rows, isoByCountry } = readCclSourceRows(await loadSource(source), values["source-tournament"]);
      const plan = await buildCclImportPlan(db, {
        rows,
        isoByCountry,
        associationMap: parseCclAssociationMap(values.map),
        tournamentId: values.tournament,
        sourceTournamentId: values["source-tournament"],
        source,
      });
      if (values.report) await writeFile(path.resolve(values.report), `${JSON.stringify(plan, null, 2)}\n`, { mode: 0o600 });
      console.log(JSON.stringify({ summary: plan.summary, issues: plan.issues, warnings: plan.warnings }, null, 2));
      if (!plan.summary.ready) process.exitCode = 2;
    }
  }
} catch (error) {
  console.error(JSON.stringify({ ok: false, code: error.code || "IMPORT_FAILED", message: error.message }));
  process.exitCode = 1;
} finally {
  if (db) await new Promise((resolve) => db.close(resolve));
}
