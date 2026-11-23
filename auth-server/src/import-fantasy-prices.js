import path from "node:path";
import { fileURLToPath } from "node:url";
import { readFile, writeFile } from "node:fs/promises";
import { parseArgs } from "node:util";
import dotenv from "dotenv";
import sqlite3 from "sqlite3";

import { ensureFantasySchema } from "./fantasy.js";
import { applyFantasyPricePlan, buildFantasyPricePlan, parseCsv, readPriceRows } from "./fantasy-import.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
dotenv.config({ path: path.join(root, ".env") });

const USAGE = `Dry-run: node src/import-fantasy-prices.js --csv PRICES.csv [--league CCL-2027-Q] [--db-path PATH]
                                       [--effective-from 2026-11-24T00:00:00Z] [--report REPORT.json]
Apply:   node src/import-fantasy-prices.js [--db-path PATH] --apply --plan REPORT.json [--backup-dir DIR]

--effective-from: when the prices start to apply (default: now). Rows whose price did not change are skipped.`;

function openDatabase(filename) {
  return new Promise((resolve, reject) => {
    const db = new sqlite3.Database(filename, sqlite3.OPEN_READWRITE, (error) => (error ? reject(error) : resolve(db)));
  });
}

let db;
try {
  const { values } = parseArgs({ options: {
    "db-path": { type: "string" }, "csv": { type: "string" }, "league": { type: "string", default: "CCL-2027-Q" },
    "effective-from": { type: "string" }, "report": { type: "string" }, "plan": { type: "string" },
    "backup-dir": { type: "string" }, "apply": { type: "boolean", default: false }, "help": { type: "boolean", default: false },
  } });
  if (values.help) {
    console.log(USAGE);
  } else {
    if (values.apply && !values.plan) throw new Error("--apply requires a reviewed --plan report");
    if (!values.apply && values.plan) throw new Error("--plan is used with --apply; generate a new report with --report");
    if (!values.apply && !values.csv) throw new Error("--csv is required for a dry-run");
    const filename = path.resolve(root, values["db-path"] || process.env.DB_PATH || "data/auth.sqlite");
    // The dry-run needs the league and price tables; creating them is harmless (the server does the same).
    db = await openDatabase(filename);
    // The running server holds its own connection; wait for its writes instead of failing.
    await new Promise((resolve, reject) => db.run("PRAGMA busy_timeout = 10000", (error) => (error ? reject(error) : resolve())));
    await ensureFantasySchema(db);
    if (values.apply) {
      const plan = JSON.parse(await readFile(path.resolve(values.plan), "utf8"));
      const result = await applyFantasyPricePlan(db, plan, {
        backupDir: values["backup-dir"] ? path.resolve(values["backup-dir"]) : undefined,
        databasePath: filename,
      });
      console.log(JSON.stringify({ ok: true, db_path: filename, ...result }, null, 2));
    } else {
      const csvPath = path.resolve(values.csv);
      const rows = readPriceRows(parseCsv(await readFile(csvPath, "utf8")));
      const plan = await buildFantasyPricePlan(db, {
        rows,
        leagueId: values.league,
        effectiveFrom: values["effective-from"] || new Date().toISOString(),
        source: path.basename(csvPath),
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
