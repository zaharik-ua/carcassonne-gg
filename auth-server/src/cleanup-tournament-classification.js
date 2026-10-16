import path from "node:path";
import { fileURLToPath } from "node:url";
import { readFile, writeFile } from "node:fs/promises";
import { parseArgs } from "node:util";
import dotenv from "dotenv";
import { openClassificationDatabase } from "./tournament-classification-migration.js";
import { applyTournamentCleanupPlan, buildTournamentCleanupPlan } from "./tournament-classification-cleanup.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
dotenv.config({ path: path.join(root, ".env") });
let db;
try {
  const { values } = parseArgs({ options: {
    "db-path": { type: "string" }, "backup-dir": { type: "string" },
    report: { type: "string" }, plan: { type: "string" },
    apply: { type: "boolean", default: false }, help: { type: "boolean", default: false },
  } });
  if (values.help) {
    console.log("Dry-run: node src/cleanup-tournament-classification.js --db-path PATH --report REPORT.json\nApply:   node src/cleanup-tournament-classification.js --db-path PATH --apply --plan REPORT.json --backup-dir DIR");
  } else {
    if (values.apply !== !!values.plan) throw new Error("--apply requires --plan; dry-run uses --report");
    const filename = path.resolve(root, values["db-path"] || process.env.DB_PATH || process.env.AUTH_SQLITE_PATH || "data/auth.sqlite");
    db = await openClassificationDatabase(filename, { readOnly: !values.apply });
    if (values.apply) {
      const plan = JSON.parse(await readFile(path.resolve(values.plan), "utf8"));
      console.log(JSON.stringify({ ok: true, db_path: filename, ...await applyTournamentCleanupPlan(db, plan, {
        backupDir: values["backup-dir"] ? path.resolve(values["backup-dir"]) : undefined,
      }) }, null, 2));
    } else {
      const plan = await buildTournamentCleanupPlan(db);
      if (values.report) await writeFile(path.resolve(values.report), `${JSON.stringify(plan, null, 2)}\n`, { mode: 0o600 });
      console.log(JSON.stringify(plan, null, 2));
      if (plan.issues.length || plan.summary.blocked) process.exitCode = 2;
    }
  }
} catch (error) {
  console.error(JSON.stringify({ ok: false, code: error.code || "CLEANUP_FAILED", message: error.message }));
  process.exitCode = 1;
} finally {
  if (db) await new Promise((resolve) => db.close(resolve));
}
