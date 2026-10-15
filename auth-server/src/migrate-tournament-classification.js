import path from "node:path";
import { fileURLToPath } from "node:url";
import { readFile, writeFile } from "node:fs/promises";
import { parseArgs } from "node:util";
import dotenv from "dotenv";
import { applyTournamentClassificationPlan, buildTournamentClassificationPlan, openClassificationDatabase } from "./tournament-classification-migration.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
dotenv.config({ path: path.join(root, ".env") });
let db;
try {
  const { values } = parseArgs({ options: {
    "db-path": { type: "string" }, "backup-dir": { type: "string" },
    "report": { type: "string" }, "plan": { type: "string" },
    "apply": { type: "boolean", default: false }, "rollback": { type: "boolean", default: false },
    "help": { type: "boolean", default: false },
  } });
  if (values.help) {
    console.log("Dry-run: node src/migrate-tournament-classification.js --db-path PATH [--rollback] [--report REPORT.json]\nApply:   node src/migrate-tournament-classification.js --db-path PATH [--rollback] --apply --plan REPORT.json [--backup-dir DIR]");
  } else {
    if (values.apply && !values.plan) throw new Error("--apply requires a reviewed --plan report");
    if (!values.apply && values.plan) throw new Error("--plan is used with --apply; generate a new report with --report");
    const filename = path.resolve(root, values["db-path"] || process.env.DB_PATH || process.env.AUTH_SQLITE_PATH || "data/auth.sqlite");
    db = await openClassificationDatabase(filename, { readOnly: !values.apply });
    if (values.apply) {
      const plan = JSON.parse(await readFile(path.resolve(values.plan), "utf8"));
      if (plan.direction !== (values.rollback ? "down" : "up")) throw new Error("Report direction does not match --rollback");
      const result = await applyTournamentClassificationPlan(db, plan, {
        backupDir: values["backup-dir"] ? path.resolve(values["backup-dir"]) : undefined,
      });
      console.log(JSON.stringify({ ok: true, db_path: filename, ...result }, null, 2));
    } else {
      const plan = await buildTournamentClassificationPlan(db, { rollback: values.rollback });
      if (values.report) await writeFile(path.resolve(values.report), `${JSON.stringify(plan, null, 2)}\n`, { mode: 0o600 });
      console.log(JSON.stringify(plan, null, 2));
      if (plan.issues.length || plan.summary.blocked) process.exitCode = 2;
    }
  }
} catch (error) {
  console.error(JSON.stringify({ ok: false, code: error.code || "MIGRATION_FAILED", message: error.message }));
  process.exitCode = 1;
} finally {
  if (db) await new Promise((resolve) => db.close(resolve));
}
