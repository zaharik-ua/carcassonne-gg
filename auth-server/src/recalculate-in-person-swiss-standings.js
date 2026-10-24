// One-off: rebuild stored Swiss standings snapshots of an in-person tournament
// after a match result was corrected directly in the database.
// Every existing revision is recomputed in place from the rounds up to its
// source round, so revision numbers (and playoff references to them) stay stable.
//
// Usage:
//   node src/recalculate-in-person-swiss-standings.js <tournament_id>          # dry run
//   node src/recalculate-in-person-swiss-standings.js <tournament_id> --apply  # write
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import sqlite3 from "sqlite3";
import { calculateSwissStandings } from "./in-person/engine.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
dotenv.config({ path: path.join(root, ".env") });

const args = process.argv.slice(2);
const tournamentId = args.find((arg) => !arg.startsWith("--"));
const apply = args.includes("--apply");
if (!tournamentId) {
  console.error("Usage: node src/recalculate-in-person-swiss-standings.js <tournament_id> [--apply]");
  process.exit(1);
}

const dbPath = path.resolve(root, process.env.DB_PATH || "./data/auth.sqlite");
const db = await new Promise((resolve, reject) => {
  const handle = new sqlite3.Database(dbPath, sqlite3.OPEN_READWRITE, (error) => {
    if (error) reject(error);
    else { handle.configure("busyTimeout", 5000); resolve(handle); }
  });
});
const all = (sql, params = []) => new Promise((resolve, reject) => {
  db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows)));
});
const run = (sql, params = []) => new Promise((resolve, reject) => {
  db.run(sql, params, function onRun(error) { error ? reject(error) : resolve(this); });
});

const STAT_FIELDS = [
  "position", "played", "wins", "buchholz", "solkoff1", "solkoff2",
  "vp_difference", "sonneborn_berger", "bye_count",
];

try {
  const [tournament] = await all("SELECT id, name_en FROM in_person_tournaments WHERE id = ?", [tournamentId]);
  if (!tournament) throw new Error(`Tournament ${tournamentId} not found`);

  const participants = await all(
    "SELECT id, name_en FROM in_person_participants WHERE tournament_id = ?",
    [tournamentId]
  );
  const nameById = new Map(participants.map((p) => [p.id, p.name_en]));

  const roundRows = await all(
    `SELECT * FROM in_person_rounds
     WHERE tournament_id = ? AND stage = 'swiss' AND status <> 'cancelled'
     ORDER BY round_number, id`,
    [tournamentId]
  );
  const rounds = [];
  for (const row of roundRows) {
    const matches = await all(
      `SELECT * FROM in_person_matches
       WHERE round_id = ? AND status <> 'cancelled'
       ORDER BY table_number, id`,
      [row.id]
    );
    rounds.push({
      ...row,
      round_number: Number(row.round_number),
      matches: matches.map((m) => ({ ...m, is_bye: Number(m.is_bye) === 1 })),
    });
  }
  const roundById = new Map(rounds.map((r) => [r.id, r]));

  const revisions = await all(
    `SELECT revision, source_completed_round_id, MIN(calculated_at) AS calculated_at
     FROM in_person_standings WHERE tournament_id = ?
     GROUP BY revision, source_completed_round_id ORDER BY revision`,
    [tournamentId]
  );

  console.log(`${tournament.name_en} (${tournament.id}) — ${apply ? "APPLY" : "DRY RUN"}`);
  await run("BEGIN IMMEDIATE TRANSACTION");
  let changedRevisions = 0;
  for (const rev of revisions) {
    const sourceRound = roundById.get(rev.source_completed_round_id);
    if (!sourceRound || sourceRound.status !== "completed") {
      console.log(`revision ${rev.revision}: source round not an active completed Swiss round, skipped`);
      continue;
    }
    const standings = calculateSwissStandings({
      participants: participants.map((p) => ({ id: p.id })),
      rounds: rounds.filter((r) => r.round_number <= sourceRound.round_number),
    });
    const stored = await all(
      "SELECT * FROM in_person_standings WHERE tournament_id = ? AND revision = ?",
      [tournamentId, rev.revision]
    );
    const storedById = new Map(stored.map((s) => [s.participant_id, s]));
    const diffs = standings.filter((s) => {
      const old = storedById.get(s.participant_id);
      return !old || STAT_FIELDS.some((field) => Number(old[field]) !== Number(s[field]));
    });
    console.log(
      `revision ${rev.revision} (after round ${sourceRound.round_number}): ${diffs.length} changed row(s)`
    );
    diffs.forEach((s) => {
      const old = storedById.get(s.participant_id) || {};
      const changes = STAT_FIELDS
        .filter((field) => Number(old[field]) !== Number(s[field]))
        .map((field) => `${field} ${old[field] ?? "-"} -> ${s[field]}`);
      console.log(`  ${nameById.get(s.participant_id) || s.participant_id}: ${changes.join(", ")}`);
    });
    if (!diffs.length && stored.length === standings.length) continue;
    changedRevisions += 1;

    await run("DELETE FROM in_person_standings WHERE tournament_id = ? AND revision = ?", [
      tournamentId,
      rev.revision,
    ]);
    for (const s of standings) {
      await run(
        `INSERT INTO in_person_standings (
           tournament_id, revision, source_completed_round_id, participant_id,
           position, played, wins, buchholz, solkoff1, solkoff2,
           vp_difference, sonneborn_berger, bye_count, calculated_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          tournamentId, rev.revision, sourceRound.id, s.participant_id,
          s.position, s.played, s.wins, s.buchholz, s.solkoff1, s.solkoff2,
          s.vp_difference, s.sonneborn_berger, s.bye_count, rev.calculated_at,
        ]
      );
    }
  }

  if (apply && changedRevisions) {
    await run("UPDATE in_person_tournaments SET revision = revision + 1, updated_at = CURRENT_TIMESTAMP WHERE id = ?", [tournamentId]);
    await run("COMMIT");
    console.log(`Committed: ${changedRevisions} revision(s) rebuilt.`);
  } else {
    await run("ROLLBACK");
    console.log(apply ? "Nothing to change." : "Dry run: rolled back. Re-run with --apply to write.");
  }
} catch (error) {
  await run("ROLLBACK").catch(() => {});
  console.error(error);
  process.exitCode = 1;
} finally {
  db.close();
}
