import {
  calculateGgDuelRating,
  ensureGgRatingsSchema,
  loadGgRatingContext,
  updateMatchGgRating,
} from "./gg-ratings.js";
import { getTournamentType } from "./tournament-classification.js";

const all = (db, sql) => new Promise((resolve, reject) => {
  db.all(sql, (error, rows) => error ? reject(error) : resolve(rows));
});
const run = (db, sql, params = []) => new Promise((resolve, reject) => {
  db.run(sql, params, (error) => error ? reject(error) : resolve());
});

// Use a dedicated connection: this function owns the transaction.
export async function updatePlannedTournamentGgRatings(db, { dryRun = false } = {}) {
  await run(db, "BEGIN IMMEDIATE");
  try {
    await ensureGgRatingsSchema(db);
    const context = await loadGgRatingContext(db);
    if (!context.ggEloByPlayerId.size) {
      throw new Error("No numeric profiles.gg_elo values found; ratings were not updated");
    }
    const duels = await all(db, `
      SELECT d.id, d.player_1_id, d.player_2_id
      FROM duels d
      LEFT JOIN matches m ON trim(m.id) = trim(d.match_id) AND m.deleted_at IS NULL
      JOIN tournaments t
        ON upper(trim(t.id)) = upper(trim(COALESCE(m.tournament_id, d.tournament_id)))
      WHERE d.deleted_at IS NULL AND t.ranking = 1
        AND (NULLIF(trim(d.match_id), '') IS NULL OR m.id IS NOT NULL)
        AND lower(trim(COALESCE(d.status, ''))) = 'planned'
    `);
    let calculatedDuels = 0;
    for (const duel of duels) {
      const { ggRatingFull, ggRating } = calculateGgDuelRating(context, duel.player_1_id, duel.player_2_id);
      await run(db, "UPDATE duels SET gg_rating_full = ?, gg_rating = ? WHERE id = ?",
        [ggRatingFull, ggRating, duel.id]);
      if (ggRatingFull !== null) calculatedDuels += 1;
    }
    const matches = await all(db, `
      SELECT t.*, m.id AS match_id FROM matches m
      JOIN tournaments t ON upper(trim(t.id)) = upper(trim(m.tournament_id))
      WHERE m.deleted_at IS NULL AND t.ranking = 1
        AND lower(trim(COALESCE(m.status, ''))) = 'planned'
    `);
    let calculatedMatches = 0;
    const teamMatches = matches.filter((match) => getTournamentType(match) === "Teams");
    for (const match of teamMatches) {
      if (await updateMatchGgRating(db, match.match_id) !== null) calculatedMatches += 1;
    }
    // Preview uses the same calculations, then rolls back schema and data together.
    await run(db, dryRun ? "ROLLBACK" : "COMMIT");
    return {
      dry_run: dryRun,
      selected_duels: duels.length,
      calculated_duels: calculatedDuels,
      duels_without_player_gg_elo: duels.length - calculatedDuels,
      selected_matches: teamMatches.length,
      calculated_matches: calculatedMatches,
      matches_without_complete_ratings: teamMatches.length - calculatedMatches,
      anchors: { low: context.lowAnchor, high: context.highAnchor },
    };
  } catch (error) {
    await run(db, "ROLLBACK").catch(() => {});
    throw error;
  }
}
