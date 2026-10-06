export function registerDuelGameBgaResultRoutes(app, {
  dbGetAsync, loadTournamentAccessForUser, canUserEditMatchResults,
  isCompletedMatchStatus, fetchBgaGameResult,
}) {
  app.post("/duels/:id/games/bga-result", async (req, res) => {
    if (!req.user) return res.status(401).json({ ok: false, message: "Unauthorized" });
    const duelId = String(req.params.id || "").trim();
    const tableId = String(req.body?.bga_table_id || "").trim();
    if (!/^\d{9,10}$/.test(tableId)) {
      return res.status(400).json({ ok: false, message: "Table ID must contain 9 or 10 digits." });
    }
    try {
      const duel = await dbGetAsync(`
        SELECT d.*, m.team_1, m.team_2, m.status AS match_status,
          COALESCE(NULLIF(trim(d.tournament_id), ''), m.tournament_id) AS tournament_id
        FROM duels d
        JOIN matches m ON trim(m.id) = trim(d.match_id) AND m.deleted_at IS NULL
        WHERE trim(d.id) = ? AND d.deleted_at IS NULL LIMIT 1
      `, [duelId]);
      if (!duel) return res.status(404).json({ ok: false, message: "Duel not found" });
      if (!isCompletedMatchStatus(duel.match_status)) {
        return res.status(403).json({ ok: false, message: "Match results can be edited only for completed matches" });
      }
      const tournament = await new Promise((resolve, reject) => {
        loadTournamentAccessForUser(duel.tournament_id, req.user, (error, row) => error ? reject(error) : resolve(row));
      });
      const access = canUserEditMatchResults({ tournament, user: req.user, matchRow: duel });
      if (!access.allowed) {
        return res.status(403).json({ ok: false, message: access.expired
          ? "Match result editing is available for 1 day after match start." : "Forbidden" });
      }
      if (![duel.player_1_id, duel.player_2_id].every((id) => /^\d+$/.test(String(id || "")))
        || String(duel.player_1_id) === String(duel.player_2_id)) {
        return res.status(400).json({ ok: false, message: "Both duel players must have valid BGA accounts." });
      }
      const result = await fetchBgaGameResult(tableId, duel.player_1_id, duel.player_2_id);
      if (!result?.ok) {
        return res.status(result?.not_found ? 404 : 502).json({ ok: false,
          message: result?.message || "Could not load the BGA result. Please try again." });
      }
      return res.json({ ok: true, game: result.game });
    } catch (error) {
      console.error(`Failed to load BGA game result for duel ${duelId}`, error);
      return res.status(502).json({ ok: false, message: "Could not load the BGA result. Please try again." });
    }
  });
}
