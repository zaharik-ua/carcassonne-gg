function normalizeDuelIds(duelIds) {
  return Array.from(new Set(
    (Array.isArray(duelIds) ? duelIds : [])
      .map((value) => String(value || "").trim())
      .filter(Boolean)
  ));
}

export function loadPublicGamesByDuelIds(db, duelIds, callback) {
  if (!db || typeof db.all !== "function") {
    throw new Error("db is required");
  }
  if (typeof callback !== "function") {
    throw new Error("callback is required");
  }
  const normalizedIds = normalizeDuelIds(duelIds);
  if (!normalizedIds.length) {
    callback(null, []);
    return undefined;
  }
  const placeholders = normalizedIds.map(() => "?").join(", ");
  return db.all(
    `
      SELECT
        g.id,
        g.duel_id,
        g.bga_table_id,
        g.game_number,
        g.player_1_score,
        g.player_2_score,
        g.player_1_rank,
        g.player_2_rank,
        g.player_1_clock,
        g.player_2_clock,
        g.status,
        gr.carcassonne_lab_url,
        gr.board_stats_json,
        gr.meeple_stats_json,
        gr.scoring_json,
        gr.player_time_json
      FROM games g
      LEFT JOIN game_replays gr
        ON gr.game_id = g.id
       AND gr.status = 'ready'
      WHERE trim(COALESCE(g.duel_id, '')) IN (${placeholders})
        AND g.deleted_at IS NULL
      ORDER BY g.duel_id COLLATE NOCASE ASC, g.game_number ASC, g.id ASC
    `,
    normalizedIds,
    callback
  );
}
