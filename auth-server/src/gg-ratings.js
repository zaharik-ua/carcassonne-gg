function percentileInclusive(sortedValues, percentile) {
  if (!Array.isArray(sortedValues) || !sortedValues.length) return null;
  const position = (sortedValues.length - 1) * percentile;
  const lowerIndex = Math.floor(position);
  const upperIndex = Math.ceil(position);
  const lowerValue = sortedValues[lowerIndex];
  const upperValue = sortedValues[upperIndex];
  return lowerValue + ((upperValue - lowerValue) * (position - lowerIndex));
}

export function buildGgRatingContext(profileRows) {
  const ggEloByPlayerId = new Map();
  const ratingList = [];
  (Array.isArray(profileRows) ? profileRows : []).forEach((row) => {
    const playerId = String(row?.id || "").trim();
    const rawRating = row?.gg_elo;
    if (!playerId || rawRating === null || rawRating === undefined || String(rawRating).trim() === "") return;
    const rating = Number(rawRating);
    if (!Number.isFinite(rating)) return;
    ggEloByPlayerId.set(playerId, rating);
    ratingList.push(rating);
  });
  ratingList.sort((a, b) => a - b);
  return {
    ggEloByPlayerId,
    lowAnchor: percentileInclusive(ratingList, 0.15),
    highAnchor: percentileInclusive(ratingList, 0.95),
  };
}

export function calculateGgDuelRating(context, player1Id, player2Id, ranking = 1) {
  const playerRatingA = context?.ggEloByPlayerId?.get(String(player1Id || "").trim());
  const playerRatingB = context?.ggEloByPlayerId?.get(String(player2Id || "").trim());
  const lowAnchor = context?.lowAnchor;
  const highAnchor = context?.highAnchor;
  if (Number(ranking) !== 1 || ![playerRatingA, playerRatingB, lowAnchor, highAnchor].every(Number.isFinite)) {
    return { ggRatingFull: null, ggRating: null };
  }

  const ratingSpan = Math.max(highAnchor - lowAnchor, 1);
  const differenceScale = ratingSpan * 0.4375;
  const maximumScore = 5.49;
  const curvePower = 0.8;
  const closenessBonus = 0.15;
  const matchAverage = (playerRatingA + playerRatingB) / 2;
  const normalizedStrength = Math.min(1, Math.max(0, (matchAverage - lowAnchor) / ratingSpan));
  const closeness = 1 - Math.min(1, Math.abs(playerRatingA - playerRatingB) / differenceScale);
  const calculatedScore = Math.min(
    maximumScore,
    maximumScore * (normalizedStrength ** curvePower)
      + closenessBonus * normalizedStrength * (closeness ** curvePower)
  );
  const ggRatingFull = playerRatingA >= highAnchor && playerRatingB >= highAnchor
    ? 6
    : calculatedScore;
  return { ggRatingFull, ggRating: Math.round(ggRatingFull) };
}

export function calculateGgMatchRating(duelRatingFullValues) {
  const rawRatings = Array.isArray(duelRatingFullValues) ? duelRatingFullValues : [];
  const ratings = rawRatings.filter((value) => typeof value === "number" && Number.isFinite(value));
  if (!ratings.length || ratings.length !== rawRatings.length) {
    return null;
  }
  const meanSquare = ratings.reduce((sum, value) => sum + value * value, 0) / ratings.length;
  return Math.round(Math.sqrt(meanSquare));
}

function all(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => error ? reject(error) : resolve(rows));
  });
}

function run(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, (error) => error ? reject(error) : resolve());
  });
}

export async function ensureGgRatingsSchema(db) {
  for (const [table, column, type] of [
    ["duels", "gg_rating_full", "REAL"],
    ["duels", "gg_rating", "INTEGER"],
    ["matches", "gg_rating", "INTEGER"],
  ]) {
    const columns = await all(db, `PRAGMA table_info(${table})`);
    if (!columns.length) throw new Error(`Missing required table: ${table}`);
    if (!columns.some((item) => item.name === column)) {
      await run(db, `ALTER TABLE ${table} ADD COLUMN ${column} ${type}`);
    }
  }
}

export async function loadGgRatingContext(db) {
  return buildGgRatingContext(await all(db, `
    SELECT id, gg_elo FROM profiles
    WHERE deleted_at IS NULL AND gg_elo IS NOT NULL
  `));
}

export async function updateMatchGgRating(db, matchId) {
  const [match] = await all(db, `
    SELECT m.id, t.ranking, t.tournament_type
    FROM matches m
    LEFT JOIN tournaments t
      ON upper(trim(t.id)) = upper(trim(m.tournament_id))
      AND t.deleted_at IS NULL
    WHERE m.id = ? AND m.deleted_at IS NULL
  `, [matchId]);
  if (!match) return null;
  let ggRating = null;
  if (Number(match.ranking) === 1
      && ["team", "teams"].includes(String(match.tournament_type || "Teams").trim().toLowerCase())) {
    const duels = await all(db, `
      SELECT gg_rating_full FROM duels
      WHERE trim(match_id) = trim(?) AND deleted_at IS NULL
    `, [matchId]);
    ggRating = calculateGgMatchRating(duels.map((duel) => duel.gg_rating_full));
  }
  await run(db, "UPDATE matches SET gg_rating = ? WHERE id = ?", [ggRating, matchId]);
  return ggRating;
}

