const ELO_SCALE = 400;

function finiteNumber(value, fallback = null) {
  if (value === null || value === undefined || String(value).trim() === "") return fallback;
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : fallback;
}

export function expectedEloScore(playerRating, opponentRating) {
  const player = finiteNumber(playerRating);
  const opponent = finiteNumber(opponentRating);
  if (player === null || opponent === null) return null;
  return 1 / (1 + (10 ** ((opponent - player) / ELO_SCALE)));
}

export function calculateSmoothedWinRate(wins, games, smoothing = 0.5) {
  const normalizedGames = Math.max(0, Math.trunc(finiteNumber(games, 0)));
  const normalizedWins = Math.min(
    normalizedGames,
    Math.max(0, finiteNumber(wins, 0))
  );
  const normalizedSmoothing = Math.max(0, finiteNumber(smoothing, 0.5));
  const denominator = normalizedGames + (2 * normalizedSmoothing);
  if (denominator <= 0) return 0.5;
  return (normalizedWins + normalizedSmoothing) / denominator;
}

export function calculateTpr(opponentRatings, targetScore) {
  const opponents = (Array.isArray(opponentRatings) ? opponentRatings : [])
    .map((rating) => finiteNumber(rating))
    .filter((rating) => rating !== null);
  if (!opponents.length) return null;

  const target = Math.min(1 - Number.EPSILON, Math.max(Number.EPSILON, finiteNumber(targetScore, 0.5)));
  let low = Math.min(...opponents) - 10000;
  let high = Math.max(...opponents) + 10000;

  for (let iteration = 0; iteration < 100; iteration += 1) {
    const candidate = (low + high) / 2;
    const expectedAverage = opponents.reduce(
      (sum, opponentRating) => sum + expectedEloScore(candidate, opponentRating),
      0
    ) / opponents.length;
    if (expectedAverage < target) low = candidate;
    else high = candidate;
  }

  return (low + high) / 2;
}

export function calculateTprConfidence(games, targetGames = 10) {
  const normalizedGames = Math.max(0, finiteNumber(games, 0));
  const normalizedTarget = Math.max(1, finiteNumber(targetGames, 10));
  return Math.min(1, normalizedGames / normalizedTarget);
}

export function calculateAdjustedTpr(elo, tpr, confidence) {
  const normalizedElo = finiteNumber(elo);
  const normalizedTpr = finiteNumber(tpr);
  if (normalizedElo === null || normalizedTpr === null) return null;
  const normalizedConfidence = Math.min(1, Math.max(0, finiteNumber(confidence, 0)));
  return normalizedElo + normalizedConfidence * (normalizedTpr - normalizedElo);
}

export function percentileInclusive(values, percentile) {
  const sorted = (Array.isArray(values) ? values : [])
    .map((value) => finiteNumber(value))
    .filter((value) => value !== null)
    .sort((left, right) => left - right);
  if (!sorted.length) return null;
  const normalizedPercentile = Math.min(1, Math.max(0, finiteNumber(percentile, 0.75)));
  const position = (sorted.length - 1) * normalizedPercentile;
  const lowerIndex = Math.floor(position);
  const upperIndex = Math.ceil(position);
  const fraction = position - lowerIndex;
  return sorted[lowerIndex] + (sorted[upperIndex] - sorted[lowerIndex]) * fraction;
}

export function calculateBounty(adjustedTpr, benchmarkTpr) {
  const player = finiteNumber(adjustedTpr);
  const benchmark = finiteNumber(benchmarkTpr);
  if (player === null || benchmark === null) return null;
  return 1 / (1 + (10 ** ((benchmark - player) / ELO_SCALE)));
}

export function calculateTournamentBountySnapshot(playerStates, options = {}) {
  const targetGames = Math.max(1, finiteNumber(options.targetGames, 10));
  const smoothing = Math.max(0, finiteNumber(options.smoothing, 0.5));
  const benchmarkPercentile = Math.min(
    1,
    Math.max(0, finiteNumber(options.benchmarkPercentile, 0.75))
  );
  const statesByPlayerId = new Map();

  (Array.isArray(playerStates) ? playerStates : []).forEach((source) => {
    const playerId = String(source?.playerId ?? source?.player_id ?? "").trim();
    if (!playerId || statesByPlayerId.has(playerId)) return;
    const games = Math.max(0, Math.trunc(finiteNumber(source?.games, 0)));
    statesByPlayerId.set(playerId, {
      playerId,
      elo: finiteNumber(source?.elo, 1500),
      games,
      wins: Math.min(games, Math.max(0, finiteNumber(source?.wins, 0))),
      opponentIds: (Array.isArray(source?.opponentIds) ? source.opponentIds : [])
        .map((opponentId) => String(opponentId || "").trim())
        .filter(Boolean),
    });
  });

  for (const state of statesByPlayerId.values()) {
    const opponentRatings = state.opponentIds
      .map((opponentId) => statesByPlayerId.get(opponentId)?.elo)
      .filter((rating) => rating !== null && rating !== undefined);
    state.smoothedWinRate = calculateSmoothedWinRate(state.wins, state.games, smoothing);
    state.tpr = calculateTpr(opponentRatings, state.smoothedWinRate);
    state.confidence = calculateTprConfidence(state.games, targetGames);
    state.adjustedTpr = calculateAdjustedTpr(state.elo, state.tpr, state.confidence);
  }

  const benchmarkTpr = percentileInclusive(
    Array.from(statesByPlayerId.values()).map((state) => state.adjustedTpr),
    benchmarkPercentile
  );
  for (const state of statesByPlayerId.values()) {
    state.bounty = calculateBounty(state.adjustedTpr, benchmarkTpr);
  }

  return { benchmarkTpr, statesByPlayerId };
}

export function calculateBountyPoints(ownBounty, defeatedOpponentBounties) {
  const own = finiteNumber(ownBounty, 0);
  const opponents = (Array.isArray(defeatedOpponentBounties) ? defeatedOpponentBounties : [])
    .map((value) => finiteNumber(value))
    .filter((value) => value !== null);
  const opponentsPoints = opponents.reduce((sum, value) => sum + value, 0);
  return {
    opponentsPoints,
    points: own + opponentsPoints,
  };
}
