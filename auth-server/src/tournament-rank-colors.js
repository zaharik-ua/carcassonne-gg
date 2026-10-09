const COLORS = new Set(["green", "blue", "red", "orange", "gold", "silver", "bronze"]);
const isObject = value => value !== null && typeof value === "object" && !Array.isArray(value);

export function normalizeTournamentRankColors(value) {
  if (value === null || value === undefined || (typeof value === "string" && !value.trim())) return null;
  let parsed = value;
  if (typeof value === "string") {
    try {
      parsed = JSON.parse(value);
    } catch (_error) {
      throw new Error("Rank colors: invalid settings.");
    }
  }
  if (!isObject(parsed)) throw new Error("Rank colors: settings must be an object.");
  if (Object.hasOwn(parsed, "stages") && Object.keys(parsed).length !== 1) {
    throw new Error("Rank colors: put all stage settings inside stages.");
  }
  const stages = Object.hasOwn(parsed, "stages") ? parsed.stages : parsed;
  if (!isObject(stages)) throw new Error("Rank colors: stages must be an object.");
  const normalizedStages = {};
  for (const [stage, settings] of Object.entries(stages)) {
    if (!/^Stage [123]$/.test(stage) || !isObject(settings)) {
      throw new Error("Rank colors: use Stage 1, Stage 2 or Stage 3 with an object of settings.");
    }
    if (Object.keys(settings).some(key => !["rankColors", "rankColorLegend"].includes(key))) {
      throw new Error(`Rank colors: ${stage} supports only rankColors and rankColorLegend.`);
    }
    const normalized = {};
    for (const field of ["rankColors", "rankColorLegend"]) {
      if (!Object.hasOwn(settings, field)) continue;
      if (!isObject(settings[field])) throw new Error(`Rank colors: ${stage}.${field} must be an object.`);
      const entries = {};
      for (const [key, entry] of Object.entries(settings[field])) {
        const color = String(field === "rankColors" ? entry : key).trim().toLowerCase();
        if (!COLORS.has(color)) {
          throw new Error("Rank colors: allowed colors are green, blue, red, orange, gold, silver and bronze.");
        }
        if (field === "rankColors") {
          if (!/^[1-9]\d*$/.test(key) || !Number.isSafeInteger(Number(key))) {
            throw new Error(`Rank colors: ${stage} positions must be positive whole numbers.`);
          }
          entries[key] = color;
        } else {
          if (typeof entry !== "string" || !entry.trim()) {
            throw new Error(`Rank colors: ${stage} legend labels must be non-empty text.`);
          }
          entries[color] = entry.trim();
        }
      }
      if (Object.keys(entries).length) normalized[field] = entries;
    }
    if (Object.keys(normalized).length) normalizedStages[stage] = normalized;
  }
  return Object.keys(normalizedStages).length ? JSON.stringify({ stages: normalizedStages }) : null;
}

export function resolveTournamentRankColorsPatch(payload, currentTournament) {
  return Object.hasOwn(payload || {}, "rank_colors")
    ? normalizeTournamentRankColors(payload.rank_colors)
    : currentTournament?.rank_colors ?? null;
}
