export const TOURNAMENT_CATEGORIES = Object.freeze([
  { name: "WTCOC", type: "Teams", mode: "official", sort_order: 40 },
  { name: "ETCOC", type: "Teams", mode: "official", sort_order: 50 },
  { name: "Asian Cup", type: "Teams", mode: "official", sort_order: 60 },
  { name: "Copa America", type: "Teams", mode: "official", sort_order: 70 },
  { name: "Friendly", type: "Teams", mode: "friendly", sort_order: 75 },
  { name: "Clubs", type: "Teams", mode: "pending", sort_order: 76 },
  { name: "CCL", type: "Individuals", mode: "pending", sort_order: 80 },
  { name: "MSO", type: "Individuals", mode: "pending", sort_order: 90 },
  { name: "Rivals", type: "Individuals", mode: "rivals", sort_order: 95 },
  { name: "King of Carcassonne", type: "Individuals", mode: "pending", sort_order: 100 },
  { name: "National", type: "Individuals", mode: "pending", sort_order: 105 },
].map(Object.freeze));

const key = (value) => String(value ?? "").trim().replace(/\s+/g, " ").toLowerCase();
const has = (object, field) => Object.prototype.hasOwnProperty.call(object || {}, field);
const categories = new Map(TOURNAMENT_CATEGORIES.map((category) => [key(category.name), category]));
categories.set("koc", categories.get("king of carcassonne"));

export function getTournamentCategory(value) {
  return categories.get(key(value)) || null;
}

function legacyAccessType(row) {
  const value = key(row.subtype) || key(row.access_type);
  return ["official", "closed", "2"].includes(value) ? "Official" : "Friendly";
}

function categoryClassification(category) {
  const accessType = category.mode === "official" ? "Official" : "Friendly";
  return {
    type: category.type,
    category: category.name,
    team_type: category.name === "Clubs" ? "Club" : "National",
    subtype: category.type === "Teams" ? accessType : null,
    access_type: accessType,
  };
}

// Legacy response fields are computed here; they are never read from storage.
export function readTournamentClassification(row = {}) {
  const type = key(row.type) === "teams" ? "Teams" : key(row.type) === "individuals" ? "Individuals" : null;
  const category = getTournamentCategory(row.category);
  const categoryName = category?.name || String(row.category || "").trim() || null;
  const accessType = type === "Teams" && category?.mode === "official" ? "Official" : "Friendly";
  return {
    type,
    category: categoryName,
    team_type: categoryName === "Clubs" ? "Club" : "National",
    subtype: type === "Teams" ? accessType : null,
    access_type: accessType,
  };
}

export function getTournamentAccessType(row) {
  return readTournamentClassification(row).access_type;
}

export function getTournamentType(row) {
  return readTournamentClassification(row).type;
}

function classificationError(message) {
  return Object.assign(new Error(message), { httpStatus: 400, code: "INVALID_TOURNAMENT_CLASSIFICATION" });
}

function inputType(value, legacy = false) {
  const normalized = key(value);
  if (["teams", ...(legacy ? ["team", "club", "clubs"] : [])].includes(normalized)) return "Teams";
  if (["individuals", ...(legacy ? ["individual", "national", "nationals"] : [])].includes(normalized)) return "Individuals";
  throw classificationError(legacy ? "tournament_type is invalid" : "type must be Teams or Individuals");
}

// Accept the new contract and old forms, while preserving omitted PATCH fields.
export function resolveTournamentClassificationInput(payload = {}, current = null) {
  const previous = current ? readTournamentClassification(current) : null;
  const usesType = has(payload, "type");
  const usesLegacyType = has(payload, "tournament_type");
  const type = usesType ? inputType(payload.type)
    : usesLegacyType ? inputType(payload.tournament_type, true) : previous?.type;
  if (!type) throw classificationError("type and category are required");
  if (usesType && usesLegacyType && inputType(payload.tournament_type, true) !== type) {
    throw classificationError("type and tournament_type disagree");
  }
  let categoryName = has(payload, "category") ? String(payload.category || "").trim() : previous?.category;
  const oldType = usesLegacyType ? key(payload.tournament_type) : "";
  if (!categoryName && !usesType) {
    if (["national", "nationals"].includes(oldType)) categoryName = "National";
    else if (["club", "clubs"].includes(oldType) || key(payload.team_type) === "club") categoryName = "Clubs";
    else if (type === "Teams" && legacyAccessType({ ...current, ...payload }) === "Friendly") categoryName = "Friendly";
  }
  const category = getTournamentCategory(categoryName);
  if (!category) {
    // Metadata edits must remain possible for historical rows not yet mapped.
    if (previous && !usesType && type === previous.type && key(categoryName) === key(previous.category)) return previous;
    throw classificationError("category must be one of the supported tournament categories");
  }
  if (category.type !== type) throw classificationError(`category ${category.name} requires type ${category.type}`);
  if (["national", "nationals"].includes(oldType) && category.name !== "National") {
    throw classificationError("legacy National tournaments require the National category");
  }
  const result = categoryClassification(category);
  if (has(payload, "team_type") && key(payload.team_type)) {
    if (!["national", "club"].includes(key(payload.team_type))) throw classificationError("team_type must be National or Club");
    // The old Clubs form sends National as an unused default.
    if (type === "Teams" && !["club", "clubs"].includes(oldType) && key(payload.team_type) !== key(result.team_type)) {
      throw classificationError("team_type disagrees with category");
    }
  }
  for (const field of ["subtype", "access_type"]) {
    if (!has(payload, field) || !key(payload[field])) continue;
    if (!["official", "closed", "2", "friendly", "open", "1"].includes(key(payload[field]))) {
      throw classificationError(`${field} is invalid`);
    }
    if (legacyAccessType({ access_type: payload[field] }) !== result.access_type) {
      throw classificationError(`${field} disagrees with category`);
    }
  }
  return result;
}

function column(alias, name) {
  if (alias && !/^[a-z_][a-z0-9_]*$/i.test(alias)) throw new Error("Invalid tournament SQL alias");
  return `${alias ? `${alias}.` : ""}${name}`;
}
const literal = (value) => `'${value.replaceAll("'", "''")}'`;

// All runtime SQL uses only the two stored classification columns.
export function tournamentTypeSql(alias = "") {
  return `(CASE lower(trim(${column(alias, "type")})) WHEN 'teams' THEN 'Teams' WHEN 'individuals' THEN 'Individuals' END)`;
}

export function tournamentCategorySql(alias = "") {
  const category = column(alias, "category");
  const cases = [...categories].map(([name, rule]) => `WHEN ${literal(name)} THEN ${literal(rule.name)}`).join(" ");
  return `(CASE lower(trim(${category})) ${cases} ELSE NULLIF(trim(${category}), '') END)`;
}

export function tournamentTeamTypeSql(alias = "") {
  return `(CASE WHEN ${tournamentCategorySql(alias)} = 'Clubs' THEN 'Club' ELSE 'National' END)`;
}

export function tournamentAccessTypeSql(alias = "") {
  const official = TOURNAMENT_CATEGORIES.filter((rule) => rule.mode === "official").map((rule) => literal(rule.name)).join(", ");
  return `(CASE WHEN ${tournamentTypeSql(alias)} = 'Teams' AND ${tournamentCategorySql(alias)} IN (${official}) THEN 'Official' ELSE 'Friendly' END)`;
}
