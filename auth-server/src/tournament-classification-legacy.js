// Frozen v1 migration adapter. Runtime database reads use type/category only.
import { getTournamentCategory } from "./tournament-classification.js";
const key = (value) => String(value ?? "").trim().replace(/\s+/g, " ").toLowerCase();

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

export function readLegacyTournamentClassification(row = {}) {
  const category = getTournamentCategory(row.category);
  if (category) return categoryClassification(category);
  const oldType = key(row.type || row.tournament_type);
  if (["national", "nationals"].includes(oldType)) return categoryClassification(getTournamentCategory("National"));
  if (["club", "clubs"].includes(oldType)
      || (!["individual", "individuals"].includes(oldType) && key(row.team_type) === "club")) {
    return categoryClassification(getTournamentCategory("Clubs"));
  }
  const type = ["individual", "individuals"].includes(oldType) ? "Individuals" : "Teams";
  const accessType = legacyAccessType(row);
  if (type === "Teams" && !key(row.category) && accessType === "Friendly") {
    return categoryClassification(getTournamentCategory("Friendly"));
  }
  return {
    type,
    category: String(row.category || "").trim() || null,
    team_type: key(row.team_type) === "club" ? "Club" : "National",
    subtype: type === "Teams" ? accessType : null,
    access_type: accessType,
  };
}

