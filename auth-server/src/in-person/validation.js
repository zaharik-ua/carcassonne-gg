import {
  IN_PERSON_DRAW_MODE,
  IN_PERSON_LOCAL_SUBTYPES,
  IN_PERSON_PLAYOFF_FIRST_ROUNDS,
  IN_PERSON_SCOPES,
  IN_PERSON_TIEBREAK_PROFILE,
} from "./constants.js";

export class InPersonError extends Error {
  constructor(status, code, message, details = null) {
    super(message);
    this.name = "InPersonError";
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

export function validationError(code, message, details = null) {
  return new InPersonError(400, code, message, details);
}

export function conflictError(code, message, details = null) {
  return new InPersonError(409, code, message, details);
}

export function notFoundError(code, message) {
  return new InPersonError(404, code, message);
}

function hasOwn(value, key) {
  return Object.prototype.hasOwnProperty.call(value || {}, key);
}

export function normalizeText(value) {
  return String(value ?? "").trim();
}

export function normalizeOptionalText(value) {
  const normalized = normalizeText(value);
  return normalized || null;
}

function selectValue(payload, current, key, fallback = null) {
  if (hasOwn(payload, key)) return payload[key];
  if (current && hasOwn(current, key)) return current[key];
  return fallback;
}

function normalizeDate(value, field) {
  const normalized = normalizeText(value);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(normalized)) {
    throw validationError("INVALID_DATE", `${field} must use YYYY-MM-DD format`, { field });
  }
  const parsed = new Date(`${normalized}T00:00:00.000Z`);
  if (Number.isNaN(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== normalized) {
    throw validationError("INVALID_DATE", `${field} must be a valid calendar date`, { field });
  }
  return normalized;
}

function normalizeUrl(value, field) {
  const normalized = normalizeOptionalText(value);
  if (!normalized) return null;
  let parsed;
  try {
    parsed = new URL(normalized);
  } catch {
    throw validationError("INVALID_URL", `${field} must be a valid http:// or https:// URL`, { field });
  }
  if (!(["http:", "https:"].includes(parsed.protocol)) || !parsed.hostname) {
    throw validationError("INVALID_URL", `${field} must be a valid http:// or https:// URL`, { field });
  }
  return normalized;
}

function normalizePositiveInteger(value, field) {
  const normalized = Number(value);
  if (!Number.isInteger(normalized) || normalized <= 0) {
    throw validationError("INVALID_POSITIVE_INTEGER", `${field} must be a positive integer`, { field });
  }
  return normalized;
}

function normalizeSlug(value) {
  const slug = normalizeText(value).toLowerCase();
  if (!slug) throw validationError("SLUG_REQUIRED", "slug is required", { field: "slug" });
  if (!/^[a-z0-9]+(?:-[a-z0-9]+)*$/.test(slug)) {
    throw validationError(
      "INVALID_SLUG",
      "slug may contain lowercase Latin letters, numbers and single hyphens",
      { field: "slug" }
    );
  }
  return slug;
}

export function normalizeCityInput(payload, current = null) {
  const associationId = normalizeText(selectValue(payload, current, "association_id"));
  const nameEn = normalizeText(selectValue(payload, current, "name_en"));
  const nameLocal = normalizeOptionalText(selectValue(payload, current, "name_local"));

  if (!associationId) {
    throw validationError("ASSOCIATION_REQUIRED", "association_id is required", {
      field: "association_id",
    });
  }
  if (!nameEn) {
    throw validationError("CITY_NAME_REQUIRED", "name_en is required", { field: "name_en" });
  }
  return { association_id: associationId, name_en: nameEn, name_local: nameLocal };
}

export function normalizeTournamentInput(payload, current = null) {
  const slug = normalizeSlug(selectValue(payload, current, "slug"));
  const nameEn = normalizeText(selectValue(payload, current, "name_en"));
  const nameLocal = normalizeOptionalText(selectValue(payload, current, "name_local"));
  const scope = normalizeText(selectValue(payload, current, "scope", "international")).toLowerCase();
  let associationId = normalizeOptionalText(selectValue(payload, current, "association_id"));
  let localSubtype = normalizeOptionalText(selectValue(payload, current, "local_subtype"));
  let qualifierCityId = normalizeOptionalText(selectValue(payload, current, "qualifier_city_id"));
  const startDate = normalizeDate(selectValue(payload, current, "start_date"), "start_date");
  const endDate = normalizeDate(selectValue(payload, current, "end_date"), "end_date");
  const organizerName = normalizeText(selectValue(payload, current, "organizer_name"));
  const organizerUrl = normalizeUrl(
    selectValue(payload, current, "organizer_url"),
    "organizer_url"
  );
  const rulesUrl = normalizeUrl(selectValue(payload, current, "rules_url"), "rules_url");
  const swissRoundsCount = normalizePositiveInteger(
    selectValue(payload, current, "swiss_rounds_count"),
    "swiss_rounds_count"
  );
  const playoffFirstRound = normalizeText(
    selectValue(payload, current, "playoff_first_round")
  ).toLowerCase();

  if (!nameEn) {
    throw validationError("TOURNAMENT_NAME_REQUIRED", "name_en is required", { field: "name_en" });
  }
  if (!IN_PERSON_SCOPES.includes(scope)) {
    throw validationError("INVALID_SCOPE", "scope must be international or local", {
      field: "scope",
    });
  }
  if (endDate < startDate) {
    throw validationError("INVALID_DATE_PERIOD", "end_date cannot be earlier than start_date", {
      fields: ["start_date", "end_date"],
    });
  }
  if (!organizerName) {
    throw validationError("ORGANIZER_NAME_REQUIRED", "organizer_name is required", {
      field: "organizer_name",
    });
  }
  if (!IN_PERSON_PLAYOFF_FIRST_ROUNDS.includes(playoffFirstRound)) {
    throw validationError(
      "INVALID_PLAYOFF_FIRST_ROUND",
      "playoff_first_round must be round_of_32, round_of_16, quarter_final or semi_final",
      { field: "playoff_first_round" }
    );
  }

  if (scope === "international") {
    associationId = null;
    localSubtype = null;
    qualifierCityId = null;
  } else {
    localSubtype = normalizeText(localSubtype).toLowerCase();
    if (!associationId) {
      throw validationError("ASSOCIATION_REQUIRED", "association_id is required for a local tournament", {
        field: "association_id",
      });
    }
    if (!IN_PERSON_LOCAL_SUBTYPES.includes(localSubtype)) {
      throw validationError("INVALID_LOCAL_SUBTYPE", "local_subtype must be final or qualifier", {
        field: "local_subtype",
      });
    }
    if (localSubtype === "final") {
      qualifierCityId = null;
    } else if (!qualifierCityId) {
      throw validationError(
        "QUALIFIER_CITY_REQUIRED",
        "qualifier_city_id is required for a local qualifier",
        { field: "qualifier_city_id" }
      );
    }
  }

  return {
    slug,
    name_en: nameEn,
    name_local: nameLocal,
    scope,
    association_id: associationId,
    local_subtype: localSubtype,
    qualifier_city_id: qualifierCityId,
    start_date: startDate,
    end_date: endDate,
    organizer_name: organizerName,
    organizer_url: organizerUrl,
    rules_url: rulesUrl,
    swiss_rounds_count: swissRoundsCount,
    playoff_first_round: playoffFirstRound,
    draw_mode: IN_PERSON_DRAW_MODE,
    swiss_tiebreak_profile: IN_PERSON_TIEBREAK_PROFILE,
  };
}

export function normalizeAdminUserIds(value) {
  if (value === undefined) return undefined;
  if (!Array.isArray(value)) {
    throw validationError("INVALID_ADMIN_USERS", "admin_user_ids must be an array", {
      field: "admin_user_ids",
    });
  }
  const result = [];
  const seen = new Set();
  value.forEach((entry) => {
    const rawId = entry && typeof entry === "object"
      ? entry.user_id ?? entry.userId ?? entry.id
      : entry;
    const userId = Number(rawId);
    if (!Number.isInteger(userId) || userId <= 0) {
      throw validationError("INVALID_ADMIN_USER", "Every admin user ID must be a positive integer", {
        field: "admin_user_ids",
      });
    }
    if (!seen.has(userId)) {
      seen.add(userId);
      result.push(userId);
    }
  });
  return result;
}

export function normalizeParticipantInput(payload, current = null, tournament = null) {
  const nameEn = normalizeText(selectValue(payload, current, "name_en"));
  const nameLocal = normalizeOptionalText(selectValue(payload, current, "name_local"));
  const bgaNickname = normalizeOptionalText(selectValue(payload, current, "bga_nickname"));
  const scope = normalizeText(tournament?.scope).toLowerCase();
  let associationId = normalizeOptionalText(selectValue(payload, current, "association_id"));
  let cityId = normalizeOptionalText(selectValue(payload, current, "city_id"));

  if (!nameEn) {
    throw validationError("PARTICIPANT_NAME_REQUIRED", "name_en is required", {
      field: "name_en",
    });
  }
  if (scope === "international") {
    if (!associationId) {
      throw validationError(
        "PARTICIPANT_ASSOCIATION_REQUIRED",
        "association_id is required for an international tournament participant",
        { field: "association_id" }
      );
    }
    cityId = null;
  } else if (scope === "local") {
    if (!cityId) {
      throw validationError(
        "PARTICIPANT_CITY_REQUIRED",
        "city_id is required for a local tournament participant",
        { field: "city_id" }
      );
    }
    associationId = null;
  } else {
    throw validationError("INVALID_TOURNAMENT_SCOPE", "Tournament scope is invalid");
  }

  return {
    name_en: nameEn,
    name_local: nameLocal,
    bga_nickname: bgaNickname,
    association_id: associationId,
    city_id: cityId,
  };
}

export function normalizeDrawNumber(value) {
  if (value === undefined) return undefined;
  if (value === null || normalizeText(value) === "") return null;
  const drawNumber = Number(value);
  if (!Number.isInteger(drawNumber) || drawNumber <= 0) {
    throw validationError("INVALID_DRAW_NUMBER", "draw_number must be a positive integer", {
      field: "draw_number",
    });
  }
  return drawNumber;
}

export function normalizeRequiredBoolean(value, field) {
  if (value === true || value === 1 || value === "1" || value === "true") return true;
  if (value === false || value === 0 || value === "0" || value === "false") return false;
  throw validationError("INVALID_BOOLEAN", `${field} must be true or false`, { field });
}
