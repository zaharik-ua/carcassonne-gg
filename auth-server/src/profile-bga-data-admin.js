export const PROFILE_BGA_DATA_PROGRESS_PREFIX = "BGA_PROFILE_BATCH_EVENT ";

function parseIntegerOption(value, fallback, { name, min, max }) {
  if (value === undefined || value === null || value === "") return fallback;
  const normalized = typeof value === "number" ? value : Number(String(value).trim());
  if (!Number.isInteger(normalized) || normalized < min || normalized > max) {
    throw new RangeError(`${name} must be an integer between ${min} and ${max}`);
  }
  return normalized;
}

function parseOptionalDateTimeOption(value, { name }) {
  if (value === undefined || value === null) return null;
  const raw = String(value).trim();
  if (!raw) return null;
  const normalized = /^\d{4}-\d{2}-\d{2}$/.test(raw)
    ? `${raw}T00:00:00Z`
    : /(?:Z|[+-]\d{2}:?\d{2})$/i.test(raw)
      ? raw
      : `${raw}Z`;
  const date = new Date(normalized);
  if (Number.isNaN(date.getTime())) {
    throw new RangeError(`${name} must be a valid date/time`);
  }
  return date.toISOString();
}

export function normalizeProfileBgaDataOptions(input = {}) {
  return {
    batch_size: parseIntegerOption(input?.batch_size, 20, {
      name: "batch_size",
      min: 1,
      max: 100,
    }),
    stop_after_consecutive_failures: parseIntegerOption(
      input?.stop_after_consecutive_failures,
      5,
      {
        name: "stop_after_consecutive_failures",
        min: 1,
        max: 100,
      }
    ),
    include_removed: input?.include_removed === true
      || input?.include_removed === 1
      || String(input?.include_removed || "").trim().toLowerCase() === "true",
    bga_data_updated_before: parseOptionalDateTimeOption(input?.bga_data_updated_before, {
      name: "bga_data_updated_before",
    }),
  };
}

export function buildProfileBgaDataScriptArgs(dbPath, options) {
  const normalized = normalizeProfileBgaDataOptions(options);
  return [
    "--db-path",
    String(dbPath),
    "--all",
    "--limit",
    String(normalized.batch_size),
    "--stop-after-consecutive-failures",
    String(normalized.stop_after_consecutive_failures),
    ...(normalized.include_removed ? ["--include-removed"] : []),
    ...(normalized.bga_data_updated_before
      ? ["--bga-data-updated-before", normalized.bga_data_updated_before]
      : []),
  ];
}

export function parseProfileBgaDataProgressLine(line) {
  const text = String(line || "").trim();
  if (!text.startsWith(PROFILE_BGA_DATA_PROGRESS_PREFIX)) return null;

  try {
    const event = JSON.parse(text.slice(PROFILE_BGA_DATA_PROGRESS_PREFIX.length));
    if (!event || typeof event !== "object" || event.type !== "batch") return null;
    return event;
  } catch (_error) {
    return null;
  }
}
