// Registry of CCL season formats. A tournament with category CCL stores one key in
// tournaments.format_key; the same format can be reused by later seasons.
// A format is never edited once tournaments use it: a changed format becomes a new key.
// Structure generation copies the settings into tournament_stages.settings_json.

const deepFreeze = (value) => {
  if (value && typeof value === "object" && !Object.isFrozen(value)) {
    Object.values(value).forEach(deepFreeze);
    Object.freeze(value);
  }
  return value;
};

// Rounds of one 8-player Double Elimination group: week = round_order, the week of the path.
const DOUBLE_ELIMINATION_8_ROUNDS = [
  { week: 1, name: "Round 1" },
  { week: 2, name: "Lower Round 1" },
  { week: 2, name: "Upper Semifinals" },
  { week: 3, name: "Lower Semifinals" },
  { week: 3, name: "Upper Final" },
  { week: 4, name: "Lower Final" },
  { week: 5, name: "Consolidated Final" },
];

// The path start date is not part of the format: the admin sets it per season on the generated stage.
const qualificationPath = ({ key, groups, grandFinal, advance }) => ({
  key,
  name: key,
  format: "double_elimination",
  groups,
  settings: {
    group_size: 8,
    seeded_per_group: 4,
    grand_final: grandFinal,
    duel_format: "Bo3",
    rounds: grandFinal === "reset"
      ? [...DOUBLE_ELIMINATION_8_ROUNDS, { week: 6, name: "Group Final" }, { week: 6, name: "Group Final Reset" }]
      : DOUBLE_ELIMINATION_8_ROUNDS,
    // Slots that leave the group for the League Stage, in finishing order.
    advance,
  },
});

const CCL_FORMATS = deepFreeze([
  { key: "ccl-2024", name: "CCL 2024", available: false },
  { key: "ccl-2025", name: "CCL 2025", available: false },
  { key: "ccl-2026", name: "CCL 2026", available: false },
  {
    key: "ccl-2027",
    name: "CCL 2027",
    available: true,
    stages: [
      {
        key: "qualifiers",
        name: "Qualifiers",
        format: null,
        children: [
          qualificationPath({
            key: "Q1",
            groups: ["A", "B", "C", "D", "E"],
            grandFinal: "none",
            advance: [{ source: "winner:11", label: "Winner" }, { source: "winner:13", label: "2nd Place" }],
          }),
          qualificationPath({
            key: "Q2",
            groups: ["F", "G", "H", "I"],
            grandFinal: "none",
            advance: [{ source: "winner:11", label: "Winner" }, { source: "winner:13", label: "2nd Place" }],
          }),
          qualificationPath({
            key: "Q3",
            groups: ["X", "Y", "Z"],
            grandFinal: "reset",
            // Match 15 is played only when the lower-bracket finalist wins match 14.
            advance: [{ source: "winner:14|15", label: "Winner" }],
          }),
        ],
      },
      // League Stage and Playoffs are created as empty containers until their formats are set up.
      { key: "league", name: "League Stage", format: "league", children: [] },
      { key: "playoffs", name: "Playoffs", format: "single_elimination", children: [] },
    ],
  },
]);

const text = (value) => String(value ?? "").trim();

export function listCclFormats() {
  return CCL_FORMATS.map(({ key, name, available }) => ({ key, name, available }));
}

export function getCclFormat(key) {
  const wanted = text(key).toLowerCase();
  return CCL_FORMATS.find((format) => format.key === wanted) || null;
}

// Validates format_key for a tournament write. Only CCL tournaments carry a format;
// any other category always stores NULL.
export function resolveTournamentFormatKeyInput(body, currentRow, { isCcl }) {
  const sent = Object.prototype.hasOwnProperty.call(body || {}, "format_key");
  if (!isCcl) {
    if (sent && text(body.format_key)) return { error: "format_key is supported only for CCL tournaments" };
    return { value: null };
  }
  if (!sent) return { value: text(currentRow?.format_key) || null };
  if (!text(body.format_key)) return { value: null };
  const format = getCclFormat(body.format_key);
  if (!format) {
    return { error: `format_key must be one of ${CCL_FORMATS.map(({ key }) => key).join(", ")}` };
  }
  // A saved key stays valid even if it is not offered for new tournaments.
  if (!format.available && format.key !== text(currentRow?.format_key).toLowerCase()) {
    return { error: `${format.name} format is not available yet` };
  }
  return { value: format.key };
}
