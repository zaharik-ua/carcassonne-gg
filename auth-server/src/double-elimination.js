// Double Elimination bracket of one group, as match templates without players.
// A player slot is described by its source:
//   draw:<position>  — position in the group draw (odd = Seeded, even = Unseeded);
//   winner:<n>       — winner of match n of the same group;
//   loser:<n>        — loser of match n of the same group.
// week = the week of the path in which the match is played (round_order of the duel).

export const GRAND_FINAL_MODES = Object.freeze(["none", "reset"]);

const match = (bracketNo, side, week, roundName, p1Source, p2Source, extra = {}) => ({
  bracket_no: bracketNo,
  bracket_side: side,
  week,
  round_name: roundName,
  p1_source: p1Source,
  p2_source: p2Source,
  is_conditional: false,
  ...extra,
});

// 8 players: 13 matches, plus Group Final and its reset with grandFinal = "reset".
// The upper semifinal losers cross over (7 → 10, 8 → 9) so they do not meet a player
// from their own half of the bracket again straight away.
function buildEightPlayerGroup(grandFinal) {
  const matches = [
    match(1, "upper", 1, "Round 1", "draw:1", "draw:2"),
    match(2, "upper", 1, "Round 1", "draw:3", "draw:4"),
    match(3, "upper", 1, "Round 1", "draw:5", "draw:6"),
    match(4, "upper", 1, "Round 1", "draw:7", "draw:8"),
    match(5, "lower", 2, "Lower Round 1", "loser:1", "loser:2"),
    match(6, "lower", 2, "Lower Round 1", "loser:3", "loser:4"),
    match(7, "upper", 2, "Upper Semifinals", "winner:1", "winner:2"),
    match(8, "upper", 2, "Upper Semifinals", "winner:3", "winner:4"),
    match(9, "lower", 3, "Lower Semifinals", "loser:8", "winner:5"),
    match(10, "lower", 3, "Lower Semifinals", "loser:7", "winner:6"),
    match(11, "upper", 3, "Upper Final", "winner:7", "winner:8"),
    match(12, "lower", 4, "Lower Final", "winner:9", "winner:10"),
    match(13, "lower", 5, "Consolidated Final", "loser:11", "winner:12"),
  ];
  if (grandFinal === "reset") {
    matches.push(
      match(14, "final", 6, "Group Final", "winner:11", "winner:13"),
      // Played only when the lower-bracket finalist (player 2) wins match 14: the same two players meet again.
      match(15, "final", 6, "Group Final Reset", "winner:11", "winner:13", { is_conditional: true, depends_on: 14 }),
    );
  }
  return matches;
}

export function buildDoubleEliminationGroup({ groupSize, grandFinal }) {
  if (!GRAND_FINAL_MODES.includes(grandFinal)) {
    throw new Error(`grand_final must be one of ${GRAND_FINAL_MODES.join(", ")}`);
  }
  if (Number(groupSize) !== 8) throw new Error("Double Elimination groups of 8 players only");
  return buildEightPlayerGroup(grandFinal);
}

// "draw:3" → { kind: "draw", ref: 3 }
export function parseSlotSource(source) {
  const match = /^(draw|winner|loser):(\d+)$/.exec(String(source ?? "").trim());
  return match ? { kind: match[1], ref: Number(match[2]) } : null;
}

// Placeholder shown in the UI before the slot is filled.
export function describeSlotSource(source) {
  const parsed = parseSlotSource(source);
  if (!parsed) return "";
  if (parsed.kind === "draw") {
    const pairNo = Math.ceil(parsed.ref / 2);
    return parsed.ref % 2 === 1 ? `Seeded #${pairNo}` : `Unseeded #${pairNo}`;
  }
  return `${parsed.kind === "winner" ? "Winner" : "Loser"} of ${parsed.ref}`;
}

// Where the winner / loser of each match goes next (bracket numbers), for the public bracket view.
// Match 15 (reset) takes the players of match 14 itself, so it is the next match of match 14 for both.
export function buildBracketLinks(matches) {
  const links = new Map(matches.map((item) => [item.bracket_no, { next_win: null, next_lose: null }]));
  for (const item of matches) {
    if (item.is_conditional) continue;
    for (const source of [item.p1_source, item.p2_source]) {
      const parsed = parseSlotSource(source);
      if (!parsed || parsed.kind === "draw" || !links.has(parsed.ref)) continue;
      links.get(parsed.ref)[parsed.kind === "winner" ? "next_win" : "next_lose"] = item.bracket_no;
    }
  }
  for (const reset of matches.filter((item) => item.is_conditional && links.has(item.depends_on))) {
    links.set(reset.depends_on, { next_win: reset.bracket_no, next_lose: reset.bracket_no });
  }
  return links;
}
