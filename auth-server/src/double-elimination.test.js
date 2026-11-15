import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  buildBracketLinks,
  buildDoubleEliminationGroup,
  describeSlotSource,
  parseSlotSource,
} from "./double-elimination.js";

// The CCL 2027 Qualifiers bracket as it was run from Google Sheets: groups A (Q1), F (Q2), X (Q3).
const fixture = JSON.parse(readFileSync(new URL("./fixtures/ccl-2027q-brackets.json", import.meta.url), "utf8")).duels;
const PATH_STARTS = { A: "2026-11-23", F: "2026-11-23", X: "2026-11-16" };
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
// Names agreed for CCL 2027 that differ from the sheet.
const RENAMED_ROUNDS = { "Losers Round 1": "Lower Round 1", "GF rematch (if needed)": "Group Final Reset" };

function sheetGroup(group) {
  return fixture.filter((row) => row.group === group && row.player1 !== null);
}

// "Lower Final: 14-20.Dec" → { name: "Lower Final", week } counted from the path start.
function parseSheetRound(round, group) {
  const [name, dates] = round.split(": ");
  const [, day, month] = /^(\d+)(?:\.(\w+))?-/.exec(dates);
  const endMonth = /\.(\w+)$/.exec(dates)[1];
  const monthIndex = MONTHS.indexOf(month || endMonth);
  const start = Date.UTC(2026, monthIndex, Number(day));
  const week = (start - Date.parse(`${PATH_STARTS[group]}T00:00:00Z`)) / (7 * 86400000) + 1;
  return { name: RENAMED_ROUNDS[name] || name, week };
}

// The sheet writes the source of a player as text; draw slots are "Seeded" / "Unseeded" in order.
function sheetSources(row) {
  const no = Number(row.knockout_id);
  return [row.player1, row.player2].map((value, index) => {
    if (value === "Seeded" || value === "Unseeded") return `draw:${(no - 1) * 2 + index + 1}`;
    const named = /^(Winner|Loser) of (\d+)$/.exec(value || "");
    return named ? `${named[1].toLowerCase()}:${named[2]}` : null;
  });
}

for (const [group, grandFinal, size] of [["A", "none", 13], ["F", "none", 13], ["X", "reset", 15]]) {
  test(`group ${group}: generated bracket matches the sheet (${grandFinal === "reset" ? "with" : "without"} Group Final)`, () => {
    const generated = buildDoubleEliminationGroup({ groupSize: 8, grandFinal });
    const sheet = sheetGroup(group);
    assert.equal(generated.length, size);
    assert.deepEqual(generated.map((item) => item.bracket_no), sheet.map((row) => Number(row.knockout_id)));
    const links = buildBracketLinks(generated);

    for (const row of sheet) {
      const item = generated.find((candidate) => candidate.bracket_no === Number(row.knockout_id));
      const label = `${group}${item.bracket_no}`;
      const round = parseSheetRound(row.round, group);
      assert.equal(item.round_name, round.name, label);
      assert.equal(item.week, round.week, label);
      assert.equal(item.bracket_side === "lower" ? "2 - lower" : "1 - upper", row.multiple_elimination_stage, label);

      // Every source the sheet spells out is the same in the generated bracket.
      const sources = sheetSources(row);
      [item.p1_source, item.p2_source].forEach((source, index) => {
        if (sources[index]) assert.equal(source, sources[index], `${label} player ${index + 1}`);
      });
      // The sheet's next_game_win points at the match the winner plays next, or at an "Advance" row.
      // X13 has it empty in the sheet; the Q3 test below checks that link.
      if (!row.next_game_win) continue;
      const next = Number(row.next_game_win);
      const isMatch = generated.some((candidate) => candidate.bracket_no === next);
      if (isMatch) assert.equal(links.get(item.bracket_no).next_win, next, `${label} next win`);
      else assert.equal(links.get(item.bracket_no).next_win, null, `${label} leaves the group`);
    }
  });
}

test("Q3 Group Final: the reset is conditional and takes the same two players", () => {
  const generated = buildDoubleEliminationGroup({ groupSize: 8, grandFinal: "reset" });
  const [final, reset] = generated.slice(-2);
  assert.deepEqual([final.p1_source, final.p2_source], ["winner:11", "winner:13"]);
  assert.deepEqual([reset.p1_source, reset.p2_source], ["winner:11", "winner:13"]);
  assert.equal(final.is_conditional, false);
  assert.equal(reset.is_conditional, true);
  assert.equal(reset.depends_on, 14);
  // The sheet leaves next_game_win of match 13 empty; it goes to the Group Final.
  assert.equal(buildBracketLinks(generated).get(13).next_win, 14);
});

test("every player except the group winner(s) is eliminated after two losses", () => {
  for (const grandFinal of ["none", "reset"]) {
    const generated = buildDoubleEliminationGroup({ groupSize: 8, grandFinal });
    const links = buildBracketLinks(generated);
    // Upper-bracket losers drop to the lower bracket; lower-bracket losers are out.
    for (const item of generated.filter((candidate) => candidate.bracket_no <= 13)) {
      const { next_lose: nextLose } = links.get(item.bracket_no);
      if (item.bracket_side === "upper") assert.ok(nextLose, `${grandFinal} ${item.bracket_no}`);
      else assert.equal(nextLose, null, `${grandFinal} ${item.bracket_no}`);
    }
    // Each draw position appears exactly once, in Round 1.
    const draws = generated.flatMap((item) => [item.p1_source, item.p2_source]).filter((source) => source.startsWith("draw:"));
    assert.deepEqual(draws, ["draw:1", "draw:2", "draw:3", "draw:4", "draw:5", "draw:6", "draw:7", "draw:8"]);
  }
});

test("placeholders and validation", () => {
  assert.deepEqual(parseSlotSource(" loser:8 "), { kind: "loser", ref: 8 });
  assert.equal(parseSlotSource("bye"), null);
  assert.deepEqual(["draw:1", "draw:2", "draw:7", "draw:8", "winner:5", "loser:11"].map(describeSlotSource), [
    "Seeded #1", "Unseeded #1", "Seeded #4", "Unseeded #4", "Winner of 5", "Loser of 11",
  ]);
  assert.throws(() => buildDoubleEliminationGroup({ groupSize: 16, grandFinal: "none" }), /8 players/);
  assert.throws(() => buildDoubleEliminationGroup({ groupSize: 8, grandFinal: "twice" }), /grand_final/);
});
