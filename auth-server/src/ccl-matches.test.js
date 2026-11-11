import assert from "node:assert/strict";
import test from "node:test";

import {
  compareCclMatches,
  getCclEligiblePlayerIds,
  getCclMatchPlayersError,
  normalizeCclMatchInput,
} from "./ccl-matches.js";

const SLOTS = [
  { player_id: "juan", entry_stage: "Main" },
  { player_id: "leo", entry_stage: "Q1", league_status: "qualified" },
  { player_id: "olena", entry_stage: "Q1" },
  { player_id: "taras", entry_stage: "Q1", league_status: "eliminated" },
  { player_id: "max", entry_stage: "Q2" },
  { player_id: null, entry_stage: "Q1" },
];

test("normalizes the admin match form", () => {
  assert.deepEqual(normalizeCclMatchInput({
    stage: "league stage",
    group_name: " A ",
    round_name: "Round 1",
    round_order: "1",
    player_1_id: "juan",
    player_2_id: "leo",
    duel_format: "Bo3",
    deadline_utc: "2026-12-14 23:59",
  }).value, {
    stage: "Main",
    group_name: "A",
    round_name: "Round 1",
    round_order: 1,
    player_1_id: "juan",
    player_2_id: "leo",
    duel_format: "Bo3",
    time_utc: null,
    deadline_utc: "2026-12-14T23:59:00.000Z",
  });
  assert.match(normalizeCclMatchInput({ stage: "Q4" }).error, /stage/);
  assert.match(normalizeCclMatchInput({ stage: "Q1", player_1_id: "a" }).error, /player_2_id/);
  assert.deepEqual(normalizeCclMatchInput({ group_name: "" }, { partial: true }).value, { group_name: null });
  assert.match(normalizeCclMatchInput({ time_utc: "soon" }, { partial: true }).error, /time_utc/);
});

test("pairs only players registered for the stage", () => {
  assert.deepEqual(getCclEligiblePlayerIds(SLOTS, "Q1"), ["leo", "olena", "taras"]);
  assert.deepEqual(getCclEligiblePlayerIds(SLOTS, "Main"), ["juan", "leo"]);
  assert.deepEqual(getCclEligiblePlayerIds(SLOTS, "Playoffs"), ["juan", "leo"]);
  assert.equal(getCclMatchPlayersError({ stage: "Q1", player1Id: "olena", player2Id: "taras", slots: SLOTS }), null);
  assert.match(getCclMatchPlayersError({ stage: "Q1", player1Id: "olena", player2Id: "max", slots: SLOTS }), /Q1.*max/);
  assert.match(getCclMatchPlayersError({ stage: "Main", player1Id: "juan", player2Id: "olena", slots: SLOTS }), /League/);
  assert.match(getCclMatchPlayersError({ stage: "Q1", player1Id: "leo", player2Id: "leo", slots: SLOTS }), /themselves/);
});

test("sorts by stage, round, group and time", () => {
  const rows = [
    { id: "c", stage: "Main", round_order: 1 },
    { id: "b", stage: "Q1", round_order: 2, group_name: "A" },
    { id: "a2", stage: "Q1", round_order: 1, group_name: "B" },
    { id: "a1", stage: "Q1", round_order: 1, group_name: "A", time_utc: "2026-11-20T18:00:00Z" },
    { id: "a0", stage: "Q1", round_order: 1, group_name: "A" },
  ];
  assert.deepEqual(rows.sort(compareCclMatches).map((row) => row.id), ["a1", "a0", "a2", "b", "c"]);
});
