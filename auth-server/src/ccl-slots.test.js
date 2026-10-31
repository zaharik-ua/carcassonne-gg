import assert from "node:assert/strict";
import test from "node:test";

import {
  CCL_LEAGUE_POTS,
  CCL_LEAGUE_POT_SIZE,
  CCL_QUALIFICATION_POTS,
  buildCclEloAssignments,
  buildCclPlayerLists,
  formCclPots,
  getCclPotsBlockers,
  getCclLeagueResultError,
  getCclPhasesAffectedByPlayerChange,
  getCclRatedElo,
  getCclSlotUpdateError,
  getCclStageDeadline,
  getCclStagePhase,
  isCclStageRegistrationOpen,
  isCclTournament,
  normalizeCclManualElo,
  normalizeCclSlotCreateInput,
  normalizeCclStage,
} from "./ccl-slots.js";

const now = Date.parse("2026-11-05T12:00:00Z");
const tournament = {
  id: "CCL-2027",
  type: "Individuals",
  category: "CCL",
  ccl_q12_registration_ends_at: "2026-11-11T23:59:00Z",
  ccl_q3_registration_ends_at: "2026-11-01T23:59:00Z",
  ccl_main_registration_ends_at: "",
};

test("CCL tournaments and stages are recognised", () => {
  assert.equal(isCclTournament({ type: "Individuals", category: "ccl" }), true);
  assert.equal(isCclTournament({ type: "Teams", category: "CCL" }), false);
  assert.equal(isCclTournament({ type: "Individuals", category: "Rivals" }), false);
  assert.equal(normalizeCclStage(" q2 "), "Q2");
  assert.equal(normalizeCclStage("Q4"), null);
  assert.equal(getCclStagePhase("Main"), "league");
  assert.equal(getCclStagePhase("Q3"), "qualification");
  assert.equal(getCclStagePhase(""), null);
});

test("stage deadlines: Q1 and Q2 share one, Q3 and Main have their own; empty means open", () => {
  assert.equal(getCclStageDeadline(tournament, "Q1"), "2026-11-11T23:59:00Z");
  assert.equal(getCclStageDeadline(tournament, "Q2"), "2026-11-11T23:59:00Z");
  assert.equal(getCclStageDeadline(tournament, "Q3"), "2026-11-01T23:59:00Z");
  assert.equal(getCclStageDeadline(tournament, "Main"), null);
  assert.equal(isCclStageRegistrationOpen(tournament, "Q1", now), true);
  assert.equal(isCclStageRegistrationOpen(tournament, "Q3", now), false);
  assert.equal(isCclStageRegistrationOpen(tournament, "Main", now), true);
  assert.equal(isCclStageRegistrationOpen(tournament, "Q1", Date.parse("2026-11-11T23:59:00Z")), false);
});

test("admin slot input: association is optional, stage and Qualified by are validated", () => {
  assert.deepEqual(
    normalizeCclSlotCreateInput({ entry_stage: "q3", qualified_by_name: " Fans Choice ", count: "2" }),
    {
      value: {
        association_id: null,
        entry_stage: "Q3",
        qualified_by_name: "Fans Choice",
        qualified_by_link: null,
        count: 2,
      },
    }
  );
  assert.equal(
    normalizeCclSlotCreateInput({
      association_id: "AR",
      entry_stage: "Main",
      qualified_by_name: "Copa America Winner",
      qualified_by_link: "https://carcassonne.gg/Copa-America-2026/",
    }).value.qualified_by_link,
    "https://carcassonne.gg/Copa-America-2026/"
  );
  for (const input of [
    { entry_stage: "Q4", qualified_by_name: "X" },
    { entry_stage: "Q1", qualified_by_name: "  " },
    { entry_stage: "Q1", qualified_by_name: "X", qualified_by_link: "javascript:alert(1)" },
    { entry_stage: "Q1", qualified_by_name: "X", count: 0 },
    { entry_stage: "Q1", qualified_by_name: "X", count: 61 },
    { entry_stage: "Q1", qualified_by_name: "X", count: 1.5 },
  ]) {
    assert.ok(normalizeCclSlotCreateInput(input).error, JSON.stringify(input));
  }
});

const qSlot = { id: "s1", association_id: "AR", entry_stage: "Q1", player_id: null, league_status: null };
const captainChange = (overrides) => getCclSlotUpdateError({
  slot: qSlot,
  changes: { player_id: "p-ar" },
  captainAssociationId: "ar",
  tournament,
  player: { id: "p-ar", association: "AR" },
  now,
  ...overrides,
});

test("captains fill and edit slots of their association before the stage deadline", () => {
  assert.equal(captainChange(), null);
  assert.equal(captainChange({ changes: { qualified_by_name: "Argentina Championship 2026", qualified_by_link: "" } }), null);
  assert.match(captainChange({ captainAssociationId: "UA" }), /their association/);
  assert.match(captainChange({ slot: { ...qSlot, association_id: null } }), /their association/);
  assert.match(captainChange({ changes: { entry_stage: "Q2" } }), /cannot change entry_stage/);
  assert.match(captainChange({ changes: { association_id: "UA" } }), /cannot change association_id/);
  assert.match(captainChange({ slot: { ...qSlot, entry_stage: "Q3" } }), /closed/);
  assert.match(captainChange({ changes: { qualified_by_name: "" } }), /qualified_by_name/);
});

test("only the stage deadline locks captains; Elo issued early does not", () => {
  const filled = { ...qSlot, player_id: "p-ar" };
  assert.equal(captainChange({ slot: filled, changes: { player_id: null } }), null);
  assert.equal(captainChange({ slot: filled, changes: { qualified_by_name: "Local Cup" } }), null);
  const closed = { ...filled, entry_stage: "Q3" };
  assert.match(captainChange({ slot: closed, changes: { player_id: null } }), /closed/);
  assert.match(captainChange({ slot: closed, changes: { qualified_by_name: "Local Cup" } }), /closed/);
});

test("players must exist, belong to the slot association and hold one slot per tournament", () => {
  assert.match(captainChange({ player: null }), /Player not found/);
  assert.match(captainChange({ player: { id: "p-ar", association: "UA" } }), /does not belong/);
  assert.match(captainChange({ playerSlotId: "s2" }), /already has a slot/);
  assert.equal(captainChange({ playerSlotId: "S1" }), null);
});

test("admins change any slot at any time, within the slot rules", () => {
  const admin = (overrides) => getCclSlotUpdateError({ isAdmin: true, tournament, now, ...overrides });
  const unassigned = { ...qSlot, association_id: null, entry_stage: "Q3" };

  assert.equal(admin({ slot: { ...qSlot, entry_stage: "Q3" }, changes: { player_id: "p-ar" }, player: { id: "p-ar", association: "AR" } }), null);
  assert.equal(admin({ slot: unassigned, changes: { association_id: "UA" } }), null);
  assert.match(admin({ slot: unassigned, changes: { player_id: "p-ar" }, player: { id: "p-ar", association: "AR" } }), /without an association/);
  assert.equal(admin({ slot: unassigned, changes: { association_id: "AR", player_id: "p-ar" }, player: { id: "p-ar", association: "AR" } }), null);

  const filled = { ...qSlot, player_id: "p-ar" };
  assert.match(admin({ slot: filled, changes: { association_id: "UA" }, player: { id: "p-ar", association: "AR" } }), /does not belong/);
  assert.match(admin({ slot: filled, changes: { association_id: null } }), /without an association/);
  assert.match(admin({ slot: { ...filled, league_status: "eliminated" }, changes: { entry_stage: "Main" } }), /qualification result/);
  assert.match(admin({ slot: qSlot, changes: { entry_stage: "Q5" } }), /entry_stage/);
  assert.match(admin({ slot: qSlot, changes: { league_status: "qualified" } }), /Unknown slot field/);
});

test("a player change resets pots of the entry phase and, for qualified slots, of the league", () => {
  assert.deepEqual(getCclPhasesAffectedByPlayerChange({ entry_stage: "Main" }), ["league"]);
  assert.deepEqual(getCclPhasesAffectedByPlayerChange({ entry_stage: "Q2" }), ["qualification"]);
  assert.deepEqual(
    getCclPhasesAffectedByPlayerChange({ entry_stage: "Q2", league_status: "qualified" }),
    ["qualification", "league"]
  );
});

test("qualification results: only Q slots; qualified needs a player and League Qualified by", () => {
  const slot = { ...qSlot, player_id: "p-ar" };
  assert.equal(getCclLeagueResultError({ slot, league_status: "qualified", qualified_by_name: "Q1, Group B Winner" }), null);
  assert.equal(getCclLeagueResultError({ slot: qSlot, league_status: "eliminated" }), null);
  assert.equal(getCclLeagueResultError({ slot, league_status: null }), null);
  assert.match(getCclLeagueResultError({ slot, league_status: "qualified" }), /qualified_by_name/);
  assert.match(getCclLeagueResultError({ slot: qSlot, league_status: "qualified", qualified_by_name: "X" }), /empty slot/);
  assert.match(getCclLeagueResultError({ slot, league_status: "won" }), /league_status/);
  assert.match(getCclLeagueResultError({ slot: { ...slot, entry_stage: "Main" }, league_status: "eliminated" }), /only to Q1/);
});

const slots = [
  { id: "ar-main", association_id: "AR", entry_stage: "Main", player_id: "juan", league_status: null },
  { id: "ar-q1", association_id: "AR", entry_stage: "Q1", player_id: "pablo", league_status: "qualified" },
  { id: "ar-q3", association_id: "AR", entry_stage: "Q3", player_id: "leo", league_status: "eliminated" },
  { id: "ua-q1", association_id: "UA", entry_stage: "Q1", player_id: "olena", league_status: "eliminated" },
  { id: "ua-q1b", association_id: "UA", entry_stage: "Q1", player_id: "taras", league_status: "qualified" },
  { id: "ua-main", association_id: "UA", entry_stage: "Main", player_id: "ivan", league_status: null },
];
const profilesById = new Map([
  ["juan", { id: "juan", bga_nickname: "Juan123", gg_elo: 1810, gg_rating_position: 3 }],
  ["pablo", { id: "pablo", bga_nickname: "pablo_ar", gg_elo: 1500, gg_rating_position: null }],
  ["leo", { id: "leo", bga_nickname: "Leo_ar", gg_elo: 1700.5, gg_rating_position: 9 }],
  ["olena", { id: "olena", bga_nickname: "Olena", gg_elo: 1700.5, gg_rating_position: 7 }],
  ["taras", { id: "taras", bga_nickname: "taras", gg_elo: null, gg_rating_position: null, avatar: "a.png" }],
  ["ivan", { id: "ivan", bga_nickname: "Ivan", gg_elo: "1650", gg_rating_position: 12 }],
]);
const ratingPositionByPlayerId = new Map(
  [...profilesById.values()].map((profile) => [profile.id, profile.gg_rating_position])
);

test("rated Elo excludes missing values and the initial 1500", () => {
  assert.equal(getCclRatedElo(1810), 1810);
  assert.equal(getCclRatedElo("1650"), 1650);
  assert.equal(getCclRatedElo(1500), null);
  assert.equal(getCclRatedElo(null), null);
  assert.equal(getCclRatedElo(""), null);
  assert.deepEqual(normalizeCclManualElo("1725.5"), { value: 1725.5 });
  assert.ok(normalizeCclManualElo("").error);
  assert.ok(normalizeCclManualElo(-1).error);
});

test("Elo is issued early to filled slots; empty, unassigned or undecided slots only block pots", () => {
  const withGaps = [
    ...slots,
    { id: "tbd", association_id: null, entry_stage: "Q3", player_id: null, league_status: null },
    { id: "ua-q2", association_id: "UA", entry_stage: "Q2", player_id: null, league_status: null },
  ];
  assert.deepEqual(getCclPotsBlockers(withGaps, "qualification"), [
    { slot_id: "tbd", reason: "association_missing" },
    { slot_id: "ua-q2", reason: "player_missing" },
  ]);
  assert.deepEqual(getCclPotsBlockers(withGaps, "league"), [
    { slot_id: "tbd", reason: "league_status_pending" },
    { slot_id: "ua-q2", reason: "league_status_pending" },
  ]);
  const early = buildCclEloAssignments({ slots: withGaps, phase: "qualification", profilesById });
  assert.deepEqual(early.assignments.map((row) => row.slot_id), ["ar-q1", "ar-q3", "ua-q1", "ua-q1b"]);
  assert.deepEqual(early.skipped, [
    { slot_id: "tbd", reason: "player_missing" },
    { slot_id: "ua-q2", reason: "player_missing" },
  ]);
  assert.equal(buildCclEloAssignments({ slots: withGaps, phase: "league", profilesById }).assignments.length, 4);
});

test("setting Elo writes GG Elo per phase entry; players without rating get null", () => {
  const qualification = buildCclEloAssignments({ slots, phase: "qualification", profilesById });
  assert.deepEqual(qualification.skipped, []);
  assert.deepEqual(
    qualification.assignments.map(({ slot_id: slotId, stage, elo }) => [slotId, stage, elo]),
    [["ar-q1", "Q1", null], ["ar-q3", "Q3", 1700.5], ["ua-q1", "Q1", 1700.5], ["ua-q1b", "Q1", null]]
  );
  assert.ok(qualification.assignments.every((row) => row.elo_source === "gg_elo" && row.seed === null));

  const league = buildCclEloAssignments({ slots, phase: "league", profilesById });
  assert.deepEqual(
    league.assignments.map(({ slot_id: slotId, stage, player_id: playerId, elo }) => [slotId, stage, playerId, elo]),
    [["ar-main", "Main", "juan", 1810], ["ar-q1", "Main", "pablo", null], ["ua-q1b", "Main", "taras", null], ["ua-main", "Main", "ivan", 1650]]
  );
});

const fixed = (slotId, stage, playerId, elo, seed = null) => ({
  slot_id: slotId,
  stage,
  player_id: playerId,
  elo,
  seed,
  elo_source: "gg_elo",
  fixed_at: "2026-11-12T10:00:00Z",
  qualified_by_name: stage === "Main" ? `${slotId} league` : `${slotId} entry`,
  qualified_by_link: null,
});

function stagePlayers(stage, count, eloFor) {
  const pathSlots = [];
  const entries = [];
  for (let index = 1; index <= count; index += 1) {
    const id = `${stage}-${String(index).padStart(2, "0")}`;
    pathSlots.push({ id, association_id: "AR", entry_stage: stage, player_id: `p-${id}`, league_status: null });
    entries.push(fixed(id, stage, `p-${id}`, eloFor(index)));
  }
  return { slots: pathSlots, entries };
}

test("qualification paths have their own pots: Q1 20, Q2 16, Q3 12 Seeded; unrated always Unseeded", () => {
  assert.deepEqual(CCL_QUALIFICATION_POTS, {
    Q1: { seeded: 20, max_players: 40 },
    Q2: { seeded: 16, max_players: 32 },
    Q3: { seeded: 12, max_players: 24 },
  });
  // Q3: 24 players, 14 rated; places 12 and 13 share an Elo and the GG rating position decides.
  const q3 = stagePlayers("Q3", 24, (index) => (index > 14 ? null : index === 13 ? 1880 : 2000 - index * 10));
  // Q2: 20 players (up to 32 allowed), exactly 16 rated.
  const q2 = stagePlayers("Q2", 20, (index) => (index > 16 ? null : 1900 - index));
  const result = formCclPots({
    slots: [...q2.slots, ...q3.slots],
    entries: [...q2.entries, ...q3.entries],
    phase: "qualification",
    ratingPositionByPlayerId: new Map([["p-Q3-13", 1], ["p-Q3-12", 50]]),
  });
  assert.equal(result.error, undefined);
  const seeded = (stage) => result.seeds.filter((row) => row.stage === stage && row.seed === "Seeded").map((row) => row.slot_id);
  assert.deepEqual(seeded("Q2"), q2.slots.slice(0, 16).map((slot) => slot.id));
  assert.equal(seeded("Q3").length, 12);
  assert.ok(seeded("Q3").includes("Q3-13"));
  assert.ok(!seeded("Q3").includes("Q3-12"));
  assert.equal(result.seeds.filter((row) => row.stage === "Q3" && row.seed === "Unseeded").length, 12);
  assert.ok(q3.slots.slice(14).every((slot) => result.seeds.find((row) => row.slot_id === slot.id).seed === "Unseeded"));

  const tooMany = stagePlayers("Q3", 25, () => 1700);
  assert.match(formCclPots({ ...tooMany, phase: "qualification" }).error, /Q3 has 25 players; at most 24/);
  const fewRated = stagePlayers("Q2", 20, (index) => (index > 15 ? null : 1700));
  assert.match(formCclPots({ ...fewRated, phase: "qualification" }).error, /Q2 needs 16 rated players for Seeded, but has 15/);
});

test("League Stage has five pots of 12: Pot 1-4 by Elo, Pot 5 the rest and every unrated player", () => {
  assert.equal(CCL_LEAGUE_POT_SIZE, 12);
  // 60 players: 52 rated, 8 without rating (initial 1500 or none).
  const league = stagePlayers("Main", 60, (index) => (index > 52 ? null : 2100 - index));
  const result = formCclPots({ ...league, phase: "league" });
  assert.equal(result.error, undefined);
  const potOf = (index) => result.seeds.find((row) => row.slot_id === `Main-${String(index).padStart(2, "0")}`).seed;
  assert.deepEqual(CCL_LEAGUE_POTS.map((pot) => result.seeds.filter((row) => row.seed === pot).length), [12, 12, 12, 12, 12]);
  assert.deepEqual([1, 12, 13, 24, 25, 36, 37, 48, 49, 52, 53, 60].map(potOf), [
    "Pot 1", "Pot 1", "Pot 2", "Pot 2", "Pot 3", "Pot 3", "Pot 4", "Pot 4", "Pot 5", "Pot 5", "Pot 5", "Pot 5",
  ]);

  // Fewer than 60 players: Pot 5 is smaller.
  const smaller = stagePlayers("Main", 50, (index) => 2100 - index);
  const smallerPots = formCclPots({ ...smaller, phase: "league" }).seeds;
  assert.equal(smallerPots.filter((row) => row.seed === "Pot 5").length, 2);

  const tooMany = stagePlayers("Main", 61, () => 1700);
  assert.match(formCclPots({ ...tooMany, phase: "league" }).error, /61 players; at most 60/);
  const fewRated = stagePlayers("Main", 60, (index) => (index > 47 ? null : 1700));
  assert.match(formCclPots({ ...fewRated, phase: "league" }).error, /needs 48 rated players for Pot 1-4, but has 47/);
});

test("pots need Elo set for the current player of every phase slot", () => {
  const entries = [
    fixed("ar-main", "Main", "juan", 1810),
    fixed("ar-q1", "Main", "pablo", null),
    fixed("ua-q1b", "Main", "someone-replaced", 1600),
  ];
  const result = formCclPots({ slots, entries, phase: "league" });
  assert.match(result.error, /Elo must be set/);
  assert.deepEqual(result.blockers, [
    { slot_id: "ua-q1b", reason: "elo_not_set" },
    { slot_id: "ua-main", reason: "elo_not_set" },
  ]);
  assert.match(formCclPots({ slots, entries, phase: "groups" }).error, /phase/);
});

test("public lists: all slots with Q Elo/seeding, and League Stage players with Main entries", () => {
  const listSlots = [
    ...slots,
    { id: "tbd", association_id: null, entry_stage: "Q3", player_id: null, league_status: null },
    { id: "ar-q2", association_id: "AR", entry_stage: "Q2", player_id: null, league_status: null },
  ];
  const entries = [
    fixed("ar-main", "Main", "juan", 1810, "Pot 1"),
    { ...fixed("ar-q1", "Q1", "pablo", null, "Unseeded"), qualified_by_name: "Association ranking" },
    fixed("ar-q1", "Main", "pablo", null, "Pot 5"),
    fixed("ar-q3", "Q3", "leo", 1700.5, "Seeded"),
    fixed("ua-q1", "Q1", "olena", 1700.5, "Seeded"),
    fixed("ua-q1b", "Q1", "someone-replaced", 1600, "Seeded"),
    fixed("ua-q1b", "Main", "taras", null),
    { ...fixed("ua-main", "Main", null, null), player_id: null, elo_source: null, fixed_at: null },
    { slot_id: "ar-q2", stage: "Q2", qualified_by_name: "Copa America Winner", qualified_by_link: "https://carcassonne.gg/Copa-America-2026/" },
    { slot_id: "tbd", stage: "Q3", qualified_by_name: "Fans Choice", qualified_by_link: null },
  ];
  const associationsById = new Map([
    ["AR", { code: "AR", name: "Argentina", flag: "ar.png" }],
    ["UA", { code: "UA", name: "Ukraine", flag: "ua.png" }],
  ]);
  const { all_players: allPlayers, league_players: leaguePlayers } = buildCclPlayerLists({
    slots: listSlots,
    entries,
    profilesById,
    associationsById,
  });

  assert.deepEqual(
    allPlayers.map((row) => [row.country, row.stage, row.player, row.elo, row.has_rating, row.seeding, row.qualified_by_name]),
    [
      ["Argentina", "Main", "Juan123", null, false, null, "ar-main league"],
      ["Argentina", "Q1", "pablo_ar", null, false, "Unseeded", "Association ranking"],
      ["Argentina", "Q2", "", null, false, null, "Copa America Winner"],
      ["Argentina", "Q3", "Leo_ar", 1700.5, true, "Seeded", "ar-q3 entry"],
      ["Ukraine", "Main", "Ivan", null, false, null, "ua-main league"],
      ["Ukraine", "Q1", "Olena", 1700.5, true, "Seeded", "ua-q1 entry"],
      ["Ukraine", "Q1", "taras", null, false, null, "ua-q1b entry"],
      ["", "Q3", "", null, false, null, "Fans Choice"],
    ]
  );
  const pablo = allPlayers.find((row) => row.slot_id === "ar-q1");
  assert.equal(pablo.elo_fixed, true);
  assert.equal(pablo.league_status, "qualified");
  assert.equal(allPlayers.find((row) => row.slot_id === "ua-q1b").elo_fixed, false);
  assert.equal(allPlayers.find((row) => row.slot_id === "ar-q2").qualified_by_link, "https://carcassonne.gg/Copa-America-2026/");
  assert.equal(allPlayers.find((row) => row.slot_id === "tbd").association_id, null);

  assert.deepEqual(
    leaguePlayers.map((row) => [row.country, row.player, row.stage, row.elo, row.elo_fixed, row.pot, row.qualified_by_name]),
    [
      ["Argentina", "Juan123", "Main", 1810, true, "Pot 1", "ar-main league"],
      ["Argentina", "pablo_ar", "Q1", null, true, "Pot 5", "ar-q1 league"],
      ["Ukraine", "Ivan", "Main", null, false, null, "ua-main league"],
      ["Ukraine", "taras", "Q1", null, true, null, "ua-q1b league"],
    ]
  );
  assert.equal(leaguePlayers.find((row) => row.player === "taras").avatar, "a.png");
  assert.equal(leaguePlayers[0].flag, "ar.png");
});
