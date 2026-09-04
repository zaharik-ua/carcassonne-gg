import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import {
  calculateSwissStandings,
  InPersonEngineError,
  pairFirstSwissRound,
  pairNextSwissRound,
  validateMatchResult,
} from "./in-person/engine.js";

function normalMatch(overrides = {}) {
  return {
    id: "match-1",
    participant_a_id: "p1",
    participant_b_id: "p2",
    starting_participant_id: "p1",
    status: "completed",
    is_bye: false,
    ...overrides,
  };
}

function completedRound(roundNumber, matches, overrides = {}) {
  return {
    id: `round-${roundNumber}`,
    stage: "swiss",
    round_number: roundNumber,
    status: "completed",
    matches,
    ...overrides,
  };
}

function completeMatch(match, result) {
  return { ...match, ...validateMatchResult(match, result) };
}

function pairingParticipants(ids) {
  return ids.map((id, index) => ({
    id,
    status: "checked_in",
    draw_number: index + 1,
  }));
}

function standingRows(ids, wins = {}) {
  return ids.map((id, index) => ({
    participant_id: id,
    position: index + 1,
    wins: wins[id] || 0,
    solkoff1: 0,
    solkoff2: 0,
    vp_difference: 0,
    sonneborn_berger: 0,
    bye_count: 0,
  }));
}

function pairKeys(result) {
  return result.matches
    .filter((match) => !match.is_bye)
    .map((match) => [match.participant_a_id, match.participant_b_id].sort().join("-"))
    .sort();
}

test("validates every MVP result mode and derives winner and loser", () => {
  const cases = [
    {
      name: "Carcassonne points",
      input: { result_type: "points", points_a: 89, points_b: 75 },
      expected: { winner: "p1", loser: "p2", reason: null, points: [89, 75] },
    },
    {
      name: "simple 1-0",
      input: { result_type: "simple", winner_participant_id: "p2" },
      expected: { winner: "p2", loser: "p1", reason: null, points: [null, null] },
    },
    {
      name: "time forfeit",
      input: {
        result_type: "time_forfeit",
        points_a: 64,
        points_b: 81,
        winner_participant_id: "p1",
      },
      expected: { winner: "p1", loser: "p2", reason: "time_forfeit", points: [64, 81] },
    },
    ...["withdrawal", "disqualification", "no_show", "admin_decision"].map((finishReason) => ({
      name: `technical ${finishReason}`,
      input: {
        result_type: "technical",
        winner_participant_id: "p2",
        finish_reason: finishReason,
        admin_note: "Judge decision",
      },
      expected: { winner: "p2", loser: "p1", reason: finishReason, points: [null, null] },
    })),
  ];

  cases.forEach(({ name, input, expected }) => {
    const result = validateMatchResult(normalMatch(), input);
    assert.equal(result.status, "completed", name);
    assert.equal(result.winner_participant_id, expected.winner, name);
    assert.equal(result.loser_participant_id, expected.loser, name);
    assert.equal(result.finish_reason, expected.reason, name);
    assert.deepEqual([result.points_a, result.points_b], expected.points, name);
  });
});

test("a tied points result is won by the participant who moved second", () => {
  const startedByA = validateMatchResult(normalMatch(), {
    result_type: "points",
    points_a: 77,
    points_b: 77,
  });
  assert.equal(startedByA.winner_participant_id, "p2");

  const startedByB = validateMatchResult(normalMatch({ starting_participant_id: "p2" }), {
    result_type: "points",
    points_a: 81,
    points_b: 81,
  });
  assert.equal(startedByB.winner_participant_id, "p1");
});

test("validates a system bye result", () => {
  const result = validateMatchResult({
    participant_a_id: "p3",
    participant_b_id: null,
    starting_participant_id: null,
    is_bye: true,
  }, { result_type: "bye" });
  assert.deepEqual(result, {
    status: "completed",
    is_bye: true,
    result_type: "bye",
    points_a: null,
    points_b: null,
    winner_participant_id: "p3",
    loser_participant_id: null,
    finish_reason: null,
    admin_note: null,
  });
});

test("rejects invalid and mutually exclusive result fields", () => {
  const cases = [
    {
      code: "PARTICIPANT_ID_REQUIRED",
      match: normalMatch({ starting_participant_id: null }),
      input: { result_type: "simple", winner_participant_id: "p1" },
    },
    {
      code: "INVALID_POINTS",
      match: normalMatch(),
      input: { result_type: "points", points_a: -1, points_b: 75 },
    },
    {
      code: "MUTUALLY_EXCLUSIVE_RESULT_FIELDS",
      match: normalMatch(),
      input: { result_type: "simple", winner_participant_id: "p1", points_a: 1 },
    },
    {
      code: "RESULT_WINNER_MISMATCH",
      match: normalMatch(),
      input: {
        result_type: "points",
        points_a: 70,
        points_b: 70,
        winner_participant_id: "p1",
      },
    },
    {
      code: "PARTICIPANT_ID_REQUIRED",
      match: normalMatch(),
      input: { result_type: "simple" },
    },
    {
      code: "INVALID_FINISH_REASON",
      match: normalMatch(),
      input: {
        result_type: "technical",
        winner_participant_id: "p1",
        finish_reason: "time_forfeit",
      },
    },
    {
      code: "INVALID_WINNER",
      match: normalMatch(),
      input: { result_type: "time_forfeit", winner_participant_id: "somebody-else" },
    },
    {
      code: "POINTS_REQUIRED",
      match: normalMatch(),
      input: {
        result_type: "time_forfeit",
        points_a: 70,
        winner_participant_id: "p1",
      },
    },
  ];

  cases.forEach(({ code, match, input }) => {
    assert.throws(
      () => validateMatchResult(match, input),
      (error) => error instanceof InPersonEngineError && error.code === code
    );
  });
});

test("time-forfeit points affect VP difference independently of the winner", () => {
  const match = completeMatch(normalMatch(), {
    result_type: "time_forfeit",
    points_a: 64,
    points_b: 81,
    winner_participant_id: "p1",
  });
  const standings = calculateSwissStandings({
    participants: [{ id: "p1" }, { id: "p2" }],
    rounds: [completedRound(1, [match])],
  });

  assert.equal(standings.find((row) => row.participant_id === "p1").wins, 1);
  assert.equal(standings.find((row) => row.participant_id === "p1").vp_difference, -17);
  assert.equal(standings.find((row) => row.participant_id === "p2").wins, 0);
  assert.equal(standings.find((row) => row.participant_id === "p2").vp_difference, 17);
});

test("calculates swiss_standard_v1 and ignores cancelled or incomplete rounds", () => {
  const participants = ["p1", "p2", "p3"].map((id) => ({
    id,
    status: id === "p2" ? "withdrawn" : "checked_in",
  }));
  const firstRound = completedRound(1, [
    completeMatch(normalMatch({ id: "r1-m1", table_number: 1 }), {
      result_type: "points",
      points_a: 89,
      points_b: 75,
    }),
    {
      id: "r1-bye",
      table_number: null,
      participant_a_id: "p3",
      participant_b_id: null,
      starting_participant_id: null,
      status: "completed",
      is_bye: true,
      result_type: "bye",
      winner_participant_id: "p3",
    },
  ]);
  const secondMatch = normalMatch({
    id: "r2-m1",
    table_number: 1,
    participant_b_id: "p3",
  });
  const secondRound = completedRound(2, [
    completeMatch(secondMatch, { result_type: "points", points_a: 77, points_b: 77 }),
    {
      id: "r2-bye",
      table_number: null,
      participant_a_id: "p2",
      participant_b_id: null,
      starting_participant_id: null,
      status: "completed",
      is_bye: true,
      result_type: "bye",
      winner_participant_id: "p2",
    },
  ]);
  const ignoredRound = completedRound(3, [
    completeMatch(normalMatch({ participant_b_id: "p3" }), {
      result_type: "simple",
      winner_participant_id: "p1",
    }),
  ], { status: "cancelled" });
  const draftRound = completedRound(4, [
    completeMatch(normalMatch({ participant_b_id: "p3" }), {
      result_type: "simple",
      winner_participant_id: "p1",
    }),
  ], { status: "published" });

  const standings = calculateSwissStandings({
    participants,
    rounds: [draftRound, secondRound, ignoredRound, firstRound],
  });
  assert.deepEqual(standings, [
    {
      participant_id: "p3",
      wins: 2,
      buchholz: 1,
      solkoff1: 1,
      solkoff2: 0,
      vp_difference: 0,
      sonneborn_berger: 1,
      bye_count: 1,
      start_sequence: "BS",
      position: 1,
    },
    {
      participant_id: "p1",
      wins: 1,
      buchholz: 3,
      solkoff1: 2,
      solkoff2: 0,
      vp_difference: 14,
      sonneborn_berger: 0,
      bye_count: 0,
      start_sequence: "FF",
      position: 2,
    },
    {
      participant_id: "p2",
      wins: 1,
      buchholz: 1,
      solkoff1: 1,
      solkoff2: 0,
      vp_difference: -14,
      sonneborn_berger: 0,
      bye_count: 1,
      start_sequence: "SB",
      position: 3,
    },
  ]);
});

test("the no-bye flag precedes stable participant ID only after all score tie-breaks", () => {
  const standings = calculateSwissStandings({
    participants: [{ id: "10" }, { id: "2" }, { id: "3" }],
    rounds: [completedRound(1, [
      {
        id: "bye",
        participant_a_id: "2",
        participant_b_id: null,
        starting_participant_id: null,
        status: "completed",
        is_bye: true,
        result_type: "bye",
        winner_participant_id: "2",
      },
      completeMatch(normalMatch({
        id: "technical",
        participant_a_id: "10",
        participant_b_id: "3",
        starting_participant_id: "10",
      }), {
        result_type: "technical",
        winner_participant_id: "10",
        finish_reason: "admin_decision",
      }),
    ])],
  });
  assert.deepEqual(standings.map((row) => row.participant_id), ["10", "2", "3"]);
});

test("reproduces all final CHU-2025 standings including legacy Sonneborn-Berger", async () => {
  const fixtureUrl = new URL("../../json-data/ua2025.json", import.meta.url);
  const fixture = JSON.parse(await readFile(fixtureUrl, "utf8"));
  const participants = fixture.players.map((player) => ({
    id: String(player.player_id),
    name: player.player_name,
    status: "checked_in",
    draw_number: Number(player.player_id),
  }));
  const rounds = [1, 2, 3, 4, 5].map((roundNumber) => ({
    id: `ua2025-round-${roundNumber}`,
    stage: "swiss",
    round_number: roundNumber,
    status: "completed",
    matches: fixture.swiss
      .filter((match) => Number(match.round) === roundNumber)
      .map((match) => {
        const participantAId = String(match.playerA_id || "") || null;
        const participantBId = String(match.playerB_id || "") || null;
        if (!participantBId) {
          return {
            id: `ua2025-${roundNumber}-${match.board}`,
            table_number: Number(match.board),
            participant_a_id: participantAId,
            participant_b_id: null,
            starting_participant_id: null,
            status: "completed",
            is_bye: true,
            result_type: "bye",
            winner_participant_id: participantAId,
          };
        }
        return {
          id: `ua2025-${roundNumber}-${match.board}`,
          table_number: Number(match.board),
          participant_a_id: participantAId,
          participant_b_id: participantBId,
          starting_participant_id: match.starter === "B" ? participantBId : participantAId,
          status: "completed",
          is_bye: false,
          result_type: "points",
          points_a: Number(match.pointsA),
          points_b: Number(match.pointsB),
          winner_participant_id: Number(match.scoreA) === 1 ? participantAId : participantBId,
          loser_participant_id: Number(match.scoreA) === 1 ? participantBId : participantAId,
        };
      }),
  }));

  assert.equal(participants.length, 37);
  assert.equal(fixture.swiss.length, 95);
  assert.equal(fixture.swiss.filter((match) => !match.playerB_id).length, 5);
  const generatedFirstRound = pairFirstSwissRound({ participants });
  const fixtureFirstRound = rounds[0].matches;
  assert.deepEqual(pairKeys(generatedFirstRound), pairKeys({ matches: fixtureFirstRound }));
  assert.equal(
    generatedFirstRound.bye_participant_id,
    fixtureFirstRound.find((match) => match.is_bye).participant_a_id
  );
  const actual = calculateSwissStandings({ participants, rounds });
  const expected = fixture.standings.map((row) => ({
    participant_id: String(row.player_id),
    wins: Number(row.wins),
    solkoff1: Number(row.solkoff1),
    solkoff2: Number(row.solkoff2),
    vp_difference: Number(row.vp_diff),
    sonneborn_berger: Number(row.SB),
    bye_count: Number(row.byes),
    start_sequence: row.start_seq,
    position: Number(row.rank),
  }));
  assert.deepEqual(
    actual.map(({ buchholz: _buchholz, ...row }) => row),
    expected
  );
});

test("first-round pairing compacts gaps and a missing draw number 1", () => {
  const result = pairFirstSwissRound({
    participants: [
      { id: "a", status: "checked_in", draw_number: 2 },
      { id: "b", status: "checked_in", draw_number: 3 },
      { id: "c", status: "checked_in", draw_number: 5 },
      { id: "d", status: "checked_in", draw_number: 8 },
      { id: "not-present", status: "registered", draw_number: null },
    ],
  });
  assert.deepEqual(pairKeys(result), ["a-c", "b-d"]);
  assert.equal(result.bye_participant_id, null);
  assert.deepEqual(result.matches.map((match) => match.table_number), [1, 2]);
  assert.deepEqual(
    result.matches.map((match) => match.starting_participant_id),
    ["a", "b"]
  );
});

test("the largest actually issued number receives the first-round bye", () => {
  const result = pairFirstSwissRound({
    participants: [
      { id: "a", status: "checked_in", draw_number: 2 },
      { id: "b", status: "checked_in", draw_number: 3 },
      { id: "c", status: "checked_in", draw_number: 5 },
      { id: "d", status: "checked_in", draw_number: 8 },
      { id: "e", status: "checked_in", draw_number: 21 },
    ],
  });
  assert.deepEqual(pairKeys(result), ["a-c", "b-d"]);
  assert.equal(result.bye_participant_id, "e");
  assert.equal(result.matches.at(-1).status, "completed");
});

test("first-round pairing rejects missing and duplicate issued numbers", () => {
  assert.throws(
    () => pairFirstSwissRound({
      participants: [
        { id: "a", status: "checked_in", draw_number: 2 },
        { id: "b", status: "checked_in", draw_number: null },
      ],
    }),
    (error) => error.code === "INVALID_DRAW_NUMBER"
  );
  assert.throws(
    () => pairFirstSwissRound({
      participants: [
        { id: "a", status: "checked_in", draw_number: 2 },
        { id: "b", status: "checked_in", draw_number: 2 },
      ],
    }),
    (error) => error.code === "DUPLICATE_DRAW_NUMBER"
  );
});

test("next-round pairing avoids rematches whenever a complete alternative exists", () => {
  const participants = pairingParticipants(["p1", "p2", "p3", "p4"]);
  const firstRound = completedRound(1, [
    completeMatch(normalMatch({ participant_a_id: "p1", participant_b_id: "p2" }), {
      result_type: "simple",
      winner_participant_id: "p1",
    }),
    completeMatch(normalMatch({
      participant_a_id: "p3",
      participant_b_id: "p4",
      starting_participant_id: "p3",
    }), {
      result_type: "simple",
      winner_participant_id: "p3",
    }),
  ]);
  const result = pairNextSwissRound({
    participants,
    standings: standingRows(["p1", "p3", "p2", "p4"], { p1: 1, p3: 1 }),
    rounds: [firstRound],
  });
  assert.equal(result.matches.length, 2);
  assert.equal(result.warnings.some((warning) => warning.code === "rematch"), false);
  assert.equal(pairKeys(result).includes("p1-p2"), false);
  assert.equal(pairKeys(result).includes("p3-p4"), false);
});

test("an unavoidable rematch is returned with a warning", () => {
  const history = completedRound(1, [
    completeMatch(normalMatch(), { result_type: "simple", winner_participant_id: "p1" }),
  ]);
  const result = pairNextSwissRound({
    participants: pairingParticipants(["p1", "p2"]),
    standings: standingRows(["p1", "p2"], { p1: 1 }),
    rounds: [history],
  });
  assert.deepEqual(pairKeys(result), ["p1-p2"]);
  assert.equal(result.warnings[0].code, "rematch");
});

test("starter assignment avoids a third identical role when possible", () => {
  const history = [1, 2].map((roundNumber) => completedRound(roundNumber, [
    completeMatch(normalMatch({ id: `m-${roundNumber}` }), {
      result_type: "simple",
      winner_participant_id: "p1",
    }),
  ]));
  const result = pairNextSwissRound({
    participants: pairingParticipants(["p1", "p2"]),
    standings: standingRows(["p1", "p2"], { p1: 2 }),
    rounds: history,
  });
  assert.equal(result.matches[0].starting_participant_id, "p2");
  assert.equal(
    result.matches[0].warnings.includes("starter_3_in_a_row"),
    false
  );
});

test("forced starter violations are explicit warnings", () => {
  const history = [1, 2].map((roundNumber) => completedRound(roundNumber, [
    completeMatch(normalMatch({
      id: `p1-history-${roundNumber}`,
      participant_a_id: "p1",
      participant_b_id: `x${roundNumber}`,
      starting_participant_id: "p1",
    }), { result_type: "simple", winner_participant_id: "p1" }),
    completeMatch(normalMatch({
      id: `p2-history-${roundNumber}`,
      participant_a_id: "p2",
      participant_b_id: `y${roundNumber}`,
      starting_participant_id: "p2",
    }), { result_type: "simple", winner_participant_id: "p2" }),
  ]));
  const result = pairNextSwissRound({
    participants: pairingParticipants(["p1", "p2"]),
    standings: standingRows(["p1", "p2"], { p1: 2, p2: 2 }),
    rounds: history,
  });
  assert.equal(result.matches[0].warnings.includes("starter_3_in_a_row"), true);
  assert.equal(result.matches[0].warnings.includes("starter_imbalance"), true);
});

test("next-round pairing stays inside score groups and floats only the odd remainder", () => {
  const ids = ["p1", "p2", "p3", "p4", "p5", "p6"];
  const wins = { p1: 2, p2: 2, p3: 2, p4: 1, p5: 1, p6: 1 };
  const result = pairNextSwissRound({
    participants: pairingParticipants(ids),
    standings: standingRows(ids, wins),
  });
  const crossGroupMatches = result.matches.filter((match) => (
    wins[match.participant_a_id] !== wins[match.participant_b_id]
  ));
  assert.equal(crossGroupMatches.length, 1);
});

test("withdrawn participants remain in standings but never receive a new opponent", () => {
  const ids = ["p1", "p2", "p3", "p4", "p5"];
  const participants = pairingParticipants(ids);
  participants[0].status = "withdrawn";
  const evenResult = pairNextSwissRound({
    participants,
    standings: standingRows(ids),
  });
  assert.equal(evenResult.bye_participant_id, null);
  assert.equal(evenResult.matches.length, 2);
  assert.equal(pairKeys(evenResult).some((key) => key.includes("p1")), false);

  const oddParticipants = pairingParticipants([...ids, "p6"]);
  oddParticipants[0].status = "disqualified";
  const oddResult = pairNextSwissRound({
    participants: oddParticipants,
    standings: standingRows([...ids, "p6"]),
  });
  assert.equal(oddResult.bye_participant_id, "p6");
  assert.equal(pairKeys(oddResult).some((key) => key.includes("p1")), false);
});

test("next-round bye goes to the lowest-ranked active participant without a previous bye", () => {
  const ids = ["p1", "p2", "p3", "p4", "p5"];
  const standings = standingRows(ids);
  standings[4].bye_count = 1;
  const result = pairNextSwissRound({
    participants: pairingParticipants(ids),
    standings,
  });
  assert.equal(result.bye_participant_id, "p4");
});

test("pairing stops when all odd-pool candidates already had a bye", () => {
  const ids = ["p1", "p2", "p3"];
  const standings = standingRows(ids).map((row) => ({ ...row, bye_count: 1 }));
  assert.throws(
    () => pairNextSwissRound({ participants: pairingParticipants(ids), standings }),
    (error) => error.code === "NO_BYE_ELIGIBLE_PARTICIPANT"
  );
});

test("next-round pairing is deterministic and ignores draw-number gaps", () => {
  const ids = ["p1", "p2", "p3", "p4", "p5", "p6"];
  const participants = pairingParticipants(ids);
  participants.forEach((participant, index) => {
    participant.draw_number = [2, 3, 5, 8, 20, 31][index];
  });
  const input = { participants, standings: standingRows(ids) };
  const first = pairNextSwissRound(input);
  participants.reverse().forEach((participant, index) => {
    participant.draw_number = 100 + index;
  });
  const second = pairNextSwissRound({ ...input, participants });
  assert.deepEqual(second, first);
});

test("pairs 256 participants within a practical pure-engine budget", () => {
  const ids = Array.from({ length: 256 }, (_, index) => `p-${String(index + 1).padStart(3, "0")}`);
  const startedAt = performance.now();
  const result = pairNextSwissRound({
    participants: pairingParticipants(ids),
    standings: standingRows(ids),
  });
  const duration = performance.now() - startedAt;
  assert.equal(result.matches.length, 128);
  assert.ok(duration < 2000, `pairing took ${duration.toFixed(1)}ms`);
});
