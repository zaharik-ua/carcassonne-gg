import assert from "node:assert/strict";
import test from "node:test";
import { getTournamentRosterUpdateError } from "./tournament-roster.js";

const now = Date.parse("2026-10-01T18:30:00Z");
const roster = {
  registrationEndsAt: "2026-10-01T20:30:00+02:00",
  isAdmin: false,
  existingPlayerIds: ["captain", "player-1"],
  playerIds: ["captain", "player-2"],
  now,
};

test("captains cannot replace or remove registered players at or after the deadline", () => {
  for (const time of [now, now + 1]) {
    for (const playerIds of [["captain", "player-2"], ["captain"], []]) {
      assert.equal(
        getTournamentRosterUpdateError({ ...roster, now: time, playerIds }),
        "Player replacements are closed after registration ends."
      );
    }
  }
});

test("captains can keep the roster, reorder it, and fill empty places after registration", () => {
  for (const playerIds of [
    ["captain", "player-1"],
    ["player-1", "captain"],
    ["captain", "player-1", "player-2"],
  ]) {
    assert.equal(getTournamentRosterUpdateError({ ...roster, playerIds }), null);
  }
});

test("newly saved players are protected on subsequent updates", () => {
  assert.ok(getTournamentRosterUpdateError({
    ...roster,
    existingPlayerIds: ["captain", "player-1", "player-2"],
    playerIds: ["captain", "player-1", "player-3"],
  }));
});

test("admins may replace registered players after the deadline", () => {
  assert.equal(getTournamentRosterUpdateError({ ...roster, isAdmin: true }), null);
});

test("captains may replace players before the deadline or without a valid deadline", () => {
  assert.equal(getTournamentRosterUpdateError({ ...roster, now: now - 1 }), null);
  for (const registrationEndsAt of [undefined, null, "", "invalid"]) {
    assert.equal(getTournamentRosterUpdateError({ ...roster, registrationEndsAt }), null);
  }
});
