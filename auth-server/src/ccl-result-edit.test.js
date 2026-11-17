import assert from "node:assert/strict";
import test from "node:test";

import { getCclResultEditAccess, getCclResultEditUntil } from "./ccl-result-edit.js";

const DUEL = { status: "Done", time_utc: "2026-11-20T18:00:00.000Z", player_1_id: "olena", player_2_id: "juan" };
const at = (iso) => new Date(iso);

test("the players' editing window ends 24 hours after the match start", () => {
  assert.equal(getCclResultEditUntil(DUEL.time_utc), "2026-11-21T18:00:00.000Z");
  assert.equal(getCclResultEditUntil(null), null);
});

test("players edit their own completed match within the window", () => {
  const inside = getCclResultEditAccess({ duel: DUEL, playerId: "olena", now: at("2026-11-21T17:59:00Z") });
  assert.equal(inside.allowed, true);
  assert.equal(inside.until, "2026-11-21T18:00:00.000Z");

  const after = getCclResultEditAccess({ duel: DUEL, playerId: "juan", now: at("2026-11-21T18:00:00Z") });
  assert.deepEqual([after.allowed, after.expired], [false, true]);

  const stranger = getCclResultEditAccess({ duel: DUEL, playerId: "max", now: at("2026-11-21T10:00:00Z") });
  assert.deepEqual([stranger.allowed, stranger.message], [false, "Forbidden"]);
});

test("admins edit any time; planned matches have no results to edit", () => {
  assert.equal(getCclResultEditAccess({ duel: DUEL, isAdmin: true, now: at("2027-01-01T00:00:00Z") }).allowed, true);
  assert.equal(getCclResultEditAccess({ duel: { ...DUEL, status: "Error" }, playerId: "olena", now: at("2026-11-21T00:00:00Z") }).allowed, true);
  assert.equal(getCclResultEditAccess({ duel: { ...DUEL, status: "Planned" }, isAdmin: true }).allowed, false);
});
