import assert from "node:assert/strict";
import test from "node:test";
import { registerDuelGameBgaResultRoutes } from "./duel-game-bga-result.js";

function context({ allowed = true, expired = false, matchStatus = "Done", result = { ok: true, game: { bga_table_id: "1234567890" } } } = {}) {
  let handler;
  const calls = [];
  registerDuelGameBgaResultRoutes({ post(path, callback) { assert.equal(path, "/duels/:id/games/bga-result"); handler = callback; } }, {
    dbGetAsync: async () => ({ id: "duel", player_1_id: "11", player_2_id: "22", tournament_id: "T", match_status: matchStatus }),
    loadTournamentAccessForUser: (id, user, done) => done(null, { id }),
    canUserEditMatchResults: () => ({ allowed, expired }),
    isCompletedMatchStatus: (status) => ["Done", "Error"].includes(status),
    fetchBgaGameResult: async (...args) => { calls.push(args); return result; },
  });
  return { calls, request: async (tableId = "1234567890", user = {}) => {
    let status = 200;
    let response;
    await handler({ params: { id: "duel" }, body: { bga_table_id: tableId }, user }, {
      status(value) { status = value; return this; }, json(value) { response = { status, ...value }; return response; },
    });
    return response;
  } };
}

test("BGA lookup uses stored duel participants and accepts 9- or 10-digit tables", async () => {
  const ctx = context();
  assert.equal((await ctx.request()).ok, true);
  assert.equal((await ctx.request("123456789")).ok, true);
  assert.deepEqual(ctx.calls, [["1234567890", "11", "22"], ["123456789", "11", "22"]]);
});

test("invalid table IDs and unauthorized or expired access never contact BGA", async () => {
  const ctx = context();
  for (const table of ["12345678", "12345678901", "12345678x"]) assert.equal((await ctx.request(table)).status, 400);
  assert.equal((await ctx.request(undefined, null)).status, 401);
  assert.equal(ctx.calls.length, 0);
  for (const options of [{ allowed: false }, { allowed: false, expired: true }, { matchStatus: "Planned" }]) {
    const restricted = context(options);
    assert.equal((await restricted.request()).status, 403);
    assert.equal(restricted.calls.length, 0);
  }
});

test("missing games and BGA failures return actionable warnings", async () => {
  for (const [notFound, expectedStatus] of [[true, 404], [false, 502]]) {
    const ctx = context({ result: { ok: false, not_found: notFound, message: "Check the Table ID." } });
    const response = await ctx.request();
    assert.equal(response.status, expectedStatus);
    assert.equal(response.message, "Check the Table ID.");
  }
});
