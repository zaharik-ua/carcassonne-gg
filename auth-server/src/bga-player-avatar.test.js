import assert from "node:assert/strict";
import test from "node:test";
import { registerBgaPlayerAvatarRoutes } from "./bga-player-avatar.js";

function context(fetch = async (player) => ({ ok: true, player_id: player, avatar: "hash" })) {
  let handler;
  let clock = 1000;
  const calls = [];
  registerBgaPlayerAvatarRoutes({ get(path, callback) {
    assert.deepEqual(path, ["/profiles/bga-player-avatar", "/bga/player-avatar"]);
    handler = callback;
  } }, {
    now: () => clock,
    fetchBgaPlayerAvatar: (player) => { calls.push(player); return fetch(player); },
  });
  return {
    calls,
    advance(ms) { clock += ms; },
    async request(player) {
      let status = 200;
      const headers = {};
      let response;
      await handler({ query: { player } }, {
        status(code) { status = code; return this; },
        set(name, value) { headers[name] = value; return this; },
        json(data) { response = { status, headers, data }; return response; },
      });
      return response;
    },
  };
}

test("ID lookups do not require GG profiles and successful results expire after one hour", async () => {
  const ctx = context();
  assert.equal((await ctx.request(" 95236669 ")).data.player_id, "95236669");
  assert.equal((await ctx.request("95236669")).status, 200);
  assert.equal(ctx.calls.length, 1);
  ctx.advance(3600001);
  await ctx.request("95236669");
  assert.equal(ctx.calls.length, 2);
});

test("invalid inputs never call BGA", async () => {
  const ctx = context();
  for (const player of [undefined, [], {}, "", "a".repeat(101), "bad\nname"]) {
    assert.equal((await ctx.request(player)).status, 400);
  }
  assert.equal(ctx.calls.length, 0);
});

test("concurrent requests for the same player share work; other cold lookups are rate limited", async () => {
  let complete;
  const ctx = context(() => new Promise(resolve => { complete = resolve; }));
  const first = ctx.request("123");
  const duplicate = ctx.request("123");
  await Promise.resolve();
  const busy = await ctx.request("456");
  assert.equal(busy.status, 429);
  assert.equal(busy.headers["Retry-After"], "2");
  complete({ ok: true, player_id: "123", avatar: null });
  assert.equal((await first).status, 200);
  assert.equal((await duplicate).status, 200);
  assert.equal(ctx.calls.length, 1);
  assert.equal((await ctx.request("456")).status, 429);
});

test("failures and malformed upstream data are not cached", async () => {
  for (const [result, status] of [
    [{ ok: false, not_found: true, message: "No player" }, 404],
    [{ ok: false }, 502],
    [{ ok: true, avatar: "hash" }, 502],
    [{ ok: true, player_id: "123" }, 502],
  ]) {
    const ctx = context(async () => result);
    assert.equal((await ctx.request("123")).status, status);
    ctx.advance(2000);
    assert.equal((await ctx.request("123")).status, status);
    assert.equal(ctx.calls.length, 2);
  }
  const ctx = context(async () => { throw new Error("BGA timeout"); });
  assert.equal((await ctx.request("123")).status, 502);
});
