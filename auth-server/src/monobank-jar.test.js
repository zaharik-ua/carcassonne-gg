import assert from "node:assert/strict";
import test from "node:test";
import { pickJar, registerMonobankJarRoutes } from "./monobank-jar.js";

const clientInfo = {
  accounts: [{ id: "secret", balance: 999999 }],
  jars: [
    { id: "a", sendId: "jar/other", balance: 100, goal: 200, currencyCode: 980 },
    { id: "b", sendId: "jar/55tWhDvC2j", balance: 125050, goal: 345000, currencyCode: 980 },
  ],
};

function context({ fetchResult = () => ({ ok: true, json: async () => clientInfo }), token = "t" } = {}) {
  let handler;
  let clock = 100000;
  const calls = [];
  registerMonobankJarRoutes({ get(path, callback) {
    assert.equal(path, "/public/donation-jar");
    handler = callback;
  } }, {
    token,
    sendId: "https://send.monobank.ua/jar/55tWhDvC2j",
    now: () => clock,
    fetchImpl: async (url, options) => { calls.push({ url, options }); return fetchResult(); },
  });
  return {
    calls,
    advance(ms) { clock += ms; },
    async request() {
      let status = 200;
      let data;
      await handler({}, {
        set() { return this; },
        status(code) { status = code; return this; },
        json(body) { data = body; return this; },
      });
      return { status, data };
    },
  };
}

test("pickJar returns only the requested jar in major currency units", () => {
  assert.deepEqual(pickJar(clientInfo, "55tWhDvC2j"), {
    ok: true, balance: 1250.5, goal: 3450, currency: "UAH",
    url: "https://send.monobank.ua/jar/55tWhDvC2j",
  });
  assert.equal(pickJar(clientInfo, "missing"), null);
});

test("route sends the token, hides account data and caches results", async () => {
  const ctx = context();
  const first = await ctx.request();
  assert.equal(first.status, 200);
  assert.equal(first.data.balance, 1250.5);
  assert.equal(first.data.accounts, undefined);
  assert.equal(ctx.calls[0].options.headers["X-Token"], "t");
  await ctx.request();
  assert.equal(ctx.calls.length, 1);
  ctx.advance(5 * 60 * 1000 + 1);
  await ctx.request();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(ctx.calls.length, 2);
});

test("route keeps the last good value when monobank fails", async () => {
  let fail = false;
  const ctx = context({ fetchResult: () => (fail ? { ok: false, status: 429 } : { ok: true, json: async () => clientInfo }) });
  await ctx.request();
  fail = true;
  ctx.advance(10 * 60 * 1000);
  const stale = await ctx.request();
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(stale.status, 200);
  assert.equal(stale.data.balance, 1250.5);
});

test("route reports missing configuration", async () => {
  const ctx = context({ token: "" });
  assert.equal((await ctx.request()).status, 503);
  assert.equal(ctx.calls.length, 0);
});
