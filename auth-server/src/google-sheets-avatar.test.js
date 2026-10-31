import assert from "node:assert/strict";
import fs from "node:fs";
import vm from "node:vm";
import test from "node:test";

const source = fs.readFileSync(new URL("../../google-sheets-scritps/getUserAvatar.gs", import.meta.url), "utf8");

function context({ profiles = [], ggCode = 200, fallback } = {}) {
  const calls = [];
  const sleeps = [];
  const cache = new Map();
  const ctx = vm.createContext({
    UrlFetchApp: { fetch(url) {
      calls.push(url);
      const isGg = url.endsWith("/profiles/public");
      const result = isGg ? { code: ggCode, data: { ok: ggCode === 200, profiles } }
        : fallback(new URL(url).searchParams.get("player"));
      return { getResponseCode: () => result.code ?? 200, getContentText: () => JSON.stringify(result.data) };
    } },
    CacheService: { getScriptCache: () => ({
      get: key => cache.get(key) ?? null,
      put: (key, value) => cache.set(key, value),
    }) },
    Utilities: { sleep: ms => sleeps.push(ms) },
  });
  vm.runInContext(source, ctx);
  return { get: ctx.getUserAvatar, build: ctx.buildBgaAvatarUrl_, calls, sleeps };
}

test("a saved GG avatar takes priority without contacting BGA", () => {
  const ctx = context({ profiles: [{ id: "123", bga_nickname: "Player", avatar: "_def_2276" }] });
  assert.equal(ctx.get(" PLAYER "), "https://x.boardgamearena.net/data/avatar/defaults/default-2276.jpg");
  assert.equal(ctx.calls.length, 1);
});

test("the missing ID from the reported Sheets error falls back to BGA and is cached", () => {
  const ctx = context({ fallback: player => {
    assert.equal(player, "95236669");
    return { data: { ok: true, player_id: player, avatar: "photo-hash" } };
  } });
  const expected = "https://x.boardgamearena.net/data/avatar/0/95/95236/95236669_184.jpg?h=photo-hash";
  assert.equal(ctx.get(95236669), expected);
  assert.equal(ctx.get("95236669"), expected);
  assert.equal(ctx.calls.filter(url => url.includes("/bga/player-avatar?")).length, 1);
});

test("empty saved avatars use the profile ID and duplicate range entries share one lookup", () => {
  const ctx = context({ profiles: [{ id: "123", bga_nickname: "Old Name", avatar: null }],
    fallback: player => {
      assert.equal(player, "123");
      return { data: { ok: true, player_id: "123", avatar: "x00000" } };
    },
  });
  const result = ctx.get([["Old Name", "123"], ["", null]]);
  const url = "https://x.boardgamearena.net/data/avatar/default_184.jpg";
  assert.deepEqual(JSON.parse(JSON.stringify(result)), [[url, url], ["", ""]]);
  assert.equal(ctx.calls.length, 2);
});

test("GG outages still allow BGA lookup and nicknames are URL encoded", () => {
  const ctx = context({ ggCode: 503, fallback: player => {
    assert.equal(player, "A&B name");
    return { data: { ok: true, player_id: "123", avatar: null } };
  } });
  assert.equal(ctx.get("A&B name"), "");
  assert.ok(ctx.calls[1].endsWith("player=A%26B%20name"));
});

test("one busy BGA request is retried; failures and invalid payloads remain visible", () => {
  let attempts = 0;
  const ctx = context({ fallback: () => ++attempts === 1
    ? { code: 429, data: { ok: false, message: "busy" } }
    : { data: { ok: true, player_id: "123", avatar: "_def_1" } } });
  assert.equal(ctx.get("123"), "https://x.boardgamearena.net/data/avatar/defaults/default-0001.jpg");
  assert.deepEqual(ctx.sleeps, [2000]);
  for (const data of [{ ok: true }, { ok: true, player_id: "123" }, { ok: false, message: "BGA failed" }]) {
    const failing = context({ fallback: () => ({ data }) });
    assert.throws(() => failing.get("123"), /Аватар гравця/);
  }
});

test("blank cells make no requests and avatar URLs match the site's existing builder", () => {
  const ctx = context();
  assert.equal(ctx.get(""), "");
  assert.equal(ctx.calls.length, 0);
  const cases = [
    ["86345496", "ueoP1vYdJf"], ["1", "_def_2276"], ["1", "_def_1"], ["1", "x00000"],
    ["1", "https://example.com/photo_32.jpg?h=x&y=z"], ["1", "http://example.com/avatar.jpg"],
    ["123456789", "a+b/&=?"], ["", "hash"], ["invalid", "hash"], ["1", null],
  ];
  const root = new URL("../../gg-html/", import.meta.url);
  let checked = 0;
  for (const file of fs.readdirSync(root, { recursive: true }).filter(file => file.endsWith(".html"))) {
    const html = fs.readFileSync(new URL(file, root), "utf8");
    const builder = html.match(/^([ \t]*)function buildBgaAvatarUrl\([^]*?^\1}/m);
    if (!builder) continue;
    const site = vm.createContext({});
    vm.runInContext(builder[0], site);
    for (const [id, avatar] of cases) assert.equal(ctx.build(id, avatar), site.buildBgaAvatarUrl(id, avatar), file);
    assert.equal(site.buildBgaAvatarUrl("100272877", "n61jaaNUiQ"),
      "https://x.boardgamearena.net/data/avatar/0/100/100272/100272877_184.jpg?h=n61jaaNUiQ", file);
    checked++;
  }
  assert.equal(checked, 13);
});

test("the first avatar folder changes at one billion, not one hundred million", () => {
  const ctx = context();
  for (const [id, path] of [
    ["99999999", "0/99/99999"],
    ["100000000", "0/100/100000"],
    ["100272877", "0/100/100272"],
    ["999999999", "0/999/999999"],
    ["1000000000", "1/1000/1000000"],
  ]) {
    assert.equal(ctx.build(id, "n61jaaNUiQ"),
      `https://x.boardgamearena.net/data/avatar/${path}/${id}_184.jpg?h=n61jaaNUiQ`);
  }
});

test("manual batches write each neighboring cell before looking up the next row and continue after errors", () => {
  const events = [];
  const values = [["First"], ["Broken"], [""], ["Last"]];
  const sheet = { getRange(...args) {
    if (typeof args[0] === "string") {
      assert.equal(args[0], "B70:B80");
      return { getNumColumns: () => 1, getValues: () => values, getRow: () => 70, getColumn: () => 2 };
    }
    return { setValue: value => events.push(["write", ...args, value]) };
  } };
  const ctx = vm.createContext({
    SpreadsheetApp: {
      getActiveSpreadsheet: () => ({ getSheetByName(name) { assert.equal(name, "test"); return sheet; } }),
      flush: () => events.push(["flush"]),
    },
    Utilities: { sleep: ms => events.push(["sleep", ms]) },
    Logger: { log() {} },
  });
  vm.runInContext(source, ctx);
  ctx.getUserAvatar = player => {
    events.push(["lookup", player]);
    if (player === "Broken") throw new Error("BGA failed");
    return "https://example.com/" + player + ".jpg";
  };
  assert.deepEqual(JSON.parse(JSON.stringify(ctx.fillUserAvatars())), { succeeded: 2, blank: 1, failed: 1 });
  assert.deepEqual(events, [
    ["lookup", "First"], ["write", 70, 3, "https://example.com/First.jpg"], ["flush"], ["sleep", 1000],
    ["lookup", "Broken"], ["write", 71, 3, "ERROR: BGA failed"], ["flush"], ["sleep", 1000],
    ["write", 72, 3, ""], ["flush"],
    ["lookup", "Last"], ["write", 73, 3, "https://example.com/Last.jpg"], ["flush"],
  ]);
});

test("manual batches reject missing sheets, multi-column input and offsets that would overwrite input", () => {
  let sheet;
  const ctx = vm.createContext({
    SpreadsheetApp: { getActiveSpreadsheet: () => ({ getSheetByName: () => sheet }) },
  });
  vm.runInContext(source, ctx);
  assert.throws(() => ctx.fillUserAvatars(), /Вкладку "test" не знайдено/);
  for (const offset of [0, -1, 0.5]) assert.throws(() => ctx.fillUserAvatars("test", "B70:B80", offset), /додатним цілим/);
  sheet = { getRange: () => ({ getNumColumns: () => 2 }) };
  assert.throws(() => ctx.fillUserAvatars("test", "B70:C80"), /в одній колонці/);
});


test("all active pages and Sheets repair saved BGA avatar paths and preserve image URLs", () => {
  const files = [
    "matches.html", "players/player-card.html", "player-hub/challenges.html", "GG-Rivals-S1.html",
    "tournaments.html",
    "admin.html",
    "ETCOC-2026.html",
    "player-hub/tournaments.html",
    "player-hub/streams.html",
    "player-hub/players.html",
    "challenges/challenges-matches.html",
    "official-matches/team-official-matches.html",
  ];
  const expected = "https://x.boardgamearena.net/data/avatar/0/100/100188/100188806_184.jpg?h=03oxUAUcAS";
  const wrong = expected.replace("/avatar/0/", "/avatar/1/");
  const builders = [["Google Sheets", context().build]];
  for (const file of files) {
    const html = fs.readFileSync(new URL(`../../gg-html/${file}`, import.meta.url), "utf8");
    const builder = html.match(/^([ \t]*)function buildBgaAvatarUrl\([^]*?^\1}/m);
    assert.ok(builder, file);
    const ctx = vm.createContext({});
    vm.runInContext(builder[0], ctx);
    builders.push([file, ctx.buildBgaAvatarUrl]);
  }
  for (const [file, build] of builders) {
    assert.equal(build("100188806", "03oxUAUcAS"), expected, file);
    assert.equal(build("100188806", wrong), expected, file);
    assert.equal(build("100188806", expected), expected, file);
    assert.equal(build("", wrong), expected, file);
    assert.equal(build("100188806", wrong.replace("_184.jpg", "_32.png") + "&v=1#photo"),
      expected.replace("_184.jpg", "_32.png") + "&v=1#photo", file);
    const billion = "https://x.boardgamearena.net/data/avatar/1/1000/1000000/1000000000_184.jpg?h=hash";
    assert.equal(build("1000000000", "hash"), billion, file);
    assert.equal(build("1000000000", billion), billion, file);
    for (const untouched of [
      "https://x.boardgamearena.net/data/avatar/default_184.jpg",
      "https://x.boardgamearena.net/data/avatar/defaults/default-2276.jpg",
      "https://example.com/data/avatar/1/100/100188/100188806_184.jpg?h=hash",
    ]) assert.equal(build("100188806", untouched), untouched, file);
  }
});
