import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const html = readFileSync(new URL("../../gg-html/player-hub/tournaments.html", import.meta.url), "utf8");
function functionSource(name) {
  const start = html.indexOf(`  function ${name}(`);
  assert.ok(start >= 0);
  return html.slice(start, html.indexOf("\n  function ", start + 1));
}
function closureSource(name) {
  const start = html.indexOf(`    const ${name} = `);
  assert.ok(start >= 0);
  return html.slice(start, html.indexOf("\n    const ", start + 1));
}
const helpers = ["parseNullableInt", "getDuelWinsLimit", "getEditableDuelCompletion"].map(functionSource).join("\n");

class Element {
  constructor(tag) {
    this.tag = tag;
    this.children = [];
    this.attributes = {};
    this.listeners = {};
    this.style = {};
    this.classList = { add() {}, remove() {}, toggle() {} };
  }
  append(...children) { this.children.push(...children); }
  appendChild(child) { this.append(child); return child; }
  setAttribute(key, value) { this.attributes[key] = value; }
  addEventListener(event, callback) { this.listeners[event] = callback; }
  fire(event) { return this.listeners[event]?.({ target: this }); }
  all(tag) { return this.children.flatMap((child) => child instanceof Element ? [...(child.tag === tag ? [child] : []), ...child.all(tag)] : []); }
}

function context(fetchResult = async () => ({ ok: true, game: {
  bga_table_id: "1234567890", player_1_score: 100, player_2_score: 90,
  player_1_rank: 1, player_2_rank: 0, player_1_clock: 0, player_2_clock: 0,
} })) {
  const entry = { id: "duel", duelFormat: "Bo3" };
  const state = { games: [], saving: false };
  const dialogs = [];
  const calls = [];
  const document = { createElement: (tag) => new Element(tag) };
  const dependencies = {
    document, HTMLInputElement: Element,
    getActiveDuelEditorState: () => state,
    isResultsEditModeEnabled: true, canEditResults: true,
    isFixableEmptyErrorDuel: () => false,
    appendDuelEditorStatuses() {}, renderLineupsRows() {},
    normalizeEditableGamesOrder: (games) => games.forEach((game, index) => { game.game_number = index + 1; }),
    showDuelGameInfoDialog: (...args) => dialogs.push(args),
    DUEL_GAME_BGA_RESULT_URL: (id) => `/duels/${id}/games/bga-result`,
    fetch: async (url, options) => { calls.push({ url, body: JSON.parse(options.body) }); return { ok: true, json: fetchResult }; },
    hasValue: (value) => value !== null && value !== undefined,
    canEditGameScore: () => true,
    isReadonlyNoShowExistingGame: (game) => !game.isNewLocal,
    createDropdown: () => ({ root: new Element("select"), setValue() {}, setDisabled() {}, setOnChange() {} }),
  };
  const functions = new Function(...Object.keys(dependencies), `${helpers}
    ${closureSource("addEditableDuelGame")}
    ${closureSource("createPendingBgaGameEditor")}
    ${closureSource("renderDuelEditorPanel")}
    return { addEditableDuelGame, createPendingBgaGameEditor, renderDuelEditorPanel, getEditableDuelCompletion };`)(...Object.values(dependencies));
  return { ...functions, state, entry, dialogs, calls };
}

test("Add Game blocks deciding Bo3/Bo5 results and permits an unfinished duel", () => {
  const ctx = context();
  const win1 = { player_1_rank: 1, player_2_rank: 0 };
  const win2 = { player_1_rank: 0, player_2_rank: 1 };
  for (const [format, games] of [["Bo3", [win1, win1]], ["Bo3", [win1, win2, win1]],
    ["Bo3", [win2, win2]], ["Bo5", [win1, win2, win1, win2, win1]]]) {
    ctx.state.games = [...games];
    ctx.entry.duelFormat = format;
    ctx.addEditableDuelGame(ctx.entry, ctx.state);
    assert.equal(ctx.state.games.length, games.length);
    assert.equal(ctx.dialogs.at(-1)[0], "Duel already complete");
  }
  ctx.entry.duelFormat = "Bo3";
  ctx.state.games = [win1, win2];
  ctx.addEditableDuelGame(ctx.entry, ctx.state);
  assert.equal(ctx.state.games.length, 3);
  assert.equal(ctx.state.games.at(-1).bgaResultLoaded, false);
  ctx.addEditableDuelGame(ctx.entry, ctx.state);
  assert.equal(ctx.state.games.length, 3);
  assert.equal(ctx.dialogs.at(-1)[0], "Finish adding the current game");
});

test("new game shows only Table ID and Get BGA result until a valid result is loaded", async () => {
  const ctx = context();
  ctx.addEditableDuelGame(ctx.entry, ctx.state);
  let panel = ctx.renderDuelEditorPanel(ctx.entry);
  assert.deepEqual(panel.all("label").map((label) => label.textContent), ["Table ID"]);
  const input = panel.all("input")[0];
  const button = panel.all("button").find((item) => item.textContent === "Get BGA result");
  assert.equal(button.disabled, true);
  for (const [value, valid] of [["12345678", false], ["123456789", true], ["12345678901", false], ["123456789x", false], ["1234567890", true]]) {
    input.value = value;
    input.fire("input");
    assert.equal(button.disabled, !valid);
    assert.equal(input.attributes["aria-invalid"], String(!valid));
  }
  await button.fire("click");
  assert.deepEqual(ctx.calls, [{ url: "/duels/duel/games/bga-result", body: { bga_table_id: "1234567890" } }]);
  assert.equal(ctx.state.games[0].bgaResultLoaded, true);
  assert.equal(ctx.state.games[0].player_1_score, 100);
  panel = ctx.renderDuelEditorPanel(ctx.entry);
  const labels = panel.all("label").map((label) => label.textContent);
  for (const label of ["Score", "Won", "Time loss", "No show"]) assert.equal(labels.filter((item) => item === label).length, 2);
  assert.equal(panel.all("input")[0].readOnly, true);
});

test("missing BGA game keeps result fields hidden, displays a warning and allows retry", async () => {
  const ctx = context(async () => ({ ok: false, message: "No completed game was found for both duel players." }));
  ctx.addEditableDuelGame(ctx.entry, ctx.state);
  let panel = ctx.renderDuelEditorPanel(ctx.entry);
  const input = panel.all("input")[0];
  input.value = "123456789";
  input.fire("input");
  await panel.all("button").find((item) => item.textContent === "Get BGA result").fire("click");
  assert.equal(ctx.state.games[0].bgaResultLoaded, false);
  panel = ctx.renderDuelEditorPanel(ctx.entry);
  assert.deepEqual(panel.all("label").map((label) => label.textContent), ["Table ID"]);
  assert.ok(panel.all("div").some((item) => item.textContent?.includes("both duel players")));
  assert.equal(panel.all("button").find((item) => item.textContent === "Get BGA result").disabled, false);
});

test("importing disables Save and an already included Table ID never contacts BGA", async () => {
  const ctx = context();
  ctx.addEditableDuelGame(ctx.entry, ctx.state);
  ctx.state.games[0].fetchingBgaResult = true;
  let panel = ctx.renderDuelEditorPanel(ctx.entry);
  assert.equal(panel.all("button").find((item) => item.textContent === "Save").disabled, true);
  ctx.state.games[0].fetchingBgaResult = false;
  ctx.state.games.push({ id: "existing", bga_table_id: "123456789", isNewLocal: false });
  panel = ctx.renderDuelEditorPanel(ctx.entry);
  const input = panel.all("input")[0];
  input.value = "123456789";
  input.fire("input");
  await panel.all("button").find((item) => item.textContent === "Get BGA result").fire("click");
  assert.equal(ctx.calls.length, 0);
  assert.match(ctx.state.games[0].bgaResultError, /already included/);
});
