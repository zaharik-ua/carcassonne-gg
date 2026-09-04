import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const menuHtml = readFileSync(
  new URL("../../gg-html/player-hub/player-hub-menu.html", import.meta.url),
  "utf8"
);
const hubHtml = readFileSync(
  new URL("../../gg-html/player-hub/player-hub.html", import.meta.url),
  "utf8"
);
const inPersonHtml = readFileSync(
  new URL("../../gg-html/player-hub/in-person.html", import.meta.url),
  "utf8"
);

function assertEmbeddedScriptsParse(html, label) {
  const scripts = [...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/gi)];
  assert.ok(scripts.length > 0, `${label} must contain a script`);
  scripts.forEach((match) => assert.doesNotThrow(() => new Function(match[1])));
}

test("Player Hub exposes In-Person only to global admins or assigned organizers", () => {
  [menuHtml, hubHtml].forEach((html) => {
    assert.match(html, /label: "In-Person"/);
    assert.match(html, /\/in-person-tournaments\/accessible/);
    assert.match(html, /item\.view === "in-person".*isAdminUser \|\| inPersonTournaments\.length > 0/s);
    assert.match(html, /item\.view === "my-tournaments" \|\| item\.view === "nationals"/);
    assert.match(html, /return isAdminUser/);
  });
});

test("In-Person page contains participant registration and check-in flows", () => {
  [
    "In-Person Tournaments",
    "Add player",
    "+ Add new city",
    "City name (English) *",
    "City name (local)",
    "City icon URL",
    "Country / association",
    "Check-in and draw numbers",
    "Gaps and a missing #1 are allowed.",
    "Possible duplicate:",
    "Ready to form the first Swiss round.",
  ].forEach((text) => assert.ok(inPersonHtml.includes(text), `missing In-Person UI text: ${text}`));

  assert.match(inPersonHtml, /\/participants/);
  assert.match(inPersonHtml, /\/participant-cities/);
  assert.match(inPersonHtml, /method: "POST"/);
  assert.match(inPersonHtml, /function createCityPickerField/);
  assert.match(inPersonHtml, /country\.input\.readOnly = true/);
  assert.match(inPersonHtml, /\/start-check-in/);
  assert.match(inPersonHtml, /\/check-in/);
  assert.match(inPersonHtml, /confirm_duplicate: confirmDuplicate/);
  assert.match(inPersonHtml, /@media \(max-width: 720px\)/);
  assert.match(inPersonHtml, /min-height: 44px/);
});

test("In-Person page contains the complete Swiss organizer workflow", () => {
  [
    "Swiss rounds",
    "Preview first round",
    "Confirm and publish",
    "Publish round",
    "Save result",
    "Complete round",
    "Undo complete round",
    "Swiss standings",
    "Solkoff1",
    "Solkoff2",
    "VP difference",
  ].forEach((text) => assert.ok(inPersonHtml.includes(text), `missing Swiss UI text: ${text}`));

  [
    /\/swiss"/,
    /\/swiss\/rounds\/preview/,
    /\/swiss\/rounds\/confirm/,
    /\/publish`/,
    /\/result`/,
    /\/complete`/,
    /\/reopen`/,
  ].forEach((pattern) => assert.match(inPersonHtml, pattern));
  assert.match(inPersonHtml, /data-ip-tab="swiss"/);
  assert.match(inPersonHtml, /data-ip-tab="standings"/);
  assert.match(inPersonHtml, /round\.progress\.completed === round\.progress\.total/);
  assert.match(inPersonHtml, /round_number: preview\.round_number, publish: true/);
});

test("participant row actions are moved into the Edit form", () => {
  const listRenderer = inPersonHtml.slice(
    inPersonHtml.indexOf("function renderPlayers()"),
    inPersonHtml.indexOf("function openPlayerForm")
  );
  const editForm = inPersonHtml.slice(
    inPersonHtml.indexOf("function openPlayerForm"),
    inPersonHtml.indexOf("async function deletePlayer")
  );
  ["Withdraw", "Disqualify", "Delete"].forEach((label) => {
    assert.equal(listRenderer.includes(`\"${label}\"`), false, `${label} must not be in the player row`);
    assert.equal(editForm.includes(`\"${label}\"`), true, `${label} must be in the Edit form`);
  });
});

test("In-Person page exposes Swiss rollback, inactive-player and late-entry recovery", () => {
  [
    "Add late player",
    "Preview late entry",
    "Confirm late entry",
    "will be paired with the current bye recipient",
    "Cancel round",
    "Draft-round no-show",
    "Withdraw",
    "Disqualify",
    "no-show",
    "These",
    "results will stop counting",
  ].forEach((text) => assert.ok(inPersonHtml.includes(text), `missing recovery UI text: ${text}`));

  [
    /\/participants\/late\/preview/,
    /\/participants\/late\/confirm/,
    /\/participants\/\$\{encodeURIComponent\(participant\.id\)\}\/status/,
    /\/cancellation-preview/,
    /\/cancel`/,
    /finish_reason: "no_show"/,
  ].forEach((pattern) => assert.match(inPersonHtml, pattern));
  assert.doesNotMatch(inPersonHtml, /Late-entry mode \*/);
});

test("In-Person page contains manual playoff setup, progression and medal completion", () => {
  [
    "Single-elimination playoff",
    "Manual ${playoff.participant_count}-player bracket setup",
    "Preview playoff bracket",
    "Confirm and start playoff",
    "Make streaming table",
    "Bronze medal match",
    "Complete tournament",
    "dependent match has already been played",
  ].forEach((text) => assert.ok(inPersonHtml.includes(text), `missing playoff UI text: ${text}`));

  [
    /data-ip-tab="playoff"/,
    /\/playoff\/preview/,
    /\/playoff\/confirm/,
    /\/playoff\/rounds\/\$\{encodeURIComponent\(round\.id\)\}\/publish/,
    /\/playoff\/matches\/\$\{encodeURIComponent\(match\.id\)\}\/table/,
    /\/streaming-table/,
    /createResultForm\(match, \{ stage: "playoff" \}\)/,
    /\/playoff\/complete/,
  ].forEach((pattern) => assert.match(inPersonHtml, pattern));
});

test("all Player Hub scripts parse after the In-Person additions", () => {
  assertEmbeddedScriptsParse(menuHtml, "Player Hub menu");
  assertEmbeddedScriptsParse(hubHtml, "Player Hub landing");
  assertEmbeddedScriptsParse(inPersonHtml, "In-Person page");
});
