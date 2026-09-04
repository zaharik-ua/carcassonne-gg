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
    "Possible duplicate:",
    "Ready to form the first Swiss round.",
  ].forEach((text) => assert.ok(inPersonHtml.includes(text), `missing In-Person UI text: ${text}`));

  assert.match(inPersonHtml, /\/participants/);
  assert.match(inPersonHtml, /\/participant-cities/);
  assert.match(inPersonHtml, /method: "POST"/);
  assert.match(inPersonHtml, /function createCityPickerField/);
  assert.match(inPersonHtml, /function createMenuSelectField/);
  assert.match(inPersonHtml, /country\.input\.readOnly = true/);
  assert.match(inPersonHtml, /\/start-check-in/);
  assert.match(inPersonHtml, /\/check-in/);
  assert.match(inPersonHtml, /confirm_duplicate: confirmDuplicate/);
  assert.match(inPersonHtml, /function openCheckInModal/);
  assert.match(inPersonHtml, /checkedIn\.checked = true/);
  assert.match(inPersonHtml, /DRAW_NUMBER_TAKEN/);
  assert.match(inPersonHtml, /ip-draw-number-display/);
  assert.match(inPersonHtml, /ip-draw-number::-webkit-inner-spin-button/);
  assert.doesNotMatch(inPersonHtml, /Gaps and a missing #1 are allowed\./);
  assert.match(inPersonHtml, /@media \(max-width: 720px\)/);
  assert.match(inPersonHtml, /min-height: 44px/);
  const playerPanelMarkup = inPersonHtml.slice(
    inPersonHtml.indexOf('id="ipPlayersPanel"'),
    inPersonHtml.indexOf('id="ipCheckInPanel"')
  );
  assert.match(playerPanelMarkup, /id="ipCounters"/);
  assert.equal((inPersonHtml.match(/id="ipCounters"/g) || []).length, 1);
});

test("In-Person tournament header uses a conditional custom switcher", () => {
  assert.match(inPersonHtml, /id="ipTournamentName"/);
  assert.match(inPersonHtml, />Switch tournament</);
  assert.match(inPersonHtml, /id="ipTournamentMenu" class="ip-city-picker-menu"/);
  assert.match(inPersonHtml, /tournamentSwitcher\.classList\.toggle\("ip-hidden", state\.tournaments\.length <= 1\)/);
  assert.match(inPersonHtml, /option\.className = `ip-city-option/);
  assert.doesNotMatch(inPersonHtml, /id="ipTournamentSelect"/);
  assert.match(inPersonHtml, /`\$\{tournament\.swiss_rounds_count\} Swiss rounds`/);
  assert.match(inPersonHtml, /tournament\.playoff_preview\?\.participant_count/);
  assert.match(inPersonHtml, /players advance to playoff/);
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
  assert.match(inPersonHtml, /function createSwissTableCard/);
  assert.match(inPersonHtml, /function openSwissResultModal/);
  assert.match(inPersonHtml, /function createSwissResultForm/);
  assert.match(inPersonHtml, /function createMenuSelectField/);
  assert.match(inPersonHtml, /blue-mipple-no-bg-small\.png/);
  assert.match(inPersonHtml, /ip-swiss-table-card\.completed/);
  assert.match(inPersonHtml, /\.ip-swiss-table-number\s*\{[\s\S]*?font-size: 20px;[\s\S]*?font-weight: 700;/);
  assert.match(inPersonHtml, /table\.textContent = match\.is_bye \? "Bye" : String\(match\.table_number\)/);
  assert.match(inPersonHtml, /\.ip-starting-player-icon\s*\{[\s\S]*?width: 12px;[\s\S]*?height: 12px;[\s\S]*?display: inline-block;[\s\S]*?margin-right: 6px;/);
  assert.match(inPersonHtml, /\.ip-swiss-table-player-name\s*\{[\s\S]*?max-width: 100%;[\s\S]*?font-size: 16px;[\s\S]*?overflow-wrap: anywhere;[\s\S]*?text-align: center;[\s\S]*?white-space: normal;/);
  assert.match(inPersonHtml, /return participant\?\.name_local[\s\S]*?participant_\$\{side\}_name_local[\s\S]*?participant\?\.name_en/);
  const swissTablePlayerRenderer = inPersonHtml.slice(
    inPersonHtml.indexOf("function createSwissTablePlayer"),
    inPersonHtml.indexOf("function createSwissTableCard")
  );
  assert.ok(
    swissTablePlayerRenderer.indexOf("name.appendChild(starter)")
      < swissTablePlayerRenderer.indexOf("name.appendChild(document.createTextNode"),
    "the starting-player icon must be inline immediately before the player name"
  );
  const swissResultModal = inPersonHtml.slice(
    inPersonHtml.indexOf("function openSwissResultModal"),
    inPersonHtml.indexOf("function createSwissResultForm")
  );
  assert.doesNotMatch(swissResultModal, /players\.textContent/);
  const swissResultForm = inPersonHtml.slice(
    inPersonHtml.indexOf("function createSwissResultForm"),
    inPersonHtml.indexOf("function createResultForm")
  );
  [
    '"Starting player:"',
    '"Time lost:"',
    'won.textContent = "Won"',
    'createButton("Add admin note"',
    'result_type: "points"',
    'result_type: "time_forfeit"',
  ].forEach((text) => assert.ok(swissResultForm.includes(text), `missing Swiss score form text: ${text}`));
  assert.doesNotMatch(swissResultForm, /"Result type"|"Win \/ loss"|"Technical reason"/);
  assert.match(inPersonHtml, /\.ip-swiss-score-input\s*\{[\s\S]*?width: 112px;[\s\S]*?height: 112px;[\s\S]*?font-size: 36px;/);
  assert.match(inPersonHtml, /\.ip-swiss-score-player-name\s*\{[\s\S]*?font-weight: 500;/);
  assert.match(inPersonHtml, /\.ip-swiss-score-won\s*\{[\s\S]*?font-size: 15px;/);
  assert.doesNotMatch(inPersonHtml, /\.ip-admin-note-toggle\s*\{[\s\S]*?text-decoration:\s*underline/);
  assert.match(swissResultForm, /timeLost\.onChange\(\(\) => syncForm\(\)\)/);
  assert.doesNotMatch(swissResultForm, /setForfeitScore|scoreLocked/);
  assert.match(swissResultForm, /result_type: "time_forfeit",[\s\S]*?points_a: Number\(scoreA\.input\.value\),[\s\S]*?points_b: Number\(scoreB\.input\.value\)/);
  assert.ok(
    swissResultForm.indexOf("scoreGrid.appendChild(scoreA.input)")
      < swissResultForm.indexOf("scoreGrid.appendChild(scoreA.won)"),
    "Won must be rendered below the score input"
  );
  const menuSelectField = inPersonHtml.slice(
    inPersonHtml.indexOf("function createMenuSelectField"),
    inPersonHtml.indexOf("function cityLabel")
  );
  assert.match(menuSelectField, /ip-city-picker-btn/);
  assert.match(menuSelectField, /ip-city-picker-menu/);
  assert.doesNotMatch(menuSelectField, /searchInput|ip-city-search/);
  assert.match(inPersonHtml, /ip-time-lost-icon/);
  assert.match(inPersonHtml, /round\.status === "published"[\s\S]*openSwissResultModal\(match\)/);
  assert.doesNotMatch(inPersonHtml, /Preview pairings, publish a round, then enter every table result\./);
  assert.match(inPersonHtml, /if \(warningCount > 0\)/);
  const swissPreviewRenderer = inPersonHtml.slice(
    inPersonHtml.indexOf("function renderSwissPreview()"),
    inPersonHtml.indexOf("function renderSwissMatches()")
  );
  assert.ok(
    swissPreviewRenderer.indexOf("swissPreview.appendChild(actions)")
      < swissPreviewRenderer.indexOf("(preview.matches || []).forEach"),
    "Swiss preview actions must be rendered above the tables"
  );
  assert.match(inPersonHtml, /round\.progress\.completed === round\.progress\.total/);
  assert.match(inPersonHtml, /function swissTableProgress\(round\)[\s\S]*?filter\(\(match\) => !match\.is_bye\)/);
  assert.match(inPersonHtml, /const rounds = state\.swiss\?\.rounds \|\| \[\];[\s\S]*?rounds\.forEach\(\(round\) =>/);
  const swissMatchesRenderer = inPersonHtml.slice(
    inPersonHtml.indexOf("function renderSwissMatches()"),
    inPersonHtml.indexOf("function renderSwiss()")
  );
  assert.doesNotMatch(swissMatchesRenderer, /progress\.textContent|heading\.appendChild\(progress\)/);
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
