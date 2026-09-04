import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const ua2025Html = readFileSync(
  new URL("../../UA/Ukraine-2025/ua2025.html", import.meta.url),
  "utf8"
);
const ua2026Html = readFileSync(
  new URL("../../UA/Ukraine-2025/ua2026.html", import.meta.url),
  "utf8"
);

function extractBlock(html, startPattern, endPattern) {
  const start = html.indexOf(startPattern);
  const end = html.indexOf(endPattern, start + startPattern.length);
  assert.ok(start >= 0 && end > start, `Missing ${startPattern} block`);
  return html.slice(start + startPattern.length, end);
}

const rendererJs = extractBlock(ua2026Html, "<script>", "</script>");
const ua2025Css = extractBlock(ua2025Html, "<style>", "</style>");
const ua2026Css = extractBlock(ua2026Html, "<style>", "</style>");
const ua2025Shell = extractBlock(ua2025Html, "<!-- HTML -->", "<!-- СКРИПТ -->");
const ua2026Shell = extractBlock(ua2026Html, "<!-- HTML -->", "<!-- СКРИПТ -->");
const normalizeFormatting = (value) => value.replace(/[ \t]+$/gm, "");

function createRendererHarness(pathname = "/") {
  return new Function(
    "window",
    "document",
    `${rendererJs}\nreturn { findDefaultOpenSwissRound };`
  )(
    { location: { pathname } },
    { addEventListener() {} }
  );
}

test("UA 2026 preserves the UA 2025 visual shell and stylesheet", () => {
  assert.equal(normalizeFormatting(ua2026Css), normalizeFormatting(ua2025Css));
  assert.equal(normalizeFormatting(ua2026Shell), normalizeFormatting(ua2025Shell));
  assert.match(ua2026Css, /\.group-table\s*\{/);
  assert.match(ua2026Css, /\.match-table\s*\{/);
  assert.match(ua2026Css, /#playoff-bracket\s*\{/);
  assert.match(ua2026Css, /@media \(max-width: 768px\)/);
});
test("UA 2026 keeps the original tab order and rendering functions", () => {
  assert.doesNotThrow(() => new Function(rendererJs));
  const playoffsIndex = rendererJs.indexOf('const stage2Tab = { id: "stage2"');
  const swissIndex = rendererJs.indexOf('let stage1Tab = { id: "stage1"');
  const roundsIndex = rendererJs.indexOf('tabDefs.push({ id: "matches"');
  const playersIndex = rendererJs.indexOf('tabDefs.push({ id: "players"');
  assert.ok(playoffsIndex >= 0);
  assert.ok(playoffsIndex < swissIndex && swissIndex < roundsIndex && roundsIndex < playersIndex);
  assert.match(rendererJs, /function renderPlayersTable\(/);
  assert.match(rendererJs, /function renderStandingsData\(/);
  assert.match(rendererJs, /function renderMatchesData\(/);
  assert.match(rendererJs, /function renderSingleElimination\(/);
});

test("UA 2026 adapts the public database aggregate to the legacy renderer", () => {
  const tournamentId = "ipt_d8937ade-73e7-4a5c-9472-a46c21c73740";
  assert.match(rendererJs, new RegExp(tournamentId));
  assert.match(rendererJs, /\/public\/in-person-tournaments\/\$\{encodeURIComponent\(currentTournamentId\)\}/);
  assert.match(rendererJs, /function adaptPublicTournament\(data\)/);
  assert.match(rendererJs, /data\?\.swiss\?\.rounds/);
  assert.match(rendererJs, /data\?\.swiss\?\.standings\?\.rows/);
  assert.match(rendererJs, /data\?\.playoff\?\.rounds/);
  assert.match(rendererJs, /city_icon_url/);
  assert.match(rendererJs, /starting_participant_id/);
  assert.match(rendererJs, /next_match_for_winner_id/);
  assert.match(rendererJs, /entity\?\.\[localField\] \|\| entity\?\.\[englishField\]/);
  assert.doesNotMatch(ua2026Html, /ua2025\.json|fetchUa2025Data/);
});

test("UA 2026 switches every localized data surface when the URL contains /en/", () => {
  assert.match(rendererJs, /\(\?:\^\|\\\/\)en\(\?:\\\/\|\$\)\/i\.test\(window\.location\.pathname\)/);
  assert.match(rendererJs, /const t = \(ua, en\) => isEn \? en : ua/);
  assert.match(rendererJs, /currentTournament\?\.name_en \|\| currentTournament\?\.name_local/);
  assert.match(rendererJs, /p\.player_name_en \|\| p\.player_name/);
  assert.match(rendererJs, /p\.city_en \?\? p\.city/);
  assert.match(rendererJs, /m\.round_en/);
  assert.match(rendererJs, /Ukrainian Carcassonne Championship 2026 — failed to load data/);
});

test("Rounds tab opens the latest active Swiss round but none after the final round", () => {
  const { findDefaultOpenSwissRound } = createRendererHarness();
  const completedMatch = { status: "completed" };
  const activeMatch = { status: "scheduled" };

  assert.equal(findDefaultOpenSwissRound([], 5), "");
  assert.equal(findDefaultOpenSwissRound([
    { round_number: 1, status: "completed", matches: [completedMatch] },
    { round_number: 2, status: "published", matches: [completedMatch, activeMatch] },
  ], 5), "2");
  assert.equal(findDefaultOpenSwissRound([
    { round_number: 2, status: "completed", matches: [completedMatch] },
  ], 5), "2", "keep the last completed round open until the next one exists");
  assert.equal(findDefaultOpenSwissRound([
    { round_number: 5, status: "published", matches: [completedMatch, activeMatch] },
  ], 5), "5");
  assert.equal(findDefaultOpenSwissRound([
    { round_number: 5, status: "published", matches: [completedMatch] },
  ], 5), "", "all configured Swiss rounds have been played");
  assert.match(rendererJs, /toggle\.dataset\.round = round/);
  assert.match(rendererJs, /toggle\.dataset\.round === defaultOpenSwissRound/);
});

test("UA 2026 stays self-contained and handles stages that have not started", () => {
  assert.match(rendererJs, /Плейофф ще не розпочався/);
  assert.match(rendererJs, /Турнірна таблиця з’явиться/);
  assert.match(rendererJs, /Раунди ще не розпочалися/);
  assert.doesNotMatch(rendererJs, /(?:tr|row)\.innerHTML\s*=/);
  assert.doesNotMatch(ua2026Html, /gg-html\/in-person|in-person-tournament\.(?:css|js)/);
  assert.ok(ua2026Html.length > ua2025Html.length);
});
