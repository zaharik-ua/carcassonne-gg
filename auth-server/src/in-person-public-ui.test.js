import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const rendererJs = readFileSync(
  new URL("../../gg-html/in-person/in-person-tournament.js", import.meta.url),
  "utf8"
);
const rendererCss = readFileSync(
  new URL("../../gg-html/in-person/in-person-tournament.css", import.meta.url),
  "utf8"
);
const reusableHtml = readFileSync(
  new URL("../../gg-html/in-person/in-person-tournament.html", import.meta.url),
  "utf8"
);
const ua2026Html = readFileSync(
  new URL("../../UA/Ukraine-2025/ua2026.html", import.meta.url),
  "utf8"
);

test("public renderer exposes permanent Playoffs, Swiss, Rounds and Players tabs", () => {
  assert.doesNotThrow(() => new Function(rendererJs));
  const playoffsIndex = rendererJs.indexOf('{ id: "playoffs"');
  const swissIndex = rendererJs.indexOf('{ id: "swiss"');
  const roundsIndex = rendererJs.indexOf('{ id: "rounds"');
  const playersIndex = rendererJs.indexOf('{ id: "players"');
  assert.ok(playoffsIndex >= 0);
  assert.ok(playoffsIndex < swissIndex && swissIndex < roundsIndex && roundsIndex < playersIndex);
  assert.match(rendererJs, /role", "tablist"/);
  assert.match(rendererJs, /role", "tabpanel"/);
  assert.match(rendererJs, /aria-selected/);
  assert.match(rendererJs, /The playoff bracket has not been published yet/);
  assert.match(rendererJs, /Standings will appear after the first completed round/);
  assert.match(rendererJs, /No Swiss rounds have been published yet/);
  assert.match(rendererJs, /The player list has not been published yet/);
});

test("public renderer uses the aggregate API, ETag refresh and safe text nodes", () => {
  assert.match(rendererJs, /\/public\/in-person-tournaments\/\$\{encodeURIComponent\(tournamentIdentifier\)\}/);
  assert.match(rendererJs, /If-None-Match/);
  assert.match(rendererJs, /response\.status === 304/);
  assert.match(rendererJs, /window\.setInterval\(\(\) => load\(\{ silent: true \}\), 30000\)/);
  assert.match(rendererJs, /name_local/);
  assert.match(rendererJs, /participant_name_local/);
  assert.match(rendererJs, /Streaming table/);
  assert.match(rendererJs, /finishReasonLabel/);
  assert.doesNotMatch(rendererJs, /\.innerHTML\s*=/);
});

test("public renderer has desktop tables and responsive mobile bracket controls", () => {
  assert.match(rendererCss, /\.ipt-public-table\s*\{/);
  assert.match(rendererCss, /\.ipt-public-bracket\s*\{/);
  assert.match(rendererCss, /overflow-x:\s*auto/);
  assert.match(rendererCss, /@media \(max-width: 720px\)/);
  assert.match(rendererCss, /grid-template-columns:\s*repeat\(2, minmax\(0, 1fr\)\)/);
  assert.match(rendererCss, /min-height:\s*44px/);
  assert.match(reusableHtml, /data-in-person-tournament/);
  assert.match(reusableHtml, /in-person-tournament\.css/);
  assert.match(reusableHtml, /in-person-tournament\.js/);
});

test("UA 2026 page is a shared-renderer entry for the requested tournament ID", () => {
  const tournamentId = "ipt_d8937ade-73e7-4a5c-9472-a46c21c73740";
  assert.match(ua2026Html, new RegExp(tournamentId));
  assert.match(ua2026Html, /data-locale="uk"/);
  assert.match(ua2026Html, /in-person-tournament\.css/);
  assert.match(ua2026Html, /in-person-tournament\.js/);
  assert.doesNotMatch(ua2026Html, /ua2025\.json|UA2025|fetchUa2025Data/);
  assert.ok(ua2026Html.length < 1500, "the tournament page should stay a thin shared-renderer entry");
});
