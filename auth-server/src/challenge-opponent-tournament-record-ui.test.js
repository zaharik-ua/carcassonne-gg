import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const html = readFileSync(
  new URL("../../gg-html/player-hub/challenges.html", import.meta.url),
  "utf8"
);

function getFunctionSource(name) {
  const start = html.indexOf(`  function ${name}(`);
  const end = html.indexOf("\n  function ", start + 1);
  assert.ok(start >= 0, `${name} must exist`);
  return html.slice(start, end >= 0 ? end : html.length);
}

test("Open to match shows a linked tournament record below BGA Elo", () => {
  const rowSource = getFunctionSource("createOpponentRow");
  const bgaEloIndex = rowSource.indexOf('createRatingLabel("BGA Elo: ")');
  const tournamentRecordIndex = rowSource.indexOf("createOpponentTournamentRecordLine(period, opponent)");
  assert.ok(bgaEloIndex >= 0 && tournamentRecordIndex > bgaEloIndex);
});

test("Tournament record uses the short title and win-loss values only for linked periods", () => {
  const source = getFunctionSource("createOpponentTournamentRecordLine");
  assert.match(source, /period\?\.rivals_tournament_id/);
  assert.match(source, /if \(!tournamentId\) return null/);
  assert.match(source, /period\?\.rivals_tournament_short_title \|\| tournamentId/);
  assert.match(source, /opponent\?\.tournament_wins/);
  assert.match(source, /opponent\?\.tournament_losses/);
});

test("Tournament wins and losses have dark colors and compact vertical spacing", () => {
  assert.match(html, /\.challenge-opponent-ratings \{[\s\S]*?gap: 2px;/);
  assert.match(html, /\.challenge-opponent-tournament-wins \{[\s\S]*?color: #166534;/);
  assert.match(html, /\.challenge-opponent-tournament-losses \{[\s\S]*?color: #991b1b;/);
});
