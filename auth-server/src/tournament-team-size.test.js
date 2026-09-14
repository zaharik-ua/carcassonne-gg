import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const myTournamentsHtml = readFileSync(
  new URL("../../gg-html/player-hub/my-tournaments.html", import.meta.url),
  "utf8"
);
const tournamentsHtml = readFileSync(
  new URL("../../gg-html/player-hub/tournaments.html", import.meta.url),
  "utf8"
);
const serverSource = readFileSync(new URL("./server.js", import.meta.url), "utf8");

test("My Tournaments edits Team size for Teams tournaments with a default of 10", () => {
  assert.match(
    myTournamentsHtml,
    /const teamSizeField = createTournamentFormRow\(\s*"Team size",\s*tournament\?\.team_size \?\? 10,/
  );
  assert.match(myTournamentsHtml, /teamSizeField\.row\.classList\.toggle\("cp-hidden", !isTeams\)/);
  assert.match(myTournamentsHtml, /team_size: teamSize/);
  assert.match(myTournamentsHtml, /Team size must be a positive integer\./);
});

test("Manage team renders the configured number of player selectors", () => {
  assert.match(
    tournamentsHtml,
    /const maxPlayers = getTournamentTeamSize\(team\?\.team_size\)[\s\S]*?\.slice\(0, maxPlayers\)[\s\S]*?selectedIds\.length < maxPlayers/
  );
  assert.match(
    tournamentsHtml,
    /teamSize: currentTournament\?\.team_size/
  );
  assert.doesNotMatch(tournamentsHtml, /\.slice\(0, 10\)[\s\S]{0,120}selectedIds\.length < 10/);
});

test("tournament API persists Team size and enforces it for roster writes", () => {
  assert.match(serverSource, /team_size INTEGER NOT NULL DEFAULT 10 CHECK \(team_size > 0\)/);
  assert.match(serverSource, /team_size: normalizeTournamentTeamSize\(row\.team_size\)/);
  assert.match(serverSource, /team_size must be a positive integer/);
  assert.match(
    serverSource,
    /const teamSize = normalizeTournamentTeamSize\(access\.tournament\?\.team_size\)[\s\S]*?playerIds\.length > teamSize/
  );
});

test("updated tournament player-hub scripts remain valid JavaScript", () => {
  [myTournamentsHtml, tournamentsHtml].forEach((html) => {
    const scripts = [...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/gi)];
    assert.ok(scripts.length > 0, "Player Hub page must contain a script");
    scripts.forEach((match) => assert.doesNotThrow(() => new Function(match[1])));
  });
});
