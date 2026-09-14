import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const tournamentsHtml = readFileSync(
  new URL("../../gg-html/player-hub/tournaments.html", import.meta.url),
  "utf8"
);
const serverSource = readFileSync(new URL("./server.js", import.meta.url), "utf8");

test("tournament captains can open roster management for their visible team", () => {
  assert.match(
    tournamentsHtml,
    /function renderTournamentPlayersSection\(team, allProfiles, tournamentId\)[\s\S]*?manageButton\.textContent = "Manage team";[\s\S]*?openManageTournamentTeamModal\(team, allProfiles, tournamentId\)/
  );
  assert.doesNotMatch(
    tournamentsHtml,
    /if \(options\.isGlobalAdmin === true\) \{[\s\S]*?manageButton\.textContent = "Manage team";/
  );
});

test("roster API limits non-admin writes to the captain's own tournament team", () => {
  assert.match(serverSource, /app\.put\("\/tournament-players", requireAuthenticated/);
  assert.match(
    serverSource,
    /!access\.canAccessAllTeams[\s\S]*?!access\.captainTeamIds\.some\(\(teamId\) => teamId === normalizedTeamId\)[\s\S]*?status\(403\)/
  );
});

test("tournaments player-hub scripts remain valid JavaScript", () => {
  const scripts = [...tournamentsHtml.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/gi)];
  assert.ok(scripts.length > 0, "Tournaments must contain a script");
  scripts.forEach((match) => assert.doesNotThrow(() => new Function(match[1])));
});
