import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const html = readFileSync(
  new URL("../../gg-html/player-hub/challenges.html", import.meta.url),
  "utf8"
);
const server = readFileSync(new URL("./server.js", import.meta.url), "utf8");

test("Challenge player UI locks actions and collapses action sections at the tournament TPR target", () => {
  assert.match(html, /function isChallengeTournamentTargetReached\(period\)/);
  assert.match(
    html,
    /function shouldCollapseChallengeActionSections\(period\)[\s\S]*?isChallengeTournamentTargetReached\(period\)/
  );
  assert.match(
    html,
    /function getLockedChallengePeriodStatus\(period\)[\s\S]*?return "tpr_target_reached"/
  );
  assert.match(html, /current_player_tpr_target_reached/);
  assert.match(html, /opponentAtTprTarget/);
  assert.match(html, /Tournament target reached/);
});

test("Challenge API enforces the TPR target for availability and request workflows", () => {
  for (const route of [
    'app.post("/challenge-periods/:id/requests"',
    'app.patch("/challenge-periods/:id/requests/:requestId/counter"',
    'app.patch("/challenge-periods/:id/requests/:requestId/accept"',
    'app.patch("/challenge-periods/:id/player-availability"',
    'app.patch("/challenge-periods/:id/player-status"',
  ]) {
    const routeStart = server.indexOf(route);
    assert.ok(routeStart >= 0, `${route} must exist`);
    const nextRoute = server.indexOf("\napp.", routeStart + route.length);
    const routeSource = server.slice(routeStart, nextRoute >= 0 ? nextRoute : server.length);
    assert.match(routeSource, /is_tpr_target_reached/);
    assert.match(routeSource, /createChallengeTprTargetReachedError/);
  }
});

test("Results Review match correction remains independent of the TPR action lock", () => {
  const routeStart = server.indexOf(
    'app.patch("/challenge-periods/:id/matches/:duelId/resolve-issue"'
  );
  const nextRoute = server.indexOf("\napp.", routeStart + 1);
  const routeSource = server.slice(routeStart, nextRoute);
  assert.ok(routeStart >= 0 && nextRoute > routeStart);
  assert.doesNotMatch(routeSource, /is_tpr_target_reached|createChallengeTprTargetReachedError/);
  assert.match(html, /result_review: "Results Review"/);
  assert.match(html, /function createChallengeDuelBlock\(/);
});

test("Challenge page scripts remain valid JavaScript", () => {
  const scripts = [...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/gi)];
  assert.ok(scripts.length > 0, "Challenge page must contain a script");
  scripts.forEach((match) => assert.doesNotThrow(() => new Function(match[1])));
});
