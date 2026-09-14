import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const myTournamentsHtml = readFileSync(
  new URL("../../gg-html/player-hub/my-tournaments.html", import.meta.url),
  "utf8"
);
const serverSource = readFileSync(new URL("./server.js", import.meta.url), "utf8");

test("My Tournaments loads the assigned-admin scope and keeps Access users global-admin only", () => {
  assert.match(myTournamentsHtml, /scope=my-tournaments/);
  assert.match(
    myTournamentsHtml,
    /isGlobalAdmin \|\| String\(tournament\?\.access_role \|\| ""\)\.trim\(\)\.toLowerCase\(\) === "admin"/
  );
  assert.match(
    myTournamentsHtml,
    /canManageAccessUsers[\s\S]*?requestTournamentJson\(USER_OPTIONS_URL[\s\S]*?Promise\.resolve\(\{ users: \[\] \}\)/
  );
  assert.match(myTournamentsHtml, /if \(canManageAccessUsers\) form\.appendChild\(accessUsersEditor\.root\)/);
  assert.match(myTournamentsHtml, /if \(canManageAccessUsers\) \{[\s\S]*?payload\.access_users/);
  assert.match(myTournamentsHtml, /canUploadSource: tournamentEditorCache\.isGlobalAdmin/);
  assert.match(myTournamentsHtml, /isAdmin: tournamentEditorCache\.isGlobalAdmin/);

  const scripts = [...myTournamentsHtml.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/gi)];
  assert.ok(scripts.length > 0, "My Tournaments must contain a script");
  scripts.forEach((match) => assert.doesNotThrow(() => new Function(match[1])));
});

test("tournament editing routes use tournament-admin authorization", () => {
  [
    'app.patch("/tournaments/:id", requireTournamentAdmin',
    'app.get("/tournament-teams", requireTournamentAdmin',
    'app.post("/standings/recalculate", requireTournamentAdmin',
    'app.put("/standings", requireTournamentAdmin',
    'app.put("/tournament-teams", requireTournamentAdmin',
  ].forEach((route) => assert.ok(serverSource.includes(route), `missing authorization on ${route}`));

  assert.match(serverSource, /requestedScope === "my-tournaments"/);
  assert.match(serverSource, /Only global admins can edit tournament access users/);
  assert.match(serverSource, /if \(!canManageAccessUsers\) \{[\s\S]*?return commitTournamentUpdate\(\)/);
});
