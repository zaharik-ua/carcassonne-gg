import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const tournamentsHtml = readFileSync(
  new URL("../../gg-html/player-hub/tournaments.html", import.meta.url),
  "utf8"
);

test("captains see the lineup reset warning before proposing or accepting a time change", () => {
  assert.match(
    tournamentsHtml,
    /function openMatchTimeLineupResetWarning\([\s\S]*?title\.textContent = "Warning"/
  );
  assert.match(
    tournamentsHtml,
    /if \(hasMatchTime && !warningConfirmed\) \{[\s\S]*?If this time change is accepted, all existing lineups will be deleted\.[\s\S]*?onConfirm: \(\) => sendProposal\(true\)/
  );
  assert.match(
    tournamentsHtml,
    /if \(hasMatchTime && !warningConfirmed\) \{[\s\S]*?Accepting this time change will delete all existing lineups\.[\s\S]*?onConfirm: \(\) => acceptProposal\(true\)/
  );
});
