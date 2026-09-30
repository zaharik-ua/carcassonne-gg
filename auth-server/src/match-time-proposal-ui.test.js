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
    /function openMatchTimeLineupResetWarning\([\s\S]*?title\.textContent = "Warning"[\s\S]*?confirmButton\.className = "cp-btn cp-btn-danger"/
  );
  assert.match(
    tournamentsHtml,
    /if \(hasMatchTime && hasLineupsToReset && !warningConfirmed\) \{[\s\S]*?If this time change is accepted, all existing lineups will be deleted\.[\s\S]*?confirmLabel: "Confirm lineup deletion"[\s\S]*?onConfirm: \(\) => sendProposal\(true\)/
  );
  assert.match(
    tournamentsHtml,
    /if \(hasMatchTime && hasLineupsToReset && !warningConfirmed\) \{[\s\S]*?Accepting this time change will delete all existing lineups\.[\s\S]*?confirmLabel: "Confirm lineup deletion"[\s\S]*?onConfirm: \(\) => acceptProposal\(true\)/
  );
});

test("captains skip the lineup reset warning when no active lineups exist", () => {
  assert.match(
    tournamentsHtml,
    /const hasLineupsToReset = \([\s\S]*?options\.hasExistingLineups === true[\s\S]*?isTruthyOne\(match\?\.team_1_lineup_added\)[\s\S]*?isTruthyOne\(match\?\.team_2_lineup_added\)[\s\S]*?hasValue\(match\?\.lineups_published_at\)[\s\S]*?\);/
  );
  assert.match(
    tournamentsHtml,
    /openMatchTimeProposalModal\(match, teamsById, captainTeamId, \{[\s\S]*?hasExistingLineups: matchLineups\.length > 0,[\s\S]*?\}\);/
  );
});
