import assert from "node:assert/strict";
import test from "node:test";

import {
  didRankedDuelTransitionToDone,
  isCompletedRankedDuel,
} from "./profile-gg-elo-trigger.js";

test("detects only ranked duel transitions to Done", () => {
  for (const previousStatus of ["Planned", "In progress", "Error", "Cancelled", ""]) {
    assert.equal(didRankedDuelTransitionToDone(previousStatus, "Done", 1), true);
  }
  assert.equal(didRankedDuelTransitionToDone("Done", "Done", 1), false);
  assert.equal(didRankedDuelTransitionToDone("Planned", "Done", 0), false);
  assert.equal(didRankedDuelTransitionToDone("Planned", "Error", 1), false);
});

test("recognizes completed ranked duels after manual result edits", () => {
  assert.equal(isCompletedRankedDuel("Done", 1), true);
  assert.equal(isCompletedRankedDuel("done", "1"), true);
  assert.equal(isCompletedRankedDuel("Done", 0), false);
  assert.equal(isCompletedRankedDuel("Error", 1), false);
});
