import assert from "node:assert/strict";
import test from "node:test";
import { resolveTournamentTextPatch } from "./tournament-update.js";

test("preserves tournament text fields omitted from a partial update", () => {
  const currentTournament = {
    about: "<p>Existing about</p>",
    rules: "<p>Existing rules</p>",
  };

  assert.equal(
    resolveTournamentTextPatch({ name: "Updated name" }, currentTournament, "about"),
    currentTournament.about
  );
  assert.equal(
    resolveTournamentTextPatch({ name: "Updated name" }, currentTournament, "rules"),
    currentTournament.rules
  );
});

test("normalizes explicitly updated or cleared tournament text fields", () => {
  const currentTournament = { about: "Old about", rules: "Old rules" };

  assert.equal(
    resolveTournamentTextPatch({ about: "  <p>New about</p>  " }, currentTournament, "about"),
    "<p>New about</p>"
  );
  assert.equal(resolveTournamentTextPatch({ rules: "" }, currentTournament, "rules"), null);
  assert.equal(resolveTournamentTextPatch({ rules: null }, currentTournament, "rules"), null);
});
