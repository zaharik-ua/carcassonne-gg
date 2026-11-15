import assert from "node:assert/strict";
import test from "node:test";
import { getCclFormat, listCclFormats, resolveTournamentFormatKeyInput } from "./ccl-formats.js";

test("the registry lists CCL 2024-2027; only CCL 2027 is available", () => {
  assert.deepEqual(listCclFormats(), [
    { key: "ccl-2024", name: "CCL 2024", available: false },
    { key: "ccl-2025", name: "CCL 2025", available: false },
    { key: "ccl-2026", name: "CCL 2026", available: false },
    { key: "ccl-2027", name: "CCL 2027", available: true },
  ]);
  assert.equal(getCclFormat(" CCL-2027 ").key, "ccl-2027");
  assert.equal(getCclFormat("ccl-2030"), null);
});

test("CCL 2027: Qualifiers with Q1, Q2, Q3, then League Stage and Playoffs", () => {
  const format = getCclFormat("ccl-2027");
  assert.deepEqual(format.stages.map((stage) => stage.name), ["Qualifiers", "League Stage", "Playoffs"]);
  const paths = format.stages[0].children;
  assert.deepEqual(paths.map((path) => [path.key, path.format, path.groups.join(""), path.settings.grand_final]), [
    ["Q1", "double_elimination", "ABCDE", "none"],
    ["Q2", "double_elimination", "FGHI", "none"],
    ["Q3", "double_elimination", "XYZ", "reset"],
  ]);
  for (const path of paths) {
    assert.equal(path.settings.group_size, 8);
    assert.equal(path.settings.seeded_per_group, 4);
    assert.equal(path.settings.duel_format, "Bo3");
  }
  assert.deepEqual(paths.map((path) => path.settings.advance.length), [2, 2, 1]);
  assert.deepEqual(paths[2].settings.rounds.slice(-2).map((round) => [round.week, round.name]), [
    [6, "Group Final"], [6, "Group Final Reset"],
  ]);
  assert.equal(Object.isFrozen(paths[0].settings.rounds), true);
});

test("format_key validation", () => {
  const ccl = { isCcl: true };
  assert.deepEqual(resolveTournamentFormatKeyInput({ format_key: "ccl-2027" }, null, ccl), { value: "ccl-2027" });
  assert.deepEqual(resolveTournamentFormatKeyInput({}, { format_key: "ccl-2027" }, ccl), { value: "ccl-2027" });
  assert.deepEqual(resolveTournamentFormatKeyInput({ format_key: null }, { format_key: "ccl-2027" }, ccl), { value: null });
  assert.match(resolveTournamentFormatKeyInput({ format_key: "ccl-2026" }, null, ccl).error, /not available/);
  // A key saved earlier stays valid on a later save.
  assert.deepEqual(resolveTournamentFormatKeyInput({ format_key: "ccl-2026" }, { format_key: "ccl-2026" }, ccl), { value: "ccl-2026" });
  assert.match(resolveTournamentFormatKeyInput({ format_key: "x" }, null, ccl).error, /must be one of/);
  // Other categories never keep a format, even one saved before the category changed.
  assert.deepEqual(resolveTournamentFormatKeyInput({}, { format_key: "ccl-2027" }, { isCcl: false }), { value: null });
  assert.match(resolveTournamentFormatKeyInput({ format_key: "ccl-2027" }, null, { isCcl: false }).error, /only for CCL/);
});
