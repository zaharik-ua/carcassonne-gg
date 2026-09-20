import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";

const source = readFileSync(new URL("./server.js", import.meta.url), "utf8");
function serverFunction(name) {
  const start = source.search(new RegExp(`(?:async )?function ${name}\\(`));
  assert.ok(start >= 0, `Missing function ${name}`);
  return source.slice(start, source.indexOf("\n}", start) + 2);
}

function createContext(tournamentOverrides = {}, matchOverrides = {}) {
  const tournament = {
    subtype: "Official", has_access: true, access_role: "admin",
    captain_team_ids: ["UA"], access_via_access_users: true,
    ...tournamentOverrides,
  };
  const match = {
    id: "match", tournament_id: "cup", team_1: "UA", team_2: "FR", status: "Planned",
    ...matchOverrides,
  };
  const context = vm.createContext({
    TOURNAMENT_ACCESS_TYPES: { OFFICIAL: "Official", FRIENDLY: "Friendly" },
    TOURNAMENT_ACCESS_ROLES: { ADMIN: "admin", CAPTAIN: "captain" },
    dbGetAsync: async () => match,
    loadTournamentAccessForUserAsync: async () => tournament,
  });
  for (const name of ["normalizeTournamentAccessType", "normalizeTournamentCaptainTeamIds", "resolveOfficialMatchCaptainTeamId", "loadMatchTimeProposalContext"]) {
    vm.runInContext(serverFunction(name), context);
  }
  return { context, tournament };
}

test("assigned team captains can propose match time when their effective role is admin", async () => {
  for (const admin of [0, 1]) {
    const { context } = createContext();
    const result = await context.loadMatchTimeProposalContext("match", { admin, association: "UA" });
    assert.equal(result.captainTeamId, "UA");
  }
});

test("an admin's association does not override their assigned captain team", async () => {
  const { context } = createContext({ captain_team_ids: ["FR"] });
  const result = await context.loadMatchTimeProposalContext("match", { admin: 1, association: "UA" });
  assert.equal(result.captainTeamId, "FR");
});

test("admin access and a matching association alone do not grant captain actions", async () => {
  for (const captain_team_ids of [[], ["DE"]]) {
    const { context } = createContext({ captain_team_ids });
    await assert.rejects(context.loadMatchTimeProposalContext("match", { admin: 1, association: "UA" }), { httpStatus: 403 });
  }
});

test("ordinary and legacy captains retain access", async () => {
  for (const captain_team_ids of [["UA"], []]) {
    const { context } = createContext({ access_role: "captain", captain_team_ids });
    const result = await context.loadMatchTimeProposalContext("match", { association: "UA" });
    assert.equal(result.captainTeamId, "UA");
  }
});

test("captain actions still reject ambiguous teams, missing access and friendly tournaments", async () => {
  for (const overrides of [
    { captain_team_ids: ["UA", "FR"] },
    { has_access: false },
    { subtype: "Friendly" },
  ]) {
    const { context } = createContext(overrides);
    await assert.rejects(context.loadMatchTimeProposalContext("match", { association: "UA" }), { httpStatus: 403 });
  }
  const { context } = createContext({}, { status: "Done" });
  await assert.rejects(context.loadMatchTimeProposalContext("match", { association: "UA" }), { httpStatus: 409 });
});
