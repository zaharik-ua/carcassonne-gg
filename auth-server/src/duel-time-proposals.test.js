import assert from "node:assert/strict";
import test from "node:test";

import {
  isDuelProposalSnapshotCurrent,
  planDuelTimeAcceptance,
  planDuelTimeProposal,
} from "./duel-time-proposals.js";

const NOW = Date.parse("2026-11-11T12:00:00Z");
const planned = (extra = {}) => ({
  id: "m1", status: "Planned", player_1_id: "olena", player_2_id: "taras", time_utc: null, ...extra,
});

test("players propose, counter and accept a time", () => {
  const first = planDuelTimeProposal({
    duel: planned(), actorPlayerId: "olena", proposedTimeUtc: "2026-11-20T18:00:00Z", nowMs: NOW,
  });
  assert.equal(first.event, "duel.time_proposal.created");
  assert.deepEqual(first.value, {
    proposed_time_utc: "2026-11-20T18:00:00.000Z", proposed_time_by_player_id: "olena", proposed_time_status: "pending",
  });

  const proposed = planned(first.value);
  assert.equal(planDuelTimeAcceptance({ duel: proposed, actorPlayerId: "olena", nowMs: NOW }).status, 403);
  const counter = planDuelTimeProposal({
    duel: proposed, actorPlayerId: "taras", proposedTimeUtc: "2026-11-21T18:00:00Z", nowMs: NOW,
  });
  assert.equal(counter.event, "duel.time_proposal.countered");
  assert.equal(planDuelTimeProposal({
    duel: proposed, actorPlayerId: "taras", proposedTimeUtc: "2026-11-20T18:00:00Z", nowMs: NOW,
  }).status, 400);

  const accepted = planDuelTimeAcceptance({ duel: planned(counter.value), actorPlayerId: "olena", nowMs: NOW });
  assert.deepEqual(accepted.value, { time_utc: "2026-11-21T18:00:00.000Z", proposed_time_status: "accepted" });

  const change = planDuelTimeProposal({
    duel: planned({ time_utc: accepted.value.time_utc, proposed_time_status: "accepted" }),
    actorPlayerId: "olena",
    proposedTimeUtc: "2026-11-22T18:00:00Z",
    nowMs: NOW,
  });
  assert.equal(change.event, "duel.time_change_proposed");
  assert.equal(change.value.proposed_time_status, "change_pending");
});

test("rejects proposals from outsiders, in the past, after the deadline or for finished matches", () => {
  const propose = (duel, actorPlayerId, proposedTimeUtc) => planDuelTimeProposal({ duel, actorPlayerId, proposedTimeUtc, nowMs: NOW });
  assert.equal(propose(planned(), "juan", "2026-11-20T18:00:00Z").status, 403);
  assert.equal(propose(planned(), "olena", "2026-11-01T18:00:00Z").status, 400);
  assert.match(propose(planned({ deadline_utc: "2026-11-15T23:59:00Z" }), "olena", "2026-11-20T18:00:00Z").error, /deadline/);
  assert.equal(propose(planned({ status: "Done" }), "olena", "2026-11-20T18:00:00Z").status, 409);
  assert.equal(planDuelTimeAcceptance({ duel: planned(), actorPlayerId: "taras", nowMs: NOW }).status, 409);
  assert.equal(planDuelTimeAcceptance({
    duel: planned({ proposed_time_utc: "2026-11-10T18:00:00Z", proposed_time_by_player_id: "olena", proposed_time_status: "pending" }),
    actorPlayerId: "taras",
    nowMs: NOW,
  }).status, 409);
});

test("detects a stale proposal snapshot", () => {
  const match = planned({ proposed_time_utc: "2026-11-20T18:00:00Z", proposed_time_by_player_id: "olena", proposed_time_status: "pending" });
  assert.equal(isDuelProposalSnapshotCurrent(match, {}), true);
  assert.equal(isDuelProposalSnapshotCurrent(match, {
    expected_proposed_time_utc: "2026-11-20T18:00:00.000Z",
    expected_proposed_time_by_player_id: "olena",
    expected_proposed_time_status: "pending",
  }), true);
  assert.equal(isDuelProposalSnapshotCurrent(match, {
    expected_proposed_time_utc: null, expected_proposed_time_by_player_id: null, expected_proposed_time_status: null,
  }), false);
});
