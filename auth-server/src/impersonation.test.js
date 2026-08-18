import assert from "node:assert/strict";
import test from "node:test";
import {
  attachImpersonationAuthContext,
  createImpersonationState,
  getImpersonationAuthContext,
  getImpersonationUserSummary,
  normalizeImpersonationState,
} from "./impersonation.js";

test("impersonation state stores only its id, target user id and start time", () => {
  const state = createImpersonationState(42, {
    id: "imp-1",
    startedAt: "2026-08-18T10:00:00.000Z",
  });

  assert.deepEqual(state, {
    id: "imp-1",
    targetUserId: 42,
    startedAt: "2026-08-18T10:00:00.000Z",
  });
});

test("invalid impersonation state is rejected", () => {
  assert.equal(createImpersonationState(0), null);
  assert.equal(normalizeImpersonationState({ targetUserId: 42 }), null);
  assert.equal(normalizeImpersonationState({ id: "imp-1", targetUserId: "x", startedAt: "now" }), null);
});

test("auth context is attached without leaking into serialized user data", () => {
  const targetUser = { id: 42, name: "Target" };
  const actorUser = { id: 1, name: "Admin", admin: 1 };
  const state = createImpersonationState(42, {
    id: "imp-1",
    startedAt: "2026-08-18T10:00:00.000Z",
  });

  attachImpersonationAuthContext(targetUser, actorUser, state);

  assert.deepEqual(getImpersonationAuthContext(targetUser), {
    actorUser,
    impersonation: state,
  });
  assert.equal(JSON.stringify(targetUser), '{"id":42,"name":"Target"}');
});

test("public user summary excludes email and Google identity", () => {
  assert.deepEqual(getImpersonationUserSummary({
    id: 42,
    name: "Target User",
    picture: "https://example.com/avatar.png",
    bga_nickname: "target",
    email: "private@example.com",
    google_id: "google-secret",
  }), {
    id: 42,
    name: "Target User",
    picture: "https://example.com/avatar.png",
    bgaNickname: "target",
  });
});
