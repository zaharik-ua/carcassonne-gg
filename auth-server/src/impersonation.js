import { randomUUID } from "node:crypto";

const IMPERSONATION_AUTH_CONTEXT = Symbol("impersonationAuthContext");

function normalizePositiveInteger(value) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

export function createImpersonationState(targetUserId, options = {}) {
  const normalizedTargetUserId = normalizePositiveInteger(targetUserId);
  if (!normalizedTargetUserId) return null;

  return {
    id: String(options.id || randomUUID()),
    targetUserId: normalizedTargetUserId,
    startedAt: String(options.startedAt || new Date().toISOString()),
  };
}

export function normalizeImpersonationState(value) {
  if (!value || typeof value !== "object") return null;
  const targetUserId = normalizePositiveInteger(value.targetUserId);
  const id = String(value.id || "").trim();
  const startedAt = String(value.startedAt || "").trim();
  if (!targetUserId || !id || !startedAt) return null;
  return { id, targetUserId, startedAt };
}

export function attachImpersonationAuthContext(targetUser, actorUser, impersonation) {
  if (!targetUser || typeof targetUser !== "object") return targetUser;
  Object.defineProperty(targetUser, IMPERSONATION_AUTH_CONTEXT, {
    configurable: false,
    enumerable: false,
    writable: false,
    value: {
      actorUser: actorUser || null,
      impersonation: normalizeImpersonationState(impersonation),
    },
  });
  return targetUser;
}

export function getImpersonationAuthContext(user) {
  if (!user || typeof user !== "object") return null;
  return user[IMPERSONATION_AUTH_CONTEXT] || null;
}

export function getImpersonationUserSummary(user) {
  if (!user || typeof user !== "object") return null;
  const id = normalizePositiveInteger(user.id);
  if (!id) return null;
  return {
    id,
    name: String(user.name || user.profile_name || user.bga_nickname || "").trim() || null,
    picture: String(user.picture || "").trim() || null,
    bgaNickname: String(user.bga_nickname || "").trim() || null,
  };
}
