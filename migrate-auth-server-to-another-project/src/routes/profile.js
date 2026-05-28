import express from "express";
import { dbGet, dbRun, loadUserWithProfile } from "../db.js";
import { requireAuthenticated } from "../middleware/auth.js";

export const profileRouter = express.Router();

function normalizeText(value) {
  const normalized = String(value ?? "").trim();
  return normalized || null;
}

function normalizeUrl(value, fieldName) {
  const normalized = normalizeText(value);
  if (!normalized) return null;

  try {
    const url = new URL(normalized);
    if (url.protocol === "http:" || url.protocol === "https:") return normalized;
  } catch (_error) {
    if (normalized.startsWith("/")) return normalized;
  }

  const error = new Error(`${fieldName} must be an http(s) URL or an absolute local path`);
  error.status = 400;
  throw error;
}

function parseMetadata(value) {
  if (value === undefined) return undefined;
  if (value === null) return {};
  if (typeof value === "object" && !Array.isArray(value)) return value;

  const error = new Error("metadata must be an object");
  error.status = 400;
  throw error;
}

function pickPayloadField(payload, snakeName, camelName, fallback) {
  if (Object.prototype.hasOwnProperty.call(payload, snakeName)) return payload[snakeName];
  if (Object.prototype.hasOwnProperty.call(payload, camelName)) return payload[camelName];
  return fallback;
}

function toProfilePayload(row) {
  return {
    id: row.profile_id,
    user_id: row.id,
    display_name: row.display_name || null,
    avatar_url: row.avatar_url || null,
    bio: row.bio || null,
    location: row.location || null,
    website_url: row.website_url || null,
    metadata: safeParseJson(row.metadata_json) || {},
  };
}

function safeParseJson(value) {
  try {
    return JSON.parse(String(value || "{}"));
  } catch (_error) {
    return null;
  }
}

profileRouter.get("/me", requireAuthenticated, (req, res) => {
  return res.json({ ok: true, profile: toProfilePayload(req.user) });
});

profileRouter.patch("/me", requireAuthenticated, async (req, res, next) => {
  try {
    const payload = req.body && typeof req.body === "object" ? req.body : {};
    const metadata = parseMetadata(payload.metadata);

    const currentMetadata = safeParseJson(req.user.metadata_json) || {};
    const nextMetadata = metadata === undefined
      ? currentMetadata
      : { ...currentMetadata, ...metadata };

    await dbRun(
      `
        UPDATE profiles
        SET
          display_name = ?,
          avatar_url = ?,
          bio = ?,
          location = ?,
          website_url = ?,
          metadata_json = ?,
          updated_at = CURRENT_TIMESTAMP
        WHERE user_id = ?
      `,
      [
        normalizeText(pickPayloadField(payload, "display_name", "displayName", req.user.display_name)),
        normalizeUrl(pickPayloadField(payload, "avatar_url", "avatarUrl", req.user.avatar_url), "avatar_url"),
        normalizeText(pickPayloadField(payload, "bio", "bio", req.user.bio)),
        normalizeText(pickPayloadField(payload, "location", "location", req.user.location)),
        normalizeUrl(pickPayloadField(payload, "website_url", "websiteUrl", req.user.website_url), "website_url"),
        JSON.stringify(nextMetadata),
        req.user.id,
      ]
    );

    const user = await loadUserWithProfile(req.user.id);
    return res.json({ ok: true, profile: toProfilePayload(user) });
  } catch (error) {
    return next(error);
  }
});

profileRouter.get("/:id", async (req, res, next) => {
  try {
    const profileId = Number.parseInt(String(req.params.id || ""), 10);
    if (!Number.isInteger(profileId) || profileId <= 0) {
      return res.status(400).json({ ok: false, message: "Invalid profile id" });
    }

    const row = await dbGet(
      `
        SELECT
          p.id AS profile_id,
          p.user_id AS id,
          p.display_name,
          p.avatar_url,
          p.bio,
          p.location,
          p.website_url,
          p.metadata_json
        FROM profiles p
        WHERE p.id = ?
        LIMIT 1
      `,
      [profileId]
    );

    if (!row) {
      return res.status(404).json({ ok: false, message: "Profile not found" });
    }

    return res.json({ ok: true, profile: toProfilePayload(row) });
  } catch (error) {
    return next(error);
  }
});
