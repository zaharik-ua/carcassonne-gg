function normalizeFeatureFlag(value) {
  return ["1", "true", "yes", "on"].includes(String(value || "").trim().toLowerCase());
}

export function registerInPersonRoutes(app, {
  enabled = false,
  requireAdmin,
  requireInPersonTournamentAdmin,
  logger = console,
} = {}) {
  const featureEnabled = normalizeFeatureFlag(enabled);
  if (!featureEnabled) {
    logger?.info?.("[in-person] Routes disabled by feature gate");
    return { enabled: false };
  }
  if (typeof requireAdmin !== "function") {
    throw new Error("requireAdmin middleware is required when in-person routes are enabled");
  }
  if (typeof requireInPersonTournamentAdmin !== "function") {
    throw new Error(
      "requireInPersonTournamentAdmin middleware is required when in-person routes are enabled"
    );
  }

  app.get("/in-person-tournaments/_foundation", requireAdmin, (_req, res) => {
    res.json({ ok: true, feature: "in_person_tournaments", status: "foundation" });
  });
  app.get(
    "/in-person-tournaments/:tournamentId/_foundation",
    requireInPersonTournamentAdmin,
    (req, res) => {
      res.json({
        ok: true,
        feature: "in_person_tournaments",
        status: "foundation",
        tournament_id: req.inPersonTournamentId,
      });
    }
  );
  logger?.info?.("[in-person] Foundation routes enabled");
  return { enabled: true };
}
