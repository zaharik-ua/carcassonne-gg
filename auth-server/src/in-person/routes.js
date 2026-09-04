import { createInPersonService } from "./service.js";
import { InPersonError } from "./validation.js";

function normalizeFeatureFlag(value) {
  return ["1", "true", "yes", "on"].includes(String(value || "").trim().toLowerCase());
}

function parseBoolean(value) {
  return ["1", "true", "yes", "on"].includes(String(value || "").trim().toLowerCase());
}

function sendError(error, res, next, logger) {
  if (error instanceof InPersonError) {
    const payload = { ok: false, code: error.code, message: error.message };
    if (error.details) payload.details = error.details;
    res.status(error.status).json(payload);
    return;
  }
  logger?.error?.("[in-person] Request failed", error);
  if (typeof next === "function") next(error);
  else res.status(500).json({ ok: false, message: "In-person request failed" });
}

function asyncHandler(handler, logger) {
  return async (req, res, next) => {
    try {
      await handler(req, res);
    } catch (error) {
      sendError(error, res, next, logger);
    }
  };
}

export function registerInPersonRoutes(app, {
  enabled = false,
  db,
  service,
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

  const inPersonService = service || createInPersonService({ db });

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

  app.get(
    "/cities",
    requireAdmin,
    asyncHandler(async (req, res) => {
      const cities = await inPersonService.listCities({
        associationId: req.query?.association_id,
        includeArchived: parseBoolean(req.query?.include_archived),
      });
      res.json({ ok: true, cities });
    }, logger)
  );
  app.post(
    "/cities",
    requireAdmin,
    asyncHandler(async (req, res) => {
      const city = await inPersonService.createCity(req.body || {});
      res.status(201).json({ ok: true, city });
    }, logger)
  );
  app.patch(
    "/cities/:cityId",
    requireAdmin,
    asyncHandler(async (req, res) => {
      const city = await inPersonService.updateCity(req.params.cityId, req.body || {});
      res.json({ ok: true, city });
    }, logger)
  );
  app.delete(
    "/cities/:cityId",
    requireAdmin,
    asyncHandler(async (req, res) => {
      const city = await inPersonService.archiveCity(req.params.cityId);
      res.json({ ok: true, city });
    }, logger)
  );
  app.post(
    "/cities/:cityId/restore",
    requireAdmin,
    asyncHandler(async (req, res) => {
      const city = await inPersonService.restoreCity(req.params.cityId);
      res.json({ ok: true, city });
    }, logger)
  );

  app.get(
    "/in-person-tournaments",
    requireAdmin,
    asyncHandler(async (_req, res) => {
      const tournaments = await inPersonService.listTournaments();
      res.json({ ok: true, tournaments });
    }, logger)
  );
  app.post(
    "/in-person-tournaments",
    requireAdmin,
    asyncHandler(async (req, res) => {
      const tournament = await inPersonService.createTournament(req.body || {});
      res.status(201).json({ ok: true, tournament });
    }, logger)
  );
  app.get(
    "/in-person-tournaments/:tournamentId",
    requireAdmin,
    asyncHandler(async (req, res) => {
      const tournament = await inPersonService.getTournament(req.params.tournamentId);
      res.json({ ok: true, tournament });
    }, logger)
  );
  app.patch(
    "/in-person-tournaments/:tournamentId",
    requireAdmin,
    asyncHandler(async (req, res) => {
      const tournament = await inPersonService.updateTournament(
        req.params.tournamentId,
        req.body || {}
      );
      res.json({ ok: true, tournament });
    }, logger)
  );
  app.post(
    "/in-person-tournaments/:tournamentId/publish",
    requireAdmin,
    asyncHandler(async (req, res) => {
      const tournament = await inPersonService.publishTournament(req.params.tournamentId);
      res.json({ ok: true, tournament });
    }, logger)
  );
  app.post(
    "/in-person-tournaments/:tournamentId/cancel",
    requireAdmin,
    asyncHandler(async (req, res) => {
      const tournament = await inPersonService.cancelTournament(req.params.tournamentId);
      res.json({ ok: true, tournament });
    }, logger)
  );
  app.get(
    "/in-person-tournaments/:tournamentId/admins",
    requireAdmin,
    asyncHandler(async (req, res) => {
      const admins = await inPersonService.listTournamentAdmins(req.params.tournamentId);
      res.json({ ok: true, admins });
    }, logger)
  );
  app.put(
    "/in-person-tournaments/:tournamentId/admins",
    requireAdmin,
    asyncHandler(async (req, res) => {
      const admins = await inPersonService.replaceTournamentAdmins(
        req.params.tournamentId,
        req.body?.admin_user_ids ?? req.body?.admins
      );
      res.json({ ok: true, admins });
    }, logger)
  );
  app.post(
    "/in-person-tournaments/:tournamentId/admins",
    requireAdmin,
    asyncHandler(async (req, res) => {
      const admin = await inPersonService.addTournamentAdmin(
        req.params.tournamentId,
        req.body?.user_id
      );
      res.status(201).json({ ok: true, admin });
    }, logger)
  );
  app.patch(
    "/in-person-tournaments/:tournamentId/admins/:userId",
    requireAdmin,
    asyncHandler(async (req, res) => {
      const requestedUserId = req.body?.user_id ?? req.params.userId;
      if (String(requestedUserId) !== String(req.params.userId)) {
        throw new InPersonError(400, "ADMIN_USER_ID_IMMUTABLE", "Tournament admin user_id cannot be changed");
      }
      const admin = await inPersonService.addTournamentAdmin(
        req.params.tournamentId,
        req.params.userId
      );
      res.json({ ok: true, admin });
    }, logger)
  );
  app.delete(
    "/in-person-tournaments/:tournamentId/admins/:userId",
    requireAdmin,
    asyncHandler(async (req, res) => {
      await inPersonService.removeTournamentAdmin(req.params.tournamentId, req.params.userId);
      res.json({ ok: true });
    }, logger)
  );

  logger?.info?.("[in-person] Admin CRUD routes enabled");
  return { enabled: true };
}
