import { createInPersonService } from "./service.js";
import { InPersonError } from "./validation.js";

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
  db,
  service,
  requireAdmin,
  requireAuthenticated,
  requireInPersonTournamentAdmin,
  logger = console,
} = {}) {
  if (typeof requireAdmin !== "function") {
    throw new Error("requireAdmin middleware is required for in-person routes");
  }
  if (typeof requireAuthenticated !== "function") {
    throw new Error("requireAuthenticated middleware is required for in-person routes");
  }
  if (typeof requireInPersonTournamentAdmin !== "function") {
    throw new Error(
      "requireInPersonTournamentAdmin middleware is required for in-person routes"
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
    "/in-person-tournaments/accessible",
    requireAuthenticated,
    asyncHandler(async (req, res) => {
      const tournaments = await inPersonService.listAccessibleTournaments(req.user);
      res.json({ ok: true, tournaments });
    }, logger)
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

  app.get(
    "/in-person-tournaments/:tournamentId/participant-cities",
    requireInPersonTournamentAdmin,
    asyncHandler(async (req, res) => {
      const cities = await inPersonService.listParticipantCities(req.inPersonTournamentId);
      res.json({ ok: true, cities });
    }, logger)
  );
  app.post(
    "/in-person-tournaments/:tournamentId/participant-cities",
    requireInPersonTournamentAdmin,
    asyncHandler(async (req, res) => {
      const city = await inPersonService.createParticipantCity(
        req.inPersonTournamentId,
        req.body || {}
      );
      res.status(201).json({ ok: true, city });
    }, logger)
  );
  app.get(
    "/in-person-tournaments/:tournamentId/participants",
    requireInPersonTournamentAdmin,
    asyncHandler(async (req, res) => {
      const overview = await inPersonService.getParticipantsOverview(req.inPersonTournamentId);
      res.json({ ok: true, ...overview });
    }, logger)
  );
  app.post(
    "/in-person-tournaments/:tournamentId/participants/late/preview",
    requireInPersonTournamentAdmin,
    asyncHandler(async (req, res) => {
      const preview = await inPersonService.previewLateParticipant(
        req.inPersonTournamentId,
        req.body || {}
      );
      res.json({ ok: true, preview });
    }, logger)
  );
  app.post(
    "/in-person-tournaments/:tournamentId/participants/late/confirm",
    requireInPersonTournamentAdmin,
    asyncHandler(async (req, res) => {
      const overview = await inPersonService.confirmLateParticipant(
        req.inPersonTournamentId,
        req.body || {},
        req.user
      );
      res.status(201).json({ ok: true, ...overview });
    }, logger)
  );
  app.post(
    "/in-person-tournaments/:tournamentId/participants",
    requireInPersonTournamentAdmin,
    asyncHandler(async (req, res) => {
      const participant = await inPersonService.createParticipant(
        req.inPersonTournamentId,
        req.body || {}
      );
      res.status(201).json({ ok: true, participant });
    }, logger)
  );
  app.patch(
    "/in-person-tournaments/:tournamentId/participants/:participantId",
    requireInPersonTournamentAdmin,
    asyncHandler(async (req, res) => {
      const participant = await inPersonService.updateParticipant(
        req.inPersonTournamentId,
        req.params.participantId,
        req.body || {}
      );
      res.json({ ok: true, participant });
    }, logger)
  );
  app.patch(
    "/in-person-tournaments/:tournamentId/participants/:participantId/status",
    requireInPersonTournamentAdmin,
    asyncHandler(async (req, res) => {
      const result = await inPersonService.setParticipantInactive(
        req.inPersonTournamentId,
        req.params.participantId,
        req.body || {}
      );
      res.json({ ok: true, ...result });
    }, logger)
  );
  app.delete(
    "/in-person-tournaments/:tournamentId/participants/:participantId",
    requireInPersonTournamentAdmin,
    asyncHandler(async (req, res) => {
      await inPersonService.deleteParticipant(
        req.inPersonTournamentId,
        req.params.participantId
      );
      res.json({ ok: true });
    }, logger)
  );
  app.post(
    "/in-person-tournaments/:tournamentId/start-check-in",
    requireInPersonTournamentAdmin,
    asyncHandler(async (req, res) => {
      const tournament = await inPersonService.startCheckIn(req.inPersonTournamentId);
      res.json({ ok: true, tournament });
    }, logger)
  );
  app.patch(
    "/in-person-tournaments/:tournamentId/participants/:participantId/check-in",
    requireInPersonTournamentAdmin,
    asyncHandler(async (req, res) => {
      const participant = await inPersonService.setParticipantCheckIn(
        req.inPersonTournamentId,
        req.params.participantId,
        req.body || {}
      );
      res.json({ ok: true, participant });
    }, logger)
  );
  app.get(
    "/in-person-tournaments/:tournamentId/check-in-readiness",
    requireInPersonTournamentAdmin,
    asyncHandler(async (req, res) => {
      const overview = await inPersonService.getParticipantsOverview(req.inPersonTournamentId);
      res.json({
        ok: true,
        tournament: overview.tournament,
        counters: overview.counters,
        readiness: overview.readiness,
      });
    }, logger)
  );

  app.get(
    "/in-person-tournaments/:tournamentId/swiss",
    requireInPersonTournamentAdmin,
    asyncHandler(async (req, res) => {
      const overview = await inPersonService.getSwissOverview(req.inPersonTournamentId);
      res.json({ ok: true, ...overview });
    }, logger)
  );
  app.post(
    "/in-person-tournaments/:tournamentId/swiss/rounds/preview",
    requireInPersonTournamentAdmin,
    asyncHandler(async (req, res) => {
      const preview = await inPersonService.previewSwissRound(
        req.inPersonTournamentId,
        req.body || {}
      );
      res.json({ ok: true, preview });
    }, logger)
  );
  app.post(
    "/in-person-tournaments/:tournamentId/swiss/rounds/confirm",
    requireInPersonTournamentAdmin,
    asyncHandler(async (req, res) => {
      const overview = await inPersonService.confirmSwissRound(
        req.inPersonTournamentId,
        req.body || {}
      );
      res.json({ ok: true, ...overview });
    }, logger)
  );
  app.post(
    "/in-person-tournaments/:tournamentId/swiss/rounds/:roundId/publish",
    requireInPersonTournamentAdmin,
    asyncHandler(async (req, res) => {
      const overview = await inPersonService.publishSwissRound(
        req.inPersonTournamentId,
        req.params.roundId
      );
      res.json({ ok: true, ...overview });
    }, logger)
  );
  app.get(
    "/in-person-tournaments/:tournamentId/swiss/rounds/:roundId/cancellation-preview",
    requireInPersonTournamentAdmin,
    asyncHandler(async (req, res) => {
      const preview = await inPersonService.previewSwissRoundCancellation(
        req.inPersonTournamentId,
        req.params.roundId
      );
      res.json({ ok: true, preview });
    }, logger)
  );
  app.post(
    "/in-person-tournaments/:tournamentId/swiss/rounds/:roundId/cancel",
    requireInPersonTournamentAdmin,
    asyncHandler(async (req, res) => {
      const overview = await inPersonService.cancelSwissRound(
        req.inPersonTournamentId,
        req.params.roundId,
        req.body || {},
        req.user
      );
      res.json({ ok: true, ...overview });
    }, logger)
  );
  app.put(
    "/in-person-tournaments/:tournamentId/swiss/matches/:matchId/result",
    requireInPersonTournamentAdmin,
    asyncHandler(async (req, res) => {
      const overview = await inPersonService.saveSwissMatchResult(
        req.inPersonTournamentId,
        req.params.matchId,
        req.body || {}
      );
      res.json({ ok: true, ...overview });
    }, logger)
  );
  app.post(
    "/in-person-tournaments/:tournamentId/swiss/rounds/:roundId/complete",
    requireInPersonTournamentAdmin,
    asyncHandler(async (req, res) => {
      const overview = await inPersonService.completeSwissRound(
        req.inPersonTournamentId,
        req.params.roundId
      );
      res.json({ ok: true, ...overview });
    }, logger)
  );
  app.post(
    "/in-person-tournaments/:tournamentId/swiss/rounds/:roundId/reopen",
    requireInPersonTournamentAdmin,
    asyncHandler(async (req, res) => {
      const overview = await inPersonService.reopenSwissRound(
        req.inPersonTournamentId,
        req.params.roundId
      );
      res.json({ ok: true, ...overview });
    }, logger)
  );

  app.get(
    "/in-person-tournaments/:tournamentId/playoff",
    requireInPersonTournamentAdmin,
    asyncHandler(async (req, res) => {
      const overview = await inPersonService.getPlayoffOverview(req.inPersonTournamentId);
      res.json({ ok: true, ...overview });
    }, logger)
  );
  app.post(
    "/in-person-tournaments/:tournamentId/playoff/preview",
    requireInPersonTournamentAdmin,
    asyncHandler(async (req, res) => {
      const preview = await inPersonService.previewPlayoff(
        req.inPersonTournamentId,
        req.body || {}
      );
      res.json({ ok: true, preview });
    }, logger)
  );
  app.post(
    "/in-person-tournaments/:tournamentId/playoff/confirm",
    requireInPersonTournamentAdmin,
    asyncHandler(async (req, res) => {
      const overview = await inPersonService.confirmPlayoff(
        req.inPersonTournamentId,
        req.body || {}
      );
      res.json({ ok: true, ...overview });
    }, logger)
  );
  app.post(
    "/in-person-tournaments/:tournamentId/playoff/rounds/:roundId/publish",
    requireInPersonTournamentAdmin,
    asyncHandler(async (req, res) => {
      const overview = await inPersonService.publishPlayoffRound(
        req.inPersonTournamentId,
        req.params.roundId
      );
      res.json({ ok: true, ...overview });
    }, logger)
  );
  app.patch(
    "/in-person-tournaments/:tournamentId/playoff/matches/:matchId/table",
    requireInPersonTournamentAdmin,
    asyncHandler(async (req, res) => {
      const overview = await inPersonService.setPlayoffMatchTable(
        req.inPersonTournamentId,
        req.params.matchId,
        req.body || {}
      );
      res.json({ ok: true, ...overview });
    }, logger)
  );
  app.post(
    "/in-person-tournaments/:tournamentId/playoff/matches/:matchId/streaming-table",
    requireInPersonTournamentAdmin,
    asyncHandler(async (req, res) => {
      const overview = await inPersonService.setPlayoffMatchTable(
        req.inPersonTournamentId,
        req.params.matchId,
        { table_number: 1 }
      );
      res.json({ ok: true, ...overview });
    }, logger)
  );
  app.put(
    "/in-person-tournaments/:tournamentId/playoff/matches/:matchId/result",
    requireInPersonTournamentAdmin,
    asyncHandler(async (req, res) => {
      const overview = await inPersonService.savePlayoffMatchResult(
        req.inPersonTournamentId,
        req.params.matchId,
        req.body || {}
      );
      res.json({ ok: true, ...overview });
    }, logger)
  );
  app.post(
    "/in-person-tournaments/:tournamentId/playoff/complete",
    requireInPersonTournamentAdmin,
    asyncHandler(async (req, res) => {
      const overview = await inPersonService.completePlayoff(req.inPersonTournamentId);
      res.json({ ok: true, ...overview });
    }, logger)
  );

  logger?.info?.("[in-person] Admin, participant, Swiss and playoff routes registered");
  return { registered: true };
}
