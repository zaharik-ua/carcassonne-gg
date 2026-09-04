import assert from "node:assert/strict";
import test from "node:test";
import { registerInPersonRoutes } from "./in-person/routes.js";

const silentLogger = { info() {} };

function createApp() {
  const routes = [];
  const app = { routes };
  ["get", "post", "put", "patch", "delete"].forEach((method) => {
    app[method] = (path, ...handlers) => {
      routes.push({ method: method.toUpperCase(), path, handlers });
    };
  });
  return app;
}

function createService() {
  return {
    addTournamentAdmin: async () => ({}),
    archiveCity: async () => ({}),
    cancelSwissRound: async () => ({}),
    cancelTournament: async () => ({}),
    completePlayoff: async () => ({}),
    completeSwissRound: async () => ({}),
    confirmLateParticipant: async () => ({}),
    confirmPlayoff: async () => ({}),
    confirmSwissRound: async () => ({}),
    createCity: async () => ({}),
    createParticipantCity: async () => ({}),
    createParticipant: async () => ({}),
    createTournament: async () => ({}),
    deleteParticipant: async () => ({}),
    getParticipantsOverview: async () => ({
      tournament: {}, participants: [], counters: {}, readiness: {},
    }),
    getPlayoffOverview: async () => ({}),
    getPublicTournamentAggregate: async () => ({
      revision: 1,
      tournament: { id: "ipt_public" },
    }),
    getSwissOverview: async () => ({}),
    getTournament: async () => ({}),
    listAccessibleTournaments: async () => [],
    listCities: async () => [],
    listParticipantCities: async () => [],
    listPublicTournaments: async () => [],
    listTournamentAdmins: async () => [],
    listTournaments: async () => [],
    previewLateParticipant: async () => ({}),
    previewPlayoff: async () => ({}),
    previewSwissRound: async () => ({}),
    previewSwissRoundCancellation: async () => ({}),
    publishTournament: async () => ({}),
    publishPlayoffRound: async () => ({}),
    publishSwissRound: async () => ({}),
    reopenSwissRound: async () => ({}),
    removeTournamentAdmin: async () => ({}),
    replaceTournamentAdmins: async () => [],
    restoreCity: async () => ({}),
    saveSwissMatchResult: async () => ({}),
    savePlayoffMatchResult: async () => ({}),
    setPlayoffMatchTable: async () => ({}),
    setParticipantCheckIn: async () => ({}),
    setParticipantInactive: async () => ({}),
    startCheckIn: async () => ({}),
    updateCity: async () => ({}),
    updateParticipant: async () => ({}),
    updateTournament: async () => ({}),
  };
}

test("always registers protected global and tournament routes", () => {
  const app = createApp();
  const requireAdmin = () => {};
  const requireAuthenticated = () => {};
  const requireInPersonTournamentAdmin = () => {};
  const result = registerInPersonRoutes(app, {
    service: createService(),
    requireAdmin,
    requireAuthenticated,
    requireInPersonTournamentAdmin,
    logger: silentLogger,
  });

  assert.deepEqual(result, { registered: true });
  assert.equal(app.routes.length, 50);
  const globalFoundation = app.routes.find((route) => (
    route.path === "/in-person-tournaments/_foundation"
  ));
  assert.equal(globalFoundation.method, "GET");
  assert.equal(globalFoundation.handlers[0], requireAdmin);
  const tournamentFoundation = app.routes.find((route) => (
    route.path === "/in-person-tournaments/:tournamentId/_foundation"
  ));
  assert.equal(tournamentFoundation.method, "GET");
  assert.equal(tournamentFoundation.handlers[0], requireInPersonTournamentAdmin);
  const publicListRoute = app.routes.find((route) => (
    route.path === "/public/in-person-tournaments"
  ));
  const publicAggregateRoute = app.routes.find((route) => (
    route.path === "/public/in-person-tournaments/:identifier"
  ));
  assert.ok(publicListRoute);
  assert.ok(publicAggregateRoute);
  assert.notEqual(publicListRoute.handlers[0], requireAdmin);
  assert.notEqual(publicAggregateRoute.handlers[0], requireInPersonTournamentAdmin);
  const accessibleRoute = app.routes.find((route) => route.path === "/in-person-tournaments/accessible");
  assert.equal(accessibleRoute.handlers[0], requireAuthenticated);
  app.routes.filter((route) => (
    route.path.startsWith("/cities")
    || (
      route.path.startsWith("/in-person-tournaments")
      && !route.path.includes("/participants")
      && !route.path.endsWith("/participant-cities")
      && !route.path.endsWith("/start-check-in")
      && !route.path.endsWith("/check-in-readiness")
      && !route.path.includes("/swiss")
      && !route.path.includes("/playoff")
      && route.path !== "/in-person-tournaments/accessible"
      && !route.path.endsWith("/_foundation")
    )
  )).forEach((route) => {
    assert.equal(route.handlers[0], requireAdmin, `${route.method} ${route.path} must require global admin`);
  });
  app.routes.filter((route) => (
    route.path.includes("/participants")
    || route.path.endsWith("/participant-cities")
    || route.path.endsWith("/start-check-in")
    || route.path.endsWith("/check-in-readiness")
    || route.path.includes("/swiss")
    || route.path.includes("/playoff")
  )).forEach((route) => {
    assert.equal(
      route.handlers[0],
      requireInPersonTournamentAdmin,
      `${route.method} ${route.path} must require tournament admin`
    );
  });
  assert.ok(app.routes.some((route) => route.method === "GET" && route.path === "/cities"));
  assert.ok(app.routes.some((route) => route.method === "POST" && route.path === "/in-person-tournaments"));
  assert.ok(app.routes.some((route) => route.method === "POST" && route.path.endsWith("/publish")));
  assert.ok(app.routes.some((route) => route.method === "PUT" && route.path.endsWith("/admins")));
  assert.ok(app.routes.some((route) => (
    route.method === "POST" && route.path.endsWith("/participant-cities")
  )));
  assert.ok(app.routes.some((route) => route.method === "PATCH" && route.path.endsWith("/check-in")));
  assert.ok(app.routes.some((route) => (
    route.method === "POST" && route.path.endsWith("/swiss/rounds/preview")
  )));
  assert.ok(app.routes.some((route) => (
    route.method === "PUT" && route.path.endsWith("/result")
  )));
  assert.ok(app.routes.some((route) => (
    route.method === "POST" && route.path.endsWith("/complete")
  )));
  assert.ok(app.routes.some((route) => (
    route.method === "POST" && route.path.endsWith("/reopen")
  )));
  assert.ok(app.routes.some((route) => (
    route.method === "GET" && route.path.endsWith("/cancellation-preview")
  )));
  assert.ok(app.routes.some((route) => (
    route.method === "POST" && route.path.endsWith("/cancel") && route.path.includes("/swiss/")
  )));
  assert.ok(app.routes.some((route) => (
    route.method === "POST" && route.path.endsWith("/participants/late/preview")
  )));
  assert.ok(app.routes.some((route) => (
    route.method === "POST" && route.path.endsWith("/participants/late/confirm")
  )));
  assert.ok(app.routes.some((route) => (
    route.method === "PATCH" && route.path.endsWith("/status")
  )));
  assert.ok(app.routes.some((route) => (
    route.method === "POST" && route.path.endsWith("/playoff/preview")
  )));
  assert.ok(app.routes.some((route) => (
    route.method === "POST" && route.path.endsWith("/playoff/confirm")
  )));
  assert.ok(app.routes.some((route) => (
    route.method === "POST" && route.path.endsWith("/streaming-table")
  )));
  assert.ok(app.routes.some((route) => (
    route.method === "PUT" && route.path.includes("/playoff/matches/") && route.path.endsWith("/result")
  )));
});
