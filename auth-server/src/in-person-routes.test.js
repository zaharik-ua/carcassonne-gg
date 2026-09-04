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
    cancelTournament: async () => ({}),
    createCity: async () => ({}),
    createParticipantCity: async () => ({}),
    createParticipant: async () => ({}),
    createTournament: async () => ({}),
    deleteParticipant: async () => ({}),
    getParticipantsOverview: async () => ({
      tournament: {}, participants: [], counters: {}, readiness: {},
    }),
    getTournament: async () => ({}),
    listAccessibleTournaments: async () => [],
    listCities: async () => [],
    listParticipantCities: async () => [],
    listTournamentAdmins: async () => [],
    listTournaments: async () => [],
    publishTournament: async () => ({}),
    removeTournamentAdmin: async () => ({}),
    replaceTournamentAdmins: async () => [],
    restoreCity: async () => ({}),
    setParticipantCheckIn: async () => ({}),
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
  assert.equal(app.routes.length, 28);
  assert.equal(app.routes[0].method, "GET");
  assert.equal(app.routes[0].path, "/in-person-tournaments/_foundation");
  assert.equal(app.routes[0].handlers[0], requireAdmin);
  assert.equal(app.routes[1].method, "GET");
  assert.equal(app.routes[1].path, "/in-person-tournaments/:tournamentId/_foundation");
  assert.equal(app.routes[1].handlers[0], requireInPersonTournamentAdmin);
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
});
