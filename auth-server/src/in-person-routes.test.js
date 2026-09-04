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
    createTournament: async () => ({}),
    getTournament: async () => ({}),
    listCities: async () => [],
    listTournamentAdmins: async () => [],
    listTournaments: async () => [],
    publishTournament: async () => ({}),
    removeTournamentAdmin: async () => ({}),
    replaceTournamentAdmins: async () => [],
    restoreCity: async () => ({}),
    updateCity: async () => ({}),
    updateTournament: async () => ({}),
  };
}

test("always registers protected global and tournament routes", () => {
  const app = createApp();
  const requireAdmin = () => {};
  const requireInPersonTournamentAdmin = () => {};
  const result = registerInPersonRoutes(app, {
    service: createService(),
    requireAdmin,
    requireInPersonTournamentAdmin,
    logger: silentLogger,
  });

  assert.deepEqual(result, { registered: true });
  assert.equal(app.routes.length, 18);
  assert.equal(app.routes[0].method, "GET");
  assert.equal(app.routes[0].path, "/in-person-tournaments/_foundation");
  assert.equal(app.routes[0].handlers[0], requireAdmin);
  assert.equal(app.routes[1].method, "GET");
  assert.equal(app.routes[1].path, "/in-person-tournaments/:tournamentId/_foundation");
  assert.equal(app.routes[1].handlers[0], requireInPersonTournamentAdmin);
  app.routes.slice(2).forEach((route) => {
    assert.equal(route.handlers[0], requireAdmin, `${route.method} ${route.path} must require global admin`);
  });
  assert.ok(app.routes.some((route) => route.method === "GET" && route.path === "/cities"));
  assert.ok(app.routes.some((route) => route.method === "POST" && route.path === "/in-person-tournaments"));
  assert.ok(app.routes.some((route) => route.method === "POST" && route.path.endsWith("/publish")));
  assert.ok(app.routes.some((route) => route.method === "PUT" && route.path.endsWith("/admins")));
});
