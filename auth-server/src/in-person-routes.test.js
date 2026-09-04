import assert from "node:assert/strict";
import test from "node:test";
import { registerInPersonRoutes } from "./in-person/routes.js";

const silentLogger = { info() {} };

function createApp() {
  const routes = [];
  return {
    routes,
    get(path, ...handlers) {
      routes.push({ method: "GET", path, handlers });
    },
  };
}

test("does not expose in-person routes while the feature gate is disabled", () => {
  const app = createApp();
  const result = registerInPersonRoutes(app, {
    enabled: false,
    requireAdmin() {},
    requireInPersonTournamentAdmin() {},
    logger: silentLogger,
  });

  assert.deepEqual(result, { enabled: false });
  assert.deepEqual(app.routes, []);
});

test("registers protected global and tournament foundation routes when enabled", () => {
  const app = createApp();
  const requireAdmin = () => {};
  const requireInPersonTournamentAdmin = () => {};
  const result = registerInPersonRoutes(app, {
    enabled: "true",
    requireAdmin,
    requireInPersonTournamentAdmin,
    logger: silentLogger,
  });

  assert.deepEqual(result, { enabled: true });
  assert.equal(app.routes.length, 2);
  assert.equal(app.routes[0].method, "GET");
  assert.equal(app.routes[0].path, "/in-person-tournaments/_foundation");
  assert.equal(app.routes[0].handlers[0], requireAdmin);
  assert.equal(app.routes[1].method, "GET");
  assert.equal(app.routes[1].path, "/in-person-tournaments/:tournamentId/_foundation");
  assert.equal(app.routes[1].handlers[0], requireInPersonTournamentAdmin);
});
