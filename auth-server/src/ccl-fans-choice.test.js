import assert from "node:assert/strict";
import test from "node:test";
import sqlite3 from "sqlite3";

import { ensureCclFansChoiceSchema, registerCclFansChoiceRoutes } from "./ccl-fans-choice.js";

function exec(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (error) => (error ? reject(error) : resolve()));
  });
}

function all(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows || [])));
  });
}

function close(db) {
  return new Promise((resolve) => db.close(resolve));
}

const USERS = {
  verifiedPl: { id: 1, player_id: "jan", bga_nickname: "Jan", association: "Poland" },
  verifiedUa: { id: 2, player_id: "taras", bga_nickname: "taras", association: "UKR" },
  unverified: { id: 3, player_id: null },
};

const OPEN = new Date("2026-11-05T12:00:00Z");

async function createDatabase(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => close(db));
  await exec(db, `
    PRAGMA foreign_keys = ON;
    CREATE TABLE tournaments (
      id TEXT PRIMARY KEY COLLATE NOCASE,
      name TEXT NOT NULL,
      type TEXT,
      category TEXT
    );
    CREATE TABLE associations (
      association_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
      code TEXT UNIQUE COLLATE NOCASE,
      name TEXT NOT NULL,
      flag TEXT
    );
    INSERT INTO tournaments VALUES
      ('CCL-2027', 'Carcassonne Champions League 2027', 'Individuals', 'CCL'),
      ('CCL-2026', 'Carcassonne Champions League 2026', 'Individuals', 'CCL'),
      ('ETCOC-2026', 'ETCOC 2026', 'Teams', 'ETCOC');
    INSERT INTO associations (code, name, flag) VALUES
      ('POL', 'Poland', 'pl.png'), ('UKR', 'Ukraine', 'ua.png'), ('ESP', 'Spain', 'es.png');
  `);
  await ensureCclFansChoiceSchema(db);
  await ensureCclFansChoiceSchema(db);
  return db;
}

function createApi(db, { now = OPEN } = {}) {
  const routes = new Map();
  const app = {};
  for (const method of ["get", "post"]) {
    app[method] = (routePath, ...handlers) => routes.set(`${method.toUpperCase()} ${routePath}`, handlers.at(-1));
  }
  const audit = [];
  const clock = { now };
  registerCclFansChoiceRoutes(app, {
    db,
    getAuditActor: (user) => ({ actor_user_id: user?.id ?? null }),
    logAuditEvent: (entry, done) => {
      audit.push(entry);
      done();
    },
    logger: { error() {} },
    now: () => clock.now,
  });
  const call = async (route, { user = null, tournamentId = "CCL-2027", body = {}, query = {} } = {}) => {
    const handler = routes.get(route);
    assert.ok(handler, `route ${route} is not registered`);
    const result = { status: 200, body: null };
    const res = {
      status(code) {
        result.status = code;
        return this;
      },
      json(value) {
        result.body = value;
        return this;
      },
    };
    await handler({ user, params: { tournamentId }, body, query, ip: "203.0.113.7" }, res);
    return result;
  };
  return { call, audit, clock };
}

const vote = (api, body, user = null) => api.call("POST /public/ccl/:tournamentId/fans-choice/votes", { user, body });
const state = (api, options = {}) => api.call("GET /public/ccl/:tournamentId/fans-choice", options);

test("state lists associations with eligibility and describes the viewer", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);

  const anonymous = await state(api);
  assert.equal(anonymous.status, 200);
  assert.equal(anonymous.body.voting.status, "open");
  assert.deepEqual(
    anonymous.body.associations.map((row) => [row.id, row.eligible, row.reason]),
    [["POL", true, null], ["ESP", true, null], ["UKR", false, "Already has 4–5 spots in CCL 2027"]]
  );
  assert.deepEqual(anonymous.body.viewer, {
    authenticated: false, verified: false, player_id: null, bga_nickname: null, association_id: null, vote: null,
  });

  const verified = await state(api, { user: USERS.verifiedPl });
  assert.equal(verified.body.viewer.verified, true);
  assert.equal(verified.body.viewer.association_id, "POL");

  const unverified = await state(api, { user: USERS.unverified });
  assert.equal(unverified.body.viewer.verified, false);
  assert.equal(unverified.body.viewer.association_id, null);

  assert.equal((await state(api, { tournamentId: "CCL-2026" })).status, 404);
  assert.equal((await state(api, { tournamentId: "ETCOC-2026" })).status, 404);
});

test("voting is accepted only inside the window", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db, { now: new Date("2026-11-03T23:59:59Z") });

  assert.equal((await state(api)).body.voting.status, "upcoming");
  const early = await vote(api, { association_id: "POL", bga_nickname: "kasia" });
  assert.equal(early.status, 409);
  assert.equal(early.body.message, "Voting has not started yet");

  api.clock.now = new Date("2026-11-09T00:00:00Z");
  assert.equal((await state(api)).body.voting.status, "closed");
  assert.equal((await vote(api, { association_id: "POL", bga_nickname: "kasia" })).body.message, "Voting is closed");
  assert.deepEqual(await all(db, "SELECT id FROM ccl_fans_choice_votes"), []);
});

test("excluded and unknown associations cannot be voted for", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);

  const excluded = await vote(api, { association_id: "ukr" }, USERS.verifiedUa);
  assert.equal(excluded.status, 400);
  assert.equal(excluded.body.message, "Ukraine: Already has 4–5 spots in CCL 2027");
  assert.equal((await vote(api, { association_id: "XXX" }, USERS.verifiedUa)).status, 400);
  assert.equal((await vote(api, {}, USERS.verifiedUa)).status, 400);
});

test("a verified player's vote is confirmed and a new vote replaces it", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);

  const first = await vote(api, { association_id: "pol", bga_nickname: "ignored" }, USERS.verifiedUa);
  assert.equal(first.status, 201, JSON.stringify(first.body));
  assert.equal(first.body.vote.status, "confirmed");
  assert.equal(first.body.vote.association_name, "Poland");

  const second = await vote(api, { association_id: "ESP" }, USERS.verifiedUa);
  assert.equal(second.status, 200);
  assert.equal(second.body.vote.id, first.body.vote.id);

  assert.deepEqual(
    await all(db, `
      SELECT tournament_id, association_id, bga_nickname, player_id, user_id, voter_association_id, status
      FROM ccl_fans_choice_votes
    `),
    [{
      tournament_id: "CCL-2027", association_id: "ESP", bga_nickname: "taras", player_id: "taras",
      user_id: 2, voter_association_id: "UKR", status: "confirmed",
    }]
  );
  assert.deepEqual(api.audit.map((entry) => entry.event_type), ["ccl_fans_choice_vote.created", "ccl_fans_choice_vote.updated"]);
  assert.equal((await state(api, { user: USERS.verifiedUa })).body.viewer.vote.association_id, "ESP");
});

test("guest votes wait for review and each nickname or account votes once", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);

  assert.equal((await vote(api, { association_id: "POL" })).body.message, "Enter your BGA nickname");

  const guest = await vote(api, { association_id: "POL", bga_nickname: "  Kasia  K " });
  assert.equal(guest.status, 201, JSON.stringify(guest.body));
  assert.equal(guest.body.vote.status, "pending");
  assert.equal(guest.body.vote.bga_nickname, "Kasia K");

  const again = await vote(api, { association_id: "ESP", bga_nickname: "kasia k" });
  assert.equal(again.status, 409);
  assert.equal(again.body.message, "This BGA nickname has already voted");

  const signedIn = await vote(api, { association_id: "ESP", bga_nickname: "pepe" }, USERS.unverified);
  assert.equal(signedIn.status, 201);
  const secondNickname = await vote(api, { association_id: "ESP", bga_nickname: "pepe2" }, USERS.unverified);
  assert.equal(secondNickname.body.message, "You have already voted");
  assert.equal((await state(api, { user: USERS.unverified })).body.viewer.vote.bga_nickname, "pepe");

  const remembered = await state(api, { query: { vote_id: String(guest.body.vote.id) } });
  assert.equal(remembered.body.remembered_vote.status, "pending");

  const [row] = await all(db, "SELECT ip_hash, player_id FROM ccl_fans_choice_votes WHERE id = ?", [guest.body.vote.id]);
  assert.match(row.ip_hash, /^[0-9a-f]{32}$/);
  assert.equal(row.player_id, null);
});

test("a verified vote takes over a guest vote with the same nickname", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);

  const guest = await vote(api, { association_id: "ESP", bga_nickname: "JAN" });
  const verified = await vote(api, { association_id: "POL" }, USERS.verifiedPl);
  assert.equal(verified.status, 200);
  assert.equal(verified.body.vote.id, guest.body.vote.id);
  assert.deepEqual(
    await all(db, "SELECT association_id, bga_nickname, player_id, status FROM ccl_fans_choice_votes"),
    [{ association_id: "POL", bga_nickname: "Jan", player_id: "jan", status: "confirmed" }]
  );
});

test("a cancelled vote lets the voter vote again", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);

  const guest = await vote(api, { association_id: "POL", bga_nickname: "kasia" });
  const player = await vote(api, { association_id: "POL" }, USERS.verifiedUa);
  await exec(db, "UPDATE ccl_fans_choice_votes SET status = 'cancelled' WHERE association_id = 'POL'");

  const viewerVote = (await state(api, { user: USERS.verifiedUa })).body.viewer.vote;
  assert.deepEqual([viewerVote.id, viewerVote.status], [player.body.vote.id, "cancelled"]);

  const guestAgain = await vote(api, { association_id: "ESP", bga_nickname: "kasia" });
  assert.equal(guestAgain.status, 201);
  assert.notEqual(guestAgain.body.vote.id, guest.body.vote.id);
  const playerAgain = await vote(api, { association_id: "ESP" }, USERS.verifiedUa);
  assert.equal(playerAgain.status, 201);
  assert.equal((await state(api, { user: USERS.verifiedUa })).body.viewer.vote.status, "confirmed");
});
