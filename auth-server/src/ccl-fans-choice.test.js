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
  admin: { id: 10, admin: 1, email: "admin@example.com" },
  tournamentAdmin: { id: 11, admin: 0, email: "ta@example.com" },
  stranger: { id: 12, admin: 0, email: "x@example.com" },
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
    CREATE TABLE profiles (
      id TEXT PRIMARY KEY,
      bga_nickname TEXT,
      deleted_at TEXT
    );
    INSERT INTO profiles (id, bga_nickname, deleted_at) VALUES
      ('111', 'Kasia', NULL), ('222', 'twin', NULL), ('333', 'twin', NULL), ('444', 'gone', '2026-01-01');
    CREATE TABLE tournament_access_users (
      tournament_entity_type TEXT,
      tournament_id TEXT,
      user_id INTEGER,
      role TEXT
    );
    INSERT INTO tournament_access_users VALUES ('tournament', 'CCL-2027', 11, 'admin');
    -- Site accounts; bga_id links an account to its profile (a verified player).
    CREATE TABLE users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      bga_id TEXT
    );
  `);
  await ensureCclFansChoiceSchema(db);
  await ensureCclFansChoiceSchema(db);
  return db;
}

function createApi(db, { now = OPEN, lookupBgaPlayer = null } = {}) {
  const routes = new Map();
  const app = {};
  for (const method of ["get", "post", "patch"]) {
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
    lookupBgaPlayer,
    bgaLookupIntervalMs: 0,
    sleep: async () => {},
  });
  const call = async (route, {
    user = null, tournamentId = "CCL-2027", params = { tournamentId }, body = {}, query = {}, ip = "203.0.113.7",
  } = {}) => {
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
    await handler({ user, params, body, query, ip }, res);
    return result;
  };
  return { call, audit, clock };
}

const vote = (api, body, user = null, ip = undefined) => api.call("POST /public/ccl/:tournamentId/fans-choice/votes", { user, body, ip });
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
  const api = createApi(db, { now: new Date("2026-11-04T16:59:59Z") });

  assert.equal((await state(api)).body.voting.status, "upcoming");
  const early = await vote(api, { association_id: "POL", bga_nickname: "kasia" });
  assert.equal(early.status, 409);
  assert.equal(early.body.message, "Voting has not started yet");

  api.clock.now = new Date("2026-11-04T17:00:00Z");
  assert.equal((await vote(api, { association_id: "POL", bga_nickname: "first" })).status, 201);
  api.clock.now = new Date("2026-11-09T23:59:30Z");
  assert.equal((await vote(api, { association_id: "POL", bga_nickname: "last" })).status, 201);

  api.clock.now = new Date("2026-11-10T00:00:00Z");
  assert.equal((await state(api)).body.voting.status, "closed");
  assert.equal((await vote(api, { association_id: "POL", bga_nickname: "kasia" })).body.message, "Voting is closed");
  assert.deepEqual((await all(db, "SELECT bga_nickname FROM ccl_fans_choice_votes ORDER BY id")).map((row) => row.bga_nickname), ["first", "last"]);
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

const adminList = (api, user = USERS.admin) => api.call("GET /ccl/:tournamentId/fans-choice/votes", { user });
const review = (api, id, body, user = USERS.admin) => api.call("PATCH /ccl/fans-choice/votes/:id", { user, params: { id: String(id) }, body });

test("an older table gets the review columns", async (t) => {
  const db = new sqlite3.Database(":memory:");
  t.after(() => close(db));
  await exec(db, `
    CREATE TABLE ccl_fans_choice_votes (
      id INTEGER PRIMARY KEY AUTOINCREMENT, tournament_id TEXT NOT NULL, association_id TEXT NOT NULL,
      bga_nickname TEXT NOT NULL, player_id TEXT, user_id INTEGER, voter_association_id TEXT,
      status TEXT NOT NULL, ip_hash TEXT, created_at TEXT, updated_at TEXT
    );
  `);
  await ensureCclFansChoiceSchema(db);
  const columns = (await all(db, "PRAGMA table_info(ccl_fans_choice_votes)")).map((column) => column.name);
  for (const column of ["bga_player_id", "reject_reason", "reviewed_at", "reviewed_by", "bga_lookup_failed_at"]) assert.ok(columns.includes(column), column);
});

test("admins list guest votes with duplicates by player and by network", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);

  await vote(api, { association_id: "POL" }, USERS.verifiedPl, "10.0.0.1");
  const kasia = await vote(api, { association_id: "ESP", bga_nickname: "kasia" }, null, "10.0.0.2");
  const other = await vote(api, { association_id: "ESP", bga_nickname: "pepe" }, null, "10.0.0.2");
  const loner = await vote(api, { association_id: "POL", bga_nickname: "loner" }, null, "10.0.0.3");
  // Kasia's BGA id is also the id of a verified voter.
  await exec(db, `UPDATE ccl_fans_choice_votes SET player_id = '111' WHERE bga_nickname = 'Jan'`);
  await exec(db, `UPDATE ccl_fans_choice_votes SET bga_player_id = '111' WHERE bga_nickname = 'kasia'`);

  assert.equal((await adminList(api, USERS.stranger)).status, 403);
  assert.equal((await adminList(api, USERS.tournamentAdmin)).status, 200);
  const list = await adminList(api);
  assert.equal(list.status, 200, JSON.stringify(list.body));
  assert.deepEqual(
    list.body.votes.map((row) => [row.id, row.association_name, row.duplicate_count, row.ip_duplicate_count]).sort((a, b) => a[0] - b[0]),
    [
      [kasia.body.vote.id, "Spain", 1, 1],
      [other.body.vote.id, "Spain", 0, 1],
      [loner.body.vote.id, "Poland", 0, 0],
    ]
  );
  assert.equal(list.body.votes[0].association_flag, "pl.png");
});

test("admins review guest votes; rejecting needs a reason", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  const guest = await vote(api, { association_id: "POL", bga_nickname: "kasia" });
  const player = await vote(api, { association_id: "POL" }, USERS.verifiedUa);

  assert.equal((await review(api, guest.body.vote.id, { status: "confirmed" }, USERS.stranger)).status, 403);
  const noReason = await review(api, guest.body.vote.id, { status: "rejected", reject_reason: "  " });
  assert.equal(noReason.status, 400);
  assert.equal(noReason.body.message, "Enter the reason for rejecting the vote");
  assert.equal((await review(api, guest.body.vote.id, { status: "maybe" })).status, 400);
  assert.equal((await review(api, player.body.vote.id, { status: "rejected", reject_reason: "x" })).status, 400);
  assert.equal((await review(api, 999, { status: "confirmed" })).status, 404);

  const rejected = await review(api, guest.body.vote.id, { status: "rejected", reject_reason: " Not a  real account " });
  assert.equal(rejected.status, 200, JSON.stringify(rejected.body));
  assert.equal(rejected.body.vote.reject_reason, "Not a real account");
  assert.equal(rejected.body.vote.reviewed_by, "admin@example.com");
  assert.ok(rejected.body.vote.reviewed_at);
  assert.equal(api.audit.at(-1).event_type, "ccl_fans_choice_vote.reviewed");

  // Any other status clears the reason.
  const confirmed = await review(api, guest.body.vote.id, { status: "confirmed", reject_reason: "ignored" }, USERS.tournamentAdmin);
  assert.deepEqual([confirmed.body.vote.status, confirmed.body.vote.reject_reason, confirmed.body.vote.reviewed_by], ["confirmed", null, "ta@example.com"]);

  // A cancelled vote cannot come back while the nickname has voted again.
  await review(api, guest.body.vote.id, { status: "cancelled" });
  await vote(api, { association_id: "ESP", bga_nickname: "kasia" });
  const restore = await review(api, guest.body.vote.id, { status: "pending" });
  assert.equal(restore.status, 409);
});

test("BGA ids of guest votes come from profiles with the same nickname in any case", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  for (const nickname of ["Kasia", "kasia2", "twin", "gone", "Pair"]) await vote(api, { association_id: "POL", bga_nickname: nickname });
  // "kasia2" differs only in case; "kasia" loses to the exact "Kasia"; "pair" and "PAIR" are two profiles, neither exact.
  await exec(db, `
    INSERT INTO profiles (id, bga_nickname) VALUES ('555', 'KASIA2'), ('666', 'kasia'), ('777', 'pair'), ('888', 'PAIR')
  `);

  const fill = (user) => api.call("POST /ccl/:tournamentId/fans-choice/votes/fill-bga-ids", { user });
  assert.equal((await fill(USERS.stranger)).status, 403);
  const result = await fill(USERS.admin);
  assert.equal(result.status, 200, JSON.stringify(result.body));
  assert.equal(result.body.updated_count, 2);
  assert.deepEqual(
    await all(db, "SELECT bga_nickname, bga_player_id FROM ccl_fans_choice_votes ORDER BY id"),
    [
      { bga_nickname: "Kasia", bga_player_id: "111" },
      { bga_nickname: "kasia2", bga_player_id: "555" },
      { bga_nickname: "twin", bga_player_id: null },
      { bga_nickname: "gone", bga_player_id: null },
      { bga_nickname: "Pair", bga_player_id: null },
    ]
  );
  assert.equal((await fill(USERS.admin)).body.updated_count, 0);
});

// The BGA lookup runs in the background: waits until the admin list shows the run finished.
async function waitForBgaLookup(api) {
  for (let attempt = 0; attempt < 200; attempt += 1) {
    const lookup = (await adminList(api)).body.bga_lookup;
    if (!lookup?.running) return lookup;
    await new Promise((resolve) => setImmediate(resolve));
  }
  return assert.fail("BGA lookup did not finish");
}

const bgaCounts = ({ total, checked, found, not_found, error }) => ({ total, checked, found, not_found, error });

test("nicknames missing from the site profiles are looked up on BGA in the background", async (t) => {
  const db = await createDatabase(t);
  const calls = [];
  const bga = { Pandeyuk: { ok: true, player_id: "999", bga_nickname: "pandeyuk" }, ghost: { ok: false, not_found: true } };
  const api = createApi(db, {
    async lookupBgaPlayer(nickname) {
      calls.push(nickname);
      if (nickname === "later") throw new Error("BGA is down");
      return bga[nickname];
    },
  });
  for (const nickname of ["Kasia", "Pandeyuk", "ghost"]) await vote(api, { association_id: "POL", bga_nickname: nickname });
  const fill = () => api.call("POST /ccl/:tournamentId/fans-choice/votes/fill-bga-ids", { user: USERS.admin });

  // Kasia is in the site profiles: no BGA request for her. The click answers before BGA does.
  const first = await fill();
  assert.equal(first.status, 200, JSON.stringify(first.body));
  assert.equal(first.body.updated_count, 1);
  assert.equal(first.body.bga_lookup.running, true);
  assert.equal(first.body.bga_lookup.total, 2);
  // A second click during the run reports the same run instead of starting another one.
  assert.equal((await fill()).body.bga_lookup.running, true);
  assert.deepEqual(bgaCounts(await waitForBgaLookup(api)), { total: 2, checked: 2, found: 1, not_found: 1, error: null });
  assert.deepEqual(calls, ["Pandeyuk", "ghost"]);
  assert.deepEqual(
    await all(db, "SELECT bga_nickname, bga_player_id, bga_lookup_failed_at IS NOT NULL AS failed FROM ccl_fans_choice_votes ORDER BY id"),
    [
      { bga_nickname: "Kasia", bga_player_id: "111", failed: 0 },
      { bga_nickname: "Pandeyuk", bga_player_id: "999", failed: 0 },
      { bga_nickname: "ghost", bga_player_id: null, failed: 1 },
    ]
  );
  assert.ok((await adminList(api)).body.votes.find((row) => row.bga_nickname === "ghost").bga_lookup_failed_at);

  // Nothing left for BGA: no run.
  assert.equal((await fill()).body.bga_lookup, null);

  // A nickname BGA did not know is not looked up again; a BGA failure leaves the nickname for the next click.
  await vote(api, { association_id: "POL", bga_nickname: "later" });
  await fill();
  assert.deepEqual(
    bgaCounts(await waitForBgaLookup(api)),
    { total: 1, checked: 0, found: 0, not_found: 0, error: "BGA lookup failed. Try again later." }
  );
  assert.deepEqual(calls, ["Pandeyuk", "ghost", "later"]);
  assert.equal((await all(db, "SELECT bga_lookup_failed_at FROM ccl_fans_choice_votes WHERE bga_nickname = 'later'"))[0].bga_lookup_failed_at, null);
});

test("one BGA run checks every nickname without a limit per click", async (t) => {
  const db = await createDatabase(t);
  const calls = [];
  const api = createApi(db, {
    async lookupBgaPlayer(nickname) {
      calls.push(nickname);
      return { ok: false, not_found: true };
    },
  });
  for (let index = 1; index <= 12; index += 1) await vote(api, { association_id: "POL", bga_nickname: `player${index}` });
  await api.call("POST /ccl/:tournamentId/fans-choice/votes/fill-bga-ids", { user: USERS.admin });

  assert.deepEqual(bgaCounts(await waitForBgaLookup(api)), { total: 12, checked: 12, found: 0, not_found: 12, error: null });
  assert.equal(calls.length, 12);
});

test("admins see guest votes of players who have a verified site account", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  // Kasia (111) and twin (222) have verified accounts; the account of "gone" points to a deleted profile.
  await exec(db, "INSERT INTO users (bga_id) VALUES ('111'), ('222'), ('444')");
  for (const nickname of ["Kasia", "twin", "gone", "nobody"]) {
    await vote(api, { association_id: "POL", bga_nickname: nickname });
  }
  // Once found, the BGA id decides: this "twin" vote is the other twin profile (333), without an account.
  await exec(db, "UPDATE ccl_fans_choice_votes SET bga_player_id = '333' WHERE bga_nickname = 'twin'");

  const list = await adminList(api);
  assert.deepEqual(
    Object.fromEntries(list.body.votes.map((row) => [row.bga_nickname, row.has_verified_account])),
    { Kasia: true, twin: false, gone: false, nobody: false }
  );
  await exec(db, "UPDATE ccl_fans_choice_votes SET bga_player_id = '222' WHERE bga_nickname = 'twin'");
  assert.equal((await adminList(api)).body.votes.find((row) => row.bga_nickname === "twin").has_verified_account, true);
});

test("cancelled votes keep an optional reason; a replaced guest vote gets one", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  const guest = await vote(api, { association_id: "POL", bga_nickname: "kasia" });

  const noReason = await review(api, guest.body.vote.id, { status: "cancelled" });
  assert.equal(noReason.status, 200, JSON.stringify(noReason.body));
  assert.equal(noReason.body.vote.reject_reason, null);
  const withReason = await review(api, guest.body.vote.id, { status: "cancelled", reject_reason: "Poland got a 4th spot" });
  assert.equal(withReason.body.vote.reject_reason, "Poland got a 4th spot");

  // The verified player Jan already voted; a guest vote typed with his nickname is then replaced.
  await vote(api, { association_id: "POL" }, USERS.verifiedPl);
  await exec(db, "UPDATE ccl_fans_choice_votes SET bga_nickname = 'Jan old' WHERE player_id = 'jan'");
  await vote(api, { association_id: "ESP", bga_nickname: "Jan" });
  await vote(api, { association_id: "ESP" }, USERS.verifiedPl);
  assert.deepEqual(
    await all(db, "SELECT bga_nickname, player_id, status, reject_reason FROM ccl_fans_choice_votes WHERE id > ? ORDER BY id", [guest.body.vote.id]),
    [
      { bga_nickname: "Jan", player_id: "jan", status: "confirmed", reject_reason: null },
      { bga_nickname: "Jan", player_id: null, status: "cancelled", reject_reason: "Replaced by the vote of the verified player" },
    ]
  );
});

test("results count confirmed and pending votes and list every vote oldest first", async (t) => {
  const db = await createDatabase(t);
  const api = createApi(db);
  await vote(api, { association_id: "POL", bga_nickname: "kasia" });
  await vote(api, { association_id: "POL" }, USERS.verifiedUa);
  const rejected = await vote(api, { association_id: "POL", bga_nickname: "bot1" });
  const cancelled = await vote(api, { association_id: "ESP", bga_nickname: "pepe" });
  await review(api, rejected.body.vote.id, { status: "rejected", reject_reason: "Not a real account" });
  await review(api, cancelled.body.vote.id, { status: "cancelled", reject_reason: "Spain got a 4th spot" });
  await exec(db, `
    UPDATE ccl_fans_choice_votes SET created_at = '2026-11-04 10:00:00', updated_at = '2026-11-04 10:00:00' WHERE bga_nickname = 'kasia';
    UPDATE ccl_fans_choice_votes SET created_at = '2026-11-04 09:00:00', updated_at = '2026-11-04 11:00:00' WHERE bga_nickname = 'taras';
    UPDATE ccl_fans_choice_votes SET created_at = '2026-11-04 08:00:00', bga_player_id = '999' WHERE bga_nickname = 'bot1';
  `);

  const results = await api.call("GET /public/ccl/:tournamentId/fans-choice/results");
  assert.equal(results.status, 200, JSON.stringify(results.body));
  assert.deepEqual(results.body.totals, { confirmed: 1, pending: 1 });
  // Sorted by name; Ukraine has no votes and is not listed.
  assert.deepEqual(
    results.body.associations.map((row) => [row.id, row.confirmed, row.pending, row.votes.length]),
    [["POL", 1, 1, 3], ["ESP", 0, 0, 1]]
  );
  const poland = results.body.associations.find((row) => row.id === "POL");
  assert.deepEqual(
    poland.votes.map((row) => [row.bga_nickname, row.status, row.reason, row.bga_player_id, row.verified, row.voted_at]),
    [
      ["bot1", "rejected", "Not a real account", "999", false, "2026-11-04 08:00:00"],
      ["kasia", "pending", null, null, false, "2026-11-04 10:00:00"],
      ["taras", "confirmed", null, "taras", true, "2026-11-04 11:00:00"],
    ]
  );
  const spain = results.body.associations.find((row) => row.id === "ESP");
  assert.deepEqual(spain.votes.map((row) => [row.status, row.reason]), [["cancelled", "Spain got a 4th spot"]]);
  // Nothing private leaves the server.
  const fields = new Set(results.body.associations.flatMap((row) => row.votes.flatMap(Object.keys)));
  for (const field of ["ip_hash", "user_id", "reviewed_by", "voter_association_id"]) assert.ok(!fields.has(field), field);
  assert.equal((await api.call("GET /public/ccl/:tournamentId/fans-choice/results", { tournamentId: "CCL-2026" })).status, 404);
});
