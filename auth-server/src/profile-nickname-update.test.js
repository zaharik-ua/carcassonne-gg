import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import sqlite3 from "sqlite3";

const serverSource = readFileSync(new URL("./server.js", import.meta.url), "utf8");
const routeSource = serverSource.slice(
  serverSource.indexOf('app.patch("/profiles/:playerId",'),
  serverSource.indexOf('app.post("/profiles/:playerId/get-bga-data",')
);
const auditSource = serverSource.slice(
  serverSource.indexOf("function normalizeAuditValue("),
  serverSource.indexOf("function buildAuditCreationChanges(")
);
const auditFieldsSource = serverSource.slice(
  serverSource.indexOf("const PROFILE_AUDIT_FIELDS ="),
  serverSource.indexOf("const NEWS_EDITOR_AUDIT_FIELDS =")
);

const admin = { admin: 1, player_id: "admin" };
const owner = { admin: 0, player_id: "123456", association: "UKR" };
const captain = { admin: 0, player_id: "captain", team_captain: 1, association: "UKR" };

async function createContext(t) {
  const db = new sqlite3.Database(":memory:");
  t.after(() => new Promise((resolve, reject) => {
    db.close((error) => error ? reject(error) : resolve());
  }));
  await new Promise((resolve, reject) => db.exec(`
    CREATE TABLE profiles (
      id TEXT PRIMARY KEY, bga_nickname TEXT, name TEXT, status TEXT,
      master_title INTEGER, master_title_date TEXT, email TEXT,
      association TEXT, team_captain INTEGER, telegram TEXT, whatsapp TEXT,
      discord TEXT, instagram TEXT, contact_email TEXT,
      updated_by TEXT, updated_at TEXT, deleted_at TEXT
    );
    INSERT INTO profiles (id, bga_nickname, name, association, status)
    VALUES ('123456', 'OriginalNick', 'Original Name', 'UKR', 'Active'),
           ('654321', 'TakenNick', 'Other Player', 'UKR', 'Active');
  `, (error) => error ? reject(error) : resolve()));

  const auditEvents = [];
  let handler;
  // Load the actual route without starting the server or its background jobs.
  new Function(
    "app", "db", "syncUsersBgaIdForProfile", "syncCaptainNewsEditorForProfile",
    "getAuditActor", "logAuditEvent",
    `${auditFieldsSource}\n${auditSource}\n${routeSource}`
  )(
    { patch: (_path, callback) => { handler = callback; } },
    db,
    (_id, _email, _options, done) => done(null),
    async () => {},
    () => ({}),
    (entry, done) => { auditEvents.push(entry); done(); }
  );
  assert.equal(typeof handler, "function");

  const patch = (user, body) => new Promise((resolve) => {
    let status = 200;
    handler({ user, params: { playerId: "123456" }, body }, {
      status(code) { status = code; return this; },
      json(data) { resolve({ status, data }); },
    });
  });
  const getProfile = () => new Promise((resolve, reject) => {
    db.get("SELECT * FROM profiles WHERE id = '123456'", (error, row) => {
      if (error) reject(error);
      else resolve(row);
    });
  });
  return { patch, getProfile, auditEvents };
}

test("global admin can rename a player and the change is persisted and audited", async (t) => {
  const { patch, getProfile, auditEvents } = await createContext(t);
  const { status, data } = await patch(admin, {
    bga_nickname: "  NewNick  ", name: "Updated Name", email: "player@example.com",
  });
  assert.equal(status, 200);
  assert.equal(data.ok, true);
  assert.equal(data.profile.bga_nickname, "NewNick");
  const saved = await getProfile();
  assert.equal(saved.id, "123456");
  assert.equal(saved.bga_nickname, "NewNick");
  assert.equal(saved.name, "Updated Name");
  assert.equal(saved.email, "player@example.com");
  assert.equal(saved.association, "UKR");
  assert.equal(saved.updated_by, "admin");
  assert.equal(auditEvents.length, 1);
  assert.deepEqual(auditEvents[0].changes.bga_nickname, { old: "OriginalNick", new: "NewNick" });
});

test("owners, captains and other non-admins cannot rename a player", async (t) => {
  const { patch, getProfile, auditEvents } = await createContext(t);
  for (const user of [owner, captain, { admin: 0, player_id: "other" }]) {
    const result = await patch(user, { bga_nickname: "ForbiddenNick" });
    assert.equal(result.status, 403);
    assert.equal(result.data.ok, false);
    assert.equal((await getProfile()).bga_nickname, "OriginalNick");
  }
  assert.equal(auditEvents.length, 0);
});

test("omitting the nickname preserves it for admin, owner and captain edits", async (t) => {
  const { patch, getProfile } = await createContext(t);
  for (const user of [admin, owner, captain]) {
    const result = await patch(user, { name: `Updated by ${user.player_id}` });
    assert.equal(result.status, 200);
    const saved = await getProfile();
    assert.equal(saved.bga_nickname, "OriginalNick");
    assert.equal(saved.name, `Updated by ${user.player_id}`);
  }
});

test("empty and case-insensitively duplicate nicknames are rejected without changing the profile", async (t) => {
  const { patch, getProfile, auditEvents } = await createContext(t);
  for (const nickname of ["", "   ", null, "  takennick  "]) {
    const result = await patch(admin, { bga_nickname: nickname, name: "Must not save" });
    assert.equal(result.status, nickname === "  takennick  " ? 409 : 400);
    assert.equal(result.data.ok, false);
    const saved = await getProfile();
    assert.equal(saved.bga_nickname, "OriginalNick");
    assert.equal(saved.name, "Original Name");
  }
  assert.equal(auditEvents.length, 0);
});

test("an admin can keep the current nickname or change only its casing", async (t) => {
  const { patch, getProfile } = await createContext(t);
  for (const nickname of ["OriginalNick", "originalnick"]) {
    const result = await patch(admin, { bga_nickname: nickname });
    assert.equal(result.status, 200);
    assert.equal((await getProfile()).bga_nickname, nickname);
  }
});
