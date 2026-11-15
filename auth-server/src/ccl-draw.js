// CCL Qualifiers draw (Q1, Q2, Q3): places the players of a path into its groups.
// Rules (design doc, "Жеребкування Q1, Q2, Q3"):
// - Seeded take odd positions (1, 3, 5, 7), Unseeded even ones: every Round 1 match is Seeded vs Unseeded.
// - Players of one association go to different groups; when an association has more players than
//   there are groups, at most ceil(n / groups) per group and never in the same Round 1 match.
// - Missing Unseeded players become Byes, at most ceil(byes / groups) per group; Byes are placed first.
// - Placement order: associations with more players in the path first (both pots together), then
//   single representatives; random inside. A dead end restarts the placement (up to 1000 attempts).
// - Randomness comes from a seeded generator, so a draw can be replayed from its seed.

import { randomInt } from "node:crypto";

import { loadAssociationsById, loadCclTournament, loadProfilesById } from "./ccl-routes.js";
import { syncCclBracketsInTransaction } from "./ccl-bracket.js";
import { loadCclPathStage } from "./ccl-structure.js";
import { hasTournamentAdminAccess } from "./tournament-admin-access.js";

export const DRAW_ATTEMPTS = 1000;
const SEEDED = "Seeded";
const UNSEEDED = "Unseeded";

const text = (value) => String(value ?? "").trim();
const idKey = (value) => text(value).toUpperCase();

// mulberry32: small, fast, good enough for a draw; the same seed gives the same sequence.
export function createRandom(seed) {
  let state = Number(seed) >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let value = state;
    value = Math.imul(value ^ (value >>> 15), value | 1);
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
    return ((value ^ (value >>> 14)) >>> 0) / 4294967296;
  };
}

export function newDrawSeed() {
  return randomInt(1, 2 ** 31);
}

const pick = (random, list) => list[Math.floor(random() * list.length)];

function shuffle(random, list) {
  const copy = [...list];
  for (let index = copy.length - 1; index > 0; index -= 1) {
    const other = Math.floor(random() * (index + 1));
    [copy[index], copy[other]] = [copy[other], copy[index]];
  }
  return copy;
}

const partnerOf = (position) => (position % 2 === 1 ? position + 1 : position - 1);

// Order in which players are placed: big associations first, singles last, random inside.
function placementOrder(random, players) {
  const byAssociation = new Map();
  for (const player of shuffle(random, players)) {
    const key = idKey(player.association_id) || `#${player.slot_id}`;
    if (!byAssociation.has(key)) byAssociation.set(key, []);
    byAssociation.get(key).push(player);
  }
  // shuffle() above already randomised the order of associations with the same size; sort is stable.
  return [...byAssociation.values()].sort((a, b) => b.length - a.length).flat();
}

function validate({ entries, groups, seededPerGroup }) {
  const capacity = seededPerGroup * groups.length;
  const seeded = entries.filter((entry) => entry.seed === SEEDED);
  const unseeded = entries.filter((entry) => entry.seed === UNSEEDED);
  if (!groups.length) return { error: "The stage has no groups; generate the structure first" };
  if (seeded.length !== capacity) return { error: `The draw needs exactly ${capacity} Seeded players; there are ${seeded.length}` };
  if (unseeded.length > capacity) return { error: `The draw takes at most ${capacity} Unseeded players; there are ${unseeded.length}` };
  if (seeded.length + unseeded.length !== entries.length) return { error: "Every player needs a Seeded or Unseeded pot" };
  return { byes: capacity - unseeded.length };
}

function attemptPlacement({ random, players, groups, seededPerGroup, byes, strict }) {
  const groupSize = seededPerGroup * 2;
  const occupant = new Map(groups.map((group) => [group.id, new Map()]));
  const associationCounts = new Map(groups.map((group) => [group.id, new Map()]));
  const associationTotals = new Map();
  for (const player of players) {
    const key = idKey(player.association_id);
    if (key) associationTotals.set(key, (associationTotals.get(key) || 0) + 1);
  }
  const cap = (key) => Math.max(1, Math.ceil((associationTotals.get(key) || 0) / groups.length));
  const freePositions = (groupId, pot) => {
    const taken = occupant.get(groupId);
    const positions = [];
    for (let position = pot === SEEDED ? 1 : 2; position <= groupSize; position += 2) {
      if (!taken.has(position)) positions.push(position);
    }
    return positions;
  };
  const placements = [];
  const conflicts = [];

  // Byes first: they spread over the groups like an association of their own.
  const byeCap = Math.max(1, Math.ceil(byes / groups.length));
  const byesInGroup = new Map(groups.map((group) => [group.id, 0]));
  for (let index = 0; index < byes; index += 1) {
    const candidates = groups
      .filter((group) => byesInGroup.get(group.id) < byeCap)
      .flatMap((group) => freePositions(group.id, UNSEEDED).map((position) => ({ group, position })));
    if (!candidates.length) return null;
    const { group, position } = pick(random, candidates);
    occupant.get(group.id).set(position, { bye: true });
    byesInGroup.set(group.id, byesInGroup.get(group.id) + 1);
    placements.push({ group_id: group.id, group_key: group.group_key, draw_position: position, bye: true });
  }

  for (const player of placementOrder(random, players)) {
    const key = idKey(player.association_id);
    const allowed = ({ group, position }) => {
      if (!key) return true;
      if ((associationCounts.get(group.id).get(key) || 0) >= cap(key)) return false;
      const partner = occupant.get(group.id).get(partnerOf(position));
      return !partner || partner.bye || idKey(partner.association_id) !== key;
    };
    const free = groups.flatMap((group) => freePositions(group.id, player.seed).map((position) => ({ group, position })));
    let candidates = free.filter(allowed);
    if (!candidates.length) {
      if (strict || !free.length) return null;
      candidates = free;
      conflicts.push({ slot_id: player.slot_id, association_id: player.association_id || null });
    }
    const { group, position } = pick(random, candidates);
    occupant.get(group.id).set(position, player);
    if (key) associationCounts.get(group.id).set(key, (associationCounts.get(group.id).get(key) || 0) + 1);
    placements.push({ group_id: group.id, group_key: group.group_key, draw_position: position, slot_id: player.slot_id, bye: false });
    if (conflicts.at(-1)?.slot_id === player.slot_id) conflicts.at(-1).group_key = group.group_key;
  }
  return { placements, conflicts };
}

// Pure. entries: [{ slot_id, player_id, association_id, seed }]; groups: [{ id, group_key }] in order.
// Returns { seed, attempts, placements (in draw order, Byes first), conflicts } or { error }.
export function runCclDraw({ entries, groups, seededPerGroup = 4, seed }) {
  const checked = validate({ entries, groups, seededPerGroup });
  if (checked.error) return { error: checked.error };
  const random = createRandom(seed);
  const options = { random, players: entries, groups, seededPerGroup, byes: checked.byes };
  for (let attempt = 1; attempt <= DRAW_ATTEMPTS; attempt += 1) {
    const result = attemptPlacement({ ...options, strict: true });
    if (result) return finish(result, seed, attempt);
  }
  // The rules cannot all hold for this field: place anyway and report who breaks them.
  const result = attemptPlacement({ ...options, strict: false });
  if (!result) return { error: "The draw could not place every player" };
  return finish(result, seed, DRAW_ATTEMPTS + 1);
}

function finish({ placements, conflicts }, seed, attempts) {
  return {
    seed,
    attempts,
    byes: placements.filter((placement) => placement.bye).length,
    placements: placements.map((placement, index) => ({ ...placement, draw_order: index + 1 })),
    conflicts,
  };
}

// ---------------------------------------------------------------------------------------------
// Database and routes

function dbAll(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows || [])));
  });
}

function dbRun(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function onRun(error) {
      if (error) reject(error);
      else resolve(this);
    });
  });
}

async function inTransaction(db, work) {
  await dbRun(db, "BEGIN IMMEDIATE TRANSACTION");
  try {
    const result = await work();
    await dbRun(db, "COMMIT");
    return result;
  } catch (error) {
    await dbRun(db, "ROLLBACK").catch(() => {});
    throw error;
  }
}

function requestError(status, message, data = undefined) {
  return Object.assign(new Error(message), { status, data });
}

async function loadGroups(db, stageId) {
  return dbAll(
    db,
    "SELECT id, group_key, name FROM tournament_groups WHERE upper(trim(stage_id)) = upper(trim(?)) ORDER BY sort_order, group_key",
    [stageId]
  );
}

// Stage entries of the path with their slot's association.
async function loadDrawEntries(db, tournamentId, stageKey) {
  return dbAll(
    db,
    `
      SELECT e.slot_id, e.player_id, e.elo, e.seed, e.group_id, e.draw_position, e.draw_order, e.drawn_at,
             tp.association_id
      FROM ccl_stage_entries e
      JOIN tournament_players tp ON upper(trim(tp.id)) = upper(trim(e.slot_id))
      WHERE upper(trim(e.tournament_id)) = upper(trim(?)) AND e.stage = ?
      ORDER BY e.slot_id
    `,
    [tournamentId, stageKey]
  );
}

function drawBlockers(entries) {
  const blockers = [];
  for (const entry of entries) {
    if (!text(entry.player_id)) blockers.push({ slot_id: entry.slot_id, reason: "player_missing" });
    else if (![SEEDED, UNSEEDED].includes(entry.seed)) blockers.push({ slot_id: entry.slot_id, reason: "seed_missing" });
  }
  return blockers;
}

async function loadDrawInput(db, tournament, stageKey) {
  const stage = await loadCclPathStage(db, tournament.id, stageKey);
  if (!stage) throw requestError(404, "Stage not found; generate the structure first");
  const groups = await loadGroups(db, stage.id);
  const entries = await loadDrawEntries(db, tournament.id, stage.stage_key);
  const settings = JSON.parse(stage.settings_json || "{}");
  return { stage, groups, entries, seededPerGroup: Number(settings.seeded_per_group) || 4 };
}

function computeDraw(input, seed) {
  const blockers = drawBlockers(input.entries);
  if (blockers.length) throw requestError(409, "Some players are not ready for the draw", { blockers });
  const result = runCclDraw({ entries: input.entries, groups: input.groups, seededPerGroup: input.seededPerGroup, seed });
  if (result.error) throw requestError(409, result.error);
  return result;
}

// Draw state for the Draw tab: the field, and the saved result once the draw is made.
async function describeDraw(db, tournament, input, result = null) {
  const profiles = await loadProfilesById(db, input.entries.map((entry) => entry.player_id));
  const associations = await loadAssociationsById(db);
  const bySlot = new Map(input.entries.map((entry) => [idKey(entry.slot_id), entry]));
  const player = (entry) => {
    const profile = profiles.get(text(entry.player_id));
    const association = associations.get(idKey(entry.association_id));
    return {
      slot_id: entry.slot_id,
      player_id: entry.player_id || null,
      bga_nickname: profile?.bga_nickname || null,
      avatar: profile?.avatar || null,
      association_id: entry.association_id || null,
      association_name: association?.name || null,
      association_flag: association?.flag || null,
      elo: entry.elo ?? null,
      seed: entry.seed || null,
    };
  };
  const drawn = text(input.stage.status) !== "structure";
  // A saved draw keeps no row for a Bye: Byes are the empty positions, shown first in group order.
  let placements = result?.placements || null;
  if (!placements && drawn) {
    const players = input.entries.filter((entry) => entry.group_id).sort((a, b) => a.draw_order - b.draw_order);
    const groupSize = input.seededPerGroup * 2;
    const byes = input.groups.flatMap((group) => {
      const taken = new Set(players.filter((entry) => idKey(entry.group_id) === idKey(group.id)).map((entry) => entry.draw_position));
      return Array.from({ length: groupSize }, (_, index) => index + 1)
        .filter((position) => !taken.has(position))
        .map((position) => ({ group_id: group.id, group_key: group.group_key, draw_position: position, bye: true }));
    });
    placements = [
      ...byes,
      ...players.map((entry) => ({
        group_id: entry.group_id,
        group_key: input.groups.find((group) => idKey(group.id) === idKey(entry.group_id))?.group_key || null,
        draw_position: entry.draw_position,
        slot_id: entry.slot_id,
        bye: false,
      })),
    ].map((placement, index) => ({ ...placement, draw_order: index + 1 }));
  }
  const withPlayers = (placement) => ({
    ...placement,
    player: placement.bye ? null : player(bySlot.get(idKey(placement.slot_id))),
  });
  return {
    tournament: { id: tournament.id, name: tournament.name },
    stage: { key: input.stage.stage_key, status: input.stage.status, start_date: input.stage.start_date || null },
    drawn,
    drawn_at: input.entries.find((entry) => entry.drawn_at)?.drawn_at || null,
    blockers: drawBlockers(input.entries),
    players: input.entries.map(player),
    groups: input.groups.map((group) => ({
      id: group.id,
      group_key: group.group_key,
      name: group.name,
      positions: (placements || [])
        .filter((placement) => idKey(placement.group_id) === idKey(group.id))
        .sort((a, b) => a.draw_position - b.draw_position)
        .map(withPlayers),
    })),
    placements: placements ? placements.map(withPlayers) : null,
    ...(result ? { seed: result.seed, attempts: result.attempts, conflicts: result.conflicts } : {}),
  };
}

const DRAW_SLOT_RE = /^draw:(\d+)$/;

async function saveDraw(db, { input, result, actorId, drawnAt }) {
  for (const placement of result.placements.filter((item) => !item.bye)) {
    await dbRun(
      db,
      `UPDATE ccl_stage_entries
       SET group_id = ?, draw_position = ?, draw_order = ?, drawn_at = ?, drawn_by = ?, updated_at = CURRENT_TIMESTAMP
       WHERE upper(trim(slot_id)) = upper(trim(?)) AND stage = ?`,
      [placement.group_id, placement.draw_position, placement.draw_order, drawnAt, actorId, placement.slot_id, input.stage.stage_key]
    );
  }
  const playerAt = new Map(result.placements
    .filter((item) => !item.bye)
    .map((item) => [`${idKey(item.group_id)}|${item.draw_position}`, input.entries.find((entry) => idKey(entry.slot_id) === idKey(item.slot_id)).player_id]));
  for (const group of input.groups) {
    const duels = await dbAll(
      db,
      "SELECT id, p1_source, p2_source FROM duels WHERE upper(trim(group_id)) = upper(trim(?)) AND deleted_at IS NULL",
      [group.id]
    );
    for (const duel of duels) {
      const slot = (source) => {
        const match = DRAW_SLOT_RE.exec(text(source));
        return match ? { player: playerAt.get(`${idKey(group.id)}|${Number(match[1])}`) || null } : null;
      };
      const p1 = slot(duel.p1_source);
      const p2 = slot(duel.p2_source);
      if (!p1 && !p2) continue;
      // A match with a Bye keeps one empty side and stays a Draft; result propagation handles it.
      const status = p1?.player && p2?.player ? "Planned" : "Draft";
      await dbRun(
        db,
        `UPDATE duels SET player_1_id = ?, player_2_id = ?, status = ?, updated_by = ?, updated_at = CURRENT_TIMESTAMP
         WHERE id = ?`,
        [p1 ? p1.player : null, p2 ? p2.player : null, status, actorId, duel.id]
      );
    }
  }
  await dbRun(
    db,
    "UPDATE tournament_stages SET status = 'drawn', updated_by = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
    [actorId, input.stage.id]
  );
}

// The draw can be undone only while no Round 1 match has a time, a result or a later status.
async function findResetBlockers(db, stageId) {
  return dbAll(
    db,
    `
      SELECT d.id, d.group_name, d.bracket_no, d.status
      FROM duels d
      JOIN tournament_groups g ON upper(trim(g.id)) = upper(trim(d.group_id))
      WHERE upper(trim(g.stage_id)) = upper(trim(?))
        AND d.deleted_at IS NULL
        AND (
          COALESCE(trim(d.time_utc), '') <> '' OR d.dw1 IS NOT NULL OR d.dw2 IS NOT NULL
          OR (COALESCE(trim(d.status), '') NOT IN ('', 'Draft', 'Planned')
              AND NOT (d.status = 'Cancelled' AND d.cancellation_reason IN ('bye', 'not_needed')))
          OR COALESCE(trim(d.proposed_time_status), '') = 'accepted'
        )
      ORDER BY d.group_name, d.bracket_no
    `,
    [stageId]
  );
}

async function resetDraw(db, { input, actorId }) {
  await dbRun(
    db,
    `UPDATE ccl_stage_entries
     SET group_id = NULL, draw_position = NULL, draw_order = NULL, drawn_at = NULL, drawn_by = NULL,
         updated_at = CURRENT_TIMESTAMP
     WHERE upper(trim(tournament_id)) = upper(trim(?)) AND stage = ?`,
    [input.stage.tournament_id, input.stage.stage_key]
  );
  for (const group of input.groups) {
    await dbRun(
      db,
      `UPDATE duels
       SET player_1_id = NULL, player_2_id = NULL, status = 'Draft', cancellation_reason = NULL,
           proposed_time_utc = NULL, proposed_time_by_player_id = NULL, proposed_time_status = NULL,
           updated_by = ?, updated_at = CURRENT_TIMESTAMP
       WHERE upper(trim(group_id)) = upper(trim(?)) AND deleted_at IS NULL`,
      [actorId, group.id]
    );
  }
  await dbRun(
    db,
    "UPDATE tournament_stages SET status = 'structure', updated_by = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
    [actorId, input.stage.id]
  );
}

export function registerCclDrawRoutes(app, {
  db,
  requireAuthenticated,
  getAuditActor = () => ({}),
  logAuditEvent = null,
  logger = console,
  now = () => new Date(),
  nextSeed = newDrawSeed,
}) {
  if (!app || !db || typeof requireAuthenticated !== "function") {
    throw new Error("app, db and requireAuthenticated are required");
  }
  const audit = (req, entry) => {
    if (typeof logAuditEvent === "function") logAuditEvent({ ...getAuditActor(req.user), ...entry }, () => {});
  };
  const handle = (failureMessage, handler) => async (req, res) => {
    try {
      return await handler(req, res);
    } catch (error) {
      const status = Number(error?.status) || 500;
      if (status >= 500) logger.error?.(failureMessage, error);
      return res.status(status).json({
        ok: false,
        message: status >= 500 ? failureMessage : error.message,
        ...(status < 500 && error.data ? error.data : {}),
      });
    }
  };
  const loadAdminTournament = async (req) => {
    const tournament = await loadCclTournament(db, req.params.tournamentId);
    if (!tournament) throw requestError(404, "CCL tournament not found");
    if (!await hasTournamentAdminAccess(db, req.user, tournament.id)) throw requestError(403, "Forbidden");
    return tournament;
  };
  const actorId = (user) => text(user?.player_id ?? user?.bga_id) || text(user?.email) || null;
  const route = "/ccl/:tournamentId/stages/:stageKey/draw";

  app.get(route, requireAuthenticated, handle("Failed to load the draw", async (req, res) => {
    const tournament = await loadAdminTournament(req);
    const input = await loadDrawInput(db, tournament, req.params.stageKey);
    return res.json({ ok: true, ...await describeDraw(db, tournament, input) });
  }));

  // "Тестування": the same draw as the real one, never saved; can be run any number of times.
  app.post(`${route}/preview`, requireAuthenticated, handle("Failed to run the test draw", async (req, res) => {
    const tournament = await loadAdminTournament(req);
    const input = await loadDrawInput(db, tournament, req.params.stageKey);
    if (text(input.stage.status) !== "structure") throw requestError(409, `${input.stage.stage_key} is already drawn`);
    const result = computeDraw(input, nextSeed());
    return res.json({ ok: true, test: true, ...await describeDraw(db, tournament, input, result) });
  }));

  // "Виконати жеребкування": one run with a fresh seed, saved at once.
  app.post(route, requireAuthenticated, handle("Failed to run the draw", async (req, res) => {
    const tournament = await loadAdminTournament(req);
    const by = actorId(req.user);
    const drawnAt = now().toISOString();
    const { input, result } = await inTransaction(db, async () => {
      const loaded = await loadDrawInput(db, tournament, req.params.stageKey);
      if (text(loaded.stage.status) !== "structure") throw requestError(409, `${loaded.stage.stage_key} is already drawn`);
      const computed = computeDraw(loaded, nextSeed());
      await saveDraw(db, { input: loaded, result: computed, actorId: by, drawnAt });
      // Matches with a Bye are settled at once: the player goes on, the Bye drops to the lower bracket.
      await syncCclBracketsInTransaction(db, tournament.id, { actorId: by });
      return { input: loaded, result: computed };
    });
    audit(req, {
      event_type: "ccl.draw.made",
      entity_type: "tournament_stage",
      action: "update",
      record_id: input.stage.id,
      changes: {},
      metadata: {
        tournament_id: tournament.id,
        stage: input.stage.stage_key,
        seed: result.seed,
        attempts: result.attempts,
        conflicts: result.conflicts,
        placements: result.placements.map(({ group_key: group, draw_position: position, slot_id: slot, bye }) => (
          bye ? { group, position, bye } : { group, position, slot }
        )),
      },
    });
    const saved = await loadDrawInput(db, tournament, req.params.stageKey);
    return res.json({ ok: true, ...await describeDraw(db, tournament, saved, result) });
  }));

  app.delete(route, requireAuthenticated, handle("Failed to reset the draw", async (req, res) => {
    const tournament = await loadAdminTournament(req);
    const by = actorId(req.user);
    const input = await inTransaction(db, async () => {
      const loaded = await loadDrawInput(db, tournament, req.params.stageKey);
      if (text(loaded.stage.status) !== "drawn") throw requestError(409, `${loaded.stage.stage_key} has no draw to reset`);
      const blockers = await findResetBlockers(db, loaded.stage.id);
      if (blockers.length) throw requestError(409, "Some matches already have a time or a result", { blockers });
      await resetDraw(db, { input: loaded, actorId: by });
      return loaded;
    });
    audit(req, {
      event_type: "ccl.draw.reset",
      entity_type: "tournament_stage",
      action: "update",
      record_id: input.stage.id,
      changes: {},
      metadata: { tournament_id: tournament.id, stage: input.stage.stage_key },
    });
    const saved = await loadDrawInput(db, tournament, req.params.stageKey);
    return res.json({ ok: true, ...await describeDraw(db, tournament, saved) });
  }));
}
