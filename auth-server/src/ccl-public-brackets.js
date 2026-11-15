// Public CCL Qualifiers brackets: GET /public/ccl/:tournamentId/brackets, no sign-in.
// The response keeps the shape of tournaments-open.json (tournaments, duels, players, countries,
// tournament_players), so the bracket renderer of CCL-2027-qualifications.html works unchanged:
// one duel row per match plus an "Advance to the League Stage" row after every match whose winner
// leaves the group; round_order is the column inside the upper or the lower bracket.

import { loadAssociationsById, loadCclTournament, loadProfilesById, loadSlots } from "./ccl-routes.js";
import { buildBracketLinks, describeSlotSource, parseSlotSource } from "./double-elimination.js";

export const ADVANCE_LABEL = "Advance to the League Stage";
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const DAY_MS = 86400000;
const UPPER = "1 - upper";
const LOWER = "2 - lower";

const text = (value) => String(value ?? "").trim();
const idKey = (value) => text(value).toUpperCase();
const pad = (value) => String(value).padStart(2, "0");

function dbAll(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows || [])));
  });
}

// Week of a match from its deadline (Sunday): "23-29.Nov", or "30.Nov-6.Dec" across months.
export function formatRoundDates(deadlineUtc) {
  const end = Date.parse(text(deadlineUtc));
  if (!Number.isFinite(end)) return "";
  const last = new Date(end);
  const first = new Date(end - 6 * DAY_MS);
  const month = (date) => MONTHS.at(date.getUTCMonth());
  return first.getUTCMonth() === last.getUTCMonth()
    ? `${first.getUTCDate()}-${last.getUTCDate()}.${month(last)}`
    : `${first.getUTCDate()}.${month(first)}-${last.getUTCDate()}.${month(last)}`;
}

// "2026-11-20T18:00:00.000Z" → "20.11.2026 18:00:00" (UTC), the time format of the page.
export function formatMatchTime(timeUtc) {
  const ts = Date.parse(text(timeUtc));
  if (!Number.isFinite(ts)) return "";
  const date = new Date(ts);
  return `${pad(date.getUTCDate())}.${pad(date.getUTCMonth() + 1)}.${date.getUTCFullYear()} `
    + `${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())}:${pad(date.getUTCSeconds())}`;
}

const sideOf = (match) => (match.bracket_side === "lower" ? LOWER : UPPER);
const isConditional = (match) => Number(match.is_conditional) === 1 || match.is_conditional === true;

// Pure. Rows of one group. playerName(id) gives the shown name of a player.
export function buildPublicGroupRows({ tournamentId, stageKey, groupKey, settings = {}, matches, playerName = (id) => id }) {
  const sorted = [...matches].sort((a, b) => a.bracket_no - b.bracket_no);
  // Columns: the rounds of each side in the order they first appear.
  const columns = { [UPPER]: [], [LOWER]: [] };
  for (const match of sorted) {
    const list = columns[sideOf(match)];
    if (!list.includes(match.round_name)) list.push(match.round_name);
  }
  const columnOf = (match) => columns[sideOf(match)].indexOf(match.round_name) + 1;
  // The only conditional match is a Group Final Reset, which follows the Group Final right before it.
  const links = buildBracketLinks(sorted.map((match) => ({
    ...match,
    is_conditional: isConditional(match),
    depends_on: match.bracket_no - 1,
  })));
  const byNo = new Map(sorted.map((match) => [match.bracket_no, match]));

  const isByeMatch = (match) => !!match && text(match.status) === "Cancelled" && text(match.cancellation_reason) === "bye";
  const side = (match, playerId, source) => {
    const id = text(playerId);
    if (id) return { name: playerName(id) || id, id };
    // An empty side of a match settled by a Bye is the Bye itself, and so is the "loser" of such a match.
    const parsed = parseSlotSource(source);
    if (isByeMatch(match) || (parsed?.kind === "loser" && isByeMatch(byNo.get(parsed.ref)))) return { name: "Bye", id: "" };
    return { name: describeSlotSource(source), id: "" };
  };

  const rows = sorted.map((match) => {
    const player1 = side(match, match.player_1_id, match.p1_source);
    const player2 = side(match, match.player_2_id, match.p2_source);
    const nextWin = links.get(match.bracket_no)?.next_win;
    // A connector is drawn only inside one side of the bracket.
    const sameSideNext = nextWin && sideOf(byNo.get(nextWin)) === sideOf(match) ? String(nextWin) : "";
    const dates = formatRoundDates(match.deadline_utc);
    const done = text(match.status) === "Done";
    return {
      tournament_id: tournamentId,
      duel_id: match.id,
      stage: stageKey,
      group: groupKey,
      round: dates ? `${match.round_name}: ${dates}` : match.round_name,
      round_order: String(columnOf(match)),
      multiple_elimination_stage: sideOf(match),
      knockout_id: String(match.bracket_no),
      next_game_win: sameSideNext,
      knockout_stage: "",
      player1: player1.name,
      player1_id: player1.id,
      player2: player2.name,
      player2_id: player2.id,
      score1: done ? String(match.dw1 ?? "") : "",
      score2: done ? String(match.dw2 ?? "") : "",
      status: text(match.status),
      duel_format: match.duel_format || "",
      time: formatMatchTime(match.time_utc),
    };
  });

  // "Advance to the League Stage" after the last match of each advance source.
  const maxNo = Math.max(0, ...sorted.map((match) => match.bracket_no));
  (settings.advance || []).forEach(({ source }, index) => {
    const numbers = (/^winner:([\d|]+)$/.exec(text(source))?.[1] || "").split("|").map(Number).filter(Boolean);
    const anchor = rows.find((row) => Number(row.knockout_id) === numbers.at(-1));
    if (!anchor) return;
    const knockoutId = String(maxNo + index + 1);
    anchor.next_game_win = knockoutId;
    rows.push({
      tournament_id: tournamentId,
      duel_id: "",
      stage: stageKey,
      group: groupKey,
      round: "",
      round_order: String(Number(anchor.round_order) + 1),
      multiple_elimination_stage: anchor.multiple_elimination_stage,
      knockout_id: knockoutId,
      next_game_win: "",
      knockout_stage: ADVANCE_LABEL,
      player1: null,
      player1_id: null,
      player2: null,
      player2_id: null,
      score1: null,
      score2: null,
      status: null,
      duel_format: null,
      time: null,
    });
  });
  return rows;
}

export async function loadPublicCclBrackets(db, tournament) {
  const stages = await dbAll(
    db,
    `SELECT id, stage_key, settings_json FROM tournament_stages
     WHERE upper(trim(tournament_id)) = upper(trim(?)) AND format = 'double_elimination'
     ORDER BY sort_order, stage_key`,
    [tournament.id]
  );
  const groups = await dbAll(
    db,
    `SELECT g.id, g.stage_id, g.group_key FROM tournament_groups g
     JOIN tournament_stages s ON upper(trim(s.id)) = upper(trim(g.stage_id))
     WHERE upper(trim(s.tournament_id)) = upper(trim(?))
     ORDER BY g.sort_order, g.group_key`,
    [tournament.id]
  );
  const duels = await dbAll(
    db,
    `SELECT id, group_id, bracket_no, bracket_side, round_name, deadline_utc, duel_format, time_utc,
            COALESCE(NULLIF(trim(status), ''), 'Draft') AS status, cancellation_reason, dw1, dw2,
            player_1_id, player_2_id, p1_source, p2_source, is_conditional
     FROM duels
     WHERE upper(trim(tournament_id)) = upper(trim(?)) AND group_id IS NOT NULL AND deleted_at IS NULL`,
    [tournament.id]
  );
  const playerIds = [...new Set(duels.flatMap((duel) => [text(duel.player_1_id), text(duel.player_2_id)]).filter(Boolean))];
  const [profiles, associations, slots] = await Promise.all([
    loadProfilesById(db, playerIds),
    loadAssociationsById(db),
    loadSlots(db, tournament.id),
  ]);
  const playerName = (id) => profiles.get(id)?.bga_nickname || profiles.get(id)?.name || id;

  const rows = [];
  for (const stage of stages) {
    let settings = {};
    try {
      settings = JSON.parse(stage.settings_json || "{}");
    } catch (_error) {
      settings = {};
    }
    for (const group of groups.filter((item) => idKey(item.stage_id) === idKey(stage.id))) {
      rows.push(...buildPublicGroupRows({
        tournamentId: tournament.id,
        stageKey: stage.stage_key,
        groupKey: group.group_key,
        settings,
        matches: duels.filter((duel) => idKey(duel.group_id) === idKey(group.id)),
        playerName,
      }));
    }
  }

  // Association of every player in the brackets: the slot's association, else the profile's.
  const slotByPlayer = new Map(slots.filter((slot) => text(slot.player_id)).map((slot) => [text(slot.player_id), slot]));
  const tournamentPlayers = playerIds.map((id) => {
    const association = associations.get(idKey(slotByPlayer.get(id)?.association_id || profiles.get(id)?.association));
    return {
      tournament_id: tournament.id,
      player_id: id,
      player: playerName(id),
      country: association?.name || "",
      flag: association?.flag || "",
    };
  });
  const usedAssociations = new Map();
  tournamentPlayers.forEach((row) => {
    if (row.country && !usedAssociations.has(row.country)) usedAssociations.set(row.country, row.flag);
  });

  return {
    tournaments: [{
      tournament_id: tournament.id,
      name: tournament.name || tournament.id,
      short_title: tournament.short_title || null,
      type: "Individual",
      stage1_html: null,
      match_width: null,
      third_place_vertical_offset: null,
    }],
    duels: rows,
    players: playerIds.map((id) => ({ player_id: id, player: playerName(id), avatar: profiles.get(id)?.avatar || null })),
    countries: [...usedAssociations].map(([team, flag]) => ({ team, flag })),
    tournament_players: tournamentPlayers,
  };
}

export function registerCclPublicBracketRoutes(app, { db, logger = console }) {
  if (!app || !db) throw new Error("app and db are required");
  app.get("/public/ccl/:tournamentId/brackets", async (req, res) => {
    try {
      const tournament = await loadCclTournament(db, req.params.tournamentId);
      if (!tournament) return res.status(404).json({ ok: false, message: "CCL tournament not found" });
      return res.json({ ok: true, ...await loadPublicCclBrackets(db, tournament) });
    } catch (error) {
      logger.error?.("Failed to load CCL brackets", error);
      return res.status(500).json({ ok: false, message: "Failed to load CCL brackets" });
    }
  });
}
