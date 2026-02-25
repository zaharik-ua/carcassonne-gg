/***** CONFIG *****/
const CONFIG = {
  // Якщо треба — обмеж список вкладок турнірів.
  // Якщо пусто, скрипт візьме всі листи, крім службових (ELO_*).
  tournamentSheetNameAllowlist: [
    "C26-D","C25-D","C24-D","W25-D","W24-D","W23-D","W22-D","W21-D","W20-D",
    "E25-D","E23-D","E21-D","E20-D","As25-D","As24-D","Am25-D","Am24-D","Am21-D"
  ],

  // Службові листи (скрипт їх не читає як турніри)
  serviceSheetPrefix: "ELO_",

  // Початковий Elo для гравця при першій появі
  initialElo: 1500,

  // Базовий K (можеш зробити функцію нижче динамічною)
  kFactorConst: 32,

  // Дільник в Elo формулі (класика 400)
  eloScale: 400,

  // Якщо дуелі можуть дублюватись між листами — увімкни дедуп
  dedupeDuels: true,
};

/**
 * Синоніми заголовків (case-insensitive).
 * Додай свої варіанти назв колонок, якщо відрізняються.
 */
const HEADER_SYNONYMS = {
  duelId: ["ID"],
  tournamentId: ["Tournament ID"],
  matchId: ["Match ID"],
  stage: ["Stage"],
  group: ["Group"],
  round: ["Round"],
  duelFormat: ["Duel Format"],
  timestamp: ["Time Start, UTC"],
  playerAid: ["Player 1 ID"],
  playerBid: ["Player 2 ID"],
  gwA: ["GW1"],
  gwB: ["GW2"],
  playerAname: ["Player 1"],
  playerBname: ["Player 2"],
  status: ["Status"],
  g1id: ["G1 ID"],
  g2id: ["G2 ID"],
  g3id: ["G3 ID"],
  g4id: ["G4 ID"],
  g5id: ["G5 ID"],
  g1score: ["G1 Score"],
  g2score: ["G2 Score"],
  g3score: ["G3 Score"],
  g4score: ["G4 Score"],
  g5score: ["G5 Score"],
};

/***** MENU *****/
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu("Elo Tools")
    .addItem("Recalculate Elo", "recalculateElo")
    .addSeparator()
    .addItem("Build Master Duel List Only", "buildMasterDuelList")
    .addToUi();
}

/***** MAIN *****/
function recalculateElo() {
  const ss = SpreadsheetApp.getActive();
  const tournamentSheets = getTournamentSheets_(ss);

  const duels = collectDuels_(tournamentSheets);
  const normalizedDuels = normalizeAndSortDuels_(duels);

  const result = computeElo_(normalizedDuels);

  const playersIndex = loadBgaPlayersIndex_(ss);

  writeDuelsLog_(ss, result.duelLog);
  writeRatings_(ss, result.ratingsTable, playersIndex);

  SpreadsheetApp.getUi().alert(
    `Done.\nDuels processed: ${result.duelLog.length}\nPlayers: ${result.ratingsTable.length}`
  );
}

function buildMasterDuelList() {
  const ss = SpreadsheetApp.getActive();
  const tournamentSheets = getTournamentSheets_(ss);

  const duels = collectDuels_(tournamentSheets);
  const normalizedDuels = normalizeAndSortDuels_(duels);

  writeMasterDuels_(ss, normalizedDuels);
  SpreadsheetApp.getUi().alert(`Master duel list built. Rows: ${normalizedDuels.length}`);
}

/***** SHEET SELECTION *****/
function getTournamentSheets_(ss) {
  const all = ss.getSheets();
  const allow = CONFIG.tournamentSheetNameAllowlist.map(s => s.toLowerCase().trim());
  return all.filter(sh => {
    const name = sh.getName();
    if (name.startsWith(CONFIG.serviceSheetPrefix)) return false;
    if (allow.length === 0) return true;
    return allow.includes(name.toLowerCase().trim());
  });
}

/***** DATA COLLECTION *****/
function collectDuels_(sheets) {
  const duels = [];
  for (const sh of sheets) {
    const range = sh.getDataRange();
    const values = range.getValues();
    const displayValues = range.getDisplayValues();
    if (values.length < 2) continue;

    const header = values[0].map(v => String(v || "").trim().toLowerCase());
    const col = mapColumns_(header);

    // Must have time + two players at least
    if (col.timestamp == null || col.playerAid == null || col.playerBid == null) {
      // skip silently; you can throw if you prefer strictness
      continue;
    }

    for (let r = 1; r < values.length; r++) {
      const row = values[r];

      const tsRaw = row[col.timestamp];
      const tsDisplay = displayValues[r][col.timestamp];
      const aRaw = row[col.playerAid];
      const bRaw = row[col.playerBid];
      if (!tsRaw || !aRaw || !bRaw) continue;
      const tsParsed = parseTimestamp_(tsRaw, tsDisplay);

      const duel = {
        sheet: sh.getName(),
        row: r + 1,
        duelId: col.duelId != null ? String(row[col.duelId] || "").trim() : "",
        tournamentId: col.tournamentId != null ? String(row[col.tournamentId] || "").trim() : "",
        matchId: col.matchId != null ? String(row[col.matchId] || "").trim() : "",
        stage: col.stage != null ? String(row[col.stage] || "").trim() : "",
        group: col.group != null ? String(row[col.group] || "").trim() : "",
        round: col.round != null ? String(row[col.round] || "").trim() : "",
        timestamp: tsParsed.date,
        timestampHasTime: tsParsed.hasTime,
        playerAid: String(aRaw).trim(),
        playerBid: String(bRaw).trim(),
        gwA: col.gwA != null ? row[col.gwA] : null,
        gwB: col.gwB != null ? row[col.gwB] : null,
        playerAname: col.playerAname != null ? String(row[col.playerAname] || "").trim() : "",
        playerBname: col.playerBname != null ? String(row[col.playerBname] || "").trim() : "",
        status: col.status != null ? String(row[col.status] || "").trim() : "",
        g1id: col.g1id != null ? String(row[col.g1id] || "").trim() : "",
        g2id: col.g2id != null ? String(row[col.g2id] || "").trim() : "",
        g3id: col.g3id != null ? String(row[col.g3id] || "").trim() : "",
        g4id: col.g4id != null ? String(row[col.g4id] || "").trim() : "",
        g5id: col.g5id != null ? String(row[col.g5id] || "").trim() : "",
        g1score: col.g1score != null ? String(row[col.g1score] || "").trim() : "",
        g2score: col.g2score != null ? String(row[col.g2score] || "").trim() : "",
        g3score: col.g3score != null ? String(row[col.g3score] || "").trim() : "",
        g4score: col.g4score != null ? String(row[col.g4score] || "").trim() : "",
        g5score: col.g5score != null ? String(row[col.g5score] || "").trim() : "",
        duelFormat: col.duelFormat != null ? row[col.duelFormat] : null,
      };

      if (!duel.timestamp || isNaN(duel.timestamp.getTime())) continue;
      if (!duel.playerAid || !duel.playerBid) continue;
      if (duel.playerAid === duel.playerBid) continue;

      duels.push(duel);
    }
  }
  return duels;
}

function mapColumns_(headerRow) {
  const findIdx = (syns) => {
    for (const s of syns) {
      const idx = headerRow.indexOf(s.toLowerCase());
      if (idx !== -1) return idx;
    }
    return null;
  };

  return {
    duelId: findIdx(HEADER_SYNONYMS.duelId),
    tournamentId: findIdx(HEADER_SYNONYMS.tournamentId),
    matchId: findIdx(HEADER_SYNONYMS.matchId),
    stage: findIdx(HEADER_SYNONYMS.stage),
    group: findIdx(HEADER_SYNONYMS.group),
    round: findIdx(HEADER_SYNONYMS.round),
    timestamp: findIdx(HEADER_SYNONYMS.timestamp),
    playerAid: findIdx(HEADER_SYNONYMS.playerAid),
    playerBid: findIdx(HEADER_SYNONYMS.playerBid),
    gwA: findIdx(HEADER_SYNONYMS.gwA),
    gwB: findIdx(HEADER_SYNONYMS.gwB),
    duelFormat: findIdx(HEADER_SYNONYMS.duelFormat),
    playerAname: findIdx(HEADER_SYNONYMS.playerAname),
    playerBname: findIdx(HEADER_SYNONYMS.playerBname),
    status: findIdx(HEADER_SYNONYMS.status),
    g1id: findIdx(HEADER_SYNONYMS.g1id),
    g2id: findIdx(HEADER_SYNONYMS.g2id),
    g3id: findIdx(HEADER_SYNONYMS.g3id),
    g4id: findIdx(HEADER_SYNONYMS.g4id),
    g5id: findIdx(HEADER_SYNONYMS.g5id),
    g1score: findIdx(HEADER_SYNONYMS.g1score),
    g2score: findIdx(HEADER_SYNONYMS.g2score),
    g3score: findIdx(HEADER_SYNONYMS.g3score),
    g4score: findIdx(HEADER_SYNONYMS.g4score),
    g5score: findIdx(HEADER_SYNONYMS.g5score),
  };
}

function parseTimestamp_(v, displayValue) {
  const hasTimeFromDisplay = hasTimeInDisplay_(displayValue);

  // If already Date
  if (Object.prototype.toString.call(v) === "[object Date]") {
    return {
      date: v,
      hasTime: hasTimeFromDisplay || hasTimeInDate_(v),
    };
  }

  // If numeric (Sheets date serial)
  if (typeof v === "number") {
    // Google Sheets serial date: days since 1899-12-30
    const ms = Math.round((v - 25569) * 86400 * 1000);
    const d = new Date(ms);
    return {
      date: d,
      hasTime: hasTimeFromDisplay || hasTimeInDate_(d),
    };
  }

  // If string
  const s = String(v).trim();
  const parsed = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?)?$/);
  if (parsed) {
    const day = Number(parsed[1]);
    const month = Number(parsed[2]);
    const year = Number(parsed[3]);
    const hours = parsed[4] != null ? Number(parsed[4]) : 0;
    const minutes = parsed[5] != null ? Number(parsed[5]) : 0;
    const seconds = parsed[6] != null ? Number(parsed[6]) : 0;
    return {
      date: new Date(year, month - 1, day, hours, minutes, seconds),
      hasTime: hasTimeFromDisplay || parsed[4] != null,
    };
  }

  // Fallback: Date parse (ISO is best)
  const d = new Date(s);
  return {
    date: d,
    hasTime: hasTimeFromDisplay || hasTimeInDate_(d),
  };
}

function hasTimeInDisplay_(v) {
  const s = String(v == null ? "" : v).trim();
  return /\b\d{1,2}:\d{2}(?::\d{2})?\b/.test(s);
}

function hasTimeInDate_(d) {
  return d.getHours() !== 0 || d.getMinutes() !== 0 || d.getSeconds() !== 0 || d.getMilliseconds() !== 0;
}

/***** NORMALIZE + SORT + DEDUPE *****/
function normalizeAndSortDuels_(duels) {
  // Dedupe key: timestamp + players + scores (if present)
  const seen = new Set();
  const out = [];

  for (const m of duels) {
    const gwA = m.gwA == null ? "" : String(m.gwA).trim();
    const gwB = m.gwB == null ? "" : String(m.gwB).trim();
    const df = m.duelFormat == null ? "" : String(m.duelFormat).trim();
    const key = `${m.timestamp.toISOString()}|${m.playerAid}|${m.playerBid}|${gwA}|${gwB}|${df}`;

    if (CONFIG.dedupeDuels) {
      if (seen.has(key)) continue;
      seen.add(key);
    }
    out.push(m);
  }

  out.sort((x, y) => x.timestamp.getTime() - y.timestamp.getTime());
  return out;
}

/***** ELO CORE *****/
function computeElo_(duels) {
  const players = new Map(); // name -> stats

  const getPlayer = (name) => {
    if (!players.has(name)) {
      players.set(name, {
        player: name,
        elo: CONFIG.initialElo,
        duels: 0,
        wins: 0,
        draws: 0,
        losses: 0,
        firstSeenSheet: "",
        firstTimePlayed: null,
        lastTimePlayed: null,
      });
    }
    return players.get(name);
  };

  const log = [];

  for (const m of duels) {
    const A = getPlayer(m.playerAid);
    const B = getPlayer(m.playerBid);

    // First seen metadata
    if (!A.firstTimePlayed || m.timestamp < A.firstTimePlayed) {
      A.firstTimePlayed = m.timestamp;
      A.firstSeenSheet = m.sheet;
    }
    if (!B.firstTimePlayed || m.timestamp < B.firstTimePlayed) {
      B.firstTimePlayed = m.timestamp;
      B.firstSeenSheet = m.sheet;
    }

    const eloA_before = A.elo;
    const eloB_before = B.elo;

    // === НОВЕ: результат тільки зі gwA/gwB (winner не потрібен) ===
    const duel = inferResultFromScores_(m.gwA, m.gwB);
    if (!duel) continue; // якщо раптом нема чисел — пропускаємо

    // === НОВЕ: N з Duel Format (Bo3/Bo5) ===
    const N = parseBoN_(m.duelFormat, m.gwA, m.gwB);

    // === НОВЕ: твій коефіцієнт Kyrylo’s K ===
    const K = kyryloK_(duel.winsWinner, duel.winsLoser, N);

    // expectation (ймовірність перемоги A)
    const Ea = expectedScore_(eloA_before, eloB_before);
    const Eb = 1 - Ea;

    const Kfactor = CONFIG.kFactorConst; // const1

    A.elo = eloA_before + Kfactor * (duel.sA - Ea) * K;
    B.elo = eloB_before + Kfactor * (duel.sB - Eb) * K;

    // Stats
    A.duels += 1;
    B.duels += 1;

    if (duel.sA === 1) { A.wins += 1; B.losses += 1; }
    else if (duel.sA === 0) { A.losses += 1; B.wins += 1; }
    else { A.draws += 1; B.draws += 1; }

    A.lastTimePlayed = m.timestamp;
    B.lastTimePlayed = m.timestamp;

    log.push({
      timestamp: m.timestamp,
      timestampHasTime: m.timestampHasTime,
      sheet: m.sheet,
      row: m.row,
      duelId: m.duelId,
      duelFormat: m.duelFormat,
      N,
      K,
      playerAid: A.player,
      playerBid: B.player,
      sA: duel.sA,
      sB: duel.sB,
      gwA: m.gwA,
      gwB: m.gwB,
      eloA_before,
      eloB_before,
      eloA_after: A.elo,
      eloB_after: B.elo,
      tournamentId: m.tournamentId,
      matchId: m.matchId,
      stage: m.stage,
      group: m.group,
      round: m.round,
      playerAname: m.playerAname,
      playerBname: m.playerBname,
      status: m.status,
      g1id: m.g1id,
      g2id: m.g2id,
      g3id: m.g3id,
      g4id: m.g4id,
      g5id: m.g5id,
      g1score: m.g1score,
      g2score: m.g2score,
      g3score: m.g3score,
      g4score: m.g4score,
      g5score: m.g5score,
    });
  }

  // Final table
  const ratings = Array.from(players.values())
    .map(p => ({
      player: p.player,
      elo: p.elo,
      duels: p.duels,
      wins: p.wins,
      draws: p.draws,
      losses: p.losses,
      firstSeenSheet: p.firstSeenSheet,
      firstTimePlayed: p.firstTimePlayed,
      lastTimePlayed: p.lastTimePlayed,
    }))
    .sort((a, b) => b.elo - a.elo);

  return { duelLog: log, ratingsTable: ratings };
}


function inferResultFromScores_(gwA, gwB) {
  const a = toNumberOrNull_(gwA);
  const b = toNumberOrNull_(gwB);
  if (a == null || b == null) return null;

  if (a > b) return { sA: 1, sB: 0, winsWinner: a, winsLoser: b, winnerIsA: true };
  if (a < b) return { sA: 0, sB: 1, winsWinner: b, winsLoser: a, winnerIsA: false };

  // На всякий — якщо раптом буде нічия
  return { sA: 0.5, sB: 0.5, winsWinner: a, winsLoser: b, winnerIsA: null };
}

function expectedScore_(ra, rb) {
  return 1 / (1 + Math.pow(10, (rb - ra) / CONFIG.eloScale));
}

function toNumberOrNull_(v) {
  if (v === null || v === undefined || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

/***** OUTPUT *****/
function ensureSheet_(ss, name) {
  let sh = ss.getSheetByName(name);
  if (!sh) sh = ss.insertSheet(name);
  sh.clear();
  return sh;
}

function writeMasterDuels_(ss, duels) {
  const sh = ensureSheet_(ss, "ELO_MasterDuels");
  const header = ["timestamp", "sheet", "row", "playerAid", "playerBid", "gwA", "gwB", "duelFormat"];
  sh.getRange(1, 1, 1, header.length).setValues([header]);

  const rows = duels.map(m => [
    m.timestamp,
    m.sheet,
    m.row,
    m.playerAid,
    m.playerBid,
    m.gwA,
    m.gwB,
    m.duelFormat,
  ]);

  if (rows.length) sh.getRange(2, 1, rows.length, header.length).setValues(rows);
  if (rows.length) applyTimestampFormats_(sh, 2, 1, duels.map(m => !!m.timestampHasTime));
  sh.autoResizeColumns(1, header.length);
}

function writeDuelsLog_(ss, log) {
  const sh = ensureSheet_(ss, "DUELS");

  const header = [
    "timestamp","sheet","row",
    "duelId","matchId","tournamentId",
    "stage","group","round",
    "duelFormat","N","K",
    "playerAid","playerAname",
    "playerBid","playerBname",
    "gwA","gwB","sA","sB",
    "status",
    "eloA_before","eloB_before","eloA_after","eloB_after",
    "g1id","g2id","g3id","g4id","g5id",
    "g1score","g2score","g3score","g4score","g5score",
  ];
  sh.getRange(1, 1, 1, header.length).setValues([header]);

  const rows = log.map(x => [
    x.timestamp,
    x.sheet,
    x.row,
    x.duelId,
    x.matchId,
    x.tournamentId,
    x.stage,
    x.group,
    x.round,
    x.duelFormat,
    x.N,
    round_(x.K),
    x.playerAid,
    x.playerAname,
    x.playerBid,
    x.playerBname,
    x.gwA,
    x.gwB,
    x.sA,
    x.sB,
    x.status,
    round_(x.eloA_before),
    round_(x.eloB_before),
    round_(x.eloA_after),
    round_(x.eloB_after),
    x.g1id,
    x.g2id,
    x.g3id,
    x.g4id,
    x.g5id,
    x.g1score,
    x.g2score,
    x.g3score,
    x.g4score,
    x.g5score,
  ]);

  if (rows.length) sh.getRange(2, 1, rows.length, header.length).setValues(rows);
  if (rows.length) applyTimestampFormats_(sh, 2, 1, log.map(x => !!x.timestampHasTime));
  sh.autoResizeColumns(1, header.length);
}

function applyTimestampFormats_(sheet, startRow, col, hasTimeFlags) {
  if (!hasTimeFlags || !hasTimeFlags.length) return;

  const dateFormat = "dd/MM/yyyy";
  const dateTimeFormat = "dd/MM/yyyy HH:mm:ss";

  let runStart = 0;
  let runHasTime = !!hasTimeFlags[0];

  for (let i = 1; i <= hasTimeFlags.length; i++) {
    const current = i < hasTimeFlags.length ? !!hasTimeFlags[i] : null;
    if (current === runHasTime) continue;

    const runLen = i - runStart;
    const fmt = runHasTime ? dateTimeFormat : dateFormat;
    sheet.getRange(startRow + runStart, col, runLen, 1).setNumberFormat(fmt);

    runStart = i;
    runHasTime = current;
  }
}


function writeRatings_(ss, ratings, playersIndex) {
  const sh = ensureSheet_(ss, "ELO_Ratings");

  // cutoff = сьогодні мінус 2 роки
  const cutoff = new Date();
  cutoff.setFullYear(cutoff.getFullYear() - 2);

  const header = [
    "rank",
    "player",   // ім'я з BGA PLAYERS якщо знайдено
    "id",       // BGA ID
    "elo",
    "duels","wins","draws","losses",
    "firstSeenSheet","firstTimePlayed","lastTimePlayed"
  ];
  sh.getRange(1, 1, 1, header.length).setValues([header]);

  const byId = playersIndex?.byId || new Map();
  const byName = playersIndex?.byName || new Map();

  // Перевірка активності за lastTimePlayed:
  // неактивні гравці теж потрапляють у список, але без значення elo.
  const isActiveByLastTimePlayed = (p) => {
    if (!p.lastTimePlayed) return false;
    const d = (Object.prototype.toString.call(p.lastTimePlayed) === "[object Date]")
      ? p.lastTimePlayed
      : new Date(p.lastTimePlayed);
    if (isNaN(d.getTime())) return false;
    return d >= cutoff;
  };

  const rows = ratings.map((p, i) => {
    const key = String(p.player || "").trim(); // у тебе це ID з матчів
    const rec = byId.get(key) || byName.get(key.toLowerCase());

    const displayName = rec?.name || key;
    const id = rec?.id || (key && /^\d+$/.test(key) ? key : "");
    const eloValue = isActiveByLastTimePlayed(p) ? round_(p.elo) : "";

    return [
      i + 1,
      displayName,
      id,
      eloValue,
      p.duels, p.wins, p.draws, p.losses,
      p.firstSeenSheet,
      p.firstTimePlayed,
      p.lastTimePlayed,
    ];
  });

  if (rows.length) sh.getRange(2, 1, rows.length, header.length).setValues(rows);
  sh.autoResizeColumns(1, header.length);
}



function round_(x) {
  return Math.round(x * 100) / 100;
}

function parseBoN_(duelFormat, gwA, gwB) {
  const s = String(duelFormat || "").toLowerCase();
  const nFromText = parseInt((s.match(/\d+/) || [])[0], 10);
  if (Number.isFinite(nFromText) && nFromText > 0) return nFromText;

  // fallback: якщо формат не вказаний — виводимо з максимального рахунку
  const a = toNumberOrNull_(gwA);
  const b = toNumberOrNull_(gwB);
  const maxW = Math.max(a || 0, b || 0);

  // типові випадки: 1 -> Bo1, 2 -> Bo3, 3 -> Bo5
  if (maxW === 1) return 1;
  if (maxW === 2) return 3;
  if (maxW === 3) return 5;

  // дефолт
  return 3;
}

function kyryloK_(winsWinner, winsLoser, N) {
  // K = 1 + (Win1 - Win2 - 1) / N
  return 1 + (winsWinner - winsLoser - 1) / N;
}

function loadBgaPlayersIndex_(ss) {
  const sh = ss.getSheetByName("BGA PLAYERS");
  if (!sh) return { byId: new Map(), byName: new Map() };

  const values = sh.getDataRange().getValues();
  if (values.length < 2) return { byId: new Map(), byName: new Map() };

  const header = values[0].map(v => String(v || "").trim().toLowerCase());
  const colId = header.indexOf("id");
  const colPlayer = header.indexOf("player");
  const colAvatar = header.indexOf("bga avatar");
  const colBgaElo = header.indexOf("bga elo");
  const colUpdated = header.indexOf("elo updated on");

  const byId = new Map();
  const byName = new Map();

  for (let r = 1; r < values.length; r++) {
    const row = values[r];
    const id = colId >= 0 ? String(row[colId] || "").trim() : "";
    const name = colPlayer >= 0 ? String(row[colPlayer] || "").trim() : "";
    if (!id && !name) continue;

    const rec = {
      id,
      name,
      avatar: colAvatar >= 0 ? row[colAvatar] : "",
      bgaElo: colBgaElo >= 0 ? row[colBgaElo] : "",
      updatedOn: colUpdated >= 0 ? row[colUpdated] : "",
    };

    if (id) byId.set(id, rec);
    if (name) byName.set(name.toLowerCase(), rec);
  }

  return { byId, byName };
}
