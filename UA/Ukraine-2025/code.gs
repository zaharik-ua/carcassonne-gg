/** -------- Swiss System for Google Sheets --------
 * Sheets used:
 *  - Players: [ID, Name, Rating]
 *  - Rounds:  [Round, Board, PlayerA_ID, PlayerA_Name, PlayerB_ID, PlayerB_Name, WinA, WinB, ResultNote]
 *  - Swiss: [ID, Name, Rating, Points, Buchholz, SB, Byes]
 *  - Config: [Key, Value] with keys: Rounds, Win, Draw, Loss, ByePoints
 */

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Swiss')
    // .addItem('Setup Template', 'swiss_setupTemplate')
    .addItem('Generate Round 1', 'swiss_generateRound1')
    .addItem('Generate Next Round', 'swiss_generateNextRound')
    .addItem('Rebuild Standings', 'swiss_rebuildStandings')
    .addSeparator()
    .addItem('Withdraw by ID...', 'swiss_withdrawById')
    .addSeparator()
    .addItem('Reset Tournament', 'swiss_resetTournament')
    .addToUi();
}

function swiss_setupTemplate() {
  const ss = SpreadsheetApp.getActive();

  const ensure = (name, headers) => {
    let sh = ss.getSheetByName(name);
    if (!sh) sh = ss.insertSheet(name);
    sh.clear();
    if (headers && headers.length) sh.getRange(1,1,1,headers.length).setValues([headers]);
    sh.setFrozenRows(1);
    return sh;
  };

  ensure('Players', ['ID','Name','Rating']);

  ensure('Rounds', [
  'Round','Board',
  'PlayerA_ID','PlayerA_Name','PlayerB_ID','PlayerB_Name',
  'Points_A','Points_B',
  'WinA','WinB',
  'Starter',
  'ResultNote'
]);

  // Актуальний хедер стендінгів з новими тай-брейками
  const st = ensure('Swiss', []);
  ensureStandingsHeader(st);

  const cfg = ensure('Config', ['Key','Value','Note']);
  cfg.getRange(2,1,5,3).setValues([
    ['Rounds',      6,  'Кількість раундів (за правилами WCF — 6)'],
    ['Win',         1,  'Очки за перемогу (використовується як запасний варіант)'],
    ['Draw',        0,  'Нічиї не застосовуються: при рівних VP програє Starter'],
    ['Loss',        0,  'Очки за поразку'],
    ['ByePoints',   1,  'Очки за bye (як 1 перемога)'],
  ]);

  SpreadsheetApp.getUi().alert('Готово! Додайте гравців і генеруйте 1-й тур.');
}

function swiss_resetTournament() {
  const ui = SpreadsheetApp.getUi();
  const res = ui.alert('Reset tournament', 'Очистити всі раунди та підсумки? Дані Players лишаться.', ui.ButtonSet.YES_NO);
  if (res !== ui.Button.YES) return;
  const ss = SpreadsheetApp.getActive();
  const rounds = ss.getSheetByName('Rounds');
  const standings = ss.getSheetByName('Swiss');
  if (rounds)    safeClearBelowHeader(rounds);
  if (standings) safeClearBelowHeader(standings);
  ui.alert('Турнір очищено. Дані гравців збережені.');
}

function swiss_generateRound1() {
  const state = loadState();
  if (state.currentRound > 0) {
    SpreadsheetApp.getUi().alert('Перший раунд вже був згенерований.');
    return;
  }
  const players = state.players.filter(p => p.active !== false);
  if (players.length < 2) {
    SpreadsheetApp.getUi().alert('Додайте щонайменше 2 гравців.');
    return;
  }
  // Сортування за рейтингом (вищий — зверху). Якщо рейтингів нема — як ввели.
  const sorted = players.slice().sort((a,b) => (b.rating||0)-(a.rating||0));
  const pairings = pairFirstRound(sorted, state);
  writePairings(state, 1, pairings);
  rebuildStandingsInternal(state);
  SpreadsheetApp.getUi().alert('Раунд 1 згенеровано.');
}

function swiss_generateNextRound() {
  const state = loadState();
  const targetRounds = state.config.rounds;

  rebuildStandingsInternal(state);

  if (state.currentRound === 0 && !state.rounds.length) {
    SpreadsheetApp.getUi().alert('Спершу згенеруйте 1-й раунд.');
    return;
  }
  if (state.currentRound >= targetRounds) {
    SpreadsheetApp.getUi().alert('Досягнуто максимальну кількість раундів (' + targetRounds + ').');
    return;
  }

  // ❗ якщо вже є незавершений раунд (рядки round > currentRound) — стоп
  if (!requireNoPendingRound(state)) return;

  const activeSet = new Set(state.players.filter(p => p.active !== false).map(p => String(p.id)));
  const standingsSortedAll = state.standings.slice().sort(compareSwissRows);
  const standingsSorted = standingsSortedAll.filter(s => activeSet.has(String(s.id)));
  if (standingsSorted.length < 2) {
    SpreadsheetApp.getUi().alert('Замало активних гравців для нового раунду.');
    return;
  }

  const nextRound = state.currentRound + 1;

  // страховка: якщо в "Rounds" вже є рядки саме цього round — не генеруємо
  if (state.rounds.some(m => m.round === nextRound)) {
    SpreadsheetApp.getUi().alert('Для раунду ' + nextRound + ' уже є рядки. Завершіть/очистьте їх перед новою генерацією.');
    return;
  }

  const pairings = pairSwissGreedy(standingsSorted, state);
  writePairings(state, nextRound, pairings);
  rebuildStandingsInternal(state);
  SpreadsheetApp.getUi().alert('Раунд ' + nextRound + ' згенеровано.');
}

function loadState() {
  const ss = SpreadsheetApp.getActive();
  const shPlayers   = ss.getSheetByName('Players');
  const shRounds    = ss.getSheetByName('Rounds');
  const shStandings = ss.getSheetByName('Swiss');
  const shConfig    = ss.getSheetByName('Config');
  if (!shPlayers || !shRounds || !shStandings || !shConfig) {
    throw new Error('Не знайдено один із потрібних аркушів (Players/Rounds/Swiss/Config). Запустіть Setup Template.');
  }

  ensurePlayersActiveColumn();

  const config = readConfig(shConfig);
  const players = readPlayers(shPlayers);
  const rounds = readRounds(shRounds);

  const currentRound = lastCompletedRoundNumber(rounds);

  // ❗ Підрахунок лише до завершеного туру включно
  const playedPairs = new Set();
  const byeCount = new Map();
  for (const m of rounds) {
    if (!m.round || m.round > currentRound) continue;
    const a = m.playerA_id, b = m.playerB_id;
    if (a && b) {
      playedPairs.add(normalPairKey(a,b));
    } else if (a && !b) {
      const k = String(a);
      byeCount.set(k, (byeCount.get(k)||0)+1);
    } else if (b && !a) {
      const k = String(b);
      byeCount.set(k, (byeCount.get(k)||0)+1);
    }
  }

  const standings = computeStandings(players, rounds, config, currentRound);

  return { ss, shPlayers, shRounds, shStandings, shConfig,
           config, players, rounds, currentRound,
           playedPairs, byeCount, standings };
}

function readConfig(sh) {
  const m = {};
  const vals = sh.getRange(2,1,Math.max(0, sh.getLastRow()-1),3).getValues();
  for (const [k,v] of vals.map(r => [r[0], r[1]])) {
    if (!k) continue;
    m[k] = v;
  }
  return {
    rounds: Number(m['Rounds']||5),
    win: Number(m['Win']||1),
    draw: Number(m['Draw']||0.5),
    loss: Number(m['Loss']||0),
    bye: Number(m['ByePoints']||1),
  };
}

function readPlayers(sh) {
  const rows = Math.max(0, sh.getLastRow()-1);
  if (!rows) return [];
  const lastCol = sh.getLastColumn();
  const headers = sh.getRange(1,1,1,lastCol).getValues()[0].map(String);
  const idxActive = headers.findIndex(h => h.trim().toLowerCase() === 'active'); // 0-based або -1
  const vals = sh.getRange(2,1,rows,lastCol).getValues();

  const players = [];
  for (let i=0;i<vals.length;i++){
    const row = vals[i];
    const id = row[0];
    const name = row[1];
    const rating = row[2];
    if (!name) continue;
    const activeCell = (idxActive >= 0) ? row[idxActive] : true;
    const active = (String(activeCell).toLowerCase() !== 'false') && (activeCell !== false) && (activeCell !== '');
    players.push({
      id: id || (i+1),
      name: String(name),
      rating: Number(rating||0),
      active
    });
    if (!id) sh.getRange(2+i,1).setValue(i+1);
  }
  return players;
}

function readRounds(sh) {
  const rows = Math.max(0, sh.getLastRow()-1);
  if (!rows) return [];
  const cols = Math.min(12, sh.getLastColumn()); // на випадок, якщо старий хедер
  const vals = sh.getRange(2,1,rows,cols).getValues();
  const res = [];
  for (const r of vals) {
    const [
      round,board,
      aId,aName,bId,bName,
      vpA,vpB,
      sa,sb,
      starter,
      note
    ] = [
      r[0], r[1],
      r[2], r[3], r[4], r[5],
      r[6], r[7],
      r[8], r[9],
      r[10],
      r[11]
    ];
    if (!round) continue;
    res.push({
      round: Number(round),
      board: Number(board||0),
      playerA_id: aId || null,
      playerA_name: aName || '',
      playerB_id: bId || null,
      playerB_name: bName || '',
      vpA: (vpA===''||vpA===null) ? null : Number(vpA),
      vpB: (vpB===''||vpB===null) ? null : Number(vpB),
      scoreA: (sa===''||sa===null)? null : Number(sa),
      scoreB: (sb===''||sb===null)? null : Number(sb),
      starter: (starter||'').toString().trim().toUpperCase()==='B' ? 'B' : ( (starter||'A') ? 'A':'A' ),
      note: note||''
    });
  }
  return res;
}

function computeStandings(players, rounds, cfg, maxRound) {
  const idStr = x => String(x);
  const wins = new Map(players.map(p => [idStr(p.id), 0]));
  const opps = new Map(players.map(p => [idStr(p.id), []]));
  const byes = new Map(players.map(p => [idStr(p.id), 0]));
  const vpDiff = new Map(players.map(p => [idStr(p.id), 0]));
  const sbTemp = new Map(players.map(p => [idStr(p.id), 0]));

  for (const m of rounds) {
    if (maxRound != null && m.round > maxRound) continue; // ⬅️ ключова лінія

    const a = m.playerA_id ? idStr(m.playerA_id) : null;
    const b = m.playerB_id ? idStr(m.playerB_id) : null;

    if (a && b) {
      let wa = null, wb = null;
      const vpa = (m.vpA===''||m.vpA===null) ? null : Number(m.vpA);
      const vpb = (m.vpB===''||m.vpB===null) ? null : Number(m.vpB);

      if (vpa !== null && vpb !== null) {
        if (vpa > vpb) { wa = 1; wb = 0; }
        else if (vpb > vpa) { wa = 0; wb = 1; }
        else {
          const st = (m.starter || 'A') === 'B' ? 'B' : 'A';
          if (st === 'A') { wa = 0; wb = 1; } else { wa = 1; wb = 0; }
        }
        vpDiff.set(a, (vpDiff.get(a)||0) + (vpa - vpb));
        vpDiff.set(b, (vpDiff.get(b)||0) + (vpb - vpa));
      } else {
        const sa = (m.scoreA===''||m.scoreA===null) ? null : Number(m.scoreA);
        const sb = (m.scoreB===''||m.scoreB===null) ? null : Number(m.scoreB);
        if (sa !== null && sb !== null) { wa = sa>sb?1:(sb>sa?0:null); wb = sa>sb?0:(sb>sa?1:null); }
      }

      if (wa !== null && wb !== null) {
        wins.set(a, (wins.get(a)||0) + wa);
        wins.set(b, (wins.get(b)||0) + wb);
        opps.get(a).push(b);
        opps.get(b).push(a);

        const pa = wins.get(a)||0, pb = wins.get(b)||0;
        sbTemp.set(a, (sbTemp.get(a)||0) + pb * wa);
        sbTemp.set(b, (sbTemp.get(b)||0) + pa * wb);
      }
    } else if (a && !b) {
      wins.set(a, (wins.get(a)||0) + Number(cfg.bye||1));
      byes.set(a, (byes.get(a)||0)+1);
    } else if (b && !a) {
      wins.set(b, (wins.get(b)||0) + Number(cfg.bye||1));
      byes.set(b, (byes.get(b)||0)+1);
    }
  }

  // Solkoff/Buchholz
  const solk1 = new Map(); // мінус найслабший
  const solk2 = new Map(); // мінус найсильніший і найслабший
  const buchholz = new Map(); // plain Buchholz

  for (const p of players) {
    const pid = idStr(p.id);

    // 1) очки опонентів
    const opponents = (opps.get(pid) || []);
    const oppWins = opponents.map(o => wins.get(o) || 0);

    // 2) кожен BYE = 0 (найслабший суперник)
    const byeCnt = byes.get(pid) || 0;
    for (let k = 0; k < byeCnt; k++) oppWins.push(0);

    // plain Buchholz — сума
    const sum = oppWins.reduce((s,v)=>s+v,0);
    buchholz.set(pid, sum);

    // Median-Buchholz (Solkoff1)
    if (oppWins.length >= 2) {
      const sorted = oppWins.slice().sort((a,b)=>a-b);
      const sum1 = sorted.slice(1).reduce((s,v)=>s+v,0); // відкинули min
      solk1.set(pid, sum1);
    } else {
      solk1.set(pid, 0);
    }

    // Modified Median-Buchholz (Solkoff2)
    if (oppWins.length >= 3) {
      const sorted = oppWins.slice().sort((a,b)=>a-b);
      const sum2 = sorted.slice(1, sorted.length-1).reduce((s,v)=>s+v,0); // відкинули min і max
      solk2.set(pid, sum2);
    } else {
      solk2.set(pid, 0); // ← доки не зіграно щонайменше 3, значення 0
    }
  }

  const startSeqs = buildStartSequences(players, rounds, maxRound);

  const table = players.map(p => ({
    id: p.id,
    name: p.name,
    rating: p.rating||0,
    wins: Number((wins.get(idStr(p.id))||0).toFixed(3)),
    solkoff1: Number((solk1.get(idStr(p.id))||0).toFixed(3)),
    solkoff2: Number((solk2.get(idStr(p.id))||0).toFixed(3)),
    vp_diff: Number((vpDiff.get(idStr(p.id))||0).toFixed(3)),
    sb: Number((sbTemp.get(idStr(p.id))||0).toFixed(3)),
    byes: Number(byes.get(idStr(p.id))||0),
    start_seq: startSeqs.get(String(p.id)) || ''
  }));

  return table;
}

function rebuildStandingsInternal(state) {
  syncScoresFromVPInRounds(state.shRounds);
  const allRounds = readRounds(state.shRounds);
  const maxRoundCap = state.currentRound; // рахуємо тільки завершені раунди
  const standingsRaw = computeStandings(state.players, allRounds, state.config, maxRoundCap);

  const standings = standingsRaw.slice().sort(compareSwissRows);

  state.standings = standings;

  const sh = state.shStandings;
  ensureStandingsHeader(sh);
  safeClearBelowHeader(sh);

  if (!standings.length) return;
  const out = standings.map((r, i) => [
    i+1, r.id, r.name, r.rating,
    r.wins, r.solkoff1, r.solkoff2, r.vp_diff, r.sb, r.byes,
    r.start_seq
  ]);
  sh.getRange(2,1,out.length,out[0].length).setValues(out);
}

function swiss_rebuildStandings() {
  const state = loadState();
  rebuildStandingsInternal(state);
  SpreadsheetApp.getUi().alert('Standings/Swiss оновлено.');
}

function getStartCounts(rounds){
  const m = new Map();
  for (const r of rounds) {
    if (r.playerA_id && r.playerB_id) {
      const who = (r.starter || 'A') === 'B' ? 'B' : 'A';
      const pid = (who==='A') ? String(r.playerA_id) : String(r.playerB_id);
      m.set(pid, (m.get(pid)||0) + 1);
    }
  }
  return m;
}

function writePairings(state, roundNo, pairings) {
  const activeSet = new Set(state.players.filter(p => p.active !== false).map(p => String(p.id)));
  const activeCount = activeSet.size;

  const explicitByes = pairings.filter(p => !(p.a && p.b));
  const allowedByes  = (activeCount % 2 === 1) ? 1 : 0;

  if (explicitByes.length > allowedByes) {
    SpreadsheetApp.getUi().alert(
      'Отримано ' + explicitByes.length + ' BYE при дозволених ' + allowedByes + '. Запис скасовано.'
    );
    return;
  }

  if (allowedByes === 1 && explicitByes.length === 0) {
    // додамо рівно один BYE з урахуванням поточного розкладу
    const paired = new Set();
    for (const p of pairings) { if (p.a) paired.add(String(p.a.id)); if (p.b) paired.add(String(p.b.id)); }
    const rankIndex = new Map((state.standings || []).map((s, idx) => [String(s.id), idx]));
    const getByes = (id) => state.byeCount.get(String(id)) || 0;
    const unpaired = state.players.filter(p => p.active !== false && !paired.has(String(p.id)));
    if (unpaired.length) {
      const byePick = pickBye(unpaired, rankIndex, getByes, state.rounds, state.currentRound);
      if (byePick) pairings.push({ a: byePick, b: null, note: 'BYE' });
    }
  }

  const normalPairs = pairings.filter(p => p.a && p.b);
  const byePairs    = pairings.filter(p => !(p.a && p.b));
  const ordered     = normalPairs.concat(byePairs);

  if (ordered.filter(p => !(p.a && p.b)).length > allowedByes) {
    SpreadsheetApp.getUi().alert('Аномалія: більше одного BYE при записі. Запис скасовано.');
    return;
  }

  const sh = state.shRounds;
  const currentLast = sh.getLastRow();
  let startRow = currentLast + 1;
  let board = 1;
  const rows = [];
  for (const p of ordered) {
    if (p.a && p.b) {
      rows.push([ roundNo, board++, p.a.id, p.a.name, p.b.id, p.b.name, '', '', '', '', 'A', p.note || '' ]);
    } else {
      const only = p.a || p.b;
      rows.push([ roundNo, board++, only ? only.id : '', only ? only.name : '', '', '', '', '', '', '', '', 'BYE' ]);
    }
  }
  if (rows.length) sh.getRange(startRow,1,rows.length,12).setValues(rows);
}

function normalPairKey(a,b) {
  const s1 = String(a), s2 = String(b);
  return (s1 < s2) ? (s1+'__'+s2) : (s2+'__'+s1);
}

/** -------- Pairing algorithms -------- */
function pairFirstRound(sortedPlayers, state) {
  const res = [];
  const n = sortedPlayers.length;
  let byePlayer = null;
  if (n % 2 === 1) {
    // найнижчий за рейтингом
    byePlayer = sortedPlayers.pop();
  }
  const half = sortedPlayers.length / 2;
  const top = sortedPlayers.slice(0, half);
  const bottom = sortedPlayers.slice(half);
  for (let i=0;i<top.length;i++) res.push({ a: top[i], b: bottom[i] });
  if (byePlayer) res.push({ a: byePlayer, b: null, note: 'BYE' }); // ← в самий кінець
  return res;
}

// Наступні раунди: greedy з FIDE-подібними обмеженнями
function pairSwissGreedy(standingsSorted, state) {
  const rankIndex = new Map(standingsSorted.map((s, idx) => [String(s.id), idx]));
  const getByes   = (id) => state.byeCount.get(String(id)) || 0;
  const played    = state.playedPairs;

  const pool = standingsSorted.map(s => ({ id:s.id, name:s.name, rating:s.rating||0, wins:s.wins }));

  const baseStats = computeSideStats(state.players, state.rounds);
  const liveStats = new Map(Array.from(baseStats.entries()).map(([k,v]) => [k, Object.assign({}, v)]));

  // групи за очками
  const groups = new Map();
  for (const p of pool){
    const key = Number(p.wins);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(p);
  }
  const winsLevels = Array.from(groups.keys()).sort((a,b)=>b-a);

  const roundUsed = new Set();
  const carry = [];
  const carrySet = new Set();
  const pairs = [];

  const K_LOOK = 8;     // ширше вікно для перших спроб
  const K_TAIL = 4;     // скільки «з хвоста» пробуємо всередині одного списку

  const notPlayedBefore = (a,b) => !played.has(normalPairKey(a,b));

  function pushPairSafe(a, b, orient, note) {
    if (roundUsed.has(a.id) || roundUsed.has(b.id)) return false;
    if (orient === 'pA') { pairs.push({ a, b, note }); applyOrientation(liveStats, a.id, b.id, 'pA'); }
    else                 { pairs.push({ a:b, b:a, note }); applyOrientation(liveStats, a.id, b.id, 'pB'); }
    roundUsed.add(a.id); roundUsed.add(b.id);
    return true;
  }

  function tryPair(a, b, cfg){
    const pStat = liveStats.get(String(a.id));
    const qStat = liveStats.get(String(b.id));
    if (!cfg.allowRematch && !notPlayedBefore(a.id,b.id)) return false;

    const choice = chooseBestOrientation(pStat, qStat, cfg.allowTriple, cfg.allowCapBreak);
    if (!choice.ok) return false;

    let note;
    if (cfg.logForces) {
      const v = inferViolations(pStat, qStat, choice.orient);
      const isRematch = !notPlayedBefore(a.id,b.id);
      const parts=[]; if (isRematch) parts.push('rematch'); if (v.length) parts.push(...v);
      if (parts.length) note = 'FORCED: ' + parts.join(', ');
    }
    return pushPairSafe(a, b, choice.orient, note);
  }

  function tryPairOrient(a, b, orient, cfg){
    if (!cfg.allowRematch && !notPlayedBefore(a.id,b.id)) return false;
    const pStat = liveStats.get(String(a.id));
    const qStat = liveStats.get(String(b.id));
    const ok =
      (orient==='pA' &&
        orientationOk(pStat,'A',cfg.allowTriple,cfg.allowCapBreak) &&
        orientationOk(qStat,'B',cfg.allowTriple,cfg.allowCapBreak)) ||
      (orient==='pB' &&
        orientationOk(pStat,'B',cfg.allowTriple,cfg.allowCapBreak) &&
        orientationOk(qStat,'A',cfg.allowTriple,cfg.allowCapBreak));
    if (!ok) return false;

    let note;
    if (cfg.logForces) {
      const v = inferViolations(pStat, qStat, orient);
      const isRematch = !notPlayedBefore(a.id,b.id);
      const parts=[]; if (isRematch) parts.push('rematch'); if (v.length) parts.push(...v);
      if (parts.length) note = 'FORCED: ' + parts.join(', ');
    }
    return pushPairSafe(a,b,orient,note);
  }

  function pushCarry(p, want){
    if (roundUsed.has(p.id)) return;
    if (carrySet.has(p.id)) return;
    carrySet.add(p.id);
    carry.push({ p, want });
  }

  // «малі перестановки»: верхній елемент list пробуємо спарити з одним із K_TAIL «хвостових»
  function pairInsideSameListReturn(list, sideWant) {
    const leftover = [];
    let used = new Array(list.length).fill(false);
    let i = 0, j = list.length - 1;
    while (i < j) {
      while (i < list.length && (used[i] || roundUsed.has(list[i].id))) i++;
      while (j >= 0 && (used[j] || roundUsed.has(list[j].id))) j--;
      if (i >= j) break;

      const p = list[i];
      let paired = false;
      const maxTail = Math.max(i+1, j - (K_TAIL-1)); // пробуємо з j, j-1, ...
      for (let idx = j; idx >= maxTail; idx--) {
        if (used[idx] || roundUsed.has(list[idx].id)) continue;
        const q = list[idx];
        const orient = (sideWant === 'A') ? 'pA' : 'pB';
        if (tryPairOrient(p, q, orient, {allowTriple:false, allowCapBreak:false, allowRematch:false, logForces:false})) {
          used[i] = true; used[idx] = true;
          paired = true;
          if (idx === j) j--; // зсунули хвіст
          break;
        }
      }
      if (!paired) {
        leftover.push(p);
        used[i] = true;
      }
      i++;
    }
    // незадіяні середні — у leftover
    for (let k = 0; k < list.length; k++) {
      if (!used[k] && !roundUsed.has(list[k].id)) leftover.push(list[k]);
    }
    return leftover;
  }

  function splitByWant(group, liveStats){
    const wantA = [], wantB = [];
    for (const p of group){
      const st = liveStats.get(String(p.id)) || {last:null, streak:0, must:null};
      const want = st.must ? st.must : (st.last==='A' ? 'B' : 'A');
      if (want === 'A') wantA.push(p); else wantB.push(p);
    }
    return { wantA, wantB };
  }

  // ===== 1) Основний прохід по групах =====
  for (const pts of winsLevels){
    let G = groups.get(pts).filter(x => !roundUsed.has(x.id));
    if (!G.length) continue;

    // обслуговуємо carry з попередньої групи
    if (carry.length){
      const nextCarry = [];
      for (const item of carry){
        const { wantA, wantB } = splitByWant(G, liveStats);
        const candidates = (item.want === 'A') ? wantB : wantA;
        let matched = false;
        for (let k=0; k<Math.min(K_LOOK, candidates.length); k++){
          const q = candidates[k];
          if (roundUsed.has(q.id)) continue;
          if (tryPair(item.p, q, {allowTriple:false, allowCapBreak:false, allowRematch:false, logForces:false})) {
            const idx = G.findIndex(x => x.id === q.id);
            if (idx >= 0) G.splice(idx,1);
            matched = true; break;
          }
        }
        if (!matched) nextCarry.push(item);
      }
      carry.length = 0; carrySet.clear();
      for (const x of nextCarry){ pushCarry(x.p, x.want); }
      G = G.filter(x => !roundUsed.has(x.id));
      if (!G.length) continue;
    }

    // внутрішньогрупове парування
    const { wantA, wantB } = splitByWant(G, liveStats);

    // 1) крос-пари A↔B (топ-K)
    const usedB = new Set();
    const leftA = [];
    for (let i=0; i<wantA.length; i++){
      const p = wantA[i];
      if (roundUsed.has(p.id)) continue;
      let paired = false;
      for (let k=0; k<Math.min(K_LOOK, wantB.length); k++){
        const q = wantB[k];
        if (usedB.has(q.id) || roundUsed.has(q.id)) continue;
        if (tryPair(p,q,{allowTriple:false, allowCapBreak:false, allowRematch:false, logForces:false})) {
          usedB.add(q.id);
          paired = true; break;
        }
      }
      if (!paired) leftA.push(p);
    }
    const leftB = wantB.filter(q => !usedB.has(q.id) && !roundUsed.has(q.id));

    // 2) перестановки всередині одного списку (верхній із одним із K_TAIL останніх)
    const leftAfterA = (leftA.length >= 2) ? pairInsideSameListReturn(leftA, 'A') : leftA.slice();
    const leftAfterB = (leftB.length >= 2) ? pairInsideSameListReturn(leftB, 'B') : leftB.slice();

    // 3) те, що лишилось — у carry
    for (const p of leftAfterA) if (!roundUsed.has(p.id) && !carrySet.has(p.id)) pushCarry(p, 'A');
    for (const q of leftAfterB) if (!roundUsed.has(q.id) && !carrySet.has(q.id)) pushCarry(q, 'B');
  }

  // ===== 2) Залишки + BYE =====
  const remainMap = new Map();
  for (const p of pool) if (!roundUsed.has(p.id)) remainMap.set(p.id, p);
  for (const item of carry) if (!roundUsed.has(item.p.id)) remainMap.set(item.p.id, item.p);
  let remain = Array.from(remainMap.values());

  let byeEntry = null;
  if (remain.length % 2 === 1) {
    byeEntry = pickBye(remain, rankIndex, getByes, state.rounds, state.currentRound);
    remain = remain.filter(p => p.id !== byeEntry.id);
  }

  // ===== 3) Багатоступеневий фолбек =====
  const passes = [
    {allowTriple:false, allowCapBreak:false, allowRematch:false, logForces:false},
    {allowTriple:false, allowCapBreak:true,  allowRematch:false, logForces:true },
    {allowTriple:false, allowCapBreak:true,  allowRematch:true,  logForces:true },
  ];

  for (const cfg of passes){
    if (remain.length < 2) break;
    const next = [];
    const usedLocal = new Set();
    for (let i=0;i<remain.length;i++){
      if (usedLocal.has(i)) continue;
      let paired = false;
      for (let j=i+1;j<remain.length;j++){
        if (usedLocal.has(j)) continue;
        const a = remain[i], b = remain[j];
        if (tryPair(a,b,cfg)) { usedLocal.add(i); usedLocal.add(j); paired = true; break; }
      }
      if (!paired) next.push(remain[i]);
    }
    remain = next;
  }

  // ===== 4) Останній truly-forced крок =====
  if (remain.length >= 2){
    const cfg = {allowTriple:true, allowCapBreak:true, allowRematch:true, logForces:true};
    const next = [];
    const usedLocal = new Set();
    for (let i=0;i<remain.length;i++){
      if (usedLocal.has(i)) continue;
      let paired = false;
      for (let j=i+1;j<remain.length;j++){
        if (usedLocal.has(j)) continue;
        const a = remain[i], b = remain[j];
        if (tryPair(a,b,cfg)) { usedLocal.add(i); usedLocal.add(j); paired = true; break; }
      }
      if (!paired) next.push(remain[i]);
    }
    remain = next;
  }

  if (remain.length === 1 && !byeEntry) {
    byeEntry = remain[0];
    remain = [];
  }
  if (byeEntry) pairs.push({ a: byeEntry, b: null, note: 'BYE' });

  return pairs;
}


// helper: розбити групу за "бажаним" кольором поточного туру
function splitByWant(group, liveStats){
  const wantA = [], wantB = [];
  for (const p of group){
    const st = liveStats.get(String(p.id)) || {last:null, streak:0, must:null};
    // якщо must існує — він переважає
    const want = st.must ? st.must : (st.last==='A' ? 'B' : 'A');
    if (want === 'A') wantA.push(p); else wantB.push(p);
  }
  return { wantA, wantB };
}

function safeClearBelowHeader(sh) {
  const lastRow = sh.getLastRow();
  if (lastRow > 1) {
    sh.getRange(2, 1, lastRow - 1, sh.getLastColumn()).clearContent();
  }
}

function opposite(side){ return side === 'A' ? 'B' : 'A'; }

function computeSideStats(players, rounds){
  const init = () => ({A:0,B:0,last:null,streak:0, pref:null, must:null});
  const stats = new Map(players.map(p => [String(p.id), init()]));

  for (const m of rounds){
    const a = m.playerA_id ? String(m.playerA_id) : null;
    const b = m.playerB_id ? String(m.playerB_id) : null;

    if (a && b){
      const sa = stats.get(a) || init();
      const sb = stats.get(b) || init();
      // A-сторона
      sa.A += 1; sa.streak = (sa.last==='A') ? sa.streak+1 : 1; sa.last='A';
      // B-сторона
      sb.B += 1; sb.streak = (sb.last==='B') ? sb.streak+1 : 1; sb.last='B';
      stats.set(a, sa); stats.set(b, sb);
    } else if (a && !b){
      // BYE у колонки A → рахуємо як «A»
      const sa = stats.get(a) || init();
      sa.A += 1; sa.streak = (sa.last==='A') ? sa.streak+1 : 1; sa.last='A';
      stats.set(a, sa);
    } else if (b && !a){
      // BYE у колонки B → теж рахуємо як «A» (грав “першим”)
      const sb = stats.get(b) || init();
      sb.A += 1; sb.streak = (sb.last==='A') ? sb.streak+1 : 1; sb.last='A';
      stats.set(b, sb);
    }
  }

  // преференції та "must" (щоб не було 3 в ряд)
  stats.forEach(v => {
    v.pref = (v.A > v.B) ? 'B' : (v.B > v.A) ? 'A' : null;
    v.must = (v.streak >= 2 && v.last) ? (v.last==='A' ? 'B' : 'A') : null;
  });
  return stats;
}

function orientationOk(stat, side, allowTriple, allowCapBreak, cap=2){
  if (!stat) return true;
  if (!allowTriple && stat.last === side && stat.streak >= 2) return false;
  if (!allowCapBreak) {
    const diff = stat.A - stat.B;
    const newDiff = (side === 'A') ? diff + 1 : diff - 1;
    if (Math.abs(newDiff) > cap) return false;
  }
  return true;
}

function orientationPenalty(stat, side){
  if (!stat) return 0;
  let p = 0;
  if (stat.pref && stat.pref !== side) p += 1;
  const diff = (stat.A - stat.B);
  if ((side==='A' && diff >= 2) || (side==='B' && diff <= -2)) p += 0.5;
  return p;
}

function chooseBestOrientation(pStat, qStat, allowTriple, allowCapBreak){
  const opt1ok = orientationOk(pStat,'A',allowTriple,allowCapBreak) && orientationOk(qStat,'B',allowTriple,allowCapBreak);
  const opt2ok = orientationOk(pStat,'B',allowTriple,allowCapBreak) && orientationOk(qStat,'A',allowTriple,allowCapBreak);
  if (!opt1ok && !opt2ok) return {ok:false};
  const pen1 = opt1ok ? (orientationPenalty(pStat,'A') + orientationPenalty(qStat,'B')) : 1e9;
  const pen2 = opt2ok ? (orientationPenalty(pStat,'B') + orientationPenalty(qStat,'A')) : 1e9;
  return (pen1 <= pen2) ? {ok:true, orient:'pA', penalty:pen1} : {ok:true, orient:'pB', penalty:pen2};
}

function applyOrientation(stats, pId, qId, orient){
  const aId = orient==='pA' ? String(pId) : String(qId);
  const bId = orient==='pA' ? String(qId) : String(pId);
  const upd = (s, side) => {
    if (side==='A'){ s.A += 1; s.streak = (s.last==='A') ? s.streak+1 : 1; s.last='A'; }
    else { s.B += 1; s.streak = (s.last==='B') ? s.streak+1 : 1; s.last='B'; }
    s.pref = (s.A > s.B) ? 'B' : (s.B > s.A) ? 'A' : null;
    s.must = (s.streak >= 2 && s.last) ? opposite(s.last) : null;
  };
  const sa = (stats.get(aId) || {A:0,B:0,last:null,streak:0,pref:null,must:null});
  const sb = (stats.get(bId) || {A:0,B:0,last:null,streak:0,pref:null,must:null});
  upd(sa,'A'); upd(sb,'B');
  stats.set(aId, sa); stats.set(bId, sb);
}

function ensureStandingsHeader(sh) {
  const header = [
    'Rank','ID','Name','Rating',
    'Wins','Solkoff1','Solkoff2','VP_Diff','SB','Byes',
    'StartSeq'
  ];
  sh.getRange(1,1,1,header.length).setValues([header]);
  sh.setFrozenRows(1);
}

function syncScoresFromVPInRounds(shRounds) {
  const lastRow = shRounds.getLastRow();
  if (lastRow <= 1) return 0;

  const rows = lastRow - 1;
  const range = shRounds.getRange(2, 1, rows, Math.min(12, shRounds.getLastColumn()));
  const data = range.getValues();

  // 1-based колонки відповідно до нашого хедера Rounds
  const COL_A_ID = 3, COL_B_ID = 5;
  const COL_VPA  = 7, COL_VPB  = 8;
  const COL_SA   = 9, COL_SB   = 10;
  const COL_ST   = 11;

  let updates = 0;

  for (let i = 0; i < data.length; i++) {
    const row = data[i];
    const round = row[0];
    if (!round) continue;

    const aId = row[COL_A_ID - 1];
    const bId = row[COL_B_ID - 1];

    // BYE → поставимо перемогу тому, хто є
    if (aId && !bId) {
      if (row[COL_SA - 1] !== 1 || row[COL_SB - 1] !== '') {
        row[COL_SA - 1] = 1; row[COL_SB - 1] = '';
        updates++;
      }
      continue;
    }
    if (bId && !aId) {
      if (row[COL_SA - 1] !== '' || row[COL_SB - 1] !== 1) {
        row[COL_SA - 1] = ''; row[COL_SB - 1] = 1;
        updates++;
      }
      continue;
    }

    const vpA = row[COL_VPA - 1], vpB = row[COL_VPB - 1];
    const hasVPA = !(vpA === '' || vpA === null);
    const hasVPB = !(vpB === '' || vpB === null);
    if (!hasVPA || !hasVPB) continue;

    const vpa = Number(vpA), vpb = Number(vpB);
    if (isNaN(vpa) || isNaN(vpb)) continue;

    let sa = row[COL_SA - 1], sb = row[COL_SB - 1];

    if (vpa > vpb) { sa = 1; sb = 0; }
    else if (vpb > vpa) { sa = 0; sb = 1; }
    else {
      const starter = String(row[COL_ST - 1] || 'A').trim().toUpperCase() === 'B' ? 'B' : 'A';
      if (starter === 'A') { sa = 0; sb = 1; }
      else { sa = 1; sb = 0; }
    }

    if (sa !== row[COL_SA - 1] || sb !== row[COL_SB - 1]) {
      row[COL_SA - 1] = sa;
      row[COL_SB - 1] = sb;
      updates++;
    }
  }

  if (updates > 0) range.setValues(data);
  return updates;
}

// F = стартував першим, S = другим, B = bye
function buildStartSequences(players, rounds, maxRound) {
  const seq = new Map(players.map(p => [String(p.id), []]));
  const sorted = rounds.slice().sort((a,b) => (a.round||0) - (b.round||0));
  for (const m of sorted) {
    if (maxRound != null && m.round > maxRound) break; // рахуємо тільки завершені
    const starter = (m.starter || 'A').toString().trim().toUpperCase() === 'B' ? 'B' : 'A';
    if (m.playerA_id && m.playerB_id) {
      const a = String(m.playerA_id), b = String(m.playerB_id);
      seq.get(a).push(starter === 'A' ? 'F' : 'S');
      seq.get(b).push(starter === 'B' ? 'F' : 'S');
    } else if (m.playerA_id && !m.playerB_id) {
      seq.get(String(m.playerA_id)).push('B');
    } else if (m.playerB_id && !m.playerA_id) {
      seq.get(String(m.playerB_id)).push('B');
    }
  }
  const out = new Map();
  for (const [pid, arr] of seq.entries()) out.set(pid, arr.join(''));
  return out;
}

function lastCompletedRoundNumber(rounds) {
  const byRound = new Map();
  for (const m of rounds) {
    if (!m.round) continue;
    if (!byRound.has(m.round)) byRound.set(m.round, []);
    byRound.get(m.round).push(m);
  }
  const nums = Array.from(byRound.keys()).sort((a,b)=>a-b);
  let last = 0;
  for (const r of nums) {
    const matches = byRound.get(r);
    const allDone = matches.every(m => {
      if (m.playerA_id && m.playerB_id) {
        const vpDone = m.vpA !== null && m.vpB !== null;
        const scDone = m.scoreA !== null && m.scoreB !== null;
        return vpDone || scDone;
      }
      // BYE-рядок не блокує завершення — раунд вважаємо завершеним,
      // якщо всі ігрові столи (A&B) мають результат
      return true;
    });
    if (allDone) last = r; else break;
  }
  return last;
}

function ensurePlayersActiveColumn() {
  const sh = SpreadsheetApp.getActive().getSheetByName('Players');
  if (!sh) return;
  const lastCol = sh.getLastColumn();
  if (lastCol === 0) return;
  const headers = sh.getRange(1, 1, 1, lastCol).getValues()[0].map(String);
  if (!headers.some(h => h.trim().toLowerCase() === 'active')) {
    const rows = Math.max(0, sh.getLastRow() - 1);
    if (lastCol < 4) sh.insertColumnsAfter(lastCol, 4 - lastCol);
    sh.getRange(1, 4).setValue('Active');
    if (rows > 0) {
      const names = sh.getRange(2, 2, rows, 1).getValues().map(r => r[0]);
      const vals = names.map(n => [ n ? true : '' ]);
      sh.getRange(2, 4, rows, 1).setValues(vals);
    }
  }
}

function compareSwissRows(a, b){
  // 1) головні тай-брейки
  if (b.wins      !== a.wins)      return b.wins      - a.wins;
  if (b.solkoff1  !== a.solkoff1)  return b.solkoff1  - a.solkoff1;
  if (b.solkoff2  !== a.solkoff2)  return b.solkoff2  - a.solkoff2;
  if (b.vp_diff   !== a.vp_diff)   return b.vp_diff   - a.vp_diff;
  if (b.sb        !== a.sb)        return b.sb        - a.sb;

  // 2) усередині «повної рівності» — без bye попереду, з bye позаду
  const aHasBye = (a.byes||0) > 0, bHasBye = (b.byes||0) > 0;
  if (aHasBye !== bHasBye) return aHasBye ? 1 : -1;

  // 3) остаточно — ID за зростанням
  return Number(a.id) - Number(b.id);
}

// ВИБІР BYE: спершу ті, хто ще не отримували, далі мін. кількість BYE,
// серед них — найнижчий у поточному ранзі (largest rankIndex)
function pickBye(candidates, rankIndex, getByes, rounds, lastRound){
  // кандидати, що мали BYE в останньому завершеному раунді — в кінець
  const recentBye = new Set();
  for (const m of rounds) {
    if (m.round === lastRound) {
      if (m.playerA_id && !m.playerB_id) recentBye.add(String(m.playerA_id));
      if (m.playerB_id && !m.playerA_id) recentBye.add(String(m.playerB_id));
    }
  }

  const byesArr = candidates.map(p => getByes(p.id));
  const minByes = Math.min(...byesArr);

  // спершу — без недавнього BYE
  let pool = candidates.filter(p => getByes(p.id) === minByes && !recentBye.has(String(p.id)));

  // якщо всі мали нещодавній BYE — доведеться з ними
  if (!pool.length) pool = candidates.filter(p => getByes(p.id) === minByes);

  // серед рівних — найнижчий у поточному ранзі (largest rankIndex)
  pool.sort((a,b) => ((rankIndex.get(String(b.id)) ?? 1e9) - (rankIndex.get(String(a.id)) ?? 1e9)));
  return pool[0] || null;
}

function inferViolations(pStat, qStat, orient, cap=2){
  const flags = [];
  const aSide = (orient === 'pA') ? 'A' : 'B';
  const bSide = (orient === 'pA') ? 'B' : 'A';

  if (pStat) {
    const tripleA = (pStat.last === aSide && (pStat.streak || 0) >= 2);
    const diffA   = (pStat.A || 0) - (pStat.B || 0);
    const newDiffA = (aSide === 'A') ? diffA + 1 : diffA - 1;
    const capA = Math.abs(newDiffA) > cap;
    if (tripleA) flags.push('color-3-in-a-row (A)');
    if (capA)    flags.push('color-cap>2 (A)');
  }
  if (qStat) {
    const tripleB = (qStat.last === bSide && (qStat.streak || 0) >= 2);
    const diffB   = (qStat.A || 0) - (qStat.B || 0);
    const newDiffB = (bSide === 'A') ? diffB + 1 : diffB - 1;
    const capB = Math.abs(newDiffB) > cap;
    if (tripleB) flags.push('color-3-in-a-row (B)');
    if (capB)    flags.push('color-cap>2 (B)');
  }
  return flags;
}

function swiss_withdrawById(){
  const ui = SpreadsheetApp.getUi();
  const res = ui.prompt('Withdraw by ID', 'Введіть ID гравця, якого потрібно зняти (Active = FALSE):', ui.ButtonSet.OK_CANCEL);
  if (res.getSelectedButton() !== ui.Button.OK) return;
  const id = String(res.getResponseText()||'').trim();
  if (!id) return;
  const sh = SpreadsheetApp.getActive().getSheetByName('Players');
  if (!sh) return;
  const last = sh.getLastRow();
  if (last <= 1) return;
  // шукаємо колонку Active
  const headers = sh.getRange(1,1,1,sh.getLastColumn()).getValues()[0].map(String);
  let colActive = headers.findIndex(h => h.trim().toLowerCase()==='active') + 1;
  if (!colActive) { sh.getRange(1,4).setValue('Active'); colActive = 4; }
  const rng = sh.getRange(2,1,last-1,Math.max(colActive,4));
  const vals = rng.getValues();
  for (let i=0;i<vals.length;i++){
    if (String(vals[i][0])===id){ sh.getRange(2+i,colActive).setValue(false); ui.alert('Гравця знято.'); return; }
  }
  ui.alert('ID не знайдено.');
}

function requireNoPendingRound(state) {
  // будь-які рядки з номером раунду > завершеного = незакритий раунд
  const pending = state.rounds.filter(m => m.round > state.currentRound);
  if (!pending.length) return true;

  const nextR = Math.min.apply(null, pending.map(m => m.round));
  const boardsOpen = pending
    .filter(m => m.round === nextR && m.playerA_id && m.playerB_id)
    .filter(m => {
      const vpDone = m.vpA !== null && m.vpB !== null;
      const scDone = m.scoreA !== null && m.scoreB !== null;
      return !(vpDone || scDone);
    })
    .map(m => m.board)
    .filter(b => b !== 0);

  SpreadsheetApp.getUi().alert(
    'Раунд ' + nextR + ' уже згенеровано і ще не завершено.\n' +
    (boardsOpen.length ? ('Незакриті столи: ' + boardsOpen.join(', ')) : 'Є незакриті рядки без результатів.') +
    '\nСпершу внесіть результати або очистьте цей раунд.'
  );
  return false;
}