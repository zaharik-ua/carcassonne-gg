function swiss_withdrawSelected() {
  const ss = SpreadsheetApp.getActive();
  const sh = ss.getSheetByName('Players');
  const rng = sh.getActiveCell();
  if (!rng || rng.getRow() <= 1) {
    SpreadsheetApp.getUi().alert('Виділіть рядок гравця на аркуші Players.');
    return;
  }
  const row = rng.getRow();
  const id = sh.getRange(row, 1).getValue();
  if (!id) { SpreadsheetApp.getUi().alert('Немає ID у цьому рядку.'); return; }
  withdrawById(String(id));
}

function swiss_withdrawById() {
  const ui = SpreadsheetApp.getUi();
  const resp = ui.prompt('Withdraw player', 'Введіть ID гравця, який знімається:', ui.ButtonSet.OK_CANCEL);
  if (resp.getSelectedButton() !== ui.Button.OK) return;
  const id = resp.getResponseText().trim();
  if (!id) return;
  withdrawById(String(id));
}

function withdrawById(idStr) {
  const ss = SpreadsheetApp.getActive();
  const shPlayers = ss.getSheetByName('Players');
  ensurePlayersActiveColumn();
  const lastRow = shPlayers.getLastRow();
  if (lastRow <= 1) return;
  const lastCol = shPlayers.getLastColumn();
  const headers = shPlayers.getRange(1,1,1,lastCol).getValues()[0].map(String);
  const idxActive = headers.findIndex(h => h.trim().toLowerCase() === 'active');
  if (idxActive < 0) return;

  // Знайдемо рядок з цим ID
  const ids = shPlayers.getRange(2,1,lastRow-1,1).getValues().map(r => String(r[0]));
  const rowIdx = ids.findIndex(x => x === idStr);
  if (rowIdx < 0) { SpreadsheetApp.getUi().alert('ID не знайдено в Players.'); return; }
  // Позначимо неактивним
  shPlayers.getRange(2+rowIdx, 1+idxActive).setValue(false);

  // Форфіт у відкритому раунді (якщо гравець уже розписаний)
  forfeitOpenRoundIfPaired(idStr);

  // Перерахунок
  const state = loadState();
  rebuildStandingsInternal(state);
  SpreadsheetApp.getUi().alert('Гравця знято. Поточне незавершене парування, якщо було, відмічено як форфіт супернику.');
}

// ставимо технічну поразку (Score 0-1) у незавершеному раунді, якщо гравець розписаний
function forfeitOpenRoundIfPaired(idStr) {
  const ss = SpreadsheetApp.getActive();
  const shRounds = ss.getSheetByName('Rounds');
  const rounds = readRounds(shRounds);
  const doneUpTo = lastCompletedRoundNumber(rounds);
  const openRound = doneUpTo + 1;

  if (!rounds.some(r => r.round === openRound)) return; // ще не згенеровано

  // знайдемо матч із цим гравцем у відкритому раунді без результату
  const lastRow = shRounds.getLastRow();
  if (lastRow <= 1) return;
  const cols = Math.min(12, shRounds.getLastColumn());
  const data = shRounds.getRange(2,1,lastRow-1,cols).getValues();

  const COL_R=1, COL_A_ID=3, COL_B_ID=5, COL_VPA=7, COL_VPB=8, COL_SA=9, COL_SB=10, COL_NOTE=12;

  let changed = false;
  for (let i=0;i<data.length;i++){
    const row = data[i];
    const rnd = Number(row[COL_R-1]||0);
    if (rnd !== openRound) continue;

    const aId = String(row[COL_A_ID-1]||'');
    const bId = String(row[COL_B_ID-1]||'');

    const bothPresent = !!(aId && bId);
    const hasResult = (
      (row[COL_VPA-1] !== '' && row[COL_VPB-1] !== '') ||
      (row[COL_SA-1] !== '' && row[COL_SB-1] !== '')
    );

    if (!bothPresent || hasResult) continue;

    if (aId === idStr) {
      row[COL_SA-1] = 0; row[COL_SB-1] = 1;
      row[COL_NOTE-1] = appendNote(row[COL_NOTE-1], 'FORFEIT (withdrawn A)');
      changed = true;
    } else if (bId === idStr) {
      row[COL_SA-1] = 1; row[COL_SB-1] = 0;
      row[COL_NOTE-1] = appendNote(row[COL_NOTE-1], 'FORFEIT (withdrawn B)');
      changed = true;
    }
  }
  if (changed) shRounds.getRange(2,1,lastRow-1,cols).setValues(data);
}

function appendNote(oldNote, add) {
  const s = String(oldNote||'').trim();
  return s ? (s+'; '+add) : add;
}
