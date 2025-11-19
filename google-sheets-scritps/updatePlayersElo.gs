function updatePlayersElo() {
  const sheetName = 'Players';
  const startRow = 2;
  const batchSize = 80;
  const stateKey = 'updatePlayersEloState';

  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(sheetName);
  if (!sheet) {
    Logger.log('❌ Players sheet not found.');
    return;
  }

  const props = PropertiesService.getScriptProperties();
  let state = {};
  const rawState = props.getProperty(stateKey);
  if (rawState) {
    try {
      state = JSON.parse(rawState);
    } catch (err) {
      Logger.log('⚠️ Failed to parse saved state, starting over. ' + err.message);
    }
  }

  let currentTriggerId = state.triggerId || null;

  const lastRow = sheet.getLastRow();
  if (lastRow < startRow) {
    Logger.log('ℹ️ No players to update.');
    sheet.getRange('K2').setValue(Utilities.formatDate(new Date(), 'UTC', 'dd.MM.yyyy HH:mm:ss'));
    if (currentTriggerId) {
      removeUpdatePlayersEloTrigger_(currentTriggerId);
    }
    props.deleteProperty(stateKey);
    return;
  }

  const totalRows = lastRow - startRow + 1;
  const targetLastRow = startRow + totalRows - 1;

  let nextRow = Number(state.nextRow) || startRow;
  let prepared = state.prepared === true;
  let stateLastRow = Number(state.lastRow) || targetLastRow;

  const shouldReset =
    !prepared ||
    stateLastRow !== targetLastRow ||
    nextRow < startRow ||
    nextRow > targetLastRow;

  if (shouldReset) {
    nextRow = startRow;
    stateLastRow = targetLastRow;
    prepared = true;
    if (currentTriggerId) {
      removeUpdatePlayersEloTrigger_(currentTriggerId);
      currentTriggerId = null;
    }

    const colBValues = sheet.getRange(startRow, 2, totalRows, 1).getValues();
    sheet.getRange(startRow, 9, totalRows, 1).setValues(colBValues);
    sheet.getRange(startRow, 9, totalRows, 1).sort({column: 9, ascending: true});
    sheet.getRange(startRow, 10, totalRows, 1).clearContent();

    Logger.log('🔁 Restarted Players ELO update from the beginning.');
  }

  if (nextRow > stateLastRow) {
    finalizeUpdatePlayersElo_(sheet, props, stateKey, currentTriggerId);
    return;
  }

  const endRow = Math.min(nextRow + batchSize - 1, stateLastRow);
  const chunkSize = endRow - nextRow + 1;
  const ids = sheet.getRange(nextRow, 9, chunkSize, 1).getValues();

  const updatedData = [];
  const backgroundColors = [];

  ids.forEach(function(row, index) {
    const id = row[0];
    const absRow = nextRow + index;
    let elo = '';
    let color = '#ffffff';

    if (id) {
      try {
        const url = 'https://uk.boardgamearena.com/playerstat?id=' + id + '&game=1';
        const response = UrlFetchApp.fetch(url, {muteHttpExceptions: true});
        const content = response.getContentText();

        const startMarker = "class='gamerank_value' >";
        const startIndex = content.indexOf(startMarker);

        if (startIndex !== -1) {
          const start = startIndex + startMarker.length;
          const end = content.indexOf('</span>', start);
          elo = content.substring(start, end).trim();
        } else {
          elo = 'Not found';
        }
      } catch (e) {
        Logger.log('💥 Fetch failed for row ' + absRow + ' (id=' + id + '): ' + e.message);
        elo = 'Error';
      }
    }

    updatedData.push([elo]);
    backgroundColors.push([color]);
  });

  const eloRange = sheet.getRange(nextRow, 10, chunkSize, 1);
  eloRange.setValues(updatedData);
  eloRange.setBackgrounds(backgroundColors);

  const newNextRow = endRow + 1;

  if (newNextRow > stateLastRow) {
    finalizeUpdatePlayersElo_(sheet, props, stateKey, currentTriggerId);
  } else {
    if (currentTriggerId) {
      removeUpdatePlayersEloTrigger_(currentTriggerId);
    }
    const nextTriggerId = scheduleNextUpdatePlayersElo_();
    props.setProperty(
      stateKey,
      JSON.stringify({
        nextRow: newNextRow,
        lastRow: stateLastRow,
        prepared: true,
        triggerId: nextTriggerId
      })
    );
    Logger.log('⏭️ Processed rows ' + nextRow + '–' + endRow + ', scheduling next batch.');
  }
}

function finalizeUpdatePlayersElo_(sheet, props, stateKey, triggerId) {
  if (triggerId) {
    removeUpdatePlayersEloTrigger_(triggerId);
  }
  props.deleteProperty(stateKey);
  const now = new Date();
  sheet.getRange('K2').setValue(Utilities.formatDate(now, 'UTC', 'dd.MM.yyyy HH:mm:ss'));
  Logger.log('✅ Players ELO updated at ' + now);
}

function scheduleNextUpdatePlayersElo_() {
  const trigger = ScriptApp.newTrigger('updatePlayersElo').timeBased().after(1 * 60 * 1000).create();
  return trigger.getUniqueId();
}

function removeUpdatePlayersEloTrigger_(triggerId) {
  if (!triggerId) {
    return;
  }
  const triggers = ScriptApp.getProjectTriggers();
  triggers.forEach(function(trigger) {
    if (trigger.getHandlerFunction() === 'updatePlayersElo' && trigger.getUniqueId() === triggerId) {
      ScriptApp.deleteTrigger(trigger);
    }
  });
}
