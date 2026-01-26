// from Jan 1
const START_DATE = 1735682400;

// to Jul 1
const END_DATE = 1751317200;

// from Jul 1
// const START_DATE = 1751317200;

// const END_DATE   = null;



// to 17.Nov
//const END_DATE = 1763337600;

// from 1.07 by default
//const START_DATE = 1751317200;

// 3y ago by default
// const START_DATE = 1640995200;

// custom
// const START_DATE = 1735686000;
// const END_DATE   = 1744405200;

//const END_DATE   = 1751317200;


const MAX_ELO_BATCH_RANGE = 'T33:T47';
const MAX_ELO_RESULT_OFFSET = 1; // P
const MAX_ELO_DETAILS_OFFSET = 2; // Q
const MAX_ELO_STATE_KEY = 'MAX_ELO_BATCH_STATE';
const MAX_ELO_TRIGGER_HANDLER = 'runNextMaxElo';
const MAX_ELO_SUCCESS_DELAY_MS = 1000;
const MAX_ELO_RETRY_DELAY_DEFAULT_MS = 5000;
const MAX_ELO_RETRY_DELAY_MAX_MS = 10000;

/**
 * Отримує найкращий elo_after - 1300 для гравця з BGA.
 *
 * @param {number|string} id BGA user ID
 * @return {number} Значення elo_after - 1300
 * @customfunction
 */
function GET_ELO_MAX(id=95199738) {
  const result = fetchMaxEloData_(id);
  Logger.log(String(Math.trunc(result.maxElo)));
  return result.maxElo;
}

/**
 * Стартує пакетну обробку діапазону (наприклад, O3:O65).
 * Викликається вручну один раз, далі все роблять тригери.
 *
 * @param {string} rangeA1 Діапазон з ID гравців
 */
function startMaxEloBatch(rangeA1 = MAX_ELO_BATCH_RANGE) {
  const sheet = SpreadsheetApp.getActiveSheet();
  const range = sheet.getRange(rangeA1);

  const state = {
    sheetName: sheet.getName(),
    startRow: range.getRow(),
    numRows: range.getNumRows(),
    column: range.getColumn(),
    nextIndex: 0
  };

  PropertiesService.getScriptProperties().setProperty(
      MAX_ELO_STATE_KEY,
      JSON.stringify(state)
  );

  deleteTriggersByFunction_(MAX_ELO_TRIGGER_HANDLER);
  runNextMaxElo();
}

/**
 * Виконує один крок пакетної обробки та планує наступний.
 * Викликається тригером або вручну.
 */
function runNextMaxElo() {
  const props = PropertiesService.getScriptProperties();
  const stateJson = props.getProperty(MAX_ELO_STATE_KEY);
  if (!stateJson) {
    Logger.log('Max Elo batch state is empty, nothing to do.');
    deleteTriggersByFunction_(MAX_ELO_TRIGGER_HANDLER);
    return;
  }

  const state = JSON.parse(stateJson);
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(state.sheetName);
  if (!sheet) {
    props.deleteProperty(MAX_ELO_STATE_KEY);
    deleteTriggersByFunction_(MAX_ELO_TRIGGER_HANDLER);
    throw new Error(`Sheet ${state.sheetName} not found`);
  }

  if (state.nextIndex >= state.numRows) {
    props.deleteProperty(MAX_ELO_STATE_KEY);
    deleteTriggersByFunction_(MAX_ELO_TRIGGER_HANDLER);
    Logger.log('Max Elo batch completed');
    return;
  }

  const rowNumber = state.startRow + state.nextIndex;
  const id = sheet.getRange(rowNumber, state.column).getValue();

  if (!id) {
    state.nextIndex++;
    props.setProperty(MAX_ELO_STATE_KEY, JSON.stringify(state));
    scheduleNextMaxEloRun_(MAX_ELO_SUCCESS_DELAY_MS);
    return;
  }

  try {
    const result = fetchMaxEloData_(id);
    sheet.getRange(rowNumber, state.column + MAX_ELO_RESULT_OFFSET).setValue(result.maxElo);
    sheet.getRange(rowNumber, state.column + MAX_ELO_DETAILS_OFFSET).setValue(result.detail);

    state.nextIndex++;
    if (state.nextIndex >= state.numRows) {
      props.deleteProperty(MAX_ELO_STATE_KEY);
      deleteTriggersByFunction_(MAX_ELO_TRIGGER_HANDLER);
      Logger.log('Max Elo batch completed');
      return;
    }

    props.setProperty(MAX_ELO_STATE_KEY, JSON.stringify(state));
    scheduleNextMaxEloRun_(MAX_ELO_SUCCESS_DELAY_MS);
  } catch (error) {
    const retryDelay = getRetryDelayMs_(error);
    Logger.log(`Max Elo error for row ${rowNumber}: ${error.message}`);

    if (retryDelay) {
      scheduleNextMaxEloRun_(retryDelay);
      return;
    }

    sheet.getRange(rowNumber, state.column + MAX_ELO_DETAILS_OFFSET).setValue(error.message);
    state.nextIndex++;
    if (state.nextIndex >= state.numRows) {
      props.deleteProperty(MAX_ELO_STATE_KEY);
      deleteTriggersByFunction_(MAX_ELO_TRIGGER_HANDLER);
      Logger.log('Max Elo batch completed with errors');
      return;
    }

    props.setProperty(MAX_ELO_STATE_KEY, JSON.stringify(state));
    scheduleNextMaxEloRun_(MAX_ELO_SUCCESS_DELAY_MS);
  }
}

/**
 * Перериває пакетну обробку та прибирає тригери.
 */
function stopMaxEloBatch() {
  PropertiesService.getScriptProperties().deleteProperty(MAX_ELO_STATE_KEY);
  deleteTriggersByFunction_(MAX_ELO_TRIGGER_HANDLER);
}

function fetchMaxEloData_(id) {
  if (!id) {
    throw new Error('Provide user ID');
  }

  const apiUrl = `${URL_BASE}/get-elo-max`;

  // Формуємо payload без null дат
  const payload = { id: Number(id) };
  if (START_DATE !== null) payload.start_date = START_DATE;
  if (END_DATE !== null) payload.end_date = END_DATE;

  console.log(apiUrl, 'res');
  const res = UrlFetchApp.fetch(apiUrl, {
    method: 'post',
    contentType: 'application/json',
    headers: { Authorization: 'Bearer ' + TOKEN },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  });

  const code = res.getResponseCode();
  const body = res.getContentText();

  // Логуємо всю відповідь сервера
  Logger.log(`HTTP ${code} → ${body}`);

  if (code !== 200) {
    throw new Error(`API HTTP ${code}: ${body}`);
  }

  const data = JSON.parse(body);
  if (!data || data.status === 'error') {
    throw new Error(`API error: ${data && data.message ? data.message : body}`);
  }

  const best = data.best_table;
  if (!best || typeof best.elo_after === 'undefined') {
    throw new Error('best_table or elo_after not found');
  }

  const maxElo = Number(best.elo_after) - 1300;
  return {
    maxElo,
    detail: `HTTP ${code} → ${body}`,
    data
  };
}

function scheduleNextMaxEloRun_(delayMs) {
  // Ensure only one time-based trigger exists for this handler to avoid hitting trigger limits.
  deleteTriggersByFunction_(MAX_ELO_TRIGGER_HANDLER);

  ScriptApp.newTrigger(MAX_ELO_TRIGGER_HANDLER)
      .timeBased()
      .after(Math.max(delayMs, 1000))
      .create();
}

function deleteTriggersByFunction_(functionName) {
  const triggers = ScriptApp.getProjectTriggers();
  triggers.forEach(trigger => {
    if (trigger.getHandlerFunction() === functionName) {
      ScriptApp.deleteTrigger(trigger);
    }
  });
}

function getRetryDelayMs_(error) {
  if (!error || typeof error.message !== 'string') {
    return null;
  }

  if (error.message.indexOf('API HTTP 503') === -1) {
    return null;
  }

  const match = error.message.match(/"retry_after":\s*(\d+)/);
  let seconds = match ? Number(match[1]) : MAX_ELO_RETRY_DELAY_DEFAULT_MS / 1000;
  if (!seconds || Number.isNaN(seconds)) {
    seconds = MAX_ELO_RETRY_DELAY_DEFAULT_MS / 1000;
  }

  seconds = Math.max(5, Math.min(seconds, MAX_ELO_RETRY_DELAY_MAX_MS / 1000));
  return seconds * 1000;
}
