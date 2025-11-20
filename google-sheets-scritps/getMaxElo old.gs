// from 1.Jan by default
 const START_DATE = 1735689600;

// from 1.07 by default
//const START_DATE = 1751317200;

// 3y ago by default
// const START_DATE = 1640995200;



// to 17.Nov
const END_DATE = 1763337600;
//const END_DATE   = null;

/**
 * Отримує найкращий elo_after - 1300 для гравця з BGA.
 *
 * @param {number|string} id BGA user ID
 * @return {number} Значення elo_after - 1300
 * @customfunction
 */
function GET_ELO_MAX(id=96757109) {
  if (!id) {
    throw new Error('Provide user ID');
  }

  const apiUrl = `${URL_BASE}/get-elo-max`;

  // Формуємо payload без null дат
  const payload = { id: Number(id) };
  if (START_DATE !== null) payload.start_date = START_DATE;
  if (END_DATE !== null) payload.end_date = END_DATE;

  console.log(apiUrl, 'res')
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
  
  Logger.log(String(Math.trunc(Number(best.elo_after) - 1300)));
  return Number(best.elo_after) - 1300;
  
}