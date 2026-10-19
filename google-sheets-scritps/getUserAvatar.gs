// Auth-server: збережені профілі та резервний запит до BGA через його сесію.
const BGA_AVATAR_API_BASE = 'https://api.carcassonne.gg';

// Налаштування ручного запуску fillUserAvatars з редактора Apps Script.
const BGA_AVATAR_BATCH_SHEET = 'test';
const BGA_AVATAR_BATCH_RANGE = 'B70:B80';
const BGA_AVATAR_BATCH_RESULT_OFFSET = 1; // Сусідня колонка праворуч: B → C.
const BGA_AVATAR_BATCH_DELAY_MS = 1000;

/**
 * Послідовно отримує аватари для однієї колонки й записує URL праворуч.
 * У редакторі Apps Script виберіть fillUserAvatars і натисніть «Виконати».
 * За замовчуванням: вкладка test, B70:B80 → C70:C80 у поточному файлі.
 * Можна викликати з іншої функції: fillUserAvatars('test', 'B70:B80', 1).
 * Порожні вхідні клітинки очищують результат; помилки записуються як текст
 * ERROR: … і не переривають обробку наступних рядків.
 * Для великих діапазонів запускайте кілька менших пачок.
 *
 * @param {string} sheetName Назва вкладки.
 * @param {string} rangeA1 Діапазон ID/ніків в одній колонці.
 * @param {number} resultOffset Зсув колонки результату праворуч (1 = сусідня).
 * @return {Object} Кількість успішних, порожніх рядків та помилок.
 */
function fillUserAvatars(
  sheetName = BGA_AVATAR_BATCH_SHEET,
  rangeA1 = BGA_AVATAR_BATCH_RANGE,
  resultOffset = BGA_AVATAR_BATCH_RESULT_OFFSET
) {
  if (!Number.isInteger(resultOffset) || resultOffset < 1) {
    throw new Error('Зсув колонки результату має бути додатним цілим числом');
  }
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(sheetName);
  if (!sheet) throw new Error(`Вкладку "${sheetName}" не знайдено`);
  const range = sheet.getRange(rangeA1);
  if (range.getNumColumns() !== 1) {
    throw new Error('Виберіть діапазон в одній колонці, наприклад B70:B80');
  }

  const values = range.getValues();
  const startRow = range.getRow();
  const resultColumn = range.getColumn() + resultOffset;
  const summary = { succeeded: 0, blank: 0, failed: 0 };

  for (let index = 0; index < values.length; index++) {
    const player = normalizeBgaAvatarPlayer_(values[index][0]);
    let result = '';
    if (!player) {
      summary.blank++;
    } else {
      try {
        result = getUserAvatar(player);
        summary.succeeded++;
      } catch (error) {
        result = 'ERROR: ' + error.message;
        summary.failed++;
        Logger.log(`${sheetName}, рядок ${startRow + index}: ${result}`);
      }
    }
    sheet.getRange(startRow + index, resultColumn).setValue(result);
    SpreadsheetApp.flush();
    if (player && index < values.length - 1) Utilities.sleep(BGA_AVATAR_BATCH_DELAY_MS);
  }

  Logger.log(`Аватари ${sheetName}!${rangeA1}: ${JSON.stringify(summary)}`);
  return summary;
}

/**
 * Повертає посилання на BGA-аватар за ID або ніком гравця.
 * Спочатку шукає збережений avatar у /profiles/public.
 * Якщо профілю/аватара немає або цей endpoint недоступний, запитує BGA
 * через /bga/player-avatar (потрібна оновлена версія auth-server).
 * Порожня клітинка або відсутній avatar у BGA повертає порожній рядок.
 * Результати BGA кешуються на годину. Для холодних BGA-запитів використовуйте
 * невеликі діапазони: custom function у Sheets має обмежений час виконання.
 *
 * Приклади формул:
 * =getUserAvatar(A2)
 * =getUserAvatar("ep45")
 * =getUserAvatar(86345496)
 * =getUserAvatar(A2:A100)
 * =IMAGE(getUserAvatar(A2))
 *
 * @param {string|number|Array<Array<string|number>>} player ID, нік або діапазон.
 * @return {string|Array<Array<string>>} URL аватара або діапазон URL.
 * @customfunction
 */
function getUserAvatar(player) {
  const isRange = Array.isArray(player);
  const values = isRange ? player : [[player]];
  const hasPlayers = values.some(row => row.some(value => normalizeBgaAvatarPlayer_(value)));
  if (!hasPlayers) {
    return isRange ? values.map(row => row.map(() => '')) : '';
  }

  // Один запит до .gg на весь діапазон.
  let profiles = [];
  let profilesError = '';
  try {
    const data = fetchBgaAvatarApiJson_('/profiles/public');
    if (!Array.isArray(data.profiles)) throw new Error('відсутній список profiles');
    profiles = data.profiles;
  } catch (error) {
    profilesError = error.message;
  }

  const profilesById = new Map();
  const profilesByNickname = new Map();
  profiles.forEach(profile => {
    if (!profile || typeof profile !== 'object') return;
    const id = normalizeBgaAvatarPlayer_(profile.id);
    const nickname = normalizeBgaAvatarPlayer_(profile.bga_nickname).toLowerCase();
    if (id) profilesById.set(id, profile);
    if (nickname) profilesByNickname.set(nickname, profile);
  });

  const fallbackResults = new Map();
  const result = values.map(row => row.map(value => {
    const key = normalizeBgaAvatarPlayer_(value);
    if (!key) return '';
    const profile = profilesById.get(key) || profilesByNickname.get(key.toLowerCase());
    const savedUrl = profile ? buildBgaAvatarUrl_(profile.id, profile.avatar) : '';
    if (savedUrl) return savedUrl;

    // Якщо нік змінився, для BGA використовуємо стабільний ID профілю.
    const lookup = profile ? normalizeBgaAvatarPlayer_(profile.id) : key;
    const fallbackKey = lookup.toLowerCase();
    if (!fallbackResults.has(fallbackKey)) {
      try {
        fallbackResults.set(fallbackKey, fetchUserAvatarFromBga_(lookup));
      } catch (error) {
        const ggDetail = profilesError ? `; .gg: ${profilesError}` : '';
        throw new Error(`Аватар гравця "${key}": ${error.message}${ggDetail}`);
      }
    }
    return fallbackResults.get(fallbackKey);
  }));
  return isRange ? result : result[0][0];
}

function fetchUserAvatarFromBga_(player) {
  const cache = CacheService.getScriptCache();
  // Нова версія ключа відкидає URL, закешовані з неправильним розрахунком папки.
  const cacheKey = 'bga-avatar:v2:' + encodeURIComponent(player.toLowerCase());
  const cached = cache.get(cacheKey);
  if (cached !== null) return JSON.parse(cached);

  const data = fetchBgaAvatarApiJson_(`/bga/player-avatar?player=${encodeURIComponent(player)}`);
  const id = normalizeBgaAvatarPlayer_(data.player_id);
  if (!/^\d+$/.test(id)) throw new Error('BGA: у відповіді відсутній ID гравця');
  if (!Object.prototype.hasOwnProperty.call(data, 'avatar')) {
    throw new Error('BGA: у відповіді відсутнє поле avatar');
  }
  const url = buildBgaAvatarUrl_(id, data.avatar);
  cache.put(cacheKey, JSON.stringify(url), 3600);
  return url;
}

function fetchBgaAvatarApiJson_(path) {
  const url = BGA_AVATAR_API_BASE.replace(/\/+$/, '') + path;
  const options = {
    method: 'get',
    muteHttpExceptions: true
  };
  let response = UrlFetchApp.fetch(url, options);
  // Резервні BGA-запити сервер обробляє по одному, щоб не перевантажувати сесію.
  if (response.getResponseCode() === 429 && path.indexOf('/bga/player-avatar?') === 0) {
    Utilities.sleep(2000);
    response = UrlFetchApp.fetch(url, options);
  }
  const code = response.getResponseCode();
  const body = response.getContentText();
  let data;
  try {
    data = JSON.parse(body);
  } catch (_error) {
    throw new Error(`Avatar API HTTP ${code}: відповідь сервера не є JSON`);
  }
  if (code !== 200 || !data || data.ok !== true) {
    throw new Error(`Avatar API HTTP ${code}: ${data && data.message ? data.message : 'помилка сервера'}`);
  }
  return data;
}

function normalizeBgaAvatarPlayer_(value) {
  return value === null || value === undefined ? '' : String(value).trim();
}

// Та сама логіка, що й buildBgaAvatarUrl у gg-html/player-hub/players.html.
function buildBgaAvatarUrl_(playerId, avatar) {
  const playerIdText = normalizeBgaAvatarPlayer_(playerId);
  const avatarText = normalizeBgaAvatarPlayer_(avatar);
  if (!avatarText) return '';

  if (/^https?:\/\//i.test(avatarText)) {
    // Repair saved BGA photo URLs that use the old 100-million folder boundary.
    return avatarText.replace(
      /^(https?:\/\/x\.boardgamearena\.net\/data\/avatar\/)\d+\/\d+\/\d+\/(\d+)(_\d+\.[a-z0-9]+)(?=[?#]|$)/i,
      (_match, base, photoPlayerId, imageSuffix) => {
        const id = Number(photoPlayerId);
        return `${base}${Math.floor(id / 1000000000)}/${Math.floor(id / 1000000)}/${Math.floor(id / 1000)}/${photoPlayerId}${imageSuffix}`;
      }
    );
  }

  if (avatarText === 'x00000') {
    return 'https://x.boardgamearena.net/data/avatar/default_184.jpg';
  }

  const defaultMatch = avatarText.match(/^_def_(\d+)$/);
  if (defaultMatch) {
    return `https://x.boardgamearena.net/data/avatar/defaults/default-${defaultMatch[1].padStart(4, '0')}.jpg`;
  }

  if (!/^\d+$/.test(playerIdText)) return '';
  const playerIdNumber = Number(playerIdText);
  if (!Number.isFinite(playerIdNumber)) return '';

  const firstFolder = String(Math.floor(playerIdNumber / 1000000000));
  const secondFolder = String(Math.floor(playerIdNumber / 1000000));
  const thirdFolder = String(Math.floor(playerIdNumber / 1000));
  return `https://x.boardgamearena.net/data/avatar/${firstFolder}/${secondFolder}/${thirdFolder}/${playerIdText}_184.jpg?h=${encodeURIComponent(avatarText)}`;
}
