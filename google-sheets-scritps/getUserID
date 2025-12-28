/**
 * Get BGA user ID by username via your server API.
 * @param {string} username The BGA username.
 * @return {string} The user ID.
 * @customfunction
 */

function getUserID(username = "ep45") {
  const url = `${URL_BASE}/get-player-id?nickname=${encodeURIComponent(username)}`;

  const response = UrlFetchApp.fetch(url, {
    method: 'get',
    headers: {
      'Authorization': 'Bearer ' + TOKEN
    },
    muteHttpExceptions: true
  });

  if (response.getResponseCode() !== 200) {
    throw new Error(`API error: ${response.getContentText()}`);
  }

  const data = JSON.parse(response.getContentText());

  if (data.status !== 'success') {
    throw new Error(`Error: ${data.message}`);
  }

  return data.id;
}
