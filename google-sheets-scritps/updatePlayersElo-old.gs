function updatePlayersElo() {
  const sheetName = 'Players';
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(sheetName);
  const lastRow = sheet.getLastRow();
  const dataRange = sheet.getRange(1, 1, lastRow, 2);
  const data = dataRange.getValues();

  const updatedData = [];
  const backgroundColors = [];

  data.forEach(function(row) {
    const nickname = row[0];
    const id = row[1];
    let elo = '';
    let color = '#ffffff'; // default white

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

          const eloNumber = parseInt(elo);
          if (!isNaN(eloNumber)) {
            if (eloNumber < 100) color = '#74bed1';
            else if (eloNumber >= 100 && eloNumber < 200) color = '#84b8de';
            else if (eloNumber >= 200 && eloNumber < 300) color = '#94acd6';
            else if (eloNumber >= 300 && eloNumber < 500) color = '#9ba5d0';
            else if (eloNumber >= 500 && eloNumber < 700) color = '#a99bc9';
            else color = '#b593c4';
          }

        } else {
          elo = 'Not found';
        }
      } catch (e) {
        elo = 'Error';
        Logger.log('❌ Failed for ' + nickname + ': ' + e.message);
      }
    }

    updatedData.push([elo]);
    backgroundColors.push([color]);
  });

  if (updatedData.length > 0) {
    const eloRange = sheet.getRange(1, 3, lastRow, 1);
    eloRange.setValues(updatedData);
    eloRange.setBackgrounds(backgroundColors);
  }

  const now = new Date();
  sheet.getRange('E2').setValue(Utilities.formatDate(now, "UTC", "dd.MM.yyyy HH:mm:ss"));

  Logger.log('✅ Players ELO updated at ' + now);
}
