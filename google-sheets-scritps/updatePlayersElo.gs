function updatePlayersElo() {
  const sheetName = 'Players';
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(sheetName);
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) {
    Logger.log('ℹ️ No players to update.');
    sheet.getRange('K2').setValue(Utilities.formatDate(new Date(), "UTC", "dd.MM.yyyy HH:mm:ss"));
    return;
  }
  // Copy values from column B to column I for all relevant rows before fetching IDs from column I
  const startRow = 2;
  const rowsCount = lastRow - 1; // starting from row 2
  const colBRange = sheet.getRange(startRow, 2, rowsCount, 1); // B2:B
  const colBValues = colBRange.getValues();
  const colIRange = sheet.getRange(startRow, 9, rowsCount, 1); // I2:I
  colIRange.setValues(colBValues);

  // Sort column I from row 2 to last row in ascending order, considering only non-empty values
  sheet.getRange(startRow, 9, rowsCount, 1).sort({column: 9, ascending: true});

  // Clear the contents of column J from row 2 down to the last row
  sheet.getRange(startRow, 10, rowsCount, 1).clearContent();

  const dataRange = sheet.getRange(startRow, 9, rowsCount, 1); // I2:I (IDs)
  const data = dataRange.getValues();

  const updatedData = [];
  const backgroundColors = [];

  data.forEach(function(row, i) {
    const id = row[0]; // from column I
    const absRow = startRow + i;

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

        } else {
          elo = 'Not found';
        }
      } catch (e) {
        Logger.log(`  💥 Fetch failed for row ${absRow} (id=${id}): ${e.message}`);
        elo = 'Error';
      }
    }

    updatedData.push([elo]);
    backgroundColors.push([color]);
  });

  if (updatedData.length > 0) {
    const eloRange = sheet.getRange(startRow, 10, rowsCount, 1); // J2:J
    eloRange.setValues(updatedData);
    eloRange.setBackgrounds(backgroundColors);
  }

  const now = new Date();
  sheet.getRange('K2').setValue(Utilities.formatDate(now, "UTC", "dd.MM.yyyy HH:mm:ss"));

  Logger.log('✅ Players ELO updated at ' + now);
}
