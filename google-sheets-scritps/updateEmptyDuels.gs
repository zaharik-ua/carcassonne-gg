function updateEmptyDuels() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Duels");
  const data = sheet.getDataRange().getValues();

  const emptyRows = [];

  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    const gw1 = row[17];
    const gw2 = row[18];
    const timeEnd = row[23];

    if ((gw1 === "" || gw1 === null) && (gw2 === "" || gw2 === null)) {
      if (!(timeEnd instanceof Date) || timeEnd > new Date()) {
        continue;
      }
      emptyRows.push(i + 1);
    }
  }

  if (emptyRows.length === 0) {
    Logger.log("✅ No empty GW1/GW2 cells to update");
    return;
  }
  Logger.log(emptyRows);
  //checkMatchesByRows(emptyRows);
}