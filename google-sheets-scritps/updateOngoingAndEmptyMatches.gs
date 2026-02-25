function updateOngoingAndEmptyMatches() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Duels");
  const data = sheet.getDataRange().getValues();
  const rowNumbers = Array.from({ length: data.length - 1 }, (_, i) => i + 2);
  
  // ongoing
  const {
    payloads: payloadsOngoing,
    rowIndexes: rowIndexesOngoing,
  } = collectMatchPayloads(data, rowNumbers, {
    filterStarted: true,
    filterOngoing: true,
    filterNotScored: true,
  });

  // empty
  const {
    payloads: payloadsEmpty,
    rowIndexes: rowIndexesEmpty,
  } = collectMatchPayloads(data, rowNumbers, {
    filterStarted: true,
    filterFinished: true,
    filterEmpty: true,
  });

  if (payloadsOngoing.length === 0) {
    Logger.log("✅ No matches currently in progress.");
  } else {
    Logger.log(`🔍 Checking ${payloadsOngoing.length} ongoing matches...`);
  }

  if (payloadsEmpty.length === 0) {
    Logger.log("✅ No empty GW1/GW2 cells to update");
  } else {
    Logger.log(`🔍 Checking ${payloadsEmpty.length} empty matches...`);
  }

  

  for (let i = 0; i < [...payloadsOngoing, ...payloadsEmpty].length; i += BATCH_LIMIT) {
    checkMany(
      sheet,
      [...payloadsOngoing, ...payloadsEmpty].slice(i, i + BATCH_LIMIT),
      [...rowIndexesOngoing, ...rowIndexesEmpty].slice(i, i + BATCH_LIMIT)
    );
  }
}
