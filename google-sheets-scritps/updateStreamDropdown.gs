function updateStreamDropdown() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const matchesSheet = ss.getSheetByName("Matches");
  const streamsSheet = ss.getSheetByName("Streams");
  const duelsSheet = ss.getSheetByName("Duels");
  const now = new Date();
  const sixHoursAgo = new Date(now.getTime() - 6 * 60 * 60 * 1000);

  // --- MATCHES => Column C in Streams ---
  const matchesData = matchesSheet.getRange("A2:M" + matchesSheet.getLastRow()).getValues();
  const upcomingMatches = matchesData.filter(row => {
    const date = new Date(row[12]);
    return date > now || date >= sixHoursAgo;
  });
  const matchTitles = upcomingMatches.map(row => row[0]);

  if (matchTitles.length > 0) {
    const matchRule = SpreadsheetApp.newDataValidation()
      .requireValueInList(matchTitles, true)
      .setAllowInvalid(false)
      .build();

    const columnC = streamsSheet.getRange("C2:C").getValues();
    let lastFilledRowC = 1;
    for (let i = 0; i < columnC.length; i++) {
      if (columnC[i][0]) lastFilledRowC = i + 2;
    }

    const startRowC = lastFilledRowC;
    const endRowC = 1000;
    const rangeC = streamsSheet.getRange(startRowC + 1, 3, endRowC - startRowC + 1);
    rangeC.setDataValidation(matchRule);
    Logger.log(`✅ Applied dropdown validation in column C from row ${startRowC + 1} to ${endRowC}`);
  } else {
    Logger.log("ℹ️ No upcoming matches found — setting 'no matches planned' for empty cells.");
    const matchRule = SpreadsheetApp.newDataValidation()
      .requireValueInList(["no matches planned"], true)
      .setAllowInvalid(false)
      .build();

    const columnC = streamsSheet.getRange("C2:C1000").getValues();
    for (let i = 0; i < columnC.length; i++) {
      if (!columnC[i][0]) {
        streamsSheet.getRange(i + 2, 3).setDataValidation(matchRule);
      }
    }
  }

  // --- DUELS => Column D in Streams ---
  const duelsData = duelsSheet.getRange("A2:O" + duelsSheet.getLastRow()).getValues();
  const futureDuels = duelsData
    .filter(row => {
      const matchId = row[3];       // column D
      const date = row[14];          // column O

      return !matchId &&            
             date instanceof Date &&
             (date > now || date >= sixHoursAgo) &&
             (date.getHours() + date.getMinutes() + date.getSeconds()) > 0;
    })
    .map(row => row[0]); // column A

  if (futureDuels.length > 0) {
    const duelRule = SpreadsheetApp.newDataValidation()
      .requireValueInList(futureDuels, true)
      .setAllowInvalid(false)
      .build();

    const columnD = streamsSheet.getRange("D2:D").getValues();
    let lastFilledRowD = 1;
    for (let i = 0; i < columnD.length; i++) {
      if (columnD[i][0]) lastFilledRowD = i + 2;
    }

    const startRowD = lastFilledRowD;
    const endRowD = 1000;
    const rangeD = streamsSheet.getRange(startRowD + 1, 4, endRowD - startRowD + 1);
    rangeD.setDataValidation(duelRule);
    Logger.log(`✅ Applied dropdown validation in column D from row ${startRowD + 1} to ${endRowD}`);
  } else {
    Logger.log("ℹ️ No upcoming duels found — setting 'no duels planned' for empty cells.");
    const duelRule = SpreadsheetApp.newDataValidation()
      .requireValueInList(["no duels planned"], true)
      .setAllowInvalid(false)
      .build();

    const columnD = streamsSheet.getRange("D2:D1000").getValues();
    for (let i = 0; i < columnD.length; i++) {
      if (!columnD[i][0]) {
        streamsSheet.getRange(i + 2, 4).setDataValidation(duelRule);
      }
    }
  }
}
