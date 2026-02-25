function fixZeroScores() {
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName("Duels");
  const data = sheet.getDataRange().getValues();

  const rowsToClear = [];
  const rowsToDash = [];
  const nowMs = Date.now();

  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    const gw1 = row[17];
    const gw2 = row[18];
    const status = row[26];
    const date = row[14];

    const gw1IsZero = gw1 === 0 || gw1 === "0";
    const gw2IsZero = gw2 === 0 || gw2 === "0";
    const gw1IsDash = gw1 === "-";
    const gw2IsDash = gw2 === "-";
    const scoreIsZeroOrDash = (gw1IsZero || gw1IsDash) && (gw2IsZero || gw2IsDash);
    const dateIsValid = date instanceof Date;
    const dateIsInPast = dateIsValid ? date.getTime() < nowMs : false;
    const dateIsInFuture = dateIsValid ? date.getTime() > nowMs : false;
    const dateIsEmpty = !date;

    // 1) Game didn't happen, time not removed: past time + 0:0 + ERROR -> set "-" "-"
    if (dateIsInPast && gw1IsZero && gw2IsZero && status === "ERROR") {
      rowsToDash.push(i + 1);
      continue;
    }

    // 2) Time removed, new not set: no time + ERROR + (0:0 or -:-) -> clear score
    if (dateIsEmpty && status === "ERROR" && scoreIsZeroOrDash) {
      rowsToClear.push(i + 1);
      continue;
    }

    // 3) Rescheduled to future: future time + PLANNED + (0:0 or -:-) -> clear score
    if (dateIsInFuture && (status === "PLANNED" || status === "PLANED") && scoreIsZeroOrDash) {
      rowsToClear.push(i + 1);
    }
  }

  if (rowsToClear.length === 0 && rowsToDash.length === 0) {
    Logger.log("✅ No GW1/GW2 zero scores to clear for PLANED/ERROR duels");
    return;
  }

  rowsToDash.forEach((rowIndex) => {
    sheet.getRange(rowIndex, 18, 1, 2).setValues([["-", "-"]]);
    sheet.getRange(rowIndex, 22).setValue("to fix");
  });

  rowsToClear.forEach((rowIndex) => {
    sheet.getRange(rowIndex, 18, 1, 2).setValues([["", ""]]);
    sheet.getRange(rowIndex, 22).setValue("to fix");
  });

  if (rowsToDash.length > 0) {
    Logger.log("✅ Set GW1/GW2 to '-' for rows: " + rowsToDash.join(", "));
  }
  if (rowsToClear.length > 0) {
    Logger.log("✅ Cleared GW1/GW2 for rows: " + rowsToClear.join(", "));
  }
}
