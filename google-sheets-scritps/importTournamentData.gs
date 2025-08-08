function importAllTournamentData() {
  const targetSpreadsheet = SpreadsheetApp.getActiveSpreadsheet();

  // Step 1: Push R+S from main table to country sheets
  const duelsSheet = targetSpreadsheet.getSheetByName("Duels");
  const duelsData = duelsSheet.getDataRange().getValues();

  const updatesByTournament = {};

  for (let i = 1; i < duelsData.length; i++) {
    const row = duelsData[i];
    const duelId = row[1];      // B
    const tournamentId = row[2]; // C
    const score1 = row[17];     // R
    const score2 = row[18];     // S
    const existing = row[21];   // V

    if (duelId && score1 !== "" && score2 !== "" && existing === "") {
      if (!updatesByTournament[tournamentId]) updatesByTournament[tournamentId] = [];
      updatesByTournament[tournamentId].push({ duelId, score1, score2 });
    }
  }

  // Налаштування джерел: кожен об'єкт — окремий файл
  const sources = [
    
    // Asian Cup
    {
      fileId: "1POamfnVvDgT24iQjsBTbfpQVT5oqLtpH6g5moP-Tfec",
      sheets: {
        "2025 Matches": { targetSheet: "Matches", startCol: 2, endCol: 19 }, // B–S
        "2025 Duels": { targetSheet: "Duels", startCol: 3, endCol: 21, tournamentId: "Asian-Cup-2025" },     // C–U
        "2025 Tournament Players": { targetSheet: "Tournament Players", startCol: 1, endCol: 5 }, // A–E
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    },

    // Friendly Matches
    {
      fileId: "1m3xbUbH1-99Qtn1qq3k9PFoVAvge-9pjtIAefeiYDOM",
      sheets: {
        "2025 Matches": { targetSheet: "Matches", startCol: 2, endCol: 19 }, // B–S
        "2025 Duels": { targetSheet: "Duels", startCol: 3, endCol: 21, tournamentId: "Friendly-Matches-2025" },     // C–V
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    },

    // Spain TECS
    {
      fileId: "1KaZXBqDYzQMulvu6ueEDr7a86Z5YRY_WNnGNMPuEelI",
      sheets: {
        "TECS-2025 Matches": { targetSheet: "Matches", startCol: 2, endCol: 19 }, // B–S
        "TECS-2025 Duels": { targetSheet: "Duels", startCol: 3, endCol: 21, tournamentId: "TECS-2025" },     // C–V
        "TECS-2025 Tournament Players": { targetSheet: "Tournament Players", startCol: 1, endCol: 5 }, // A–E
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    },

    // Belarus
    {
      fileId: "17sAfGdBHuwSTfyLtfjiYslbeFt3LGzDnDUKyBZverZM",
      sheets: {
        "BCPL-2025-Sum": { targetSheet: "Duels", startCol: 3, endCol: 21, tournamentId: "BCPL-2025-Sum" },     // C–V
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    },

    // Czechia
    {
      fileId: "1K6lVrUCZAXBE6rVojhO1YsQpOxSxS9EcOVLpmvfl-9Y",
      sheets: {
        "CZ-2025-COC": { targetSheet: "Duels", startCol: 3, endCol: 21, tournamentId: "CZ-2025-COC" },     // C–V
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    },
    
    // Croatia
    {
      fileId: "1tsH-p-zILqEhXgQn2TKSPwV5GHUkq8J--auhi0iv8l0",
      sheets: {
        "HR-2025-OC-2": { targetSheet: "Duels", startCol: 3, endCol: 21, tournamentId: "HR-2025-OC-2" },     // C–V
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    }
  ];

  for (const source of sources) {
    for (const [sourceSheetName, config] of Object.entries(source.sheets)) {
      if (config.targetSheet === "Duels" && config.tournamentId && updatesByTournament[config.tournamentId]) {
        const ss = SpreadsheetApp.openById(source.fileId);
        const sheet = ss.getSheetByName(sourceSheetName);
        if (!sheet) continue;

        const range = sheet.getDataRange();
        const values = range.getValues();

        const duelIdToRowMap = {};
        for (let i = 1; i < values.length; i++) {
          const id = values[i][1]; // Column B = index 1
          duelIdToRowMap[id] = i + 1; // Store row number for later use
        }

        for (const update of updatesByTournament[config.tournamentId]) {
          const targetRow = duelIdToRowMap[update.duelId];
          if (targetRow) {
            const targetV = sheet.getRange(targetRow, 22).getValue(); // Column V = index 21
            if (targetV === "") {
              sheet.getRange(targetRow, 18).setValue(update.score1); // Column R
              sheet.getRange(targetRow, 19).setValue(update.score2); // Column S
            }
          }
        }
      }
    }
  }

  const dataMap = {}; // ключ = targetSheet, значення = масив рядків

  // Збираємо всі дані
  for (const source of sources) {
    const ss = SpreadsheetApp.openById(source.fileId);
    for (const [sourceSheetName, config] of Object.entries(source.sheets)) {
      const sheet = ss.getSheetByName(sourceSheetName);
      if (!sheet) continue;

      const rowCount = sheet.getLastRow();
      if (rowCount < 2) continue;

      const colCount = config.endCol - config.startCol + 1;
      const values = sheet.getRange(2, config.startCol, rowCount - 1, colCount).getDisplayValues();

      // Фільтруємо порожні рядки (навіть якщо є формули з "")
      const nonEmptyRows = values.filter(row => row.some(cell => cell !== ""));

      const key = JSON.stringify({ sheet: config.targetSheet, startCol: config.startCol });
      if (!dataMap[key]) dataMap[key] = [];
      dataMap[key].push(...nonEmptyRows);
    }
  }

  // Очищення та вставка у цільові вкладки
  for (const key in dataMap) {
    const { sheet: targetSheetName, startCol } = JSON.parse(key);
    const rows = dataMap[key];
    const targetSheet = targetSpreadsheet.getSheetByName(targetSheetName);
    if (!targetSheet || rows.length === 0) continue;

    // Очищаємо відповідний діапазон, залишаючи заголовки
    const lastRow = targetSheet.getLastRow();
    if (lastRow > 1) {
      targetSheet.getRange(2, startCol, lastRow - 1, rows[0].length).clearContent();
    }

    let cleanedRows = rows;

    if (targetSheetName === "Players") {
      // Обробка: збереження нулів
      let processedRows = rows.map(row =>
        row.map(cell => {
          if (typeof cell === "string" && /^0\d+/.test(cell)) {
            return `'${cell}`;
          }
          return cell;
        })
      );

      // Фільтрація дублів по колонці A
      const seen = new Set();
      cleanedRows = processedRows.filter(row => {
        const key = row[0];
        if (!key || seen.has(key)) return false;
        seen.add(key);
        return true;
      });

    } else if (targetSheetName === "Tournament Players") {
      // Обробка: збереження нулів для всіх полів
      cleanedRows = rows.map(row =>
        row.map(cell => {
          if (typeof cell === "string" && /^0\d+/.test(cell)) {
            return `'${cell}`;
          }
          return cell;
        })
      );

    } else if (targetSheetName === "Duels") {
      // Обробка: збереження нулів тільки для колонок Q (17) і T (20)
      const qIndex = 17 - startCol;
      const tIndex = 20 - startCol;

      cleanedRows = rows.map(row =>
        row.map((cell, idx) => {
          if (
            (idx === qIndex || idx === tIndex) &&
            typeof cell === "string" &&
            /^0\d+/.test(cell)
          ) {
            return `'${cell}`;
          }
          return cell;
        })
      );
    }

    // Вставляємо дані
    targetSheet.getRange(2, startCol, cleanedRows.length, cleanedRows[0].length).setValues(cleanedRows);

  }
}