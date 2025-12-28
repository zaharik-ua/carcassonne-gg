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

    // CCL
    {
      fileId: "1KMye5MRf204O5cTRWohnDLrg24dpZdGLFz3xlwsJhdA",
      sheets: {
        "CCL-2026Q": { targetSheet: "Duels", startCol: 3, endCol: 22, tournamentId: "CCL-2026Q" },     // C–V
        "CCL-2026": { targetSheet: "Duels", startCol: 3, endCol: 22, tournamentId: "CCL-2026" },     // C–V
        "Tournament Players": { targetSheet: "Tournament Players", startCol: 1, endCol: 11 }, // A–K
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    },

    // Friendly Matches
    {
      fileId: "1m3xbUbH1-99Qtn1qq3k9PFoVAvge-9pjtIAefeiYDOM",
      sheets: {
        "2025 Matches": { targetSheet: "Matches", startCol: 2, endCol: 19 }, // B–S
        "2025 Duels": { targetSheet: "Duels", startCol: 3, endCol: 22, tournamentId: "Friendly-Matches-2025" },     // C–V
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    },

    // China
    {
      fileId: "16csmJ_sPfYyegKZcLRbxdCmY4zWKUDNdUVe6TtBA1e0",
      sheets: {
        "CCOC-2025": { targetSheet: "Duels", startCol: 3, endCol: 22, tournamentId: "CCOC-2025" },     // C–V
        "Tournament Players": { targetSheet: "Tournament Players", startCol: 1, endCol: 11 }, // A–G
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    },
    
    // Ukraine
    {
      fileId: "1Jc5uG0WQer2OgDsgaLsDN7MOLPWt7VCaou1aOQVNykk",
      sheets: {
        "UCOC-2026": { targetSheet: "Duels", startCol: 3, endCol: 22, tournamentId: "UCOC-2026" },      // C–V
        "UCDTC-2025-Matches": { targetSheet: "Matches", startCol: 2, endCol: 19 }, // B–S
        "UCDTC-2025-Duels": { targetSheet: "Duels", startCol: 3, endCol: 22, tournamentId: "UCDTC-2025-Duels" },     // C–V
        "Tournament Players": { targetSheet: "Tournament Players", startCol: 1, endCol: 11 }, // A–G
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    },

    // Belgium
    {
      fileId: "1AGxPg5s5iPcU61E8p2EBLj5xol4fLJRd7QP2cL0wMLo",
      sheets: {
        "BCOC-2026": { targetSheet: "Duels", startCol: 3, endCol: 22, tournamentId: "BCOC-2026" },     // C–V
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    },

    // Italy
    {
      fileId: "1iSgoxWzamJ-QvDvsdgkb3twiJ5y-sUdOl8fMUfkKALM",
      sheets: {
        "ITCCL-2025": { targetSheet: "Duels", startCol: 3, endCol: 22, tournamentId: "ITCCL-2025" },     // C–V
        "Tournament Players": { targetSheet: "Tournament Players", startCol: 1, endCol: 11 }, // A–G
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    },

    // Belarus
    {
      fileId: "17sAfGdBHuwSTfyLtfjiYslbeFt3LGzDnDUKyBZverZM",
      sheets: {
        "BCPL-2026-WIN-Major": { targetSheet: "Duels", startCol: 3, endCol: 22, tournamentId: "BCPL-2026-WIN-Major" },     // C–V
        "BCPL-2026-WIN-1st": { targetSheet: "Duels", startCol: 3, endCol: 22, tournamentId: "BCPL-2026-WIN-1st" },     // C–V
        "BCPL-2026-WIN-2nd": { targetSheet: "Duels", startCol: 3, endCol: 22, tournamentId: "BCPL-2026-WIN-2nd" },     // C–V
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    }

    // OTHER
    // {
    //   fileId: "1C_EdvtfRkV5aawBJLoAKTpVFKh-xOLabEWzXgEV7IvI",
    //   sheets: {
    //     "OTHER": { targetSheet: "Duels", startCol: 3, endCol: 22 },     // C–V
    //     "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
    //   }
    // }

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
      let values = sheet.getRange(2, config.startCol, rowCount - 1, colCount).getDisplayValues();

      // For Matches: update B–N and S from source, do NOT touch O–R.
      // Source row is relative to startCol=B:
      // 0..12 => B..N, 13..16 => O..R (skip), 17 => S
      if (config.targetSheet === "Matches") {
        // Єдина маска відбору: беремо рядки тільки там, де ЗАПОВНЕНА колонка C
        // (relative index 1 у values, бо startCol = B)
        const keepMask = values.map(row => row[1] !== "");

        const rowsBN = [];
        const rowsS  = [];

        for (let i = 0; i < values.length; i++) {
          if (!keepMask[i]) continue;
          const row = values[i];
          rowsBN.push(row.slice(0, 13));          // B..N (13 cols)
          rowsS.push([row[17] !== undefined ? row[17] : ""]); // S (1 col)
        }

        const keyBN = JSON.stringify({ sheet: config.targetSheet, startCol: 2 });   // B
        const keyS  = JSON.stringify({ sheet: config.targetSheet, startCol: 19 });  // S

        if (!dataMap[keyBN]) dataMap[keyBN] = [];
        if (!dataMap[keyS])  dataMap[keyS]  = [];

        dataMap[keyBN].push(...rowsBN);
        dataMap[keyS].push(...rowsS);

        // важливо: пропускаємо стандартну обробку, щоб не чіпати O–R
        continue;
      }

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
      const orderedKeys = [];
      const rowsByKey = new Map();

      for (const row of processedRows) {
        const key = row[1]; // use player id from column B as dedupe key
        if (!key) continue; // skip rows without id

        const hasColumnC = row[2] !== null && row[2] !== undefined && row[2] !== "";
        if (!rowsByKey.has(key)) {
          rowsByKey.set(key, { row, hasColumnC });
          orderedKeys.push(key);
          continue;
        }

        const stored = rowsByKey.get(key);
        if (!stored.hasColumnC && hasColumnC) {
          rowsByKey.set(key, { row, hasColumnC });
        }
      }

      cleanedRows = orderedKeys.map(key => rowsByKey.get(key).row);

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
