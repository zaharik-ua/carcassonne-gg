function importAllTournamentData() {
  const targetSpreadsheet = SpreadsheetApp.getActiveSpreadsheet();

  // Налаштування джерел: кожен об'єкт — окремий файл
  const sources = [
    
    // United States
    {
      fileId: "1CTP9hEDCAXMY8F7aB7Uy1-lvkBbXvObQVH8aaeIEDbY",
      sheets: {
        "USCC-2025": { targetSheet: "Duels", startCol: 3, endCol: 22},     // C–V
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    },
    
    // Taiwan
    {
      fileId: "17Nk9CBfKsfR9W9UzW0R8huJ04NAlu8J6Zbrr2mVEBiM",
      sheets: {
        "TWCOC-2025": { targetSheet: "Duels", startCol: 3, endCol: 22 },     // C–V
        "TWCOC-2024": { targetSheet: "Duels", startCol: 3, endCol: 22 },   // C–V
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    },

    // Ukraine
    {
      fileId: "1Jc5uG0WQer2OgDsgaLsDN7MOLPWt7VCaou1aOQVNykk",
      sheets: {
        "UCOC-2025": { targetSheet: "Duels", startCol: 3, endCol: 22 },      // C–V
        "UCOCup-2024": { targetSheet: "Duels", startCol: 3, endCol: 22 },      // C–V
        "UCOCup-2025": { targetSheet: "Duels", startCol: 3, endCol: 22 },     // C–V
        "Tournament Players": { targetSheet: "Tournament Players", startCol: 1, endCol: 7 }, // A–G
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    },

    // Australia
    {
      fileId: "1802ASsodmVvllyVdsoE8ps7Fwbjugb2F_L175NFQGk4",
      sheets: {
        "ACOC-2025": { targetSheet: "Duels", startCol: 3, endCol: 22 },   // C–V
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    },

    // Belarus
    {
      fileId: "17sAfGdBHuwSTfyLtfjiYslbeFt3LGzDnDUKyBZverZM",
      sheets: {
        "BCPL-2025-Sum": { targetSheet: "Duels", startCol: 3, endCol: 22 },     // C–V
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    },

    // Argentina
    {
      fileId: "1c_CezOhxguHuzSJmRv6kZ7nsu3HvCbPVN9cUPmknzTo",
      sheets: {
        "AR-2025-LNE": { targetSheet: "Duels", startCol: 3, endCol: 22 },     // C–V
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    },

    // Thailand
    {
      fileId: "1EZGWpJHI3h8gEjHsc5bjhIlqK1g96s3rmcteWa97I_M",
      sheets: {
        "THCOC-2025": { targetSheet: "Duels", startCol: 3, endCol: 22 },     // C–V
        "Tournament Players": { targetSheet: "Tournament Players", startCol: 1, endCol: 7 }, // A–G
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    },

    // Asian Cup
    {
      fileId: "1POamfnVvDgT24iQjsBTbfpQVT5oqLtpH6g5moP-Tfec",
      sheets: {
        "2025 Matches": { targetSheet: "Matches", startCol: 2, endCol: 19 }, // B–S
        "2025 Duels": { targetSheet: "Duels", startCol: 3, endCol: 22 },     // C–V
        "Tournament Players": { targetSheet: "Tournament Players", startCol: 1, endCol: 7 }, // A–G
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    },

    // Spain TECS
    {
      fileId: "1KaZXBqDYzQMulvu6ueEDr7a86Z5YRY_WNnGNMPuEelI",
      sheets: {
        "TECS-2025 Matches": { targetSheet: "Matches", startCol: 2, endCol: 19 }, // B–S
        "TECS-2025 Duels": { targetSheet: "Duels", startCol: 3, endCol: 22},     // C–V
        "Tournament Players": { targetSheet: "Tournament Players", startCol: 1, endCol: 7 }, // A–G
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    },

    // Copa America
    {
      fileId: "1OGCxfkSz7CBi8rCE4FjRjOIZuwCTm6tGnYcDuFqYGdo",
      sheets: {
        "2021 Matches": { targetSheet: "Matches", startCol: 2, endCol: 19 }, // B–S
        "2021 Duels": { targetSheet: "Duels", startCol: 3, endCol: 22 },     // C–V
        "Tournament Players": { targetSheet: "Tournament Players", startCol: 1, endCol: 7 }, // A–G
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    },
    
    // Finland
    {
      fileId: "1McOcRE47Lh26JPTFj0Ugn5wurQ4BGodxWlo-qe7su20",
      sheets: {
        "OCFC-2025": { targetSheet: "Duels", startCol: 3, endCol: 22 },     // C–V
        "Tournament Players": { targetSheet: "Tournament Players", startCol: 1, endCol: 7 }, // A–G
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    },

    // Hungary
    {
      fileId: "1GxoYMFwkpY7VV6fLMZ44BR6rRf8wOaXl3KtZ-RMmjN4",
      sheets: {
        "MOCB-2025": { targetSheet: "Duels", startCol: 3, endCol: 22 },     // C–V
        "Tournament Players": { targetSheet: "Tournament Players", startCol: 1, endCol: 7 }, // A–G
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    },

    // Croatia
    {
      fileId: "1tsH-p-zILqEhXgQn2TKSPwV5GHUkq8J--auhi0iv8l0",
      sheets: {
        "HR-2025-OC": { targetSheet: "Duels", startCol: 3, endCol: 22 },   // C–V
        "CCAL-2025": { targetSheet: "Duels", startCol: 3, endCol: 22 },     // C–V
        "CCLF-2025": { targetSheet: "Duels", startCol: 3, endCol: 22 },     // C–V
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    },

    // Chile
    {
      fileId: "1CUWYseWvGlolB5joejOYikparuVFh7lAqqHM_Sxm9iA",
      sheets: {
        "CCCC-2025": { targetSheet: "Duels", startCol: 3, endCol: 22 },     // C–V
        "Tournament Players": { targetSheet: "Tournament Players", startCol: 1, endCol: 7 }, // A–G
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    },

    // Germany
    {
      fileId: "11ppLd6Bq_kDwA2CB7DsbeK5rqeYn0BllPbIJ03wQRns",
      sheets: {
        "GCOC-2025": { targetSheet: "Duels", startCol: 3, endCol: 22 },     // C–V
        "Tournament Players": { targetSheet: "Tournament Players", startCol: 1, endCol: 7 }, // A–G
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    },

    // Latvia
    {
      fileId: "1TaoZZfM5HsvTAgpXop5tqmxMcBh-GW3bMDKf3lFpuUM",
      sheets: {
        "LCOC-2025": { targetSheet: "Duels", startCol: 3, endCol: 22 },     // C–V
        "Tournament Players": { targetSheet: "Tournament Players", startCol: 1, endCol: 7 }, // A–G
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    },

    // Mexico
    {
      fileId: "1qIsxlltZ9mFcc1fnq8kahCvjY21uP0m9fhe_dxY4Oj4",
      sheets: {
        "MEX-2025": { targetSheet: "Duels", startCol: 3, endCol: 22 },     // C–V
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    },

    // Romania
    {
      fileId: "1ejwK3hnNP0WvTbepN5IabHdc2YTWEZpOhHh5oYuWfwc",
      sheets: {
        "ROCOC-2025": { targetSheet: "Duels", startCol: 3, endCol: 22 },     // C–V
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    },

    // ETCOC 2025
    {
      fileId: "109idkP0idm7YVG1rGf57HfgJsXf2MsA18ITMBzCC8gw",
      sheets: {
        "2025 Matches": { targetSheet: "Matches", startCol: 2, endCol: 19 }, // B–S
        "2025 Duels": { targetSheet: "Duels", startCol: 3, endCol: 22 },     // C–V
        "Players": { targetSheet: "Players", startCol: 1, endCol: 3 }        // A–C
      }
    }
    
  ];

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

      // Фільтрація дублів по колонці A з пріоритетом рядків, де є Avatar (колонка C)
      const orderedKeys = [];
      const rowsByKey = new Map();

      for (const row of processedRows) {
        const key = row[0];
        if (!key) continue;

        const hasAvatar = row[2] !== null && row[2] !== undefined && row[2] !== "";
        if (!rowsByKey.has(key)) {
          rowsByKey.set(key, { row, hasAvatar });
          orderedKeys.push(key);
          continue;
        }

        const stored = rowsByKey.get(key);
        if (!stored.hasAvatar && hasAvatar) {
          rowsByKey.set(key, { row, hasAvatar });
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
