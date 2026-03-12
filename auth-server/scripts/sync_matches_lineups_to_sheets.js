import path from "node:path";
import { fileURLToPath } from "node:url";
import sqlite3 from "sqlite3";
import dotenv from "dotenv";
import { google } from "googleapis";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const DB_PATH = process.env.DB_PATH || "./data/auth.sqlite";
const dbFullPath = path.resolve(__dirname, "..", DB_PATH);

const SPREADSHEET_ID = String(process.env.GOOGLE_SHEETS_SPREADSHEET_ID || "").trim();
const MATCHES_SHEET = String(process.env.GOOGLE_SHEETS_MATCHES_SHEET || "matches").trim();
const LINEUPS_SHEET = String(process.env.GOOGLE_SHEETS_LINEUPS_SHEET || "lineups").trim();
const SERVICE_ACCOUNT_EMAIL = String(process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL || "").trim();
const SERVICE_ACCOUNT_PRIVATE_KEY = String(process.env.GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY || "")
  .replaceAll("\\n", "\n")
  .trim();

if (!SPREADSHEET_ID) {
  throw new Error("Missing GOOGLE_SHEETS_SPREADSHEET_ID");
}
if (!SERVICE_ACCOUNT_EMAIL) {
  throw new Error("Missing GOOGLE_SERVICE_ACCOUNT_EMAIL");
}
if (!SERVICE_ACCOUNT_PRIVATE_KEY) {
  throw new Error("Missing GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY");
}

const db = new sqlite3.Database(dbFullPath);

function allAsync(sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (err, rows) => {
      if (err) return reject(err);
      return resolve(rows || []);
    });
  });
}

async function getColumns(tableName) {
  const columns = await allAsync(`PRAGMA table_info(${tableName})`);
  return columns.map((c) => String(c.name).trim()).filter(Boolean);
}

async function getTableData(tableName, orderBy) {
  const columns = await getColumns(tableName);
  if (!columns.length) {
    return { columns: [], rows: [] };
  }

  const selectColumns = columns.map((c) => `"${c}"`).join(", ");
  const hasDeletedAt = columns.includes("deleted_at");
  const whereClause = hasDeletedAt ? "WHERE deleted_at IS NULL" : "";
  const rows = await allAsync(
    `SELECT ${selectColumns} FROM ${tableName} ${whereClause} ORDER BY ${orderBy}`
  );
  return { columns, rows };
}

function toSheetValues(columns, rows) {
  const header = columns;
  const body = rows.map((row) =>
    columns.map((col) => {
      const value = row[col];
      if (value === null || value === undefined) return "";
      if (typeof value === "object") return JSON.stringify(value);
      return String(value);
    })
  );
  return [header, ...body];
}

async function ensureSheetExists(sheets, spreadsheetId, title) {
  const meta = await sheets.spreadsheets.get({ spreadsheetId });
  const existing = (meta.data.sheets || []).some((s) => s.properties?.title === title);
  if (existing) return;

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId,
    requestBody: {
      requests: [
        {
          addSheet: {
            properties: {
              title,
              gridProperties: { frozenRowCount: 1 },
            },
          },
        },
      ],
    },
  });
}

async function writeSheet(sheets, spreadsheetId, sheetName, values) {
  await ensureSheetExists(sheets, spreadsheetId, sheetName);
  await sheets.spreadsheets.values.clear({
    spreadsheetId,
    range: `${sheetName}!A:ZZ`,
  });
  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range: `${sheetName}!A1`,
    valueInputOption: "RAW",
    requestBody: { values },
  });
}

async function main() {
  try {
    const auth = new google.auth.JWT({
      email: SERVICE_ACCOUNT_EMAIL,
      key: SERVICE_ACCOUNT_PRIVATE_KEY,
      scopes: ["https://www.googleapis.com/auth/spreadsheets"],
    });
    const sheets = google.sheets({ version: "v4", auth });

    const [matchesData, lineupsData] = await Promise.all([
      getTableData("matches", "time_utc DESC, id ASC"),
      getTableData("lineups", "match_id ASC, id ASC"),
    ]);

    const matchesValues = toSheetValues(matchesData.columns, matchesData.rows);
    const lineupsValues = toSheetValues(lineupsData.columns, lineupsData.rows);

    await writeSheet(sheets, SPREADSHEET_ID, MATCHES_SHEET, matchesValues);
    await writeSheet(sheets, SPREADSHEET_ID, LINEUPS_SHEET, lineupsValues);

    console.log(
      `Synced: matches=${matchesData.rows.length}, lineups=${lineupsData.rows.length}, spreadsheet=${SPREADSHEET_ID}`
    );
  } finally {
    db.close();
  }
}

main().catch((err) => {
  console.error("Google Sheets sync failed", err);
  process.exitCode = 1;
});
