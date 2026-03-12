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

function formatUtcForSheet(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  const ts = Date.parse(raw);
  if (!Number.isFinite(ts)) return raw;
  const dt = new Date(ts);
  const yyyy = String(dt.getUTCFullYear());
  const mm = String(dt.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(dt.getUTCDate()).padStart(2, "0");
  const hh = String(dt.getUTCHours()).padStart(2, "0");
  const mi = String(dt.getUTCMinutes()).padStart(2, "0");
  const ss = String(dt.getUTCSeconds()).padStart(2, "0");
  // Google Sheets reliably parses this as date-time with USER_ENTERED mode.
  return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}`;
}

async function getMatchesExportData() {
  const columns = [
    "id",
    "tournament_id",
    "time_utc",
    "team_1",
    "team_2",
  ];

  const rows = await allAsync(
    `
      SELECT
        m.id AS id,
        m.tournament_id AS tournament_id,
        m.time_utc AS time_utc,
        COALESCE(t1.name, m.team_1, '') AS team_1,
        COALESCE(t2.name, m.team_2, '') AS team_2
      FROM matches m
      LEFT JOIN teams t1 ON upper(trim(t1.id)) = upper(trim(m.team_1))
      LEFT JOIN teams t2 ON upper(trim(t2.id)) = upper(trim(m.team_2))
      WHERE m.deleted_at IS NULL
      ORDER BY m.time_utc DESC, m.id ASC
    `
  );

  const mappedRows = rows.map((row) => ({
    id: row.id,
    tournament_id: row.tournament_id,
    time_utc: formatUtcForSheet(row.time_utc),
    team_1: row.team_1,
    team_2: row.team_2,
  }));

  return { columns, rows: mappedRows };
}

async function getLineupsExportData() {
  const columns = [
    "id",
    "tournament_id",
    "match_id",
    "duel_format",
    "time_utc",
    "player_1_id",
    "player_1_name",
    "player_2_id",
    "player_2_name",
  ];

  const rows = await allAsync(
    `
      SELECT
        l.id AS id,
        l.tournament_id AS tournament_id,
        l.match_id AS match_id,
        l.duel_format AS duel_format,
        l.time_utc AS time_utc,
        l.player_1_id AS player_1_id,
        COALESCE((
          SELECT COALESCE(p.bga_nickname, p.name, '')
          FROM profiles p
          WHERE trim(p.id) = trim(l.player_1_id)
          ORDER BY p.updated_at DESC, p.id DESC
          LIMIT 1
        ), '') AS player_1_name,
        l.player_2_id AS player_2_id,
        COALESCE((
          SELECT COALESCE(p.bga_nickname, p.name, '')
          FROM profiles p
          WHERE trim(p.id) = trim(l.player_2_id)
          ORDER BY p.updated_at DESC, p.id DESC
          LIMIT 1
        ), '') AS player_2_name
      FROM lineups l
      WHERE l.deleted_at IS NULL
      ORDER BY l.match_id ASC, l.id ASC
    `
  );

  const mappedRows = rows.map((row) => ({
    id: row.id,
    tournament_id: row.tournament_id,
    match_id: row.match_id,
    duel_format: row.duel_format,
    time_utc: formatUtcForSheet(row.time_utc),
    player_1_id: row.player_1_id,
    player_1_name: row.player_1_name,
    player_2_id: row.player_2_id,
    player_2_name: row.player_2_name,
  }));

  return { columns, rows: mappedRows };
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
    valueInputOption: "USER_ENTERED",
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
      getMatchesExportData(),
      getLineupsExportData(),
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
