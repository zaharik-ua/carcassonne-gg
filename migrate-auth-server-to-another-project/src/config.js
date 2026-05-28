import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

dotenv.config();

const requiredEnv = [
  "SESSION_SECRET",
  "GOOGLE_CLIENT_ID",
  "GOOGLE_CLIENT_SECRET",
  "GOOGLE_CALLBACK_URL",
];

for (const envName of requiredEnv) {
  if (!process.env[envName]) {
    throw new Error(`Missing required env var: ${envName}`);
  }
}

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, "..");
const dbPath = process.env.DB_PATH || "./data/app.sqlite";

const defaultFrontendOrigins = [
  "http://localhost:8080",
  "http://127.0.0.1:8080",
  "http://localhost:5500",
  "http://127.0.0.1:5500",
];

const frontendOrigins = Array.from(
  new Set(
    (process.env.FRONTEND_ORIGIN || "")
      .split(",")
      .map((origin) => origin.trim())
      .filter(Boolean)
      .concat(defaultFrontendOrigins)
  )
);

const isProd = process.env.NODE_ENV === "production";
const cookieSameSite = process.env.COOKIE_SAME_SITE || (isProd ? "none" : "lax");
const cookieSecure = process.env.COOKIE_SECURE
  ? process.env.COOKIE_SECURE === "true"
  : isProd;

if (cookieSameSite === "none" && !cookieSecure) {
  throw new Error("COOKIE_SAME_SITE=none requires COOKIE_SECURE=true");
}

export const config = {
  PORT: Number(process.env.PORT || 3100),
  PROJECT_ROOT: projectRoot,
  DB_PATH: path.isAbsolute(dbPath) ? dbPath : path.resolve(projectRoot, dbPath),
  FRONTEND_ORIGINS: frontendOrigins,
  PRIMARY_FRONTEND_ORIGIN: frontendOrigins[0] || "http://localhost:8080",
  SITE_BASE_URL: process.env.SITE_BASE_URL || frontendOrigins[0] || "http://localhost:8080",
  SESSION_SECRET: process.env.SESSION_SECRET,
  COOKIE_SAME_SITE: cookieSameSite,
  COOKIE_SECURE: cookieSecure,
  GOOGLE_CLIENT_ID: process.env.GOOGLE_CLIENT_ID,
  GOOGLE_CLIENT_SECRET: process.env.GOOGLE_CLIENT_SECRET,
  GOOGLE_CALLBACK_URL: process.env.GOOGLE_CALLBACK_URL,
  ADMIN_EMAILS: new Set(
    (process.env.ADMIN_EMAILS || "")
      .split(",")
      .map((email) => email.trim().toLowerCase())
      .filter(Boolean)
  ),
};
