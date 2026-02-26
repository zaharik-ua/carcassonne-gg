import path from "node:path";
import { fileURLToPath } from "node:url";
import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import session from "express-session";
import sqlite3 from "sqlite3";
import passport from "passport";
import { Strategy as GoogleStrategy } from "passport-google-oauth20";
import connectSqlite3 from "connect-sqlite3";

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

const PORT = Number(process.env.PORT || 3100);
const FRONTEND_ORIGINS = (process.env.FRONTEND_ORIGIN || "http://localhost:8080")
  .split(",")
  .map((origin) => origin.trim())
  .filter(Boolean);
const PRIMARY_FRONTEND_ORIGIN = FRONTEND_ORIGINS[0];
const DB_PATH = process.env.DB_PATH || "./data/auth.sqlite";
const isProd = process.env.NODE_ENV === "production";
const cookieSameSite = process.env.COOKIE_SAME_SITE || (isProd ? "none" : "lax");
const cookieSecure = process.env.COOKIE_SECURE
  ? process.env.COOKIE_SECURE === "true"
  : isProd;

if (cookieSameSite === "none" && !cookieSecure) {
  throw new Error("COOKIE_SAME_SITE=none requires COOKIE_SECURE=true");
}

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const dbFullPath = path.resolve(__dirname, "..", DB_PATH);

const db = new sqlite3.Database(dbFullPath);

db.serialize(() => {
  db.run(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      google_id TEXT NOT NULL UNIQUE,
      email TEXT,
      name TEXT,
      picture TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
  `);
});

passport.serializeUser((user, done) => {
  done(null, user.id);
});

passport.deserializeUser((id, done) => {
  db.get(
    "SELECT id, google_id, email, name, picture FROM users WHERE id = ?",
    [id],
    (err, row) => {
      if (err) return done(err);
      return done(null, row || false);
    }
  );
});

passport.use(
  new GoogleStrategy(
    {
      clientID: process.env.GOOGLE_CLIENT_ID,
      clientSecret: process.env.GOOGLE_CLIENT_SECRET,
      callbackURL: process.env.GOOGLE_CALLBACK_URL,
    },
    (accessToken, refreshToken, profile, done) => {
      const googleId = profile.id;
      const email = profile.emails?.[0]?.value || null;
      const name = profile.displayName || null;
      const picture = profile.photos?.[0]?.value || null;

      db.run(
        `
          INSERT INTO users (google_id, email, name, picture)
          VALUES (?, ?, ?, ?)
          ON CONFLICT(google_id)
          DO UPDATE SET
            email = excluded.email,
            name = excluded.name,
            picture = excluded.picture,
            updated_at = CURRENT_TIMESTAMP
        `,
        [googleId, email, name, picture],
        (insertErr) => {
          if (insertErr) return done(insertErr);

          db.get(
            "SELECT id, google_id, email, name, picture FROM users WHERE google_id = ?",
            [googleId],
            (selectErr, row) => {
              if (selectErr) return done(selectErr);
              return done(null, row);
            }
          );
        }
      );
    }
  )
);

const app = express();
const SQLiteStore = connectSqlite3(session);

app.set("trust proxy", 1);
app.use(express.json());
app.use(
  cors({
    origin: (origin, callback) => {
      if (!origin || FRONTEND_ORIGINS.includes(origin)) {
        return callback(null, true);
      }
      return callback(new Error(`CORS blocked for origin: ${origin}`));
    },
    credentials: true,
  })
);

app.use(
  session({
    store: new SQLiteStore({ db: "sessions.sqlite", dir: path.dirname(dbFullPath) }),
    secret: process.env.SESSION_SECRET,
    resave: false,
    saveUninitialized: false,
    cookie: {
      httpOnly: true,
      secure: cookieSecure,
      sameSite: cookieSameSite,
      maxAge: 1000 * 60 * 60 * 24 * 30,
    },
  })
);

app.use(passport.initialize());
app.use(passport.session());

app.get("/health", (_req, res) => {
  res.json({ ok: true });
});

app.get(
  "/auth/google",
  passport.authenticate("google", {
    scope: ["profile", "email"],
    prompt: "select_account",
  })
);

app.get(
  "/auth/google/callback",
  passport.authenticate("google", {
    failureRedirect: "/auth/failure",
    session: true,
  }),
  (_req, res) => {
    res.type("html").send(`<!doctype html>
<html>
<head><meta charset="utf-8"><title>Auth complete</title></head>
<body>
<script>
  if (window.opener) {
    window.opener.postMessage({ type: "google-auth-success" }, "*");
    window.close();
  } else {
    window.location.href = "${PRIMARY_FRONTEND_ORIGIN}";
  }
</script>
</body>
</html>`);
  }
);

app.get("/auth/failure", (_req, res) => {
  res.status(401).json({ ok: false, message: "Google auth failed" });
});

app.get("/auth/me", (req, res) => {
  if (!req.user) {
    return res.status(401).json({ authenticated: false });
  }

  const { id, google_id, email, name, picture } = req.user;
  return res.json({
    authenticated: true,
    user: { id, googleId: google_id, email, name, picture },
  });
});

app.post("/auth/logout", (req, res, next) => {
  req.logout((logoutErr) => {
    if (logoutErr) return next(logoutErr);
    req.session.destroy((destroyErr) => {
      if (destroyErr) return next(destroyErr);
      res.clearCookie("connect.sid");
      return res.json({ ok: true });
    });
  });
});

app.listen(PORT, () => {
  console.log(`Auth server running on port ${PORT}`);
});
