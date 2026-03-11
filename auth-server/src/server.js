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
const SITE_BASE_URL = process.env.SITE_BASE_URL || "https://carcassonne.gg";
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

function addColumnIfMissing(columns, tableName, columnName, sqlDefinition) {
  if (columns.some((col) => col.name === columnName)) return;

  db.run(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${sqlDefinition}`, (alterErr) => {
    if (alterErr) {
      console.error(`Failed to add ${columnName} column to ${tableName}`, alterErr);
    }
  });
}

function ensureProfilesSchema() {
  db.all("PRAGMA table_info(profiles)", (pragmaErr, columns) => {
    if (pragmaErr) {
      console.error("Failed to inspect profiles schema", pragmaErr);
      return;
    }

    addColumnIfMissing(columns, "profiles", "player_id", "TEXT");
    addColumnIfMissing(columns, "profiles", "admin", "INTEGER NOT NULL DEFAULT 0");
    addColumnIfMissing(columns, "profiles", "bga_nickname", "TEXT");
    addColumnIfMissing(columns, "profiles", "name", "TEXT");
    addColumnIfMissing(columns, "profiles", "association", "TEXT");
    addColumnIfMissing(columns, "profiles", "master_title", "INTEGER NOT NULL DEFAULT 0");
    addColumnIfMissing(columns, "profiles", "master_title_date", "DATE");
    addColumnIfMissing(columns, "profiles", "team_captain", "INTEGER NOT NULL DEFAULT 0");
    addColumnIfMissing(columns, "profiles", "telegram", "TEXT");
    addColumnIfMissing(columns, "profiles", "whatsapp", "TEXT");
    addColumnIfMissing(columns, "profiles", "discord", "TEXT");
    addColumnIfMissing(columns, "profiles", "instagram", "TEXT");
    addColumnIfMissing(columns, "profiles", "contact_email", "TEXT");
    addColumnIfMissing(columns, "profiles", "created_at", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP");
    addColumnIfMissing(columns, "profiles", "updated_at", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP");
  });
}

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

  db.get(
    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'profiles'",
    (profilesCheckErr, profilesTable) => {
      if (profilesCheckErr) {
        console.error("Failed to check profiles table", profilesCheckErr);
        return;
      }

      db.get(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'user_player_links'",
        (linksCheckErr, legacyTable) => {
          if (linksCheckErr) {
            console.error("Failed to check user_player_links table", linksCheckErr);
            return;
          }

          const shouldRenameLegacyTable = !profilesTable && legacyTable;

          if (shouldRenameLegacyTable) {
            db.run("ALTER TABLE user_player_links RENAME TO profiles", (renameErr) => {
              if (renameErr) {
                console.error("Failed to rename user_player_links to profiles", renameErr);
                return;
              }
              ensureProfilesSchema();
            });
            return;
          }

          db.run(`
            CREATE TABLE IF NOT EXISTS profiles (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              email TEXT NOT NULL UNIQUE COLLATE NOCASE,
              bga_nickname TEXT,
              name TEXT,
              association TEXT,
              master_title INTEGER NOT NULL DEFAULT 0,
              master_title_date DATE,
              team_captain INTEGER NOT NULL DEFAULT 0,
              telegram TEXT,
              whatsapp TEXT,
              discord TEXT,
              instagram TEXT,
              contact_email TEXT,
              player_id TEXT,
              admin INTEGER NOT NULL DEFAULT 0,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
          `);

          ensureProfilesSchema();

          db.run(`
            CREATE TABLE IF NOT EXISTS associations (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              name TEXT NOT NULL UNIQUE COLLATE NOCASE,
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
          `);
        }
      );
    }
  );
});

passport.serializeUser((user, done) => {
  done(null, user.id);
});

passport.deserializeUser((id, done) => {
  db.get(
    `
      SELECT
        u.id,
        u.google_id,
        u.email,
        u.name,
        u.picture,
        (
          SELECT p.player_id
          FROM profiles p
          WHERE lower(p.email) = lower(u.email)
          ORDER BY p.updated_at DESC, p.player_id ASC
          LIMIT 1
        ) AS player_id,
        COALESCE((
          SELECT p.admin
          FROM profiles p
          WHERE lower(p.email) = lower(u.email)
          ORDER BY p.updated_at DESC, p.player_id ASC
          LIMIT 1
        ), 0) AS admin,
        (
          SELECT p.bga_nickname
          FROM profiles p
          WHERE lower(p.email) = lower(u.email)
          ORDER BY p.updated_at DESC, p.player_id ASC
          LIMIT 1
        ) AS bga_nickname,
        (
          SELECT p.association
          FROM profiles p
          WHERE lower(p.email) = lower(u.email)
          ORDER BY p.updated_at DESC, p.player_id ASC
          LIMIT 1
        ) AS association,
        COALESCE((
          SELECT p.master_title
          FROM profiles p
          WHERE lower(p.email) = lower(u.email)
          ORDER BY p.updated_at DESC, p.player_id ASC
          LIMIT 1
        ), 0) AS master_title,
        (
          SELECT p.master_title_date
          FROM profiles p
          WHERE lower(p.email) = lower(u.email)
          ORDER BY p.updated_at DESC, p.player_id ASC
          LIMIT 1
        ) AS master_title_date,
        COALESCE((
          SELECT p.team_captain
          FROM profiles p
          WHERE lower(p.email) = lower(u.email)
          ORDER BY p.updated_at DESC, p.player_id ASC
          LIMIT 1
        ), 0) AS team_captain
      FROM users u
      WHERE u.id = ?
    `,
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

function requireAdmin(req, res, next) {
  if (!req.user) {
    return res.status(401).json({ ok: false, message: "Unauthorized" });
  }
  if (Number(req.user.admin) !== 1) {
    return res.status(403).json({ ok: false, message: "Forbidden" });
  }
  return next();
}

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

app.get("/profiles/public", (_req, res, next) => {
  db.all(
    `
      SELECT
        player_id,
        name,
        association,
        email,
        COALESCE(master_title, 0) AS master_title,
        master_title_date,
        COALESCE(team_captain, 0) AS team_captain
      FROM profiles
      WHERE trim(COALESCE(player_id, '')) <> ''
    `,
    (err, rows) => {
      if (err) return next(err);
      return res.json({ ok: true, profiles: rows || [] });
    }
  );
});

app.get("/profiles/contacts/:playerId", (req, res) => {
  const requestedPlayerId = String(req.params.playerId || "").trim();
  if (!requestedPlayerId) {
    return res.status(400).json({ ok: false, message: "playerId is required" });
  }

  if (!req.user) {
    return res.status(401).json({ ok: false, message: "Unauthorized" });
  }

  const linkedPlayerId = String(req.user.player_id || "").trim();
  if (!linkedPlayerId) {
    return res.status(403).json({ ok: false, message: "BGA profile is not linked" });
  }

  return db.get(
    `
      SELECT
        telegram,
        whatsapp,
        discord,
        instagram,
        contact_email
      FROM profiles
      WHERE player_id = ?
      LIMIT 1
    `,
    [requestedPlayerId],
    (err, row) => {
      if (err) {
        return res.status(500).json({ ok: false, message: "Failed to load contacts" });
      }
      if (!row) {
        return res.status(404).json({ ok: false, message: "Profile not found" });
      }
      return res.json({
        ok: true,
        contacts: {
          telegram: row.telegram || null,
          whatsapp: row.whatsapp || null,
          discord: row.discord || null,
          instagram: row.instagram || null,
          contact_email: row.contact_email || null,
        },
      });
    }
  );
});

app.get("/associations", (_req, res, next) => {
  db.all(
    `
      SELECT id, name
      FROM associations
      WHERE trim(COALESCE(name, '')) <> ''
      ORDER BY name COLLATE NOCASE ASC
    `,
    (err, rows) => {
      if (err) return next(err);
      return res.json({ ok: true, associations: rows || [] });
    }
  );
});

app.post("/associations", requireAdmin, (req, res) => {
  const name = String(req.body?.name || "").trim();
  if (!name) {
    return res.status(400).json({ ok: false, message: "name is required" });
  }

  return db.run(
    `
      INSERT INTO associations (name, created_at, updated_at)
      VALUES (?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    `,
    [name],
    function onInsert(err) {
      if (err) {
        if (String(err.message || "").includes("UNIQUE")) {
          return res.status(409).json({ ok: false, message: "Association already exists" });
        }
        return res.status(500).json({ ok: false, message: "Failed to create association" });
      }

      return db.get(
        "SELECT id, name FROM associations WHERE id = ? LIMIT 1",
        [this.lastID],
        (selectErr, row) => {
          if (selectErr) {
            return res.status(500).json({ ok: false, message: "Failed to load association" });
          }
          return res.json({ ok: true, association: row || null });
        }
      );
    }
  );
});

app.patch("/associations/:id", requireAdmin, (req, res) => {
  const associationId = Number(req.params.id);
  if (!Number.isInteger(associationId) || associationId <= 0) {
    return res.status(400).json({ ok: false, message: "Invalid association id" });
  }

  const name = String(req.body?.name || "").trim();
  if (!name) {
    return res.status(400).json({ ok: false, message: "name is required" });
  }

  return db.run(
    `
      UPDATE associations
      SET name = ?, updated_at = CURRENT_TIMESTAMP
      WHERE id = ?
    `,
    [name, associationId],
    function onUpdate(err) {
      if (err) {
        if (String(err.message || "").includes("UNIQUE")) {
          return res.status(409).json({ ok: false, message: "Association already exists" });
        }
        return res.status(500).json({ ok: false, message: "Failed to update association" });
      }
      if (!this || this.changes === 0) {
        return res.status(404).json({ ok: false, message: "Association not found" });
      }

      return db.get(
        "SELECT id, name FROM associations WHERE id = ? LIMIT 1",
        [associationId],
        (selectErr, row) => {
          if (selectErr) {
            return res.status(500).json({ ok: false, message: "Failed to load association" });
          }
          return res.json({ ok: true, association: row || null });
        }
      );
    }
  );
});

app.delete("/associations/:id", requireAdmin, (req, res) => {
  const associationId = Number(req.params.id);
  if (!Number.isInteger(associationId) || associationId <= 0) {
    return res.status(400).json({ ok: false, message: "Invalid association id" });
  }

  return db.run(
    "DELETE FROM associations WHERE id = ?",
    [associationId],
    function onDelete(err) {
      if (err) {
        return res.status(500).json({ ok: false, message: "Failed to delete association" });
      }
      if (!this || this.changes === 0) {
        return res.status(404).json({ ok: false, message: "Association not found" });
      }
      return res.json({ ok: true });
    }
  );
});

app.get("/tournaments", (_req, res, next) => {
  db.all(
    `
      SELECT
        id,
        name,
        short_title,
        logo,
        link
      FROM tournaments
      ORDER BY id COLLATE NOCASE ASC
    `,
    (err, rows) => {
      if (err) return next(err);
      return res.json({ ok: true, tournaments: rows || [] });
    }
  );
});

app.get("/teams", (_req, res, next) => {
  db.all(
    `
      SELECT
        id,
        name,
        logo,
        type
      FROM teams
      ORDER BY name COLLATE NOCASE ASC
    `,
    (err, rows) => {
      if (err) return next(err);
      return res.json({ ok: true, teams: rows || [] });
    }
  );
});

app.get("/matches", (_req, res, next) => {
  db.all(
    `
      SELECT
        id,
        tournament_id,
        time_utc,
        lineup_type,
        lineup_deadline_utc,
        number_of_duels,
        team_1,
        team_2,
        status,
        dw1,
        dw2,
        gw1,
        gw2
      FROM matches
      ORDER BY time_utc DESC, id ASC
    `,
    (err, rows) => {
      if (err) return next(err);
      return res.json({ ok: true, matches: rows || [] });
    }
  );
});

app.get("/auth/me", (req, res) => {
  if (!req.user) {
    return res.status(401).json({ authenticated: false });
  }

  const {
    id,
    google_id,
    email,
    name,
    picture,
    player_id,
    admin,
    bga_nickname,
    association,
    master_title,
    master_title_date,
    team_captain,
  } = req.user;
  const myProfileUrl = player_id
    ? `${SITE_BASE_URL}/player/?id=${encodeURIComponent(player_id)}`
    : `${SITE_BASE_URL}/player/?id=noprofile`;
  const isAdmin = Number(admin) === 1;

  return res.json({
    authenticated: true,
    user: {
      id,
      googleId: google_id,
      email,
      name,
      picture,
      playerId: player_id || null,
      isAdmin,
      myProfileUrl,
      adminPanelUrl: isAdmin ? `${SITE_BASE_URL}/admin` : null,
      profile: {
        bgaNickname: bga_nickname || null,
        name: name || null,
        association: association || null,
        email: email || null,
        masterTitle: Number(master_title) === 1,
        masterTitleDate: master_title_date || null,
        teamCaptain: Number(team_captain) === 1,
      },
    },
  });
});

app.get("/auth/can-edit-player/:playerId", (req, res) => {
  if (!req.user) {
    return res.status(401).json({ authenticated: false, canEdit: false });
  }

  const requestedPlayerId = String(req.params.playerId || "").trim();
  const linkedPlayerId = String(req.user.player_id || "").trim();
  const isAdmin = Number(req.user.admin) === 1;
  const isTeamCaptain = Number(req.user.team_captain) === 1;
  const userAssociation = String(req.user.association || "").trim().toLowerCase();

  if (!requestedPlayerId) {
    return res.json({
      authenticated: true,
      canEdit: false,
      isAdmin,
      isTeamCaptain,
      requestedPlayerId,
      linkedPlayerId: linkedPlayerId || null,
    });
  }

  // Rule 1: admin can edit any profile.
  if (isAdmin) {
    return res.json({
      authenticated: true,
      canEdit: true,
      isAdmin,
      isTeamCaptain,
      requestedPlayerId,
      linkedPlayerId: linkedPlayerId || null,
    });
  }

  // Rule 2: owner can edit own profile.
  if (requestedPlayerId === linkedPlayerId) {
    return res.json({
      authenticated: true,
      canEdit: true,
      isAdmin,
      isTeamCaptain,
      requestedPlayerId,
      linkedPlayerId: linkedPlayerId || null,
    });
  }

  // Rule 3: team captain can edit profiles with the same association.
  if (isTeamCaptain && userAssociation) {
    return db.get(
      "SELECT association FROM profiles WHERE player_id = ? LIMIT 1",
      [requestedPlayerId],
      (targetErr, targetRow) => {
        if (targetErr) return res.status(500).json({ authenticated: true, canEdit: false });
        const targetAssociation = String(targetRow?.association || "").trim().toLowerCase();
        const canEdit = targetAssociation !== "" && targetAssociation === userAssociation;
        return res.json({
          authenticated: true,
          canEdit,
          isAdmin,
          isTeamCaptain,
          requestedPlayerId,
          linkedPlayerId: linkedPlayerId || null,
        });
      }
    );
  }

  return res.json({
    authenticated: true,
    canEdit: false,
    isAdmin,
    isTeamCaptain,
    requestedPlayerId,
    linkedPlayerId: linkedPlayerId || null,
  });
});

app.patch("/profiles/:playerId", (req, res) => {
  if (!req.user) {
    return res.status(401).json({ ok: false, message: "Unauthorized" });
  }

  const requestedPlayerId = String(req.params.playerId || "").trim();
  if (!requestedPlayerId) {
    return res.status(400).json({ ok: false, message: "playerId is required" });
  }

  const linkedPlayerId = String(req.user.player_id || "").trim();
  const isAdmin = Number(req.user.admin) === 1;
  const isTeamCaptain = Number(req.user.team_captain) === 1;
  const userAssociation = String(req.user.association || "").trim().toLowerCase();

  const payload = req.body && typeof req.body === "object" ? req.body : {};

  const normalizeText = (value) => {
    const v = String(value ?? "").trim();
    return v === "" ? null : v;
  };

  const normalizeBool = (value) => {
    if (typeof value === "boolean") return value;
    if (typeof value === "number") return value === 1;
    const raw = String(value ?? "").trim().toLowerCase();
    return raw === "1" || raw === "true" || raw === "yes" || raw === "on";
  };

  const profilePatch = {
    name: normalizeText(payload.name),
    master_title: normalizeBool(payload.master_title) ? 1 : 0,
    master_title_date: normalizeText(payload.master_title_date),
    email: normalizeText(payload.email),
    association: normalizeText(payload.association),
    team_captain: normalizeBool(payload.team_captain) ? 1 : 0,
    telegram: normalizeText(payload.telegram),
    whatsapp: normalizeText(payload.whatsapp),
    discord: normalizeText(payload.discord),
    instagram: normalizeText(payload.instagram),
    contact_email: normalizeText(payload.contact_email),
  };

  if (profilePatch.master_title === 1 && !profilePatch.master_title_date) {
    return res.status(400).json({
      ok: false,
      message: "master_title_date is required when master_title is enabled",
    });
  }

  if (profilePatch.master_title === 0) {
    profilePatch.master_title_date = null;
  }

  if (isAdmin && !profilePatch.association) {
    return res.status(400).json({ ok: false, message: "association cannot be empty" });
  }

  const ownerCanEdit = requestedPlayerId === linkedPlayerId;
  const decideAndUpdate = (captainCanEdit) => {
    const canEdit = isAdmin || ownerCanEdit || captainCanEdit;
    if (!canEdit) {
      return res.status(403).json({ ok: false, message: "Forbidden" });
    }

    const allowedPatch = isAdmin
      ? profilePatch
      : captainCanEdit
        ? {
            name: profilePatch.name,
            master_title: profilePatch.master_title,
            master_title_date: profilePatch.master_title_date,
            email: profilePatch.email,
            telegram: profilePatch.telegram,
            whatsapp: profilePatch.whatsapp,
            discord: profilePatch.discord,
            instagram: profilePatch.instagram,
            contact_email: profilePatch.contact_email,
          }
        : {
            name: profilePatch.name,
            master_title: profilePatch.master_title,
            master_title_date: profilePatch.master_title_date,
            telegram: profilePatch.telegram,
            whatsapp: profilePatch.whatsapp,
            discord: profilePatch.discord,
            instagram: profilePatch.instagram,
            contact_email: profilePatch.contact_email,
          };

    const sql = isAdmin
      ? `
          UPDATE profiles
          SET
            name = ?,
            master_title = ?,
            master_title_date = ?,
            email = ?,
            association = ?,
            team_captain = ?,
            telegram = ?,
            whatsapp = ?,
            discord = ?,
            instagram = ?,
            contact_email = ?,
            updated_at = CURRENT_TIMESTAMP
          WHERE player_id = ?
        `
      : captainCanEdit
        ? `
          UPDATE profiles
          SET
            name = ?,
            master_title = ?,
            master_title_date = ?,
            email = ?,
            telegram = ?,
            whatsapp = ?,
            discord = ?,
            instagram = ?,
            contact_email = ?,
            updated_at = CURRENT_TIMESTAMP
          WHERE player_id = ?
        `
      : `
          UPDATE profiles
          SET
            name = ?,
            master_title = ?,
            master_title_date = ?,
            telegram = ?,
            whatsapp = ?,
            discord = ?,
            instagram = ?,
            contact_email = ?,
            updated_at = CURRENT_TIMESTAMP
          WHERE player_id = ?
        `;

    const params = isAdmin
      ? [
          allowedPatch.name,
          allowedPatch.master_title,
          allowedPatch.master_title_date,
          allowedPatch.email,
          allowedPatch.association,
          allowedPatch.team_captain,
          allowedPatch.telegram,
          allowedPatch.whatsapp,
          allowedPatch.discord,
          allowedPatch.instagram,
          allowedPatch.contact_email,
          requestedPlayerId,
        ]
      : captainCanEdit
        ? [
          allowedPatch.name,
          allowedPatch.master_title,
          allowedPatch.master_title_date,
          allowedPatch.email,
          allowedPatch.telegram,
          allowedPatch.whatsapp,
          allowedPatch.discord,
          allowedPatch.instagram,
          allowedPatch.contact_email,
          requestedPlayerId,
        ]
      : [
          allowedPatch.name,
          allowedPatch.master_title,
          allowedPatch.master_title_date,
          allowedPatch.telegram,
          allowedPatch.whatsapp,
          allowedPatch.discord,
          allowedPatch.instagram,
          allowedPatch.contact_email,
          requestedPlayerId,
        ];

    return db.run(sql, params, function onUpdate(updateErr) {
      if (updateErr) {
        return res.status(500).json({ ok: false, message: "Failed to update profile" });
      }
      if (!this || this.changes === 0) {
        return res.status(404).json({ ok: false, message: "Profile not found" });
      }

      return db.get(
        `
          SELECT
            player_id,
            name,
            association,
            email,
            COALESCE(master_title, 0) AS master_title,
            master_title_date,
            COALESCE(team_captain, 0) AS team_captain,
            telegram,
            whatsapp,
            discord,
            instagram,
            contact_email
          FROM profiles
          WHERE player_id = ?
          LIMIT 1
        `,
        [requestedPlayerId],
        (selectErr, row) => {
          if (selectErr) {
            return res.status(500).json({ ok: false, message: "Failed to load updated profile" });
          }
          return res.json({ ok: true, profile: row || null });
        }
      );
    });
  };

  if (isTeamCaptain && userAssociation && !isAdmin) {
    if (ownerCanEdit) {
      return decideAndUpdate(true);
    }
    return db.get(
      "SELECT association FROM profiles WHERE player_id = ? LIMIT 1",
      [requestedPlayerId],
      (targetErr, targetRow) => {
        if (targetErr) {
          return res.status(500).json({ ok: false, message: "Failed to validate access" });
        }
        const targetAssociation = String(targetRow?.association || "").trim().toLowerCase();
        const captainCanEdit = targetAssociation !== "" && targetAssociation === userAssociation;
        return decideAndUpdate(captainCanEdit);
      }
    );
  }

  return decideAndUpdate(false);
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
