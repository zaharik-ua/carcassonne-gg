import path from "node:path";
import express from "express";
import cors from "cors";
import session from "express-session";
import connectSqlite3 from "connect-sqlite3";
import { config } from "./config.js";
import { dbRun, ensureCoreSchema } from "./db.js";
import { ensureProjectSchema } from "./project-schema.js";
import { configurePassport, passport } from "./auth.js";
import { authRouter } from "./routes/auth.js";
import { profileRouter } from "./routes/profile.js";

await ensureCoreSchema();
await ensureProjectSchema({ dbRun });
configurePassport();

const app = express();
const SQLiteStore = connectSqlite3(session);

app.set("trust proxy", 1);
app.use(express.json());
app.use(
  cors({
    origin: (origin, callback) => {
      if (!origin || config.FRONTEND_ORIGINS.includes(origin)) {
        return callback(null, true);
      }
      return callback(new Error(`CORS blocked for origin: ${origin}`));
    },
    credentials: true,
  })
);

app.use(
  session({
    store: new SQLiteStore({
      db: "sessions.sqlite",
      dir: path.dirname(config.DB_PATH),
    }),
    secret: config.SESSION_SECRET,
    resave: false,
    saveUninitialized: false,
    cookie: {
      httpOnly: true,
      secure: config.COOKIE_SECURE,
      sameSite: config.COOKIE_SAME_SITE,
      maxAge: 1000 * 60 * 60 * 24 * 30,
    },
  })
);

app.use(passport.initialize());
app.use(passport.session());

app.use((req, _res, next) => {
  const userId = Number(req.user?.id);
  if (!Number.isInteger(userId) || userId <= 0) {
    next();
    return;
  }

  dbRun(
    `
      UPDATE users
      SET
        last_login = CURRENT_TIMESTAMP,
        updated_at = CURRENT_TIMESTAMP
      WHERE id = ?
        AND (
          last_login IS NULL
          OR datetime(last_login) < datetime('now', '-5 minutes')
        )
    `,
    [userId]
  ).then(
    () => next(),
    () => next()
  );
});

app.get("/health", (_req, res) => {
  res.json({ ok: true });
});

app.use("/auth", authRouter);
app.use("/profile", profileRouter);

app.use((err, _req, res, _next) => {
  console.error(err);
  const status = Number.isInteger(err?.status) ? err.status : 500;
  res.status(status).json({
    ok: false,
    message: status === 500 ? "Internal server error" : err.message,
  });
});

app.listen(config.PORT, () => {
  console.log(`Auth/profile server running on port ${config.PORT}`);
});
