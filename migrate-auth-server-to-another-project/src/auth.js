import passport from "passport";
import { Strategy as GoogleStrategy } from "passport-google-oauth20";
import { config } from "./config.js";
import { dbGet, dbRun, loadUserWithProfile } from "./db.js";

function normalizeEmail(value) {
  return String(value || "").trim().toLowerCase();
}

async function ensureProfileForUser(user) {
  const profile = await dbGet("SELECT id FROM profiles WHERE user_id = ? LIMIT 1", [user.id]);
  if (profile) return;

  await dbRun(
    `
      INSERT INTO profiles (user_id, display_name, avatar_url)
      VALUES (?, ?, ?)
    `,
    [user.id, user.name || null, user.picture || null]
  );
}

export function configurePassport() {
  passport.serializeUser((user, done) => {
    done(null, user.id);
  });

  passport.deserializeUser(async (id, done) => {
    try {
      const user = await loadUserWithProfile(id);
      done(null, user || false);
    } catch (error) {
      done(error);
    }
  });

  passport.use(
    new GoogleStrategy(
      {
        clientID: config.GOOGLE_CLIENT_ID,
        clientSecret: config.GOOGLE_CLIENT_SECRET,
        callbackURL: config.GOOGLE_CALLBACK_URL,
      },
      async (_accessToken, _refreshToken, profile, done) => {
        try {
          const googleId = profile.id;
          const email = profile.emails?.[0]?.value || null;
          const name = profile.displayName || null;
          const picture = profile.photos?.[0]?.value || null;
          const isSeedAdmin = config.ADMIN_EMAILS.has(normalizeEmail(email));

          await dbRun(
            `
              INSERT INTO users (google_id, email, name, picture, admin, last_login)
              VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
              ON CONFLICT(google_id)
              DO UPDATE SET
                email = excluded.email,
                name = excluded.name,
                picture = excluded.picture,
                admin = CASE
                  WHEN excluded.admin = 1 THEN 1
                  ELSE COALESCE(users.admin, 0)
                END,
                last_login = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            `,
            [googleId, email, name, picture, isSeedAdmin ? 1 : 0]
          );

          const user = await dbGet("SELECT * FROM users WHERE google_id = ? LIMIT 1", [googleId]);
          await ensureProfileForUser(user);
          done(null, await loadUserWithProfile(user.id));
        } catch (error) {
          done(error);
        }
      }
    )
  );

  return passport;
}

export { passport };
