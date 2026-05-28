import express from "express";
import { config } from "../config.js";
import { passport } from "../auth.js";

export const authRouter = express.Router();

function toUserPayload(user) {
  const isAdmin = Number(user.admin) === 1;
  return {
    id: user.id,
    user_id: user.id,
    googleId: user.google_id,
    google_id: user.google_id,
    email: user.email || null,
    name: user.name || null,
    picture: user.picture || null,
    isAdmin,
    admin: isAdmin ? 1 : 0,
    profile: {
      id: user.profile_id || null,
      displayName: user.display_name || null,
      display_name: user.display_name || null,
      avatarUrl: user.avatar_url || null,
      avatar_url: user.avatar_url || null,
      bio: user.bio || null,
      location: user.location || null,
      websiteUrl: user.website_url || null,
      website_url: user.website_url || null,
    },
  };
}

authRouter.get(
  "/google",
  passport.authenticate("google", {
    scope: ["profile", "email"],
    prompt: "select_account",
  })
);

authRouter.get(
  "/google/callback",
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
    window.location.href = "${config.PRIMARY_FRONTEND_ORIGIN}";
  }
</script>
</body>
</html>`);
  }
);

authRouter.get("/failure", (_req, res) => {
  res.status(401).json({ ok: false, message: "Google auth failed" });
});

authRouter.get("/me", (req, res) => {
  if (!req.user) {
    return res.status(401).json({ authenticated: false });
  }
  return res.json({
    authenticated: true,
    user: toUserPayload(req.user),
  });
});

authRouter.post("/logout", (req, res, next) => {
  req.logout((logoutErr) => {
    if (logoutErr) return next(logoutErr);
    req.session.destroy((destroyErr) => {
      if (destroyErr) return next(destroyErr);
      res.clearCookie("connect.sid");
      return res.json({ ok: true });
    });
  });
});
