export function requireAuthenticated(req, res, next) {
  if (!req.user) {
    return res.status(401).json({ ok: false, message: "Unauthorized" });
  }
  return next();
}

export function requireAdmin(req, res, next) {
  if (!req.user) {
    return res.status(401).json({ ok: false, message: "Unauthorized" });
  }
  if (Number(req.user.admin) !== 1) {
    return res.status(403).json({ ok: false, message: "Forbidden" });
  }
  return next();
}
