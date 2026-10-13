// Public read-only lookup for Sheets. Bound BGA work and cache successful results.
export function registerBgaPlayerAvatarRoutes(app, { fetchBgaPlayerAvatar, now = Date.now }) {
  const cache = new Map();
  const pending = new Map();
  let lastStartedAt = -Infinity;
  const ttlMs = 60 * 60 * 1000;

  // /profiles/ already passes through the production nginx proxy.
  // Keep the earlier path as an alias for installations that proxy /bga/ too.
  app.get(["/profiles/bga-player-avatar", "/bga/player-avatar"], async (req, res) => {
    if (typeof req.query?.player !== "string") {
      return res.status(400).json({ ok: false, message: "Provide a BGA player ID or nickname" });
    }
    const player = req.query.player.trim();
    if (!player || player.length > 100 || /[\u0000-\u001f\u007f]/.test(player)) {
      return res.status(400).json({ ok: false, message: "Invalid BGA player ID or nickname" });
    }
    const key = player.toLowerCase();
    const cached = cache.get(key);
    if (cached && now() < cached.expiresAt) return res.json(cached.data);
    cache.delete(key);

    if (!pending.has(key)) {
      if (pending.size || now() - lastStartedAt < 1000) {
        res.set("Retry-After", "2");
        return res.status(429).json({ ok: false, message: "BGA avatar lookup is busy. Please retry in 2 seconds." });
      }
      lastStartedAt = now();
      pending.set(key, Promise.resolve().then(() => fetchBgaPlayerAvatar(player)));
    }
    try {
      const data = await pending.get(key);
      if (!data?.ok) {
        return res.status(data?.not_found ? 404 : 502).json({
          ok: false, message: data?.message || "Could not load the BGA avatar. Please try again.",
        });
      }
      if (!/^\d+$/.test(String(data.player_id || "")) || !Object.hasOwn(data, "avatar")) {
        return res.status(502).json({ ok: false, message: "Invalid BGA avatar response" });
      }
      if (cache.size >= 1000) cache.delete(cache.keys().next().value);
      cache.set(key, { data, expiresAt: now() + ttlMs });
      return res.json(data);
    } catch (_error) {
      return res.status(502).json({ ok: false, message: "Could not load the BGA avatar. Please try again." });
    } finally {
      pending.delete(key);
    }
  });
}
