// Public progress of one monobank jar, read through the owner's Personal API token.
// monobank allows client-info once per 60 s, so results are cached and served stale on errors.
const CLIENT_INFO_URL = "https://api.monobank.ua/personal/client-info";
const CURRENCY_CODES = { 980: "UAH", 840: "USD", 978: "EUR" };

function normalizeSendId(value) {
  return String(value || "").trim().replace(/^.*\/jar\//, "").replace(/^jar\//, "");
}

export function pickJar(clientInfo, sendId) {
  const wanted = normalizeSendId(sendId);
  const jar = (clientInfo?.jars || []).find((item) => normalizeSendId(item?.sendId) === wanted);
  if (!jar) return null;
  const balance = Number(jar.balance);
  const goal = Number(jar.goal);
  if (!Number.isFinite(balance)) return null;
  return {
    ok: true,
    balance: balance / 100,
    goal: Number.isFinite(goal) && goal > 0 ? goal / 100 : null,
    currency: CURRENCY_CODES[jar.currencyCode] || String(jar.currencyCode || ""),
    url: `https://send.monobank.ua/jar/${wanted}`,
  };
}

export function registerMonobankJarRoutes(app, {
  token,
  sendId,
  fetchImpl = globalThis.fetch,
  now = Date.now,
  ttlMs = 5 * 60 * 1000,
}) {
  let cached = null;
  let pending = null;
  let lastAttemptAt = -Infinity;

  async function load() {
    const response = await fetchImpl(CLIENT_INFO_URL, {
      headers: { "X-Token": token },
      signal: AbortSignal.timeout(10000),
    });
    if (!response.ok) throw new Error(`monobank client-info HTTP ${response.status}`);
    const jar = pickJar(await response.json(), sendId);
    if (!jar) throw new Error("monobank jar not found");
    return jar;
  }

  app.get("/public/donation-jar", async (_req, res) => {
    res.set("Cache-Control", "public, max-age=60");
    if (!token || !sendId) {
      return res.status(503).json({ ok: false, message: "Donation jar is not configured" });
    }
    const fresh = cached && now() - cached.at < ttlMs;
    if (!fresh && !pending && now() - lastAttemptAt >= 61000) {
      lastAttemptAt = now();
      pending = load()
        .then((data) => { cached = { data, at: now() }; })
        .catch((error) => { console.error("[donation-jar]", error.message); })
        .finally(() => { pending = null; });
    }
    if (pending && !cached) await pending;
    if (!cached) {
      return res.status(502).json({ ok: false, message: "Could not load the donation jar" });
    }
    return res.json({ ...cached.data, updated_at: new Date(cached.at).toISOString() });
  });
}
