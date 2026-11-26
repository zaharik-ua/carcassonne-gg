// Public progress of one monobank jar, read through the owner's Personal API token.
// monobank allows client-info once per 60 s, so results are cached and served stale on errors.
const CLIENT_INFO_URL = "https://api.monobank.ua/personal/client-info";
const CURRENCY_URL = "https://api.monobank.ua/bank/currency";
const RATE_TTL_MS = 60 * 60 * 1000;
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
    currencyCode: Number(jar.currencyCode),
    url: `https://send.monobank.ua/jar/${wanted}`,
  };
}

// UAH per 1 USD at the bank's selling rate (what paying a USD bill costs).
export function pickUsdRate(rates, currencyCode) {
  const row = (Array.isArray(rates) ? rates : []).find(
    (item) => item?.currencyCodeA === 840 && item?.currencyCodeB === currencyCode
  );
  const rate = Number(row?.rateSell || row?.rateCross);
  return Number.isFinite(rate) && rate > 0 ? rate : null;
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
  let usdRate = null;

  async function loadUsdRate(currencyCode) {
    if (usdRate?.code === currencyCode && now() - usdRate.at < RATE_TTL_MS) return usdRate.value;
    try {
      const response = await fetchImpl(CURRENCY_URL, { signal: AbortSignal.timeout(10000) });
      if (!response.ok) throw new Error(`monobank currency HTTP ${response.status}`);
      const value = pickUsdRate(await response.json(), currencyCode);
      if (!value) throw new Error("monobank USD rate not found");
      usdRate = { code: currencyCode, value, at: now() };
    } catch (error) {
      console.error("[donation-jar]", error.message);
    }
    return usdRate?.code === currencyCode ? usdRate.value : null;
  }

  async function load() {
    const response = await fetchImpl(CLIENT_INFO_URL, {
      headers: { "X-Token": token },
      signal: AbortSignal.timeout(10000),
    });
    if (!response.ok) throw new Error(`monobank client-info HTTP ${response.status}`);
    const jar = pickJar(await response.json(), sendId);
    if (!jar) throw new Error("monobank jar not found");
    const { currencyCode, ...data } = jar;
    const rate = jar.currency === "USD" ? 1 : await loadUsdRate(currencyCode);
    if (rate) {
      data.balance_usd = Math.round((jar.balance / rate) * 100) / 100;
      data.usd_rate = rate;
    }
    return data;
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
