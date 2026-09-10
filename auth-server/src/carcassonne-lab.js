const CARCASSONNE_LAB_ORIGIN = "https://www.carcassonnelab.com";

export function normalizeBgaTableId(value) {
  const tableId = String(value || "").trim();
  return /^[1-9]\d*$/.test(tableId) ? tableId : "";
}

export function normalizeCarcassonneLabUrl(value) {
  const rawUrl = String(value || "").trim();
  if (!rawUrl) return "";
  try {
    const url = new URL(rawUrl);
    if (url.origin !== CARCASSONNE_LAB_ORIGIN || !url.hash.startsWith("#/")) return "";
    return url.toString();
  } catch (_error) {
    return "";
  }
}

export function parseCarcassonneLabScriptOutput(output) {
  const rawOutput = String(output || "").trim();
  let payload = null;
  try {
    payload = JSON.parse(rawOutput);
  } catch (_error) {
    throw new Error("CarcassonneLab generator returned an invalid response");
  }

  if (!payload || payload.ok !== true) {
    throw new Error(String(payload?.message || "Could not load the BGA replay"));
  }

  const url = normalizeCarcassonneLabUrl(payload.url);
  if (!url) throw new Error("CarcassonneLab generator returned an invalid URL");
  return url;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

export function renderCarcassonneLabErrorPage({ tableId, message }) {
  const safeTableId = escapeHtml(tableId);
  const safeMessage = escapeHtml(message || "Could not prepare this replay.");
  const bgaUrl = `https://boardgamearena.com/table?table=${encodeURIComponent(String(tableId || ""))}`;
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>CarcassonneLab replay unavailable</title>
  <style>
    body { margin: 0; min-height: 100vh; display: grid; place-items: center; background: #f4f6f8; color: #172232; font-family: Arial, sans-serif; }
    main { width: min(520px, calc(100% - 40px)); box-sizing: border-box; padding: 28px; border-radius: 10px; background: #fff; box-shadow: 0 12px 36px rgba(15, 28, 48, .16); }
    h1 { margin: 0 0 12px; font-size: 22px; }
    p { margin: 8px 0; line-height: 1.5; }
    a { color: #0277bd; }
  </style>
</head>
<body>
  <main>
    <h1>Replay unavailable</h1>
    <p>${safeMessage}</p>
    <p><a href="${bgaUrl}">Open BGA table #${safeTableId}</a></p>
  </main>
</body>
</html>`;
}
