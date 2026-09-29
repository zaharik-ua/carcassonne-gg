import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";
import { fileURLToPath } from "node:url";

const repoRoot = fileURLToPath(new URL("../../", import.meta.url));

function readRepoFile(relativePath) {
  return readFileSync(new URL(relativePath, `file://${repoRoot}/`), "utf8");
}

function extractRecoveryInstaller(html) {
  const startMarker = "    function installApiFetchRecovery(apiBase) {";
  const endMarker = "    installApiFetchRecovery(AUTH_BASE);";
  const start = html.indexOf(startMarker);
  const end = html.indexOf(endMarker, start);
  assert.notEqual(start, -1, "recovery installer is present");
  assert.notEqual(end, -1, "recovery installer call is present");
  return html.slice(start, end).trim();
}

function installRecovery(installerCode, nativeFetch) {
  const window = {
    location: { href: "https://carcassonne.gg/test" },
    fetch: nativeFetch,
    setTimeout,
  };
  const context = vm.createContext({
    window,
    URL,
    Request: class Request {},
    DOMException,
    Set,
    Promise,
    Math,
    Error,
  });
  vm.runInContext(
    `${installerCode}\ninstallApiFetchRecovery("https://api.carcassonne.gg");`,
    context
  );
  return window;
}

const loginHtml = readRepoFile("gg-html/login-popup.html");
const mobileMenuHtml = readRepoFile("gg-html/mobile-menu.html");
const installerCode = extractRecoveryInstaller(loginHtml);

test("desktop and mobile auth use the same API recovery installer", () => {
  assert.equal(extractRecoveryInstaller(mobileMenuHtml), installerCode);
});

test("failed API GET waits for health and then retries", async () => {
  const calls = [];
  const window = installRecovery(installerCode, async (input, init) => {
    const url = String(input);
    calls.push([url, init?.method || "GET"]);
    if (calls.length === 1) throw new TypeError("Failed to fetch");
    if (url.endsWith("/health")) return { ok: true, status: 200 };
    return { ok: true, status: 200 };
  });

  const response = await window.fetch("https://api.carcassonne.gg/auth/me", {
    credentials: "include",
  });

  assert.equal(response.status, 200);
  assert.deepEqual(calls.map(([url]) => url), [
    "https://api.carcassonne.gg/auth/me",
    "https://api.carcassonne.gg/health",
    "https://api.carcassonne.gg/auth/me",
  ]);
});

test("temporary gateway responses wait for health and then retry", async () => {
  const statuses = [503, 200, 200];
  const calls = [];
  const window = installRecovery(installerCode, async (input) => {
    calls.push(String(input));
    const status = statuses.shift();
    return { ok: status === 200, status };
  });

  const response = await window.fetch("https://api.carcassonne.gg/public/news");

  assert.equal(response.status, 200);
  assert.deepEqual(calls, [
    "https://api.carcassonne.gg/public/news",
    "https://api.carcassonne.gg/health",
    "https://api.carcassonne.gg/public/news",
  ]);
});

test("concurrent failed API GETs share one health check", async () => {
  let healthCalls = 0;
  let endpointCalls = 0;
  const window = installRecovery(installerCode, async (input) => {
    const url = String(input);
    if (url.endsWith("/health")) {
      healthCalls += 1;
      return { ok: true, status: 200 };
    }
    endpointCalls += 1;
    if (endpointCalls <= 2) throw new TypeError("Failed to fetch");
    return { ok: true, status: 200 };
  });

  await Promise.all([
    window.fetch("https://api.carcassonne.gg/teams"),
    window.fetch("https://api.carcassonne.gg/profiles/public"),
  ]);

  assert.equal(healthCalls, 1);
  assert.equal(endpointCalls, 4);
});

test("mutating API requests are never retried", async () => {
  let calls = 0;
  const window = installRecovery(installerCode, async () => {
    calls += 1;
    throw new TypeError("Failed to fetch");
  });

  await assert.rejects(
    window.fetch("https://api.carcassonne.gg/auth/logout", { method: "POST" }),
    /Failed to fetch/
  );
  assert.equal(calls, 1);
});

test("auth controls do not treat a transport failure as logout", () => {
  assert.match(loginHtml, /catch \(error\) \{\s*scheduleSessionRetry\(\);\s*\}/);
  assert.match(mobileMenuHtml, /catch \(error\) \{\s*scheduleSessionRetry\(\);\s*\}/);
  assert.match(
    mobileMenuHtml,
    /id="ggAuthSignInButton" class="gg-mobile-nav__auth-button gg-mobile-nav__hidden"/
  );
});
