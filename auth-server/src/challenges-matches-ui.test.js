import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const htmlPath = new URL("../../gg-html/challenges/challenges-matches.html", import.meta.url);
const html = readFileSync(htmlPath, "utf8");
const script = html.match(/<script>([\s\S]*?)<\/script>/)?.[1] || "";

test("Challenge match Lab badges use the server redirect in a new tab", () => {
  assert.match(
    html,
    /const CARCASSONNE_LAB_REVIEW_URL = "https:\/\/api\.carcassonne\.gg\/public\/games";/
  );
  assert.match(html, /\/\$\{encodeURIComponent\(game\.tableId\)\}\/carcassonne-lab/);
  assert.match(html, /badge\.target = "_blank"/);
  assert.match(html, /const labBadge = badge\.cloneNode\(true\)/);
  assert.match(html, /https:\/\/www\.carcassonnelab\.com\/icons\/icon-192\.png/);
});

test("Challenge matches inline script parses after adding Lab badges", () => {
  assert.ok(script);
  assert.doesNotThrow(() => new Function(script));
});
