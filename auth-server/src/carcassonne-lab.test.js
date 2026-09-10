import assert from "node:assert/strict";
import test from "node:test";
import {
  normalizeBgaTableId,
  normalizeCarcassonneLabUrl,
  parseCarcassonneLabScriptOutput,
  renderCarcassonneLabErrorPage,
} from "./carcassonne-lab.js";

test("normalizes BGA table IDs", () => {
  assert.equal(normalizeBgaTableId(" 909842009 "), "909842009");
  assert.equal(normalizeBgaTableId("0"), "");
  assert.equal(normalizeBgaTableId("12.3"), "");
  assert.equal(normalizeBgaTableId("table-12"), "");
});

test("accepts only CarcassonneLab review URLs", () => {
  const url = "https://www.carcassonnelab.com/#/0/0/B-z/1115?players=Alice,Bob&colors=red,blue";
  assert.equal(normalizeCarcassonneLabUrl(url), url);
  assert.equal(normalizeCarcassonneLabUrl("https://example.com/#/0/0/test"), "");
  assert.equal(normalizeCarcassonneLabUrl("https://www.carcassonnelab.com/"), "");
});

test("parses successful generator output and reports generator errors", () => {
  const url = "https://www.carcassonnelab.com/#/0/0/B-z/1115?players=Alice,Bob&colors=red,blue";
  assert.equal(parseCarcassonneLabScriptOutput(JSON.stringify({ ok: true, url })), url);
  assert.throws(
    () => parseCarcassonneLabScriptOutput(JSON.stringify({ ok: false, message: "Replay missing" })),
    /Replay missing/
  );
  assert.throws(() => parseCarcassonneLabScriptOutput("not-json"), /invalid response/);
});

test("escapes errors rendered in the fallback page", () => {
  const html = renderCarcassonneLabErrorPage({ tableId: "123", message: "Replay <missing> & unavailable" });
  assert.match(html, /Replay &lt;missing&gt; &amp; unavailable/);
  assert.match(html, /table\?table=123/);
  assert.doesNotMatch(html, /Replay <missing>/);
});
