import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const menuHtml = readFileSync(
  new URL("../../gg-html/player-hub/player-hub-menu.html", import.meta.url),
  "utf8"
);
const hubHtml = readFileSync(
  new URL("../../gg-html/player-hub/player-hub.html", import.meta.url),
  "utf8"
);
const inPersonHtml = readFileSync(
  new URL("../../gg-html/player-hub/in-person.html", import.meta.url),
  "utf8"
);

function assertEmbeddedScriptsParse(html, label) {
  const scripts = [...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/gi)];
  assert.ok(scripts.length > 0, `${label} must contain a script`);
  scripts.forEach((match) => assert.doesNotThrow(() => new Function(match[1])));
}

test("Player Hub exposes In-Person only to global admins or assigned organizers", () => {
  [menuHtml, hubHtml].forEach((html) => {
    assert.match(html, /label: "In-Person"/);
    assert.match(html, /\/in-person-tournaments\/accessible/);
    assert.match(html, /item\.view === "in-person".*isAdminUser \|\| inPersonTournaments\.length > 0/s);
    assert.match(html, /item\.view === "my-tournaments" \|\| item\.view === "nationals"/);
    assert.match(html, /return isAdminUser/);
  });
});

test("In-Person page contains participant registration and check-in flows", () => {
  [
    "In-Person Tournaments",
    "Add player",
    "Check-in and draw numbers",
    "Gaps and a missing #1 are allowed.",
    "Possible duplicate:",
    "Ready to form the first Swiss round.",
  ].forEach((text) => assert.ok(inPersonHtml.includes(text), `missing In-Person UI text: ${text}`));

  assert.match(inPersonHtml, /\/participants/);
  assert.match(inPersonHtml, /\/participant-cities/);
  assert.match(inPersonHtml, /\/start-check-in/);
  assert.match(inPersonHtml, /\/check-in/);
  assert.match(inPersonHtml, /confirm_duplicate: confirmDuplicate/);
  assert.match(inPersonHtml, /@media \(max-width: 720px\)/);
  assert.match(inPersonHtml, /min-height: 44px/);
});

test("all Player Hub scripts parse after the In-Person additions", () => {
  assertEmbeddedScriptsParse(menuHtml, "Player Hub menu");
  assertEmbeddedScriptsParse(hubHtml, "Player Hub landing");
  assertEmbeddedScriptsParse(inPersonHtml, "In-Person page");
});
