import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const adminHtml = readFileSync(new URL("../../gg-html/admin.html", import.meta.url), "utf8");

test("admin UI exposes the In-Person tournament section and stage-two endpoints", () => {
  assert.match(adminHtml, /title: "In-Person Tournaments"/);
  assert.match(adminHtml, /title: "Cities"/);
  assert.match(adminHtml, /const IN_PERSON_TOURNAMENTS_URL = `\$\{AUTH_BASE\}\/in-person-tournaments`/);
  assert.match(adminHtml, /const CITIES_URL = `\$\{AUTH_BASE\}\/cities`/);
  assert.match(adminHtml, /function openInPersonTournamentForm/);
  assert.match(adminHtml, /function renderInPersonTournaments/);
  assert.match(adminHtml, /function openCityForm/);
  assert.match(adminHtml, /function renderCities/);
  assert.match(adminHtml, /\/publish`/);
  assert.match(adminHtml, /\/cancel`/);
});

test("admin form contains conditional location, date period, format, admins and inline city controls", () => {
  [
    "Name (English) *",
    "Name (local)",
    "Public slug *",
    "Country / association *",
    "Local type *",
    "Qualifier city *",
    "Start date *",
    "End date",
    "Organizer name *",
    "Organizer URL",
    "Rules URL",
    "Swiss rounds count *",
    "Playoff first round *",
    "Tournament admins",
    "Create city",
    "City icon URL",
    "New city: icon URL",
    "Bronze medal match is always included",
  ].forEach((label) => assert.ok(adminHtml.includes(label), `missing admin UI text: ${label}`));
});

test("the embedded admin script parses after In-Person UI changes", () => {
  const match = adminHtml.match(/<script>([\s\S]*?)<\/script>/);
  assert.ok(match, "admin script must exist");
  assert.doesNotThrow(() => new Function(match[1]));
});
