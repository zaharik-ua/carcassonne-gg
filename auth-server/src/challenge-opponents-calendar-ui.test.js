import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const html = readFileSync(
  new URL("../../gg-html/player-hub/challenges.html", import.meta.url),
  "utf8"
);

function getFunctionSource(name) {
  const start = html.indexOf(`  function ${name}(`);
  const end = html.indexOf("\n  function ", start + 1);
  assert.ok(start >= 0, `${name} must exist`);
  return html.slice(start, end >= 0 ? end : html.length);
}

test("Open to match places Calendar between the player count and filters", () => {
  const source = getFunctionSource("createOpponentsPanel");
  const countIndex = source.indexOf("controls.appendChild(count)");
  const calendarIndex = source.indexOf("controls.appendChild(calendarButton)");
  const filterIndex = source.indexOf("controls.appendChild(filterToggle)");
  assert.ok(countIndex >= 0 && calendarIndex > countIndex && filterIndex > calendarIndex);
  assert.match(source, /openChallengeOpponentsCalendarDialog\(period, item\.opponentsByView\.available\)/);
});

test("Opponent calendar includes only inviteable players with playing times", () => {
  const source = getFunctionSource("openChallengeOpponentsCalendarDialog");
  const metricsSource = getFunctionSource("getChallengeCalendarViewportMetrics");
  assert.match(source, /opponent\?\.can_invite === true/);
  assert.match(source, /Array\.isArray\(opponent\?\.availability\)/);
  assert.match(source, /opponent\.availability\.length > 0/);
  assert.match(source, /const laneMinWidth = 70/);
  assert.match(source, /Math\.max\(\s*125,/);
  assert.match(source, /group\.width \* 0\.9/);
  assert.match(source, /When can opponents play\?/);
  assert.match(metricsSource, /Math\.max\(20, Math\.min\(25,/);
});

test("Overlapping opponent times receive separate lanes", () => {
  const source = getFunctionSource("assignChallengeOpponentCalendarLanes");
  const assignLanes = new Function(`${source}; return assignChallengeOpponentCalendarLanes;`)();
  const result = assignLanes([
    { name: "A", startHour: 8, endHour: 10 },
    { name: "B", startHour: 8, endHour: 9 },
    { name: "C", startHour: 8, endHour: 11 },
    { name: "D", startHour: 12, endHour: 13 },
  ]);
  assert.equal(result.maxLaneCount, 3);
  assert.equal(
    new Set(result.events.filter((event) => event.startHour === 8).map((event) => event.laneIndex)).size,
    3
  );
  assert.equal(result.events.find((event) => event.name === "D")?.laneCount, 1);
});

test("Opponent calendar blocks are view-only", () => {
  const source = getFunctionSource("openChallengeOpponentsCalendarDialog");
  assert.doesNotMatch(source, /eventEl\.addEventListener/);
  assert.match(html, /\.challenge-opponents-calendar-event \{[\s\S]*?cursor: default;/);
});

test("Opponent calendar wraps names according to each daily segment duration", () => {
  const source = getFunctionSource("openChallengeOpponentsCalendarDialog");
  assert.match(source, /const durationHours = Math\.max\(1, event\.endHour - event\.startHour\)/);
  assert.match(source, /durationHours === 1[\s\S]*?"is-one-hour"/);
  assert.match(source, /durationHours === 2[\s\S]*?"is-two-hours"[\s\S]*?"is-long"/);
  assert.match(
    html,
    /\.challenge-opponents-calendar-event\.is-two-hours \.challenge-opponents-calendar-event-name \{[\s\S]*?-webkit-line-clamp: 2;/
  );
  assert.match(
    html,
    /\.challenge-opponents-calendar-event\.is-long \.challenge-opponents-calendar-event-name \{[\s\S]*?text-overflow: clip;[\s\S]*?white-space: normal;/
  );
});

test("Past opponent availability segments use a light-grey state", () => {
  const source = getFunctionSource("openChallengeOpponentsCalendarDialog");
  assert.match(source, /endTime: lastSlot\.end\.getTime\(\)/);
  assert.match(source, /event\.endTime <= currentTime/);
  assert.match(source, /isPast \? " is-past" : ""/);
  assert.match(
    html,
    /\.challenge-opponents-calendar-event\.is-past \{[\s\S]*?background: #f1f3f5;[\s\S]*?color: #7a8492;/
  );
});
