import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import {
  PROFILE_BGA_DATA_PROGRESS_PREFIX,
  buildProfileBgaDataScriptArgs,
  normalizeProfileBgaDataOptions,
  parseProfileBgaDataProgressLine,
} from "./profile-bga-data-admin.js";

test("normalizes profile BGA data admin options", () => {
  assert.deepEqual(normalizeProfileBgaDataOptions({}), {
    batch_size: 20,
    stop_after_consecutive_failures: 5,
    include_removed: false,
  });
  assert.deepEqual(normalizeProfileBgaDataOptions({
    batch_size: "10",
    stop_after_consecutive_failures: "3",
    include_removed: true,
  }), {
    batch_size: 10,
    stop_after_consecutive_failures: 3,
    include_removed: true,
  });
  assert.throws(() => normalizeProfileBgaDataOptions({ batch_size: 0 }), /batch_size/);
  assert.throws(
    () => normalizeProfileBgaDataOptions({ stop_after_consecutive_failures: 101 }),
    /stop_after_consecutive_failures/
  );
});

test("builds arguments for an all-profiles batch run", () => {
  assert.deepEqual(
    buildProfileBgaDataScriptArgs("/srv/auth.sqlite", {
      batch_size: 25,
      stop_after_consecutive_failures: 7,
      include_removed: true,
    }),
    [
      "--db-path",
      "/srv/auth.sqlite",
      "--all",
      "--limit",
      "25",
      "--stop-after-consecutive-failures",
      "7",
      "--include-removed",
    ]
  );
});

test("parses structured batch progress and ignores regular stderr", () => {
  const event = {
    type: "batch",
    batch: 2,
    processed: 20,
    failed: 1,
  };
  assert.deepEqual(
    parseProfileBgaDataProgressLine(`${PROFILE_BGA_DATA_PROGRESS_PREFIX}${JSON.stringify(event)}`),
    event
  );
  assert.equal(parseProfileBgaDataProgressLine("[batch 2] 19 of 20 completed"), null);
  assert.equal(parseProfileBgaDataProgressLine(`${PROFILE_BGA_DATA_PROGRESS_PREFIX}{bad json`), null);
});

test("admin UI exposes BGA profile preview, settings, run and live status polling", () => {
  const adminHtml = readFileSync(new URL("../../gg-html/admin.html", import.meta.url), "utf8");
  const serverSource = readFileSync(new URL("./server.js", import.meta.url), "utf8");
  assert.match(adminHtml, /id: "profile-bga-data"/);
  assert.match(adminHtml, /Batch size/);
  assert.match(adminHtml, /Stop after consecutive failures/);
  assert.match(adminHtml, /Include profiles already marked Removed/);
  assert.match(adminHtml, /\/preview/);
  assert.match(adminHtml, /\/run/);
  assert.match(adminHtml, /\/status/);
  assert.match(adminHtml, /window\.setTimeout\(pollJob, 1000\)/);
  assert.match(serverSource, /admin-scripts\/profile-bga-data\/preview/);
  assert.match(serverSource, /admin-scripts\/profile-bga-data\/run/);
  assert.match(serverSource, /admin-scripts\/profile-bga-data\/status/);
  assert.match(serverSource, /requireAdmin/);
});
