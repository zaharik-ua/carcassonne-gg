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
    bga_data_updated_before: null,
  });
  assert.deepEqual(normalizeProfileBgaDataOptions({
    batch_size: "10",
    stop_after_consecutive_failures: "3",
    include_removed: true,
    bga_data_updated_before: "2026-09-08T10:30:00Z",
  }), {
    batch_size: 10,
    stop_after_consecutive_failures: 3,
    include_removed: true,
    bga_data_updated_before: "2026-09-08T10:30:00.000Z",
  });
  assert.throws(() => normalizeProfileBgaDataOptions({ batch_size: 0 }), /batch_size/);
  assert.throws(
    () => normalizeProfileBgaDataOptions({ stop_after_consecutive_failures: 101 }),
    /stop_after_consecutive_failures/
  );
  assert.throws(
    () => normalizeProfileBgaDataOptions({ bga_data_updated_before: "not-a-date" }),
    /bga_data_updated_before/
  );
  assert.equal(
    normalizeProfileBgaDataOptions({ bga_data_updated_before: "2026-09-08T10:30" }).bga_data_updated_before,
    "2026-09-08T10:30:00.000Z"
  );
});

test("builds arguments for an all-profiles batch run", () => {
  assert.deepEqual(
    buildProfileBgaDataScriptArgs("/srv/auth.sqlite", {
      batch_size: 25,
      stop_after_consecutive_failures: 7,
      include_removed: true,
      bga_data_updated_before: "2026-09-08T10:30:00Z",
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
      "--bga-data-updated-before",
      "2026-09-08T10:30:00.000Z",
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

test("admin UI exposes persistent BGA profile runs and resilient live status polling", () => {
  const adminHtml = readFileSync(new URL("../../gg-html/admin.html", import.meta.url), "utf8");
  const serverSource = readFileSync(new URL("./server.js", import.meta.url), "utf8");
  assert.match(adminHtml, /id: "profile-bga-data"/);
  assert.match(adminHtml, /Batch size/);
  assert.match(adminHtml, /Stop after consecutive failures/);
  assert.match(adminHtml, /Include profiles already marked Removed/);
  assert.match(adminHtml, /Refresh if last BGA update was before \(UTC\)/);
  assert.match(adminHtml, /bga_data_updated_before/);
  assert.match(adminHtml, /\/preview/);
  assert.match(adminHtml, /\/run/);
  assert.match(adminHtml, /\/status/);
  assert.match(adminHtml, /window\.setTimeout\(pollJob, 1000\)/);
  assert.match(adminHtml, /Retrying automatically/);
  assert.match(adminHtml, /admin-script-poll-warning/);
  assert.match(serverSource, /admin-scripts\/profile-bga-data\/preview/);
  assert.match(serverSource, /admin-scripts\/profile-bga-data\/run/);
  assert.match(serverSource, /admin-scripts\/profile-bga-data\/status/);
  assert.match(serverSource, /requireAdmin/);
  assert.match(serverSource, /adminScriptRunStore\.saveBatch/);
  assert.match(serverSource, /adminScriptRunStore\.loadLatestRun/);
});
