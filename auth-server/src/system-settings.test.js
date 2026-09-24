import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const server = readFileSync(new URL("./server.js", import.meta.url), "utf8");
const admin = readFileSync(
  new URL("../../gg-html/admin.html", import.meta.url),
  "utf8"
);

test("BGA archive retry delay is a bounded integer system setting", () => {
  assert.match(
    server,
    /key: "bga_replay_archive_retry_minutes"[\s\S]*?value_type: "integer"[\s\S]*?default_value: "2"[\s\S]*?min_value: 1[\s\S]*?max_value: 60/
  );
  assert.match(server, /if \(definition\?\.value_type === "integer"\)/);
  assert.match(server, /Value must be a whole number/);
  assert.match(server, /Value must be between \$\{minimum\} and \$\{maximum\}/);
});

test("Admin renders integer system settings as bounded number inputs", () => {
  assert.match(
    admin,
    /item\?\.value_type === "integer"[\s\S]*?createNumberRow\([\s\S]*?min: item\?\.min_value[\s\S]*?max: item\?\.max_value/
  );
});
