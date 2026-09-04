import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

test("every legacy tournament access query has an explicit domain predicate", async () => {
  const serverSource = await readFile(new URL("./server.js", import.meta.url), "utf8");
  const lines = serverSource.split(/\r?\n/);
  let readOrDeleteReferences = 0;
  let insertReferences = 0;

  lines.forEach((line, index) => {
    if (/\b(?:FROM|DELETE FROM) tournament_access_users\b/.test(line)) {
      readOrDeleteReferences += 1;
      const queryWindow = lines.slice(index, index + 8).join("\n");
      assert.match(
        queryWindow,
        /tournament_entity_type\s*=\s*'tournament'/,
        `missing legacy tournament type predicate near server.js:${index + 1}`
      );
    }

    if (/\bINSERT INTO tournament_access_users\b/.test(line)) {
      insertReferences += 1;
      const queryWindow = lines.slice(index, index + 16).join("\n");
      assert.match(queryWindow, /tournament_entity_type/);
      assert.match(queryWindow, /VALUES\s*\(\s*'tournament'/);
    }
  });

  assert.ok(readOrDeleteReferences > 0, "expected legacy access read/delete queries");
  assert.ok(insertReferences > 0, "expected a legacy access insert query");
});
