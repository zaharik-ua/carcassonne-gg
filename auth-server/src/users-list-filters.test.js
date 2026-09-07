import assert from "node:assert/strict";
import test from "node:test";
import { buildUsersListFilter } from "./users-list-filters.js";

test("builds independent email, BGA nickname and name filters", () => {
  const result = buildUsersListFilter({
    email: " EXAMPLE.COM ",
    bga_nickname: "Meeple_Master",
    name: "Alice",
  });

  assert.match(result.whereSql, /u\.email/);
  assert.match(result.whereSql, /p\.bga_nickname/);
  assert.match(result.whereSql, /u\.name/);
  assert.equal((result.whereSql.match(/ AND /g) || []).length, 2);
  assert.deepEqual(result.params, ["%EXAMPLE.COM%", "%Meeple\\_Master%", "%Alice%"]);
});

test("omits empty filters and escapes SQL LIKE wildcard characters", () => {
  assert.deepEqual(buildUsersListFilter(), { whereSql: "", params: [] });
  assert.deepEqual(buildUsersListFilter({ email: "  ", name: "50%_win\\rate" }).params, [
    "%50\\%\\_win\\\\rate%",
  ]);
});
