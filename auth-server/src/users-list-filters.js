const USER_FILTER_COLUMNS = [
  ["email", "u.email"],
  ["bga_nickname", "p.bga_nickname"],
  ["name", "u.name"],
];

function escapeLikePattern(value) {
  return String(value).replace(/[\\%_]/g, "\\$&");
}

export function buildUsersListFilter(query = {}) {
  const whereClauses = [];
  const params = [];

  USER_FILTER_COLUMNS.forEach(([queryKey, column]) => {
    const value = String(query?.[queryKey] || "").trim();
    if (!value) return;
    whereClauses.push(`LOWER(COALESCE(${column}, '')) LIKE LOWER(?) ESCAPE '\\'`);
    params.push(`%${escapeLikePattern(value)}%`);
  });

  return {
    whereSql: whereClauses.length ? `WHERE ${whereClauses.join(" AND ")}` : "",
    params,
  };
}
