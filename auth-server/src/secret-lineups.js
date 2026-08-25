export const SECRET_LINEUP_SIZE = 5;

export function isBlindLineupType(value) {
  return ["blind", "secret", "closed"].includes(String(value || "").trim().toLowerCase());
}

function dbGet(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (error, row) => {
      if (error) {
        reject(error);
        return;
      }
      resolve(row || null);
    });
  });
}

function dbAll(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.all(sql, params, (error, rows) => {
      if (error) {
        reject(error);
        return;
      }
      resolve(rows || []);
    });
  });
}

function dbRun(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function onRun(error) {
      if (error) {
        reject(error);
        return;
      }
      resolve(this);
    });
  });
}

function dbExec(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (error) => {
      if (error) {
        reject(error);
        return;
      }
      resolve();
    });
  });
}

export async function ensureSecretLineupsSchema(db) {
  const matchColumns = await dbAll(db, "PRAGMA table_info(matches)");
  const matchColumnNames = new Set(
    matchColumns.map((column) => String(column?.name || "").trim()).filter(Boolean)
  );
  const requiredMatchColumns = [
    ["lineup_deadline_h", "INTEGER"],
    ["lineup_deadline_utc", "TEXT"],
    ["lineups_published_at", "TEXT"],
    ["team_1_lineup_added", "INTEGER NOT NULL DEFAULT 0 CHECK (team_1_lineup_added IN (0, 1))"],
    ["team_2_lineup_added", "INTEGER NOT NULL DEFAULT 0 CHECK (team_2_lineup_added IN (0, 1))"],
  ];
  for (const [columnName, columnDefinition] of requiredMatchColumns) {
    if (matchColumnNames.has(columnName)) continue;
    try {
      await dbRun(db, `ALTER TABLE matches ADD COLUMN ${columnName} ${columnDefinition}`);
      matchColumnNames.add(columnName);
    } catch (error) {
      if (!String(error?.message || "").toLowerCase().includes("duplicate column")) {
        throw error;
      }
    }
  }

  await dbExec(db, `
    CREATE TABLE IF NOT EXISTS match_lineup_submissions (
      match_id TEXT NOT NULL,
      team_id TEXT NOT NULL COLLATE NOCASE,
      first_submitted_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      submitted_by TEXT,
      revision INTEGER NOT NULL DEFAULT 1,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (match_id, team_id),
      FOREIGN KEY (match_id) REFERENCES matches(id) ON UPDATE CASCADE ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS match_lineup_entries (
      match_id TEXT NOT NULL,
      team_id TEXT NOT NULL COLLATE NOCASE,
      position INTEGER NOT NULL CHECK (position BETWEEN 1 AND ${SECRET_LINEUP_SIZE}),
      player_id TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (match_id, team_id, position),
      UNIQUE (match_id, team_id, player_id),
      FOREIGN KEY (match_id, team_id)
        REFERENCES match_lineup_submissions(match_id, team_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
      FOREIGN KEY (player_id) REFERENCES profiles(id)
    );

    CREATE INDEX IF NOT EXISTS idx_match_lineup_submissions_match
      ON match_lineup_submissions(match_id);

    CREATE INDEX IF NOT EXISTS idx_match_lineup_entries_player
      ON match_lineup_entries(player_id);

    CREATE INDEX IF NOT EXISTS idx_matches_secret_lineups_due
      ON matches(lineup_deadline_utc)
      WHERE lineups_published_at IS NULL;
  `);

  // Normalize legacy values while continuing to recognize them during rollout.
  await dbRun(
    db,
    `
      UPDATE matches
      SET lineup_type = 'Blind'
      WHERE lower(trim(COALESCE(lineup_type, ''))) IN ('secret', 'closed')
    `
  );
  await dbExec(db, `
    UPDATE matches
    SET lineup_deadline_h = 24
    WHERE lower(trim(COALESCE(lineup_type, ''))) = 'blind'
      AND COALESCE(lineup_deadline_h, 0) NOT IN (6, 12, 24, 48);

    UPDATE matches
    SET lineup_deadline_utc = strftime(
      '%Y-%m-%dT%H:%M:%fZ',
      datetime(time_utc, '-' || lineup_deadline_h || ' hours')
    )
    WHERE lower(trim(COALESCE(lineup_type, ''))) = 'blind'
      AND datetime(time_utc) IS NOT NULL
      AND datetime(lineup_deadline_utc) IS NULL;

    UPDATE matches
    SET team_1_lineup_added = 1
    WHERE COALESCE(team_1_lineup_added, 0) = 0
      AND EXISTS (
        SELECT 1
        FROM match_lineup_submissions s
        WHERE trim(COALESCE(s.match_id, '')) = trim(COALESCE(matches.id, ''))
          AND upper(trim(COALESCE(s.team_id, ''))) = upper(trim(COALESCE(matches.team_1, '')))
      );

    UPDATE matches
    SET team_2_lineup_added = 1
    WHERE COALESCE(team_2_lineup_added, 0) = 0
      AND EXISTS (
        SELECT 1
        FROM match_lineup_submissions s
        WHERE trim(COALESCE(s.match_id, '')) = trim(COALESCE(matches.id, ''))
          AND upper(trim(COALESCE(s.team_id, ''))) = upper(trim(COALESCE(matches.team_2, '')))
      );
  `);

  const duelsTable = await dbGet(
    db,
    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'duels' LIMIT 1"
  );
  if (duelsTable) {
    await dbExec(db, `
      UPDATE matches
      SET team_1_lineup_added = 1
      WHERE COALESCE(team_1_lineup_added, 0) = 0
        AND EXISTS (
          SELECT 1
          FROM duels d
          WHERE trim(COALESCE(d.match_id, '')) = trim(COALESCE(matches.id, ''))
            AND d.deleted_at IS NULL
            AND trim(COALESCE(d.player_1_id, '')) <> ''
        );

      UPDATE matches
      SET team_2_lineup_added = 1
      WHERE COALESCE(team_2_lineup_added, 0) = 0
        AND EXISTS (
          SELECT 1
          FROM duels d
          WHERE trim(COALESCE(d.match_id, '')) = trim(COALESCE(matches.id, ''))
            AND d.deleted_at IS NULL
            AND trim(COALESCE(d.player_2_id, '')) <> ''
        );
    `);

    // Existing duels were already public before this feature. Mark those matches as
    // published so the migration never hides or rematerializes historical data.
    await dbRun(
      db,
      `
        UPDATE matches
        SET lineups_published_at = COALESCE(updated_at, CURRENT_TIMESTAMP)
        WHERE lineups_published_at IS NULL
          AND lower(trim(COALESCE(lineup_type, ''))) IN ('blind', 'secret', 'closed')
          AND EXISTS (
            SELECT 1
            FROM duels d
            WHERE trim(COALESCE(d.match_id, '')) = trim(COALESCE(matches.id, ''))
              AND d.deleted_at IS NULL
          )
      `
    );
  }
}

function normalizeTeamId(value) {
  return String(value || "").trim().toUpperCase();
}

function buildPublishedDuelId(matchId, position) {
  return `blind-lineup-${String(matchId || "").trim()}-${position}`;
}

export async function publishSecretLineupMatchInTransaction(db, matchId, actorPlayerId = null) {
  const normalizedMatchId = String(matchId || "").trim();
  if (!normalizedMatchId) {
    return { published: false, reason: "match_id_required" };
  }

  const match = await dbGet(
    db,
    `
      SELECT
        id,
        tournament_id,
        COALESCE((
          SELECT t.ranking
          FROM tournaments t
          WHERE upper(trim(COALESCE(t.id, ''))) = upper(trim(COALESCE(matches.tournament_id, '')))
          LIMIT 1
        ), 1) AS ranking,
        COALESCE(is_test, 0) AS is_test,
        time_utc,
        lineup_type,
        lineup_deadline_utc,
        lineups_published_at,
        team_1,
        team_2,
        CASE
          WHEN datetime(lineup_deadline_utc) IS NOT NULL
            AND datetime(lineup_deadline_utc) <= CURRENT_TIMESTAMP
          THEN 1 ELSE 0
        END AS deadline_passed
      FROM matches
      WHERE trim(COALESCE(id, '')) = trim(?)
        AND deleted_at IS NULL
      LIMIT 1
    `,
    [normalizedMatchId]
  );
  if (!match) return { published: false, reason: "match_not_found" };
  if (!isBlindLineupType(match.lineup_type)) {
    return { published: false, reason: "not_blind" };
  }
  if (match.lineups_published_at) {
    return {
      published: false,
      reason: "already_published",
      publishedAt: match.lineups_published_at,
    };
  }
  if (Number(match.deadline_passed) !== 1) {
    return { published: false, reason: "deadline_not_reached" };
  }

  const team1 = normalizeTeamId(match.team_1);
  const team2 = normalizeTeamId(match.team_2);
  if (!team1 || !team2 || team1 === team2) {
    return { published: false, reason: "invalid_teams" };
  }

  const submissions = await dbAll(
    db,
    `
      SELECT team_id
      FROM match_lineup_submissions
      WHERE trim(COALESCE(match_id, '')) = trim(?)
        AND upper(trim(COALESCE(team_id, ''))) IN (?, ?)
    `,
    [normalizedMatchId, team1, team2]
  );
  const submittedTeams = new Set(submissions.map((row) => normalizeTeamId(row?.team_id)));
  if (!submittedTeams.has(team1) || !submittedTeams.has(team2)) {
    return { published: false, reason: "waiting_for_lineup" };
  }

  const entries = await dbAll(
    db,
    `
      SELECT team_id, position, player_id
      FROM match_lineup_entries
      WHERE trim(COALESCE(match_id, '')) = trim(?)
        AND upper(trim(COALESCE(team_id, ''))) IN (?, ?)
      ORDER BY position ASC
    `,
    [normalizedMatchId, team1, team2]
  );
  const entriesByTeam = new Map([[team1, new Map()], [team2, new Map()]]);
  entries.forEach((entry) => {
    const teamId = normalizeTeamId(entry?.team_id);
    const position = Number(entry?.position);
    const playerId = String(entry?.player_id || "").trim();
    if (!entriesByTeam.has(teamId) || !Number.isInteger(position) || !playerId) return;
    entriesByTeam.get(teamId).set(position, playerId);
  });
  const hasCompleteLineup = (teamId) => (
    entriesByTeam.get(teamId)?.size === SECRET_LINEUP_SIZE
    && Array.from({ length: SECRET_LINEUP_SIZE }, (_, index) => index + 1)
      .every((position) => entriesByTeam.get(teamId).has(position))
  );
  if (!hasCompleteLineup(team1) || !hasCompleteLineup(team2)) {
    return { published: false, reason: "incomplete_lineup" };
  }

  for (let position = 1; position <= SECRET_LINEUP_SIZE; position += 1) {
    const duelId = buildPublishedDuelId(normalizedMatchId, position);
    await dbRun(
      db,
      `
        INSERT INTO duels (
          id,
          tournament_id,
          match_id,
          is_test,
          ranking,
          duel_number,
          duel_format,
          time_utc,
          custom_time,
          player_1_id,
          player_2_id,
          dw1,
          dw2,
          status,
          created_by,
          updated_by,
          deleted_by,
          deleted_at,
          created_at,
          updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, 'Bo3', ?, 0, ?, ?, NULL, NULL, 'Planned', ?, ?, NULL, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(id) DO UPDATE SET
          tournament_id = excluded.tournament_id,
          match_id = excluded.match_id,
          is_test = excluded.is_test,
          ranking = excluded.ranking,
          duel_number = excluded.duel_number,
          duel_format = excluded.duel_format,
          time_utc = excluded.time_utc,
          custom_time = 0,
          player_1_id = excluded.player_1_id,
          player_2_id = excluded.player_2_id,
          dw1 = NULL,
          dw2 = NULL,
          status = 'Planned',
          updated_by = excluded.updated_by,
          deleted_by = NULL,
          deleted_at = NULL,
          updated_at = CURRENT_TIMESTAMP
      `,
      [
        duelId,
        match.tournament_id,
        normalizedMatchId,
        Number(match.is_test) === 1 ? 1 : 0,
        Number(match.ranking) === 1 ? 1 : 0,
        position,
        match.time_utc,
        entriesByTeam.get(team1).get(position),
        entriesByTeam.get(team2).get(position),
        actorPlayerId,
        actorPlayerId,
      ]
    );
  }

  await dbRun(
    db,
    `
      UPDATE matches
      SET
        lineups_published_at = CURRENT_TIMESTAMP,
        team_1_lineup_added = 1,
        team_2_lineup_added = 1,
        updated_by = COALESCE(?, updated_by),
        updated_at = CURRENT_TIMESTAMP
      WHERE trim(COALESCE(id, '')) = trim(?)
        AND lineups_published_at IS NULL
    `,
    [actorPlayerId, normalizedMatchId]
  );
  const publishedMatch = await dbGet(
    db,
    "SELECT lineups_published_at FROM matches WHERE trim(COALESCE(id, '')) = trim(?) LIMIT 1",
    [normalizedMatchId]
  );
  return {
    published: true,
    reason: "published",
    publishedAt: publishedMatch?.lineups_published_at || null,
    matchId: normalizedMatchId,
  };
}

export async function publishSecretLineupMatch(db, matchId, actorPlayerId = null) {
  await dbRun(db, "BEGIN IMMEDIATE TRANSACTION");
  try {
    const result = await publishSecretLineupMatchInTransaction(db, matchId, actorPlayerId);
    await dbRun(db, "COMMIT");
    return result;
  } catch (error) {
    await dbRun(db, "ROLLBACK").catch(() => {});
    throw error;
  }
}

export async function publishDueSecretLineups(db, actorPlayerId = null) {
  const dueMatches = await dbAll(
    db,
    `
      SELECT m.id
      FROM matches m
      WHERE m.deleted_at IS NULL
        AND lower(trim(COALESCE(m.lineup_type, ''))) IN ('blind', 'secret', 'closed')
        AND m.lineups_published_at IS NULL
        AND datetime(m.lineup_deadline_utc) IS NOT NULL
        AND datetime(m.lineup_deadline_utc) <= CURRENT_TIMESTAMP
        AND EXISTS (
          SELECT 1
          FROM match_lineup_submissions s1
          WHERE trim(COALESCE(s1.match_id, '')) = trim(COALESCE(m.id, ''))
            AND upper(trim(COALESCE(s1.team_id, ''))) = upper(trim(COALESCE(m.team_1, '')))
        )
        AND EXISTS (
          SELECT 1
          FROM match_lineup_submissions s2
          WHERE trim(COALESCE(s2.match_id, '')) = trim(COALESCE(m.id, ''))
            AND upper(trim(COALESCE(s2.team_id, ''))) = upper(trim(COALESCE(m.team_2, '')))
        )
      ORDER BY datetime(m.lineup_deadline_utc) ASC, m.id ASC
    `
  );

  const results = [];
  for (const row of dueMatches) {
    results.push(await publishSecretLineupMatch(db, row.id, actorPlayerId));
  }
  return results;
}
