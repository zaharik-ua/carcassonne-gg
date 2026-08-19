export const TOURNAMENT_CASE_TYPES = new Set(["complaint", "problem", "request", "other"]);
export const TOURNAMENT_CASE_STATUSES = new Set(["open", "in_progress", "resolved", "closed"]);
export const TOURNAMENT_CASE_PRIORITIES = new Set(["low", "normal", "high", "urgent"]);

export function ensureTournamentCasesSchema(db) {
  return new Promise((resolve, reject) => {
    db.exec(
      `
        CREATE TABLE IF NOT EXISTS tournament_cases (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          case_type TEXT NOT NULL DEFAULT 'problem',
          category TEXT,
          status TEXT NOT NULL DEFAULT 'open',
          priority TEXT NOT NULL DEFAULT 'normal',
          subject TEXT NOT NULL,
          details TEXT NOT NULL DEFAULT '',
          submitted_by_user_id INTEGER,
          submitted_by_player_id TEXT NOT NULL,
          responsible_user_id INTEGER,
          reported_player_id TEXT,
          match_id TEXT,
          duel_id TEXT,
          tournament_id TEXT,
          challenge_period_id TEXT,
          related_entity_type TEXT,
          related_entity_id TEXT,
          resolution TEXT,
          resolved_at TEXT,
          deleted_at TEXT,
          created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
          FOREIGN KEY (submitted_by_user_id) REFERENCES users(id) ON DELETE SET NULL,
          FOREIGN KEY (submitted_by_player_id) REFERENCES profiles(id) ON DELETE RESTRICT,
          FOREIGN KEY (responsible_user_id) REFERENCES users(id) ON DELETE SET NULL,
          FOREIGN KEY (reported_player_id) REFERENCES profiles(id) ON DELETE SET NULL,
          FOREIGN KEY (match_id) REFERENCES matches(id) ON DELETE SET NULL,
          FOREIGN KEY (duel_id) REFERENCES duels(id) ON DELETE SET NULL,
          FOREIGN KEY (tournament_id) REFERENCES tournaments(id) ON DELETE SET NULL,
          FOREIGN KEY (challenge_period_id) REFERENCES challenge_periods(id) ON DELETE SET NULL,
          CHECK (case_type IN ('complaint', 'problem', 'request', 'other')),
          CHECK (status IN ('open', 'in_progress', 'resolved', 'closed')),
          CHECK (priority IN ('low', 'normal', 'high', 'urgent'))
        );

        CREATE INDEX IF NOT EXISTS idx_tournament_cases_status_priority
          ON tournament_cases(status, priority, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_tournament_cases_submitted_by
          ON tournament_cases(submitted_by_player_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_tournament_cases_responsible
          ON tournament_cases(responsible_user_id, status, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_tournament_cases_duel
          ON tournament_cases(duel_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_tournament_cases_match
          ON tournament_cases(match_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_tournament_cases_tournament
          ON tournament_cases(tournament_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_tournament_cases_challenge_period
          ON tournament_cases(challenge_period_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_tournament_cases_related_entity
          ON tournament_cases(related_entity_type, related_entity_id, created_at DESC);
      `,
      (error) => {
        if (error) reject(error);
        else resolve();
      }
    );
  });
}
