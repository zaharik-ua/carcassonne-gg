function dbExec(db, sql) {
  return new Promise((resolve, reject) => {
    db.exec(sql, (error) => {
      if (error) reject(error);
      else resolve();
    });
  });
}

/**
 * Match IDs are derived from the scheduled date and teams, so they can change
 * after a time proposal is accepted or an administrator edits a match. Keep
 * every table that stores that ID in sync in the same SQLite statement.
 */
export function ensureMatchIdReferenceSync(db) {
  return dbExec(db, `
    CREATE TRIGGER IF NOT EXISTS trg_matches_sync_references_after_id_update
    AFTER UPDATE OF id ON matches
    WHEN trim(COALESCE(OLD.id, '')) <> trim(COALESCE(NEW.id, ''))
    BEGIN
      UPDATE duels
      SET match_id = NEW.id
      WHERE trim(COALESCE(match_id, '')) = trim(COALESCE(OLD.id, ''));

      UPDATE match_lineup_submissions
      SET match_id = NEW.id
      WHERE trim(COALESCE(match_id, '')) = trim(COALESCE(OLD.id, ''));

      UPDATE match_lineup_entries
      SET match_id = NEW.id
      WHERE trim(COALESCE(match_id, '')) = trim(COALESCE(OLD.id, ''));

      UPDATE news
      SET match_id = NEW.id
      WHERE trim(COALESCE(match_id, '')) = trim(COALESCE(OLD.id, ''));

      UPDATE streams
      SET entity_id = NEW.id
      WHERE lower(trim(COALESCE(entity_type, ''))) = 'match'
        AND trim(COALESCE(entity_id, '')) = trim(COALESCE(OLD.id, ''));

      UPDATE tournament_cases
      SET match_id = NEW.id
      WHERE trim(COALESCE(match_id, '')) = trim(COALESCE(OLD.id, ''));

      UPDATE tournament_cases
      SET related_entity_id = NEW.id
      WHERE lower(trim(COALESCE(related_entity_type, ''))) IN ('match', 'matches')
        AND trim(COALESCE(related_entity_id, '')) = trim(COALESCE(OLD.id, ''));
    END;
  `);
}
