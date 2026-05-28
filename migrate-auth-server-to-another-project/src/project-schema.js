export async function ensureProjectSchema({ dbRun }) {
  // Add project-specific tables here. Keep auth/profile tables in db.js.
  //
  // Example:
  // await dbRun(`
  //   CREATE TABLE IF NOT EXISTS projects (
  //     id INTEGER PRIMARY KEY AUTOINCREMENT,
  //     owner_user_id INTEGER NOT NULL,
  //     name TEXT NOT NULL,
  //     created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  //     FOREIGN KEY (owner_user_id) REFERENCES users(id) ON DELETE CASCADE
  //   )
  // `);
}
