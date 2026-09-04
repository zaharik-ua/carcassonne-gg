# In-person schema migration runbook

Цей runbook стосується першого production deploy схеми `in_person` і перебудови `tournament_access_users`.

## Що змінюється

- усі нові доменні таблиці зберігаються окремо як `in_person_*`;
- `tournament_access_users` отримує `tournament_entity_type`;
- primary key змінюється з `(tournament_id, user_id)` на `(tournament_entity_type, tournament_id, user_id)`;
- усі наявні access rows переносяться з типом `tournament` без зміни ролі або timestamps;
- server починає слухати HTTP port лише після успішного завершення критичної міграції;
- `IN_PERSON_TOURNAMENTS_ENABLED` за замовчуванням вимкнений, тому foundation route не публікується під час міграції.

## Перед deploy

1. Зупинити auth-server, щоб під час backup і міграції не було записів у SQLite.
2. Визначити абсолютний шлях до DB із фактичного `DB_PATH` deployment environment.
3. Створити timestamped backup через SQLite online backup command, а не копіювати лише основний файл за наявності WAL:

   ```sh
   sqlite3 /absolute/path/auth.sqlite ".backup '/absolute/path/backups/auth-before-in-person-YYYYMMDD-HHMMSS.sqlite'"
   ```

4. Перевірити backup:

   ```sh
   sqlite3 /absolute/path/backups/auth-before-in-person-YYYYMMDD-HHMMSS.sqlite "PRAGMA integrity_check;"
   sqlite3 /absolute/path/backups/auth-before-in-person-YYYYMMDD-HHMMSS.sqlite "SELECT COUNT(*) FROM tournament_access_users;"
   ```

5. Записати контрольну кількість access rows і розподіл ролей:

   ```sql
   SELECT COUNT(*) AS access_rows FROM tournament_access_users;
   SELECT lower(trim(role)) AS role, COUNT(*)
   FROM tournament_access_users
   GROUP BY lower(trim(role));
   ```

## Deploy і перевірка

1. Deploy backend із `IN_PERSON_TOURNAMENTS_ENABLED=false`.
2. Запустити auth-server і перевірити structured log `[in-person] Schema foundation ready`.
3. Переконатися, що log містить очікувану кількість `accessRows`.
4. Виконати перевірки:

   ```sql
   PRAGMA integrity_check;
   PRAGMA table_info(tournament_access_users);

   SELECT tournament_entity_type, lower(trim(role)) AS role, COUNT(*)
   FROM tournament_access_users
   GROUP BY tournament_entity_type, lower(trim(role));

   SELECT COUNT(*)
   FROM tournament_access_users
   WHERE tournament_entity_type <> 'tournament';
   ```

   Під час першої міграції останній запит має повернути `0`, бо production in-person records ще не створювались.

5. Виконати smoke checks старих flow:

   - global admin бачить і редагує звичайні tournaments;
   - tournament admin/captain бачить ті самі турніри, що до deploy;
   - Player Hub показує старі матчі без змін;
   - користувач без доступу не отримує доступ до official tournament.

6. Лише після smoke checks можна окремим deploy/config change увімкнути feature gate, коли з'являться routes наступних етапів.

## Rollback

Старий backend не розуміє новий composite key, тому rollback лише коду після успішної міграції не є безпечним.

1. Зупинити auth-server.
2. Зберегти окрему копію failed/current DB для діагностики.
3. Відновити перевірений pre-migration backup на шлях `DB_PATH`.
4. Повернути попередню версію backend.
5. Запустити server і повторити smoke checks доступів.

Після створення реальних `in_person_tournaments` відновлення pre-migration backup видалить їх. Тому перед будь-яким пізнішим rollback потрібен окремий export/план перенесення вже створених in-person даних.
