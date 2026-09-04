# Очні турніри — план реалізації

Статус: `draft for review`

Базовий документ: [`IN_PERSON_TOURNAMENTS_REQUIREMENTS.md`](./IN_PERSON_TOURNAMENTS_REQUIREMENTS.md).

Перший production-турнір: Чемпіонат України 2026. Дані ЧУ-2025 не мігруються до production-БД і використовуються лише як regression fixture.

## 1. Результат реалізації

Після завершення MVP мають працювати три окремі UI-контури:

1. `Admin → In-Person Tournaments` — створення та базове налаштування турніру адміністратором сайту.
2. `Player Hub → In-Person` — операційне проведення турніру його організаторами: учасники, check-in, жеребкування, Swiss, результати, standings і плейоф.
3. Окрема публічна сторінка кожного турніру — вкладки `Playoffs`, `Swiss`, `Rounds`, `Players`.

Новий домен працює через серверну SQLite-БД та API, не використовує наявні `tournaments`, `matches`, `duels`, `games`, `standings` і не впливає на GG Elo/rankings.

## 2. Поточна технічна база

- Backend — Express + SQLite у `auth-server/src/server.js`.
- Схема БД зараз створюється та доповнюється `ensure*Schema`-функціями під час запуску сервера.
- Нові ізольовані модулі вже використовуються, наприклад `images.js`, `challenges.js`, `tournament-cases.js`.
- Тести backend запускаються через Node test runner командою `npm test` у `auth-server`.
- Глобальна адмінка — монолітний snippet `gg-html/admin.html` із `SECTION_CONFIG` і окремими render/form-функціями.
- Player Hub має окремі snippets сторінок і два варіанти меню: `player-hub.html` та `player-hub-menu.html`.
- Публічні турнірні сторінки зараз є HTML/snippet-файлами, які самостійно читають JSON/API.
- Поточний deploy workflow автоматично оновлює та перезапускає лише `auth-server`; публікацію frontend snippets/CMS-сторінок треба включити в release checklist окремо.
- У `associations` технічним стабільним зовнішнім ключем є `code`; endpoint `/associations` повертає його як `id`.

## 3. Архітектурні рішення для реалізації

### 3.1. Модуль backend

Нову бізнес-логіку не додавати великим блоком безпосередньо в `server.js`. Рекомендована структура:

```text
auth-server/src/in-person/
  constants.js
  schema.js
  validation.js
  permissions.js
  repository.js
  result.js
  standings.js
  pairing.js
  playoff.js
  service.js
  serializers.js
  routes.js
  test-fixtures/ua2025.json
```

`server.js` має лише:

- дочекатися завершення schema migration;
- передати модулю `app`, `db`, auth middleware та DB helpers;
- зареєструвати `registerInPersonRoutes(...)`.

Pure-функції result/standings/pairing/playoff не повинні залежати від Express або глобального DB connection. Це дозволить тестувати правила без запуску всього сервера.

### 3.2. Ідентифікатори

- Для `in_person_tournaments`, participants, rounds і matches використовувати server-generated stable IDs, незалежні від slug, імені, draw number та table number.
- `slug` є окремим унікальним case-insensitive полем публічного URL і після публікації не змінюється звичайним редагуванням.
- API-поле `association_id` містить `associations.code`, тому реалізація не повинна шукати неіснуючу колонку `associations.id`.

### 3.3. Транзакції та безпечні повторні запити

Одна транзакція охоплює кожну складену команду:

- генерацію або скасування раунду;
- завершення раунду та запис standings revision;
- додавання запізнілого учасника;
- запуск плейоф;
- збереження результату та перенесення учасника в наступний playoff match;
- атомарну зміну трансляційного столу №1.

Загальну таблицю `in_person_idempotency_keys` у MVP не створювати. Повторну генерацію раунду захищають транзакція та унікальність одного нескасованого раунду з конкретним номером; повторний запит повертає вже створений раунд. Результат матчу зберігається retry-safe командою з поточним повним значенням, після невизначеної відповіді клієнт перечитує серверний стан. Окрему модель idempotency keys можна додати після MVP для складніших конкурентних команд.

### 3.4. Поточний результат матчу

Окрему one-to-one таблицю `in_person_match_results` не створювати. Один поточний результат зберігати в `in_person_matches`. Майбутню історію результатів можна буде додати окремою таблицею без зміни ідентичності матчу.

## 4. Цільова схема БД

Остаточний DDL оформлюється в `auth-server/src/in-person/schema.js` як versioned/idempotent migration із тестом на чистій і на оновлюваній БД.

### 4.1. `tournament_access_users`

Поточний primary key `(tournament_id, user_id)` треба без втрати даних замінити на:

```text
(tournament_entity_type, tournament_id, user_id)
```

Кроки міграції SQLite:

1. Створити replacement-таблицю з `tournament_entity_type NOT NULL` і CHECK для `tournament`/`in_person_tournament`.
2. Скопіювати всі старі записи зі значенням `tournament`.
3. Перевірити кількість і контрольні вибірки.
4. У межах транзакції перейменувати таблиці та відновити indexes.
5. Оновити всі наявні SQL-запити до `tournament_access_users`, додавши `tournament_entity_type = 'tournament'`. Без цього однакові ID двох доменів можуть видати неправильний доступ.
6. Для нового домену використовувати лише `tournament_entity_type = 'in_person_tournament'` і роль `admin`.
7. Оскільки SQLite не підтримує polymorphic foreign key на дві таблиці, існування target tournament перевіряти service layer і DB triggers; orphan access rows видаляти trigger-ом під час видалення відповідного турніру.

Ця міграція є найризикованішою частиною фундаменту й має бути окремим PR із regression-тестами наявних `/tournaments`, `/matches`, Player Hub і доступів captain/admin.

### 4.2. Нові таблиці

`cities`

- ID;
- `association_id` = `associations.code`;
- `name_en`, nullable `name_local`;
- `archived_at`, timestamps;
- partial unique index для активного normalized `name_en` у межах association.

`in_person_tournaments`

- stable ID, immutable public `slug`;
- `name_en`, `name_local`;
- scope `international`/`local`;
- nullable `association_id`;
- local subtype `final`/`qualifier`;
- nullable `qualifier_city_id`;
- `start_date`, `end_date` як `YYYY-MM-DD`;
- organizer name/URL, rules URL;
- `swiss_rounds_count`, `playoff_first_round`;
- fixed `draw_mode = manual_draw_numbers`;
- fixed `swiss_tiebreak_profile = swiss_standard_v1`;
- lifecycle status, publication fields, timestamps.

`in_person_participants`

- stable ID та tournament FK;
- `name_en`, `name_local`, BGA nickname;
- location: association для international або city для local;
- status `registered`/`checked_in`/`withdrawn`/`disqualified`;
- nullable unique-in-tournament `draw_number`;
- check-in/withdrawal/disqualification metadata;
- ознака та режим late entry;
- timestamps.

`in_person_rounds`

- stable ID та tournament FK;
- stage `swiss`/`playoff`;
- Swiss round number або playoff round key/order;
- status `draft`/`published`/`completed`/`cancelled`;
- publication/completion/cancellation metadata;
- timestamps.

MVP не додає до раунду `version`, `replaces_round_id`, `pairing_algorithm_version`, `seed` або `input_standings_revision`. Повторно згенерований раунд є новим active-записом із тим самим номером; скасований запис лишається в БД без явного version lineage.

`in_person_matches`

- stable ID та round FK;
- bracket position, `table_number`;
- participant A/B та `starting_participant_id`;
- match status та bye flag;
- result type, points A/B, winner/loser, finish reason, admin note;
- result/current-state revision та timestamps;
- playoff `next_match_for_winner`, `next_match_for_loser` і target slots;
- cancellation metadata.

`in_person_standings`

- tournament ID, revision, source completed round ID, participant ID;
- position, wins, Buchholz/Solkoff1/Solkoff2, VP difference, Sonneborn–Berger, byes;
- calculated timestamp;
- primary key `(tournament_id, revision, participant_id)`.

### 4.3. Обмеження та indexes

Обов'язково протестувати:

- conditional tournament fields для international/local/final/qualifier;
- `end_date >= start_date`;
- унікальний slug;
- унікальний active draw number у межах турніру;
- один active participant не може двічі бути в одному раунді;
- унікальний active table number у межах раунду;
- рівно один table №1 у кожному опублікованому playoff round;
- один нескасований Swiss round із конкретним номером;
- cancelled matches і rounds не входять до active read models;
- winner, loser і starter належать учасникам матчу;
- result-mode fields взаємовиключні.

## 5. API та права

Точні URL можна уточнити під час contract-тестів, але планова група route — `/in-person-tournaments`.

### 5.1. Global admin API

- `GET/POST /in-person-tournaments`;
- `GET/PATCH /in-person-tournaments/:id`;
- publish/cancel-before-start commands;
- `GET/POST/PATCH/DELETE /in-person-tournaments/:id/admins`;
- `GET/POST/PATCH /cities`, archive city;
- user options та association options для форм.

Mutations захищає наявний `requireAdmin`.

### 5.2. Organizer API

- список доступних користувачу in-person tournaments;
- CRUD participants до старту та дозволене редагування після старту;
- check-in/draw number validation;
- late-participant preview/confirm;
- Swiss round preview/confirm/publish/cancel-one;
- save/cancel match result;
- complete round/rebuild standings;
- withdraw/disqualify participant;
- create/confirm manual playoff bracket;
- set/swap playoff tables;
- complete playoff and tournament.

Кожен route використовує спільний middleware `requireInPersonTournamentAdmin`, який допускає global admin або user із access row типу `in_person_tournament`.

### 5.3. Public API

- public list опублікованих in-person tournaments;
- aggregate `GET /public/in-person-tournaments/:slug`;
- response містить tournament metadata, players, current Swiss standings, published active rounds і playoff bracket;
- response не містить user IDs, admin notes, draft/cancelled rounds або cancelled tournament;
- top-level `revision`/`updated_at` підтримує короткий cache/revalidation.

## 6. Етапи реалізації

### Етап 0. Закрити технічний контракт

Завдання:

- зафіксувати в UI contracts погоджений label Player Hub `In-Person` і повний заголовок `In-Person Tournaments`;
- зафіксувати API payloads/error codes і state transition table;
- зафіксувати точний порядок послаблення Swiss pairing constraints;
- перенести формулу Sonneborn–Berger із legacy-коду в окремий golden test;
- підготувати нормалізований test fixture ЧУ-2025 без importer-а;
- підготувати DDL та план backup/restore для `tournament_access_users`.

Результат етапу: schema/API contract не має невизначеностей, які можуть змінити таблиці після початку UI.

### Етап 1. Schema foundation і доступи

Завдання:

- створити backend module skeleton;
- реалізувати й протестувати schema migration;
- мігрувати `tournament_access_users` на polymorphic key;
- додати type predicate до кожного старого query;
- реалізувати permission helper для in-person domain;
- зареєструвати routes після успішної schema migration та захистити їх permission middleware;
- додати structured logs без audit events.

Тести:

- clean in-memory DB;
- upgrade DB із legacy access rows;
- повторний запуск migration;
- однакові tournament IDs у двох доменах не змішують права;
- старі captain/admin flows продовжують працювати.

Gate: усі наявні backend-тести зелені; row counts і доступи після тестової міграції збігаються.

Покриття: `IPT-ARC-*`, `IPT-ACL-*`, `IPT-DB-*`, частина `IPT-NFR-*`; `UH-34`.

Статус на 2026-09-04: етап реалізовано. Додано модулі schema/access/routes, атомарну перебудову `tournament_access_users`, explicit type predicate в усіх legacy queries, permission middleware, regression-тести та [production migration runbook](docs/in-person-schema-migration.md). In-person routes реєструються завжди й захищені middleware; окремого ENV feature flag немає. Загальна таблиця idempotency keys не створюється.

### Етап 2. Admin CRUD і базове налаштування

Backend:

- CRUD/archive для cities;
- CRUD/publish/cancel-before-start для in-person tournaments;
- conditional validation scope/subtype/association/city;
- period dates, organizer/rules URLs, Swiss/playoff configuration;
- призначення organizer users через adapted access table.

Frontend:

- додати `IN_PERSON_TOURNAMENTS_URL` і section до `gg-html/admin.html`;
- list/create/edit forms;
- inline creation city;
- user multiselect для tournament admins;
- preview розміру playoff bracket;
- посилання на Player Hub і public page.

Тести:

- local final, local qualifier, international;
- invalid city/association combinations;
- one-day/multi-day dates;
- duplicate slug/city;
- format cannot change after Swiss starts.

Gate: адміністратор сайту може створити повністю валідну draft-конфігурацію ЧУ-2026 і призначити організатора.

Покриття: `IPT-TRN-*`, `IPT-CITY-*`, `IPT-CFG-*`, `IPT-UI-001/002/008`; `UH-20`, `UH-29`, `UH-37` для admin selectors.

Статус на 2026-09-04: етап реалізовано. Додано admin API для міст і очних турнірів, conditional validation, server-generated IDs, стабільний slug, date periods, publish/cancel-before-start, блокування формату після старту Swiss та атомарне керування адміністраторами через `tournament_access_users`. У глобальну адмінку додано розділ `In-Person Tournaments` із create/edit формою, inline-створенням міста, playoff preview, user picker і переходами до майбутніх Player Hub/public сторінок. Повний backend suite після видалення ENV feature flag: 61/61 tests passed.

### Етап 3. Player Hub shell, participants і check-in

Backend:

- endpoint доступних organizer tournaments;
- participant CRUD і location validation;
- duplicate warning із дозволеним підтвердженням справжнього тезки;
- check-in та draw number rules;
- counters і readiness validation першого раунду.

Frontend:

- створити `gg-html/player-hub/in-person.html`;
- додати conditional item до `player-hub.html` і `player-hub-menu.html`;
- tournament selector/landing;
- screens `Players` і `Check-in`;
- responsive controls для мобільного використання в залі;
- явні validation errors для гравців без номера та дублів.

Тести:

- organizer бачить лише призначені tournaments;
- користувач без доступу не бачить menu item і отримує 403 від API;
- international/local participant location;
- gaps у draw numbers, включно з відсутнім №1;
- duplicate participant override;
- edit name/location після появи матчів не змінює IDs.

Gate: склад ЧУ-2026 можна повністю внести, check-in пройти з телефона, readiness першого раунду однозначний.

Покриття: `IPT-PLY-*`, `IPT-CHK-*`, `IPT-UI-003..007`; `UH-07`, `UH-27`, `UH-28`, `UH-37`, `UH-43`.

Статус на 2026-09-04: етап реалізовано. Організатор бачить лише призначені йому нескасовані `in_person_tournaments`; global admin бачить усі нескасовані. Додано CRUD учасників із conditional country/city validation, попередження про можливий дубль із явним підтвердженням справжнього тезки, check-in, унікальні додатні draw numbers без вимоги неперервної послідовності, counters і readiness першого Swiss-раунду. У Player Hub додано conditional розділ `In-Person`, tournament selector та мобільні screens `Players`/`Check-in`; `My Tournaments` і `Nationals` поки доступні лише global admin. Повний backend suite: 73/73 tests passed.

### Етап 4. Pure tournament engine

Реалізувати без HTTP/UI:

1. Result validator:
   - Carcassonne points;
   - simple `1–0`;
   - time forfeit;
   - withdrawal/disqualification/no-show/admin decision;
   - за рівних points перемагає гравець, який ходив другим.
2. Standings calculator:
   - Wins → Solkoff1 → Solkoff2 → VP difference → Sonneborn–Berger → bye flag → stable ID;
   - cancelled/incomplete rounds не враховуються;
   - withdrawn participants залишаються в standings.
3. First-round pairing:
   - compact list лише фактично виданих draw numbers;
   - останній/найбільший виданий номер отримує bye за odd count;
   - split halves і відповідні позиції.
4. Next-round pairing:
   - standings groups і float до сусідньої групи;
   - no rematch, якщо можливо;
   - starter balance;
   - задокументований deterministic fallback із warning reasons.

Тести:

- table-driven unit tests кожного result mode і validation error;
- gaps `2,3,5,8`, missing №1, odd list;
- rematch avoidance, starter streak, withdrawal, odd/even pools;
- deterministic result для однакового актуального стану турніру без випадкового seed;
- regression fixture ЧУ-2025;
- performance test на 256 participants.

Gate: pure engine повністю проходить unit/golden tests до підключення mutating routes.

Покриття: `IPT-SWP-*`, `IPT-RES-*`, `IPT-STD-*`; `UH-13`, `UH-15`, `UH-17`, `UH-18`, `UH-19`, `UH-43`.

### Етап 5. Swiss workflow API та UI

Backend commands:

- preview/confirm first round;
- preview/confirm next round;
- publish round;
- save results;
- complete round і write standings revision;
- блокування next round до завершення поточного;
- retry-safe поведінка для double click/network retry без загальної idempotency-таблиці;
- атомарність generation та standings update.

Player Hub:

- current round dashboard;
- pairing preview з warnings;
- publish action;
- швидке внесення result/starter по table number;
- progress `completed/total`;
- standings view, де остання видима колонка tie-break — VP difference;
- generate next round.

Тести:

- API integration happy path для 4, 5 та 8 учасників;
- double submit не створює duplicates;
- fault injection усередині generation/complete transaction залишає нуль partial changes;
- incomplete or invalid results block completion;
- lost-response retry повертає актуальний server state.

Gate: повний Swiss можна провести через Player Hub без прямого доступу до БД.

Покриття: `IPT-LIF-*`, основні `IPT-API-*`, `IPT-NFR-001/002`; `UH-05`, `UH-06`, `UH-13`, `UH-15`, `UH-17..19`, `UH-31`.

### Етап 6. Swiss exceptions і rollback

Реалізувати:

- cancel exactly one last Swiss round із preview результатів;
- повернення standings на попередню revision;
- послідовне повторне скасування для повернення ще на один раунд;
- створення нового active-запису з тим самим номером під час повторної генерації, без `version`/`replaces_round_id` та UI історії версій;
- withdrawal між раундами;
- withdrawal/no-show під час незавершеного матчу;
- late participant у першому раунді:
  - `late_bye`;
  - `pair_with_bye` із скасуванням початкової bye-перемоги;
- повторне завершення першого раунду та standings rebuild.

Тести:

- cancel empty, partially played і completed last round;
- edit previous result, rebuild, regenerate;
- cancel round 3, потім окремо cancel round 2;
- withdrawn participant не отримує opponent;
- late participant обома режимами, включно з двома bye у винятковому `late_bye` case;
- draft no-show і published no-show.

Gate: організатор може без ручного SQL виправити Swiss-помилку з погоджених MVP unhappy scenarios.

Покриття: `IPT-WDR-*`, `IPT-RBK-*`, `IPT-LAT-*`; `UH-01`, `UH-02`, `UH-08..11`, `UH-16`.

### Етап 7. Плейоф

Backend:

- створити bracket structure з вибраного first round;
- manual participant slot assignment без top-N restriction;
- validate duplicates/full bracket;
- configure table numbers і atomic swap table №1;
- winner propagation;
- semifinal loser propagation у Bronze medal match;
- safe correction ancestor, якщо descendant ще не зіграний;
- block correction, якщо descendant already played;
- technical Bronze result;
- completion requires Final + Bronze.

Player Hub:

- manual bracket setup і preview;
- table assignment per round;
- action `Make streaming table`;
- result forms і visual progression;
- clear block reason для played descendant.

Тести:

- brackets із Round of 32/16/Quarter-final/Semi-final;
- participant duplicate and missing slots;
- winner/loser routing, Final/Bronze exclusivity;
- table №1 uniqueness і atomic swap;
- ancestor correction with unplayed descendant;
- technical Bronze result та completion gate.

Gate: повний playoff із Bronze можна провести через Player Hub; rollback played descendants не реалізується.

Покриття: `IPT-POF-*`, `IPT-TBL-*`; `UH-22`, `UH-23`, `UH-25`, `UH-26`, `UH-39`, `UH-41`, `UH-42`.

### Етап 8. Публічна сторінка

Backend:

- public aggregate serializer;
- revision/updated timestamp;
- exclusion draft/cancelled/admin fields;
- cache policy з короткою revalidation.

Frontend:

- створити reusable snippet, рекомендовано `gg-html/in-person/in-person-tournament.html`;
- визначати tournament за slug/config сторінки;
- реалізувати постійні tabs `Playoffs`, `Swiss`, `Rounds`, `Players`;
- empty states до старту стадій;
- responsive playoff bracket і tables;
- streaming marker для table №1;
- локалізована назва з English fallback;
- створити окрему CMS/site page ЧУ-2026, яка використовує shared renderer.

Тести:

- public response не містить draft/cancelled/admin data;
- old regenerated rounds не видно;
- cancelled tournament не повертає tournament data;
- cache refresh після publish/result/rollback;
- mobile/desktop smoke tests усіх tabs.

Gate: публічний глядач бачить актуальні дані ЧУ-2026 без JSON export або git commit даних.

Покриття: `IPT-PUB-*`, `IPT-UI-009..011`; `UH-32`, `UH-39`, `UH-40` залишається поза MVP.

### Етап 9. End-to-end hardening і release

Автоматизація:

- оновити `auth-server/package.json`, щоб Node test runner охоплював нові nested tests;
- додати integration test повного малого турніру;
- додати regression fixture ЧУ-2025 без Excel/runtime importer;
- додати API authorization matrix tests;
- додати browser E2E критичного organizer flow або, якщо browser runner ще не підключений, окремий обов'язковий pre-release smoke script/checklist.

Release rehearsal:

1. Створити backup production SQLite через SQLite backup mechanism, а не file copy активної БД.
2. Запустити migration на свіжій копії production DB.
3. Перевірити `foreign_key_check`, integrity check, access row counts і старі tournament endpoints.
4. Розгорнути backend до публікації frontend menu item.
5. Встановити/оновити admin, Player Hub і public snippets/CMS pages.
6. Створити draft ЧУ-2026 та провести rehearsal із тестовими учасниками.
7. Перевірити rollback раунду, late participant, withdrawal, Final/Bronze і public refresh.
8. Очистити rehearsal-дані або залишити їх у непублічному test tournament.
9. Лише після UAT опублікувати ЧУ-2026.

Rollback release:

- frontend menu/public page можна прибрати без видалення БД;
- backend rollback допускається лише до версії, яка розуміє новий schema `tournament_access_users`, або разом із відновленням pre-migration SQLite backup;
- production migration не запускати без перевіреної restore-команди.

Gate: виконані `IPT-ACC-001..019`, усі MVP unhappy tests зелені, старі tournament flows не мають regression.

## 7. Рекомендований порядок PR

| PR | Вміст | Залежність |
|---|---|---|
| 1 | Schema module, migration `tournament_access_users`, permissions, legacy query predicates | немає |
| 2 | Cities + in-person tournament CRUD/API + Admin section | PR 1 |
| 3 | Player Hub shell + participants + check-in/draw numbers | PR 2 |
| 4 | Pure result/standings/pairing engine + ЧУ-2025 fixture tests | PR 1 |
| 5 | Swiss workflow API/UI | PR 3, PR 4 |
| 6 | Rollback, withdrawal, no-show, late participant | PR 5 |
| 7 | Playoff engine/API/UI + table №1 | PR 5 |
| 8 | Public aggregate API + reusable tournament page | PR 6, PR 7 |
| 9 | E2E, performance, migration rehearsal, release docs | PR 8 |

PR 3 і PR 4 можна виконувати паралельно після стабілізації schema/API contracts. Решта етапів залежить від завершення backend invariants попереднього кроку.

## 8. Матриця MVP unhappy scenarios

Обов'язкові automated integration/E2E tests:

- rollback/results: `UH-01`, `UH-02`, `UH-16`;
- retry safety/atomicity/validation: `UH-05`, `UH-06`, `UH-07`, `UH-17`, `UH-18`, `UH-19`, `UH-31`;
- roster exceptions: `UH-08`, `UH-09`, `UH-10`, `UH-11`, `UH-27`, `UH-28`, `UH-29`, `UH-37`, `UH-43`;
- pairing: `UH-13`, `UH-15`;
- playoff: `UH-22`, `UH-23`, `UH-25`, `UH-26`, `UH-39`, `UH-41`, `UH-42`;
- public/domain isolation: `UH-32`, `UH-34`.

Не реалізовувати спеціальні flows для `UH-03`, `UH-04`, `UH-12`, `UH-14`, `UH-21`, `UH-24`, `UH-30`, `UH-33`, `UH-35`, `UH-36`, `UH-38`, `UH-40`. Базові DB constraints і безпечне блокування можуть лишатися, але окремі recovery/UX-сценарії не повинні розширювати scope MVP.

## 9. Основні ризики

| Ризик | Як зменшити |
|---|---|
| Змішування доступів двох типів tournament | Окремий migration PR, type predicate у кожному legacy query, collision tests однакових ID |
| Частково застосована schema при старті | Versioned transaction migration, server readiness тільки після успіху schema setup |
| Помилка Swiss algorithm на edge cases | Один фіксований deterministic engine без випадковості, ЧУ-2025 golden fixture, small exhaustive fixtures; не деплоїти зміну алгоритму під час активного турніру |
| Неправильні standings після rollback | Source of truth — active completed matches; rebuild із нуля та revision acceptance tests |
| Дублювання результату через поганий інтернет у залі | Retry-safe запис повного поточного результату, server-confirmed state і повторне читання актуального матчу |
| Розбіжність Player Hub і public page | Один aggregate/service layer, revision-based refresh, відсутність generated JSON |
| Подальше розростання `server.js` | Новий route/service/repository module замість inline implementation |
| Frontend не потрапив у production разом із backend | Явний CMS/snippet release checklist і smoke verification кожного з трьох UI-контурів |

## 10. Definition of Done MVP

MVP готовий до ЧУ-2026, коли одночасно виконано все нижче:

- ЧУ-2026 створюється і налаштовується в global Admin.
- Призначений організатор бачить його в Player Hub і не отримує доступу до global Admin.
- Учасники, міста, check-in і draw numbers проходять усі server validations.
- Повний Swiss, включно з approved rollback і late participant, проводиться без ручного SQL/Sheets.
- Standings відтворюється з active results; visible tie-breaks завершуються VP difference.
- Плейоф вручну заповнюється, table №1 гнучко призначається, Final і Bronze завершуються.
- Окрема public page оновлюється з API та не показує draft/cancelled/history/admin data.
- Жоден in-person result не потрапляє в GG Elo/rankings або BGA jobs.
- Regression fixture ЧУ-2025 і acceptance tests `IPT-ACC-001..019` проходять.
- Production migration перевірена на копії БД, backup і restore procedure протестовані.
- Усі старі backend tests та smoke tests існуючих tournaments/Player Hub залишаються зеленими.
