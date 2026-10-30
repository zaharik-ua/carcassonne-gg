# CCL: управління гравцями — дизайн рішення

Статус: чернетка дизайну для рев'ю (30.10.2026). Код, схема БД та API ще не змінювалися.

Документ описує, як адміністратори турніру CCL створюють місця (плейсхолдери)
для асоціацій, як капітани обирають у них гравців, як фіксується GG Elo для
жеребкувань і як будуються два публічні списки гравців.

## 1. Контекст

- Турнір зберігається як `type = Individuals`, `category = CCL`
  (див. [tournament-classification.md](tournament-classification.md)).
  Зараз це один запис, наприклад `CCL-2027Q`.
- Турнір завжди має три етапи:
  1. **Qualifications** — раунди `Q1`, `Q2`, `Q3`;
  2. **League Stage**;
  3. **Playoffs**.
- Зараз список гравців CCL-2027 береться з Google Sheets через
  `json-data/tournaments-open.json` (`tournament_players`: `country`, `player_id`,
  `stage`, `qualified_by_name`, `qualified_by_link`, `elo`, `seeding`) і показується
  статичною сторінкою `gg-html/CCL-2027/CCL-2027-all-players.html`.
  Мета — перенести ці дані в `auth-server` з керуванням через Player Hub.

Цей документ стосується лише **складу учасників** та **фіксації Elo для
жеребкувань**. Саме жеребкування, групи, матчі League Stage і Playoffs —
поза обсягом.

## 2. Терміни

| Термін | Значення |
| --- | --- |
| **Слот** (плейсхолдер) | Одне місце асоціації в турнірі. Створює адмін; гравця в нього обирає капітан. |
| **Стадія входу** (`entry_stage`) | Куди слот дає путівку: `Main`, `Q1`, `Q2`, `Q3`. `Main` = пряма путівка в League Stage. |
| **Qualified by** | Підстава отримання слота: вільний текст + необов'язкове посилання. Напр. `Copa America Winner`. |
| **League qualified by** | Підстава потрапляння в League Stage для гравця з кваліфікації. Напр. `Q2, Group G Winner`. |
| **Фіксація Elo** | Знімок `profiles.gg_elo` гравців на момент жеребкування конкретної стадії + призначена корзина. |
| **Корзина** | Для Q-стадій: `Seeded` / `Unseeded`. Для League Stage: `Pot 1`…`Pot 5`. |

## 3. Ролі та сценарії

### 3.1. Адміністратор турніру

Глобальний адмін або користувач із роллю `admin` у `tournament_access_users`
(чинний механізм `hasTournamentAdminAccess`).

1. Додає асоціації-учасниці та призначає капітана кожної.
2. Створює слоти асоціації. Приклад для Argentina:

   | Стадія | Qualified by |
   | --- | --- |
   | Main | Copa America Winner |
   | Q1 | Copa America Winner |
   | Q3 | Copa America Winner |
   | Q3 | KoC Championship Winner |

3. Контролює заповнення, за потреби сам обирає або замінює гравця.
4. Перед жеребкуванням Q-стадії фіксує Elo і корзини `Seeded/Unseeded`.
5. Після кваліфікації позначає, хто з Q-гравців пройшов у League Stage
   (з текстом на кшталт `Q2, Group G Winner`), а хто вибув.
6. Перед жеребкуванням League Stage фіксує Elo і корзини `Pot 1–5`.

### 3.2. Капітан асоціації

Бачить лише слоти своєї асоціації. Для кожного слота обирає гравця зі своєї
асоціації (`profiles.association = association_id`), може замінити гравця до
дедлайну реєстрації цієї стадії. Стадію та `Qualified by` змінювати не може.

### 3.3. Публічні відвідувачі

Бачать два списки (розділ 6). Порожні слоти показуються як незаповнені місця.

## 4. Модель даних

### 4.1. Асоціації та капітани — повторне використання `tournament_teams`

Для CCL асоціація-учасниця зберігається як рядок у `tournament_teams`
(`team_id` = код асоціації, `captain_id` = капітан). Це дає без змін:

- доступ капітана до турніру в My Tournaments (`has_access`, `access_role = captain`,
  `captain_team_ids`);
- однакову семантику з командними турнірами.

Потрібна зміна: зараз `PUT /tournament-teams` дозволений лише для `type = Teams`.
Дозволити його також для пари `Individuals / CCL`. Інші інструменти командних
турнірів (склади `tournament_players`, lineups, командні матчі, standings)
для CCL **не вмикаються**.

При додаванні асоціації капітан за замовчуванням підставляється з профілю
з `team_captain = 1` цієї асоціації; адмін може обрати іншого.

> Рішення: гравці CCL **не** записуються в `tournament_players`. Єдине джерело
> правди — таблиця слотів нижче. `tournament_players` має `UNIQUE(tournament, team, player)`
> і не має місця для стадії, Qualified by чи корзин.

### 4.2. Нова таблиця `ccl_slots`

| Колонка | Тип | Опис |
| --- | --- | --- |
| `id` | TEXT PK | Стабільний ID слота |
| `tournament_id` | TEXT NOT NULL | FK `tournaments(id)` ON DELETE CASCADE |
| `association_id` | TEXT NOT NULL | Код асоціації; FK `(tournament_id, association_id)` → `tournament_teams(tournament_id, team_id)` ON DELETE CASCADE |
| `entry_stage` | TEXT NOT NULL | `CHECK IN ('Main','Q1','Q2','Q3')` |
| `qualified_by_name` | TEXT NOT NULL | Вільний текст |
| `qualified_by_link` | TEXT | URL, необов'язковий |
| `sort_order` | INTEGER | Порядок слотів у межах асоціації |
| `player_id` | TEXT NULL | FK `profiles(id)`; `NULL` = місце не заповнене |
| `league_status` | TEXT NULL | Лише для Q-слотів: `NULL` (ще грає / не визначено), `qualified`, `eliminated` |
| `league_qualified_by_name` | TEXT | Напр. `Q2, Group G Winner`; обов'язковий при `qualified` |
| `league_qualified_by_link` | TEXT | Напр. посилання на групу на сторінці кваліфікації |
| `created_at`, `updated_at`, `created_by`, `updated_by` | | Як в інших таблицях |

Обмеження:

- `UNIQUE (tournament_id, player_id)` для непорожніх `player_id` — один гравець
  займає максимум одне місце в турнірі (гравець із кваліфікації не може пізніше
  отримати пряму путівку — це вже правило CCL).
- Гравець слота належить асоціації слота (перевірка на сервері, як у чинному
  `PUT /tournament-players`).
- `league_status` для `Main`-слотів завжди `NULL`: вони в League Stage за визначенням.

### 4.3. Нова таблиця `ccl_draw_seeds` — фіксація Elo

Один рядок = один гравець у жеребкуванні однієї стадії.

| Колонка | Тип | Опис |
| --- | --- | --- |
| `tournament_id` | TEXT NOT NULL | |
| `draw_stage` | TEXT NOT NULL | `Q1`, `Q2`, `Q3`, `Main` |
| `slot_id` | TEXT NOT NULL | FK `ccl_slots(id)` ON DELETE CASCADE |
| `player_id` | TEXT NOT NULL | Гравець на момент фіксації |
| `elo` | REAL NOT NULL | Знімок `profiles.gg_elo` (або ручне значення, див. 5.3) |
| `seed` | TEXT NOT NULL | `Seeded`/`Unseeded` для Q; `Pot 1`…`Pot 5` для `Main` |
| `elo_source` | TEXT NOT NULL | `gg_elo` або `manual` |
| `fixed_at`, `fixed_by` | | Коли й хто зафіксував |

`UNIQUE (tournament_id, draw_stage, slot_id)`.

Окрема таблиця (а не колонки в `ccl_slots`) потрібна, бо один гравець може мати
два знімки: Q-стадія і потім League Stage, — і обидва показуються в різних списках.
Також це покриває можливий перехід Q1 → Q2 (відкрите питання 1).

### 4.4. Нова таблиця `ccl_stage_settings`

| Колонка | Опис |
| --- | --- |
| `tournament_id`, `stage` | PK; `stage` ∈ `Q1`, `Q2`, `Q3`, `Main` |
| `registration_ends_at` | Дедлайн для капітанів (напр. 11.11 для Q, 30.12 для Main) |
| `seeded_count` | Для Q: скільки гравців отримують `Seeded` (за замовчуванням — половина) |
| `pot_sizes` | Для `Main`: JSON з 5 чисел (за замовчуванням — рівний поділ) |

Чинне `tournaments.registration_ends_at` залишається загальним полем турніру;
для CCL доступ капітанів визначають дедлайни стадій.

## 5. Правила

### 5.1. Хто що може змінювати

| Дія | Капітан | Адмін турніру |
| --- | --- | --- |
| Додати/видалити асоціацію, змінити капітана | — | ✓ |
| Створити/видалити слот, змінити стадію і Qualified by | — | ✓ |
| Обрати гравця в порожній слот | ✓ до дедлайну стадії | ✓ завжди |
| Замінити або прибрати гравця | ✓ до дедлайну, якщо стадію ще не зафіксовано | ✓ (з інвалідацією знімка, 5.3) |
| Зафіксувати / скасувати фіксацію Elo | — | ✓ |
| Позначити результат кваліфікації | — | ✓ |

### 5.2. Учасники жеребкування стадії

- **Q1/Q2/Q3:** заповнені слоти з `entry_stage` = ця стадія.
  (Якщо переможці Q1 потрапляють у Q2 — див. відкрите питання 1.)
- **Main (League Stage):** заповнені `Main`-слоти + Q-слоти з `league_status = qualified`.

Фіксацію не можна запустити, поки в стадії є порожні слоти або Q-гравці з
невизначеним `league_status` (для `Main`). Адмін бачить список блокерів.

### 5.3. Фіксація Elo

1. Адмін натискає **Preview draw seeds** для стадії. Сервер рахує, нічого не зберігаючи:
   - бере `profiles.gg_elo` кожного учасника;
   - сортує за спаданням Elo; нічия → за `profiles.gg_rating_position`;
   - для Q: перші `seeded_count` → `Seeded`, решта → `Unseeded`;
   - для `Main`: послідовно заповнює `Pot 1`…`Pot 5` за `pot_sizes`.
2. Адмін бачить таблицю попереднього перегляду. Гравцям без `gg_elo`
   потрібно вказати ручне значення (`elo_source = manual`), інакше фіксація
   заблокована.
3. **Fix** зберігає знімок у `ccl_draw_seeds` однією транзакцією, з записом в `audit_trail`.
4. Після фіксації капітани не можуть міняти гравців у слотах цієї стадії.
   Якщо адмін змінює гравця в зафіксованому слоті, рядок знімка для цього слота
   видаляється, і стадія позначається як «потребує перефіксації».
5. **Unfix** видаляє весь знімок стадії (лише адмін, із підтвердженням).

Ручне перевизначення корзини окремого гравця в першій версії не робимо:
корзини — чиста функція Elo і розмірів корзин. Якщо регламент цього вимагатиме,
додамо `seed_source = manual`.

### 5.4. Результати кваліфікації

Для кожного Q-слота адмін ставить `qualified` + `League qualified by`
(текст і посилання) або `eliminated`. Це можна робити поступово, по мірі
завершення груп.

## 6. Списки гравців

### 6.1. Список 1 — All players

Усі слоти всіх асоціацій: і `Main`, і `Q1–Q3`.

| Колонка | Вміст |
| --- | --- |
| Association | Прапор + назва; об'єднані клітинки на всі слоти асоціації (як зараз) |
| Player | Аватар + нікнейм; порожній слот → `—` |
| Elo | Лише для Q-слотів. Після фіксації — зафіксоване Elo + іконка корзини (зелена — `Seeded`, червона — `Unseeded`, чинні `green-dot.png` / `red-dot.png`). До фіксації — поточне GG Elo сірим без іконки. Для `Main` — порожньо |
| Qualified by | `qualified_by_name` з посиланням `qualified_by_link` |
| Stage | `Main`, `Q1`, `Q2`, `Q3` |

Сортування: асоціація → стадія (`Main`, `Q1`, `Q2`, `Q3`) → `sort_order`.
Фільтри (як зараз): `Registered`, `Stage`. Додатково: результат кваліфікації
(`Qualified` / `Eliminated`) — візуально позначати рядок вибулого гравця
приглушеним стилем.

### 6.2. Список 2 — League Stage players

`Main`-слоти + Q-слоти з `league_status = qualified`. Вибулі та невизначені не показуються.

| Колонка | Вміст |
| --- | --- |
| Association | Як у списку 1 |
| Player | Як у списку 1 |
| Elo | Для всіх. Після фіксації `Main` — зафіксоване Elo + іконка корзини 1–5. До фіксації — поточне GG Elo сірим |
| Qualified by | `Main`-слот → `qualified_by_name/link` (те саме, що в списку 1). Q-слот → `league_qualified_by_name/link`, напр. `Q2, Group G Winner` |

Сортування: асоціація → гравець. Фільтр за корзиною `Pot 1–5`.

Іконки корзин 1–5: п'ять кольорових кружечків із цифрою (CSS, без картинок);
кольори — відкрите питання 4.

## 7. API

Автентифіковані (адмін турніру або капітан — сервер обрізає дані до своїх асоціацій):

| Метод | Шлях | Хто | Призначення |
| --- | --- | --- | --- |
| PUT | `/tournament-teams` | адмін | Чинний ендпоінт; дозволити для CCL. Асоціації + капітани |
| GET | `/ccl/:tournamentId/slots` | адмін, капітан | Слоти, гравці, статуси, знімки, дедлайни |
| POST | `/ccl/:tournamentId/slots` | адмін | Створити `count` слотів: `{association_id, entry_stage, qualified_by_name, qualified_by_link, count}` |
| PATCH | `/ccl/slots/:id` | адмін; капітан — лише `player_id` | Зміна слота / вибір гравця |
| DELETE | `/ccl/slots/:id` | адмін | Видалити слот |
| PATCH | `/ccl/slots/:id/result` | адмін | `{league_status, league_qualified_by_name, league_qualified_by_link}` |
| PUT | `/ccl/:tournamentId/stages/:stage` | адмін | Дедлайн, `seeded_count`, `pot_sizes` |
| POST | `/ccl/:tournamentId/draws/:stage/preview` | адмін | Розрахунок корзин без збереження |
| POST | `/ccl/:tournamentId/draws/:stage/fix` | адмін | Зберегти знімок (`{manual_elo: {slot_id: value}}`) |
| DELETE | `/ccl/:tournamentId/draws/:stage` | адмін | Скасувати фіксацію |

Публічний:

| Метод | Шлях | Призначення |
| --- | --- | --- |
| GET | `/public/ccl/:tournamentId/players` | `{ all_players: [...], league_players: [...], stages: {...} }` |

Рядки публічної відповіді зберігають поля чинного JSON (`country`, `player`, `player_id`,
`stage`, `qualified_by_name`, `qualified_by_link`, `elo`, `seeding`), щоб
`CCL-2027-all-players.html` переключився на нове джерело з мінімальними змінами.
Додаткові поля: `slot_id`, `association_id`, `flag`, `avatar`, `elo_fixed` (bool),
`league_status`, `pot`.

Перевірки — у чистих функціях окремого модуля `auth-server/src/ccl-slots.js`
(з тестами `ccl-slots.test.js`), за зразком `tournament-roster.js` і
`planned-tournament-gg-ratings.js`.

## 8. Інтерфейс

### 8.1. My Tournaments → CCL турнір (режим Admin)

Нова секція **CCL players** замість командних секцій.

```
CCL players                                    [+ Add association]
┌──────────────┬─────────────────┬─────────────────────────────────────────────┐
│ Association  │ Captain         │ Slots                                       │
├──────────────┼─────────────────┼─────────────────────────────────────────────┤
│ 🇦🇷 Argentina │ [player ▾]      │ Main · Copa America Winner      • Juan123  ✎ │
│              │                 │ Q1   · Copa America Winner      • —        ✎ │
│              │                 │ Q3   · Copa America Winner      • —        ✎ │
│              │                 │ Q3   · KoC Championship Winner  • Leo_ar   ✎ │
│              │                 │ [+ Add slot]                                │
└──────────────┴─────────────────┴─────────────────────────────────────────────┘

Add slot:  Stage [Main ▾]  Qualified by [________]  Link [________]  Count [1]  [Add]
```

Нижче — блок **Draws**, по рядку на стадію:

```
Stage  Deadline     Players  Empty  Status          Actions
Q1     11.11 23:59  38/39    1      Not fixed       [Preview] [Settings]
Q2     11.11 23:59  32/32    0      Fixed 14.11     [View] [Unfix]
Q3     ...
Main   30.12 23:59  40/40    0      Needs re-fix ⚠  [Preview] [Unfix]
```

І блок **Qualification results**: таблиця Q-гравців із вибором
`Qualified` / `Eliminated` / `—` та полями League qualified by.

### 8.2. My Tournaments → CCL турнір (режим Captain)

```
Argentina — CCL 2027 slots
Stage  Qualified by               Player              Deadline
Main   Copa America Winner        [Juan123      ▾]    30.12
Q1     Copa America Winner        [Select player ▾]   11.11
Q3     Copa America Winner        [Select player ▾]   11.11
Q3     KoC Championship Winner    Leo_ar  🔒 fixed    —
```

Селектор гравця — пошук по профілях асоціації; гравці, які вже займають
інший слот турніру, неактивні з поясненням.

### 8.3. Публічні сторінки

`gg-html/CCL-2027/CCL-2027-all-players.html` → список 1;
`CCL-2026-main-players.html` як основа для списку 2 (у ній уже є Pot 1–5).
Обидві читають `/public/ccl/:tournamentId/players`.

## 9. Перенесення наявних даних CCL-2027Q

Зараз у JSON 134 рядки (`Main` 38, `Q1` 39, `Q2` 32, `Q3` 25), більшість без гравців.
Одноразовий скрипт `import_ccl_slots` (з `--dry-run`):

- `country` → `association_id` (за назвою команди);
- `stage`, `qualified_by_name`, `qualified_by_link` → слот;
- `player_id` → `player_id`, якщо є;
- `elo`, `seeding` не переносяться (Elo ще не фіксувалося).

Після імпорту Google Sheets перестає бути джерелом для CCL.

## 10. Етапи реалізації

1. **Модель і правила:** схема трьох таблиць, модуль `ccl-slots.js` (валідація,
   розрахунок корзин, побудова двох списків), unit-тести.
2. **API:** ендпоінти розділу 7, дозвіл `tournament_teams` для CCL, audit trail; тести API.
3. **Admin UI** у My Tournaments: асоціації, слоти, дедлайни, результати кваліфікації, фіксація.
4. **Captain UI** у My Tournaments.
5. **Публічний API і сторінки** списків 1 і 2.
6. **Імпорт** наявних слотів CCL-2027Q, перевірка на копії БД, потім на робочій.

## 11. Відкриті питання

1. **Перехід між Q-раундами.** Чи можуть переможці Q1 потрапляти в Q2 (а Q2 — у Q3),
   чи кожен Q-раунд веде лише в League Stage? У 2026 були групи A–E (Q1) і F–I (Q2)
   по 8 гравців. Якщо переходи є, потрібне поле «поточна стадія» і показ цього у списку 1.
2. **Кількість Seeded** у Q-стадії: рівно половина, чи залежить від кількості груп
   (напр. 4 на групу з 8)?
3. **Розміри корзин** League Stage: завжди 5 рівних, чи бувають нерівні?
4. **Іконки Pot 1–5:** CSS-кружечки з цифрою чи окремі картинки в `gallery`? Кольори?
5. **ID турніру:** залишаємо `CCL-2027Q` для всіх трьох етапів чи перейменовуємо на `CCL-2027`?
6. **Дедлайни:** чи однаковий дедлайн для Q1, Q2, Q3, чи окремий для кожного?
7. **Qualified by:** вільний текст, чи варто давати вибір із турнірів carcassonne.gg
   з автопідстановкою назви й посилання (з можливістю ввести довільний текст)?
8. **Видимість порожніх слотів** публічно: показувати як зараз (рядок без гравця)?
