# BGA game replay: актуальна production-логіка

## 1. Призначення

Цей документ описує лише поточну реалізацію отримання, зберігання та
оновлення BGA replay у Carcassonne GG. Тут немає старої polling-логіки,
варіантів майбутньої реалізації або етапів упровадження.

Система:

- ставить replay нових рейтингових ігор у персистентну SQLite-чергу;
- не робить replay-запит безпосередньо під час запису результату гри;
- виконує не більше одного `logs.html` для однієї гри за один прохід;
- не очікує підготовки BGA-архіву всередині HTTP-процесу;
- зберігає готовий replay навіть із fallback-кольорами;
- окремо планує оновлення кольорів і повторну перевірку замовленого архіву;
- пропускає всі `logs.html` через спільний rolling budget;
- надає основний пул акаунтів 1–3 і два послідовні standby-рівні: акаунти 4 та 5;
- розділяє `fresh`, `historical` і `manual` використання;
- показує чергу, бюджети, cooldown та overrides у `Admin → BGA Replay Queue`.

Усі дати, записані worker у SQLite, є UTC.

## 2. BGA endpoints

### Отримання replay

```text
GET /archive/archive/logs.html?table={bga_table_id}&translated=true
```

Кожне звернення до цього endpoint резервується до HTTP-запиту і витрачає одну
одиницю локального rolling-бюджету незалежно від результату.

### Замовлення архіву

```text
GET /gamereview/gamereview/requestTableArchive.html?table={bga_table_id}
```

Цей endpoint викликається не більше одного разу для одного запису replay.
Запит журналюється у `bga_replay_requests`, але не входить до бюджету
`logs.html`. Після нього немає polling або `sleep`: перевірку виконує наступний
запуск worker після `next_attempt_at`.

## 3. Дані у SQLite

### `game_replays`

Один запис відповідає одній грі з `games`.

Основні поля:

| Поле | Значення |
|---|---|
| `game_id` | Первинний ключ, посилання на `games.id` |
| `bga_table_id` | Ідентифікатор BGA table |
| `status` | `pending`, `fetching`, `ready` або `error` |
| `events_json` | Нормалізовані `pickTile`, `playTile`, `playPartisan` |
| `players_json` | Гравці та нормалізовані кольори |
| `carcassonne_lab_url` | Закодований replay для CarcassonneLab |
| `board_stats_json` | Фінальні `width` і `height` поля |
| `meeple_stats_json` | Розміщення, повернення та залишок міплів |
| `scoring_json` | Події та суми очок за типами об'єктів |
| `player_time_json` | Тривалість гри й активний час гравців |
| `fetched_at` | Час останнього успішного отримання replay |
| `last_attempt_at` | Час останнього `logs.html` |
| `last_error` | Остання класифікована помилка з endpoint |
| `next_attempt_at` | Найраніший час наступної автоматичної спроби |
| `retry_reason` | `initial`, `archive`, `colors` або `NULL` |
| `queue_class` | `fresh` або `historical` |
| `queued_at` | Час додавання до черги |
| `historical_batch_id` | Необов'язковий ID historical-пакета |
| `history_request_count` | Кількість виконаних `logs.html` для гри |
| `color_refresh_count` | Кількість виконаних color-refresh |
| `color_source` | `bga`, `fallback` або `NULL` до першого успіху |
| `archive_requested_at` | Час одноразового `requestTableArchive` |
| `last_account_label` | Акаунт останнього replay-запиту |
| `lease_owner`, `lease_until` | Захоплення запису worker |

`status = ready` означає, що replay придатний для public API. Це справедливо і
для `color_source = fallback`, навіть якщо `retry_reason = colors` ще
запланований.

Raw BGA logs не зберігаються: нормалізація та побудова похідних даних
відбуваються в пам'яті.

### Budget-таблиці

`bga_replay_requests` містить кожен `logs.html` і кожен запит підготовки архіву:

- account label без пароля;
- BGA table ID;
- endpoint;
- `request_class`: `fresh`, `historical` або `manual`;
- час спроби;
- результат або помилку;
- `budget_override_id`, якщо запит став можливим завдяки override.

`bga_replay_account_state` містить `cooldown_until`, `last_limit_at`,
`last_selected_at` і `last_error`.

`bga_replay_budget_overrides` зберігає активні, прострочені та відкликані
тимчасові зміни лімітів.

## 4. Створення fresh-завдання

Коли result sync уперше додає нову рейтингову гру, він викликає
`enqueue_fresh_game_replay` у тій самій SQLite-транзакції.

Створюється стан:

```text
status = pending
retry_reason = initial
queue_class = fresh
queued_at = now
next_attempt_at = now + 5 minutes
history_request_count = 0
color_refresh_count = 0
color_source = NULL
```

Нерейтингові ігри автоматично в replay-чергу не додаються. До настання
`next_attempt_at` BGA-запит не виконується.

## 5. Результат одного replay-запиту

### Replay із повними BGA-кольорами

Worker записує replay та похідні дані й завершує автоматичний цикл:

```text
status = ready
color_source = bga
retry_reason = NULL
next_attempt_at = NULL
last_error = NULL
```

### Replay без повних BGA-кольорів

Кольори вважаються повними, лише якщо знайдено обох гравців із ходами, обидва
`color_hex` підтримуються, а нормалізовані кольори різні.

Якщо умова не виконана, обом гравцям призначається узгоджена пара `red` і
`green` у порядку першого `playTile`. Fallback одночасно записується в
`players_json`, `events_json`, `meeple_stats_json` і CarcassonneLab URL.

Після першого успішного fetch:

```text
status = ready
color_source = fallback
retry_reason = colors
next_attempt_at = now + 15 minutes
```

Color-refresh не приховує готовий replay. Якщо повторна відповідь містить
повні кольори, дані атомарно замінюються і `color_source` стає `bga`. Якщо
кольори знову неповні, наявний fallback залишається без змін, збільшується
`color_refresh_count`, а автоматичні color-refresh завершуються:

```text
status = ready
color_source = fallback
retry_reason = NULL
next_attempt_at = NULL
```

### BGA-архів відсутній

Якщо `logs.html` повертає `Cannot find gamenotifs log file` і
`archive_requested_at` порожній, система:

1. один раз викликає `requestTableArchive.html`;
2. записує `archive_requested_at`;
3. не робить polling;
4. планує повторну перевірку.

Для першого отримання replay:

```text
status = pending
retry_reason = archive
next_attempt_at = now + archive_retry_minutes
```

Для вже готового fallback replay, який оновлює кольори:

```text
status = ready
retry_reason = colors
next_attempt_at = now + archive_retry_minutes
```

Затримка береться із system setting
`bga_replay_archive_retry_minutes`. Допустимий діапазон — 1–60 хвилин,
значення за замовчуванням — 2 хвилини.

Якщо під час наступної перевірки архів досі відсутній, другий
`requestTableArchive` не надсилається. Автоматичне опрацювання завершується:

- для replay без корисних даних: `status = error`;
- для готового fallback replay: `status = ready`;
- в обох випадках `retry_reason` і `next_attempt_at` очищуються, а
  `last_error` пояснює причину.

### Тимчасова помилка

Auth/network та інші тимчасові помилки не запускають каскад акаунтів.
Зберігається поточний `retry_reason`, а наступна спроба планується через
15 хвилин. Готовий fallback replay залишається `ready`.

Під час створення BGA HTTP-сесії Selenium очікує до 10 секунд появи
`bgaConfig.requestToken`. Якщо token не з'явився, повне оновлення сесії
повторюється до трьох разів. Після трьох невдалих спроб помилка вважається
тимчасовою і replay відкладається.

### Replay limit

Явна відповідь `limit (replay)` завершує поточний worker, не перевіряє ту саму
гру іншим акаунтом, ставить вибраний акаунт у cooldown на 24 години за
замовчуванням і залишає завдання due для наступного безпечного запуску.

### Постійна помилка доступу або некласифікована прикладна помилка

Автоматичні спроби завершуються. Запис без готового replay отримує
`status = error`; готовий fallback replay залишається `ready`. У будь-якому
випадку `retry_reason = NULL`, `next_attempt_at = NULL`, а діагностика
зберігається у `last_error`.

## 6. Worker modes і пріоритети

CLI `retry_pending_game_replays.py` має три режими. Усі вони використовують
спільний код, один lock-файл, lease і rolling budget.

| Mode | Що обробляє | systemd timer | Budget class |
|---|---|---|---|
| `fresh` | Due-записи `queue_class = fresh` | кожні 2 хвилини | `fresh` |
| `archive-follow-up` | Due historical із `archive_requested_at IS NOT NULL` | щохвилини | `historical` |
| `historical` | Звичайний due historical backlog | кожні 15 хвилин | `historical` |

`archive-follow-up` — режим виконання, а не значення `queue_class`. Записи
залишаються `historical`, тому цей режим не витрачає fresh reserve, не обробляє
untouched historical backlog і в статистиці Admin входить до `Historical`.

Fresh має абсолютний пріоритет. Перед кожним historical HTTP-запитом worker
повторно перевіряє due fresh. Якщо fresh з'явився, historical-запис
звільняється, а запуск завершується з `stop_reason = fresh_priority`.

Загальні обмеження одного запуску:

- `--limit` від 1 до 3;
- максимум один `logs.html` для однієї гри;
- у historical і `archive-follow-up` — максимум один успішно зарезервований
  `logs.html` на один акаунт за запуск;
- один активний replay-worker через `flock`;
- lease за замовчуванням 300 секунд;
- прострочений lease може бути підібраний наступним worker.

Через правило «один historical-запит на акаунт за запуск» worker може
зупинитися раніше за `--limit`. Якщо доступний лише один акаунт активного
standby-рівня, один historical-запуск виконає через нього один запит. Наступний
запис може бути повернутий у чергу з `stopped_by_budget = true`, хоча
rolling-бюджет цього акаунта ще не вичерпаний. Новий запуск знову може
використати цей акаунт один раз.

`processed` у JSON summary означає кількість захоплених записів, а `requests` —
фактичні `logs.html`. Тому при поверненні завдання у чергу `processed` може
бути більшим за `requests`.

## 7. Основний пул акаунтів і standby

Credentials читаються з `.env`:

```env
BGA_EMAIL=...
BGA_PASSWORD=...
BGA_EMAIL_2=...
BGA_PASSWORD_2=...
BGA_EMAIL_3=...
BGA_PASSWORD_3=...
BGA_EMAIL_4=...
BGA_PASSWORD_4=...
BGA_EMAIL_5=...
BGA_PASSWORD_5=...
```

Акаунти 1–3 утворюють звичайний пул. Серед кандидатів без cooldown і з
доступним бюджетом вибирається акаунт із найменшим використанням за останні
24 години; при рівності враховується останній запит і порядок акаунтів.

Акаунти поділені на три послідовні рівні пріоритету:

- рівень 0: основний пул акаунтів 1–3;
- рівень 1: перший standby — акаунт 4;
- рівень 2: другий standby — акаунт 5.

Replay gateway використовує послідовні standby-умови. Тому:

- акаунт 4 доступний, коли кожен із налаштованих акаунтів 1–3 або має
  активний cooldown, або має `0 total available` у rolling-бюджеті;
- акаунт 5 доступний, коли кожен із налаштованих акаунтів 1–4 або має
  активний cooldown, або має `0 total available` у rolling-бюджеті;
- акаунти 4 і 5 не беруть участі у звичайному балансуванні основного пулу;
- акаунти 4 і 5 виключені зі звичайної автоматичної ротації HTTP-сесій;
- replay gateway може явно вибрати їх лише після перевірки відповідної
  standby-умови.

В Admin акаунти показуються як `Standby 1` і `Standby 2`. Стан акаунта
`Available` означає, що він не має власного cooldown; eligibility відповідного
standby-рівня та фактичний залишок rolling-бюджету показуються окремо. Повторне
використання в тому самому historical-запуску все одно заборонене per-run
правилом.

## 8. Rolling budget і overrides

Базові значення на акаунт:

```env
BGA_REPLAY_TOTAL_LIMIT=80
BGA_REPLAY_FRESH_RESERVE=50
BGA_REPLAY_HISTORICAL_LIMIT=30
BGA_REPLAY_MAX_TOTAL_LIMIT=100
BGA_REPLAY_COOLDOWN_HOURS=24
```

Rolling-вікно — останні 24 години. Для `fresh` і `manual` діє:

```text
total_used_24h < effective_total_limit
```

Для `historical`, включно з `archive-follow-up`, одночасно діє:

```text
historical_used_24h < effective_historical_limit
total_used_24h < effective_total_limit - effective_fresh_reserve
```

Перед HTTP-запитом gateway у `BEGIN IMMEDIATE` перевіряє cooldown і standby,
рахує rolling usage, застосовує active override, вибирає акаунт і вставляє
`bga_replay_requests` із `outcome = reserved`. Лише після commit виконується
HTTP-запит. Тому невдалий або rate-limited `logs.html` також витрачає одиницю
локального бюджету.

Global admin може створити тимчасовий override для одного або кількох
акаунтів: `extra_historical_limit = E`, необов'язковий
`total_limit_override = T`, обов'язкові expiry та reason.

```text
effective_total_limit = T або base total
effective_historical_limit = min(base historical + E, effective total)
effective_fresh_reserve = effective total - effective historical
```

`T` має бути більшим за base total і не більшим за configured maximum 100.
Новий override відкликає попередній active override для вибраного акаунта, а
не додається до нього. Прострочені та відкликані записи зберігаються для
аудиту. Вже виконані запити залишаються у rolling-вікні після завершення
override.

## 9. Historical queue і legacy backfill

Historical enqueue є ідемпотентним:

- готовий replay із `color_source = bga` не додається повторно без `force`;
- новий або неготовий replay отримує `retry_reason = initial`;
- готовий fallback replay отримує `retry_reason = colors`, не втрачаючи даних;
- уже запланований fresh або historical запис не дублюється;
- fallback із уже виконаним color-refresh не планується повторно без `force`.

Для historical зберігається `queue_class = historical`, тому всі наступні
архівні та color-refresh переходи залишаються в historical budget.

One-off `backfill_existing_game_replays.py` використовується лише для
підготовки legacy-записів:

- видаляє всі `status = error` при `--apply`;
- проходить усі `status = ready`;
- не змінює `color_source`, бо його поточне значення є авторитетним;
- перебудовує доступні похідні поля;
- ставить ready/fallback із `color_refresh_count < 1` у historical
  color-refresh;
- не переплановує fallback, для якого color-refresh уже виконувався;
- dry-run є режимом за замовчуванням;
- `--apply` виконує зміни однією транзакцією під replay lock.

## 10. Admin → BGA Replay Queue

Розділ доступний global admin і показує:

- due та scheduled для `fresh` і `historical`;
- ready із BGA-кольорами та fallback-кольорами;
- `error` і manual-required;
- rolling-24h attempts та successful окремо для `fresh`, `historical`,
  `manual`;
- effective total/historical limits і fresh reserve;
- `historical_available_now`;
- cooldown та його завершення;
- standby-рівень та eligibility акаунтів 4 і 5;
- active і попередні overrides.

Відпрацювання `archive-follow-up` збільшує `Historical attempts` та, якщо
успішне, `Historical successful`. Окремої категорії `archive-follow-up` в UI
немає, тому конкретний режим запуску з агрегованої таблиці визначити не можна.

Admin API:

```text
GET    /admin/bga-replay-budget
POST   /admin/bga-replay-budget/overrides
DELETE /admin/bga-replay-budget/overrides/{id}
```

Refresh сторінки не виконує BGA-запитів.

## 11. systemd

Production використовує template service `bga-replay-worker@.service` і три
timers:

```text
bga-replay-fresh.timer
bga-replay-archive-follow-up.timer
bga-replay-historical.timer
```

Фактичні інтервали:

- fresh: `OnUnitInactiveSec=2min`;
- archive follow-up: `OnUnitInactiveSec=1min`;
- historical: `OnUnitInactiveSec=15min`.

Кожен service запускає `retry_pending_game_replays.py` з відповідним
`--queue-class` і `--limit 3`. Timeout одного запуску — 5 хвилин. Stdout і
stderr додаються до:

```text
/var/log/carcassonne/bga-replay-worker.log
```

Усі режими використовують спільний lock:

```text
data/auth.sqlite.bga-replay-worker.lock
```

Наявність lock-файлу не означає, що lock зайнятий. Не треба видаляти цей файл
під час роботи; власника перевіряють через `lsof` або `fuser`.

## 12. Ручні команди

Усі production-команди нижче запускаються з:

```bash
cd /home/carcassonne-gg/auth-server
```

### 12.1. Backup production SQLite

```bash
backup_file="data/auth-$(date -u +%Y%m%dT%H%M%SZ).sqlite"
sqlite3 data/auth.sqlite ".backup '$backup_file'"
sqlite3 "$backup_file" "PRAGMA quick_check;"
echo "$backup_file"
```

Очікуваний результат `PRAGMA quick_check`: `ok`.

### 12.2. Перевірити credentials без показу секретів

```bash
grep -E '^BGA_(EMAIL|PASSWORD)(_2|_3|_4)?=' .env \
  | sed 's/=.*/=<set>/'
```

### 12.3. Подивитися due-чергу

```bash
sqlite3 -header -column data/auth.sqlite "
SELECT queue_class, status, retry_reason, COUNT(*) AS due
FROM game_replays
WHERE retry_reason IN ('initial', 'archive', 'colors')
  AND next_attempt_at IS NOT NULL
  AND datetime(next_attempt_at) <= datetime('now')
GROUP BY queue_class, status, retry_reason
ORDER BY queue_class, retry_reason;
"
```

Historical archive follow-up:

```bash
sqlite3 -header -column data/auth.sqlite "
SELECT
  game_id, status, retry_reason, last_account_label,
  archive_requested_at, next_attempt_at,
  substr(last_error, 1, 120) AS last_error
FROM game_replays
WHERE queue_class = 'historical'
  AND archive_requested_at IS NOT NULL
  AND retry_reason IN ('archive', 'colors')
ORDER BY datetime(next_attempt_at), game_id
LIMIT 20;
"
```

### 12.4. Smoke tests worker

Один fresh:

```bash
./.venv/bin/python retry_pending_game_replays.py \
  --db-path data/auth.sqlite \
  --queue-class fresh \
  --limit 1
```

До трьох historical:

```bash
./.venv/bin/python retry_pending_game_replays.py \
  --db-path data/auth.sqlite \
  --queue-class historical \
  --limit 3
```

Один historical після `requestTableArchive`:

```bash
./.venv/bin/python retry_pending_game_replays.py \
  --db-path data/auth.sqlite \
  --queue-class archive-follow-up \
  --limit 1
```

Якщо доступний лише один акаунт активного standby-рівня, для кількох
historical потрібні окремі запуски:

```bash
for run in 1 2 3; do
  echo "===== Historical run $run/3 ====="
  ./.venv/bin/python retry_pending_game_replays.py \
    --db-path data/auth.sqlite \
    --queue-class historical \
    --limit 1
done
```

`status = locked` означає, що інший worker утримує спільний `flock`. Lock-файл
видаляти не потрібно.

### 12.5. Перевірити останні змінені replay

```bash
sqlite3 -header -column data/auth.sqlite "
SELECT
  game_id, status, queue_class, retry_reason, color_source,
  color_refresh_count, last_account_label, archive_requested_at,
  next_attempt_at, substr(last_error, 1, 120) AS last_error, updated_at
FROM game_replays
ORDER BY datetime(updated_at) DESC
LIMIT 20;
"
```

Один конкретний запис:

```bash
sqlite3 -header -column data/auth.sqlite "
SELECT * FROM game_replays WHERE game_id = 'GAME_ID';
"
```

### 12.6. Останні BGA replay-запити та rolling usage

```bash
sqlite3 -header -column data/auth.sqlite "
SELECT
  id, account_label, bga_table_id, endpoint, request_class,
  outcome, substr(error, 1, 120) AS error, attempted_at
FROM bga_replay_requests
ORDER BY id DESC
LIMIT 30;
"
```

```bash
sqlite3 -header -column data/auth.sqlite "
SELECT
  account_label, request_class, COUNT(*) AS attempts_24h,
  SUM(CASE WHEN outcome = 'ready' THEN 1 ELSE 0 END) AS successful_24h
FROM bga_replay_requests
WHERE endpoint = '/archive/archive/logs.html'
  AND datetime(attempted_at) > datetime('now', '-24 hours')
GROUP BY account_label, request_class
ORDER BY account_label, request_class;
"
```

### 12.7. Cooldown акаунтів

```bash
sqlite3 -header -column data/auth.sqlite "
SELECT
  account_label, cooldown_until, last_limit_at,
  CASE
    WHEN datetime(cooldown_until) > datetime('now') THEN 'Cooldown'
    ELSE 'Available'
  END AS state,
  last_error
FROM bga_replay_account_state
ORDER BY account_label;
"
```

### 12.8. Archive retry setting

```bash
sqlite3 -header -column data/auth.sqlite "
SELECT setting_key, setting_value
FROM system_settings
WHERE setting_key = 'bga_replay_archive_retry_minutes';
"
```

Змінювати значення рекомендовано через `Admin → System Settings`. Worker
приймає лише ціле значення від 1 до 60 і використовує 2 при відсутньому або
некоректному значенні.

### 12.9. Ручне отримання конкретної гри

```bash
./.venv/bin/python get_game_replay.py 'GAME_ID' \
  --db-path data/auth.sqlite
```

Примусово оновити ready replay:

```bash
./.venv/bin/python get_game_replay.py 'GAME_ID' \
  --db-path data/auth.sqlite \
  --force
```

Команда використовує `request_class = manual`, спільний budget і максимум
один `logs.html`. Вона не каскадує запит через інші акаунти.

### 12.10. Ручне отримання replay для дуелі або матчу

```bash
./.venv/bin/python get_duel_game_replays.py 'DUEL_ID' \
  --db-path data/auth.sqlite \
  --include-pending \
  --include-errors \
  --include-fallback \
  --max-requests 3
```

```bash
./.venv/bin/python get_match_game_replays.py 'MATCH_ID' \
  --db-path data/auth.sqlite \
  --include-pending \
  --include-errors \
  --include-fallback \
  --max-requests 3
```

Без відповідних `--include-*` pending, error і fallback пропускаються.
`--force` вибирає всі active games, включно з ready/BGA. Batch-команди
зупиняються після `--max-failed-games` помилок, за замовчуванням після першої.

### 12.11. Додати конкретну гру в historical queue

```bash
GAME_ID='GAME_ID' ./.venv/bin/python - <<'PY'
import json
import os
from update_matches.replay_worker import enqueue_historical_game_replay

result = enqueue_historical_game_replay(
    'data/auth.sqlite',
    os.environ['GAME_ID'],
    historical_batch_id='manual-historical',
)
print(json.dumps(result, ensure_ascii=False, indent=2))
PY
```

`force=True` скидає queue counters та `archive_requested_at`, тому перед ним
потрібні backup, перевірка конкретного запису й усвідомлене рішення.

### 12.12. One-off cleanup/backfill legacy replay

Dry-run:

```bash
./.venv/bin/python backfill_existing_game_replays.py \
  --db-path data/auth.sqlite
```

Після production backup:

```bash
./.venv/bin/python backfill_existing_game_replays.py \
  --db-path data/auth.sqlite \
  --apply \
  --batch-id legacy-ready-backfill
```

`--apply` видаляє всі replay зі `status = error`; це одноразова destructive
maintenance-операція, а не регулярний worker.

### 12.13. Встановити, увімкнути та перевірити timers

```bash
sudo cp systemd/bga-replay-worker@.service /etc/systemd/system/
sudo cp systemd/bga-replay-fresh.timer /etc/systemd/system/
sudo cp systemd/bga-replay-archive-follow-up.timer /etc/systemd/system/
sudo cp systemd/bga-replay-historical.timer /etc/systemd/system/
sudo cp systemd/bga-replay-worker.logrotate /etc/logrotate.d/bga-replay-worker
sudo systemctl daemon-reload
```

```bash
sudo systemctl enable --now \
  bga-replay-fresh.timer \
  bga-replay-archive-follow-up.timer \
  bga-replay-historical.timer
```

```bash
sudo systemctl list-timers --all --no-pager \
  bga-replay-fresh.timer \
  bga-replay-archive-follow-up.timer \
  bga-replay-historical.timer
```

```bash
for timer in \
  bga-replay-fresh.timer \
  bga-replay-archive-follow-up.timer \
  bga-replay-historical.timer
do
  echo "===== $timer ====="
  sudo systemctl is-enabled "$timer"
  sudo systemctl is-active "$timer"
done
```

Нормальний стан timer: `enabled` і `active`. Oneshot service між запусками
може бути `inactive (dead)` — це нормально, якщо останній результат
`status=0/SUCCESS`.

### 12.14. Логи й lock

```bash
sudo journalctl \
  -u 'bga-replay-worker@fresh.service' \
  -u 'bga-replay-worker@archive-follow-up.service' \
  -u 'bga-replay-worker@historical.service' \
  --since today \
  --no-pager
```

```bash
sudo tail -n 200 /var/log/carcassonne/bga-replay-worker.log
```

```bash
sudo lsof data/auth.sqlite.bga-replay-worker.lock
sudo fuser -v data/auth.sqlite.bga-replay-worker.lock
ps aux | grep '[r]etry_pending_game_replays.py'
```

### 12.15. Автоматичні тести

```bash
python3 -m unittest discover -s update_matches -t . -p 'test_*.py'
npm test
```

Тести використовують тимчасові SQLite-бази й mocked/injected BGA-відповіді;
вони не витрачають production replay quota.

## 13. Інтерпретація JSON summary

| Поле | Значення |
|---|---|
| `status` | `ok`, `stopped`, `locked` або `error` |
| `queue_class` | Запущений mode |
| `due` | Due на початку запуску |
| `processed` | Кількість захоплених записів |
| `requests` | Фактичні `logs.html` |
| `ready_bga_colors` | Успішні replay з BGA-кольорами |
| `ready_fallback_colors` | Успішні replay з fallback-кольорами |
| `archives_requested` | Надіслані `requestTableArchive` |
| `deferred` | Записи, відкладені на наступну спробу |
| `manual_required` | Записи, автоматичні спроби яких завершено |
| `stopped_by_budget` | Немає доступного кандидата для reservation |
| `stopped_by_bga_limit` | BGA повернула явний replay-limit |
| `remaining` | Due після завершення запуску |
| `errors` | Діагностика оброблених проблемних записів |
| `stop_reason` | Причина дострокової зупинки |

`errors` не завжди означає фінальний `status = error`. Наприклад,
`archive_missing` після успішного `requestTableArchive` є очікуваною подією:
запис отримує `deferred`, а наступну перевірку виконує timer.

## 14. Public API

Public replay доступний, коли:

```sql
game_replays.status = 'ready'
```

Тому готовий fallback replay залишається видимим під час очікування кольорів,
архіву або тимчасового retry. `color_source` зберігає provenance і не дає
сприймати fallback `red`/`green` як підтверджені BGA-кольори.
