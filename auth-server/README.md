# Google OAuth auth-server for carcassonne.gg

Мінімальний сервер авторизації через Google з сесіями та SQLite.

## 1) Що робить

- `GET /auth/google` - починає Google OAuth
- `GET /auth/google/callback` - завершує OAuth, створює сесію
- `GET /auth/me` - повертає поточного юзера
- `POST /auth/logout` - завершує сесію
- `GET /health` - healthcheck

## 2) Налаштування Google Cloud

1. Відкрийте Google Cloud Console: `APIs & Services`.
2. Створіть (або виберіть) project.
3. Налаштуйте `OAuth consent screen`:
   - User type: `External` (або `Internal`, якщо Google Workspace).
   - Додайте app name, support email, developer email.
   - Додайте scope: `email`, `profile`, `openid`.
   - Для тесту додайте ваші Google-акаунти в `Test users`.
4. Створіть OAuth credentials:
   - `Credentials` -> `Create Credentials` -> `OAuth client ID`.
   - Application type: `Web application`.
   - `Authorized JavaScript origins`:
     - `https://carcassonne.gg`
     - (для локалки) `http://localhost:8080`
   - `Authorized redirect URIs`:
     - `https://carcassonne.gg/auth/google/callback`
     - (для локалки) `http://localhost:3100/auth/google/callback`
5. Збережіть `Client ID` і `Client Secret`.

## 3) Локальний запуск

```bash
cd auth-server
cp .env.example .env
# заповніть .env реальними значеннями
npm install
npm start
```

## 4) ENV

- `PORT` - порт сервера, напр. `3100`
- `FRONTEND_ORIGIN` - origin фронтенду, напр. `https://carcassonne.gg`
- `SESSION_SECRET` - довгий випадковий секрет (мінімум 32 символи)
- `GOOGLE_CLIENT_ID` - з Google Cloud
- `GOOGLE_CLIENT_SECRET` - з Google Cloud
- `GOOGLE_CALLBACK_URL` - напр. `https://carcassonne.gg/auth/google/callback`
- `DB_PATH` - шлях до SQLite, напр. `./data/auth.sqlite`
- `NODE_ENV=production` - для secure cookie у проді

## 5) Реверс-проксі (nginx приклад)

Ідея: фронтенд і auth-server мають бути на одному домені (`carcassonne.gg`), тоді cookie працюють стабільніше.

```nginx
location /auth/ {
  proxy_pass http://127.0.0.1:3100;
  proxy_set_header Host $host;
  proxy_set_header X-Forwarded-Proto $scheme;
  proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
}

location /health {
  proxy_pass http://127.0.0.1:3100;
  proxy_set_header Host $host;
}
```

## 6) Підключення у фронтенді

`gg-html/login-popup.html` вже налаштований на:
- popup логін через `/auth/google`
- перевірку сесії через `/auth/me`
- вихід через `/auth/logout`

Якщо auth-server не на тому ж origin, задайте глобальну змінну перед цим скриптом:

```html
<script>window.AUTH_BASE_URL = "https://api.carcassonne.gg";</script>
```

## 7) Що ще потрібно від вас

1. Дати домен/піддомен для auth (рекомендовано той самий: `carcassonne.gg`).
2. Додати OAuth credentials у Google Cloud (див. крок 2).
3. Заповнити `.env` на сервері.
4. Запустити `auth-server` як systemd/pm2 сервіс.
5. Налаштувати HTTPS (обов'язково для прод cookie `secure`).

## 8) Регулярний sync `matches` + `duels` у Google Sheets

У проєкті є скрипт:

```bash
npm run sync:google-sheets
```

Він:
- читає `matches` та `duels` з SQLite (`DB_PATH`);
- бере лише записи, де `deleted_at IS NULL` (як і API);
- перезаписує 2 листи в Google Spreadsheet.

### ENV для sync

Додайте в `.env`:

- `GOOGLE_SHEETS_SPREADSHEET_ID` - ID таблиці Google Sheets
- `GOOGLE_SHEETS_MATCHES_SHEET` - назва листа для матчів (за замовчуванням `matches`)
- `GOOGLE_SHEETS_DUELS_SHEET` - назва листа для дуелей (за замовчуванням `duels`)
- `GOOGLE_SERVICE_ACCOUNT_EMAIL` - email service account
- `GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY` - private key service account (з `\n`)

### Права доступу в Google Sheets

1. Створіть Service Account у Google Cloud.
2. Увімкніть Google Sheets API.
3. Поділіться вашим Google Sheet з `GOOGLE_SERVICE_ACCOUNT_EMAIL` (Editor).

### Запуск по розкладу (cron)

Приклад: кожні 10 хвилин.

```cron
*/10 * * * * cd /path/to/carcassonne-gg/auth-server && /usr/bin/env npm run sync:google-sheets >> /var/log/carcassonne-sync.log 2>&1
```

Перевірити вручну:

```bash
cd auth-server
npm install
npm run sync:google-sheets
```

## 9) Оновлення Elo гравців з BGA

При старті `auth-server` таблиця `profiles` тепер автоматично отримує поля:

- `bga_elo INTEGER`
- `bga_elo_updated_at TEXT`

Python-скрипт для ручного оновлення одного гравця:

```bash
cd auth-server
python3 -m pip install requests python-dotenv
python3 run_update_player_elo.py --player-id 85016225
```

Альтернатива через shell-обгортку:

```bash
cd auth-server
./scripts/run_update_player_elo.sh --player-id 85016225
```

Як це працює:

- бере `profiles.id` як numeric BGA player id;
- читає сторінку `https://boardgamearena.com/playerstat?id=<id>&game=1`;
- зберігає Elo в `profiles.bga_elo`;
- ставить час успішного оновлення в `profiles.bga_elo_updated_at`.

Для майбутнього регулярного запуску по всіх профілях скрипт уже вміє працювати порціями:

```bash
cd auth-server
python3 run_update_player_elo.py --batch-size 50 --limit 200
```

У такому режимі він бере профілі з найстарішим `bga_elo_updated_at` першими, тому підходить для cron/systemd batch-run без одночасного проходу по всій таблиці.

## 10) Регулярний фікс майбутніх матчів

Є окремий maintenance-скрипт:

```bash
cd auth-server
python3 run_fix_matches_and_duels.py
```

Або через shell-обгортку:

```bash
cd auth-server
./scripts/run_fix_matches_and_duels.sh
```

Що він робить зараз для `matches`:

- якщо `time_utc` у майбутньому;
- і при цьому `status <> 'Planned'` або будь-яке з полів `dw1`, `dw2`, `gw1`, `gw2` не порожнє;
- тоді ставить `status = 'Planned'` і очищає `dw1`, `dw2`, `gw1`, `gw2`.

Для `duels` логіка поки що не реалізована: скрипт повертає явний `skipped` у summary.

Безпечна перевірка без запису:

```bash
cd auth-server
python3 run_fix_matches_and_duels.py --dry-run
```

Приклад cron на запуск щогодини:

```cron
0 * * * * cd /path/to/carcassonne-gg/auth-server && /usr/bin/env python3 run_fix_matches_and_duels.py >> /var/log/carcassonne-fix-matches-and-duels.log 2>&1
```

### systemd-варіант на запуск раз на годину

У репозиторії є готові файли:

- `auth-server/systemd/fix-matches-and-duels.service`
- `auth-server/systemd/fix-matches-and-duels.timer`

Встановлення на сервері:

```bash
sudo cp auth-server/systemd/fix-matches-and-duels.service /etc/systemd/system/
sudo cp auth-server/systemd/fix-matches-and-duels.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now fix-matches-and-duels.timer
sudo systemctl status fix-matches-and-duels.timer
```
