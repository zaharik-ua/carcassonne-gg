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
