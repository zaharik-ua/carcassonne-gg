# Auth Server Starter For Another Project

Це компактний starter, зібраний з підходу `auth-server` у поточному проєкті, але без Carcassonne-специфіки: матчів, турнірів, BGA, новин, systemd-джобів і production-скриптів.

## Що всередині

- `package.json` - мінімальні Node-залежності для Express, Passport Google OAuth, cookie sessions і SQLite.
- `.env.example` - потрібні env-змінні для Google OAuth, сесій, CORS і SQLite.
- `src/server.js` - Express app, CORS, `express-session`, SQLite session store, healthcheck і підключення routes.
- `src/config.js` - читання env, frontend origins, cookie settings, абсолютний шлях до SQLite.
- `src/db.js` - SQLite helper-и й базова схема `users` та `profiles`.
- `src/auth.js` - Google OAuth strategy, upsert юзера, створення дефолтного профілю після першого логіну.
- `src/routes/auth.js` - `/auth/google`, `/auth/google/callback`, `/auth/me`, `/auth/logout`.
- `src/routes/profile.js` - `/profile/me` для читання/редагування власного профілю та `/profile/:id` для public profile.
- `src/middleware/auth.js` - `requireAuthenticated` і `requireAdmin`.
- `src/project-schema.js` - місце для власних таблиць іншого проєкту.

## Локальний старт

```bash
cd migrate-auth-server-to-another-project
cp .env.example .env
npm install
npm start
```

У Google Cloud OAuth credential потрібно додати callback:

```text
http://localhost:3100/auth/google/callback
```

## API

- `GET /health` - healthcheck.
- `GET /auth/google` - почати Google login.
- `GET /auth/google/callback` - callback від Google.
- `GET /auth/me` - поточний авторизований юзер і його profile.
- `POST /auth/logout` - logout.
- `GET /profile/me` - власний profile.
- `PATCH /profile/me` - оновити `display_name`, `avatar_url`, `bio`, `location`, `website_url`, `metadata`.
- `GET /profile/:id` - public profile.

## Як додавати таблиці нового проєкту

Додавайте `CREATE TABLE IF NOT EXISTS ...` у `src/project-schema.js`. Якщо таблиці мають належати конкретному юзеру, використовуйте `owner_user_id INTEGER NOT NULL` з foreign key на `users(id)`.

Приклад ownership-перевірки в route:

```js
const row = await dbGet("SELECT owner_user_id FROM projects WHERE id = ?", [projectId]);
if (!row || Number(row.owner_user_id) !== Number(req.user.id)) {
  return res.status(403).json({ ok: false, message: "Forbidden" });
}
```

## Важливі відмінності від поточного `auth-server`

- У цьому starter немає BGA player id. `profiles.id` є внутрішнім autoincrement id, а звʼязок з Google-юзером іде через `profiles.user_id`.
- Профіль створюється автоматично при першому Google login.
- `avatar_url` зберігає URL або локальний абсолютний path, але upload-файлів тут не реалізований. Якщо у новому проєкті потрібен upload картинок, краще додати його окремо під конкретні правила зберігання.
- Admin-и задаються через `ADMIN_EMAILS` у `.env`; якщо email є у списку, юзер стає admin після логіну.
