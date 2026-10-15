# Міграція турнірів

БД: `/home/carcassonne-gg/auth-server/data/auth.sqlite`.
Виконуй блоки по черзі. При помилці зупинись.

## 1. Резервна копія

```bash
cd /home/carcassonne-gg/auth-server
umask 077
mkdir -p backups
sqlite3 -readonly data/auth.sqlite ".backup 'backups/auth-before-migration-$(date -u +%Y%m%dT%H%M%SZ).sqlite'"
```

Якщо новий backend ще не опублікований — зроби deploy **після цієї копії**
і дочекайся його успішного запуску. Далі потрібен код етапів 2–4.

## 2. Перевірка

Призупини фонові імпорти/таймери, які пишуть у БД, і дочекайся завершення їхніх завдань.

```bash
test -f src/migrate-tournament-classification.js
systemctl stop carcassonne-auth.service
node src/migrate-tournament-classification.js --db-path data/auth.sqlite \
  --report backups/tournament-plan.json > backups/tournament-dry-run.log
node -e '
const p = require("./backups/tournament-plan.json");
console.log({issues: p.issues, ...p.summary});
console.table(p.rows.map(r => ({id: r.id, type: r.after.type, category: r.after.category, errors: r.blockers.join(", ")})));
'
```

Продовжуй, якщо `issues: []`, `blocked: 0` і пари type/category правильні.
Якщо є `CATALOG_NOT_READY` — підготовка довідника новим backend ще не завершилась.

## 3. Міграція

```bash
node src/migrate-tournament-classification.js --db-path data/auth.sqlite \
  --apply --plan backups/tournament-plan.json --backup-dir backups
```

Очікується `applied: true` або `already_applied: true`.
Скрипт сам створить перевірений backup; матчі, результати й ролі збережуться.

## 4. Запуск

```bash
systemctl start carcassonne-auth.service
curl -fsS --retry 10 --retry-delay 1 --retry-connrefused http://127.0.0.1:3100/health
```

Віднови призупинені таймери. Перевір турніри, доступи, Friendly та Rivals.

При `STALE_PLAN` повтори крок 2. При іншій помилці запусти backend командою
`systemctl start carcassonne-auth.service` і збережи повідомлення помилки.
