# Очищення старих колонок

Перехід на `type/category` уже виконано. Повторно запускати стару міграцію не потрібно.

Новий backend готовий до видалення `tournament_type`, `team_type`, `subtype`, `access_type` з `tournaments`.
**Спочатку залий новий backend і перезапусти його. HTML оновлювати не потрібно.**

На сервері — перевірка без змін БД:

```bash
cd /home/carcassonne-gg/auth-server
umask 077
mkdir -p backups
node src/cleanup-tournament-classification.js --db-path data/auth.sqlite \
  --report backups/classification-cleanup.json > backups/classification-cleanup.log &&
node -e 'const p=require("./backups/classification-cleanup.json"); console.log(p.summary, p.issues)'
```

Якщо `blocked: 0`, `issues: []`, `columns_to_remove: 4` — застосувати:

```bash
node src/cleanup-tournament-classification.js --db-path data/auth.sqlite \
  --apply --plan backups/classification-cleanup.json --backup-dir backups
```

Бекап створюється й перевіряється автоматично. Дані та зв'язки перевіряються в транзакції;
помилка або зміна БД після перевірки скасовує видалення. При `STALE_PLAN` повтори перевірку.
Backend і таймери можна залишити запущеними. `already_applied: true` означає, що колонки вже видалені.

Після видалення старий backend і старий `--rollback` несумісні зі схемою.
Для аварійного відновлення потрібен новий бекап, шлях до якого виводить команда.

[Попередня міграція та перевірки](./tournament-classification.md#11-результат-етапу-6).
