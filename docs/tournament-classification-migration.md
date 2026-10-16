# Перехід на type/category завершено

**16.10.2026 робочу БД успішно очищено. Повторно запускати міграції не потрібно.**

- З `tournaments` видалено `tournament_type`, `team_type`, `subtype`, `access_type`.
- Усі 10 турнірів збережені; класифікація — `type/category`.
- Скрипт перевірив збереження даних, схеми, foreign keys та цілісність БД.
- Після очищення перевірено 6 публічних API, включно з `/health`, турнірами та Friendly: HTTP 200, `ok: true`.

БД: `/home/carcassonne-gg/auth-server/data/auth.sqlite`.
Бекап перед очищенням у `/home/carcassonne-gg/auth-server/backups/`:
`tournament-classification-cleanup-v1-2026-10-16T08-53-54.897Z-5ce0a0bc-9430-4693-aaa0-eaff93247eb2.sqlite`.

Старий backend і старий `--rollback` несумісні з очищеною схемою.
[Журнал виконання](./tournament-classification.md#12-очищення-старих-колонок).
