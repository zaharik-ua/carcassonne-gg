# In-Person Swiss workflow

Етап 5 підключає pure engine до SQLite, organizer API та `Player Hub → In-Person`.
Окрема migration не потрібна: `in_person_rounds`, `in_person_matches` і
`in_person_standings` уже входять до foundation schema.

## API

Усі маршрути вимагають global admin або користувача з роллю `admin` для конкретного
`in_person_tournament`.

- `GET /in-person-tournaments/:id/swiss` повертає rounds, current round, progress,
  останню актуальну standings revision та дозволені наступні дії.
- `POST /in-person-tournaments/:id/swiss/rounds/preview` приймає optional
  `round_number`. Preview нічого не записує у БД.
- `POST /in-person-tournaments/:id/swiss/rounds/confirm` приймає той самий
  `round_number` і атомарно створює draft round разом з усіма matches.
- `POST /in-person-tournaments/:id/swiss/rounds/:roundId/publish` переводить draft
  у `published`.
- `PUT /in-person-tournaments/:id/swiss/matches/:matchId/result` приймає повний
  поточний стан результату: `starting_participant_id`, `result_type`, відповідні
  points або winner, optional `finish_reason` та `admin_note`.
- `POST /in-person-tournaments/:id/swiss/rounds/:roundId/complete` перевіряє всі
  столи, завершує раунд і записує нову standings revision в одній транзакції.

## Invariants і retry

- Активні Swiss rounds мають послідовні номери без пропусків.
- Одночасно існує не більше одного незавершеного активного Swiss round.
- Наступний раунд не формується до `completed` попереднього та актуальної standings
  revision, створеної саме ним.
- Bye створюється як уже завершений system result і не редагується.
- Повторний `confirm` з тим самим `round_number` повертає наявний round.
- Повторні `publish` і `complete` повертають поточний стан без нової мутації.
- Повторне збереження ідентичного повного result не збільшує revision.
- Після невизначеної мережевої відповіді клієнт може повторити command або перечитати
  `GET .../swiss`; окрема таблиця idempotency keys не потрібна.

## Atomicity

`confirm` охоплює insert round, усі match inserts і перехід tournament у `swiss`
однією `BEGIN IMMEDIATE` transaction. `complete` так само об'єднує зміну status
раунду, повний standings rebuild, insert нової revision і tournament revision.
Fault-injection integration tests переривають обидва сценарії всередині transaction
і перевіряють нуль partial writes.

## Player Hub

Вкладка `Swiss` показує поточний раунд, кількість завершених раундів і столів,
pairing preview з warning reasons, publish, форми result/starter та complete action.
Вкладка `Standings` читає останню актуальну revision. Видимі поля ранжування:
`Wins`, `Solkoff1`, `Solkoff2`, `VP difference`; внутрішні Sonneborn–Berger, bye flag
і stable ID не показуються.
