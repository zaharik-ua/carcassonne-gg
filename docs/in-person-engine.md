# In-Person pure tournament engine

Модуль `auth-server/src/in-person/engine.js` не звертається до БД, HTTP або UI. Він приймає plain objects, не змінює вхідні дані й повертає повний результат операції або `InPersonEngineError` із машинним `code`.

## Public API

- `validateMatchResult(match, input)` — валідує та нормалізує завершений результат;
- `calculateSwissStandings({ participants, rounds })` — повністю перебудовує `swiss_standard_v1`;
- `pairFirstSwissRound({ participants })` — формує перший раунд за реально виданими draw numbers;
- `pairNextSwissRound({ participants, standings, rounds })` — формує наступний раунд зі standings та історії;
- `compareSwissStandings(left, right)` — канонічний comparator standings;
- `compareStableParticipantIds(left, right)` — locale-independent фінальний fallback.

До engine передаються Swiss-раунди з вкладеним масивом `matches`. Standings враховує лише раунди зі `stage = swiss` і `status = completed`; скасовані та незавершені раунди не впливають на таблицю.

## Результати

Підтримуються режими:

- `points` — цілі невід'ємні `points_a` і `points_b`;
- `simple` — результат `1–0` із явно вибраним переможцем;
- `time_forfeit` — `1–0`, переможець і причина `time_forfeit`;
- `technical` — переможець і причина `withdrawal`, `disqualification`, `no_show` або `admin_decision`;
- `bye` — лише для системного bye-запису без другого учасника.

Для рівних Carcassonne points переможцем автоматично стає учасник, який ходив другим. Поля іншого result mode відхиляються, а winner, loser і starter обов'язково перевіряються відносно учасників матчу.

## Standings

Порядок `swiss_standard_v1`:

1. Wins;
2. Solkoff1;
3. Solkoff2;
4. VP difference;
5. Sonneborn–Berger;
6. учасник без bye вище учасника з bye;
7. незмінний `participant_id`.

Bye дає одну перемогу та додає `0` до списку результатів суперників. `Solkoff1` відкидає найменше значення, а `Solkoff2` — найменше й найбільше. VP difference рахується лише для `points`.

MVP формула Sonneborn–Berger відтворює ЧУ-2025: за кожну перемогу додається кількість перемог переможеного суперника перед відповідною партією. Golden test перевіряє всі 37 рядків фінального standings із `json-data/ua2025.json`.

## Pairing

Перший раунд:

1. беруться лише checked-in учасники;
2. кожен має унікальний додатний `draw_number`;
3. учасники сортуються за фактично виданими номерами без створення позицій для пропусків;
4. за непарної кількості найбільший виданий номер отримує bye;
5. решта ділиться навпіл, відповідні позиції утворюють пари;
6. у фіксованому правилі MVP першим ходить учасник з першої половини.

Для наступного раунду withdrawn/disqualified учасники лишаються у standings, але не входять до candidate pool. За непарної кількості bye отримує найнижчий у standings активний учасник без попереднього bye. Якщо bye вже мали всі кандидати, engine повертає `NO_BYE_ELIGIBLE_PARTICIPANT`, бо окрема repeat-bye policy не входить у MVP.

Пари обираються детерміновано: спочатку в межах однакової кількості перемог, потім із найменшим можливим float до сусідньої score group. Повне парування перевіряється maximum-matching fallback, тому rematch не використовується, якщо існує повне парування без нього.

Обмеження послаблюються в такому порядку:

1. допустити starter imbalance більше двох;
2. допустити третій однаковий first/second role поспіль;
3. лише після цього допустити rematch.

Фактичні порушення повертаються на матчі та у загальному масиві `warnings` із кодами `starter_imbalance`, `starter_3_in_a_row` і `rematch`. Випадковий seed, version алгоритму та snapshot input у MVP відсутні.
