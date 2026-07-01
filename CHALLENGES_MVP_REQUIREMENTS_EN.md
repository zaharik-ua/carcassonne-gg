# Challenges MVP - requirements and implementation tracker

Document status: `approved for implementation`

The purpose of the document is to capture the MVP scope of the Challenges mode and use the checkboxes below to track implementation.

## Tracker rules

- `[ ]` - requirement not yet implemented or verified.
- `[x]` - requirement implemented, tested and accepted.
- The requirement is considered fulfilled only after the implementation of backend rules, the corresponding UI and the verification of critical scenarios, if applicable.
- Changes to the scope are first made in this document, and then implemented in the code.
- Requirement IDs (`CH-PER-*`, `CH-REQ-*`, etc.) do not change after implementation.

## 1. MVP goal

Challenges is a mode in which the player during a certain game period:

1. Chooses their participation status.
2. Finds an opponent from another association.
3. Creates or receives a match request.
4. Coordinates time and Bo3/Bo5 format.
5. Plays no more than one match per period.
6. Reviews and, if necessary, adjusts the result of the match.

## 2. Established product decisions

- Calculating or updating the rating is not part of this MVP.
- The logic for automatically finding and importing BGA results is not included in this MVP. Challenges only uses the result obtained by an existing or separately implemented importer.
- The result is stored in the existing `duels` and `games` entities. A separate Result entity is not created.
- Score adjustment works on trust: one player's change is applied immediately, without confirmation by the other player.
- A challenge match is the product name of a singles series between two players. At the data level, it is stored as one `duel` and its associated `games`, without creating a team entry in `matches`.
- For Challenge duels, the existing `duels` statuses are used, expanded only if necessary.
- One player can play no more than one Challenge match in one period.
- All dates and times are stored in UTC.

## 3. Terminology and statuses

### 3.1. Period statuses

|Status|Meaning|
|---|---|
| `draft` |The administrator prepares the period. Players cannot see it.|
| `planning_open` |Players can choose their participation status and create requests.|
| `active` |You can create requests and play matches.|
| `result_review` |New requests are not available. Players can view and adjust scores.|
| `archived` |The period is completely completed.|
| `cancelled` |The period has been canceled by the administrator.|

### 3.2. Player statuses during the period

The status belongs to the specific pair `player_id + period_id`.

|Status|Display|Meaning|
|---|---|---|
| `not_selected` |Not selected|The player has not yet made a selection or their previous match has been cancelled.|
| `available` |Open to a match|The player is open to offers.|
| `unavailable` |Closed for match|The player does not participate in this period.|
| `match_scheduled` |The match is scheduled|The player has a confirmed match.|
| `played` |The match is played|The match ended with a result.|

### 3.3. Request statuses

|Status|Meaning|
|---|---|
| `pending` |Pending player action with `awaiting_player_id`.|
| `accepted` |The request has been accepted and is linked to a confirmed match.|
| `declined` |The request was rejected by the player from whom a response was expected.|
| `cancelled_by_sender` |The request has been withdrawn by its original author.|
| `auto_cancelled` |Request automatically closed due to another confirmed match or change in availability.|
| `expired` |The last time proposed in the request has passed.|

### 3.4. Challenge match statuses (`duel`)

Available statuses `duels` are marked separately from statuses to be added for Challenges:

|Status|Availability|Meaning|
|---|---|---|
| `Draft` |New|The match has been prepared by an administrator, but is not yet open or confirmed.|
| `Requested new time` |New|The match is awaiting the approval of a new time after the postponement request.|
| `Planned` |Available|The match is confirmed by both players.|
| `In progress` |New|The time of the match has arrived, but the final result has not yet been received.|
| `Done` |Available|The match is played; in the Challenges UI it shows as `Played`.|
| `Error` |Available|The match ended, but the correct result was not obtained.|
| `Cancelled` |New|Match canceled by player or administrator.|

## 4. Challenge periods

### 4.1. Period data

- [X] **CH-PER-001** Admin can create Challenges period.
- [x] **CH-PER-002** The period contains a name and, if necessary, a short description.
- [X] **CH-PER-003** Period contains `planning_starts_at`.
- [x] **CH-PER-004** Period contains `play_starts_at`.
- [X] **CH-PER-005** Period contains `play_ends_at`.
- [X] **CH-PER-006** Period contains `result_review_ends_at`.
- [X] **CH-PER-007** All period dates are stored in UTC.
- [ ] **CH-PER-008** Backend checks date order: `planning_starts_at <= play_starts_at < play_ends_at <= result_review_ends_at`.
- [x] **CH-PER-009** Admin can edit period data and dates.

### 4.2. Life cycle period

- [ ] **CH-PER-010** New period is created in status `draft`.
- [ ] **CH-PER-011** The period in `draft` is displayed to players without the ability to change participation status or create requests.
- [ ] **CH-PER-012** In `planning_open`, players can change participation status and create requests.
- [ ] **CH-PER-013** In `active`, players can change participation status, create requests and play matches.
- [ ] **CH-PER-014** `result_review` cannot create or accept new requests.
- [ ] **CH-PER-015** `result_review` can view and adjust match results.
- [ ] **CH-PER-016** In `archived`, all player actions are blocked.
- [ ] **CH-PER-017** Admin can transfer period to `cancelled` only after canceling all active bids and pending matches.
- [ ] **CH-PER-018** The Challenges page displays all periods with statuses `draft`, `planning_open`, `active`, and `result_review`.
- [ ] **CH-PER-019** The simultaneous existence of the current `active` period and the next `planning_open` period is supported by the UI and API.

## 5. Banner on the main page

- [ ] **CH-BAN-001** Challenges banner is displayed on the main page for an open planning period.
- [ ] **CH-BAN-002** The banner contains the name and main dates of the period.
- [ ] **CH-BAN-003** The banner contains a link to the Challenges page.
- [ ] **CH-BAN-004** Banner does not show `draft`, `archived` or `cancelled` periods.

## 6. Status of the player during the period

- [x] **CH-PLY-001** There is no more than one status record for each player and period.
- [x] **CH-PLY-002** Initial player status is `not_selected`.
- [x] **CH-PLY-003** Player can go from `not_selected` or `unavailable` to `available` during `planning_open` or `active`.
- [x] **CH-PLY-004** Player can go to `unavailable` if they don't have `Planned` match in this period.
- [x] **CH-PLY-005** Before going to `unavailable`, the UI shows a warning about automatic closing of requests.
- [x] **CH-PLY-006** When going to `unavailable`, all pending requests involving this player go to `auto_cancelled`.
- [x] **CH-PLY-008** When confirming a match, the status of both players becomes `match_scheduled`.
- [ ] **CH-PLY-009** After receiving a correct result, the status of both players becomes `played`.
- [ ] **CH-PLY-010** After a normal cancellation of a scheduled match, the status of both players becomes `not_selected`.
- [ ] **CH-PLY-011** When transferring a match to `Requested new time`, both players become `available`.
- [x] **CH-PLY-012** Player from `match_scheduled` cannot go to `unavailable` until canceling the match.

## 7. Available opponents and eligibility

- [ ] **CH-ELG-001** Challenges actions are only available to the authorized user associated with the player profile.
- [ ] **CH-ELG-002** Active and inactive profiles can be participants in Challenges.
- [ ] **CH-ELG-003** The "Open to Match" list contains players with the status `available` in the selected period.
- [ ] **CH-ELG-004** The current player does not appear as an available opponent for himself.
- [ ] **CH-ELG-005** The list of available opponents does not show players from the same association.
- [ ] **CH-ELG-006** The list of available opponents does not show players who already have a scheduled match in the period.
- [ ] **CH-ELG-007** It is possible to invite a player with `not_selected` via manual selection or the Players page, unless otherwise restricted.
- [ ] **CH-ELG-008** Unable to invite player with status `unavailable`.
- [ ] **CH-ELG-009** Cannot invite a player who already has a scheduled match in the period.
- [ ] **CH-ELG-010** Cannot create a second pending request between the same pair of players in the same period.
- [ ] **CH-ELG-011** After the terminal status of the previous request, the same pair of players can be requested again.

Terminal statuses for re-invitation:

- `declined`;
- `cancelled_by_sender`;
- `auto_cancelled`;
- `expired`.

## 8. Creation of an request

### 8.1. Entry points

- [ ] **CH-REQ-001** A challenge can be created via the Player Hub → Challenges clean form.
- [x] **CH-REQ-002** A request can be created from a list of available players with a pre-selected opponent.
- [ ] **CH-REQ-003** Request can be created from the Players page via the "Invite to match" button with a pre-selected opponent.

### 8.2. Request data

- [x] **CH-REQ-004** Request belongs to one `period_id`.
- [x] **CH-REQ-005** The request contains two different participants.
- [x] **CH-REQ-006** `created_by_player_id` indicates the originator of the request.
- [x] **CH-REQ-007** `awaiting_player_id` indicates the player who is currently expected to act.
- [x] **CH-REQ-008** Request contains one to three unique time options.
- [x] **CH-REQ-009** Each time variant is within `play_starts_at..play_ends_at` of the corresponding period.
- [x] **CH-REQ-010** Request offers Bo3, Bo5 or both formats.
- [x] **CH-REQ-011** New request is created in status `pending`.
- [x] **CH-REQ-012** The request stores creation and last update timestamps.

### 8.3. Request limit

- [x] **CH-REQ-013** One player can be the author of no more than three pending requests at the same time.
- [x] **CH-REQ-014** The limit is calculated for `created_by_player_id`.
- [x] **CH-REQ-015** Another player's counteroffer does not release the bid from its original submitter's limit.
- [x] **CH-REQ-016** The limit check is performed on the backend in the same transaction as the request creation.

### 8.4. Withdrawal and deletion

- [ ] **CH-REQ-017** The original author can withdraw his pending request.
- [ ] **CH-REQ-018** After revoking, request goes to `cancelled_by_sender`.
- [ ] **CH-REQ-019** After revocation, the author can hide/delete the request from his list without deleting the audit history.
- [ ] **CH-REQ-020** Another participant cannot revoke a request on behalf of its original author.

### 8.5. Expiration

- [ ] **CH-REQ-021** The request does not expire after passing a separate time option, if future options remain in it.
- [ ] **CH-REQ-022** Pending request goes to `expired` after last suggested time.
- [ ] **CH-REQ-023** Expiration is performed automatically and idempotently.

## 9. Review of requests

- [ ] **CH-LST-001** The player sees a list of pending requests, in which his response is expected.
- [ ] **CH-LST-002** The player sees a list of the requests they have created with their current statuses.
- [ ] **CH-LST-003** For a pending request, the proposed times, formats and player from whom a response is expected are shown.
- [ ] **CH-LST-004** For the terminal request, its final status is shown.
- [ ] **CH-LST-005** Player sees his confirmed match for each open period.

## 10. Answer and counteroffer

- [ ] **CH-RSP-001** Only a player with `awaiting_player_id` can accept or reject a pending request.
- [ ] **CH-RSP-002** If rejected, the request goes to `declined`.
- [ ] **CH-RSP-003** Player with `awaiting_player_id` can offer one to three other timing options.
- [ ] **CH-RSP-005** When counteroffered, `awaiting_player_id` changes to another participant.
- [ ] **CH-RSP-006** When counteroffered, the status remains `pending`.
- [ ] **CH-RSP-007** New counteroffer time options must also be within the game period.
- [ ] **CH-RSP-008** Counteroffer is recorded in audit log along with actor and time option changes.

## 11. Request acceptance

- [ ] **CH-ACC-001** When accepting, the player chooses one of the current suggested times.
- [ ] **CH-ACC-002** If both formats are offered, player chooses Bo3 or Bo5.
- [ ] **CH-ACC-003** If one format is offered, it is used automatically.
- [ ] **CH-ACC-004** Request acceptance is performed by one DB transaction.
- [ ] **CH-ACC-005** The transaction is rechecking that the request is still `pending`.
- [ ] **CH-ACC-006** Transaction checks that actor is `awaiting_player_id`.
- [ ] **CH-ACC-007** The transaction checks that the period allows the request to be accepted.
- [ ] **CH-ACC-008** The transaction checks that both players do not already have another confirmed match in this period.
- [ ] **CH-ACC-009** Request goes to `accepted`.
- [ ] **CH-ACC-010** A new `Planned` duel is being created or an associated duel in status `Requested new time` is being confirmed.
- [ ] **CH-ACC-011** Duel contains two players, agreed time and format.
- [ ] **CH-ACC-012** The status of both players becomes `match_scheduled`.
- [ ] **CH-ACC-013** All other pending bids involving either of these two players go to `auto_cancelled`.
- [ ] **CH-ACC-014** Duels associated with auto-cancelled requests in status `Requested new time` go to `Cancelled`.
- [ ] **CH-ACC-015** DB-level constraint does not allow one player to have more than one active Challenge-duel in one period.
- [ ] **CH-ACC-016** Repeating the same accept does not create a duplicate duel.

### 11.1. Creating a match "by the way"

This flow is used if players have already played a match, but did not create an request in advance, or created it, but did not have time to confirm it before the start of the match. Obtaining and importing actual results is done by existing separate functionality and is not part of this flow.

- [ ] **CH-RET-001** During the `active` period status, a player can create a request with a match time in the past if that time is within `play_starts_at..play_ends_at` the corresponding period.
- [ ] **CH-RET-002** A request with time in the past is clearly marked in the UI as a request for an already played match.
- [ ] **CH-RET-003** Another player can accept a request for a match that has already been played, if both players do not already have another confirmed Challenge match in that period.
- [ ] **CH-RET-004** If a request was created before a match but not accepted in time, a player with `awaiting_player_id` can confirm it after the suggested time as a match already played, even if the request has changed to `expired` status.
- [ ] **CH-RET-005** The creation and confirmation of a request "in fact" is available to `play_ends_at` and also during `result_review`; `archived` and `cancelled` cannot create or confirm such a match.
- [ ] **CH-RET-006** When "in fact" is confirmed, the request goes to `accepted` and the Challenge-duel is created by a single DB transaction with the agreed participants, format and actual match time in the past.
- [ ] **CH-RET-007** Before confirmation, the backend rechecks the period status, actual match time limits, actor, eligibility of both players and the limit of "no more than one Challenge match per period".
- [ ] **CH-RET-008** After creating a Challenge-duel, the existing mechanism for obtaining actual results is launched or applied; a separate logic for searching or importing results is not implemented within this flow.
- [ ] **CH-RET-009** If the existing mechanism immediately finds a correct result, the duel goes to `Done` and both players go to `played`; if the result has not yet been received, the further status is determined by the general rules of Challenge-duel and receiving results.
- [ ] **CH-RET-010** Confirmation "in fact" automatically closes other pending requests of both players according to the general request acceptance rules.
- [ ] **CH-RET-011** Reconfirming the same request "in fact" does not create a duplicate duel and does not trigger duplicate retrieval of results.

## 12. Challenge match as a duel

- [ ] **CH-MAT-001** Challenge match uses an existing `duels` entity and does not create an entry in the `matches` team entity.
- [ ] **CH-MAT-002** Duel is associated with `challenge_period_id`.
- [ ] **CH-MAT-003** Duel is associated with `challenge_request_id`.
- [ ] **CH-MAT-004** Duel has the source token `source_type = challenge` or an equivalent single token.
- [ ] **CH-MAT-005** Challenge-duel does not require parent team `match`; its `match_id` remains empty.
- [ ] **CH-MAT-006** Bo3 uses the appropriate existing duel format.
- [ ] **CH-MAT-007** Bo5 uses the appropriate existing duel format.
- [ ] **CH-MAT-008** Members are stored directly in `duels.player_1_id` and `duels.player_2_id`.
- [ ] **CH-MAT-009** Challenge-match games are stored in `games` with a link to Challenge-duel via `games.duel_id`.
- [ ] **CH-MAT-010** On the Challenges page, a confirmed match shows opponent, association, UTC/local time, format and status.

## 13. Postponement of the match

- [ ] **CH-RSC-001** Any participant can initiate a time change either before or after the scheduled start time of the match.
- [ ] **CH-RSC-003** When porting, the associated request is returned at `pending`.
- [ ] **CH-RSC-005** `awaiting_player_id` becomes another member of the match.
- [ ] **CH-RSC-006** Match goes to `Requested new time`.
- [ ] **CH-RSC-007** Pre-confirmed time not saved as active match slot.
- [ ] **CH-RSC-008** Both players go to `available` and can receive or accept other bids.
- [ ] **CH-RSC-009** When accepting a new time, the request goes to `accepted` and the match goes to `Planned`.
- [ ] **CH-RSC-010** After re-accepting, both players go to `match_scheduled`.
- [ ] **CH-RSC-011** When the new time is rejected, the request goes to `declined`.
- [ ] **CH-RSC-012** When rejecting a new time, the linked match in status `Requested new time` changes to `Cancelled`.
- [ ] **CH-RSC-013** Once rejected, both players remain `available`.
- [ ] **CH-RSC-014** If one of the participants accepted another request, the transfer request goes to `auto_cancelled` and its match in status `Requested new time` goes to `Cancelled`.

## 14. Cancellation of the match

- [ ] **CH-CAN-001** Any participant may cancel a `Planned` match either before or after the scheduled start time if the match has not been played and the result has not been recorded.
- [ ] **CH-CAN-002** After the scheduled start time, cancellation of an unplayed `Planned` match remains available via player flow.
- [ ] **CH-CAN-003** When cancelled, the match goes to `Cancelled` or equivalently soft-deleted with a recorded cancellation reason.
- [ ] **CH-CAN-004** When canceled, the status of both players becomes `not_selected`.
- [ ] **CH-CAN-005** After canceling, players can re-enter `available`.
- [ ] **CH-CAN-006** Another member receives a cancellation notification.
- [ ] **CH-CAN-007** Admin can cancel a match regardless of start time.

## 15. Match results

- [ ] **CH-RES-001** An automatically obtained correct result is stored in the existing `duels` and `games` without creating or updating the team `match`.
- [ ] **CH-RES-002** Challenge-duel goes to `Done` after correct completion.
- [ ] **CH-RES-003** In the Challenges UI, the status of `Done` is displayed as `Played`.
- [ ] **CH-RES-004** After correct completion, the status of both players becomes `played`.
- [ ] **CH-RES-005** If a correct result is not obtained after the end of the match, Challenge-duel goes to `Error`.
- [ ] **CH-RES-006** Before `result_review_ends_at`, any participant in a match could adjust the result without confirmation from another participant.
- [ ] **CH-RES-007** Player can change actual match time.
- [ ] **CH-RES-008** Player may delete mistakenly added games.
- [ ] **CH-RES-009** Player can add BGA table ID manually.
- [ ] **CH-RES-010** Player can reject timeout/no-show.
- [ ] **CH-RES-011** After adjustment, Challenge-duel score and status are recalculated based on existing duel/game logic.
- [ ] **CH-RES-012** Another participant receives a result change notification.
- [ ] **CH-RES-013** Result changes are displayed to both participants on the site.
- [ ] **CH-RES-014** Each manual change of the result is recorded in the audit log.
- [ ] **CH-RES-015** After `result_review_ends_at`, the result can only be changed by an administrator.

## 16. Notifications

- [ ] **CH-NTF-001** The player receives a notification about a new request.
- [ ] **CH-NTF-002** Player receives counteroffer notification.
- [ ] **CH-NTF-003** The original author receives an email about the acceptance of the request.
- [ ] **CH-NTF-004** The original author receives an email about the rejection of the request.
- [ ] **CH-NTF-005** Player receives a notification if their request was automatically closed.
- [ ] **CH-NTF-007** Player receives notification of proposed transfer.
- [ ] **CH-NTF-008** Player receives match cancellation notification.
- [ ] **CH-NTF-009** The player receives a notification about the manual change of the result by another player.
- [ ] **CH-NTF-010** Repeating the same operation does not generate duplicate notifications.

## 17. Administration

- [ ] **CH-ADM-001** Admin can see the list of Challenges periods.
- [ ] **CH-ADM-002** Administrator can create and edit periods.
- [ ] **CH-ADM-003** Admin can change period status according to allowed transitions.
- [ ] **CH-ADM-004** The administrator sees all requests of the period, including terminal statuses.
- [ ] **CH-ADM-005** Admin can see the status of each player in the selected period.
- [ ] **CH-ADM-006** Admin can manually create a Challenge match.
- [ ] **CH-ADM-007** Admin can edit Challenge match.
- [ ] **CH-ADM-008** Admin can transfer Challenge match without player approval flow.
- [ ] **CH-ADM-009** Admin can cancel Challenge match.
- [ ] **CH-ADM-010** Admin can adjust result after `result_review_ends_at`.

## 18. Audit log

- [ ] **CH-AUD-001** The creation of an request is recorded in the audit log.
- [ ] **CH-AUD-002** The counter proposal is recorded in the audit log.
- [ ] **CH-AUD-003** Request acceptance is recorded in the audit log.
- [ ] **CH-AUD-004** Request rejection is recorded in the audit log.
- [ ] **CH-AUD-005** Request withdrawal is recorded in the audit log.
- [ ] **CH-AUD-006** Automatic closing of the request is recorded in the audit log.
- [ ] **CH-AUD-007** Request expiration is recorded in the audit log.
- [ ] **CH-AUD-008** Actor, action, request ID, previous state, next state and timestamp are stored for each event.
- [ ] **CH-AUD-009** The audit log history is not deleted when the player hides the request.

## 19. Competitiveness and data integrity

- [ ] **CH-CON-001** All multi-entity status transitions are performed transactionally.
- [ ] **CH-CON-002** Two simultaneous accepts for one player cannot create two confirmed Challenge-duels.
- [ ] **CH-CON-003** Accept and player transition to `unavailable` executed at the same time end with one valid state with no partially updated data.
- [ ] **CH-CON-004** Accept of an request that has already become a terminal returns a conflict and does not change the data.
- [ ] **CH-CON-005** Resubmitting the same API request does not create duplicate duels, games, audit events, or notifications.
- [ ] **CH-CON-006** Soft-deleted/Cancelled match does not block new match creation for players in the same period.
- [ ] **CH-CON-007** A match in status `Requested new time` is not considered scheduled and does not block other bids.

## 20. Critical acceptance scenarios

- [ ] **CH-E2E-001** Player becomes available, creates bid, opponent accepts, match is created, both get `match_scheduled`.
- [ ] **CH-E2E-002** A player with three pending requests cannot create a fourth.
- [ ] **CH-E2E-003** The counteroffer modifies `awaiting_player_id` and does not affect the original author's limit.
- [ ] **CH-E2E-004** After decline or cancel, the same pair of players can be invited again with a different time.
- [ ] **CH-E2E-005** Accepting one request automatically closes all other pending requests of both players.
- [ ] **CH-E2E-006** Two simultaneous accepts for the same player end up creating only one confirmed match.
- [ ] **CH-E2E-007** Switching to unavailable closes the corresponding requests and is not allowed for a scheduled match.
- [ ] **CH-E2E-008** Rescheduling moves the match to `Requested new time`, the request to `pending`, and the players to `available`.
- [ ] **CH-E2E-009** Reject transfer moves match from `Requested new time` to `Cancelled` and leaves both players `available`.
- [x] **CH-E2E-010** While the match time is being renegotiated and the match is in `Requested new time`, a player can confirm a match with another player; after that, the transfer request automatically goes to `auto_cancelled` and the previous match goes to `Cancelled`.
- [ ] **CH-E2E-011** An unplayed `Planned` match without a fixed result can be canceled via player flow both before and after the scheduled start time.
- [ ] **CH-E2E-012** Manually changing the score is immediately visible to both players and generates a notification and audit event.
- [ ] **CH-E2E-013** After `result_review_ends_at`, the player cannot change the result, but the admin can.
- [ ] **CH-E2E-014** Proposed time outside the game period is rejected by the backend.
- [ ] **CH-E2E-015** Request expires only after passing the last offered time option.
- [ ] **CH-E2E-016** During `active`, a player creates a request with the time of a match already played within the period, the opponent accepts it, a duel is created and results are obtained through the available functionality.
- [ ] **CH-E2E-017** A request that is not accepted by the suggested time can be confirmed "in fact" to `play_ends_at`; at the same time, only one duel is created, and the result is obtained through the existing functionality.

## 21. Data scheme of new DB objects

The schema below is targeted for SQLite. All new entity identifiers are of type `TEXT` , all timestamps are stored as UTC ISO 8601 in fields of type `TEXT` , and boolean values ​​are stored as `INTEGER` with a value of `0` or `1` .

### 21.1. `challenge_periods`

One entry describes one period of Challenges.

|Field|Type| Null | Default |Purpose|
|---|---|---:|---|---|
| `id` | `TEXT` |no| — | Primary key. |
| `name` | `TEXT` |no| — |Name of the period.|
| `description` | `TEXT` |yes| `NULL` |Brief description.|
| `logo` | `TEXT` |yes| `NULL` |Link to a picture of the period logo.|
| `status` | `TEXT` |no| `'draft'` |`draft`, `planning_open`, `active`, `result_review`, `archived` or `cancelled`.|
| `planning_starts_at` | `TEXT` |no| — |Start planning in UTC.|
| `play_starts_at` | `TEXT` |no| — |Start of game period in UTC.|
| `play_ends_at` | `TEXT` |no| — |End of game period in UTC.|
| `result_review_ends_at` | `TEXT` |no| — |Completion of result verification in UTC.|
| `created_by` | `TEXT` |yes| `NULL` |The ID of the administrator who created the record.|
| `updated_by` | `TEXT` |yes| `NULL` |The ID of the administrator who last modified the record.|
| `created_at` | `TEXT` |no| `CURRENT_TIMESTAMP` |Creation time.|
| `updated_at` | `TEXT` |no| `CURRENT_TIMESTAMP` |Last update time.|

Mandatory constraints and indexes:

- `CHECK (planning_starts_at <= play_starts_at AND play_starts_at < play_ends_at AND play_ends_at <= result_review_ends_at)`;
- `CHECK` for allowed values ​​`status`;
- index `(status, planning_starts_at, play_ends_at, result_review_ends_at)` to find open periods.

### 21.2. `challenge_period_players`

One record stores the state of one player in one period.

|Field|Type| Null | Default |Purpose|
|---|---|---:|---|---|
| `period_id` | `TEXT` |no| — | FK → `challenge_periods.id`. |
| `player_id` | `TEXT` |no| — | FK → `profiles.id`. |
| `status` | `TEXT` |no| `'not_selected'` |`not_selected`, `available`, `unavailable`, `match_scheduled` or `played`.|
| `challenge_duel_id` | `TEXT` |yes| `NULL` |FK → `duels.id`; the player's current confirmed or played Challenge-duel. For `Requested new time` and after cancellation - `NULL`.|
| `created_at` | `TEXT` |no| `CURRENT_TIMESTAMP` |Creation time.|
| `updated_at` | `TEXT` |no| `CURRENT_TIMESTAMP` |Last update time.|

Mandatory constraints and indexes:

- `PRIMARY KEY (period_id, player_id)`;
- `CHECK` for allowed values ​​`status`;
- `CHECK`, which is set to `challenge_duel_id` for `match_scheduled` and `played`, and `NULL` for other statuses;
- index `(period_id, status)` for the list of available players;
- accepting a match atomically sets `challenge_duel_id` to both players only if it is still `NULL`; this field is the blocking DB level of the second confirmed match in the same period.

### 21.3. `challenge_requests`

One entry stores the bid or current counteroffer between two players.

|Field|Type| Null | Default |Purpose|
|---|---|---:|---|---|
| `id` | `TEXT` |no| — | Primary key. |
| `period_id` | `TEXT` |no| — | FK → `challenge_periods.id`. |
| `player_1_id` | `TEXT` |no| — |FK → `profiles.id`; first participant|
| `player_2_id` | `TEXT` |no| — |FK → `profiles.id`; second participant|
| `created_by_player_id` | `TEXT` |no| — |FK → `profiles.id`; original author, does not change with counter-proposals.|
| `awaiting_player_id` | `TEXT` |no| — |FK → `profiles.id`; the participant from whom the next action is expected.|
| `status` | `TEXT` |no| `'pending'` |`pending`, `accepted`, `declined`, `cancelled_by_sender`, `auto_cancelled` or `expired`.|
| `time_option_1_utc` | `TEXT` |no| — |The first suggested time is in UTC.|
| `time_option_2_utc` | `TEXT` |yes| `NULL` |The second suggested time is in UTC.|
| `time_option_3_utc` | `TEXT` |yes| `NULL` |The third suggested time is in UTC.|
| `allows_bo3` | `INTEGER` |no| `0` |Is the Bo3 format available?|
| `allows_bo5` | `INTEGER` |no| `0` |Is Bo5 format available.|
| `accepted_time_utc` | `TEXT` |yes| `NULL` |Selected or actual time after acceptance.|
| `accepted_format` | `TEXT` |yes| `NULL` |Agreed format: `Bo3` or `Bo5`.|
| `hidden_by_creator_at` | `TEXT` |yes| `NULL` |Hides the terminal request from the author list without physical deletion.|
| `created_at` | `TEXT` |no| `CURRENT_TIMESTAMP` |Creation time.|
| `updated_at` | `TEXT` |no| `CURRENT_TIMESTAMP` |Time of last change or counteroffer.|

Mandatory constraints and indexes:

- `CHECK (player_1_id <> player_2_id)`;
- `CHECK` that `created_by_player_id` and `awaiting_player_id` is one of two participants;
- `CHECK (allows_bo3 = 1 OR allows_bo5 = 1)` and checking boolean fields on `0/1`;
- `CHECK` for allowed statuses and formats;
- `CHECK` that `time_option_1_utc` is given, `time_option_3_utc` cannot be given without `time_option_2_utc`, and all time options given are unique;
- for `accepted` both fields `accepted_time_utc` and `accepted_format` are mandatory;
- the backend checks that `accepted_time_utc` is equal to one of the current request time options;
- upon counteroffer `time_option_1_utc`, `time_option_2_utc` and `time_option_3_utc` are replaced transactionally, and the previous value remains in `audit_trail`;
- limits `play_starts_at..play_ends_at` for time options are checked by the backend in the transaction because they depend on `challenge_periods`;
- partial unique index for one `pending`-request for the normalized pair `(period_id, min(player_1_id, player_2_id), max(player_1_id, player_2_id))`;
- indices `(awaiting_player_id, status)`, `(created_by_player_id, status)` and `(period_id, status)`;
- the backend counts the three-request limit via `(created_by_player_id, status = 'pending')` in the creation transaction.

### 21.4. `notifications`

The universal table is the outbox and in-app notification store for all future site domains. One logical event can create separate entries for `in_app` and `email` channels. For Challenges, the domain context is stored via `domain`, `event_type`, `source_entity_type`, `source_entity_id`, and `payload`, without separate Challenge-specific columns.

|Field|Type| Null | Default |Purpose|
|---|---|---:|---|---|
| `id` | `TEXT` |no| — | Primary key. |
| `recipient_user_id` | `INTEGER` |no| — |FK → `users.id`; the user to whom the notification is addressed.|
| `domain` | `TEXT` |no| — |Event domain: `challenge`, `tournament`, `news`, `system`, etc.|
| `event_type` | `TEXT` |no| — |Namespaced event type, such as `challenge.request.created` or `challenge.duel.cancelled`.|
| `source_entity_type` | `TEXT` |yes| `NULL` |Source object type: `challenge_request`, `challenge_duel`, `tournament`, `news_post`, etc.|
| `source_entity_id` | `TEXT` |yes| `NULL` |ID of the source object.|
| `channel` | `TEXT` |no| `'in_app'` |`in_app` or `email`.|
| `payload` | `TEXT` |no| `'{}'` |JSON data for display, email template and domain context.|
| `deduplication_key` | `TEXT` |no| — |A stable key for event, recipient, and channel identity.|
| `delivery_status` | `TEXT` |no| `'pending'` |`pending`, `sent` or `failed`.|
| `sent_at` | `TEXT` |yes| `NULL` |Time of successful delivery.|
| `read_at` | `TEXT` |yes| `NULL` |Time to read the in-app notification.|
| `last_error` | `TEXT` |yes| `NULL` |Last delivery error.|
| `created_at` | `TEXT` |no| `CURRENT_TIMESTAMP` |Creation time.|
| `updated_at` | `TEXT` |no| `CURRENT_TIMESTAMP` |Last update time.|

Mandatory constraints and indexes:

- `UNIQUE (deduplication_key)` to protect against duplicate notifications;
- `CHECK` for `domain`, `event_type`, `channel` and `delivery_status`;
- `CHECK` that `source_entity_type` and `source_entity_id` are either both given, or both `NULL`;
- indices `(recipient_user_id, channel, read_at, created_at)`, `(delivery_status, channel, created_at)` and `(domain, source_entity_type, source_entity_id)`.

### 21.5. Extending the existing table `duels`

For Challenge matches, fields are added to `duels`:

|Field|Type| Null | Default |Purpose|
|---|---|---:|---|---|
| `challenge_period_id` | `TEXT` |yes| `NULL` |FK → `challenge_periods.id`; set only for Challenge-duel.|
| `challenge_request_id` | `TEXT` |yes| `NULL` |FK → `challenge_requests.id`; `NULL` for match created by admin without request.|
| `source_type` | `TEXT` |yes| `NULL` |For Challenge-duel is `challenge`.|
| `cancelled_by_player_id` | `TEXT` |yes| `NULL` |ID of the player who canceled the match; `NULL` if the match is not cancelled. Sentinel value `'1'` is used for admin cancellation.|
| `cancellation_reason` | `TEXT` |yes| `NULL` |Recorded cancellation reason.|
| `cancelled_at` | `TEXT` |yes| `NULL` |Time of cancellation in UTC.|

For Challenge-duel, available fields are used as follows:

- `match_id = NULL`;
- `player_1_id` and `player_2_id` contain members;
- `time_utc` contains the agreed and, after adjustment, the actual match time;
- `duel_format` contains `Bo3` or `Bo5` according to the entry in `duel_formats`;
- `status` contains `Draft`, `Requested new time`, `Planned`, `In progress`, `Done`, `Error` or `Cancelled`;
- `deleted_at` is not used for normal Challenge-cancel if `Cancelled` status is maintained.

Mandatory constraints and indexes:

- unique partial index on `challenge_request_id` when it is not `NULL` so that one request does not create two duels;
- indexes `(challenge_period_id, status)` and `(source_type, player_1_id, player_2_id)`;
- `CHECK`, which `source_type = 'challenge'` is set to `challenge_period_id`, both are different players, `match_id IS NULL` and allowed Challenge-status;
- `cancelled_by_player_id` refers to `profiles.id` for player cancellation; the value `'1'` is reserved for admin cancellation;
- A DB trigger or equivalent transactional check matches `challenge_period_players.challenge_duel_id` with duel participants and disallows a second active/played Challenge-duel in the period.

### 21.6. Extending the existing table `audit_trail`

A new audit log table is not created. The following are added to the existing `audit_trail`:

|Field|Type| Null | Default |Purpose|
|---|---|---:|---|---|
| `actor_player_id` | `TEXT` |yes| `NULL` |FK → `profiles.id`; Challenge player who performed the action.|
| `idempotency_key` | `TEXT` |yes| `NULL` |A stable key to prevent a duplicate audit event during a repeated API request.|

For Challenge events, `entity_type` contains `challenge_period`, `challenge_request` or `challenge_duel`, `record_id` is the ID of the corresponding object, `changes` is the JSON with the previous/next state, and `metadata` is the associated `period_id`, `request_id`, `duel_id` and the technical context of the operation. A unique partial index is created for `idempotency_key IS NOT NULL`.

### 21.7. Objects that are not created

- A separate result table is not needed: the result is stored in the existing `duels` and `games`.
- A separate Challenge match object is not required: the match is stored as `duels` with `source_type = 'challenge'`.
- A separate `challenge_request_time_options` table is not created for MVP: the three current time options are stored by `time_option_1_utc`, `time_option_2_utc` and `time_option_3_utc` fields in `challenge_requests`.
- A separate table `challenge_notifications` is not created: Challenge notifications are stored in a universal table `notifications` with `domain = 'challenge'`.
- `games` does not require new Challenge fields and is linked via the existing `games.duel_id`.
- The team table `matches` is not used for Challenges.

## 22. Implementation tracker

This section tracks large technical blocks. Detailed readiness is determined by the requirement checkboxes above.

- [x] **CH-IMP-001** Database schema and migrations.
- [x] **CH-IMP-002** Period API and admin UI.
- [ ] **CH-IMP-003** Player-period status API and UI.
- [x] **CH-IMP-004** Eligibility and list of available opponents.
- [x] **CH-IMP-005** Request API and player UI.
- [ ] **CH-IMP-006** Accept/counteroffer/cancel/expire state transitions.
- [ ] **CH-IMP-007** Integration of Challenge requests with `duels` and `games` without using team `matches`.
- [ ] **CH-IMP-008** Reschedule and match cancellation flows.
- [ ] **CH-IMP-009** Result review and manual adjustment.
- [ ] **CH-IMP-010** Notifications and email.
- [ ] **CH-IMP-011** Banner on main page.
- [ ] **CH-IMP-012** Audit log.
- [ ] **CH-IMP-013** Background expiration/status jobs.
- [ ] **CH-IMP-014** Automated tests of critical state transitions and race conditions.
- [ ] **CH-IMP-015** End-to-end MVP validation.

## 23. Outside of MVP

- Calculation and updating of the rating.
- Implementation or change of the mechanism of automatic import of BGA results.
- Report an Issue.
- Penalties for no-show, incorrect result or late cancellation.
- Special logic for changing the association in the middle of a period.
- Historical UI for archived periods, requests, matches and rating delta.
- Advanced admin filters, search and export.
- Reminder before the match.
- A reminder before the end of the result review period.

## 24. After MVP

The following features can use saved Challenge matches and audit history, but require a separate scope:

- the impact of the results of Challenges on the rating;
- automatic matching of BGA tables with the Challenge match;
- Report an Issue and admin dispute workflow;
- sanctions for abuse;
- participation history and Challenges statistics;
- reminders and advanced notification channels.
