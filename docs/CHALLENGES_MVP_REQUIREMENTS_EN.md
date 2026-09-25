# Challenges MVP - requirements and implementation tracker

Document status: `draft for review`

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
5. Plays no more than `max_matches_per_player` matches per period.
6. Reviews and, if necessary, adjusts the result of the match.

## 2. Established product decisions

- Calculating or updating the rating is not part of this MVP.
- The logic for automatically finding and importing BGA results is not included in this MVP. Challenges only uses the result obtained by an existing or separately implemented importer.
- The result is stored in the existing `duels` and `games` entities. A separate Result entity is not created.
- Score adjustment works on trust: one player's change is applied immediately, without confirmation by the other player.
- A challenge match is the product name of a singles series between two players. At the data level, it is stored as one `duel` and its associated `games`, without creating a team entry in `matches`.
- For Challenge duels, the existing `duels` statuses are used, expanded only if necessary.
- Tournament complaints, problems, and player requests are stored in the universal `tournament_cases` table. In the current MVP, only a `no_show` case is created automatically; a case-list and case-management UI is not included yet.
- A mutual cancellation of a problematic Challenge match does not create a `tournament_cases` record; a no-show creates an open complaint and stores the details supplied by the player.
- Each Challenge period defines `max_matches_per_player`; one player may have no more than that many slot-occupying matches in the period.
- The match limit is stored only in `challenge_periods`. The actual number of scheduled/played matches is not duplicated in `challenge_period_players`; it is calculated transactionally from `duels`.
- A player's participation status (`not_selected`, `available`, `unavailable`) is not an aggregate match status. A player may have confirmed or completed matches and remain `available` until the limit is reached.
- Within one `Rivals` tournament, the same pair of players may have no more than one non-cancelled Challenge match, regardless of how many Challenge periods are linked to that tournament.
- For schedule-conflict checks, a Bo3 lasts 90 minutes and a Bo5 lasts 150 minutes.
- The limit of pending requests created by a player is configured separately for each period through `max_pending_requests_per_player`, with a default of `3`.
- A player with `available` status may optionally specify up to three time windows during which they can both start and finish a match. These windows are informational and do not restrict request creation or the time options proposed in a request.
- The Challenges page has a public preview mode: periods, request sections, the open-to-match player list, and action buttons are also displayed to unauthenticated users and users without a linked BGA profile. Mutating actions remain available only after sign-in and BGA account verification.
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

The status belongs to the specific pair `player_id + period_id` and controls visibility in the `Open to match` list and a full opt-out from new matches. Individual match states and the number of used match slots are derived from `duels`.

|Status|Display|Meaning|
|---|---|---|
| `not_selected` |Not selected|The player is hidden from the `Open to match` list but may create requests and be selected manually through `Create request` while a match slot remains.|
| `available` |Open to match|The player is open to new requests while at least one match slot remains.|
| `unavailable` |Not playing this period|The player has fully opted out of creating, receiving, and accepting requests during this period.|

### 3.3. Request statuses

|Status|Meaning|
|---|---|
| `pending` |Pending player action with `awaiting_player_id`.|
| `accepted` |The request has been accepted and is linked to a confirmed match.|
| `declined` |The request was rejected by the player from whom a response was expected.|
| `cancelled_by_sender` |The request has been withdrawn by its original author.|
| `auto_cancelled` |The request was automatically closed because a match limit was reached, a player switched to `unavailable`, the period closed, or a match for the same pair was confirmed within the Rivals tournament.|
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
- [ ] **CH-PER-014** In `result_review`, new requests cannot be created or accepted; every pending request in the period automatically becomes `expired`.
- [ ] **CH-PER-015** `result_review` can view and adjust match results.
- [ ] **CH-PER-016** In `archived`, all player actions are blocked.
- [ ] **CH-PER-017** An admin may move a period to `cancelled` only after cancelling unfinished matches; when a period moves to `archived` or `cancelled`, all of its pending requests automatically become `expired`.
- [ ] **CH-PER-018** The Challenges page displays all periods with statuses `draft`, `planning_open`, `active`, and `result_review`.
- [ ] **CH-PER-019** The simultaneous existence of the current `active` period and the next `planning_open` period is supported by the UI and API.
- [ ] **CH-PER-020** A period contains a positive integer `max_matches_per_player` with a default of `1`.
- [ ] **CH-PER-021** A period contains a positive integer `max_pending_requests_per_player` with a default of `3`.
- [ ] **CH-PER-022** An admin can edit both limits in the Challenge-period form.

## 5. Banner on the main page

- [ ] **CH-BAN-001** Challenges banner is displayed on the main page for an open planning period.
- [ ] **CH-BAN-002** The banner contains the name and main dates of the period.
- [ ] **CH-BAN-003** The banner contains a link to the Challenges page.
- [ ] **CH-BAN-004** Banner does not show `draft`, `archived` or `cancelled` periods.

## 6. Status of the player during the period

- [x] **CH-PLY-001** There is no more than one status record for each player and period.
- [x] **CH-PLY-002** Initial player status is `not_selected`.
- [ ] **CH-PLY-003** A player may switch among `not_selected`, `available`, and `unavailable` during `planning_open` or `active`, even when they already have scheduled or completed matches, while `matches_count < matches_limit`.
- [ ] **CH-PLY-004** Switching to `not_selected` removes the player from the `Open to match` list but preserves their ability to create requests and be selected manually, without changing pending requests, linked duels, or confirmed matches.
- [ ] **CH-PLY-005** Before switching from `available` or `not_selected` to `unavailable`, the UI warns that pending requests will be closed automatically when such requests exist.
- [ ] **CH-PLY-006** When switching from `available` or `not_selected` to `unavailable`, every pending request involving that player becomes `auto_cancelled`; linked duels in `Draft` or `Requested new time` become `Cancelled`, while confirmed matches remain unchanged.
- [ ] **CH-PLY-007** Switching from `available` to `not_selected` does not close pending requests or change linked duels in `Draft` or `Requested new time`.
- [ ] **CH-PLY-008** Confirming a match does not change either player's stored manual participation status; after the limit is reached, the UI overrides it with a derived locked state.
- [ ] **CH-PLY-009** Receiving a valid result and moving a duel to `Done` does not change the stored manual participation status, but may change the derived UI state from `Matches scheduled` to `Matches played`.
- [ ] **CH-PLY-010** Cancelling one of several matches does not change either player's participation status.
- [ ] **CH-PLY-011** Moving a duel to `Requested new time` does not change either player's participation status.
- [ ] **CH-PLY-012** A player with scheduled or completed matches and a free slot may hide themselves from the `Open to match` list through `not_selected` or fully close themselves to additional matches through `unavailable`.
- [ ] **CH-PLY-013** The migration maps legacy `match_scheduled` and `played` statuses to `available`, preserves existing duels, and removes `challenge_duel_id`; derived match limits additionally constrain actual eligibility after migration.
- [x] **CH-PLY-014** When `matches_count >= matches_limit`, `Your period status` shows the locked `Matches scheduled` state instead of the manual status while not all allowed matches have the `Done` status.
- [x] **CH-PLY-015** When the number of Challenge duels in the `Done` status reaches `matches_limit`, `Your period status` shows the locked `Matches played` state.
- [x] **CH-PLY-016** `Matches scheduled` and `Matches played` are derived UI states only and do not overwrite the manual status in `challenge_period_players`; after a slot is released, the manual status is displayed and editable again.

### 6.1. Match limit and derived counters

- [x] **CH-CAP-001** A match occupies one player slot when its Challenge duel is not soft-deleted and has status `Planned`, `In progress`, `Done`, or `Error`.
- [x] **CH-CAP-002** Duels in `Draft`, `Requested new time`, or `Cancelled`, and soft-deleted duels, do not occupy a slot.
- [x] **CH-CAP-003** A player's `matches_count` is calculated transactionally from `duels` using `challenge_period_id`, participant, and the statuses in `CH-CAP-001`; it is not stored in `challenge_period_players`.
- [x] **CH-CAP-004** `matches_limit` comes from `challenge_periods.max_matches_per_player` and is not duplicated in `challenge_period_players`.
- [x] **CH-CAP-005** For each period, the player API returns the derived fields `matches_count`, `matches_played_count`, `matches_limit`, `matches_remaining = max(0, matches_limit - matches_count)`, `is_match_limit_reached`, and `is_match_limit_played`.
- [x] **CH-CAP-006** A player may create, receive through manual selection, or accept a request when their status is `available` or `not_selected` and `matches_count < matches_limit`; `unavailable` blocks these actions.
- [x] **CH-CAP-007** Confirming a match increases the derived `matches_count` for both players without storing a separate cached counter.
- [x] **CH-CAP-008** When a player reaches `matches_limit` after confirming a match, every other pending request involving that player becomes `auto_cancelled`; while the player remains below the limit, the other pending requests remain open.
- [x] **CH-CAP-009** Duels in `Draft` or `Requested new time` that belong to auto-cancelled requests become `Cancelled`.
- [x] **CH-CAP-010** If a slot is released by `Cancelled`, `Requested new time`, or soft deletion, a player whose status is `available` or `not_selected` may create and handle requests again without manually changing status; only `available` is shown in the `Open to match` list.
- [x] **CH-CAP-011** A duel in `Error` occupies a slot indefinitely until the existing player/admin flow moves it to `Done`, `Cancelled`, or a soft-deleted state; there is no separate automatic slot release.

### 6.2. Player availability time windows

- [x] **CH-AVL-001** A player with `available` status can save zero to three availability time windows for a specific period.
- [x] **CH-AVL-002** Each window represents the full interval during which the player can both start and finish a match.
- [x] **CH-AVL-003** Windows can be changed only while the period is `planning_open` or `active` and the player's status remains `available`.
- [x] **CH-AVL-004** An empty availability array removes all saved time windows.
- [x] **CH-AVL-005** Each window must contain valid timestamps, satisfy `start < end`, and remain fully within `play_starts_at..play_ends_at`.
- [x] **CH-AVL-006** The start, end, and duration of each window use whole-hour boundaries in the player's association timezone.
- [x] **CH-AVL-007** A player's time windows cannot overlap; the backend sorts them by start time before saving.
- [x] **CH-AVL-008** Windows are stored in UTC and returned by the API as an array of `{ start_utc, end_utc }` objects.
- [x] **CH-AVL-009** The player UI displays a calendar in the player's association timezone, blocks hours outside the period's playing window, and supports drag selection.
- [x] **CH-AVL-010** A saved window can be selected, resized by dragging its top or bottom edge, and removed with the `×` button or the Delete/Backspace key.
- [x] **CH-AVL-011** On mobile, the calendar provides horizontal day scrolling, compact columns, and previous/next day controls.
- [x] **CH-AVL-012** The “Open to match” list displays an opponent's saved windows in the current player's timezone.
- [x] **CH-AVL-013** Updates use `PATCH /challenge-periods/:id/player-availability`, revalidate the period and player statuses, and create the `challenge_period_player.availability_updated` audit event.

## 7. Available opponents and eligibility

- [x] **CH-ELG-001** Challenges mutating actions can only be performed by an authenticated user associated with a verified player profile; the sections and buttons themselves may remain visible in public preview mode.
- [x] **CH-ELG-002** Active and inactive profiles can be participants in Challenges.
- [x] **CH-ELG-003** The "Open to Match" list contains players with the status `available` in the selected period.
- [x] **CH-ELG-004** The current player does not appear as an available opponent for himself.
- [x] **CH-ELG-005** The list of available opponents does not show players from the same association.
- [x] **CH-ELG-006** The available-opponents list excludes players who have reached `max_matches_per_player`; a player with one or more matches below the limit remains listed while their status is `available`.
- [x] **CH-ELG-007** `Create request`, another manual selector, or the Players page may invite a player with status `available` or `not_selected`, an unused match slot, and no other eligibility restriction.
- [x] **CH-ELG-008** A player with `not_selected` is hidden from the `Open to match` list but remains available through manual selection; a player with `unavailable` cannot be invited.
- [x] **CH-ELG-009** Having a scheduled or completed match does not by itself block a new request while the player remains below the limit.
- [x] **CH-ELG-010** Cannot create a second pending request between the same pair of players in the same period.
- [x] **CH-ELG-011** After the previous request becomes terminal, the same pair can be invited again only if it has no other non-cancelled Challenge duel in the linked Rivals tournament.

Terminal statuses for re-invitation:

- `declined`;
- `cancelled_by_sender`;
- `auto_cancelled`;
- `expired`.

### 7.1. Opponent uniqueness within a Rivals tournament

- [x] **CH-RIV-001** When a Challenge period has a `rivals_tournament_id`, the backend checks the player pair across every Challenge period with the same `rivals_tournament_id`.
- [x] **CH-RIV-002** A pair is considered already used when it has a non-soft-deleted Challenge duel in `Draft`, `Requested new time`, `Planned`, `In progress`, `Done`, or `Error`.
- [x] **CH-RIV-003** A duel in `Cancelled` or a soft-deleted duel does not block a later request between the same pair.
- [x] **CH-RIV-004** For a period without `rivals_tournament_id`, pair uniqueness is enforced at least within that Challenge period.
- [x] **CH-RIV-005** Pair uniqueness is checked when a request is created and checked again transactionally on accept; for a reschedule, the current duel is excluded from the check.
- [x] **CH-RIV-006** When a pair confirms a match, all other pending requests for that pair across the linked Rivals tournament become `auto_cancelled`.
- [x] **CH-RIV-007** Pair comparison is unordered: `(player A, player B)` and `(player B, player A)` are the same pair.

### 7.2. “Open to match” list toggle

- [x] **CH-OPP-001** The `Open to match` section has a two-option toggle: `Available opponents` and `All players`.
- [x] **CH-OPP-002** `Available opponents` is selected by default whenever the page or period is opened.
- [x] **CH-OPP-003** `All players` shows every player whose participation status is `available` and who has a free match slot, including the current player, players from the same association, and players who already have a non-cancelled match with the current player in the linked Rivals tournament.
- [x] **CH-OPP-004** `Available opponents` uses the same base list, shows the current player first, and then lists the eligible opponents.
- [x] **CH-OPP-005** `Available opponents` excludes players from the current player's association.
- [x] **CH-OPP-006** `Available opponents` excludes players who already have a non-cancelled Challenge duel with the current player in any period of the linked Rivals tournament; for a period without `rivals_tournament_id`, the current period is checked.
- [x] **CH-OPP-007** In `All players`, the current player's row has no invite action; same-association and already-matched rows show a disabled invite action or a clear reason why they are unavailable.
- [x] **CH-OPP-008** Switching the toggle works without reloading the page and does not change backend eligibility: a direct request to an unavailable opponent is still rejected.
- [x] **CH-OPP-009** The API returns enough data for both views, including `is_current_player`, `is_same_association`, `has_rivals_match`, `matches_count`, `matches_limit`, and `is_match_limit_reached`, or equivalent separate collections.
- [x] **CH-OPP-010** `Invite to match` is blue when a request can be created and both players have a free slot; when unavailable, it is transparent, remains clickable, and opens a popup explaining the blocking reason.

### 7.3. Public preview and Challenges access onboarding

- [x] **CH-ONB-001** An unauthenticated user and an authenticated user without a verified BGA profile can see all open Challenge periods, `Your period status`, the `Requests` / `Incoming` / `Sent` sections, the `Create request` button, the `Open to match` section, and `Invite to match` buttons; the page renders the normal UI instead of `Unauthorized` or `Linked player profile is required`.
- [x] **CH-ONB-002** When a user without full access attempts to change `Your period status`, click `Create request`, or click `Invite to match`, no mutating request is performed and a Challenges-styled onboarding popup opens.
- [x] **CH-ONB-003** The popup explains in clear English that scheduling a Challenge match requires signing in to carcassonne.gg and verifying a BGA account.
- [x] **CH-ONB-004** The popup contains two sequential items: `Sign in to the site` and `Verify your BGA account`; completed items have a completed state.
- [x] **CH-ONB-005** `Sign in with Google` opens authentication in a separate popup window. After successful sign-in, the onboarding popup remains open, refetches `/auth/me`, and lets the user continue with the second item.
- [x] **CH-ONB-006** `Verify your BGA account` is complete when `/auth/me` returns a linked player/BGA profile ID and the profile's `Login Email`.
- [x] **CH-ONB-007** Before verification, the second item displays its instructions immediately without an intermediate `Details` link: the user's login email with a copy button, a captains-list link, steps for the captain, a clickable `Players` label linking to `https://carcassonne.gg/player-hub/players/`, and the support email address.
- [x] **CH-ONB-008** `Check verification` refetches the user state, displays `https://carcassonne.gg/gallery/loading-red.gif` while checking, and is disabled against repeated clicks during the request.
- [x] **CH-ONB-009** If the BGA profile remains unverified after a manual check, the popup displays `Your BGA account is not linked yet. Please contact your association captain.` and remains open.
- [x] **CH-ONB-010** While the popup is open, the BGA verification state is also refreshed automatically; after successful verification, the second item switches to its completed state.
- [x] **CH-ONB-011** When both items are complete, the popup footer contains only the secondary `Start playing` button; it only closes the popup and does not automatically repeat the action that opened onboarding.
- [x] **CH-ONB-012** `GET /challenge-periods/player` and `GET /challenge-periods/:id/eligible-opponents` support public reads, while request creation, player-status changes, and other mutating Challenge endpoints remain protected by authentication and backend eligibility checks.

## 8. Creation of an request

### 8.1. Entry points

- [x] **CH-REQ-001** A request can be created through the Player Hub → Challenges clean form; its opponent selector contains eligible players with `available` or `not_selected` status.
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

- [ ] **CH-REQ-013** One player may be the author of no more than `challenge_periods.max_pending_requests_per_player` pending requests within one period; the default limit is `3`.
- [x] **CH-REQ-014** The limit is calculated for the pair `period_id + created_by_player_id`.
- [x] **CH-REQ-015** Another player's counteroffer does not release the bid from its original submitter's limit.
- [ ] **CH-REQ-016** The backend reads the period's current limit and checks it in the same transaction that creates the request.

### 8.4. Withdrawal and deletion

- [ ] **CH-REQ-017** The original author can withdraw his pending request.
- [ ] **CH-REQ-018** After revoking, request goes to `cancelled_by_sender`.
- [ ] **CH-REQ-019** After revocation, the author can hide/delete the request from his list without deleting the audit history.
- [ ] **CH-REQ-020** Another participant cannot revoke a request on behalf of its original author.

### 8.5. Expiration

- [ ] **CH-REQ-021** The request does not expire after passing a separate time option, if future options remain in it.
- [ ] **CH-REQ-022** Pending request goes to `expired` after last suggested time.
- [ ] **CH-REQ-023** Expiration is performed automatically and idempotently.
- [x] **CH-REQ-024** `Create request` is blue while the player has a free match slot and may create a request; after reaching the match or pending-request limit, or with `unavailable` status, it is transparent, remains clickable, and shows a popup with the specific reason.

## 9. Review of requests

- [ ] **CH-LST-001** The player sees a list of pending requests, in which his response is expected.
- [ ] **CH-LST-002** The player sees a list of the requests they have created with their current statuses.
- [ ] **CH-LST-003** For a pending request, the proposed times, formats and player from whom a response is expected are shown.
- [ ] **CH-LST-004** For the terminal request, its final status is shown.
- [x] **CH-LST-005** A player sees all of their Challenge matches in every open period, not only the first match.
- [x] **CH-LST-006** The match-list heading shows a counter formatted as `X / N matches`, where `X = matches_count` and `N = matches_limit`.
- [x] **CH-LST-007** Each match in the list retains its own reschedule, cancellation, result-view, and result-edit actions according to its duel status.

## 10. Answer and counteroffer

- [ ] **CH-RSP-001** Only a player with `awaiting_player_id` can accept or reject a pending request.
- [ ] **CH-RSP-002** If rejected, the request goes to `declined`.
- [ ] **CH-RSP-003** Player with `awaiting_player_id` can offer one to three other timing options.
- [ ] **CH-RSP-005** When counteroffered, `awaiting_player_id` changes to another participant.
- [ ] **CH-RSP-006** When counteroffered, the status remains `pending`.
- [ ] **CH-RSP-007** New counteroffer time options must also be within the game period.
- [ ] **CH-RSP-008** Counteroffer is recorded in audit log along with actor and time option changes.

## 11. Request acceptance

- [x] **CH-ACC-001** When accepting, the player chooses one of the current suggested times.
- [x] **CH-ACC-002** If both formats are offered, player chooses Bo3 or Bo5.
- [x] **CH-ACC-003** If one format is offered, it is used automatically.
- [x] **CH-ACC-004** Request acceptance is performed by one DB transaction.
- [x] **CH-ACC-005** The transaction is rechecking that the request is still `pending`.
- [x] **CH-ACC-006** Transaction checks that actor is `awaiting_player_id`.
- [x] **CH-ACC-007** The transaction checks that the period allows the request to be accepted.
- [x] **CH-ACC-008** Within the transaction, `matches_count` is recalculated for both players and the condition `matches_count < max_matches_per_player` is checked.
- [x] **CH-ACC-009** Request goes to `accepted`.
- [x] **CH-ACC-010** A new `Planned` duel is being created or an associated duel in status `Requested new time` is being confirmed.
- [x] **CH-ACC-011** Duel contains two players, agreed time and format.
- [x] **CH-ACC-012** Confirmation does not change either player's participation status, and the API returns updated derived match counters.
- [x] **CH-ACC-013** Other pending requests involving these players remain open if the relevant player still has a free slot after accept; for a player who reached the limit, they become `auto_cancelled`.
- [x] **CH-ACC-014** Duels in `Draft` or `Requested new time` that belong to auto-cancelled requests become `Cancelled`.
- [x] **CH-ACC-015** The transactional check prevents either player's `matches_count` from exceeding `max_matches_per_player`, including during simultaneous accepts.
- [x] **CH-ACC-016** Repeating the same accept does not create a duplicate duel.

### 11.1. Schedule conflicts between matches

- [x] **CH-SCH-001** For scheduling checks, the standard duration is `90` minutes for Bo3 and `150` minutes for Bo5.
- [x] **CH-SCH-002** The check includes the player's non-soft-deleted duels in the current Challenge period with a valid `time_utc` and status `Planned`, `In progress`, `Done`, or `Error`.
- [x] **CH-SCH-003** For every new/existing match pair, the absolute difference between their `time_utc` values must be at least the greater standard duration of the two formats: 90 minutes for Bo3/Bo3 and 150 minutes for Bo3/Bo5 or Bo5/Bo5.
- [x] **CH-SCH-004** Exactly 90 or 150 minutes, as applicable, is allowed; a smaller interval before or after an existing start time is a conflict.
- [x] **CH-SCH-005** The backend checks both players transactionally on accept after a specific time and format are selected; the same rule applies to re-accept after rescheduling and to retroactive confirmation.
- [x] **CH-SCH-006** On conflict, the backend returns a conflict response with the conflicting duel and does not change the request, duel, or any other entity.
- [x] **CH-SCH-007** Time options may be proposed even when a potential conflict exists; the selected option is blocked only during accept.

### 11.2. Creating a match retroactively

This flow is used if players have already played a match, but did not create an request in advance, or created it, but did not have time to confirm it before the start of the match. Obtaining and importing actual results is done by existing separate functionality and is not part of this flow.

- [ ] **CH-RET-001** During the `active` period status, a player can create a request with a match time in the past if that time is within `play_starts_at..play_ends_at` the corresponding period.
- [ ] **CH-RET-002** A request with time in the past is clearly marked in the UI as a request for an already played match.
- [ ] **CH-RET-003** The other player may accept a request for an already played match when both players have a free slot, pass the Rivals opponent-uniqueness check, and have no schedule conflict.
- [ ] **CH-RET-004** If a request was created before a match but not accepted in time, a player with `awaiting_player_id` can confirm it after the suggested time as a match already played, even if the request has changed to `expired` status.
- [ ] **CH-RET-005** The creation and confirmation of a request "in fact" is available to `play_ends_at` and also during `result_review`; `archived` and `cancelled` cannot create or confirm such a match.
- [ ] **CH-RET-006** When "in fact" is confirmed, the request goes to `accepted` and the Challenge-duel is created by a single DB transaction with the agreed participants, format and actual match time in the past.
- [ ] **CH-RET-007** Before confirmation, the backend rechecks period status, actual-match time boundaries, actor, both players' eligibility, match limits, opponent uniqueness, and schedule conflicts.
- [ ] **CH-RET-008** After creating a Challenge-duel, the existing mechanism for obtaining actual results is launched or applied; a separate logic for searching or importing results is not implemented within this flow.
- [ ] **CH-RET-009** If the existing mechanism immediately finds a valid result, the duel moves to `Done` without changing either player's participation status; if no result is available yet, the subsequent status follows the general Challenge-duel and result-retrieval rules.
- [ ] **CH-RET-010** Retroactive confirmation closes other pending requests only under the general match-limit and Rivals pair-uniqueness rules.
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
- [ ] **CH-MAT-011** One player may participate in multiple Challenge duels in one period up to `max_matches_per_player`; membership is determined through `duels.player_1_id`/`player_2_id`, not through one duel FK in `challenge_period_players`.

## 13. Postponement of the match

- [ ] **CH-RSC-001** Any participant can initiate a time change either before or after the scheduled start time of the match.
- [ ] **CH-RSC-003** When porting, the associated request is returned at `pending`.
- [ ] **CH-RSC-005** `awaiting_player_id` becomes another member of the match.
- [ ] **CH-RSC-006** Match goes to `Requested new time`.
- [ ] **CH-RSC-007** Pre-confirmed time not saved as active match slot.
- [ ] **CH-RSC-008** `Requested new time` does not occupy a match slot; neither player's participation status changes, and their ability to use other requests depends on their status and remaining matches.
- [x] **CH-RSC-009** When accepting a new time, the request becomes `accepted` and the match becomes `Planned` only if both players still have a slot and the new time does not conflict with their other matches.
- [ ] **CH-RSC-010** After re-accept, the match is counted in `matches_count` again, but neither player's participation status changes.
- [ ] **CH-RSC-011** When the new time is rejected, the request goes to `declined`.
- [ ] **CH-RSC-012** When rejecting a new time, the linked match in status `Requested new time` changes to `Cancelled`.
- [ ] **CH-RSC-013** Rejecting the new time does not change either player's participation status.
- [x] **CH-RSC-014** If accepting another request causes either participant to reach `max_matches_per_player`, the pending reschedule request becomes `auto_cancelled` and its duel in `Requested new time` becomes `Cancelled`; while a slot remains, the reschedule request stays open.

## 14. Cancellation of the match

- [ ] **CH-CAN-001** Any participant may cancel a `Planned` match either before or after the scheduled start time if the match has not been played and the result has not been recorded.
- [ ] **CH-CAN-002** After the scheduled start time, cancellation of an unplayed `Planned` match remains available via player flow.
- [ ] **CH-CAN-003** When cancelled, the match goes to `Cancelled` or equivalently soft-deleted with a recorded cancellation reason.
- [ ] **CH-CAN-004** Cancelling a match reduces both players' derived `matches_count` and does not change their participation status.
- [ ] **CH-CAN-005** After a slot is released, a player with status `available` or `not_selected` may create, receive, and accept requests again; `not_selected` remains hidden from the `Open to match` list, while `unavailable` remains fully closed.
- [ ] **CH-CAN-006** Another member receives a cancellation notification.
- [ ] **CH-CAN-007** Admin can cancel a match regardless of start time.

## 15. Match results

- [ ] **CH-RES-001** An automatically obtained correct result is stored in the existing `duels` and `games` without creating or updating the team `match`.
- [ ] **CH-RES-002** Challenge-duel goes to `Done` after correct completion.
- [ ] **CH-RES-003** In the Challenges UI, the status of `Done` is displayed as `Played`.
- [ ] **CH-RES-004** `Done` continues to occupy one match slot for both players but does not change their participation status.
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

### 15.1. Resolving an `Error` match without a result

- [x] **CH-CAS-001** For a Challenge duel with `Error` status and no recorded duel/game result, the player UI displays `Error` in red.
- [x] **CH-CAS-002** Participants do not see `Edit Results` for such a match; the short `Resolve` action is displayed instead.
- [x] **CH-CAS-003** `Resolve` opens a Challenges-style modal with one custom `Match outcome` dropdown and a disabled `Submit` button until an outcome is selected.
- [x] **CH-CAS-004** The dropdown contains `Match cancelled by mutual agreement of both players` and one `Player <nickname> did not show up for the match` option for each participant; the nickname is displayed in bold.
- [x] **CH-CAS-005** Mutual cancellation changes the duel to `Cancelled`, stores `cancellation_reason = 'Match cancelled by mutual agreement of both players.'`, and does not create a `tournament_cases` record.
- [x] **CH-CAS-006** For a no-show, the modal displays a required editable `Details` textarea prefilled in English with the absent player, opponent, Challenge period name, and scheduled UTC date and time.
- [x] **CH-CAS-007** A no-show changes the duel to `Cancelled`, stores the final `Details` text in `duels.cancellation_reason`, and creates an open `tournament_cases` record with type `complaint` and category `no_show`.
- [x] **CH-CAS-008** The no-show case is linked to the duel, Challenge period, and the period's `rivals_tournament_id` when present; it also stores the submitting player and reported player.
- [ ] **CH-CAS-009** Both resolution flows transactionally change the related request to `auto_cancelled`; the `Cancelled` duel stops occupying a slot, but neither player's participation status changes.
- [x] **CH-CAS-010** The backend permits resolution only to a participant in the relevant Challenge duel, rechecks the `Error` status and absence of a result, and returns a conflict when the match has already changed.
- [x] **CH-CAS-011** `tournament_cases` is not exposed by a dedicated list API and is not displayed anywhere in the UI yet.

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
- [x] **CH-ADM-004** The administrator sees all requests of the period, including terminal statuses.
- [ ] **CH-ADM-005** Admin can see the status of each player in the selected period.
- [ ] **CH-ADM-006** Admin can manually create a Challenge match.
- [ ] **CH-ADM-007** Admin can edit Challenge match.
- [ ] **CH-ADM-008** Admin can transfer Challenge match without player approval flow.
- [ ] **CH-ADM-009** Admin can cancel Challenge match.
- [ ] **CH-ADM-010** Admin can adjust result after `result_review_ends_at`.
- [x] **CH-ADM-011** Above all blocks on the Challenges page, a global admin sees an `Admin mode` toggle; the toggle is not displayed to other users.
- [x] **CH-ADM-012** When `Admin mode` is enabled, every open Challenge period shows all requests from all players rather than only requests involving the current user.
- [x] **CH-ADM-013** When `Admin mode` is enabled, all Challenge matches in the period are shown, including a duel created by an administrator without a `challenge_request_id`.
- [x] **CH-ADM-014** After `Admin mode` is enabled, a `Removed items` toggle appears next to it; enabling it adds hidden requests with `hidden_by_creator_at IS NOT NULL` and soft-deleted duels with `deleted_at IS NOT NULL` to the lists.
- [x] **CH-ADM-015** Requests and matches belonging to other players are read-only in `Admin mode`, without player actions for accepting, declining, rescheduling, cancelling, or removing them.
- [x] **CH-ADM-016** The backend permits the `admin_mode=1` and `include_removed=1` parameters only for global admins; requests for these modes by authenticated non-admins return `403` and do not expose other players' or removed records.
- [x] **CH-ADM-017** Outside `Admin mode`, the API and UI retain player-scoped filtering, and hidden requests and soft-deleted duels are not displayed.
- [ ] **CH-ADM-018** The period admin UI can edit `max_matches_per_player` and `max_pending_requests_per_player` and shows derived `matches_count / matches_limit` for each player without storing those counters in `challenge_period_players`.

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
- [x] **CH-AUD-010** Resolving a problematic Challenge match records audit events for the duel and request; a no-show additionally records `tournament_case.created`.

## 19. Competitiveness and data integrity

- [ ] **CH-CON-001** All multi-entity status transitions are performed transactionally.
- [ ] **CH-CON-002** Simultaneous accepts for one player may create only as many confirmed Challenge duels as there were free slots at the start of the transactions; `matches_count` never exceeds `max_matches_per_player`.
- [ ] **CH-CON-003** Accept and a simultaneous player transition to `not_selected` or `unavailable` finish in one valid state without partially updated data and apply the distinct pending-request preservation/closure rules of those statuses.
- [ ] **CH-CON-004** Accept of an request that has already become a terminal returns a conflict and does not change the data.
- [ ] **CH-CON-005** Resubmitting the same API request does not create duplicate duels, games, audit events, or notifications.
- [ ] **CH-CON-006** Soft-deleted/Cancelled match does not block new match creation for players in the same period.
- [ ] **CH-CON-007** A match in status `Requested new time` is not considered scheduled and does not block other bids.
- [ ] **CH-CON-008** Two simultaneous accepts in different Challenge periods of the same Rivals tournament cannot create two non-cancelled matches between the same pair.
- [ ] **CH-CON-009** Match-limit, pair-uniqueness, and schedule-conflict checks run in the same transaction that creates or replans the duel.

## 20. Critical acceptance scenarios

- [ ] **CH-E2E-001** A player becomes `available`, creates a request, and the opponent accepts it; the match is created, the counter becomes `1 / N matches`, and neither player's participation status changes.
- [ ] **CH-E2E-002** A player who has reached the configured `max_pending_requests_per_player` cannot create another pending request.
- [ ] **CH-E2E-003** The counteroffer modifies `awaiting_player_id` and does not affect the original author's limit.
- [ ] **CH-E2E-004** After decline or duel cancellation, the same pair may be invited again if it has no other non-cancelled duel in the Rivals tournament.
- [ ] **CH-E2E-005** With `max_matches_per_player > 1`, accepting the first request leaves other pending requests open while the relevant player has a free slot.
- [ ] **CH-E2E-006** Multiple simultaneous accepts for one player do not create matches beyond `max_matches_per_player`.
- [ ] **CH-E2E-007** A player with scheduled matches moves to `not_selected`, leaving pending requests and linked `Draft`/`Requested new time` duels unchanged; after moving to `unavailable`, they are closed, while confirmed matches remain unchanged in both cases.
- [ ] **CH-E2E-008** Rescheduling moves the match to `Requested new time`, the request to `pending`, releases one slot, and does not change either player's participation status.
- [ ] **CH-E2E-009** Rejecting a reschedule moves the match from `Requested new time` to `Cancelled` and does not change either player's participation status.
- [ ] **CH-E2E-010** While a duel is in `Requested new time`, a player may confirm another match; the reschedule request is automatically closed only if this causes the player to reach the limit.
- [ ] **CH-E2E-011** An unplayed `Planned` match without a fixed result can be canceled via player flow both before and after the scheduled start time.
- [ ] **CH-E2E-012** Manually changing the score is immediately visible to both players and generates a notification and audit event.
- [ ] **CH-E2E-013** After `result_review_ends_at`, the player cannot change the result, but the admin can.
- [ ] **CH-E2E-014** Proposed time outside the game period is rejected by the backend.
- [ ] **CH-E2E-015** Request expires only after passing the last offered time option.
- [ ] **CH-E2E-016** During `active`, a player creates a request with the time of a match already played within the period, the opponent accepts it, a duel is created and results are obtained through the available functionality.
- [ ] **CH-E2E-017** A request that is not accepted by the suggested time can be confirmed "in fact" to `play_ends_at`; at the same time, only one duel is created, and the result is obtained through the existing functionality.
- [x] **CH-E2E-018** A player with `available` status creates up to three non-overlapping whole-hour windows in the local calendar, saves them in UTC, edits or removes them, and another player sees those windows in their own timezone.
- [ ] **CH-E2E-019** An unauthenticated user sees the full Challenges page preview; each of the three gated actions opens onboarding, Google sign-in does not close the main popup, a manual BGA check displays loading/error states, and after both items are complete `Start playing` closes the popup without automatically repeating the original action.
- [ ] **CH-E2E-020** A global admin enables `Admin mode` and sees other players' requests and matches without player actions, then enables `Removed items` and additionally sees hidden requests and soft-deleted duels; a non-admin does not see the toggles and receives `403` when directly requesting the admin API parameters.
- [ ] **CH-E2E-021** A participant opens an `Error` match without a result, selects mutual cancellation, and sees `Cancelled` after Submit; no case is created, the request is closed, and neither player remains blocked by the duel.
- [ ] **CH-E2E-022** A participant opens an `Error` match without a result, selects one player as a no-show, edits the generated Details, and after Submit receives a `Cancelled` duel and exactly one open `tournament_cases` record linked to the duel, period, and Rivals tournament.
- [ ] **CH-E2E-023** After a player reaches `N / N matches`, all of their other pending requests, including requests linked to `Requested new time`, are automatically closed; below the limit they remain open.
- [ ] **CH-E2E-024** Accept is rejected when the new Bo3/Bo5 start is less than the required 90/150 minutes before or after another match of either participant; the exact boundary is allowed.
- [ ] **CH-E2E-025** Players who already have a non-cancelled duel in an earlier Challenge period of a Rivals tournament cannot create or accept another match with each other in a different period of that tournament.
- [ ] **CH-E2E-026** A period with `max_pending_requests_per_player = 5` allows an author to have five pending requests and rejects the sixth.
- [ ] **CH-E2E-027** The player UI shows all matches in the period and the `X / N matches` counter; after a first match below the limit, a player whose status is `available` remains in the `Open to match` list.
- [ ] **CH-E2E-028** The `Open to match` list opens in `Available opponents` by default, shows the current player first, and excludes other players from their association and previously used Rivals opponents; switching to `All players` displays every available row but still prevents an invalid request.
- [ ] **CH-E2E-029** A player with `available` or `not_selected` status opens `Create request`, selects an eligible opponent with `not_selected`, and successfully creates the request; that opponent still does not appear in the `Open to match` list.

## 21. Data scheme of new DB objects

The schema below targets SQLite. New entity identifiers use `TEXT` unless a table explicitly states otherwise; all timestamps are stored as UTC ISO 8601 values in `TEXT` fields, and boolean values are stored as `INTEGER` values of `0` or `1`.

### 21.1. `challenge_periods`

One entry describes one period of Challenges.

|Field|Type| Null | Default |Purpose|
|---|---|---:|---|---|
| `id` | `TEXT` |no| — | Primary key. |
| `name` | `TEXT` |no| — |Name of the period.|
| `description` | `TEXT` |yes| `NULL` |Brief description.|
| `logo` | `TEXT` |yes| `NULL` |Link to a picture of the period logo.|
| `rivals_tournament_id` | `TEXT` |yes| `NULL` |ID of the related tournament in the `Rivals` category.|
| `max_matches_per_player` | `INTEGER` |no| `1` |Maximum number of matches one player may have in this period.|
| `max_pending_requests_per_player` | `INTEGER` |no| `3` |Maximum number of pending requests simultaneously created by one player in this period.|
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
- `CHECK (max_matches_per_player >= 1)`;
- `CHECK (max_pending_requests_per_player >= 1)`;
- `CHECK` for allowed values ​​`status`;
- index on `rivals_tournament_id` for listing periods of a related Rivals tournament;
- index `(status, planning_starts_at, play_ends_at, result_review_ends_at)` to find open periods.

### 21.2. `challenge_period_players`

One record stores only one player's manually selected participation status and availability windows for one period. It does not store a match reference, match limit, or cached counters.

|Field|Type| Null | Default |Purpose|
|---|---|---:|---|---|
| `period_id` | `TEXT` |no| — | FK → `challenge_periods.id`. |
| `player_id` | `TEXT` |no| — | FK → `profiles.id`. |
| `status` | `TEXT` |no| `'not_selected'` |`not_selected`, `available`, or `unavailable`; the status does not aggregate the player's match states.|
| `availability_start_1_utc` | `TEXT` |yes| `NULL` |Start of the first availability window in UTC.|
| `availability_end_1_utc` | `TEXT` |yes| `NULL` |End of the first availability window in UTC.|
| `availability_start_2_utc` | `TEXT` |yes| `NULL` |Start of the second availability window in UTC.|
| `availability_end_2_utc` | `TEXT` |yes| `NULL` |End of the second availability window in UTC.|
| `availability_start_3_utc` | `TEXT` |yes| `NULL` |Start of the third availability window in UTC.|
| `availability_end_3_utc` | `TEXT` |yes| `NULL` |End of the third availability window in UTC.|
| `created_at` | `TEXT` |no| `CURRENT_TIMESTAMP` |Creation time.|
| `status_updated_at` | `TEXT` |no| `CURRENT_TIMESTAMP` |Time of the last actual `status` change; used to sort `Open to match`.|
| `updated_at` | `TEXT` |no| `CURRENT_TIMESTAMP` |Last update time.|

Mandatory constraints and indexes:

- `PRIMARY KEY (period_id, player_id)`;
- `CHECK` for allowed values ​​`status`;
- each availability window is stored only as a complete start/end pair; the backend validates the three-window limit, period boundaries, whole hours, positive duration, and non-overlap;
- indexes `(period_id, status)` and `(period_id, status, status_updated_at)` for listing and sorting available players;
- `matches_count`, `matches_limit`, `matches_remaining`, and `is_match_limit_reached` are built at query/API level from `duels` and `challenge_periods` and are not added as columns to this table.

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
| `comment` | `TEXT` |yes| `NULL` |Player comment for the current proposal; replaced by a counteroffer.|
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
- within the creation transaction, the backend counts pending requests using `(period_id, created_by_player_id, status = 'pending')` and compares the count with the current `challenge_periods.max_pending_requests_per_player`.

### 21.4. `tournament_cases`

This universal table stores tournament complaints, problems, and player requests. A case can be linked to a team match, duel, tournament, Challenge period, or a future entity type. The current Challenge flow creates a record only for a no-show.

| Field | Type | Null | Default | Purpose |
|---|---|---:|---|---|
| `id` | `INTEGER` | no | auto | Autoincrement primary key. |
| `case_type` | `TEXT` | no | `'problem'` | `complaint`, `problem`, `request`, or `other`. |
| `category` | `TEXT` | yes | `NULL` | Case subtype; `no_show` for the current flow. |
| `status` | `TEXT` | no | `'open'` | `open`, `in_progress`, `resolved`, or `closed`. |
| `priority` | `TEXT` | no | `'normal'` | `low`, `normal`, `high`, or `urgent`. |
| `subject` | `TEXT` | no | — | Short case subject. |
| `details` | `TEXT` | no | `''` | Details supplied by the player or system. |
| `submitted_by_user_id` | `INTEGER` | yes | `NULL` | FK → `users.id`; authenticated user who created the case. |
| `submitted_by_player_id` | `TEXT` | no | — | FK → `profiles.id`; player who submitted the case. |
| `responsible_user_id` | `INTEGER` | yes | `NULL` | FK → `users.id`; user responsible for handling the case. |
| `reported_player_id` | `TEXT` | yes | `NULL` | FK → `profiles.id`; player the case concerns. |
| `match_id` | `TEXT` | yes | `NULL` | FK → `matches.id`; related team match, when present. |
| `duel_id` | `TEXT` | yes | `NULL` | FK → `duels.id`; related duel, when present. |
| `tournament_id` | `TEXT` | yes | `NULL` | FK → `tournaments.id`; related tournament, when present. |
| `challenge_period_id` | `TEXT` | yes | `NULL` | FK → `challenge_periods.id`; related Challenge period, when present. |
| `related_entity_type` | `TEXT` | yes | `NULL` | Extensible type for another related entity. |
| `related_entity_id` | `TEXT` | yes | `NULL` | ID of another related entity. |
| `resolution` | `TEXT` | yes | `NULL` | Decision or outcome of case handling. |
| `resolved_at` | `TEXT` | yes | `NULL` | Resolution time in UTC. |
| `deleted_at` | `TEXT` | yes | `NULL` | Soft-deletion time in UTC. |
| `created_at` | `TEXT` | no | `CURRENT_TIMESTAMP` | Creation time. |
| `updated_at` | `TEXT` | no | `CURRENT_TIMESTAMP` | Last update time. |

Required constraints and indexes:

- `CHECK` constraints for allowed `case_type`, `status`, and `priority` values;
- indexes for workflow `(status, priority, created_at)`, submitter `(submitted_by_player_id, created_at)`, and assignee `(responsible_user_id, status, created_at)`;
- separate indexes for `match_id`, `duel_id`, `tournament_id`, `challenge_period_id`, and the extensible `(related_entity_type, related_entity_id)` pair;
- a no-show case is created in the same `BEGIN IMMEDIATE` transaction that changes the duel to `Cancelled` and closes the request; neither player's participation status changes;
- mutual cancellation does not create a record in this table.

### 21.5. `notifications`

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

### 21.6. Extending the existing table `duels`

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
- indexes `(challenge_period_id, status)`, `(challenge_period_id, player_1_id, status)`, `(challenge_period_id, player_2_id, status)`, and `(source_type, player_1_id, player_2_id)` for slot counts and match lookup;
- `CHECK`, which `source_type = 'challenge'` is set to `challenge_period_id`, both are different players, `match_id IS NULL` and allowed Challenge-status;
- `cancelled_by_player_id` refers to `profiles.id` for player cancellation; the value `'1'` is reserved for admin cancellation;
- accept/re-accept transactionally count each participant's non-soft-deleted duels in `Planned`, `In progress`, `Done`, and `Error` and do not allow `challenge_periods.max_matches_per_player` to be exceeded;
- request creation and accept/re-accept transactionally verify that the same normalized pair has no other non-cancelled Challenge duel across all periods of the relevant `rivals_tournament_id`;
- accept/re-accept transactionally check both participants' `time_utc` conflicts using 90 minutes for Bo3 and 150 minutes for Bo5.

### 21.7. Extending the existing table `audit_trail`

A new audit log table is not created. The following are added to the existing `audit_trail`:

|Field|Type| Null | Default |Purpose|
|---|---|---:|---|---|
| `actor_player_id` | `TEXT` |yes| `NULL` |FK → `profiles.id`; Challenge player who performed the action.|
| `idempotency_key` | `TEXT` |yes| `NULL` |A stable key to prevent a duplicate audit event during a repeated API request.|

For Challenge events, `entity_type` contains `challenge_period`, `challenge_request` or `challenge_duel`, `record_id` is the ID of the corresponding object, `changes` is the JSON with the previous/next state, and `metadata` is the associated `period_id`, `request_id`, `duel_id` and the technical context of the operation. A unique partial index is created for `idempotency_key IS NOT NULL`.

### 21.8. Objects that are not created

- A separate result table is not needed: the result is stored in the existing `duels` and `games`.
- A separate Challenge match object is not required: the match is stored as `duels` with `source_type = 'challenge'`.
- A separate `challenge_request_time_options` table is not created for MVP: the three current time options are stored by `time_option_1_utc`, `time_option_2_utc` and `time_option_3_utc` fields in `challenge_requests`.
- A separate availability-window table is not created for MVP: up to three start/end pairs are stored directly in `challenge_period_players`.
- A separate table `challenge_notifications` is not created: Challenge notifications are stored in a universal table `notifications` with `domain = 'challenge'`.
- `games` does not require new Challenge fields and is linked via the existing `games.duel_id`.
- The team table `matches` is not used for Challenges.

## 22. Implementation tracker

This section tracks large technical blocks. Detailed readiness is determined by the requirement checkboxes above.

- [ ] **CH-IMP-001** Database schema and migrations.
- [ ] **CH-IMP-002** Period API and admin UI.
- [ ] **CH-IMP-003** Player-period status API and UI.
- [ ] **CH-IMP-004** Eligibility and list of available opponents.
- [ ] **CH-IMP-005** Request API and player UI.
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
- [x] **CH-IMP-016** Player availability time windows: DB fields, API, audit, desktop/mobile calendar, and opponent display.
- [x] **CH-IMP-017** Public Challenges preview and sign-in/BGA-verification onboarding before mutating actions.
- [x] **CH-IMP-018** Global-admin mode on the Challenges page: all requests and matches, read-only presentation, optional inclusion of soft-deleted records, and backend access control.
- [x] **CH-IMP-019** `tournament_cases` schema and the player `Resolve` flow for an `Error` Challenge match without a result, including mutual cancellation and no-show case creation.
- [x] **CH-IMP-020** Multi-match refactor: remove `challenge_duel_id`, make player status independent, add derived counters and N-match eligibility, make auto-cancel conditional, and list all matches.
- [x] **CH-IMP-021** Configurable pending-request limit, Rivals-wide opponent uniqueness, schedule-conflict checks, and the `Available opponents` / `All players` toggle.

## 23. Outside of MVP

- Calculation and updating of the rating.
- Implementation or change of the mechanism of automatic import of BGA results.
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
- admin UI for listing, assigning, reviewing, and closing `tournament_cases`;
- sanctions for abuse;
- participation history and Challenges statistics;
- reminders and advanced notification channels.
