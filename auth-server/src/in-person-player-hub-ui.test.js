import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const menuHtml = readFileSync(
  new URL("../../gg-html/player-hub/player-hub-menu.html", import.meta.url),
  "utf8"
);
const hubHtml = readFileSync(
  new URL("../../gg-html/player-hub/player-hub.html", import.meta.url),
  "utf8"
);
const inPersonHtml = readFileSync(
  new URL("../../gg-html/player-hub/in-person.html", import.meta.url),
  "utf8"
);

function assertEmbeddedScriptsParse(html, label) {
  const scripts = [...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/gi)];
  assert.ok(scripts.length > 0, `${label} must contain a script`);
  scripts.forEach((match) => assert.doesNotThrow(() => new Function(match[1])));
}

test("Player Hub exposes managed sections to their assigned admins", () => {
  [menuHtml, hubHtml].forEach((html) => {
    assert.match(html, /label: "In-Person"/);
    assert.match(html, /\/in-person-tournaments\/accessible/);
    assert.match(html, /item\.view === "in-person".*isAdminUser \|\| inPersonTournaments\.length > 0/s);
    assert.match(html, /scope=my-tournaments/);
    assert.match(html, /item\.view === "my-tournaments"[\s\S]*?return hasTournamentAdminAccess/);
    assert.match(html, /item\.view === "nationals"[\s\S]*?return isAdminUser/);
  });
});

test("In-Person page contains participant registration and check-in flows", () => {
  [
    "In-Person Tournaments",
    "Add player",
    "Add test players",
    "Test check-in",
    "TEST TOURNAMENT",
    "Reset data",
    "+ Add new city",
    "City name (English) *",
    "City name (local)",
    "City icon URL",
    "Country / association",
    "Current Elo",
    "Average Elo",
    "Peak Elo",
    "Number of games",
    "Player description",
    "Player photo",
    "Select image",
    "Remove photo",
    "Choose image",
    "Check-in and draw numbers",
    "Possible duplicate:",
  ].forEach((text) => assert.ok(inPersonHtml.includes(text), `missing In-Person UI text: ${text}`));

  assert.match(inPersonHtml, /\/participants/);
  assert.match(inPersonHtml, /\/participant-cities/);
  assert.match(inPersonHtml, /method: "POST"/);
  assert.match(inPersonHtml, /function createCityPickerField/);
  assert.match(inPersonHtml, /function createAssociationPickerField/);
  assert.match(inPersonHtml, /Type to filter associations\.\.\./);
  assert.match(inPersonHtml, /Select association/);
  assert.match(inPersonHtml, /flag\.className = "ip-association-flag"/);
  assert.match(inPersonHtml, /const nameLocal = tournament\.scope === "local" \? createField\("Name \(local\)"\) : null/);
  assert.match(inPersonHtml, /if \(nameLocal\) form\.appendChild\(nameLocal\.field\)/);
  assert.match(inPersonHtml, /tournament\?\.scope === "international"[\s\S]*?participant\.name_en/);
  assert.match(inPersonHtml, /function playerMeta\(participant, tournament\)/);
  assert.match(inPersonHtml, /function createParticipantListPerson\(participant, tournament\)/);
  assert.match(inPersonHtml, /participant\?\.association_flag \|\| association\?\.flag/);
  assert.match(inPersonHtml, /flag\.className = "ip-participant-flag"/);
  assert.match(inPersonHtml, /if \(tournament\?\.scope === "international"\)/);
  assert.equal(
    (inPersonHtml.match(/createParticipantListPerson\(participant, tournament\)/g) || []).length,
    3,
    "the shared participant renderer must be used by Players and Check-in"
  );
  assert.match(inPersonHtml, /tournament\?\.scope === "international"[\s\S]*?\? ""[\s\S]*?participant\.city_name_local/);
  assert.doesNotMatch(inPersonHtml, /participant\.city_name_local \|\| participant\.city_name_en \|\| "—"/);
  assert.match(inPersonHtml, /current_elo: optionalInteger\(currentElo\.input\)/);
  assert.match(inPersonHtml, /average_elo: optionalInteger\(averageElo\.input\)/);
  assert.match(inPersonHtml, /max_elo: optionalInteger\(maxElo\.input\)/);
  assert.match(inPersonHtml, /number_of_games: optionalInteger\(numberOfGames\.input\)/);
  assert.match(inPersonHtml, /player_information: String\(playerInformation\.textarea\.value/);
  assert.match(inPersonHtml, /player_photo: playerPhoto/);
  assert.match(inPersonHtml, /function showImagePickerDialog/);
  assert.match(inPersonHtml, /function createPlayerImageField/);
  assert.match(inPersonHtml, /currentImageUrl = "";[\s\S]*?preview\.removeAttribute\("src"\)/);
  assert.match(inPersonHtml, /player_photo: playerPhoto/);
  assert.match(inPersonHtml, /IMAGE_UPLOAD_URL/);
  assert.match(inPersonHtml, /className = "ip-image-preview"/);
  assert.match(inPersonHtml, /function createMenuSelectField/);
  assert.match(inPersonHtml, /country\.input\.readOnly = true/);
  assert.match(inPersonHtml, /\/start-check-in/);
  assert.match(inPersonHtml, /\/check-in/);
  assert.match(inPersonHtml, /\/participants\/test-data/);
  assert.match(inPersonHtml, /\/check-in\/test-data/);
  assert.match(inPersonHtml, /\/reset-test-data/);
  assert.match(inPersonHtml, /function openTestQuantityModal/);
  assert.match(inPersonHtml, /function openResetTestDataModal/);
  assert.match(inPersonHtml, /class="ip-btn test-action ip-hidden"/);
  assert.match(inPersonHtml, /class="ip-btn test-reset ip-hidden"/);
  assert.match(inPersonHtml, /available_players/);
  assert.match(inPersonHtml, /confirm_duplicate: confirmDuplicate/);
  assert.match(inPersonHtml, /function openCheckInModal/);
  assert.match(inPersonHtml, /checkedIn\.checked = true/);
  assert.match(inPersonHtml, /DRAW_NUMBER_TAKEN/);
  assert.match(inPersonHtml, /ip-draw-number-display/);
  assert.match(inPersonHtml, /ip-draw-number::-webkit-inner-spin-button/);
  assert.match(
    inPersonHtml,
    /function preventNumberInputWheelChange\(event\)[\s\S]*?input instanceof HTMLInputElement[\s\S]*?input\.type === "number"[\s\S]*?document\.activeElement === input[\s\S]*?event\.preventDefault\(\)/
  );
  assert.match(
    inPersonHtml,
    /document\.addEventListener\("wheel", preventNumberInputWheelChange, \{ passive: false \}\)/
  );
  assert.doesNotMatch(inPersonHtml, /Gaps and a missing #1 are allowed\./);
  assert.match(inPersonHtml, /@media \(max-width: 720px\)/);
  assert.match(inPersonHtml, /min-height: 44px/);
  const playerPanelMarkup = inPersonHtml.slice(
    inPersonHtml.indexOf('id="ipPlayersPanel"'),
    inPersonHtml.indexOf('id="ipCheckInPanel"')
  );
  const checkInPanelMarkup = inPersonHtml.slice(
    inPersonHtml.indexOf('id="ipCheckInPanel"'),
    inPersonHtml.indexOf('id="ipSwissPanel"')
  );
  const swissPanelMarkup = inPersonHtml.slice(
    inPersonHtml.indexOf('id="ipSwissPanel"'),
    inPersonHtml.indexOf('id="ipStandingsPanel"')
  );
  assert.match(playerPanelMarkup, /id="ipCounters"/);
  assert.match(checkInPanelMarkup, /id="ipCheckInCounters"/);
  assert.doesNotMatch(checkInPanelMarkup, /ipSwissReadiness|ip-readiness/);
  assert.match(swissPanelMarkup, /id="ipSwissReadiness"/);
  assert.match(inPersonHtml, /function renderSwiss\(\)[\s\S]*?renderReadiness\(\)/);
  assert.doesNotMatch(inPersonHtml, /Ready to form the first Swiss round\./);
  assert.equal((inPersonHtml.match(/id="ipCounters"/g) || []).length, 1);
  assert.equal((inPersonHtml.match(/id="ipCheckInCounters"/g) || []).length, 1);
  assert.match(inPersonHtml, /\[countersEl, checkInCountersEl\]\.forEach/);
});

test("In-Person page lists accessible tournaments and opens a dedicated tournament view", () => {
  assert.match(inPersonHtml, /id="ipTournamentList" class="ip-tournament-list"/);
  assert.match(inPersonHtml, /id="ipPageHeader" class="ip-head"/);
  assert.match(inPersonHtml, /function renderTournamentList\(\)/);
  assert.match(inPersonHtml, /pageHeader\.classList\.toggle\("ip-hidden", isDetailView\)/);
  assert.match(inPersonHtml, /row\.className = "ip-tournament-list-row"/);
  assert.match(inPersonHtml, /logo\.className = "ip-tournament-list-logo"/);
  assert.match(inPersonHtml, /logo\.src = tournament\.logo_url/);
  assert.match(inPersonHtml, /view\.textContent = "View"/);
  assert.match(inPersonHtml, /viewUrl\.searchParams\.set\("tournament", tournament\.id\)/);
  assert.match(inPersonHtml, /view\.href = viewUrl\.toString\(\)/);
  assert.match(inPersonHtml, /id="ipTournamentName"/);
  assert.doesNotMatch(inPersonHtml, /Switch tournament/);
  assert.doesNotMatch(inPersonHtml, /id="ipTournamentSwitcher"|id="ipTournamentSwitchBtn"|id="ipTournamentMenu"/);
  assert.doesNotMatch(inPersonHtml, /id="ipTournamentSelect"/);
  assert.match(
    inPersonHtml,
    /state\.selectedTournamentId = state\.tournaments\.some\(\(item\) => item\.id === requestedId\)[\s\S]*?\? requestedId[\s\S]*?: "";/
  );
  assert.match(inPersonHtml, /`\$\{tournament\.swiss_rounds_count\} Swiss rounds`/);
  assert.match(inPersonHtml, /tournament\.playoff_preview\?\.participant_count/);
  assert.match(inPersonHtml, /players advance to playoff/);
  const tournamentCardMarkup = inPersonHtml.slice(
    inPersonHtml.indexOf('id="ipTournamentCard"'),
    inPersonHtml.indexOf('id="ipStatus"')
  );
  assert.match(tournamentCardMarkup, /id="ipTournamentLogo"/);
  assert.match(tournamentCardMarkup, /id="ipTournamentName"/);
  assert.match(tournamentCardMarkup, /id="ipFullViewBtn"[\s\S]*?aria-expanded="false"[\s\S]*?>Full View<\/button>/);
  assert.match(tournamentCardMarkup, /id="ipRefreshBtn"/);
  assert.match(tournamentCardMarkup, /id="ipPlayoffCompletion"/);
  const tournamentDetailMarkup = inPersonHtml.slice(
    inPersonHtml.indexOf('id="ipTournamentDetail"'),
    inPersonHtml.indexOf('id="ipTournamentList"')
  );
  assert.match(tournamentDetailMarkup, /id="ipTournamentName"/);
  assert.match(tournamentDetailMarkup, /data-ip-tab="players"/);
  assert.match(tournamentDetailMarkup, /data-ip-tab="check-in"/);
  assert.match(tournamentDetailMarkup, /data-ip-tab="swiss"/);
  assert.match(tournamentDetailMarkup, /data-ip-tab="standings"/);
  assert.match(tournamentDetailMarkup, /data-ip-tab="playoff"/);
  assert.match(inPersonHtml, /function openTournamentFullView\(\)/);
  assert.match(inPersonHtml, /content\.appendChild\(tournamentDetail\)/);
  assert.match(inPersonHtml, /fullViewBtn\.addEventListener\("click", \(\) => \{[\s\S]*?if \(closeTournamentFullView\) closeTournamentFullView\(\);[\s\S]*?else openTournamentFullView\(\);/);
  assert.match(inPersonHtml, /fullViewBtn\.textContent = "Close Full View"/);
  assert.match(inPersonHtml, /fullViewBtn\.textContent = "Full View"/);
  assert.match(inPersonHtml, /\.ip-fullview-overlay\s*\{[\s\S]*?position: fixed;[\s\S]*?inset: 0;/);
  assert.doesNotMatch(inPersonHtml, /--ip-site-menu-height/);
  assert.match(inPersonHtml, /\.ip-fullview-overlay\s*\{[\s\S]*?z-index: 2147483000;/);
  assert.match(inPersonHtml, /\.ip-fullview-overlay\s*\{[\s\S]*?padding: 12px;/);
  assert.match(inPersonHtml, /\.ip-fullview-close\s*\{[\s\S]*?top: 20px;/);
  assert.doesNotMatch(inPersonHtml, /\.ip-fullview-content \.ip-fullview-btn\s*\{[\s\S]*?display: none;/);
  assert.ok(
    inPersonHtml.indexOf('id="ipTournamentCard"') < inPersonHtml.indexOf('id="ipStatus"'),
    "the selected tournament card must be above status and workspace content"
  );
  assert.ok(
    inPersonHtml.indexOf('id="ipPlayoffCompletion"') < inPersonHtml.indexOf('id="ipWorkspace"'),
    "final placements must be rendered above the tabbed workspace"
  );
  assert.equal((inPersonHtml.match(/id="ipPlayoffCompletion"/g) || []).length, 1);
  const tournamentMetaRenderer = inPersonHtml.slice(
    inPersonHtml.indexOf("function renderTournamentMeta()"),
    inPersonHtml.indexOf("function renderCounters()")
  );
  assert.match(tournamentMetaRenderer, /tournament\.logo_url/);
  assert.doesNotMatch(tournamentMetaRenderer, /tournament\.status|tournament\.scope|formatDates\(tournament\)/);

  const tournamentListRenderer = inPersonHtml.slice(
    inPersonHtml.indexOf("function renderTournamentList()"),
    inPersonHtml.indexOf("function renderTournamentMeta()")
  );
  const listBadgeValues = tournamentListRenderer.slice(
    tournamentListRenderer.indexOf('meta.className = "ip-tournament-list-meta"'),
    tournamentListRenderer.indexOf("].filter(Boolean).forEach")
  );
  assert.doesNotMatch(listBadgeValues, /tournament\.id/);
  assert.match(listBadgeValues, /formatDates\(tournament\)[\s\S]*?tournament\.scope[\s\S]*?tournament\.status/);
});

test("In-Person page contains the complete Swiss organizer workflow", () => {
  [
    "Swiss rounds",
    "Preview first round",
    "Confirm and publish",
    "Publish round",
    "Save result",
    "Complete round",
    "Undo complete round",
    "Swiss standings",
    "Solkoff1",
    "Solkoff2",
    "VP difference",
  ].forEach((text) => assert.ok(inPersonHtml.includes(text), `missing Swiss UI text: ${text}`));

  [
    /\/swiss"/,
    /\/swiss\/rounds\/preview/,
    /\/swiss\/rounds\/confirm/,
    /\/swiss\/rounds\/\$\{encodeURIComponent\(round\.id\)\}\/test-results/,
    /\/publish`/,
    /\/result`/,
    /\/complete`/,
    /\/reopen`/,
  ].forEach((pattern) => assert.match(inPersonHtml, pattern));
  assert.match(inPersonHtml, /data-ip-tab="swiss"/);
  assert.match(inPersonHtml, /data-ip-tab="standings"/);
  assert.match(inPersonHtml, /"Auto-fill test results"[\s\S]*?"ip-btn test-action"/);
  assert.match(inPersonHtml, /is_test_tournament === true[\s\S]*?round\?\.status === "published"/);
  assert.match(inPersonHtml, /function createSwissTableCard/);
  assert.match(inPersonHtml, /function openSwissResultModal/);
  assert.match(inPersonHtml, /function createSwissResultForm/);
  assert.match(inPersonHtml, /function createMenuSelectField/);
  assert.match(inPersonHtml, /blue-mipple-no-bg-small\.png/);
  assert.match(inPersonHtml, /ip-swiss-table-card\.completed/);
  assert.match(inPersonHtml, /\.ip-swiss-table-head\s*\{[\s\S]*?display: flex;[\s\S]*?justify-content: space-between;/);
  assert.match(inPersonHtml, /\.ip-swiss-table-number\s*\{[\s\S]*?font-size: 20px;[\s\S]*?font-weight: 700;/);
  assert.match(inPersonHtml, /\.ip-swiss-table-admin-note\s*\{[\s\S]*?font-size: 9px;[\s\S]*?text-align: right;[\s\S]*?text-overflow: ellipsis;[\s\S]*?white-space: nowrap;/);
  assert.match(inPersonHtml, /table\.textContent = match\.is_bye \? "Bye" : String\(match\.table_number\)/);
  assert.match(inPersonHtml, /\.ip-starting-player-icon\s*\{[\s\S]*?width: 12px;[\s\S]*?height: 12px;[\s\S]*?display: inline-block;[\s\S]*?margin-right: 6px;/);
  assert.match(inPersonHtml, /\.ip-swiss-table-player-name\s*\{[\s\S]*?max-width: 100%;[\s\S]*?font-size: 16px;[\s\S]*?overflow-wrap: anywhere;[\s\S]*?text-align: center;[\s\S]*?white-space: normal;/);
  assert.match(inPersonHtml, /\.ip-swiss-table-player-flag\s*\{[\s\S]*?width: 21px;[\s\S]*?height: 14px;/);
  assert.match(inPersonHtml, /return participant\?\.name_local[\s\S]*?participant_\$\{side\}_name_local[\s\S]*?participant\?\.name_en/);
  const swissTablePlayerRenderer = inPersonHtml.slice(
    inPersonHtml.indexOf("function createSwissTablePlayer"),
    inPersonHtml.indexOf("function createSwissTableCard")
  );
  assert.ok(
    swissTablePlayerRenderer.indexOf("name.appendChild(starter)")
      < swissTablePlayerRenderer.indexOf("name.appendChild(document.createTextNode"),
    "the starting-player icon must be inline immediately before the player name"
  );
  assert.match(swissTablePlayerRenderer, /tournament\?\.scope === "international"/);
  assert.match(swissTablePlayerRenderer, /participantAssociation\(participant\)/);
  assert.match(swissTablePlayerRenderer, /if \(side === "a"\) player\.appendChild\(flag\)/);
  assert.match(swissTablePlayerRenderer, /if \(flag && side === "b"\) player\.appendChild\(flag\)/);
  const swissTableCardRenderer = inPersonHtml.slice(
    inPersonHtml.indexOf("function createSwissTableCard"),
    inPersonHtml.indexOf("function createPairingCard")
  );
  assert.match(swissTableCardRenderer, /const adminNoteText = String\(match\.admin_note \|\| ""\)\.trim\(\)/);
  assert.match(swissTableCardRenderer, /adminNote\.className = "ip-swiss-table-admin-note"/);
  assert.match(swissTableCardRenderer, /adminNote\.textContent = adminNoteText/);
  assert.match(swissTableCardRenderer, /adminNote\.title = adminNoteText/);
  const swissResultModal = inPersonHtml.slice(
    inPersonHtml.indexOf("function openSwissResultModal"),
    inPersonHtml.indexOf("function createSwissResultForm")
  );
  assert.doesNotMatch(swissResultModal, /players\.textContent/);
  const swissResultForm = inPersonHtml.slice(
    inPersonHtml.indexOf("function createSwissResultForm"),
    inPersonHtml.indexOf("function createResultForm")
  );
  [
    '"Starting player:"',
    '"Time lost:"',
    'won.textContent = "Won"',
    'createButton("Add admin note"',
    'result_type: "points"',
    'result_type: "time_forfeit"',
  ].forEach((text) => assert.ok(swissResultForm.includes(text), `missing Swiss score form text: ${text}`));
  assert.doesNotMatch(swissResultForm, /"Result type"|"Win \/ loss"|"Technical reason"/);
  assert.match(inPersonHtml, /\.ip-swiss-score-input\s*\{[\s\S]*?width: 112px;[\s\S]*?height: 112px;[\s\S]*?font-size: 36px;/);
  assert.match(inPersonHtml, /\.ip-swiss-score-player-name\s*\{[\s\S]*?font-weight: 500;/);
  assert.match(inPersonHtml, /\.ip-swiss-score-won\s*\{[\s\S]*?font-size: 15px;/);
  assert.doesNotMatch(inPersonHtml, /\.ip-admin-note-toggle\s*\{[\s\S]*?text-decoration:\s*underline/);
  assert.match(swissResultForm, /timeLost\.onChange\(\(\) => syncForm\(\)\)/);
  assert.match(swissResultForm, /save\.disabled = saving \|\| !starter\.getValue\(\)/);
  assert.match(swissResultForm, /if \(hasScoreA !== hasScoreB\)/);
  assert.match(swissResultForm, /Enter a score for both players or leave both scores empty\./);
  assert.match(swissResultForm, /Enter scores for both players when selecting Time lost\./);
  assert.match(swissResultForm, /const starterOnly = !hasScoreA[\s\S]*?&& !hasScoreB/);
  assert.match(swissResultForm, /starterOnly[\s\S]*?starting_participant_id: starter\.getValue\(\)/);
  assert.match(swissResultForm, /match\.status === "completed"[\s\S]*?"Reset result"/);
  assert.match(swissResultForm, /\{ method: "DELETE" \}/);
  assert.doesNotMatch(swissResultForm, /setForfeitScore|scoreLocked/);
  assert.match(swissResultForm, /result_type: "time_forfeit",[\s\S]*?points_a: Number\(scoreA\.input\.value\),[\s\S]*?points_b: Number\(scoreB\.input\.value\)/);
  assert.ok(
    swissResultForm.indexOf("scoreGrid.appendChild(scoreA.input)")
      < swissResultForm.indexOf("scoreGrid.appendChild(scoreA.won)"),
    "Won must be rendered below the score input"
  );
  const menuSelectField = inPersonHtml.slice(
    inPersonHtml.indexOf("function createMenuSelectField"),
    inPersonHtml.indexOf("function appendAssociationLabel")
  );
  assert.match(menuSelectField, /ip-city-picker-btn/);
  assert.match(menuSelectField, /ip-city-picker-menu/);
  assert.doesNotMatch(menuSelectField, /searchInput|ip-city-search/);
  assert.match(inPersonHtml, /ip-time-lost-icon/);
  assert.match(inPersonHtml, /round\.status === "published"[\s\S]*openSwissResultModal\(match\)/);
  assert.doesNotMatch(inPersonHtml, /Preview pairings, publish a round, then enter every table result\./);
  assert.match(inPersonHtml, /if \(warningCount > 0\)/);
  const swissPreviewRenderer = inPersonHtml.slice(
    inPersonHtml.indexOf("function renderSwissPreview()"),
    inPersonHtml.indexOf("function renderSwissMatches()")
  );
  assert.ok(
    swissPreviewRenderer.indexOf("swissPreview.appendChild(actions)")
      < swissPreviewRenderer.indexOf("(preview.matches || []).forEach"),
    "Swiss preview actions must be rendered above the tables"
  );
  assert.match(swissPreviewRenderer, /createSwissTableCard\(match, \{ showRecord: true \}\)/);
  assert.match(inPersonHtml, /function swissParticipantRecord\(participantId\)[\s\S]*?record\.wins \+= 1[\s\S]*?record\.losses \+= 1/);
  assert.match(inPersonHtml, /recordEl\.textContent = `\$\{record\.wins\} – \$\{record\.losses\}`/);
  assert.match(inPersonHtml, /recordEl\.title = "Wins – losses"/);
  assert.match(inPersonHtml, /\.ip-standings-table\s*\{[\s\S]*?font-size: 14px;/);
  assert.match(inPersonHtml, /\.ip-standings-table thead th\s*\{[\s\S]*?font-size: 12px;/);
  assert.match(inPersonHtml, /\.ip-standings-player-flag\s*\{[\s\S]*?width: 21px;[\s\S]*?height: 14px;/);
  const standingsRenderer = inPersonHtml.slice(
    inPersonHtml.indexOf("function renderStandings()"),
    inPersonHtml.indexOf("const PLAYOFF_MAIN_ROUNDS")
  );
  assert.match(standingsRenderer, /tournament\?\.scope === "international"/);
  assert.match(standingsRenderer, /participantAssociation\(participant\)/);
  assert.match(standingsRenderer, /flag\.className = "ip-standings-player-flag"/);
  assert.match(standingsRenderer, /playerName\.textContent = standing\.participant_name_en/);
  assert.match(inPersonHtml, /round\.progress\.completed === round\.progress\.total/);
  const completeRoundFlow = inPersonHtml.slice(
    inPersonHtml.indexOf("async function completeSwissRound"),
    inPersonHtml.indexOf("function cancellationResultLabel")
  );
  assert.doesNotMatch(completeRoundFlow, /window\.confirm/);
  assert.match(
    completeRoundFlow,
    /adoptSwissResponse\(data\);[\s\S]*?if \(data\.swiss_complete\)[\s\S]*?tournamentUrl\("\/playoff"\)[\s\S]*?adoptPlayoffResponse\(playoffData\);[\s\S]*?renderWorkspace\(\);/
  );
  assert.match(inPersonHtml, /function swissTableProgress\(round\)[\s\S]*?filter\(\(match\) => !match\.is_bye\)/);
  assert.match(inPersonHtml, /const rounds = state\.swiss\?\.rounds \|\| \[\];[\s\S]*?rounds\.forEach\(\(round\) =>/);
  const swissMatchesRenderer = inPersonHtml.slice(
    inPersonHtml.indexOf("function renderSwissMatches()"),
    inPersonHtml.indexOf("function renderSwiss()")
  );
  assert.doesNotMatch(swissMatchesRenderer, /progress\.textContent|heading\.appendChild\(progress\)/);
  assert.doesNotMatch(swissMatchesRenderer, /showRecord: true/);
  assert.match(swissMatchesRenderer, /const isCompletedRound = round\.status === "completed"/);
  assert.match(swissMatchesRenderer, /content\.hidden = true/);
  assert.match(swissMatchesRenderer, /className = "ip-swiss-round-toggle"/);
  assert.match(swissMatchesRenderer, /toggle\.setAttribute\("aria-expanded", "false"\)/);
  assert.match(swissMatchesRenderer, /content\.hidden = !expanded/);
  assert.match(
    swissMatchesRenderer,
    /\.sort\(\(left, right\) => Number\(right\.round_number\) - Number\(left\.round_number\)\)/
  );
  assert.match(
    swissMatchesRenderer,
    /Number\(left\.table_number \?\? Number\.MAX_SAFE_INTEGER\)[\s\S]*?- Number\(right\.table_number \?\? Number\.MAX_SAFE_INTEGER\)/
  );
  assert.match(inPersonHtml, /round_number: preview\.round_number, publish: true/);
  assert.doesNotMatch(inPersonHtml, /ipStandingsMeta|updated after the latest completed round/);
});

test("participant row actions are moved into the Edit form", () => {
  const listRenderer = inPersonHtml.slice(
    inPersonHtml.indexOf("function renderPlayers()"),
    inPersonHtml.indexOf("function openPlayerForm")
  );
  const editForm = inPersonHtml.slice(
    inPersonHtml.indexOf("function openPlayerForm"),
    inPersonHtml.indexOf("async function deletePlayer")
  );
  ["Withdraw", "Disqualify", "Delete"].forEach((label) => {
    assert.equal(listRenderer.includes(`\"${label}\"`), false, `${label} must not be in the player row`);
    assert.equal(editForm.includes(`\"${label}\"`), true, `${label} must be in the Edit form`);
  });
});

test("the add-player form disables its launcher and keeps primary actions at the top", () => {
  const addForm = inPersonHtml.slice(
    inPersonHtml.indexOf("function openPlayerForm"),
    inPersonHtml.indexOf("async function deletePlayer")
  );
  assert.match(addForm, /addPlayerBtn\.disabled = !participant \|\| playerCreationDisabled\(tournament\)/);
  assert.match(addForm, /else form\.insertBefore\(actions, form\.firstChild\)/);
  assert.match(addForm, /addPlayerBtn\.disabled = playerCreationDisabled\(tournament\)/);
});

test("In-Person page exposes Swiss rollback, inactive-player and late-entry recovery", () => {
  [
    "Add late player",
    "Preview late entry",
    "Confirm late entry",
    "will be paired with the current bye recipient",
    "Cancel round",
    "Draft-round no-show",
    "Withdraw",
    "Disqualify",
    "no-show",
    "These",
    "results will stop counting",
  ].forEach((text) => assert.ok(inPersonHtml.includes(text), `missing recovery UI text: ${text}`));

  [
    /\/participants\/late\/preview/,
    /\/participants\/late\/confirm/,
    /\/participants\/\$\{encodeURIComponent\(participant\.id\)\}\/status/,
    /\/cancellation-preview/,
    /\/cancel`/,
    /finish_reason: "no_show"/,
  ].forEach((pattern) => assert.match(inPersonHtml, pattern));
  assert.doesNotMatch(inPersonHtml, /Late-entry mode \*/);
  const cancellationFlow = inPersonHtml.slice(
    inPersonHtml.indexOf("function openSwissRoundCancellationModal"),
    inPersonHtml.indexOf("async function recordNoShow")
  );
  assert.match(cancellationFlow, /ip-modal-overlay/);
  assert.match(cancellationFlow, /ip-cancellation-results/);
  assert.match(cancellationFlow, /Cancelled by tournament organizer/);
  assert.doesNotMatch(cancellationFlow, /window\.(prompt|confirm)/);
});

test("In-Person page uses an interactive playoff bracket and result modal", () => {
  [
    "Single-elimination playoff",
    "Select both players for this first-round match.",
    "Starting player",
    "Save players",
    "Auto-seed",
    "Start playoff",
    "Back to Swiss",
    "Reset playoff bracket",
    "Auto-fill test results",
    "Publish medal round",
    "Click a published match to enter or correct its result.",
    "Bronze medal match",
    "Complete tournament",
    "Reset result",
  ].forEach((text) => assert.ok(inPersonHtml.includes(text), `missing playoff UI text: ${text}`));

  [
    /data-ip-tab="playoff"/,
    /function buildEmptyPlayoffRounds/,
    /const persistedRounds = state\.playoff\?\.rounds \|\| \[\]/,
    /function playoffSetupParticipantName/,
    /`#\$\{position\} • \$\{name\}`/,
    /function openPlayoffParticipantModal/,
    /function playoffSetupStartingAssignments/,
    /Players become available after every Swiss round is complete/,
    /Player 1 placeholder/,
    /function playoffSetupTableAssignments/,
    /table_numbers: tableNumbers/,
    /starting_participants: startingParticipants/,
    /Higher-ranked players start/,
    /positionA <= positionB \? participantAId : participantBId/,
    /\/playoff\/matches\/\$\{encodeURIComponent\(match\.id\)\}\/placeholders/,
    /swissPosition\(left\.id\) - swissPosition\(right\.id\)/,
    /function playoffSeedOrder/,
    /function seedPlayoffFromSwiss/,
    /function playoffTableNumber/,
    /function returnPlayoffSetupToSwiss/,
    /function resetPlayoffBracket/,
    /function layoutPlayoffRounds/,
    /function appendPlayoffConnector/,
    /className = "ip-playoff-connector"/,
    /\/playoff\/preview/,
    /\/playoff\/confirm/,
    /\/playoff\/reset/,
    /\/playoff\/test-results/,
    /\/playoff\/test-results\/reset/,
    /\/playoff\/rounds\/\$\{encodeURIComponent\(round\.id\)\}\/publish/,
    /function openPlayoffResultModal/,
    /stage: "playoff"/,
    /tournamentUrl\(`\/\$\{stage\}\/matches\/\$\{encodeURIComponent\(match\.id\)\}\/result`\)/,
    /\/playoff\/matches\/\$\{encodeURIComponent\(match\.id\)\}\/table/,
    /\/playoff\/complete/,
  ].forEach((pattern) => assert.match(inPersonHtml, pattern));

  assert.doesNotMatch(inPersonHtml, /function createPlayoffTableEditor|Save table/);

  assert.doesNotMatch(inPersonHtml, /Swiss stage \d+(?:st|nd|rd|th)(?: place)?/);
  assert.doesNotMatch(inPersonHtml, /window\.localStorage\.(?:getItem|setItem|removeItem)/);

  assert.match(
    inPersonHtml,
    /\.ip-playoff-bracket-scroll\s*\{[\s\S]*?width: 100%;[\s\S]*?min-width: 0;[\s\S]*?max-width: 100%;[\s\S]*?overflow-x: auto;/
  );

  [
    "Fill every first-round slot manually, then run Final and Bronze medal match.",
    "First-round slots",
    "Playoff state",
    "Manual ${playoff.participant_count}-player bracket setup",
  ].forEach((text) => assert.ok(!inPersonHtml.includes(text), `obsolete playoff UI text: ${text}`));
  assert.ok(!inPersonHtml.includes("Click a first-round match to select its players."));
  assert.ok(!inPersonHtml.includes("Complete every configured Swiss round to open the playoff bracket."));
  assert.ok(!inPersonHtml.includes("Tournament cannot be completed yet."));
  assert.ok(!inPersonHtml.includes("Final must be completed."));
  assert.ok(!inPersonHtml.includes("Bronze medal match must be completed."));
  assert.doesNotMatch(inPersonHtml, /id="ipPlayoffSummary"|id="ipPlayoffSetup"|id="ipPlayoffPreview"/);

  const seedOrderSource = inPersonHtml.slice(
    inPersonHtml.indexOf("function playoffSeedOrder"),
    inPersonHtml.indexOf("function seedPlayoffFromSwiss")
  );
  const playoffSeedOrder = new Function(`${seedOrderSource}; return playoffSeedOrder;`)();
  assert.deepEqual(playoffSeedOrder(4), [1, 4, 2, 3]);
  assert.deepEqual(playoffSeedOrder(8), [1, 8, 4, 5, 2, 7, 3, 6]);
  assert.deepEqual(playoffSeedOrder(16), [
    1, 16, 8, 9, 4, 13, 5, 12, 2, 15, 7, 10, 3, 14, 6, 11,
  ]);
  assert.deepEqual(playoffSeedOrder(32), [
    1, 32, 16, 17, 8, 25, 9, 24, 4, 29, 13, 20, 5, 28, 12, 21,
    2, 31, 15, 18, 7, 26, 10, 23, 3, 30, 14, 19, 6, 27, 11, 22,
  ]);

  const participantModal = inPersonHtml.slice(
    inPersonHtml.indexOf("function openPlayoffParticipantModal"),
    inPersonHtml.indexOf("async function startPlayoffFromBracket")
  );
  assert.doesNotMatch(participantModal, /!participantAId \|\| !participantBId/);
  assert.doesNotMatch(participantModal, /error\.textContent = "Select both players\."/);
  assert.match(participantModal, /participantAId && participantAId === participantBId/);

  const bracketMatchRenderer = inPersonHtml.slice(
    inPersonHtml.indexOf("function createPlayoffBracketMatch"),
    inPersonHtml.indexOf("function layoutPlayoffRounds")
  );
  assert.match(
    bracketMatchRenderer,
    /isFirstRoundSetupMatch[\s\S]*?playoffSetupParticipantName\(participantId\)/
  );
  assert.match(bracketMatchRenderer, /participantAssociation\(participant\)/);
  assert.match(bracketMatchRenderer, /flag\.className = "ip-playoff-player-flag"/);
  assert.ok(
    bracketMatchRenderer.indexOf("name.appendChild(flag)")
      < bracketMatchRenderer.indexOf("name.appendChild(nameText)")
      && bracketMatchRenderer.indexOf("name.appendChild(nameText)")
        < bracketMatchRenderer.indexOf("name.appendChild(starter)"),
    "the playoff flag must be left of the name and the starting-player icon must be right of it"
  );
  assert.match(
    inPersonHtml,
    /\.ip-playoff-match-placeholder\s*\{[\s\S]*?font-size: 11px;[\s\S]*?font-style: italic;/
  );

  const playoffLayout = inPersonHtml.slice(
    inPersonHtml.indexOf("function layoutPlayoffRounds"),
    inPersonHtml.indexOf("function appendPlayoffConnector")
  );
  assert.match(
    playoffLayout,
    /table_number: playoffTableNumber\(round\.round_key, match\.table_number\)/
  );

  const resultModal = inPersonHtml.slice(
    inPersonHtml.indexOf("function openMatchResultModal"),
    inPersonHtml.indexOf("function openSwissResultModal")
  );
  assert.match(resultModal, /createSwissResultForm\(match, \{[\s\S]*?stage,[\s\S]*?round,/);
  const sharedResultForm = inPersonHtml.slice(
    inPersonHtml.indexOf("function createSwissResultForm"),
    inPersonHtml.indexOf("function createResultForm")
  );
  assert.match(sharedResultForm, /stage === "playoff" && round[\s\S]*?createField\("Table", "number"\)/);
  assert.match(sharedResultForm, /tableChanged[\s\S]*?method: "PATCH"[\s\S]*?method: "PUT"/);
  assert.match(sharedResultForm, /const starterOnly = !hasScoreA[\s\S]*?starting_participant_id: starter\.getValue\(\)/);
  const playoffActionsRenderer = inPersonHtml.slice(
    inPersonHtml.indexOf("function renderPlayoffActions"),
    inPersonHtml.indexOf("function renderPlayoffBracket")
  );
  assert.match(playoffActionsRenderer, /new Set\(\["final", "bronze_medal_match"\]\)/);
  assert.match(playoffActionsRenderer, /"Publish medal round"/);
  assert.match(playoffActionsRenderer, /state\.swiss\?\.can_reopen_current_round/);
  assert.match(playoffActionsRenderer, /playoff\.can_reset/);
  assert.match(
    playoffActionsRenderer,
    /is_test_tournament === true[\s\S]*?!playoff\.can_complete[\s\S]*?"ip-btn test-action"/
  );
  assert.match(inPersonHtml, /Array\.isArray\(data\.participant_ids\)/);
});

test("all Player Hub scripts parse after the In-Person additions", () => {
  assertEmbeddedScriptsParse(menuHtml, "Player Hub menu");
  assertEmbeddedScriptsParse(hubHtml, "Player Hub landing");
  assertEmbeddedScriptsParse(inPersonHtml, "In-Person page");
});
