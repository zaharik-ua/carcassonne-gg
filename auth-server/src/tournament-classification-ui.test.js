import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";
import { TOURNAMENT_CATEGORIES } from "./tournament-classification.js";
import { serializeCategory } from "./categories.js";

const pages = Object.fromEntries([
  ["admin", "admin.html"], ["my", "player-hub/my-tournaments.html"],
  ["hub", "player-hub/player-hub.html"], ["menu", "player-hub/player-hub-menu.html"],
  ["matches", "player-hub/tournaments.html"],
].map(([key, file]) => [key, readFileSync(new URL(`../../gg-html/${file}`, import.meta.url), "utf8")]));
const catalog = TOURNAMENT_CATEGORIES.map((entry, index) => serializeCategory({
  id: index + 1, name: entry.name, sort_order: entry.sort_order, is_tournament: 1,
}));
function pageFunction(page, name) {
  const source = pages[page];
  const start = source.search(new RegExp(`  (?:async )?function ${name}\\(`));
  assert.ok(start >= 0, `${page}.${name} must exist`);
  return source.slice(start, source.indexOf("\n  }", start) + 4);
}
function load(context, page, names) {
  for (const name of names) vm.runInContext(pageFunction(page, name), context);
}
function pageConst(page, name) {
  const source = pages[page];
  const start = source.indexOf(`  const ${name} = `);
  assert.ok(start >= 0, `${page}.${name} must exist`);
  return vm.runInNewContext(source.slice(start, source.indexOf("\n  ];", start) + 4).replace(`const ${name} =`, "(") + ")");
}

// Enough DOM to execute real form event handlers and the styled category dropdown.
class Element {
  constructor(tag = "div") { this.tagName = tag; this.children = []; this.listeners = {}; this.style = {}; this.dataset = {}; this.className = ""; }
  classList = {
    contains: (name) => this.className.split(" ").includes(name),
    add: (...names) => { this.className = [...new Set([...this.className.split(" "), ...names])].join(" ").trim(); },
    remove: (...names) => { this.className = this.className.split(" ").filter((name) => !names.includes(name)).join(" "); },
    toggle: (name, force) => {
      const enabled = force ?? !this.classList.contains(name);
      this.classList[enabled ? "add" : "remove"](name); return enabled;
    },
  };
  appendChild(child) { child.parent = this; this.children.push(child); return child; }
  set innerHTML(value) { this.children = []; this._value = undefined; this.html = value; }
  get innerHTML() { return this.html || ""; }
  set value(value) { this._value = String(value ?? ""); }
  get value() { return this._value ?? (this.tagName === "select" ? (this.children.find((child) => child.selected) || this.children[0])?.value : "") ?? ""; }
  get options() { return this.children; }
  setAttribute(name, value) { this[name] = value; }
  addEventListener(type, handler) { (this.listeners[type] ||= []).push(handler); }
  async fire(type) { for (const handler of this.listeners[type] || []) await handler({ target: this, preventDefault() {}, stopPropagation() {} }); }
  querySelectorAll(selector) {
    const selectors = selector.split(",").map((part) => part.trim());
    const result = [];
    for (const child of this.children) {
      if (selectors.some((part) => part.startsWith(".") ? child.classList.contains(part.slice(1)) : child.tagName === part)) result.push(child);
      result.push(...child.querySelectorAll(selector));
    }
    return result;
  }
  querySelector(selector) { return this.querySelectorAll(selector)[0] || null; }
  contains(element) { return element === this || this.children.some((child) => child.contains(element)); }
  remove() { this.parent.children = this.parent.children.filter((child) => child !== this); }
}

function contextForForm(page, tournament, { globalAdmin = true, mode = "edit" } = {}) {
  const fields = new Map();
  const requests = [];
  const editors = new Map();
  let form;
  const makeField = (label, value, options = {}) => {
    const input = new Element();
    input.value = value;
    input.checked = value === true;
    input.choices = (options.selectOptions || []).map((entry) => typeof entry === "object" ? entry.value : entry);
    input.setOptions = (choices, selected) => {
      input.choices = choices.map((entry) => typeof entry === "object" ? entry.value : entry);
      input.value = input.choices.includes(selected) ? selected : input.choices[0];
    };
    const row = new Element(); row.appendChild(input);
    const field = { row, input };
    fields.set(label, field);
    return field;
  };
  const makeEditor = (name, values = []) => {
    const editor = { root: new Element(), row: new Element(), getValues: () => values,
      getValidationError: () => "", onChange() {}, setDisabled(disabled) { this.disabled = disabled; },
      setOfficial(value) { this.official = value; }, setTeamType(value) { this.teamType = value; },
    };
    editor.row = editor.root;
    editors.set(name, editor);
    return editor;
  };
  const contentEl = new Element();
  const cclSections = [];
  const request = async (url, options) => {
    if (options.method !== "GET") requests.push({ url, method: options.method, payload: JSON.parse(options.body) });
    return { tournament_teams: [{ team_id: "UA", captain_id: "player" }] };
  };
  const context = vm.createContext({
    document: { createElement: (tag) => new Element(tag) }, URLSearchParams,
    state: { categories: catalog, userOptions: [], teams: [], profilesPublic: [], tournaments: [tournament] },
    ensureUserOptionsLoaded() {}, ensureCategoriesLoaded() {}, ensureTeamsLoaded() {}, ensureProfilesPublicLoaded() {},
    createTextRow: makeField, createNumberRow: makeField, createCheckboxRow: makeField,
    createTournamentFormRow: makeField,
    createTournamentAccessPickerRow: (_label, _users, entries) => makeEditor("access", entries),
    createTournamentTeamsPickerRow: () => makeEditor("teams", [{ team_id: "UA", captain_id: "player" }]),
    createTournamentAccessUsersEditor: (_users, entries) => makeEditor("access", entries),
    createTournamentTeamsEditor: () => makeEditor("teams", [{ team_id: "UA", captain_id: "player" }]),
    createTournamentStandingsEditor: () => makeEditor("standings", [{ team_id: "UA", points: 3 }]),
    createTournamentRankColorsEditor: () => makeEditor("rank", {}),
    createRivalsChallengePeriodsSection: () => { const section = new Element(); editors.set("periods", { root: section }); return section; },
    createTournamentRichTextField: (_label, html) => ({ row: new Element(), editor: { getHtml: () => html, isUploading: () => false, setDisabled() {} } }),
    toApiAbsoluteUrl: (value) => value, toDatetimeLocalFromUtc: () => "", toUtcIsoFromDatetimeLocal: () => null,
    requestTournamentJson: request, apiJson: request,
    TOURNAMENTS_URL: "/tournaments", TOURNAMENT_TEAMS_URL: "/tournament-teams", STANDINGS_URL: "/standings",
    mountDetailForm: (value) => { form = value; }, showCreateForm: (value) => { form = value; },
    sectionForm: new Element(), contentEl, closeInlineForm() {}, setStatus() {}, loadSectionData() {}, renderSection() {},
    initMyTournamentsPage() {}, tournamentEditorCache: { isGlobalAdmin: globalAdmin },
    renderMyTournamentsList() {},
    CCL_DEADLINES: page === "my" ? pageConst("my", "CCL_DEADLINES") : [],
    createCclPlayersSection: (value) => { cclSections.push(value.id); return new Element(); },
  });
  load(context, page, ["getTournamentClassification", "getTournamentCategoryValues"]);
  if (page === "admin") {
    load(context, page, ["hasTournamentCategory"]);
    for (const name of ["createSelectRow", "createSelectOptionsRow"]) {
      vm.runInContext(pageFunction(page, name), context);
      const original = context[name];
      context[name] = (...args) => { const result = original(...args); fields.set(args[0], { ...result, input: result.select }); return result; };
    }
    load(context, page, ["openTournamentForm"]);
  } else load(context, page, ["renderMyTournamentEditForm"]);
  return {
    context, fields, requests, editors, cclSections,
    async open() {
      if (page === "admin") await context.openTournamentForm(mode, mode === "create" ? null : tournament, new Element());
      else context.renderMyTournamentEditForm(tournament, { categories: catalog, icons: [], users: [], teams: [], profiles: [], tournamentTeams: [], standings: [], canManageAccessUsers: globalAdmin });
      form ||= contentEl.children[0];
    },
    submit: () => form.fire("submit"),
  };
}
const tournament = (entry, extra = {}) => ({
  id: `cup-${entry.name}`, name: entry.name, type: entry.type, category: entry.name,
  player_hub_visibility: "Hidden", lineup_size_type: 1, lineup_size: 3, team_size: 7,
  tournament_format: "1 Stage", stage1_format: "Round-robin",
  access_users: [{ user_id: 3, role: "admin" }, { user_id: 4, role: "captain" }],
  ...extra,
});

test("all independent snippets support canonical pairs and legacy National/Clubs/KoC/Friendly", () => {
  for (const page of Object.keys(pages)) {
    const context = vm.createContext({}); load(context, page, ["getTournamentClassification"]);
    for (const entry of TOURNAMENT_CATEGORIES) {
      const result = context.getTournamentClassification(tournament(entry));
      assert.equal(result.type, entry.type, `${page}: ${entry.name}`);
      assert.equal(result.category, entry.name);
      assert.equal(result.isOfficial, entry.mode === "official");
      assert.equal(result.supportsTeamTools, ["official", "friendly"].includes(entry.mode));
    }
    for (const [legacy, type, category] of [
      [{ tournament_type: "National" }, "Individuals", "National"],
      [{ tournament_type: "Clubs" }, "Teams", "Clubs"],
      [{ tournament_type: "Individuals", category: "KoC" }, "Individuals", "King of Carcassonne"],
      [{ tournament_type: "Teams", subtype: "Friendly" }, "Teams", "Friendly"],
    ]) {
      const result = context.getTournamentClassification(legacy);
      assert.equal(result.type, type); assert.equal(result.category, category);
    }
    assert.equal(context.getTournamentClassification({ type: "Teams", category: "" }).supportsTeamTools, false);
  }
});

for (const page of ["admin", "my"]) {
  test(`${page}: all eleven categories save only type/category; future/individual categories do not write team data`, async () => {
    for (const entry of TOURNAMENT_CATEGORIES) {
      const ui = contextForForm(page, tournament(entry)); await ui.open(); await ui.submit();
      const patch = ui.requests.find((request) => request.method === "PATCH");
      assert.ok(patch, `${entry.name}: form saved`);
      assert.equal(patch.payload.type, entry.type); assert.equal(patch.payload.category, entry.name);
      for (const name of ["tournament_type", "team_type", "subtype", "access_type"]) assert.equal(name in patch.payload, false);
      assert.equal(patch.payload.player_hub_visibility, "Hidden");
      // My Tournaments also manages CCL tournament admins through access users.
      const isMyCcl = page === "my" && entry.name === "CCL";
      assert.equal("access_users" in patch.payload, entry.mode === "official" || isMyCcl);
      if (entry.mode === "official" || isMyCcl) assert.deepEqual(patch.payload.access_users, tournament(entry).access_users);
      for (const name of ["ccl_main_registration_ends_at", "ccl_q12_registration_ends_at", "ccl_q3_registration_ends_at"]) {
        assert.equal(name in patch.payload, isMyCcl, `${entry.name}: ${name}`);
      }
      if (page === "my") assert.deepEqual(ui.cclSections, isMyCcl ? [tournament(entry).id] : []);
      assert.equal(ui.requests.some((request) => request.url === "/tournament-teams"), ["official", "friendly"].includes(entry.mode));
      if (!["official", "friendly"].includes(entry.mode)) {
        for (const name of ["lineup_size", "lineup_size_type", "team_size"]) assert.equal(name in patch.payload, false);
        assert.equal(ui.requests.some((request) => request.url === "/standings"), false);
      }
      const fieldNames = Array.from(ui.fields.keys()).map((value) => value.toLowerCase());
      assert.equal(fieldNames.includes("subtype"), false); assert.equal(fieldNames.includes("team type"), false);
      assert.equal(fieldNames.includes("tournament_type"), false);
    }
  });
  test(`${page}: changing type removes incompatible categories and blocks save until a valid pair is chosen`, async () => {
    const ui = contextForForm(page, tournament(TOURNAMENT_CATEGORIES[0])); await ui.open();
    const type = ui.fields.get(page === "admin" ? "type" : "Type").input;
    const category = ui.fields.get(page === "admin" ? "category" : "Category").input;
    type.value = "Individuals"; await type.fire("change");
    assert.equal(category.value, "");
    const choices = page === "admin" ? category.options.map((option) => option.value) : category.choices;
    assert.equal(choices.includes("WTCOC"), false); assert.equal(choices.includes("National"), true);
    assert.equal(choices.includes("King of Carcassonne"), true);
    await ui.submit(); assert.equal(ui.requests.length, 0);
    category.value = "CCL"; await category.fire("change"); await ui.submit();
    assert.equal(ui.requests[0].payload.category, "CCL"); assert.equal(ui.requests.length, 1);
  });
  test(`${page}: editing Rivals preserves TPR and unknown historical classification permits metadata edits`, async () => {
    const rivals = TOURNAMENT_CATEGORIES.find((entry) => entry.name === "Rivals");
    const ui = contextForForm(page, tournament(rivals, { standings_scoring: "bounty_tpr", tpr_target_games: 17, tpr_smoothing: 0.8, tpr_benchmark_percentile: 0.6 }));
    await ui.open(); await ui.submit();
    assert.equal(ui.requests[0].payload.tpr_target_games, 17);
    assert.equal(ui.requests[0].payload.tpr_smoothing, 0.8);
    assert.equal(ui.requests[0].payload.standings_scoring, "bounty_tpr");
    if (page === "my") assert.equal(ui.editors.get("periods").root.classList.contains("cp-hidden"), false);
    const unknown = contextForForm(page, { id: "old", name: "Historical", type: "Teams", category: "Old cup" });
    await unknown.open(); await unknown.submit();
    assert.equal(unknown.requests.length, 1); assert.equal("type" in unknown.requests[0].payload, false);
    assert.equal("category" in unknown.requests[0].payload, false);
  });
  test(`${page}: filters use the new classification and reset category when type changes`, async () => {
    const context = vm.createContext({ document: { createElement: (tag) => new Element(tag) } });
    load(context, page, ["getTournamentClassification", "filterTournamentsByClassification", "createTournamentFilters"]);
    const rows = TOURNAMENT_CATEGORIES.map((entry) => tournament(entry));
    const filters = { type: "Teams", category: "WTCOC" };
    assert.equal(context.filterTournamentsByClassification(rows, filters).length, 1);
    const root = context.createTournamentFilters(rows, filters, () => {});
    const [type, category] = root.querySelectorAll("select");
    assert.equal(category.options.some((option) => option.value === "Rivals"), false);
    type.value = "Individuals"; await type.fire("change");
    assert.equal(filters.category, "");
    assert.equal(context.filterTournamentsByClassification(rows, filters).length, 5);
  });
}

test("assigned tournament admins preserve global-only access assignments", async () => {
  const ui = contextForForm("my", tournament(TOURNAMENT_CATEGORIES[0]), { globalAdmin: false });
  await ui.open(); await ui.submit();
  assert.equal("access_users" in ui.requests[0].payload, false);
  assert.equal(ui.requests.some((request) => request.url === "/tournament-teams"), true);
});

test("new Admin tournaments require a category and Friendly has no ID restriction", async () => {
  const ui = contextForForm("admin", {}, { mode: "create" }); await ui.open();
  const type = ui.fields.get("type").input;
  assert.deepEqual(type.options.map((option) => option.value), ["Teams", "Individuals"]);
  ui.fields.get("id").input.value = "another-friendly";
  ui.fields.get("name").input.value = "Another friendly tournament";
  await ui.submit(); assert.equal(ui.requests.length, 0);
  ui.fields.get("category").input.value = "Friendly";
  await ui.fields.get("category").input.fire("change"); await ui.submit();
  assert.equal(ui.requests[0].method, "POST");
  assert.equal(ui.requests[0].payload.id, "another-friendly");
  assert.equal(ui.requests[0].payload.category, "Friendly");
});

test("My Tournaments preserves configured standings and does not clear them when hidden", async () => {
  const ui = contextForForm("my", tournament(TOURNAMENT_CATEGORIES[0])); await ui.open(); await ui.submit();
  const standings = ui.requests.find((request) => request.url === "/standings");
  assert.deepEqual(standings.payload.standings, [{ team_id: "UA", points: 3 }]);
  for (const freePlay of [true, false]) {
    const hidden = contextForForm("my", tournament(TOURNAMENT_CATEGORIES[0])); await hidden.open();
    const field = hidden.fields.get(freePlay ? "Tournament Format" : "Stage 1 Format").input;
    field.value = freePlay ? "Free Play" : "Single Elimination"; await field.fire("change");
    await hidden.submit();
    assert.equal(hidden.requests.some((request) => request.url === "/standings"), false);
  }
});

test("styled category dropdown replaces options and rejects stale selections", async () => {
  const context = vm.createContext({ document: { createElement: (tag) => new Element(tag) }, openTournamentDropdown: null });
  load(context, "my", ["createTournamentDropdown"]);
  const dropdown = context.createTournamentDropdown("WTCOC", ["", "WTCOC", "Friendly"]);
  dropdown.setOptions(["", "CCL", "National"], "WTCOC");
  assert.equal(dropdown.value, "");
  dropdown.value = "WTCOC"; assert.equal(dropdown.value, "");
  const national = dropdown.root.querySelectorAll(".cp-dd-option").find((option) => option.dataset.value === "National");
  await national.fire("click"); assert.equal(dropdown.value, "National");
});

test("both Player Hub menus keep official/Friendly permissions and exclude future and individual team-match links", () => {
  for (const page of ["hub", "menu"]) {
    const context = vm.createContext({});
    load(context, page, ["getTournamentClassification", "normalizeTournamentAccessUsers", "getTournamentSubtype", "isTournamentHiddenInPlayerHub", "canSeeTournament"]);
    const auth = { isAuthenticatedUser: true, hasLinkedBgaId: true, isCaptainUser: true, currentUserAuthId: "4" };
    for (const entry of TOURNAMENT_CATEGORIES) {
      const row = tournament(entry, { player_hub_visibility: "Visible" });
      assert.equal(context.canSeeTournament(row, { ...auth, isAdminUser: true }), ["official", "friendly"].includes(entry.mode));
      assert.equal(context.canSeeTournament(row, auth), ["official", "friendly"].includes(entry.mode));
      assert.equal(context.canSeeTournament({ ...row, player_hub_visibility: "Hidden" }, { ...auth, isAdminUser: true }), false);
    }
    const cup = tournament(TOURNAMENT_CATEGORIES[0], { player_hub_visibility: "Visible", access_users: [] });
    assert.equal(context.canSeeTournament(cup, auth), false);
  }
});

test("all changed HTML snippets contain valid JavaScript", () => {
  for (const [page, html] of Object.entries(pages)) {
    for (const [, script] of html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/gi)) assert.doesNotThrow(() => new Function(script), page);
  }
});
