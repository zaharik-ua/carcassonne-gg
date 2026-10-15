import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import vm from "node:vm";

const admin = readFileSync(new URL("../../gg-html/admin.html", import.meta.url), "utf8");

function pageFunction(name) {
  const start = admin.indexOf(`  function ${name}(`);
  assert.ok(start >= 0, `${name} must exist`);
  return admin.slice(start, admin.indexOf("\n  }", start) + 4);
}

class Element {
  children = [];
  listeners = {};
  classList = { add() {}, remove() {}, toggle() {} };
  appendChild(child) { this.children.push(child); return child; }
  addEventListener(name, listener) { this.listeners[name] = listener; }
}

function formContext() {
  const fields = new Map();
  const requests = [];
  let form;
  const field = (name, value) => {
    const result = { row: new Element(), input: Object.assign(new Element(), { value }) };
    fields.set(name, result);
    return result;
  };
  const context = vm.createContext({
    document: { createElement: () => new Element() },
    createTextRow: field, createNumberRow: field, createCheckboxRow: field,
    mountDetailForm: (value) => { form = value; },
    showCreateForm: (value) => { form = value; }, closeInlineForm() {},
    setStatus() {}, loadSectionData() {}, renderSection() {},
    state: { categories: [] }, CATEGORIES_URL: "/categories",
    apiJson: async (...args) => { requests.push(args); return {}; },
  });
  vm.runInContext(pageFunction("openCategoryForm"), context);
  return { context, fields, requests, getForm: () => form };
}

test("system category details disable every editable field, expose only Close, and ignore submitted edits", async () => {
  const { context, fields, requests, getForm } = formContext();
  context.openCategoryForm("edit", { id: 1, name: "Rivals", is_tournament: 1, is_read_only: true }, {});
  for (const name of ["name", "sort_order", "Tournament"]) assert.equal(fields.get(name).input.disabled, true);
  const actions = getForm().children.find((element) => element.className === "cp-edit-actions");
  assert.deepEqual(actions.children.map((button) => button.textContent), ["Close"]);
  let prevented = false;
  await getForm().listeners.submit({ preventDefault() { prevented = true; } });
  assert.equal(prevented, true);
  assert.equal(requests.length, 0);
});

test("ordinary categories still expose editable fields and Save/Delete; category creation stays available", () => {
  for (const mode of ["edit", "create"]) {
    const { context, fields, getForm } = formContext();
    context.openCategoryForm(mode, { id: 2, name: "Custom", is_read_only: false }, {});
    for (const name of ["name", "sort_order", "Tournament"]) assert.equal(fields.get(name).input.disabled, false);
    const actions = getForm().children.find((element) => element.className === "cp-edit-actions");
    assert.deepEqual(actions.children.map((button) => button.textContent),
      mode === "edit" ? ["Save", "Cancel", "Delete"] : ["Create", "Cancel"]);
  }
});

test("category list offers View for system categories and Edit for ordinary categories", () => {
  const rows = [];
  const createButton = new Element();
  const context = vm.createContext({
    sectionTitle: {}, sectionCreateBtn: createButton,
    sectionList: { appendChild: (row) => rows.push(row) },
    createListRow: (options) => options,
    state: { categories: [
      { id: 1, name: "Rivals", is_tournament: 1, is_read_only: true },
      { id: 2, name: "Custom", is_read_only: false },
    ] },
  });
  vm.runInContext(pageFunction("renderCategories"), context);
  context.renderCategories();
  assert.equal(rows.find((row) => row.title === "Rivals").buttonLabel, "View");
  assert.match(rows.find((row) => row.title === "Rivals").subtitle, /Read-only/);
  assert.equal(rows.find((row) => row.title === "Custom").buttonLabel, "Edit");
  assert.equal(createButton.textContent, "Create");
});
