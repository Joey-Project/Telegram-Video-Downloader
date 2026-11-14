import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { deflateSync } from "node:zlib";
import test from "node:test";
import vm from "node:vm";

const html = readFileSync(new URL("../assets/torrent-widget.html", import.meta.url), "utf8");
const taskId = "0123456789abcdef0123456789abcdef";

class Element {
  constructor(tagName = "div", id = "") {
    this.tagName = tagName;
    this.id = id;
    this.children = [];
    this.listeners = new Map();
    this.disabled = false;
    this.className = "";
    this._text = "";
  }
  set textContent(value) { this._text = String(value); }
  get textContent() { return this._text; }
  appendChild(child) { this.children.push(child); return child; }
  replaceChildren() { this.children = []; }
  addEventListener(name, callback) { this.listeners.set(name, callback); }
  click() { if (!this.disabled) this.listeners.get("click")?.(); }
}

function controls(container) {
  const result = [];
  const visit = (node) => {
    if (node.tagName === "button") result.push(node);
    node.children.forEach(visit);
  };
  visit(container);
  return result;
}

function createWidget(initialState, answerFor) {
  const nodes = new Map(["title", "summary", "notice", "listing", "pages", "actions"].map((id) => [id, new Element("div", id)]));
  const requests = [];
  const eventListeners = new Map();
  const widget = {
    chatData: JSON.stringify(initialState),
    onEvent(name, callback) { eventListeners.set(name, callback); },
    request(data) {
      requests.push(data);
      return Promise.resolve({ result: JSON.stringify(answerFor(data)) });
    },
  };
  const document = {
    getElementById(id) { return nodes.get(id); },
    createElement(tagName) { return new Element(tagName); },
  };
  const script = html.match(/<script>\s*([\s\S]*?)<\/script>/u)?.[1];
  assert.ok(script, "widget script exists");
  vm.runInNewContext(script, { window: { TelegramWidget: widget }, document });
  return {
    nodes,
    requests,
    publish(nextState) {
      widget.chatData = JSON.stringify(nextState);
      eventListeners.get("chatDataChanged")?.();
    },
  };
}

const baseState = {
  v: 1,
  t: taskId,
  i: "0000000000000012",
  s: "0000000000000004",
  r: "0000000000000001",
  x: "Torrent 🎵",
  q: "Downloading",
  p: 100,
  b: 1000,
  B: 1000,
  f: 0,
  n: "/",
  d: null,
  g: 0,
  G: 2,
  e: [{ k: "f", i: 1, n: "Track.flac", z: 1000, p: 100, d: true, m: true, s: false, u: null }],
  c: 0,
  u: 0,
  o: 0,
  a: 0,
  z: null,
};

test("static HTML remains within Telegram's UTF-8 and zlib level-9 limits", () => {
  const byteLength = Buffer.byteLength(html, "utf8");
  assert.ok(byteLength > 0 && byteLength <= 32_768);
  assert.ok(deflateSync(Buffer.from(html, "utf8"), { level: 9 }).byteLength <= 4_096);
  assert.match(html, /textContent/u);
  assert.doesNotMatch(html, /innerHTML/u);
});

test("widget buttons request page, file, folder and confirmed import actions", async () => {
  let state = baseState;
  const respond = (data) => {
    if (data.endsWith(":b:0:1")) {
      state = {
        ...state,
        r: (BigInt(state.r) + 1n).toString(16).padStart(16, "0"),
        g: 1,
        e: [
          { k: "d", i: 3, n: "Album", f: 2, m: 2, c: 0 },
          { k: "f", i: 4, n: "Disc 1.flac", z: 1000, p: 100, d: true, m: true, s: false, u: null },
        ],
      };
    } else if (data.endsWith(":f:4:1")) {
      state = { ...state, r: (BigInt(state.r) + 1n).toString(16).padStart(16, "0"), s: (BigInt(state.s) + 1n).toString(16).padStart(16, "0"), c: 1, e: state.e.map((entry) => entry.k === "f" ? { ...entry, s: true } : entry) };
    } else if (data.endsWith(":d:3:1")) {
      state = { ...state, r: (BigInt(state.r) + 1n).toString(16).padStart(16, "0"), s: (BigInt(state.s) + 1n).toString(16).padStart(16, "0"), c: 3, e: state.e.map((entry) => entry.k === "d" ? { ...entry, c: 2 } : entry) };
    } else if (data.endsWith(":i")) {
      state = { ...state, r: (BigInt(state.r) + 1n).toString(16).padStart(16, "0"), c: 0, u: 3 };
    }
    return state;
  };
  const { nodes, requests } = createWidget(state, respond);

  controls(nodes.get("pages")).find((button) => button.textContent === "Next ›").click();
  await Promise.resolve();
  assert.equal(requests[0], `t1:${taskId}:0000000000000012:b:0:1`);

  controls(nodes.get("listing")).find((button) => button.textContent.startsWith("□ Disc 1.flac")).click();
  await Promise.resolve();
  assert.equal(requests[1], `t1:${taskId}:0000000000000012:f:4:1`);

  controls(nodes.get("listing")).find((button) => button.textContent === "Select").click();
  await Promise.resolve();
  assert.equal(requests[2], `t1:${taskId}:0000000000000012:d:3:1`);

  controls(nodes.get("actions")).find((button) => button.textContent === "Review 3 selected").click();
  controls(nodes.get("actions")).find((button) => button.textContent === "Confirm import of 3 selected").click();
  await Promise.resolve();
  assert.equal(requests[3], `t1:${taskId}:0000000000000006:i`);
  assert.ok(requests.every((request) => Buffer.byteLength(request, "utf8") <= 64));
});

test("widget errors show feedback without replacing the current revision", async () => {
  const { nodes } = createWidget(baseState, () => ({ error: "The torrent changed; refresh and try again." }));
  controls(nodes.get("pages")).find((button) => button.textContent === "Refresh").click();
  await Promise.resolve();
  assert.equal(nodes.get("title").textContent, baseState.x);
  assert.match(nodes.get("notice").textContent, /torrent changed/u);
});

test("import confirmation stays armed through progress updates but expires on selection revision change", async () => {
  let current = {
    ...baseState,
    c: 1,
    e: [{ k: "f", i: 1, n: "Track.flac", z: 1000, p: 100, d: true, m: true, s: true, u: null }],
  };
  const { nodes, requests, publish } = createWidget(current, () => {
    current = { ...current, r: "0000000000000005", c: 0, u: 2 };
    return current;
  });
  controls(nodes.get("actions")).find((button) => button.textContent === "Review 1 selected").click();

  current = { ...current, r: "0000000000000002", p: 95, q: "Updating" };
  publish(current);
  assert.ok(controls(nodes.get("actions")).some((button) => button.textContent === "Confirm import of 1 selected"));

  current = { ...current, r: "0000000000000003", s: "0000000000000006", c: 2 };
  publish(current);
  assert.ok(!controls(nodes.get("actions")).some((button) => button.textContent.startsWith("Confirm import")));
  assert.ok(controls(nodes.get("actions")).some((button) => button.textContent === "Review 2 selected"));
  assert.match(nodes.get("notice").textContent, /selection changed/u);
  assert.equal(requests.length, 0, "state updates must not make background widget requests");

  controls(nodes.get("actions")).find((button) => button.textContent === "Review 2 selected").click();
  current = { ...current, r: "0000000000000004", p: 100, q: "Complete" };
  publish(current);
  controls(nodes.get("actions")).find((button) => button.textContent === "Confirm import of 2 selected").click();
  await Promise.resolve();
  assert.equal(requests[0], `t1:${taskId}:0000000000000006:i`);
});
