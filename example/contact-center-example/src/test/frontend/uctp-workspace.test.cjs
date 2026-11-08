'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const tick = () => new Promise((resolve) => setImmediate(resolve));

// This minimal DOM harness exercises the shared controller, not browser layout,
// networking, permission prompts, media transport or accessibility rendering.
class Element {
  constructor(tag, document) {
    this.tagName = tag; this.ownerDocument = document; this.children = []; this.attributes = {}; this.dataset = {};
    this.disabled = false; this.hidden = false; this.value = ''; this.text = ''; this.scrollTop = 0; this.scrollHeight = 0; this.clientHeight = 0;
    this.className = ''; this.classList = {add: (...names) => { this.className += ' ' + names.join(' '); }, remove: (...names) => { this.className = this.className.split(' ').filter((n) => !names.includes(n)).join(' '); }};
  }
  set id(id) { this._id = id; this.ownerDocument.ids.set(id, this); }
  get id() { return this._id; }
  get firstChild() { return this.children[0]; }
  get list() { return null; }
  set textContent(text) { this.text = String(text); this.children = []; }
  get textContent() { return this.text + this.children.map((c) => c.textContent).join(''); }
  append(...children) { for (const child of children) { child.remove(); this.children.push(child); child.parentElement = this; } }
  appendChild(child) { this.append(child); return child; }
  replaceChildren(...children) { for (const child of this.children) child.parentElement = null; this.children = []; this.text = ''; this.append(...children); }
  remove() { if (this.parentElement) { this.parentElement.children = this.parentElement.children.filter((c) => c !== this); this.parentElement = null; } }
  setAttribute(key, value) {
    this.attributes[key] = String(value);
    if (key === 'id') this.id = value;
    if (key.startsWith('data-')) this.dataset[key.slice(5).replace(/-([a-z])/g, (_, c) => c.toUpperCase())] = value;
  }
  querySelectorAll(selector) { return query(this, selector); }
  focus() { this.ownerDocument.activeElement = this; }
  showModal() { this.open = true; }
  close() { this.open = false; }
  pause() { this.paused = true; }
  async play() { this.paused = false; }
}
function descendants(el) { return el.children.flatMap((child) => [child, ...descendants(child)]); }
function match(el, selector) {
  if (selector.startsWith('[')) return Object.hasOwn(el.attributes, selector.slice(1, -1));
  if (selector.startsWith('#')) return el.id === selector.slice(1);
  return el.tagName === selector;
}
function query(root, selector) {
  const results = [];
  for (const term of selector.split(',').map((s) => s.trim())) {
    const parts = term.split(' ');
    for (const child of descendants(root)) {
      if (!match(child, parts.at(-1))) continue;
      if (parts.length > 1) {
        let parent = child.parentElement;
        while (parent && !match(parent, parts[0])) parent = parent.parentElement;
        if (!parent) continue;
      }
      results.push(child);
    }
  }
  return results;
}
class Document {
  constructor(role) { this.ids = new Map(); this.body = new Element('body', this); this.body.dataset.role = role; this.hidden = false; this.listeners = new Map(); }
  createElement(tag) { return new Element(tag, this); }
  getElementById(id) { return this.ids.get(id); }
  querySelectorAll(selector) { return this.body.querySelectorAll(selector); }
  addEventListener(type, fn) { this.listeners.set(type, fn); }
}
class Stream {
  constructor() { this.tracks = []; }
  getTracks() { return this.tracks; }
  addTrack(t) { this.tracks.push(t); }
}
class Client {
  static instance;
  constructor() { Client.instance = this; this.handlers = new Map(); this.sent = []; this.connected = false; this.id = 0; }
  on(type, fn) { if (!this.handlers.has(type)) this.handlers.set(type, []); this.handlers.get(type).push(fn); }
  async emit(type, payload = {}, scope = {}) { for (const fn of this.handlers.get(type) || []) await fn({type, payload, ...scope}); }
  close() { this.connected = false; }
  async connect(token) { this.identityId = token; this.connected = true; return {identity_id: token}; }
  async newId(prefix) { return prefix + (++this.id); }
  async send(type, payload, scope) { this.sent.push({type, payload, ...scope}); }
  async request(type, payload, scope) {
    this.sent.push({type, payload, ...scope});
    if (type === 'message.send' && this.messageGate) return this.messageGate;
    if (type === 'workforce.enqueue') {
      await this.emit('workforce.queued', {cid: 'cid1', customer_participant_id: 'pidcustomer1', position: 0});
      return {type: 'workforce.queued'};
    }
    if (type === 'workforce.agents') return {type: 'workforce.roster'};
    return {type: 'ack'};
  }
}
function fixture(role) {
  const document = new Document(role);
  for (const id of ['app', 'status', 'connectionDot']) { const el = document.createElement('div'); el.id = id; document.body.append(el); }
  const context = vm.createContext({document, window: {addEventListener() {}}, TestClient: Client, MediaStream: Stream, console, URL, setTimeout, clearTimeout, location: {origin: 'http://localhost'}, fetch: async () => ({ok: true, json: async () => ({ice_servers: [], max_upload_bytes: 10485760})})});
  for (const file of ['uctp-client.js', 'uctp-chat.js', 'uctp-media.js']) vm.runInContext(fs.readFileSync(path.join(__dirname, '../../main/resources/static', file), 'utf8'), context, {filename: file});
  vm.runInContext('UctpClient = TestClient;', context);
  vm.runInContext(fs.readFileSync(path.join(__dirname, '../../main/resources/static/uctp-workspace.js'), 'utf8'), context, {filename: 'uctp-workspace.js'});
  return {document, client: Client.instance, $: (id) => document.getElementById(id)};
}
async function open(client, cid, assigned = true) {
  await client.emit('conversation.opened', {participants: [{identity_id: client.identityId, participant_id: 'pidme', display_name: '我', role: client.identityId.startsWith('agent') ? 'agent' : 'customer'}, {identity_id: 'customer-' + cid, participant_id: 'pid' + cid, display_name: '客户' + cid, role: 'customer'}]}, {cid});
  if (assigned) await client.emit('workforce.assigned', {customer_identity_id: 'customer-' + cid, agent_identity_id: 'agent-1'}, {cid});
}

test('customer start-consultation and assignment never initiate voice/video or media capture', async () => {
  const {$, client} = fixture('customer');
  assert.equal($('btnEnqueue').disabled, true);
  assert.equal($('btnVoice').disabled, true);
  await $('btnConnect').onclick();
  assert.equal($('btnEnqueue').disabled, false);
  await $('btnEnqueue').onclick();
  await client.emit('workforce.assigned', {agent_identity_id: 'agent-1', agent_display_name: '客服'}, {cid: 'cid1'});
  assert.equal(client.sent.some((s) => s.type === 'session.invite'), false);
  assert.equal($('btnChat').disabled, false);
  assert.equal($('btnVoice').disabled, false);
  assert.equal($('callCount').textContent, '0');
});

test('shared agent controller sends received receipts, reads only selected cid, and deduplicates history', async () => {
  const {$, client} = fixture('agent');
  await $('btnConnect').onclick();
  await open(client, 'cid1'); await open(client, 'cid2');
  const message = {msg_id: 'msgid111', from: 'pidcid2', content_type: 'text/plain', body: '客户二消息'};
  await client.emit('message.send', message, {cid: 'cid2', ts: '2026-11-01T00:00:00Z'});
  assert.equal(client.sent.filter((s) => s.type === 'message.received' && s.cid === 'cid2').length, 1);
  assert.equal(client.sent.filter((s) => s.type === 'message.read' && s.cid === 'cid2').length, 0);
  assert.equal($('messages').textContent.includes('客户二消息'), false);
  await client.emit('message.send', message, {cid: 'cid2', in_reply_to: 'envidhistory'});
  assert.equal(client.sent.filter((s) => s.type === 'message.received' && s.cid === 'cid2').length, 1);
  $('conversationList').children[1].onclick(); await tick();
  assert.equal($('messages').textContent.includes('客户二消息'), true);
  assert.equal($('messages').children.length, 1);
  assert.equal(client.sent.filter((s) => s.type === 'message.read' && s.cid === 'cid2').length, 1);
  $('chatText').value = '客户二草稿'; $('chatText').oninput();
  $('conversationList').children[0].onclick();
  assert.equal($('chatText').value, '');
  $('conversationList').children[1].onclick();
  assert.equal($('chatText').value, '客户二草稿');
});

test('controller labels message submitted only after ACK and busy state keeps assigned chat usable', async () => {
  const {$, client} = fixture('agent');
  await $('btnConnect').onclick(); await open(client, 'cid1');
  let ack;
  client.messageGate = new Promise((resolve) => { ack = resolve; });
  $('chatText').value = '测试提交';
  $('composer').onsubmit({preventDefault() {}}); await tick();
  assert.match($('messages').textContent, /提交中/);
  assert.equal($('messages').textContent.includes('已提交'), false);
  ack({type: 'ack'}); await tick();
  assert.match($('messages').textContent, /已提交/);
  assert.equal($('messages').textContent.includes('对端收到'), false);
  const sentMessage = client.sent.find((s) => s.type === 'message.send');
  await client.emit('message.delivered', {msg_id: sentMessage.payload.msg_id}, {cid: 'cid1'});
  assert.match($('messages').textContent, /对端收到/);
  await client.emit('workforce.roster', {agents: [{identity_id: 'agent-1', state: 'busy', assigned_cids: ['cid1'], active_conversations: 1}], waiting: []});
  assert.equal($('agentState').value, 'busy');
  assert.equal($('btnChat').disabled, false);
  await client.emit('client.closed');
  for (const id of ['btnChat', 'btnVoice', 'btnVideo', 'btnCloseConversation', 'btnHandoff']) assert.equal($(id).disabled, true, id);
});

test('incoming calls show explicit accept/reject controls and ending one does not erase consultation', async () => {
  const {$, client} = fixture('agent');
  await $('btnConnect').onclick(); await open(client, 'cid1');
  await client.emit('session.invite', {medium: 'voice', from: 'pidcid1'}, {cid: 'cid1', sid: 'sid1'});
  assert.equal($('callCount').textContent, '1');
  assert.match($('callList').textContent, /接听.*拒绝/);
  assert.equal(client.sent.some((s) => s.type === 'session.accept'), false);
  await client.emit('session.ended', {reason: 'cancelled'}, {cid: 'cid1', sid: 'sid1'});
  assert.equal($('callCount').textContent, '0');
  assert.equal($('btnChat').disabled, false);
  assert.equal($('conversationCount').textContent, '1');
});
