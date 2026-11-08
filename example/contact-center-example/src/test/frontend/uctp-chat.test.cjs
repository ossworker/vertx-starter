'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {ConversationStore, safeUrl, renderBody, upload, MAX_UPLOAD_BYTES} = require('../../main/resources/static/uctp-chat.js');
const base = 'https://example.test';

// Minimal DOM test double for renderer logic; these tests are not browser E2E.
class Element {
  constructor(tag) { this.tagName = tag; this.children = []; this.textContent = ''; this.className = ''; this.classList = {add: (name) => { this.className += ' ' + name; }}; }
  append(...children) { this.children.push(...children); }
}
function flatten(node) { return [node, ...node.children.flatMap(flatten)]; }

test('rich media URL whitelist blocks schemes, external origins and traversal', () => {
  assert.equal(safeUrl('/api/files/photo.png', base, true), '/api/files/photo.png');
  assert.equal(safeUrl(base + '/api/files/file.pdf', base, true), '/api/files/file.pdf');
  for (const url of ['javascript:alert(1)', 'data:image/png;base64,123', 'blob:https://example.test/id', 'https://evil.test/api/files/a.png', '//evil.test/api/files/a', 'https://user:pass@example.test/api/files/a', '/agent.html', '/api/upload', '/api/files/../secret', '/api/files/%2e%2e/secret', '/api/files/a%2fb', '/api/files/a\\b', '/api/files/%255cfile', '/api/files/x.svg?script=1', '/api/files/%ZZ', '/api/files/a\n.png']) {
    assert.equal(safeUrl(url, base, true), null, url);
  }
});

test('safe DOM rendering keeps hostile strings as text and does not load unsafe media', () => {
  const previous = global.document;
  global.document = {createElement: (tag) => new Element(tag)};
  try {
    const hostile = '<img src=x onerror=alert(1)><script>alert(1)</script>';
    const text = renderBody({content_type: 'text/plain', body: hostile}, null, base);
    assert.equal(text.children[0].textContent, hostile);
    assert.equal(flatten(text).some((n) => n.tagName === 'script' || n.tagName === 'img'), false);
    const image = renderBody({content_type: 'application/vnd.uctp.media.image+json', body: JSON.stringify({url: 'https://evil.test/x.png'})}, null, base);
    assert.equal(flatten(image).some((n) => n.src), false);
    const card = renderBody({content_type: 'application/vnd.uctp.card.product+json', body: JSON.stringify({title: hostile, description: hostile, url: 'javascript:alert(1)', image_url: 'https://evil.test/p.png', actions: [{url: 'javascript:alert(1)', label: hostile}]})}, () => {}, base);
    assert.ok(flatten(card).some((n) => n.textContent === hostile));
    assert.equal(flatten(card).some((n) => n.href || n.src || n.tagName === 'script'), false);
    const unknown = renderBody({content_type: 'application/unknown', body: hostile}, null, base);
    assert.equal(unknown.children[0].textContent, hostile);
  } finally { global.document = previous; }
});

test('card actions send a reference without claiming backend business success', () => {
  const previous = global.document;
  global.document = {createElement: (tag) => new Element(tag)};
  try {
    let action;
    const card = renderBody({msg_id: 'msgid123', content_type: 'application/vnd.uctp.card.action+json', body: JSON.stringify({title: '确认信息', actions: [{label: '确认', action: 'confirm'}]})}, (...args) => { action = args; }, base);
    const button = flatten(card).find((n) => n.tagName === 'button');
    button.onclick();
    assert.deepEqual(action.slice(0, 2), ['msgid123', 'confirm']);
    const receipt = renderBody({content_type: 'application/vnd.uctp.card-action+json', body: '{"action":"confirm"}'}, null, base);
    assert.match(receipt.children[0].textContent, /不代表执行成功/);
  } finally { global.document = previous; }
});

function storeFixture() {
  const store = new ConversationStore('agent-1');
  for (const cid of ['cid1', 'cid2']) store.opened({cid, payload: {participants: [{identity_id: 'agent-1', participant_id: 'pidagent', display_name: '客服'}, {identity_id: 'customer-' + cid, participant_id: 'pid' + cid, display_name: '客户' + cid}]}});
  store.select('cid1');
  return store;
}
function incoming(cid, msgId, body = '消息') { return {cid, ts: '2026-11-01T00:00:00Z', payload: {msg_id: msgId, from: 'pid' + cid, body, content_type: 'text/plain'}}; }

test('conversation switching isolates drafts, unread counts, read scopes and message lists', () => {
  const store = storeFixture();
  store.selected.draft = '客户一草稿';
  store.receive(incoming('cid1', 'msgid1'));
  store.receive(incoming('cid2', 'msgid2', '订单问题'));
  store.receive(incoming('cid2', 'msgid3'));
  assert.equal(store.ensure('cid1').unread, 0);
  assert.equal(store.ensure('cid2').unread, 2);
  assert.deepEqual(store.unreadMessages('cid2'), []);
  assert.deepEqual(store.unreadMessages('cid1'), ['msgid1']);
  store.select('cid2');
  assert.equal(store.selected.unread, 0);
  assert.equal(store.selected.messages.size, 2);
  assert.equal(store.selected.draft, '');
  assert.deepEqual(store.unreadMessages('cid1'), []);
  assert.deepEqual(store.search('订单').map((c) => c.cid), ['cid2']);
  store.select('cid1');
  assert.equal(store.selected.draft, '客户一草稿');
  assert.equal(store.selected.messages.size, 1);
});

test('history is deduplicated by msg_id without unread increment or replay', () => {
  const store = storeFixture();
  const env = incoming('cid2', 'msgid1');
  assert.equal(store.receive(env).added, true);
  assert.equal(store.receive(env).added, false);
  assert.equal(store.ensure('cid2').unread, 1);
  assert.equal(store.receive({...env, in_reply_to: 'envidhistory'}, true).added, false);
  assert.equal(store.receive(incoming('cid2', 'msgid2'), true).added, true);
  assert.equal(store.ensure('cid2').unread, 1);
  assert.equal(store.ensure('cid2').messages.size, 2);
});

test('message status starts pending and only explicit evidence advances receipts', () => {
  const store = storeFixture();
  store.outgoing('cid1', {msg_id: 'msgid1', body: '你好'});
  assert.equal(store.selected.messages.get('msgid1').status, 'pending');
  store.receive({cid: 'cid1', payload: {msg_id: 'msgid1', from: 'pidagent', body: '你好'}}, true);
  assert.equal(store.selected.messages.get('msgid1').status, 'pending', 'history cannot impersonate the pending message ACK');
  store.receipt('cid1', 'msgid1', 'submitted');
  assert.equal(store.selected.messages.get('msgid1').status, 'submitted');
  store.receipt('cid1', 'msgid1', 'received');
  store.receipt('cid1', 'msgid1', 'read');
  store.receipt('cid1', 'msgid1', 'submitted');
  store.receipt('cid1', 'msgid1', 'failed');
  assert.equal(store.selected.messages.get('msgid1').status, 'read');
  store.outgoing('cid2', {msg_id: 'msgid2'});
  store.receipt('cid2', 'msgid2', 'failed');
  assert.equal(store.ensure('cid2').messages.get('msgid2').status, 'failed');
  assert.equal(store.selected.messages.size, 1);
});

test('upload uses raw bytes, encoded filename, local URLs and 10MB limit', async () => {
  const file = {name: '商品 & 订单.png', size: 4, type: 'image/png'};
  let call;
  const content = await upload(file, MAX_UPLOAD_BYTES, async (url, options) => {
    call = {url, options};
    return {ok: true, json: async () => ({url: '/api/files/photo.png', size: 4})};
  }, base);
  assert.equal(call.options.body, file);
  assert.equal(call.options.method, 'POST');
  assert.equal(call.options.headers['Content-Type'], 'application/octet-stream');
  assert.match(call.url, /^\/api\/upload\?kind=image&filename=/);
  assert.equal(new URL(call.url, base).searchParams.get('filename'), file.name);
  assert.equal(content.content_type, 'application/vnd.uctp.media.image+json');
  assert.equal(JSON.parse(content.body).url, '/api/files/photo.png');
  await assert.rejects(upload({...file, size: MAX_UPLOAD_BYTES + 1}, MAX_UPLOAD_BYTES, () => { throw new Error('must not fetch'); }, base), /超过/);
  await assert.rejects(upload(file, 2, () => { throw new Error('must not fetch'); }, base), /超过/);
  await assert.rejects(upload(file, MAX_UPLOAD_BYTES, async () => ({ok: false, status: 413}), base), /上传失败: 413/);
  await assert.rejects(upload(file, MAX_UPLOAD_BYTES, async () => ({ok: true, json: async () => ({url: 'https://evil.test/file', size: 4})}), base), /无效/);
});
