'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {UctpClient} = require('../../main/resources/static/uctp-client.js');
const tick = () => new Promise((resolve) => setImmediate(resolve));

function idSource() {
  let next = 1000000000000000000n;
  const calls = [];
  const fetch = async (url, options) => {
    calls.push({url, options});
    await tick();
    return {ok: true, json: async () => ({ids: Array.from({length: 256}, () => String(next++))})};
  };
  return {fetch, calls};
}
function readyClient(options = {}) {
  const source = idSource();
  const sent = [];
  const client = new UctpClient({fetch: source.fetch, timeoutMs: 100, ...options});
  client.connected = true;
  client.ws = {readyState: 1, send: (data) => sent.push(JSON.parse(data)), close() { this.readyState = 3; }};
  return {client, sent, source};
}

test('4097 concurrent ID requests never exhaust a shared 256-ID refill', async () => {
  const source = idSource();
  const client = new UctpClient({fetch: source.fetch});
  const prefixes = ['cid', 'sid', 'connid', 'msgid', 'pid', 'envid', 'strmid'];
  const ids = await Promise.all(Array.from({length: 4097}, (_, i) => client.newId(prefixes[i % prefixes.length])));
  assert.equal(new Set(ids).size, 4097);
  assert.ok(ids.every((id) => /^(cid|sid|connid|msgid|pid|envid|strmid)\d+$/.test(id)));
  assert.equal(source.calls.length, 17);
  for (const call of source.calls) {
    assert.equal(call.url, '/uctp/ids?count=256');
    assert.equal(call.options.cache, 'no-store');
  }
  await assert.rejects(client.newId('msg_'), /前缀/);
});

test('reject malformed ID batches and recover from an allocation failure', async () => {
  const source = idSource();
  let fail = true;
  const client = new UctpClient({fetch: (...args) => fail ? Promise.resolve({ok: true, json: async () => ({ids: Array(256).fill('123')})}) : source.fetch(...args)});
  await assert.rejects(client.newId('cid'), /无效/);
  assert.equal(client.refillPromise, null);
  fail = false;
  assert.match(await client.newId('sid'), /^sid\d+$/);
});

test('request resolves only with the matching ACK, not socket write or another ACK', async () => {
  const {client, sent} = readyClient();
  let resolved = false;
  const request = client.request('message.send', {msg_id: 'msgid1'}, {cid: 'cid1'}).then((env) => { resolved = true; return env; });
  await client.sendQueue;
  assert.equal(resolved, false);
  assert.equal(client.pending.size, 1);
  client.handleResponse({type: 'ack', in_reply_to: 'unrelated'});
  await tick();
  assert.equal(resolved, false);
  client.handleResponse({type: 'ack', in_reply_to: sent[0].id, payload: {}});
  assert.equal((await request).type, 'ack');
  assert.equal(client.pending.size, 0);
});

test('error responses and ACK timeout reject and clean pending requests', async () => {
  const {client, sent} = readyClient({timeoutMs: 15});
  const request = client.request('message.send', {}, {cid: 'cid1'});
  const rejected = assert.rejects(request, /403: forbidden/);
  await client.sendQueue;
  client.handleResponse({type: 'error', in_reply_to: sent[0].id, payload: {code: 403, reason: 'forbidden'}});
  await rejected;
  assert.equal(client.pending.size, 0);
  await assert.rejects(client.request('message.history', {}, {cid: 'cid1'}), /超时.*未知/);
  assert.equal(client.pending.size, 0);
});

test('queued sends retain order after failure and closed requests are rejected', async () => {
  const {client, sent} = readyClient();
  client.ws.readyState = 3;
  await assert.rejects(client.send('message.received', {}), /未连接/);
  client.ws.readyState = 1;
  await Promise.all([client.send('first', {}), client.send('second', {})]);
  assert.deepEqual(sent.map((env) => env.type), ['first', 'second']);
  const pending = client.request('message.history');
  const rejected = assert.rejects(pending, /关闭.*未知/);
  await client.sendQueue;
  client.close();
  await rejected;
  assert.equal(client.pending.size, 0);
  await assert.rejects(client.request('session.invite'), /尚未认证/);
});

test('an ID operation pending across reconnection cannot write onto the new socket', async () => {
  const {client, sent} = readyClient();
  const pending = client.send('message.send', {});
  const rejected = assert.rejects(pending, /切换/);
  client.close();
  client.connected = true;
  client.ws = {readyState: 1, send: (data) => sent.push(JSON.parse(data))};
  await rejected;
  assert.equal(sent.length, 0);
});

class FakeSocket {
  static instances = [];
  constructor(url) { this.url = url; this.readyState = 0; this.sent = []; FakeSocket.instances.push(this); }
  open() { this.readyState = 1; this.onopen(); }
  send(data) { this.sent.push(JSON.parse(data)); }
  deliver(type, payload, extra = {}) { this.onmessage({data: JSON.stringify({v: 1, type, id: 'envid999', payload, ...extra})}); }
  close() { this.readyState = 3; this.onclose?.(); }
}
function storage() {
  const values = new Map();
  return {getItem: (key) => values.get(key), setItem: (key, value) => values.set(key, value), removeItem: (key) => values.delete(key)};
}
async function waitSocket() { while (!FakeSocket.instances.length) await tick(); return FakeSocket.instances.at(-1); }

test('invalid resume token retries a fresh challenge and new connections clear prior identity', async () => {
  FakeSocket.instances.length = 0;
  const source = idSource();
  const tokens = storage();
  tokens.setItem('uctp.token.agent-1', 'expired-token');
  const client = new UctpClient({fetch: source.fetch, storage: tokens, WebSocket: FakeSocket, location: {protocol: 'http:', host: 'localhost:8080'}, timeoutMs: 500});
  const connected = client.connect('agent-1');
  const socket = await waitSocket();
  assert.equal(socket.url, 'ws://localhost:8080/uctp');
  socket.open(); await client.sendQueue;
  assert.equal(socket.sent[0].payload.resume_token, 'expired-token');
  socket.deliver('error', {code: 401, reason: 'resume-token-invalid'}); await client.sendQueue;
  assert.equal(socket.sent.at(-1).type, 'auth.hello');
  assert.equal(socket.sent.at(-1).payload.resume_token, undefined);
  socket.deliver('auth.challenge', {nonce: 'challenge'}); await client.sendQueue;
  assert.equal(socket.sent.at(-1).type, 'auth.response');
  assert.equal(socket.sent.at(-1).in_reply_to, 'envid999');
  socket.deliver('auth.session', {identity_id: 'agent-1', session_token: 'secure-server-token'});
  assert.equal((await connected).identity_id, 'agent-1');
  assert.equal(client.connected, true);
  client.close();
  assert.equal(client.identityId, null);
  assert.equal(client.connected, false);
});

test('connect allocation and authentication failures are visible without orphan promises', async () => {
  const allocation = new UctpClient({fetch: async () => { throw new Error('allocation-down'); }});
  await assert.rejects(allocation.connect('agent-1'), /allocation-down/);
  assert.equal(allocation.abortAuth, undefined);
  FakeSocket.instances.length = 0;
  const source = idSource();
  const client = new UctpClient({fetch: source.fetch, WebSocket: FakeSocket, storage: storage(), location: {protocol: 'https:', host: 'localhost'}, timeoutMs: 500});
  const promise = client.connect('invalid');
  const rejected = assert.rejects(promise, /401: invalid-credential/);
  const socket = await waitSocket(); socket.open(); await client.sendQueue;
  socket.deliver('error', {code: 401, reason: 'invalid-credential'});
  await rejected;
  assert.equal(client.connected, false);
  assert.equal(socket.readyState, 3);
  await tick();
});
