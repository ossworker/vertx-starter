import test from 'node:test';
import assert from 'node:assert/strict';
import { localTarget, localResource, integer, parseArgs, commonOptions, parseFixture, credentials, closeConsultations, redactor, validateIds, envelope, percentiles, Runtime, Peer, now } from './protocol-tools.mjs';
import { loadOptions, Measurements } from './load.mjs';

// Pure/mocked tests only. No listener, fetch, WebSocket connection or service load.
test('literal loopback allowlist rejects remote targets and URL canonicalization tricks', () => {
  for (const value of ['http://localhost', 'http://localhost:8080/', 'https://127.0.0.1:9443', 'http://[::1]:8080']) assert.ok(localTarget(value));
  for (const value of ['http://example.com', 'http://192.168.0.1', 'http://0.0.0.0', 'http://127.0.0.2',
    'http://127.1', 'http://2130706433', 'http://0x7f000001', 'http://0177.0.0.1', 'http://localhost.evil',
    'http://localhost@evil.test', 'http://user:pass@localhost', 'http://localhost:0', 'http://localhost:65536',
    'http://localhost/path', 'http://localhost?target=remote', 'http://localhost#x', 'ws://localhost',
    'http://[::ffff:127.0.0.1]', 'http://localhost\\@example.com']) assert.throws(() => localTarget(value));
  assert.equal(localResource('http://127.0.0.1:8080', '/api/files/probe.bin').origin, 'http://127.0.0.1:8080');
  for (const value of ['https://example.com/file', '//example.com/file', 'http://localhost:8080/file',
    'http://127.0.0.1:8081/file', 'http://user:pass@127.0.0.1:8080/file']) assert.throws(() => localResource('http://127.0.0.1:8080', value));
});

test('numeric and CLI safety limits are fail-closed', () => {
  assert.equal(integer('200', 'connections', 1, 200), 200);
  for (const value of ['0', '201', '-1', '1.5', 'NaN', 'Infinity', '1e2', '', ' 2']) assert.throws(() => integer(value, 'connections', 1, 200));
  assert.throws(() => parseArgs(['--unrecognized', 'value']));
  assert.throws(() => parseArgs(['--rate', '200', '--rate', '201']));
  for (const args of [{ duration: '121' }, { rate: '2001' }, { 'max-messages': '100001' }, { 'timeout-ms': '10001' }]) assert.throws(() => commonOptions(args));
  assert.equal(commonOptions({ duration: '120', rate: '2000', 'max-messages': '100000' }).maxMessages, 100000);
  const options = loadOptions({ stages: '1,199', 'stage-seconds': '1', 'warmup-seconds': '0' });
  assert.deepEqual(options.stages, [1, 199]);
  for (const args of [{ stages: '200' }, { stages: '0' }, { stages: '10,25,50', 'stage-seconds': '40' }, { 'max-inflight': '2001' }, { stages: '1,1,1,1,1,1,1,1,1' }]) assert.throws(() => loadOptions(args));
});

test('narrow local fixture reader uses supplied entries, not hardcoded credentials', () => {
  const fixture = 'application:\n  tokens:\n    customer-fixture:\n      identity-id: identity_c\n      role: customer\n    "agent-fixture":\n      identity-id: identity_a\n      role: agent\n  other:\n    not-a-token: ignored\n';
  assert.deepEqual(parseFixture(fixture), [
    { token: 'customer-fixture', identityId: 'identity_c', role: 'customer' },
    { token: 'agent-fixture', identityId: 'identity_a', role: 'agent' },
  ]);
  assert.throws(() => parseFixture('tokens:\n  ${UNRESOLVED}:\n    identity-id: identity\n    role: agent\n'));
  const scrub = redactor(['fixture-secret']);
  const result = scrub({ token: 'fixture-secret', authorization: 'Bearer fixture-secret', nested: { session_token: 'newly-issued-secret' }, reason: 'error fixture-secret', list: ['fixture-secret'] });
  const encoded = JSON.stringify(result);
  assert.ok(!encoded.includes('fixture-secret')); assert.ok(!encoded.includes('newly-issued-secret'));
});

test('three explicit identities suffice; the second customer can probe nonmember access to the first CID', async () => {
  const result = await credentials({ 'agent-token': 'fixture-agent', 'customer-token': 'fixture-first', 'customer2-token': 'fixture-second' }, true);
  assert.equal(result.stranger, 'fixture-second');
  await assert.rejects(credentials({ 'agent-token': 'fixture-same', 'customer-token': 'fixture-same' }), /distinct/);
});

test('cleanup uses the customer for an unassigned queued CID and records failures without skipping', async () => {
  const calls = [];
  const agent = { request: async (_type, _payload, scope) => { calls.push(`agent:${scope.cid}`); } };
  const customer = { request: async (_type, _payload, scope) => { calls.push(`customer:${scope.cid}`); } };
  const broken = { request: async () => { throw new Error('simulated close failure'); } };
  const tracked = [
    { cid: 'cid1', customer, assigned: false }, { cid: 'cid2', customer, assigned: true },
    { cid: 'cid3', customer: broken, assigned: false },
  ];
  const errors = [];
  await closeConsultations(agent, tracked, errors);
  assert.deepEqual(calls.sort(), ['agent:cid2', 'customer:cid1']);
  assert.equal(errors.length, 1); assert.equal(tracked[0].closed, true); assert.equal(tracked[2].closed, undefined);
});

test('v1 envelope and ID representation keep Snowflakes as strings with JSON body scope', () => {
  assert.deepEqual(validateIds({ ids: ['1', '9223372036854775807'] }, 2), ['1', '9223372036854775807']);
  for (const ids of [[1], ['0'], ['-1'], ['1.0'], ['9223372036854775808'], ['1', '1']]) assert.throws(() => validateIds({ ids }, ids.length));
  const env = envelope('envid123', 'message.send', { body: 'hello' }, { cid: 'cid456', sid: 'sid789', connid: 'connid321', in_reply_to: 'envid111', token: 'ignored' });
  assert.equal(env.v, 1); assert.equal(env.cid, 'cid456'); assert.equal(env.sid, 'sid789'); assert.equal(env.connid, 'connid321');
  assert.equal(env.payload.body, 'hello'); assert.ok(!('token' in env));
  assert.throws(() => envelope('envid_uuid', 'message.send'));
});

test('nearest-rank percentiles report null instead of invented empty samples', () => {
  assert.deepEqual(percentiles([]), { count: 0, p50: null, p95: null, p99: null, max: null });
  assert.deepEqual(percentiles([5, 1, 3, 2, 4]), { count: 5, p50: 3, p95: 5, p99: 5, max: 5 });
  assert.equal(percentiles(Array.from({ length: 100 }, (_, i) => i + 1)).p99, 99);
});

const message = (id, cid = 'cid10') => ({ id: `envid${id}`, cid, payload: { msg_id: `msgid${id}` } });
const ack = (env) => ({ type: 'ack', in_reply_to: env.id });
const delivery = (env) => ({ type: 'message.send', cid: env.cid, payload: { msg_id: env.payload.msg_id } });

test('ACK is not delivery; both can arrive out of order with independent latency samples', () => {
  const metrics = new Measurements(100);
  const one = message('1'); metrics.begin(one, 'measured', 1000);
  metrics.observeAck(ack(one), 1005);
  assert.equal(metrics.summary('measured').successful, 0);
  assert.equal(metrics.summary('measured').delivery.successful, 0);
  metrics.observeDelivery({ type: 'message.delivered', cid: one.cid, payload: one.payload }, 1006);
  assert.equal(metrics.summary('measured').delivery.successful, 0);
  metrics.observeDelivery(delivery(one), 1015);
  assert.equal(metrics.summary('measured').successful, 1);
  assert.equal(metrics.summary('measured').ack.latency_ms.p50, 5);
  assert.equal(metrics.summary('measured').delivery.latency_ms.p50, 15);
  const two = message('2'); metrics.begin(two, 'measured', 2000);
  metrics.observeDelivery(delivery(two), 2008); metrics.observeAck(ack(two), 2020);
  assert.equal(metrics.summary('measured').successful, 2);
  assert.equal(metrics.byMessage.size, 0);
});

test('history, foreign CID and uncorrelated ACK never fabricate success; duplicates are detected', () => {
  const metrics = new Measurements(100);
  const env = message('3'); metrics.begin(env, 'measured', 1000);
  metrics.observeDelivery({ ...delivery(env), in_reply_to: 'envid_history' }, 1001);
  metrics.observeDelivery({ ...delivery(env), cid: 'cid99' }, 1002);
  metrics.observeAck({ type: 'ack', in_reply_to: 'envid999' }, 1003);
  assert.equal(metrics.summary('measured').ack.successful, 0); assert.equal(metrics.summary('measured').delivery.successful, 0);
  metrics.observeDelivery(delivery(env), 1010); metrics.observeAck(ack(env), 1011);
  metrics.observeDelivery(delivery(env), 1012);
  assert.equal(metrics.duplicates, 1); assert.equal(metrics.summary('measured').successful, 1);
});

test('missing delivery / missing ACK, errors and forced stop have separate counters', () => {
  const metrics = new Measurements(100);
  const one = message('4'); metrics.begin(one, 'measured', 1000); metrics.observeAck(ack(one), 1005); metrics.expire(1101);
  let stats = metrics.summary('measured');
  assert.equal(stats.ack.successful, 1); assert.equal(stats.delivery.timeouts, 1); assert.equal(stats.timeouts, 1); assert.equal(stats.successful, 0);
  const two = message('5'); metrics.begin(two, 'measured', 2000); metrics.observeDelivery(delivery(two), 2005); metrics.expire(2101);
  stats = metrics.summary('measured'); assert.equal(stats.ack.timeouts, 1); assert.equal(stats.delivery.successful, 1);
  const three = message('6'); metrics.begin(three, 'measured', 3000); metrics.observeAck({ type: 'error', in_reply_to: three.id }, 3001);
  stats = metrics.summary('measured'); assert.equal(stats.errors, 1); assert.equal(stats.ack.errors, 1); assert.equal(stats.delivery.errors, 1);
  const four = message('7'); metrics.begin(four, 'measured', 4000); metrics.expire(4001, true);
  stats = metrics.summary('measured'); assert.equal(stats.initiated, 4); assert.equal(stats.timeouts, 3);
  assert.equal(stats.successful + stats.errors + stats.timeouts, stats.initiated); assert.equal(metrics.byEnvelope.size, 0);
});

test('warmup measurements cannot contaminate measured samples or success counts', () => {
  const metrics = new Measurements(100);
  const warm = message('8'); metrics.begin(warm, 'warmup', 1000); metrics.observeAck(ack(warm), 1090); metrics.observeDelivery(delivery(warm), 1095);
  const measured = message('9'); metrics.begin(measured, 'measured', 2000); metrics.observeAck(ack(measured), 2001); metrics.observeDelivery(delivery(measured), 2002);
  assert.equal(metrics.summary('warmup').delivery.latency_ms.p99, 95);
  assert.equal(metrics.summary('measured').delivery.latency_ms.p99, 2);
  assert.equal(metrics.summary('measured').initiated, 1);
});

test('rate permit serializes callers and enforces a rolling one-second window', async () => {
  const runtime = new Runtime({ ...commonOptions({ rate: '2' }), maxMessages: 100 });
  try {
    const start = now();
    await Promise.all([runtime.permit(), runtime.permit(), runtime.permit()]);
    assert.ok(now() - start >= 990, 'third frame at 2/s must wait for rolling window');
    assert.equal(runtime.sent, 3);
  } finally { await runtime.dispose(); }
});

test('message caps reserve cleanup and include cleanup frames; deadline never resets', async () => {
  const runtime = new Runtime({ ...commonOptions({ rate: '2000' }), maxMessages: 100 });
  try {
    runtime.sent = 80;
    await assert.rejects(runtime.permit(), /message limit/);
    runtime.sent = 99; await runtime.permit(true);
    await assert.rejects(runtime.permit(true), /message limit/);
    runtime.workDeadline = now() - 1;
    assert.throws(() => runtime.check(), /duration limit/);
    assert.doesNotThrow(() => runtime.check(true));
    runtime.deadline = now() - 1;
    assert.throws(() => runtime.check(true), /duration limit/);
  } finally { await runtime.dispose(); }
});

test('connection cap includes opening/closing peers before constructing any WebSocket', async () => {
  const runtime = new Runtime(commonOptions({}));
  try {
    for (let i = 0; i < 200; i++) runtime.peers.add({});
    const peer = new Peer(runtime, {}, 'pure-limit-check');
    await assert.rejects(peer.open(), /connection limit/);
    assert.equal(peer.ws, undefined);
  } finally { runtime.peers.clear(); await runtime.dispose(); }
});

test('local HTTP requests set redirect:error; off-origin assets fail before fetch', async () => {
  const realFetch = globalThis.fetch;
  let calls = 0;
  const runtime = new Runtime(commonOptions({}));
  globalThis.fetch = async (url, init) => {
    calls++; assert.equal(url.origin, 'http://127.0.0.1:8080'); assert.equal(init.redirect, 'error'); assert.ok(init.signal);
    return new Response('{"ids":["1"]}', { status: 200, headers: { 'cache-control': 'no-store' } });
  };
  try {
    const response = await runtime.fetch('/uctp/ids?count=1');
    assert.deepEqual(response.json(), { ids: ['1'] });
    await assert.rejects(runtime.fetch('https://example.com/file'), /local target/);
    assert.equal(calls, 1);
  } finally { globalThis.fetch = realFetch; await runtime.dispose(); }
});
