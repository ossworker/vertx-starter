import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { performance } from 'node:perf_hooks';

export const LIMITS = Object.freeze({ connections: 200, seconds: 120, messages: 100000, rate: 2000 });
export const DEFAULT_FIXTURE = fileURLToPath(new URL('../src/main/resources/application.yml', import.meta.url));
export const now = () => performance.now();
export const delay = (ms) => new Promise((resolve) => setTimeout(resolve, Math.max(0, ms)));

export function integer(value, name, min, max) {
  if (!/^\d+$/.test(String(value)) || !Number.isSafeInteger(Number(value)) || Number(value) < min || Number(value) > max) {
    throw new Error(`${name} must be an integer in ${min}..${max}`);
  }
  return Number(value);
}

// Validate the literal authority too: WHATWG URL canonicalizes 127.1 / integer IPs.
export function localTarget(value = 'http://127.0.0.1:8080') {
  const match = /^(https?):\/\/(localhost|127\.0\.0\.1|\[::1\])(?::(\d{1,5}))?\/?$/.exec(value);
  if (!match) throw new Error('Target must be a literal localhost/127.0.0.1/[::1] HTTP(S) origin without credentials, query or path');
  if (match[3]) integer(match[3], 'port', 1, 65535);
  return new URL(value).origin;
}

export function localResource(target, resource) {
  const origin = localTarget(target);
  const url = new URL(resource, origin);
  if (url.origin !== origin || url.username || url.password || url.hash) throw new Error('Resource must stay on the exact local target origin');
  localTarget(url.origin);
  return url;
}

export function parseArgs(argv, extra = []) {
  const allowed = new Set(['target', 'fixture', 'agent-token', 'customer-token', 'customer2-token', 'stranger-token',
    'duration', 'timeout-ms', 'max-messages', 'rate', 'help', ...extra]);
  const out = {};
  for (let i = 0; i < argv.length; i++) {
    const key = argv[i].replace(/^--/, '');
    if (!argv[i].startsWith('--') || !allowed.has(key) || key in out) throw new Error('Unknown or duplicate argument');
    if (key === 'help') { out.help = true; continue; }
    if (!argv[i + 1] || argv[i + 1].startsWith('--')) throw new Error(`Missing value for --${key}`);
    out[key] = argv[++i];
  }
  return out;
}

export function commonOptions(args) {
  if (Number(process.versions.node.split('.')[0]) !== 24 || typeof WebSocket !== 'function' || typeof fetch !== 'function') {
    throw new Error('This harness requires Node 24 native WebSocket and fetch');
  }
  const duration = integer(args.duration ?? 120, 'duration', 10, LIMITS.seconds);
  return {
    target: localTarget(args.target), duration,
    timeoutMs: integer(args['timeout-ms'] ?? 5000, 'timeout-ms', 100, 10000),
    maxMessages: integer(args['max-messages'] ?? LIMITS.messages, 'max-messages', 1, LIMITS.messages),
    rate: integer(args.rate ?? 200, 'rate', 1, LIMITS.rate),
    maxConnections: LIMITS.connections,
  };
}

// A deliberately narrow reader for the already-provided development tokens block;
// not a general YAML parser. No interpolation, imports, remote fixtures or secrets in diagnostics.
export function parseFixture(text) {
  const records = [];
  let tokensIndent = -1;
  let current;
  for (const raw of text.split(/\r?\n/)) {
    if (!raw.trim() || raw.trimStart().startsWith('#')) continue;
    const indent = raw.length - raw.trimStart().length;
    if (tokensIndent < 0) {
      if (/^\s*tokens:\s*(?:#.*)?$/.test(raw)) tokensIndent = indent;
      continue;
    }
    if (indent <= tokensIndent) break;
    if (indent === tokensIndent + 2) {
      const match = /^\s*([^:]+):\s*(?:#.*)?$/.exec(raw);
      if (!match) throw new Error('Unsupported local token fixture syntax');
      const token = match[1].trim().replace(/^(['"])(.*)\1$/, '$2');
      if (token.includes('${')) throw new Error('Fixture interpolation is not supported; supply token arguments');
      current = { token };
      records.push(current);
    } else if (indent === tokensIndent + 4 && current) {
      const match = /^\s*(identity-id|role):\s*([^#]+?)(?:\s+#.*)?$/.exec(raw);
      if (match) current[match[1] === 'identity-id' ? 'identityId' : 'role'] = match[2].trim().replace(/^(['"])(.*)\1$/, '$2');
    }
  }
  return records.filter((r) => r.identityId && r.role);
}

export async function credentials(args, requireThird = false) {
  let records = [];
  const needed = !args['agent-token'] || !args['customer-token'] || (requireThird && !args['customer2-token']);
  if (needed || args.fixture) {
    try { records = parseFixture(await readFile(args.fixture ?? DEFAULT_FIXTURE, 'utf8')); }
    catch { throw new Error('Cannot read local development fixture; supply explicit token arguments or a supported local fixture'); }
  }
  const customers = records.filter((r) => r.role === 'customer');
  const agent = args['agent-token'] ?? records.find((r) => r.role === 'agent')?.token;
  const customer = args['customer-token'] ?? customers[0]?.token;
  const customer2 = args['customer2-token'] ?? customers.find((r) => r.token !== customer)?.token;
  const stranger = args['stranger-token'] ?? records.find((r) => ![agent, customer, customer2].includes(r.token) && r.role === 'supervisor')?.token
    ?? records.find((r) => ![agent, customer, customer2].includes(r.token))?.token ?? customer2;
  if (!agent || !customer || (requireThird && (!customer2 || !stranger))) throw new Error('Required distinct development identities are missing; supply token arguments');
  if (agent === customer) throw new Error('Agent and customer credentials must be distinct');
  if (requireThird && (new Set([agent, customer, customer2]).size !== 3 || [agent, customer].includes(stranger))) {
    throw new Error('E2E requires agent/two customer credentials and a stranger distinct from the first conversation members');
  }
  return { agent, customer, customer2, stranger };
}

export function redactor(secrets) {
  const values = [...new Set(secrets.filter(Boolean))].sort((a, b) => b.length - a.length);
  return (value) => {
    const walk = (v, key = '') => {
      if (/token|credential|authorization/i.test(key)) return '[REDACTED]';
      if (typeof v === 'string') { for (const secret of values) v = v.split(secret).join('[REDACTED]'); return v; }
      if (Array.isArray(v)) return v.map((item) => walk(item));
      if (v && typeof v === 'object') return Object.fromEntries(Object.entries(v).map(([k, item]) => [k, walk(item, k)]));
      return v;
    };
    return walk(value);
  };
}

export function validateIds(body, count) {
  assert.ok(Array.isArray(body.ids), 'ids response must have an array');
  assert.equal(body.ids.length, count, 'ids response count');
  for (const id of body.ids) {
    assert.equal(typeof id, 'string', 'Snowflake IDs must remain strings');
    assert.match(id, /^[1-9]\d{0,18}$/, 'decimal Snowflake string');
    assert.ok(BigInt(id) <= 9223372036854775807n, 'Snowflake must fit signed long');
  }
  assert.equal(new Set(body.ids).size, count, 'unique allocated IDs');
  return body.ids;
}

export function envelope(id, type, payload = {}, scope = {}) {
  assert.match(id, /^envid[1-9]\d{0,18}$/, 'v1 envelope ID');
  const env = { v: 1, type, id, ts: new Date().toISOString(), payload };
  // cid/sid/connid are JSON envelope-body scope, not HTTP headers or payload.to.
  for (const key of ['cid', 'sid', 'connid', 'in_reply_to']) if (scope[key]) env[key] = scope[key];
  return env;
}

export function percentiles(samples) {
  if (!samples.length) return { count: 0, p50: null, p95: null, p99: null, max: null };
  const sorted = [...samples].sort((a, b) => a - b);
  const at = (p) => sorted[Math.max(0, Math.ceil(sorted.length * p) - 1)];
  return { count: sorted.length, p50: at(0.5), p95: at(0.95), p99: at(0.99), max: sorted.at(-1) };
}

export class Runtime {
  constructor(options, secrets = []) {
    this.options = options;
    this.startedAt = now();
    this.deadline = this.startedAt + options.duration * 1000;
    this.workDeadline = this.deadline - 5000; // reserve cleanup/report time inside the 120s bound
    this.peers = new Set();
    this.peakConnections = 0;
    this.abort = new AbortController();
    this.sent = 0;
    this.sendTimes = [];
    this.rateChain = Promise.resolve();
    this.cleanupErrors = [];
    this.redact = redactor(secrets);
    this.stopReason = null;
    this.watchdog = setTimeout(() => {
      this.stop('hard-duration-limit');
      this.onHardDeadline?.();
    }, options.duration * 1000);
    this.workTimer = setTimeout(() => this.stop('work-duration-limit'), Math.max(0, this.workDeadline - now()));
  }
  stop(reason) {
    this.stopReason ??= reason;
    if (!this.abort.signal.aborted) this.abort.abort(new Error(reason));
    for (const peer of this.peers) peer.fail(new Error(reason));
  }
  check(cleanup = false) {
    if (now() >= (cleanup ? this.deadline : this.workDeadline)) throw new Error('Total duration limit reached');
    if (!cleanup && this.abort.signal.aborted) throw new Error(this.stopReason ?? 'stopped');
  }
  async fetch(resource, init = {}, cleanup = false) {
    this.check(cleanup);
    const url = localResource(this.options.target, resource);
    const ms = Math.max(1, Math.min(this.options.timeoutMs, (cleanup ? this.deadline : this.workDeadline) - now()));
    const signal = cleanup ? AbortSignal.timeout(ms) : AbortSignal.any([this.abort.signal, AbortSignal.timeout(ms)]);
    try {
      // Read the entire body within the same timeout: a slow body cannot evade the bound.
      const response = await fetch(url, { ...init, redirect: 'error', signal });
      const bytes = new Uint8Array(await response.arrayBuffer());
      return { status: response.status, ok: response.ok, headers: response.headers,
        bytes, text: () => new TextDecoder().decode(bytes), json: () => JSON.parse(new TextDecoder().decode(bytes)) };
    } catch { throw new Error('Local HTTP request failed or timed out (redirects forbidden)'); }
  }
  async permit(cleanup = false) {
    const action = async () => {
      this.check(cleanup);
      const reserve = cleanup ? 0 : this.peers.size * 2 + 20;
      if (this.sent >= this.options.maxMessages - reserve) throw new Error('Total message limit reached (cleanup reserve retained)');
      let t = now();
      this.sendTimes = this.sendTimes.filter((stamp) => t - stamp < 1000);
      if (this.sendTimes.length >= this.options.rate) {
        await delay(1001 - (t - this.sendTimes[0]));
        this.check(cleanup);
        t = now();
        this.sendTimes = this.sendTimes.filter((stamp) => t - stamp < 1000);
      }
      this.sent++;
      this.sendTimes.push(t);
    };
    const pending = this.rateChain.then(action);
    this.rateChain = pending.catch(() => {});
    return pending;
  }
  async dispose({ final = true } = {}) {
    const results = await Promise.allSettled([...this.peers].map((peer) => peer.close()));
    for (const r of results) if (r.status === 'rejected') this.cleanupErrors.push('WebSocket close timeout');
    if (final) {
      clearTimeout(this.workTimer);
      if (!this.peers.size) clearTimeout(this.watchdog);
    }
  }
}

export class IdPool {
  constructor(runtime) { this.runtime = runtime; this.ids = []; this.refill = null; }
  async next(prefix = 'envid', cleanup = false) {
    while (!this.ids.length) {
      if (!this.refill) this.refill = this.runtime.fetch('/uctp/ids?count=256', {}, cleanup).then((response) => {
        assert.equal(response.status, 200, 'ID allocation status');
        assert.match(response.headers.get('cache-control') ?? '', /no-store/i, 'ID responses must not be cached');
        this.ids.push(...validateIds(response.json(), 256));
      }).finally(() => { this.refill = null; });
      await this.refill;
    }
    return prefix + this.ids.shift();
  }
}

export class Peer {
  constructor(runtime, pool, label, retain = true) {
    this.runtime = runtime; this.pool = pool; this.label = label; this.retain = retain;
    this.frames = []; this.sequence = 0; this.waiters = new Set(); this.listeners = new Set();
    this.closed = false; this.failure = null;
  }
  async open() {
    this.runtime.check();
    if (this.runtime.peers.size >= this.runtime.options.maxConnections) throw new Error('Concurrent connection limit reached');
    this.runtime.peers.add(this); // includes sockets still opening or closing
    this.runtime.peakConnections = Math.max(this.runtime.peakConnections, this.runtime.peers.size);
    this.runtime.onPeerCount?.(this.runtime.peers.size);
    const url = new URL(this.runtime.options.target); url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:'; url.pathname = '/uctp';
    // Node 24 native WebSocket follows the WebSocket spec: handshake redirects are errors.
    this.ws = new WebSocket(url);
    this.ws.addEventListener('message', (event) => {
      try {
        const raw = JSON.parse(String(event.data));
        if (raw.type === 'auth.session' && (typeof raw.payload?.session_token !== 'string' || !raw.payload.session_token)) {
          throw new Error('auth.session must carry a nonempty session token');
        }
        if (raw.type === 'auth.session') this.sessionToken = raw.payload.session_token;
        const frame = this.runtime.redact(raw);
        assert.equal(frame.v, 1, 'server envelope version');
        assert.match(frame.id, /^envid[1-9]\d{0,18}$/, 'server envelope ID');
        const entry = { frame, at: now(), seq: ++this.sequence };
        if (this.retain) { this.frames.push(entry); if (this.frames.length > 4096) this.frames.shift(); }
        for (const fn of this.listeners) fn(entry);
        for (const waiter of [...this.waiters]) if (entry.seq > waiter.after && waiter.predicate(frame)) waiter.finish(null, entry);
      } catch { this.fail(new Error(`${this.label}: malformed server protocol frame or listener failure`)); }
    });
    this.ws.addEventListener('close', () => { this.closed = true; this.runtime.peers.delete(this); this.fail(new Error(`${this.label}: WebSocket closed`)); });
    this.ws.addEventListener('error', () => this.fail(new Error(`${this.label}: WebSocket transport error`)));
    await new Promise((resolve, reject) => {
      const signal = this.runtime.abort.signal;
      const finish = (error) => {
        clearTimeout(timer); this.ws.removeEventListener('open', opened); this.ws.removeEventListener('error', failed); this.ws.removeEventListener('close', failed);
        signal.removeEventListener('abort', aborted); error ? reject(error) : resolve();
      };
      const opened = () => finish(); const failed = () => finish(new Error(`${this.label}: connect failed`));
      const aborted = () => finish(new Error(`${this.label}: connect interrupted`));
      const timer = setTimeout(() => finish(new Error(`${this.label}: connect timeout`)), Math.max(1, Math.min(this.runtime.options.timeoutMs, this.runtime.workDeadline - now())));
      this.ws.addEventListener('open', opened, { once: true }); this.ws.addEventListener('error', failed, { once: true }); this.ws.addEventListener('close', failed, { once: true });
      signal.addEventListener('abort', aborted, { once: true });
      if (signal.aborted) aborted();
    });
    return this;
  }
  fail(error) { this.failure ??= error; for (const waiter of [...this.waiters]) waiter.finish(error); }
  wait(predicate, { after = this.sequence, timeoutMs = this.runtime.options.timeoutMs, cleanup = false } = {}) {
    const existing = this.frames.find((entry) => entry.seq > after && predicate(entry.frame));
    if (existing) return Promise.resolve(existing);
    if (!cleanup && this.failure) return Promise.reject(this.failure);
    let finish;
    const promise = new Promise((resolve, reject) => {
      const timer = setTimeout(() => finish(new Error(`${this.label}: protocol response timeout`)), Math.max(1, Math.min(timeoutMs, this.runtime.deadline - now())));
      const waiter = { after, predicate, finish: (error, entry) => { clearTimeout(timer); this.waiters.delete(waiter); error ? reject(error) : resolve(entry); } };
      finish = waiter.finish; this.waiters.add(waiter);
    });
    promise.catch(() => {}); // caller still observes rejection; parallel waits never leak unhandled rejection
    promise.cancel = () => finish(new Error('Wait cancelled'));
    return promise;
  }
  async prepare(type, payload = {}, scope = {}, cleanup = false) { return envelope(await this.pool.next('envid', cleanup), type, payload, scope); }
  async transmit(env, { cleanup = false, onSent } = {}) {
    await this.runtime.permit(cleanup);
    if (this.ws.readyState !== WebSocket.OPEN) throw new Error(`${this.label}: socket not open`);
    if (!cleanup && this.failure) throw this.failure;
    if (this.ws.bufferedAmount > 1024 * 1024) throw new Error(`${this.label}: outbound buffer limit reached`);
    const at = now();
    onSent?.(at);
    this.ws.send(JSON.stringify(env));
    return at;
  }
  async request(type, payload = {}, scope = {}, { expected = 'ack', cleanup = false } = {}) {
    const env = await this.prepare(type, payload, scope, cleanup);
    const pending = this.wait((frame) => frame.in_reply_to === env.id && (frame.type === expected || frame.type === 'error'), { cleanup });
    try {
      await this.transmit(env, { cleanup });
      const entry = await pending.catch(error => { throw new Error(`${this.label}: ${type}: ${error.message}`); });
      if (expected !== 'error' && entry.frame.type === 'error') throw new Error(`${this.label}: ${type} rejected code=${entry.frame.payload?.code}`);
      assert.equal(entry.frame.type, expected, `${type} response type`);
      return entry;
    } finally { pending.cancel?.(); }
  }
  async authenticate(token) {
    const challenge = await this.request('auth.hello', { auth_methods: ['bearer'], capabilities: {}, device: { id: 'node-protocol-test', kind: 'test' } }, {}, { expected: 'auth.challenge' });
    assert.ok(challenge.frame.payload?.accepted_methods?.includes('bearer'), 'bearer challenge support');
    assert.equal(typeof challenge.frame.payload?.nonce, 'string', 'challenge nonce');
    const env = await this.prepare('auth.response', { method: 'bearer', credential: token, nonce: challenge.frame.payload.nonce }, { in_reply_to: challenge.frame.id });
    const pending = this.wait((frame) => frame.type === 'auth.session' || (frame.type === 'error' && frame.in_reply_to === env.id));
    try {
      await this.transmit(env);
      const { frame } = await pending.catch(error => { throw new Error(`${this.label}: auth.response: ${error.message}; observed=${this.frames.map(entry => entry.frame.type).join(',')}`); });
      assert.equal(frame.type, 'auth.session', 'valid development authentication must succeed');
      assert.equal(typeof frame.payload?.identity_id, 'string', 'authenticated identity');
      this.identityId = frame.payload.identity_id;
      return this;
    } finally { pending.cancel?.(); }
  }
  async close() {
    if (!this.ws || this.closed) { this.runtime.peers.delete(this); return; }
    this.fail(new Error(`${this.label}: client cleanup`));
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`${this.label}: close handshake timeout`)), Math.max(1, Math.min(2000, this.runtime.deadline - now())));
      this.ws.addEventListener('close', () => { clearTimeout(timer); resolve(); }, { once: true });
      try { this.ws.close(1000, 'test complete'); } catch { clearTimeout(timer); reject(new Error('WebSocket cleanup failed')); }
    });
  }
}

export async function enqueue(customer, tracked) {
  const queued = await customer.request('workforce.enqueue', { skills: ['general'], policy: 'persistent', metadata: { source: 'bounded-node-test' } }, {}, { expected: 'workforce.queued' });
  const { cid, customer_participant_id: participantId } = queued.frame.payload ?? {};
  assert.match(cid ?? '', /^cid[1-9]\d{0,18}$/, 'queued CID');
  assert.match(participantId ?? '', /^pid[1-9]\d{0,18}$/, 'queued participant ID');
  const conv = { cid, participantId, customer, customerCursor: queued.seq - 1 };
  tracked.push(conv); // register cleanup as soon as the server allocates a CID
  return conv;
}

export async function assigned(conv, agent, agentCursor = 0) {
  const [customerEvent, agentEvent] = await Promise.all([
    conv.customer.wait((f) => f.type === 'workforce.assigned' && f.cid === conv.cid, { after: conv.customerCursor }),
    agent.wait((f) => f.type === 'workforce.assigned' && f.cid === conv.cid, { after: agentCursor }),
  ]);
  assert.equal(customerEvent.frame.payload.agent_identity_id, agent.identityId, 'assigned customer must name this agent');
  assert.equal(agentEvent.frame.payload.customer_identity_id, conv.customer.identityId, 'assigned agent must name this customer');
  conv.assigned = true;
  return conv;
}

export async function closeConsultations(agent, tracked, errors) {
  const remaining = tracked.filter((conv) => !conv.closed);
  // Bounded parallel cleanup avoids 199 * timeout serial stalls.
  for (let start = 0; start < remaining.length; start += 32) {
    await Promise.all(remaining.slice(start, start + 32).map(async (conv) => {
      const member = conv.assigned ? agent : conv.customer; // a still-queued CID has no agent member yet
      try { await member.request('conversation.close', { reason: 'test-cleanup' }, { cid: conv.cid }, { cleanup: true }); conv.closed = true; }
      catch { errors.push(`conversation.close failed for ${conv.cid}`); }
    }));
  }
}
