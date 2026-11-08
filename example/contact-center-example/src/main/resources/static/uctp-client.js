'use strict';

class UctpClient {
  constructor(options = {}) {
    this.handlers = new Map();
    this.pending = new Map();
    this.sessionToken = null;
    this.identityId = null;
    this.connected = false;
    this.idPool = [];
    this.refillPromise = null;
    this.sendQueue = Promise.resolve();
    this.fetch = options.fetch || ((...args) => fetch(...args));
    this.WebSocket = options.WebSocket || globalThis.WebSocket;
    this.storage = options.storage || globalThis.sessionStorage;
    this.location = options.location || globalThis.location;
    this.timeoutMs = options.timeoutMs || 12000;
    this.generation = 0;
  }

  on(type, fn) {
    if (!this.handlers.has(type)) this.handlers.set(type, new Set());
    this.handlers.get(type).add(fn);
    return () => this.handlers.get(type).delete(fn);
  }

  emit(env) {
    for (const fn of this.handlers.get(env.type) || []) {
      Promise.resolve().then(() => fn(env)).catch((error) => {
        if (env.type !== 'client.error') this.reportError(error);
      });
    }
  }

  reportError(error) {
    this.emit({type: 'client.error', payload: {reason: error.message || String(error)}});
  }

  refillIds() {
    if (this.refillPromise) return this.refillPromise;
    this.refillPromise = this.fetch('/uctp/ids?count=256', {cache: 'no-store'})
      .then(async (response) => {
        if (!response.ok) throw new Error('ID 分配失败: ' + response.status);
        const body = await response.json();
        if (!Array.isArray(body.ids) || body.ids.length !== 256 ||
            body.ids.some((id) => typeof id !== 'string' || !/^\d+$/.test(id)) ||
            new Set(body.ids).size !== body.ids.length) throw new Error('无效的雪花 ID 响应');
        this.idPool.push(...body.ids);
      }).finally(() => { this.refillPromise = null; });
    return this.refillPromise;
  }

  async newId(prefix) {
    if (!['cid', 'sid', 'connid', 'msgid', 'pid', 'envid', 'strmid'].includes(prefix)) throw new Error('无效的 ID 前缀');
    // Every waiter must recheck: one refill cannot serve >256 concurrent callers.
    while (!this.idPool.length) await this.refillIds();
    return prefix + this.idPool.shift();
  }

  storedToken() {
    try { return this.storage?.getItem('uctp.token.' + this.token); } catch (_) { return null; }
  }

  storeToken(value) {
    try {
      const key = 'uctp.token.' + this.token;
      if (value) this.storage?.setItem(key, value); else this.storage?.removeItem(key);
    } catch (_) { /* Storage is optional. */ }
  }

  async connect(token) {
    this.close();
    const generation = ++this.generation;
    this.token = token;
    if (!token) throw new Error('请输入演示令牌');
    // Initialize before creating an authentication promise to avoid orphan rejections.
    await this.refillIds();
    if (generation !== this.generation) throw new Error('连接已取消');
    const proto = this.location.protocol === 'https:' ? 'wss' : 'ws';
    const ws = new this.WebSocket(`${proto}://${this.location.host}/uctp`);
    this.ws = ws;
    return new Promise((resolve, reject) => {
      let settled = false;
      let resumed = Boolean(this.storedToken());
      const timer = setTimeout(() => fail(new Error('认证超时')), this.timeoutMs);
      const finish = (error, payload) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        this.abortAuth = null;
        if (error) reject(error); else resolve(payload);
      };
      const fail = (error) => {
        finish(error);
        if (generation !== this.generation) return;
        this.connected = false;
        if (ws.readyState < 2) ws.close();
      };
      this.abortAuth = (error) => finish(error);
      const hello = (resume) => this.send('auth.hello', {
        device: {id: 'browser', kind: 'web', platform: 'browser', sdk_version: 'uctp-web/1'},
        auth_methods: ['bearer'],
        capabilities: {audio_codecs: [{name: 'opus', params: {sample_rate: 48000, channels: 1}}]},
        ...(resume ? {resume_token: resume} : {}),
      }).catch(fail);
      ws.onopen = () => { if (generation === this.generation) hello(this.storedToken()); };
      ws.onmessage = (event) => {
        if (generation !== this.generation) return;
        let env;
        try {
          env = JSON.parse(event.data);
          if (!env || env.v !== 1 || typeof env.type !== 'string') throw new Error('无效的 UCTP v1 信封');
        } catch (error) { this.reportError(error); return; }
        this.handleResponse(env);
        if (env.type === 'auth.challenge') {
          this.send('auth.response', {method: 'bearer', credential: this.token}, {in_reply_to: env.id}).catch(fail);
        } else if (env.type === 'auth.session') {
          this.sessionToken = env.payload.session_token;
          this.identityId = env.payload.identity_id;
          this.connected = true;
          this.storeToken(this.sessionToken);
          finish(null, env.payload);
        } else if (env.type === 'error' && !settled) {
          if (resumed && /resume.*(invalid|expired)|invalid.*resume/i.test(env.payload.reason || '')) {
            resumed = false;
            this.storeToken(null);
            hello(null);
          } else fail(this.protocolError(env));
        }
        this.emit(env);
      };
      ws.onerror = () => {
        if (generation !== this.generation) return;
        if (!settled) fail(new Error('WebSocket 连接失败'));
        else this.reportError(new Error('WebSocket 连接错误'));
      };
      ws.onclose = () => {
        if (generation !== this.generation) return;
        this.connected = false;
        finish(new Error('WebSocket 已断开'));
        this.rejectPending(new Error('连接已断开；提交结果未知，请重连查看历史'));
        this.emit({type: 'client.closed', payload: {}});
      };
    });
  }

  protocolError(env) {
    const error = new Error(`${env.payload?.code || '错误'}: ${env.payload?.reason || '请求失败'}`);
    error.code = env.payload?.code;
    error.envelope = env;
    return error;
  }

  handleResponse(env) {
    const item = this.pending.get(env.in_reply_to);
    if (!item) return;
    if (env.type === 'error') item.finish(this.protocolError(env));
    else if (item.expected.has(env.type)) item.finish(null, env);
  }

  async envelope(type, payload, scope = {}) {
    const env = {v: 1, type, id: await this.newId('envid'), ts: new Date().toISOString(), payload};
    for (const field of ['cid', 'sid', 'connid', 'in_reply_to']) if (scope[field]) env[field] = scope[field];
    return env;
  }

  enqueue(operation) {
    const result = this.sendQueue.then(operation);
    this.sendQueue = result.catch(() => {});
    return result;
  }

  send(type, payload, scope = {}) {
    const generation = this.generation;
    return this.enqueue(async () => {
      const env = await this.envelope(type, payload, scope);
      if (generation !== this.generation) throw new Error('连接已切换，旧请求已取消');
      return this.sendRaw(env);
    });
  }

  request(type, payload = {}, scope = {}, options = {}) {
    const generation = this.generation;
    return new Promise((resolve, reject) => {
      this.enqueue(async () => {
        if (!this.connected) throw new Error('尚未认证，请先连接');
        const env = await this.envelope(type, payload, scope);
        if (generation !== this.generation) throw new Error('连接已切换，旧请求已取消');
        let timer;
        const finish = (error, reply) => {
          clearTimeout(timer);
          this.pending.delete(env.id);
          if (error) reject(error); else resolve(reply);
        };
        this.pending.set(env.id, {type, expected: new Set(options.expected || ['ack']), finish});
        timer = setTimeout(() => finish(new Error('请求超时，提交结果未知；请重连查看历史')), options.timeoutMs || this.timeoutMs);
        try { this.sendRaw(env); } catch (error) { finish(error); }
      }).catch(reject);
    });
  }

  sendRaw(env) {
    if (!this.ws || this.ws.readyState !== 1) throw new Error('WebSocket 未连接');
    this.ws.send(JSON.stringify(env));
    return env;
  }

  rejectPending(error) {
    for (const item of [...this.pending.values()]) item.finish(error);
  }

  close() {
    ++this.generation;
    this.connected = false;
    this.identityId = null;
    this.sessionToken = null;
    this.abortAuth?.(new Error('连接已取消'));
    this.rejectPending(new Error('连接已关闭；提交结果未知'));
    if (this.ws && this.ws.readyState < 2) this.ws.close();
    this.ws = null;
  }
}

function logLine(el, text) {
  const line = document.createElement('div');
  line.textContent = `[${new Date().toLocaleTimeString()}] ${text}`;
  el.appendChild(line);
  while (el.children.length > 160) el.firstChild.remove();
  el.scrollTop = el.scrollHeight;
}

if (typeof module !== 'undefined' && module.exports) module.exports = {UctpClient};
