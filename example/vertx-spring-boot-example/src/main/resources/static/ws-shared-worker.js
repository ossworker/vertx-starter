// 方案 A：WebSocket 连接由 SharedWorker 持有。
// 普通页面 F5 会销毁页面内 JS 上下文，页内 WebSocket 必然关闭；
// 而 SharedWorker 独立于页面存活，连接可在刷新后继续存在（刷新时 TCP 不断）。
// 多标签页共享同一个 worker，因此也共享同一条到后端的 WS 连接。
'use strict';

var ports = [];        // 所有连到本 worker 的页面端口（多标签页）
var ws = null;         // 后端 WebSocket（全局唯一）
var params = null;     // 连接参数
var lastSeq = 0;       // 最近一条 chat 序号，用于断线 resume
var backoff = 0;       // 重连退避计时
var timer = null;
var cachedWelcome = null; // 最近一次 welcome 原文（新页面 attach 时重放，刷新后恢复 UI）
var cachedRoster = null;  // 最近一次 roster 原文

function enc(s) { return encodeURIComponent(s == null ? '' : String(s)); }

// SharedWorker 不会出现在页面级 DevTools 里（Chrome 设计如此，只能经
// chrome://inspect/#workers 查看），且默认一句日志都没有。这里加带前缀的
// 生命周期日志，便于在 worker 专属控制台/console 上下文下拉里观察它。
function log() {
  try {
    var args = ['[SharedWorker]'].concat(Array.prototype.slice.call(arguments));
    console.log.apply(console, args);
  } catch (e) { /* ignore */ }
}

self.onconnect = function (e) {
  var port = e.ports[0];
  ports.push(port);
  log('port attached, total ports =', ports.length);
  // 新页面（典型场景：F5 刷新）挂接到 worker。若连接仍然活着，把缓存的会话状态
  // 重放给这个端口——页面据此恢复聊天界面，既不需要重新登录，也不会新建连接。
  // 没有存活连接时什么都不发，页面停留在登录页（首次访问的正常流程）。
  if (ws && ws.readyState === 1 && cachedWelcome) {
    log('live connection found -> replaying cached welcome/roster to the new port');
    try {
      port.postMessage({ type: 'status', status: 'open' });
      port.postMessage({ type: 'message', data: cachedWelcome });
      if (cachedRoster) port.postMessage({ type: 'message', data: cachedRoster });
    } catch (err) { /* drop dead port */ }
  } else {
    log('no live connection -> new page stays on login page');
  }
  port.onmessage = function (ev) {
    var d = ev.data;
    if (d.type === 'connect') {
      params = d.params;
      lastSeq = params.resume || 0;
      backoff = 0;
      cachedWelcome = null;
      cachedRoster = null;
      log('connect requested ->', params.name + ' #' + params.room + ' (uid=' + params.uid + ', resume=' + lastSeq + ')');
      openWs();
    } else if (d.type === 'send') {
      if (ws && ws.readyState === 1) {
        try { ws.send(JSON.stringify(d.data)); } catch (err) { /* ignore */ }
      }
    } else if (d.type === 'close') {
      // 页面主动要求断开（如切换连接模式）：关连接、清缓存、取消重连。
      log('close requested -> stopping, no reconnect');
      if (timer) { clearTimeout(timer); timer = null; }
      backoff = 0;
      if (ws) { try { ws.onclose = null; ws.close(); } catch (err) {} ws = null; }
      cachedWelcome = null;
      cachedRoster = null;
    } else if (d.type === 'ping') {
      port.postMessage({ type: 'pong' });
    }
  };
  port.onmessageerror = function () { /* ignore */ };
};

function broadcast(obj) {
  for (var i = 0; i < ports.length; i++) {
    try { ports[i].postMessage(obj); } catch (err) { /* drop dead port */ }
  }
}

function openWs() {
  if (!params) return;
  // 防止重复 connect 时旧连接泄漏（切模式/重复登录会再次走到这里）。
  if (ws) { try { ws.onclose = null; ws.close(); } catch (err) {} ws = null; }
  var url = params.baseWs + '?name=' + enc(params.name) + '&role=' + enc(params.role) +
            '&room=' + enc(params.room) + '&uid=' + enc(params.uid) + '&resume=' + lastSeq;
  log('opening WebSocket ->', url);
  broadcast({ type: 'status', status: 'connecting' });
  try {
    ws = new WebSocket(url);
  } catch (err) {
    log('WebSocket construction failed:', err && err.message);
    scheduleReconnect();
    return;
  }
  ws.onopen = function () {
    backoff = 0;
    log('WebSocket open');
    broadcast({ type: 'status', status: 'open' });
  };
  ws.onmessage = function (ev) {
    var m;
    try { m = JSON.parse(ev.data); } catch (err) { return; }
    if (m.type === 'chat' && m.seq) { lastSeq = m.seq; }
    // 缓存会话状态快照，供刷新后的新页面重放恢复。
    if (m.type === 'welcome') cachedWelcome = ev.data;
    if (m.type === 'roster') cachedRoster = ev.data;
    broadcast({ type: 'message', data: ev.data });
  };
  ws.onclose = function () {
    log('WebSocket closed -> scheduling reconnect');
    broadcast({ type: 'status', status: 'reconnecting' });
    scheduleReconnect();
  };
  ws.onerror = function () { /* 交给 onclose 处理重连 */ };
}

function scheduleReconnect() {
  if (timer) return;
  var delay = Math.min(backoff + 1000, 10000);
  backoff = delay;
  timer = setTimeout(function () {
    timer = null;
    openWs();
  }, delay);
}
