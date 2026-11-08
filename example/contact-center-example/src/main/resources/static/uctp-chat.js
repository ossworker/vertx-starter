'use strict';

const UctpChat = (() => {
  const MAX_UPLOAD_BYTES = 10485760;
  const RECEIPT_RANK = {pending: 0, retained: 1, submitted: 1, received: 2, read: 3};
  const RECEIPT_TEXT = {pending: '提交中', retained: '服务器历史记录', submitted: '已提交', received: '对端收到', read: '已读', failed: '提交失败 / 结果未知'};

  function safeUrl(value, base = globalThis.location?.origin || 'http://localhost', filesOnly = false) {
    if (typeof value !== 'string' || !value || /[\\\s\u0000-\u001f\u007f]/.test(value) || /%(2f|5c)/i.test(value)) return null;
    try {
      const rawDecoded = decodeURIComponent(value);
      if (/\\|(^|\/)\.\.?($|\/|[?#])/.test(rawDecoded)) return null;
      const url = new URL(value, base);
      const origin = new URL(base).origin;
      if (!['http:', 'https:'].includes(url.protocol) || url.origin !== origin || url.username || url.password) return null;
      const decoded = decodeURIComponent(url.pathname);
      if (/%|\\|\/\/|(^|\/)\.\.?($|\/)/.test(decoded)) return null;
      // Explicit local routes only: never allow arbitrary executable same-origin links.
      if (!/^\/api\/files\/[a-zA-Z0-9._/-]+$/.test(decoded)) return null;
      if (filesOnly && (url.search || url.hash)) return null;
      return url.pathname + url.search + url.hash;
    } catch (_) { return null; }
  }

  function parseBody(body) {
    if (body && typeof body === 'object' && !Array.isArray(body)) return body;
    if (typeof body !== 'string') return null;
    try {
      const parsed = JSON.parse(body);
      return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? parsed : null;
    } catch (_) { return null; }
  }

  function node(tag, text, className) {
    const el = document.createElement(tag);
    if (text !== undefined) el.textContent = String(text);
    if (className) el.className = className;
    return el;
  }

  function renderBody(payload, onAction, base) {
    const root = node('div', undefined, 'message-content');
    const type = payload.content_type || 'text/plain';
    const body = parseBody(payload.body);
    const fallback = () => root.append(node('p', typeof payload.body === 'string' ? payload.body : JSON.stringify(payload.body ?? '')));
    if (type === 'text/plain' || !body) { fallback(); return root; }
    const media = /^application\/vnd\.uctp\.media\.(image|audio|video|file)\+json$/.exec(type);
    if (media) {
      const url = safeUrl(body.url, base, true);
      if (!url) { root.append(node('p', '附件地址不安全，已阻止加载')); fallback(); return root; }
      if (media[1] === 'image') {
        const img = node('img');
        img.src = url; img.alt = String(body.filename || body.name || '图片附件'); img.loading = 'lazy';
        img.onerror = () => { img.replaceWith(node('p', '图片加载失败')); };
        root.append(img);
      } else if (media[1] === 'audio' || media[1] === 'video') {
        const player = node(media[1]);
        player.src = url; player.controls = true; player.preload = 'none';
        player.onerror = () => { player.replaceWith(node('p', '媒体加载失败，请下载附件')); };
        root.append(player);
      }
      const link = node('a', body.filename || body.name || '下载附件', 'attachment-link');
      link.href = url; link.target = '_blank'; link.rel = 'noopener noreferrer';
      root.append(link);
      if (Number.isFinite(body.size)) root.append(node('small', `${(body.size / 1024).toFixed(1)} KB`));
      return root;
    }
    const card = /^application\/vnd\.uctp\.card\.(product|order|invoice|address|location|refund|action)\+json$/.exec(type);
    if (card) {
      const labels = {product: '商品信息', order: '订单信息', invoice: '发票信息', address: '地址确认', location: '位置信息', refund: '退款 / 退货', action: '操作请求'};
      root.classList.add('rich-card');
      root.append(node('small', labels[card[1]], 'eyebrow'), node('h3', body.title || body.name || labels[card[1]]));
      for (const [key, label] of Object.entries({product_id: '商品编号', order_id: '订单号', order_no: '订单号', amount: '金额', price: '价格', currency: '币种', status: '状态', description: '说明', address: '地址', reason: '原因'})) {
        if (body[key] !== undefined) root.append(node('p', `${label}：${typeof body[key] === 'object' ? JSON.stringify(body[key]) : body[key]}`));
      }
      if (Array.isArray(body.items)) for (const item of body.items.slice(0, 20)) root.append(node('p', typeof item === 'object' ? `${item.name || item.title || '项目'} × ${item.quantity || 1}` : item));
      const image = safeUrl(body.image_url, base, true);
      if (image) { const img = node('img'); img.src = image; img.alt = '商品图片'; img.loading = 'lazy'; root.append(img); }
      const url = safeUrl(body.url, base);
      if (url) { const link = node('a', '查看附件'); link.href = url; link.rel = 'noopener noreferrer'; link.target = '_blank'; root.append(link); }
      for (const action of Array.isArray(body.actions) ? body.actions.slice(0, 6) : []) {
        if (!action || typeof action !== 'object') continue;
        const actionUrl = safeUrl(action.url, base);
        if (action.url) {
          if (actionUrl) { const a = node('a', action.label || '查看附件'); a.href = actionUrl; a.rel = 'noopener noreferrer'; a.target = '_blank'; root.append(a); }
        } else if (typeof action.action === 'string' && onAction) {
          const button = node('button', action.label || action.action);
          button.type = 'button';
          button.onclick = () => onAction(payload.msg_id, action.action, button);
          root.append(button);
        }
      }
      return root;
    }
    if (type === 'application/vnd.uctp.card-action+json') {
      root.append(node('p', `操作回执：${body.action || '未知操作'}（仅传递给业务系统，不代表执行成功）`));
      return root;
    }
    fallback();
    return root;
  }

  class ConversationStore {
    constructor(identityId = null) { this.identityId = identityId; this.conversations = new Map(); this.selectedCid = null; }
    ensure(cid) {
      if (!cid) throw new Error('缺少 cid');
      if (!this.conversations.has(cid)) this.conversations.set(cid, {cid, participants: [], messages: new Map(), unread: 0, status: '咨询中', draft: '', typing: new Map(), readIds: new Set(), calls: new Set(), assigned: false});
      return this.conversations.get(cid);
    }
    get selected() { return this.conversations.get(this.selectedCid); }
    select(cid) { const conv = this.ensure(cid); this.selectedCid = cid; conv.unread = 0; return conv; }
    participant(conv) { return conv.participants.find((p) => p.identity_id === this.identityId)?.participant_id; }
    opened(env) {
      const conv = this.ensure(env.cid);
      if (Array.isArray(env.payload.participants)) conv.participants = env.payload.participants;
      conv.metadata = env.payload.metadata || conv.metadata || {};
      conv.openedAt = env.payload.opened_at || conv.openedAt;
      return conv;
    }
    receive(env, history = false) {
      const conv = this.ensure(env.cid);
      const payload = env.payload;
      if (!payload?.msg_id) return {conv, added: false};
      const own = payload.from === this.participant(conv);
      const existing = conv.messages.get(payload.msg_id);
      if (existing) {
        if (history && existing.own && existing.status === 'failed') this.receipt(env.cid, payload.msg_id, 'retained');
        return {conv, added: false, own};
      }
      conv.messages.set(payload.msg_id, {...payload, own, ts: env.ts, status: own ? 'retained' : null, history});
      if (!own && !history && env.cid !== this.selectedCid) conv.unread++;
      return {conv, added: true, own};
    }
    outgoing(cid, payload) {
      const conv = this.ensure(cid);
      conv.messages.set(payload.msg_id, {...payload, own: true, ts: new Date().toISOString(), status: 'pending'});
      return conv;
    }
    receipt(cid, msgId, status) {
      const msg = this.conversations.get(cid)?.messages.get(msgId);
      if (!msg?.own) return;
      // Late ACK must not downgrade a real receipt; failures do not erase receipt evidence.
      if (status === 'failed') { if (msg.status === 'pending') msg.status = status; return; }
      if ((RECEIPT_RANK[status] ?? -1) > (RECEIPT_RANK[msg.status] ?? -1) || (status === 'submitted' && msg.status === 'retained')) msg.status = status;
    }
    unreadMessages(cid) {
      const conv = this.conversations.get(cid);
      if (!conv || cid !== this.selectedCid) return [];
      return [...conv.messages.values()].filter((m) => !m.own && !conv.readIds.has(m.msg_id)).map((m) => m.msg_id);
    }
    search(query) {
      const needle = query.trim().toLowerCase();
      return [...this.conversations.values()].filter((conv) => !needle || `${conv.cid} ${conv.customerIdentity || ''} ${conv.agentIdentity || ''} ${conv.participants.map((p) => p.display_name || p.identity_id).join(' ')} ${[...conv.messages.values()].map((m) => typeof m.body === 'string' ? m.body : '').join(' ')}`.toLowerCase().includes(needle));
    }
  }

  function uploadKind(file) {
    if (/^image\/(png|jpeg|gif|webp|avif)$/.test(file.type)) return 'image';
    if (file.type.startsWith('video/')) return 'video';
    if (file.type.startsWith('audio/')) return 'audio';
    return 'file';
  }

  async function upload(file, maxBytes = MAX_UPLOAD_BYTES, fetcher = globalThis.fetch, base) {
    if (!file || file.size <= 0) throw new Error('请选择非空文件');
    if (file.size > Math.min(MAX_UPLOAD_BYTES, maxBytes)) throw new Error('文件超过上传限制（最多 10 MB）');
    const kind = uploadKind(file);
    const response = await fetcher(`/api/upload?kind=${kind}&filename=${encodeURIComponent(file.name)}`, {method: 'POST', headers: {'Content-Type': 'application/octet-stream'}, body: file, cache: 'no-store'});
    if (!response.ok) throw new Error('上传失败: ' + response.status);
    const data = await response.json();
    const url = safeUrl(data.url, base, true);
    if (!url || !Number.isFinite(data.size) || data.size < 0 || data.size > Math.min(MAX_UPLOAD_BYTES, maxBytes)) throw new Error('上传返回了无效的文件地址或大小');
    return {content_type: `application/vnd.uctp.media.${kind}+json`, body: JSON.stringify({url, size: data.size, filename: file.name}), body_encoding: 'utf8'};
  }

  return {ConversationStore, safeUrl, parseBody, node, renderBody, upload, uploadKind, MAX_UPLOAD_BYTES, RECEIPT_TEXT};
})();
if (typeof module !== 'undefined' && module.exports) module.exports = UctpChat;
