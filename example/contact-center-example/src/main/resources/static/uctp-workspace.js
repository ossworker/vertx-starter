'use strict';

(() => {
  const isAgent = document.body.dataset.role === 'agent';
  const $ = (id) => document.getElementById(id);
  const {node, ConversationStore, renderBody, RECEIPT_TEXT} = UctpChat;
  const client = new UctpClient();
  let store = new ConversationStore();
  let connecting = false;
  let loginReady = false;
  let configLoaded = false;
  let maxUploadBytes = UctpChat.MAX_UPLOAD_BYTES;
  let uploading = false;
  let enqueueing = false;
  let roster = null;
  let typingSentAt = 0;
  let typingTimer;
  const historyLoaded = new Set();
  const historyLoading = new Set();

  // Shared semantic DOM: no HTML-string interpolation, external UI libraries or CDN.
  function el(tag, attrs = {}, children = []) {
    const result = node(tag);
    for (const [key, value] of Object.entries(attrs)) {
      if (key === 'text') result.textContent = value;
      else if (key === 'class') result.className = value;
      else if (key in result && key !== 'list' && !key.startsWith('aria-')) result[key] = value;
      else result.setAttribute(key, value);
    }
    result.append(...children);
    return result;
  }
  function button(id, text, className = '', disabled = true) { return el('button', {id, text, class: className, type: 'button', disabled}); }
  function field(text, control) { return el('label', {}, [node('span', text), control]); }
  function heading(text, counterId) { return el('div', {class: 'section-heading'}, [node('h2', text), ...(counterId ? [el('span', {id: counterId, class: 'counter', text: '0'})] : [])]); }
  function select(id, values) { return el('select', {id}, values.map(([value, text]) => el('option', {value, text}))); }
  function buildUi() {
    const connection = el('section', {class: 'connection-bar', 'aria-label': '连接与接待状态'}, [
      field('演示令牌', el('input', {id: 'token', value: isAgent ? 'agent-1' : 'customer-1', type: 'password', autocomplete: 'off', spellcheck: false})),
      field(isAgent ? '接待技能' : '咨询技能', el('input', {id: 'skills', value: 'general', placeholder: '多个技能用逗号分隔'})),
      button('btnConnect', isAgent ? '上线 / 重连' : '连接 / 重连', 'primary', false),
    ]);
    const stateSelect = select('agentState', [['available', '可接待'], ['busy', '忙碌'], ['break', '小休']]);
    stateSelect.disabled = true;
    if (isAgent) connection.append(field('接待状态', stateSelect), el('span', {id: 'rosterSummary', class: 'muted', text: '状态只影响新咨询，不中断已有会话'}));
    else connection.append(button('btnEnqueue', '开始咨询', 'primary'), el('span', {id: 'queueInfo', class: 'muted', text: '开始咨询仅创建聊天，不会自动开启通话'}));
    const sidebar = el('aside', {class: 'conversation-sidebar panel', 'aria-label': '咨询列表'}, [
      heading(isAgent ? '接待会话' : '我的咨询', 'conversationCount'),
      field('搜索咨询', el('input', {id: 'search', type: 'search', placeholder: '客户、会话或消息'})),
      node('p', '每个会话独立管理消息与通话', 'list-caption'),
      el('div', {id: 'conversationList', class: 'conversation-list'}),
      node('p', isAgent ? '忙碌 / 小休仍可处理已有咨询' : '语音视频需对方明确接听', 'sidebar-foot'),
    ]);
    const chat = el('section', {class: 'chat-panel panel', 'aria-label': '聊天工作区'}, [
      el('header', {class: 'chat-heading'}, [el('div', {}, [node('span', '当前咨询', 'eyebrow'), el('h1', {id: 'conversationTitle', text: isAgent ? '选择会话，开始服务' : '您好，有什么可以帮您？'}), el('p', {id: 'conversationMeta', class: 'muted', text: isAgent ? '上线后，新分配会话会出现在左侧' : '连接后点击“开始咨询”，由客服为您服务'})]), el('span', {id: 'conversationStatus', class: 'tag', text: '等待连接'})]),
      el('div', {class: 'chat-actions'}, [button('btnVoice', '语音呼叫'), button('btnVideo', '视频呼叫'), ...(isAgent ? [button('btnHandoff', '转接技能组')] : []), button('btnCloseConversation', '结束整个咨询', 'danger-quiet')]),
      el('div', {id: 'messages', class: 'messages', role: 'log', 'aria-label': '消息记录', 'aria-live': 'polite'}),
      el('div', {id: 'typingStatus', class: 'typing-status', role: 'status'}),
    ]);
    const quick = el('section', {id: 'quickReplies', class: 'quick-replies', 'aria-label': '快捷回复'}, [node('span', '快捷回复')]);
    if (isAgent) {
      for (const [title, reply] of [['问候', '您好，很高兴为您服务。请问有什么可以帮您？'], ['正在核实', '收到，我正在为您核实，请稍等。'], ['确认解决', '感谢您的耐心，问题是否已经解决？']]) quick.append(el('button', {type: 'button', text: title, 'data-reply': reply, disabled: true}));
      chat.append(quick);
    }
    chat.append(el('form', {id: 'composer', class: 'composer'}, [
      field('消息内容', el('textarea', {id: 'chatText', rows: 3, placeholder: '输入消息… Enter 发送，Shift + Enter 换行', disabled: true})),
      el('div', {class: 'composer-footer'}, [el('div', {class: 'composer-tools'}, [button('btnUpload', '上传附件'), button('btnCard', '发送卡片'), el('span', {id: 'uploadStatus', class: 'muted', text: '图片 / 音频 / 视频 / 文件 · ≤ 10 MB'}), el('input', {id: 'fileInput', type: 'file', hidden: true})]), el('button', {id: 'btnChat', class: 'primary', text: '发送消息', type: 'submit', disabled: true})]),
    ]));
    const details = el('aside', {class: 'details-sidebar', 'aria-label': '详情与通话'}, [
      el('section', {class: 'panel detail-card'}, [heading('参与者详情'), el('div', {id: 'participants', class: 'participants'}, [node('p', '尚未选中咨询', 'muted')]), el('p', {id: 'detailCid', class: 'identifier'})]),
      el('section', {class: 'panel call-panel'}, [heading('通话面板', 'callCount'), node('p', '只有选中的通话启麦、播放远端媒体。', 'muted'), el('div', {id: 'callList', class: 'call-list'}), el('div', {id: 'mediaStage', class: 'media-stage'}, [node('p', '暂无选中通话')]), el('p', {id: 'callState', class: 'call-state', role: 'status', text: '未发起通话'}), el('p', {id: 'mediaWarning', class: 'muted'}), el('div', {class: 'call-controls'}, [button('btnMute', '静音'), button('btnCamera', '关闭摄像头'), button('btnScreen', '共享屏幕'), button('btnPlay', '播放通话'), button('btnHangup', '挂断此通话', 'danger')])]),
      el('section', {class: 'panel event-panel'}, [el('div', {class: 'section-heading'}, [node('h2', '事件与状态'), button('btnClearLog', '清空', 'text-button', false)]), el('div', {id: 'log', class: 'event-log', role: 'log', 'aria-live': 'polite'})]),
    ]);
    $('app').append(connection, el('div', {id: 'notice', class: 'notice', role: 'alert', hidden: true}), el('main', {class: 'workspace'}, [sidebar, chat, details]));
    const dialog = (id, formId, title, text, controls, confirm, danger = false) => el('dialog', {id}, [el('form', {id: formId}, [node('h2', title), node('p', text, 'muted'), ...controls, el('div', {class: 'dialog-actions'}, [el('button', {type: 'button', text: '取消', 'data-close-dialog': ''}), el('button', {type: 'submit', text: confirm, class: danger ? 'danger' : 'primary'})])])]);
    if (isAgent) $('app').append(dialog('handoffDialog', 'handoffForm', '转接咨询', '按目标技能重新分配；咨询和已有消息不会结束。结果以服务器事件为准。', [field('目标技能', el('input', {id: 'handoffSkills', value: 'general', required: true, list: 'skillOptions'})), el('datalist', {id: 'skillOptions'}, [el('option', {value: 'general'})])], '请求转接'));
    $('app').append(dialog('closeDialog', 'closeForm', '结束整个咨询？', '将结束当前咨询及其中所有通话。其他咨询不受影响。', [], '确认结束', true), dialog('cardDialog', 'cardForm', '发送信息卡片', '仅发送 JSON 消息，不创建订单或执行业务操作。操作回执不代表业务执行成功。', [field('卡片类型', select('cardType', [['product', '商品卡'], ['order', '订单卡'], ['action', '操作卡']])), field('卡片 JSON', el('textarea', {id: 'cardBody', rows: 8, value: '{"title":"订单信息","order_id":"","status":"待核实"}', required: true}))], '发送卡片'));
  }
  buildUi();

  function notice(text) { $('notice').textContent = text; $('notice').hidden = false; log(text); }
  function log(text) { logLine($('log'), text); }
  function skills(value = $('skills').value) { return [...new Set(value.split(',').map((s) => s.trim()).filter(Boolean))]; }
  function online() { return client.connected && loginReady && !connecting; }
  function writable(conv = store.selected) { return online() && conv && conv.restored && !conv.closed && (!isAgent || conv.assigned); }
  function target(conv) { return isAgent ? conv.customerIdentity : conv.agentIdentity; }
  function name(conv) {
    const peer = conv.participants.find((p) => p.identity_id === target(conv)) || conv.participants.find((p) => p.identity_id !== client.identityId && p.role !== 'observer');
    return peer?.display_name || target(conv) || (conv.closed ? '已结束咨询' : '等待客服接入');
  }
  async function safely(fn) { try { return await fn(); } catch (error) { notice(error.message || String(error)); } finally { renderControls(); } }
  function bind(id, fn) { if ($(id)) $(id).onclick = () => safely(fn); }
  function bindForm(id, fn) { $(id).onsubmit = (event) => { event.preventDefault(); safely(fn); }; }

  const media = new UctpMedia(client, {
    participant: (cid) => store.participant(store.ensure(cid)),
    onNotice: notice,
    onChange: (call) => {
      if (call) store.ensure(call.cid).calls.add(call.sid);
      renderConversationList(); renderCalls(); renderControls();
    },
  });

  function renderControls() {
    const enabled = Boolean(writable());
    for (const id of ['chatText', 'btnChat', 'btnUpload', 'btnCard', 'btnCloseConversation', 'btnHandoff']) if ($(id)) $(id).disabled = !enabled || (id === 'btnUpload' && uploading);
    for (const btn of document.querySelectorAll('[data-reply]')) btn.disabled = !enabled;
    for (const id of ['btnVoice', 'btnVideo']) $(id).disabled = !enabled || !configLoaded || !target(store.selected);
    if ($('agentState')) $('agentState').disabled = !online();
    if ($('btnEnqueue')) $('btnEnqueue').disabled = !online() || enqueueing;
    $('btnConnect').disabled = connecting;
    const call = media.selected();
    const active = online() && call && call.cid === store.selectedCid && call.state !== 'ended';
    $('btnHangup').disabled = !active;
    $('btnMute').disabled = !active || !call.localStream?.getAudioTracks().length;
    $('btnCamera').disabled = !active || !(call.localStream?.getVideoTracks().length || call.screenStream);
    $('btnScreen').disabled = !active || call.medium !== 'video' || call.pc?.connectionState !== 'connected';
    $('btnPlay').disabled = !active || !call.remoteVideo;
    $('btnMute').textContent = call?.muted ? '取消静音' : '静音';
    $('btnCamera').textContent = call?.cameraOff ? '开启摄像头' : '关闭摄像头';
    $('btnScreen').textContent = call?.screenStream ? '停止共享' : '共享屏幕';
    // Re-check invitations and JSON actions after disconnect without rebuilding messages.
    for (const btn of document.querySelectorAll('#callList button')) btn.disabled = !online();
    for (const btn of document.querySelectorAll('#messages button')) btn.disabled = !enabled;
  }

  function renderConversationList() {
    const conversations = store.search($('search').value);
    $('conversationCount').textContent = String([...store.conversations.values()].filter((c) => !c.closed).length);
    $('conversationList').replaceChildren();
    if (!conversations.length) $('conversationList').append(node('p', store.conversations.size ? '没有匹配的咨询' : '暂无咨询，连接后等待接入', 'list-empty'));
    for (const conv of conversations) {
      const row = el('button', {type: 'button', class: 'conversation-item' + (conv.cid === store.selectedCid ? ' selected' : ''), 'aria-pressed': String(conv.cid === store.selectedCid)});
      const last = [...conv.messages.values()].at(-1);
      const ringing = media.list(conv.cid).some((c) => c.state === '等待接听');
      const top = el('div', {class: 'conversation-item-top'}, [node('strong', name(conv))]);
      if (conv.unread) top.append(node('span', conv.unread > 99 ? '99+' : conv.unread, 'unread-badge'));
      row.append(top, node('small', conv.cid, 'identifier'), node('p', ringing ? '来电邀请 · 请选择接听或拒绝' : last ? (last.content_type === 'text/plain' ? last.body : '[卡片 / 附件]') : conv.status, 'message-preview'), node('span', conv.closed ? '已结束' : !conv.restored ? '待重连恢复' : isAgent && !conv.assigned ? '观察中 / 已转出' : conv.status, 'list-state'));
      row.onclick = () => selectConversation(conv.cid);
      $('conversationList').append(row);
    }
  }

  function renderMessages() {
    const root = $('messages');
    const nearBottom = root.scrollHeight - root.scrollTop - root.clientHeight < 100;
    const oldScroll = root.scrollTop;
    for (const player of root.querySelectorAll('audio, video')) player.pause();
    root.replaceChildren();
    const conv = store.selected;
    if (!conv || !conv.messages.size) {
      root.append(el('div', {class: 'empty-state'}, [node('span', '···', 'empty-symbol'), node('h2', conv ? '咨询已开启，欢迎发送消息' : '每一次咨询，都有清晰的上下文'), node('p', '支持文字、图片、音视频附件与卡片。通话不会自动开启设备。')]));
      return;
    }
    const ordered = [...conv.messages.values()].sort((a, b) => String(a.ts || '').localeCompare(String(b.ts || '')));
    for (const msg of ordered) {
      const participant = conv.participants.find((p) => p.participant_id === msg.from);
      const row = el('article', {class: 'message-row' + (msg.own ? ' own' : ''), 'data-msg-id': msg.msg_id});
      row.append(node('small', `${msg.own ? '我' : participant?.display_name || msg.from || '对方'} · ${msg.ts ? new Date(msg.ts).toLocaleTimeString() : ''}`, 'message-author'));
      row.append(renderBody(msg, async (ref, action, btn) => {
        btn.disabled = true;
        await safely(async () => {
          if (store.selectedCid !== conv.cid || !writable(conv)) throw new Error('请选中该咨询并连接后操作');
          await sendMessage(conv.cid, {content_type: 'application/vnd.uctp.card-action+json', body: JSON.stringify({ref_msg_id: ref, action}), body_encoding: 'utf8'});
        });
      }));
      if (msg.own) row.append(node('small', RECEIPT_TEXT[msg.status] || '提交结果未知', 'message-receipt' + (msg.status === 'failed' ? ' failed' : '')));
      root.append(row);
    }
    if (nearBottom) root.scrollTop = root.scrollHeight; else root.scrollTop = oldScroll;
    renderControls();
  }

  function renderDetails() {
    const conv = store.selected;
    $('participants').replaceChildren();
    $('detailCid').textContent = conv?.cid || '';
    if (!conv) { $('participants').append(node('p', '尚未选中咨询', 'muted')); return; }
    const roles = {agent: '客服', customer: '客户', observer: '观察者', supervisor: '主管'};
    for (const p of conv.participants) $('participants').append(el('div', {class: 'participant'}, [node('span', (p.display_name || p.identity_id || '?').slice(0, 1), 'avatar'), el('div', {}, [node('strong', p.display_name || p.identity_id), node('small', `${roles[p.role] || p.role || '参与者'}${p.identity_id === client.identityId ? ' · 我' : ''}`), node('small', p.identity_id, 'identifier')])]));
    if (!conv.participants.length) $('participants').append(node('p', '等待服务器同步参与者', 'muted'));
  }

  const stateLabels = {new: '媒体初始化', connecting: '媒体连接中', connected: '媒体已连接', disconnected: '媒体暂时断开', failed: '媒体连接失败', closed: '媒体已关闭'};
  function renderCalls() {
    const calls = store.selectedCid ? media.list(store.selectedCid) : [];
    $('callCount').textContent = String(calls.length);
    $('callList').replaceChildren();
    if (!calls.length) $('callList').append(node('p', '暂无通话 · 聊天不受影响', 'muted'));
    for (const call of calls) {
      const row = el('div', {class: 'call-item' + (media.selectedSid === call.sid ? ' selected' : '')});
      const choose = el('button', {type: 'button', class: 'call-select', text: `${call.medium === 'video' ? '视频' : '语音'} · ${stateLabels[call.state] || call.state}`});
      choose.onclick = () => { media.select(call.sid); renderCalls(); renderControls(); };
      row.append(choose, node('small', call.sid, 'identifier'));
      if (!call.initiator && call.state === '等待接听') {
        const accept = el('button', {type: 'button', text: '接听', class: 'primary', disabled: !online()});
        const reject = el('button', {type: 'button', text: '拒绝', class: 'danger-quiet', disabled: !online()});
        accept.onclick = () => safely(() => media.accept(call.sid));
        reject.onclick = () => safely(() => media.reject(call.sid));
        row.append(el('div', {class: 'invitation-actions'}, [accept, reject]));
      }
      $('callList').append(row);
    }
    const call = media.selected();
    if (!call || call.cid !== store.selectedCid || call.state === 'ended') {
      $('mediaStage').replaceChildren(node('p', '请选择通话；未选中通话已隔离'));
      $('callState').textContent = '未选中活动通话';
      $('mediaWarning').textContent = '';
    } else {
      if (call.remoteVideo && $('mediaStage').firstChild !== call.remoteVideo) $('mediaStage').replaceChildren(call.remoteVideo);
      else if (!call.remoteVideo) $('mediaStage').replaceChildren(node('p', call.medium === 'video' ? '等待视频画面' : '语音咨询'));
      const connected = call.pc?.connectionState === 'connected';
      $('callState').textContent = `${stateLabels[call.pc?.connectionState] || call.state}${connected ? (call.playbackBlocked ? ' · 等待点击“播放通话”' : call.serverReady && call.serverStarted ? ' · 双端确认，通话中' : ' · 等待双端 / 服务器确认') : call.negotiated ? ' · SDP 已协商，等待真实连接' : ''}`;
      $('mediaWarning').textContent = call.warning || '';
    }
    renderControls();
  }

  function renderTyping() {
    const conv = store.selected;
    const active = conv ? [...conv.typing.values()].some((time) => Date.now() - time < 5000) : false;
    $('typingStatus').textContent = active ? '对方正在输入…' : '';
  }
  function render() {
    const conv = store.selected;
    $('conversationTitle').textContent = conv ? name(conv) : isAgent ? '选择会话，开始服务' : '您好，有什么可以帮您？';
    $('conversationMeta').textContent = conv ? conv.cid : '连接后开始聊天 · 语音视频独立邀请';
    $('conversationStatus').textContent = conv ? conv.closed ? '已结束' : !conv.restored ? '待恢复' : conv.status : '暂无咨询';
    renderConversationList(); renderMessages(); renderDetails(); renderCalls(); renderTyping(); renderControls();
  }

  function selectConversation(cid) {
    if (store.selected) store.selected.draft = $('chatText').value;
    store.select(cid);
    $('chatText').value = store.selected.draft;
    const previous = media.selected();
    if (previous?.cid !== cid) media.select(null);
    render();
    safely(() => readSelected());
    if (online() && store.selected.restored && !store.selected.closed) safely(() => loadHistory(cid));
  }

  async function loadHistory(cid, force = false) {
    if ((!force && historyLoaded.has(cid)) || historyLoading.has(cid)) return;
    historyLoading.add(cid);
    try {
      await client.request('message.history', {limit: 100}, {cid});
      historyLoaded.add(cid);
      if (cid === store.selectedCid) await readSelected();
    } finally { historyLoading.delete(cid); }
  }

  async function readSelected() {
    const cid = store.selectedCid;
    if (!online() || document.hidden || !store.selected?.restored || store.selected.closed) return;
    const conv = store.selected;
    const ids = store.unreadMessages(cid);
    if (!ids.length) return;
    // Mark pending reads before sending, so rendering/visibility callbacks cannot duplicate them.
    ids.forEach((id) => conv.readIds.add(id));
    try { await client.request('message.read', {msg_ids: ids}, {cid}); }
    catch (error) { ids.forEach((id) => conv.readIds.delete(id)); throw error; }
  }

  async function sendMessage(cid, content) {
    const conv = store.ensure(cid);
    if (!writable(conv)) throw new Error('此咨询尚未恢复、已转出或未连接');
    const payload = {msg_id: await client.newId('msgid'), to: 'all', ...content};
    store.outgoing(cid, payload);
    if (store.selectedCid === cid) renderMessages();
    renderConversationList();
    try {
      await client.request('message.send', payload, {cid});
      store.receipt(cid, payload.msg_id, 'submitted');
    } catch (error) { store.receipt(cid, payload.msg_id, 'failed'); throw error; }
    finally { if (store.selectedCid === cid) renderMessages(); renderConversationList(); }
  }

  bind('btnConnect', async () => {
    connecting = true; loginReady = false; media.suspend();
    $('notice').hidden = true; $('status').textContent = '连接与认证中…'; $('connectionDot').classList.remove('online');
    const oldIdentity = store.identityId;
    for (const conv of store.conversations.values()) conv.restored = false;
    render();
    try {
      const info = await client.connect($('token').value.trim());
      if (oldIdentity && oldIdentity !== info.identity_id) {
        for (const call of media.calls.values()) media.cleanup(call.sid, '切换身份');
        store = new ConversationStore(info.identity_id);
        roster = null;
        $('chatText').value = '';
      }
      store.identityId = info.identity_id;
      historyLoaded.clear(); historyLoading.clear();
      if (isAgent) await client.request('workforce.login', {skills: skills(), display_name: info.identity_id});
      loginReady = true; media.resume();
      $('status').textContent = '已连接 · ' + info.identity_id; $('connectionDot').classList.add('online');
      log('认证成功，恢复咨询列表');
      await client.request('conversation.list', {});
      if (isAgent) await client.request('workforce.agents', {}, {}, {expected: ['workforce.roster']});
      try {
        const response = await fetch('/uctp/config', {cache: 'no-store', headers: {Authorization: 'Bearer ' + client.sessionToken}});
        if (!response.ok) throw new Error('配置加载失败: ' + response.status);
        const config = await response.json();
        if (!Array.isArray(config.ice_servers) || !Number.isFinite(config.max_upload_bytes) || config.max_upload_bytes <= 0) throw new Error('媒体配置格式无效');
        media.config = config; maxUploadBytes = Math.min(config.max_upload_bytes, UctpChat.MAX_UPLOAD_BYTES); configLoaded = true;
        $('uploadStatus').textContent = `图片 / 音频 / 视频 / 文件 · ≤ ${(maxUploadBytes / 1048576).toFixed(1)} MB`;
      } catch (error) { configLoaded = false; notice(error.message + '；聊天可用，语音视频暂不可用'); }
    } catch (error) { client.close(); loginReady = false; $('status').textContent = '连接失败，可重试'; notice(error.message); }
    finally {
      connecting = false;
      render();
      if (online()) for (const conv of store.conversations.values()) if (conv.restored && !conv.closed) safely(async () => { await loadHistory(conv.cid); await readSelected(); });
    }
  });

  bind('btnEnqueue', async () => {
    enqueueing = true; renderControls();
    $('queueInfo').textContent = '正在创建聊天咨询…';
    try { await client.request('workforce.enqueue', {skills: skills(), priority: 0, metadata: {source: 'web-customer'}}, {}, {expected: ['workforce.queued']}); }
    catch (error) { $('queueInfo').textContent = '咨询创建失败或结果未知，请重连查看列表'; throw error; }
    finally { enqueueing = false; }
  });
  if (isAgent) $('agentState').onchange = () => safely(async () => {
    const value = $('agentState').value;
    try {
      await client.request('workforce.status', {state: value});
      log('接待状态提交：' + value + '（现有咨询继续）');
    } catch (error) {
      if (roster) $('agentState').value = roster.state;
      throw error;
    }
  });
  bindForm('composer', async () => {
    const cid = store.selectedCid;
    const text = $('chatText').value.trim();
    if (!text || !cid) return;
    $('chatText').value = ''; store.selected.draft = '';
    try { await sendMessage(cid, {content_type: 'text/plain', body: text, body_encoding: 'utf8'}); }
    catch (error) {
      const conv = store.ensure(cid);
      if (!conv.draft) conv.draft = text;
      if (store.selectedCid === cid && !$('chatText').value) $('chatText').value = text;
      throw error;
    }
  });
  $('chatText').onkeydown = (event) => {
    if (event.key === 'Enter' && !event.shiftKey && !event.isComposing) { event.preventDefault(); if (!$('btnChat').disabled) $('composer').requestSubmit(); }
  };
  $('chatText').oninput = () => {
    if (store.selected) store.selected.draft = $('chatText').value;
    if (writable() && Date.now() - typingSentAt > 1800) {
      typingSentAt = Date.now();
      client.send('message.typing', {state: 'typing'}, {cid: store.selectedCid}).catch((error) => notice('输入状态发送失败：' + error.message));
    }
  };
  for (const btn of document.querySelectorAll('[data-reply]')) btn.onclick = () => { if (!writable()) return; $('chatText').value = btn.dataset.reply; store.selected.draft = btn.dataset.reply; $('chatText').focus(); };
  $('search').oninput = renderConversationList;
  bind('btnVoice', () => media.invite(store.selectedCid, target(store.selected), 'voice'));
  bind('btnVideo', () => media.invite(store.selectedCid, target(store.selected), 'video'));
  bind('btnMute', () => media.mute()); bind('btnCamera', () => media.camera()); bind('btnScreen', () => media.shareScreen());
  bind('btnPlay', () => media.playRemote());
  bind('btnHangup', () => media.hangup(media.selectedSid));
  bind('btnClearLog', () => $('log').replaceChildren());
  bind('btnUpload', () => { $('fileInput').dataset.cid = store.selectedCid; $('fileInput').click(); });
  $('fileInput').onchange = () => safely(async () => {
    const file = $('fileInput').files[0];
    const cid = $('fileInput').dataset.cid;
    if (!file) return;
    uploading = true; $('uploadStatus').textContent = '正在上传 ' + file.name; renderControls();
    try {
      if (!writable(store.conversations.get(cid))) throw new Error('咨询未连接或已结束，无法上传');
      const content = await UctpChat.upload(file, maxUploadBytes, (url, options) => fetch(url, {
        ...options, headers: {...options.headers, Authorization: 'Bearer ' + client.sessionToken},
      }));
      await sendMessage(cid, content);
      $('uploadStatus').textContent = '附件已提交：' + file.name;
    } catch (error) { $('uploadStatus').textContent = '附件未提交：' + error.message; throw error; }
    finally { uploading = false; $('fileInput').value = ''; }
  });

  function openDialog(id) { $(id).dataset.cid = store.selectedCid; $(id).showModal(); }
  function dialogConversation(id) {
    const cid = $(id).dataset.cid;
    const conv = store.conversations.get(cid);
    if (!writable(conv) || cid !== store.selectedCid) throw new Error('咨询已切换或不可操作，请重新打开');
    return conv;
  }
  bind('btnHandoff', () => openDialog('handoffDialog'));
  bind('btnCloseConversation', () => openDialog('closeDialog'));
  bind('btnCard', () => openDialog('cardDialog'));
  for (const btn of document.querySelectorAll('[data-close-dialog]')) btn.onclick = () => btn.closest('dialog').close();
  if (isAgent) bindForm('handoffForm', async () => {
    const conv = dialogConversation('handoffDialog');
    const selectedSkills = skills($('handoffSkills').value);
    if (!selectedSkills.length) throw new Error('请选择至少一个技能');
    await client.request('workforce.handoff', {cid: conv.cid, ...(media.selected()?.cid === conv.cid ? {sid: media.selectedSid} : {}), customer_identity_id: conv.customerIdentity, skills: selectedSkills, reason: 'web-handoff'}, {}, {expected: ['workforce.queued']});
    $('handoffDialog').close(); conv.status = '转接排队中'; render();
  });
  bindForm('closeForm', async () => {
    const conv = dialogConversation('closeDialog');
    await client.request('conversation.close', {reason_code: 200, reason: 'consultation-complete'}, {cid: conv.cid});
    $('closeDialog').close(); log('结束咨询请求已确认：' + conv.cid);
  });
  bindForm('cardForm', async () => {
    const conv = dialogConversation('cardDialog');
    const body = UctpChat.parseBody($('cardBody').value);
    if (!body) throw new Error('卡片必须是有效 JSON 对象');
    const type = $('cardType').value;
    if (!['product', 'order', 'action'].includes(type)) throw new Error('不支持的卡片类型');
    await sendMessage(conv.cid, {content_type: `application/vnd.uctp.card.${type}+json`, body: JSON.stringify(body), body_encoding: 'utf8'});
    $('cardDialog').close();
  });

  client.on('client.error', (env) => notice(env.payload.reason));
  client.on('error', (env) => {
    // Never log credentials, session tokens or raw auth envelopes.
    if (env.payload?.reason === 'resume-token-invalid') log('恢复令牌失效，重新挑战认证');
    else notice(`${env.payload?.code || '错误'}：${env.payload?.reason || '请求失败'}`);
  });
  client.on('client.closed', () => {
    loginReady = false; configLoaded = false; media.suspend();
    $('status').textContent = '连接断开，请重连'; $('connectionDot').classList.remove('online');
    for (const conv of store.conversations.values()) conv.restored = false;
    notice('信令已断开，设备已隔离；请重连恢复咨询与历史'); render();
  });
  client.on('conversation.opened', (env) => {
    const conv = store.opened(env);
    conv.restored = true; conv.closed = false;
    conv.customerIdentity = conv.participants.find((p) => p.role === 'customer')?.identity_id || conv.customerIdentity;
    if (!isAgent) {
      conv.agentIdentity = conv.participants.find((p) => p.role === 'agent')?.identity_id || conv.agentIdentity;
      if (conv.agentIdentity && conv.status === '等待客服') conv.status = '咨询中';
    }
    if (isAgent && roster) conv.assigned = (roster.assigned_cids || []).includes(conv.cid);
    if (!store.selectedCid) selectConversation(conv.cid); else render();
    if (online()) safely(() => loadHistory(conv.cid));
  });
  client.on('conversation.closed', (env) => {
    const conv = store.ensure(env.cid); conv.closed = true; conv.status = '已结束'; conv.assigned = false;
    media.closeConversation(env.cid); log('咨询已结束：' + env.cid); render();
  });
  client.on('workforce.queued', (env) => {
    const conv = store.ensure(env.payload.cid || env.cid);
    conv.restored = true;
    if (env.payload.transfer) conv.status = '转接排队中';
    else {
      if (!conv.agentIdentity && !conv.assigned) conv.status = '等待客服';
      if (env.payload.customer_participant_id && !conv.participants.some((p) => p.identity_id === client.identityId)) conv.participants.push({identity_id: client.identityId, participant_id: env.payload.customer_participant_id, role: 'customer', display_name: client.identityId});
      if ($('queueInfo')) $('queueInfo').textContent = `聊天咨询已创建 · 排队位置 ${Number.isFinite(env.payload.position) ? env.payload.position + 1 : '待同步'}`;
      selectConversation(conv.cid);
    }
    log((env.payload.transfer ? '转接' : '咨询') + '已入队：' + conv.cid); render();
  });
  client.on('workforce.assigned', (env) => {
    const conv = store.ensure(env.cid); conv.restored = true; conv.status = '咨询中'; conv.assigned = true;
    if (isAgent) conv.customerIdentity = env.payload.customer_identity_id;
    else { conv.agentIdentity = env.payload.agent_identity_id; if ($('queueInfo')) $('queueInfo').textContent = '客服已接入：' + (env.payload.agent_display_name || conv.agentIdentity); }
    if (!store.selectedCid) selectConversation(conv.cid);
    log('客服已分配（仅聊天，未自动呼叫）：' + conv.cid); render();
  });
  client.on('workforce.roster', (env) => {
    const agents = env.payload.agents || [];
    roster = agents.find((a) => a.identity_id === client.identityId) || null;
    if (isAgent && roster) {
      $('agentState').value = roster.state;
      for (const conv of store.conversations.values()) conv.assigned = (roster.assigned_cids || []).includes(conv.cid);
      for (const cid of roster.assigned_cids || []) store.ensure(cid).assigned = true;
      $('rosterSummary').textContent = `正在接待 ${roster.active_conversations ?? 0} 个咨询 · 排队 ${(env.payload.waiting || []).length} 人`;
      $('skillOptions').replaceChildren(...[...new Set(agents.flatMap((a) => a.skills || []))].map((s) => el('option', {value: s})));
    }
    render();
  });
  client.on('message.send', async (env) => {
    const history = Boolean(env.in_reply_to);
    const result = store.receive(env, history);
    if (!history && !result.own && env.payload?.msg_id) {
      await client.send('message.received', {msg_id: env.payload.msg_id}, {cid: env.cid});
    }
    if (result.added) { renderConversationList(); if (env.cid === store.selectedCid) renderMessages(); }
    if (!history && env.cid === store.selectedCid) await readSelected();
  });
  client.on('message.history', async (env) => {
    for (const item of env.payload.messages || []) {
      const message = item.type === 'message.send' ? item : {cid: env.cid, ts: item.ts, payload: item.payload || item};
      store.receive(message, true);
    }
    render(); await readSelected();
  });
  for (const [type, status] of [['message.delivered', 'received'], ['message.read', 'read']]) client.on(type, (env) => {
    const ids = env.payload.msg_ids || (env.payload.msg_id ? [env.payload.msg_id] : []);
    for (const id of ids) store.receipt(env.cid, id, status);
    if (env.cid === store.selectedCid) renderMessages();
  });
  client.on('message.typing', (env) => {
    const conv = store.ensure(env.cid);
    const from = env.payload.from || env.payload.by_participant || 'peer';
    if (from === store.participant(conv)) return;
    if (env.payload.state === 'typing') conv.typing.set(from, Date.now()); else conv.typing.delete(from);
    renderTyping(); clearTimeout(typingTimer); typingTimer = setTimeout(renderTyping, 5100);
  });
  for (const type of ['session.invite', 'session.accept', 'session.reject', 'session.cancel', 'session.started', 'session.ended', 'connection.negotiated', 'connection.ready']) client.on(type, (env) => log(`${type} · ${env.cid || ''} · ${env.sid || ''}`));
  client.on('session.update', (env) => {
    if (env.payload.kind === 'role-change') {
      const conv = store.ensure(env.cid);
      const p = conv.participants.find((p) => p.participant_id === env.payload.details?.participant_id);
      if (p) p.role = env.payload.details.new_role;
      render();
    }
  });
  document.addEventListener('visibilitychange', () => { if (!document.hidden) safely(readSelected); });
  window.addEventListener('pagehide', () => {
    for (const call of media.calls.values()) media.cleanup(call.sid, '页面关闭');
    clearTimeout(typingTimer); client.close();
  });
  render();
})();
