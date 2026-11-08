'use strict';

class UctpMedia {
  constructor(client, options = {}) {
    this.client = client;
    this.calls = new Map(); // sid -> independent PC, streams and pending ICE
    this.selectedSid = null;
    this.config = {ice_servers: []};
    this.onChange = options.onChange || (() => {});
    this.onNotice = options.onNotice || (() => {});
    this.PeerConnection = options.PeerConnection || globalThis.RTCPeerConnection;
    this.mediaDevices = options.mediaDevices || globalThis.navigator?.mediaDevices;
    this.createStream = options.createStream || (() => new MediaStream());
    this.createVideo = options.createVideo || (() => document.createElement('video'));
    this.participant = options.participant || (() => null);
    this.online = true;
    const run = (type, fn) => client.on(type, async (env) => {
      const call = this.calls.get(env.sid);
      if (call && call.cid !== env.cid) return;
      try { await fn.call(this, env, call); } catch (error) {
        this.onNotice('通话失败：' + error.message);
        if (call) await this.fail(call, error);
      }
    });
    run('session.invite', this.receiveInvite);
    run('session.accept', async function (_, call) {
      if (!call || !call.initiator || call.state === 'ended' || call.accepted) return;
      call.accepted = true;
      call.state = '等待选中后协商';
      call.pendingOffer = true;
      if (this.selectedSid === call.sid) await this.makeOffer(call);
      this.changed(call);
    });
    run('connection.offer', this.receiveOffer);
    run('connection.answer', async function (env, call) {
      if (!call?.pc || call.connid !== env.connid || call.state === 'ended') return;
      await call.pc.setRemoteDescription({type: 'answer', sdp: env.payload.substrate_setup.sdp});
      await this.flushRemoteCandidates(call);
      this.changed(call);
    });
    run('connection.ice-candidate', async function (env, call) {
      if (!call || call.state === 'ended' || (call.connid && call.connid !== env.connid)) return;
      const candidate = env.payload.candidate;
      if (!candidate) return;
      call.remoteCandidates.push({connid: env.connid, candidate});
      if (call.pc?.remoteDescription) await this.flushRemoteCandidates(call);
    });
    run('connection.negotiated', function (_, call) { if (call && call.state !== 'ended') { call.negotiated = true; this.changed(call); } });
    run('connection.ready', function (_, call) { if (call && call.state !== 'ended') { call.serverReady = true; this.changed(call); } });
    run('session.started', function (_, call) { if (call && call.state !== 'ended') { call.serverStarted = true; this.changed(call); } });
    for (const type of ['session.reject', 'session.cancel', 'session.ended', 'connection.end']) {
      run(type, function (env, call) { if (call) this.cleanup(call.sid, env.payload?.reason || type); });
    }
  }

  changed(call) { this.onChange(call); }
  scope(call, connection = false) { return {cid: call.cid, sid: call.sid, ...(connection ? {connid: call.connid} : {})}; }
  valid(call) { return this.calls.get(call.sid) === call && call.state !== 'ended'; }
  list(cid) { return [...this.calls.values()].filter((call) => call.cid === cid && call.state !== 'ended'); }
  selected() { return this.calls.get(this.selectedSid); }

  createCall(cid, sid, medium, initiator) {
    const call = {cid, sid, medium, initiator, state: initiator ? '邀请提交中' : '等待接听', accepted: false, pc: null, localStream: null, remoteStream: this.createStream(), remoteCandidates: [], localCandidates: [], streamIds: new Map(), muted: false, cameraOff: false, signalingReady: false, confirmed: false, serverReady: false, serverStarted: false};
    this.calls.set(sid, call);
    this.changed(call);
    return call;
  }

  async invite(cid, target, medium) {
    if (!this.client.connected) throw new Error('请先连接');
    if (!target) throw new Error('尚未分配通话对象');
    if (!['voice', 'video'].includes(medium)) throw new Error('不支持的通话类型');
    const call = this.createCall(cid, await this.client.newId('sid'), medium, true);
    this.select(call.sid);
    try {
      await this.client.request('session.invite', {
        from: this.participant(cid), to: [target], medium, intent: 'synchronous-engagement',
        capabilities_offer: {audio_codecs: [{name: 'opus', params: {sample_rate: 48000, channels: 1}}], ...(medium === 'video' ? {video_codecs: [{name: 'VP8'}]} : {})},
      }, this.scope(call));
      if (this.valid(call) && !call.accepted) { call.state = '等待对方接听'; this.changed(call); }
      return call;
    } catch (error) { await this.fail(call, error); throw error; }
  }

  receiveInvite(env) {
    if (this.calls.has(env.sid)) return;
    if (!['voice', 'video'].includes(env.payload.medium)) { this.onNotice('收到不支持的通话类型'); return; }
    const call = this.createCall(env.cid, env.sid, env.payload.medium, false);
    call.from = env.payload.from;
    // No media permission or automatic acceptance here.
    this.changed(call);
  }

  async accept(sid) {
    const call = this.calls.get(sid);
    if (!call || call.initiator || call.state !== '等待接听') return;
    this.select(sid);
    call.state = '正在接听'; this.changed(call);
    try {
      await this.client.request('session.accept', {capabilities_answer: {audio_codecs: [{name: 'opus'}], ...(call.medium === 'video' ? {video_codecs: [{name: 'VP8'}]} : {})}}, this.scope(call));
      if (!this.valid(call)) return;
      call.accepted = true; call.state = '等待协商'; this.changed(call);
    } catch (error) { await this.fail(call, error); throw error; }
  }

  async reject(sid) {
    const call = this.calls.get(sid);
    if (!call) return;
    await this.client.request('session.reject', {reason_code: 486, reason: 'declined'}, this.scope(call));
    this.cleanup(sid, '已拒绝');
  }

  async hangup(sid) {
    const call = this.calls.get(sid);
    if (!call || call.state === 'ended') return;
    const type = !call.accepted ? (call.initiator ? 'session.cancel' : 'session.reject') : 'session.end';
    // Stop local capture immediately; the consultation and other calls remain intact.
    const scope = this.scope(call);
    this.cleanup(sid, '本地已挂断');
    await this.client.request(type, {reason_code: 200, reason: 'normal-clearing'}, scope);
  }

  async fail(call, error) {
    const scope = this.scope(call);
    const type = !call.accepted ? (call.initiator ? 'session.cancel' : 'session.reject') : 'session.end';
    this.cleanup(call.sid, '失败：' + error.message);
    if (this.client.connected) {
      try { await this.client.request(type, {reason_code: 500, reason: 'media-failed'}, scope); }
      catch (cancelError) { this.onNotice('通话资源已清理；服务器清理失败：' + cancelError.message); }
    }
  }

  select(sid) {
    this.selectedSid = sid || null;
    for (const call of this.calls.values()) this.applyIsolation(call);
    const call = this.selected();
    if (call?.pendingOffer && !call.offering) this.makeOffer(call).catch((error) => {
      this.onNotice('协商失败：' + error.message); this.fail(call, error).catch((e) => this.onNotice(e.message));
    });
    this.changed(call);
  }

  applyIsolation(call) {
    const active = call.sid === this.selectedSid && this.online && call.state !== 'ended';
    for (const track of call.localStream?.getTracks() || []) track.enabled = active && (track.kind === 'audio' ? !call.muted : !call.cameraOff);
    for (const track of call.screenStream?.getTracks() || []) track.enabled = active && !call.cameraOff;
    if (call.remoteVideo) {
      call.remoteVideo.muted = !active;
      if (!active) call.remoteVideo.pause();
      else if (call.remoteVideo.srcObject) this.playRemote(call);
    }
  }

  async playRemote(call = this.selected()) {
    if (!call?.remoteVideo || !this.valid(call) || call.sid !== this.selectedSid || !this.online) return;
    try {
      call.remoteVideo.muted = false;
      await call.remoteVideo.play();
      if (call.playbackBlocked) { call.playbackBlocked = false; this.changed(call); }
    } catch (error) {
      if (error.name === 'AbortError' || !this.valid(call) || call.sid !== this.selectedSid || !this.online) return;
      call.playbackBlocked = true;
      this.changed(call);
      this.onNotice(error.name === 'NotAllowedError' ? '浏览器阻止自动播放，请点击“播放通话”' : '远端媒体播放失败：' + error.message);
    }
  }

  async setupPeer(call) {
    if (call.peerPromise) return call.peerPromise;
    call.peerPromise = this.initializePeer(call);
    return call.peerPromise;
  }

  async initializePeer(call) {
    if (call.pc) return call.pc;
    if (!this.PeerConnection) throw new Error('浏览器不支持 WebRTC');
    const pc = new this.PeerConnection({iceServers: this.config.ice_servers || []});
    call.pc = pc;
    call.remoteVideo = this.createVideo();
    call.remoteVideo.playsInline = true; call.remoteVideo.controls = false; call.remoteVideo.muted = true;
    call.remoteVideo.setAttribute('aria-label', '远端通话画面');
    call.remoteVideo.srcObject = call.remoteStream;
    pc.ontrack = (event) => {
      if (!this.valid(call)) return;
      if (!call.remoteStream.getTracks().some((t) => t.id === event.track.id)) call.remoteStream.addTrack(event.track);
      this.applyIsolation(call); this.changed(call);
    };
    pc.onicecandidate = (event) => {
      if (!event.candidate || !this.valid(call)) return;
      call.localCandidates.push(event.candidate.toJSON ? event.candidate.toJSON() : event.candidate);
      if (call.signalingReady) this.flushLocalCandidates(call).catch((error) => this.onNotice('ICE 中继失败：' + error.message));
    };
    pc.onconnectionstatechange = () => {
      if (!this.valid(call)) return;
      call.state = pc.connectionState;
      if (pc.connectionState === 'connected') this.confirmConnected(call);
      if (pc.connectionState === 'failed') { this.fail(call, new Error('媒体连接失败')).catch((e) => this.onNotice(e.message)); }
      this.changed(call);
    };
    if (this.selectedSid === call.sid) {
      let stream;
      try {
        if (!this.mediaDevices?.getUserMedia) throw new Error('请使用 HTTPS 或 localhost 访问并授权设备');
        stream = await this.mediaDevices.getUserMedia({audio: true, video: call.medium === 'video'});
      } catch (error) {
        if (call.medium === 'voice') {
          const reasons = {
            NotAllowedError: '请在浏览器地址栏允许麦克风，并检查系统的麦克风权限后重新呼叫',
            NotFoundError: '请连接麦克风后重新呼叫',
            NotReadableError: '请检查麦克风是否被其他应用占用后重新呼叫',
          };
          throw new Error('无法开启麦克风：' + (reasons[error.name] || error.message));
        }
        call.warning = '摄像头不可用，尝试语音；' + error.name;
        try {
          stream = await this.mediaDevices?.getUserMedia({audio: true, video: false});
          if (stream) call.warning = '摄像头不可用，已降为语音发送（仍可接收视频）：' + error.name;
        }
        catch (audioError) { call.warning = '摄像头 / 麦克风不可用，已降为只听：' + audioError.name; }
        this.onNotice(call.warning);
      }
      if (!this.valid(call)) { stream?.getTracks().forEach((t) => t.stop()); pc.close(); throw new Error('通话已取消'); }
      call.localStream = stream || null;
      for (const track of stream?.getTracks() || []) pc.addTrack(track, stream);
    } else { call.warning = '未选中此通话，只接收媒体；切换不会自动请求麦克风'; }
    const tracks = call.localStream?.getTracks() || [];
    if (!tracks.some((t) => t.kind === 'audio')) pc.addTransceiver('audio', {direction: 'recvonly'});
    if (call.medium === 'video' && !tracks.some((t) => t.kind === 'video')) pc.addTransceiver('video', {direction: 'recvonly'});
    this.applyIsolation(call);
    return pc;
  }

  confirmConnected(call) {
    if (!this.valid(call) || !call.signalingReady || call.pc?.connectionState !== 'connected' || call.confirmed) return;
    call.confirmed = true;
    // Gate on answer/offer ACK as well: a fast peer must not confirm before the server knows its connection.
    this.client.send('connection.confirm', {state: 'connected'}, this.scope(call, true)).catch((error) => {
      call.confirmed = false; this.onNotice('连接确认失败：' + error.message);
    });
  }

  async streamsOffered(call) {
    const offered = [];
    for (const transceiver of call.pc.getTransceivers()) {
      const kind = transceiver.sender.track?.kind || transceiver.receiver.track?.kind;
      if (!['audio', 'video'].includes(kind)) continue;
      if (!call.streamIds.has(transceiver)) call.streamIds.set(transceiver, await this.client.newId('strmid'));
      offered.push({id: call.streamIds.get(transceiver), kind, direction: transceiver.direction, codec_preferences: kind === 'audio' ? ['opus'] : ['VP8']});
    }
    return offered;
  }

  async makeOffer(call) {
    if (!this.valid(call) || call.offering || !call.pendingOffer || this.selectedSid !== call.sid) return;
    call.offering = true; call.pendingOffer = false;
    try {
      call.connid = await this.client.newId('connid');
      const pc = await this.setupPeer(call);
      if (!this.valid(call)) return;
      call.state = '协商中';
      await pc.setLocalDescription(await pc.createOffer());
      const streams = await this.streamsOffered(call);
      if (!this.valid(call)) return;
      await this.client.request('connection.offer', {by_participant: this.participant(call.cid), substrate: 'websocket+webrtc', streams_offered: streams, substrate_setup: {kind: 'websocket+webrtc', sdp: pc.localDescription.sdp}}, this.scope(call, true));
      call.signalingReady = true;
      this.confirmConnected(call);
      await this.flushLocalCandidates(call);
      this.changed(call);
    } finally { call.offering = false; }
  }

  async receiveOffer(env, call) {
    if (!call || call.initiator || call.state === '等待接听' || call.state === 'ended') return;
    if (call.offerReceived || (call.connid && call.connid !== env.connid)) return;
    call.offerReceived = true;
    call.accepted = true;
    call.connid = env.connid;
    const pc = await this.setupPeer(call);
    if (!this.valid(call)) return;
    await pc.setRemoteDescription({type: 'offer', sdp: env.payload.substrate_setup.sdp});
    await this.flushRemoteCandidates(call);
    await pc.setLocalDescription(await pc.createAnswer());
    const streams = await this.streamsOffered(call);
    if (!this.valid(call)) return;
    await this.client.request('connection.answer', {by_participant: this.participant(call.cid), substrate: 'websocket+webrtc', streams_offered: streams, substrate_setup: {kind: 'websocket+webrtc', sdp: pc.localDescription.sdp}}, this.scope(call, true));
    call.signalingReady = true;
    this.confirmConnected(call);
    await this.flushLocalCandidates(call);
    this.changed(call);
  }

  async flushRemoteCandidates(call) {
    if (!call.pc?.remoteDescription) return;
    while (call.remoteCandidates.length && this.valid(call)) {
      const item = call.remoteCandidates.shift();
      if (item.connid === call.connid) await call.pc.addIceCandidate(item.candidate);
    }
  }

  async flushLocalCandidates(call) {
    if (call.flushingIce) return;
    call.flushingIce = true;
    try {
      while (call.localCandidates.length && this.valid(call)) {
        await this.client.send('connection.ice-candidate', {candidate: call.localCandidates.shift()}, this.scope(call, true));
      }
    } finally { call.flushingIce = false; }
  }

  mute() { const call = this.selected(); if (!call) return; call.muted = !call.muted; this.applyIsolation(call); this.changed(call); }
  camera() { const call = this.selected(); if (!call) return; call.cameraOff = !call.cameraOff; this.applyIsolation(call); this.changed(call); }

  async shareScreen() {
    const call = this.selected();
    if (!call?.pc || call.pc.connectionState !== 'connected' || call.medium !== 'video') throw new Error('请选择已连接的视频通话');
    if (call.screenStream) return this.stopScreen(call);
    if (!this.mediaDevices?.getDisplayMedia) throw new Error('浏览器不支持屏幕共享');
    const sender = call.pc.getSenders().find((s) => s.track?.kind === 'video');
    if (!sender) throw new Error('没有已协商的视频发送轨道，请重新发起视频通话');
    const stream = await this.mediaDevices.getDisplayMedia({video: true, audio: false});
    if (!this.valid(call) || this.selectedSid !== call.sid) { stream.getTracks().forEach((t) => t.stop()); return; }
    const track = stream.getVideoTracks()[0];
    try { await sender.replaceTrack(track); }
    catch (error) { stream.getTracks().forEach((t) => t.stop()); throw error; }
    call.screenStream = stream; call.screenSender = sender;
    track.onended = () => this.stopScreen(call).catch((error) => this.onNotice('恢复摄像头失败：' + error.message));
    this.applyIsolation(call); this.changed(call);
  }

  async stopScreen(call) {
    if (!call.screenStream) return;
    const stream = call.screenStream;
    call.screenStream = null;
    for (const track of stream.getTracks()) { track.onended = null; track.stop(); }
    if (this.valid(call)) await call.screenSender.replaceTrack(call.localStream?.getVideoTracks()[0] || null);
    this.applyIsolation(call); this.changed(call);
  }

  cleanup(sid, reason) {
    const call = this.calls.get(sid);
    if (!call || call.state === 'ended') return;
    call.state = 'ended'; call.endReason = reason;
    for (const stream of [call.localStream, call.screenStream, call.remoteStream]) for (const track of stream?.getTracks() || []) { track.onended = null; track.stop(); }
    if (call.pc) {
      call.pc.ontrack = null; call.pc.onicecandidate = null; call.pc.onconnectionstatechange = null;
      call.pc.close();
    }
    if (call.remoteVideo) { call.remoteVideo.pause(); call.remoteVideo.srcObject = null; call.remoteVideo.remove(); }
    call.pc = null; call.peerPromise = null; call.localStream = null; call.screenStream = null;
    call.remoteCandidates.length = 0; call.localCandidates.length = 0;
    if (this.selectedSid === sid) this.selectedSid = null;
    this.changed(call);
  }

  closeConversation(cid) { for (const call of this.list(cid)) this.cleanup(call.sid, '咨询已结束'); }
  suspend() { this.online = false; for (const call of this.calls.values()) this.applyIsolation(call); }
  resume() { this.online = true; for (const call of this.calls.values()) this.applyIsolation(call); }
}
if (typeof module !== 'undefined' && module.exports) module.exports = {UctpMedia};
