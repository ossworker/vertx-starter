'use strict';
const {test} = require('node:test');
const assert = require('node:assert/strict');
const {UctpMedia} = require('../../main/resources/static/uctp-media.js');

// Signal and media doubles verify state/cleanup only, not real WebRTC or browser permissions.
class Signal {
  constructor() { this.handlers = new Map(); this.connected = true; this.sent = []; this.sequence = 0; }
  on(type, fn) { if (!this.handlers.has(type)) this.handlers.set(type, []); this.handlers.get(type).push(fn); }
  async emit(type, cid, sid, payload = {}, connid) { await Promise.all((this.handlers.get(type) || []).map((fn) => fn({type, cid, sid, connid, payload}))); }
  async newId(prefix) { return prefix + (++this.sequence); }
  async request(type, payload, scope) { this.sent.push({type, payload, ...scope}); return {type: 'ack'}; }
  async send(type, payload, scope) { this.sent.push({type, payload, ...scope}); }
}
class Track {
  constructor(kind, id = kind) { this.kind = kind; this.id = id; this.enabled = true; this.stopped = false; }
  stop() { this.stopped = true; }
}
class Stream {
  constructor(tracks = []) { this.tracks = tracks; }
  getTracks() { return this.tracks; }
  getAudioTracks() { return this.tracks.filter((t) => t.kind === 'audio'); }
  getVideoTracks() { return this.tracks.filter((t) => t.kind === 'video'); }
  addTrack(track) { this.tracks.push(track); }
}
class Video {
  constructor() { this.paused = true; this.muted = true; }
  setAttribute() {}
  async play() { this.paused = false; }
  pause() { this.paused = true; }
  remove() { this.removed = true; }
}
class Peer {
  constructor(config) { this.config = config; this.transceivers = []; this.connectionState = 'new'; this.remoteDescription = null; this.candidates = []; }
  addTrack(track) {
    const sender = {track, async replaceTrack(next) { this.track = next; }};
    this.transceivers.push({sender, receiver: {track: new Track(track.kind)}, direction: 'sendrecv'});
    return sender;
  }
  addTransceiver(kind, config) { this.transceivers.push({sender: {track: null}, receiver: {track: new Track(kind)}, direction: config.direction}); }
  getTransceivers() { return this.transceivers; }
  getSenders() { return this.transceivers.map((t) => t.sender); }
  async createOffer() { return {type: 'offer', sdp: 'unit-offer'}; }
  async createAnswer() { return {type: 'answer', sdp: 'unit-answer'}; }
  async setLocalDescription(description) { this.localDescription = description; }
  async setRemoteDescription(description) { this.remoteDescription = description; }
  async addIceCandidate(candidate) { assert.ok(this.remoteDescription); this.candidates.push(candidate); }
  close() { this.connectionState = 'closed'; this.closed = true; }
}
function fixture(mediaDevices) {
  const client = new Signal();
  const notices = [];
  let captures = 0;
  const media = new UctpMedia(client, {PeerConnection: Peer, createStream: () => new Stream(), createVideo: () => new Video(), participant: (cid) => 'pid' + cid, onNotice: (text) => notices.push(text), mediaDevices: mediaDevices || {getUserMedia: async ({video}) => { captures++; return new Stream([new Track('audio'), ...(video ? [new Track('video')] : [])]); }}});
  return {client, media, notices, captures: () => captures};
}

test('incoming invitations never accept or acquire media automatically', async () => {
  const {client, media, captures} = fixture();
  await client.emit('session.invite', 'cid1', 'sid1', {medium: 'video', from: 'pidcustomer'});
  assert.equal(captures(), 0);
  assert.equal(client.sent.length, 0);
  assert.equal(media.calls.get('sid1').state, '等待接听');
  await client.emit('connection.offer', 'cid1', 'sid1', {substrate_setup: {sdp: 'unexpected'}}, 'connid1');
  assert.equal(captures(), 0);
  assert.equal(media.calls.get('sid1').pc, null);
  await media.reject('sid1');
  assert.equal(client.sent[0].type, 'session.reject');
  assert.equal(media.calls.get('sid1').state, 'ended');
});

test('outgoing call waits for explicit accept, audio/video have separate stream IDs', async () => {
  const {client, media, captures} = fixture();
  const call = await media.invite('cid1', 'customer-1', 'video');
  assert.equal(captures(), 0);
  assert.equal(client.sent[0].type, 'session.invite');
  assert.equal(client.sent[0].payload.medium, 'video');
  await client.emit('session.accept', 'cid1', call.sid);
  assert.equal(captures(), 1);
  const offer = client.sent.find((env) => env.type === 'connection.offer');
  assert.equal(offer.payload.substrate, 'websocket+webrtc');
  assert.equal(offer.payload.substrate_setup.sdp, 'unit-offer');
  assert.deepEqual(offer.payload.streams_offered.map((s) => s.kind), ['audio', 'video']);
  assert.notEqual(offer.payload.streams_offered[0].id, offer.payload.streams_offered[1].id);
  assert.ok(offer.payload.streams_offered.every((s) => /^strmid\d+$/.test(s.id)));
  assert.deepEqual(call.pc.config.iceServers, []);
  assert.equal(client.sent.some((env) => env.type === 'connection.confirm'), false);
  await client.emit('connection.answer', 'cid1', call.sid, {substrate_setup: {sdp: 'unit-answer'}}, call.connid);
  await client.emit('connection.negotiated', 'cid1', call.sid, {}, call.connid);
  assert.equal(call.negotiated, true);
  assert.equal(client.sent.some((env) => env.type === 'connection.confirm'), false);
  call.pc.connectionState = 'connected'; call.pc.onconnectionstatechange();
  assert.equal(client.sent.filter((env) => env.type === 'connection.confirm').length, 1);
  assert.deepEqual(client.sent.at(-1).payload, {state: 'connected'});
  media.cleanup(call.sid, 'test end');
});

test('ICE arriving before SDP is queued and scoped to the correct connection', async () => {
  const {client, media} = fixture();
  await client.emit('session.invite', 'cid1', 'sid1', {medium: 'voice'});
  await media.accept('sid1');
  await client.emit('connection.ice-candidate', 'cid1', 'sid1', {candidate: {candidate: 'candidate-before-sdp'}}, 'connid1');
  const call = media.calls.get('sid1');
  assert.equal(call.remoteCandidates.length, 1);
  assert.equal(call.pc, null);
  await client.emit('connection.offer', 'cid1', 'sid1', {substrate_setup: {sdp: 'unit-offer'}}, 'connid1');
  assert.deepEqual(call.pc.candidates, [{candidate: 'candidate-before-sdp'}]);
  await client.emit('connection.ice-candidate', 'cid2', 'sid1', {candidate: {candidate: 'other-client'}}, 'connid1');
  await client.emit('connection.ice-candidate', 'cid1', 'sid1', {candidate: {candidate: 'other-connection'}}, 'connid2');
  assert.equal(call.pc.candidates.length, 1);
  media.cleanup('sid1', 'test end');
});

test('switching calls mutes capture and remote playback; hanging up one preserves other cid/session', async () => {
  const {client, media} = fixture();
  const first = await media.invite('cid1', 'customer-1', 'voice');
  await client.emit('session.accept', 'cid1', first.sid);
  const firstPc = first.pc;
  const firstTrack = first.localStream.getAudioTracks()[0];
  const second = await media.invite('cid2', 'customer-2', 'voice');
  await client.emit('session.accept', 'cid2', second.sid);
  assert.equal(firstTrack.enabled, false);
  assert.equal(first.remoteVideo.muted, true);
  assert.equal(first.remoteVideo.paused, true);
  assert.equal(second.localStream.getAudioTracks()[0].enabled, true);
  media.select(first.sid);
  assert.equal(firstTrack.enabled, true);
  assert.equal(second.localStream.getAudioTracks()[0].enabled, false);
  media.mute(); assert.equal(firstTrack.enabled, false);
  media.mute(); assert.equal(firstTrack.enabled, true);
  const secondPc = second.pc;
  await media.hangup(first.sid);
  assert.equal(firstTrack.stopped, true);
  assert.equal(firstPc.closed, true);
  assert.equal(first.cid, 'cid1');
  assert.equal(second.pc, secondPc);
  assert.equal(second.pc.closed, undefined);
  assert.equal(media.list('cid2').length, 1);
  assert.equal(client.sent.at(-1).type, 'session.end');
  assert.equal(client.sent.at(-1).cid, 'cid1');
  media.select(second.sid); assert.equal(second.localStream.getAudioTracks()[0].enabled, true);
  media.suspend(); assert.equal(second.localStream.getAudioTracks()[0].enabled, false);
  assert.equal(second.remoteVideo.muted, true);
  media.cleanup(second.sid, 'test end');
});

test('camera denial falls back to audio while preserving video reception', async () => {
  const denied = Object.assign(new Error('denied'), {name: 'NotAllowedError'});
  const {client, media, notices} = fixture({getUserMedia: async ({video}) => { if (video) throw denied; return new Stream([new Track('audio')]); }});
  const call = await media.invite('cid1', 'customer-1', 'video');
  await client.emit('session.accept', 'cid1', call.sid);
  assert.ok(notices.some((n) => n.includes('摄像头不可用')));
  assert.equal(call.localStream.getVideoTracks().length, 0);
  assert.ok(call.pc.getTransceivers().some((t) => t.receiver.track.kind === 'video' && t.direction === 'recvonly'));
  media.cleanup(call.sid, 'test end');
});

test('denied microphone ends outgoing voice without offering an inactive audio connection', async () => {
  const {client, media, notices} = fixture({getUserMedia: async () => { throw Object.assign(new Error('denied'), {name: 'NotAllowedError'}); }});
  const voice = await media.invite('cid1', 'customer-1', 'voice');
  await client.emit('session.accept', 'cid1', voice.sid);
  assert.equal(voice.state, 'ended');
  assert.equal(voice.localStream, null);
  assert.equal(voice.pc, null);
  assert.equal(media.selectedSid, null);
  assert.equal(client.sent.some((env) => env.type === 'connection.offer'), false);
  assert.equal(client.sent.some((env) => env.type === 'connection.confirm'), false);
  assert.equal(client.sent.at(-1).type, 'session.end');
  assert.ok(notices.some((text) => text.includes('麦克风') && text.includes('允许')));
});

test('unavailable microphone ends accepted incoming voice and leaves the consultation intact', async () => {
  const {client, media, notices} = fixture({getUserMedia: async () => { throw Object.assign(new Error('missing'), {name: 'NotFoundError'}); }});
  await client.emit('session.invite', 'cid1', 'sid1', {medium: 'voice'});
  await media.accept('sid1');
  await client.emit('connection.offer', 'cid1', 'sid1', {substrate_setup: {sdp: 'unit-offer'}}, 'connid1');
  assert.equal(media.calls.get('sid1').state, 'ended');
  assert.equal(media.calls.get('sid1').pc, null);
  assert.equal(client.sent.some((env) => env.type === 'connection.answer'), false);
  assert.equal(client.sent.at(-1).type, 'session.end');
  assert.equal(client.sent.at(-1).cid, 'cid1');
  assert.equal(client.sent.some((env) => env.type === 'conversation.close'), false);
  assert.ok(notices.some((text) => text.includes('连接麦克风')));
});

test('interrupted playback is not misreported as an autoplay permission failure', async () => {
  const {client, media, notices} = fixture();
  const call = await media.invite('cid1', 'customer-1', 'voice');
  await client.emit('session.accept', 'cid1', call.sid);
  call.remoteVideo.play = async () => { throw Object.assign(new Error('interrupted'), {name: 'AbortError'}); };
  await media.playRemote(call);
  assert.equal(Boolean(call.playbackBlocked), false);
  assert.deepEqual(notices, []);
  media.cleanup(call.sid, 'test end');
});

test('autoplay rejection remains visible until explicit playback succeeds and isolation is retained', async () => {
  const {client, media, notices} = fixture();
  const call = await media.invite('cid1', 'customer-1', 'voice');
  await client.emit('session.accept', 'cid1', call.sid);
  call.remoteVideo.play = async () => { throw Object.assign(new Error('denied'), {name: 'NotAllowedError'}); };
  await media.playRemote(call);
  assert.equal(call.playbackBlocked, true);
  assert.ok(notices.some((text) => text.includes('播放通话')));
  call.remoteVideo.play = async () => { call.remoteVideo.paused = false; };
  await media.playRemote();
  assert.equal(call.playbackBlocked, false);
  media.select(null);
  await media.playRemote(call);
  assert.equal(call.remoteVideo.paused, true);
  assert.equal(call.remoteVideo.muted, true);
  media.cleanup(call.sid, 'test end');
});

test('screen sharing replaces selected video track and restores camera when stopped', async () => {
  const screen = new Track('video', 'screen');
  const {client, media} = fixture({getUserMedia: async () => new Stream([new Track('audio'), new Track('video', 'camera')]), getDisplayMedia: async () => new Stream([screen])});
  const call = await media.invite('cid1', 'customer-1', 'video');
  await client.emit('session.accept', 'cid1', call.sid);
  call.pc.connectionState = 'connected';
  const sender = call.pc.getSenders().find((s) => s.track.kind === 'video');
  const camera = sender.track;
  await media.shareScreen(); assert.equal(sender.track, screen);
  media.select(null); assert.equal(screen.enabled, false);
  media.select(call.sid); assert.equal(screen.enabled, true);
  await media.stopScreen(call);
  assert.equal(sender.track, camera);
  assert.equal(screen.stopped, true);
  assert.equal(call.screenStream, null);
  media.cleanup(call.sid, 'test end');
});

test('fast connected state does not confirm before connection.answer ACK', async () => {
  const {client, media} = fixture();
  media.PeerConnection = class FastPeer extends Peer {
    async setLocalDescription(description) {
      await super.setLocalDescription(description);
      if (description.type === 'answer') { this.connectionState = 'connected'; this.onconnectionstatechange(); }
    }
  };
  let release;
  const answerAck = new Promise((resolve) => { release = resolve; });
  const request = client.request.bind(client);
  client.request = async (...args) => {
    const result = await request(...args);
    if (args[0] === 'connection.answer') await answerAck;
    return result;
  };
  await client.emit('session.invite', 'cid1', 'sid1', {medium: 'voice'});
  await media.accept('sid1');
  const offer = client.emit('connection.offer', 'cid1', 'sid1', {substrate_setup: {sdp: 'unit-offer'}}, 'connid1');
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(media.calls.get('sid1').pc.connectionState, 'connected');
  assert.equal(client.sent.some((s) => s.type === 'connection.confirm'), false);
  release(); await offer;
  assert.equal(client.sent.filter((s) => s.type === 'connection.confirm').length, 1);
  media.cleanup('sid1', 'test end');
});

test('late device permission resolution after hangup stops every newly acquired track', async () => {
  let release;
  const permission = new Promise((resolve) => { release = resolve; });
  const {client, media} = fixture({getUserMedia: () => permission});
  const call = await media.invite('cid1', 'customer-1', 'voice');
  const accepted = client.emit('session.accept', 'cid1', call.sid);
  await new Promise((resolve) => setImmediate(resolve));
  const pc = call.pc;
  await media.hangup(call.sid);
  const track = new Track('audio');
  release(new Stream([track]));
  await accepted;
  assert.equal(track.stopped, true);
  assert.equal(pc.closed, true);
  assert.equal(call.localStream, null);
  assert.equal(call.state, 'ended');
});

test('two sessions in one cid remain independent and closing cid leaves other conversations alive', async () => {
  const {client, media} = fixture();
  const first = await media.invite('cid1', 'customer-1', 'voice');
  await client.emit('session.accept', 'cid1', first.sid);
  const second = await media.invite('cid1', 'customer-1', 'video');
  await client.emit('session.accept', 'cid1', second.sid);
  const other = await media.invite('cid2', 'customer-2', 'voice');
  await client.emit('session.accept', 'cid2', other.sid);
  const secondPeer = second.pc;
  await media.hangup(first.sid);
  assert.equal(media.list('cid1').length, 1);
  assert.equal(second.pc, secondPeer);
  assert.equal(secondPeer.closed, undefined);
  media.closeConversation('cid1');
  assert.equal(media.list('cid1').length, 0);
  assert.equal(secondPeer.closed, true);
  assert.equal(other.pc.closed, undefined);
  assert.equal(media.list('cid2').length, 1);
  media.cleanup(other.sid, 'test end');
});
