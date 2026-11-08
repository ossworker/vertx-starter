import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { parseArgs, commonOptions, credentials, Runtime, IdPool, Peer, delay, validateIds, enqueue, assigned, closeConsultations, localResource } from './protocol-tools.mjs';

const args = parseArgs(process.argv.slice(2));
if (args.help) {
  console.log('Node 24: node e2e.test.mjs [--target http://127.0.0.1:8080] [--fixture /absolute/local/application.yml] [--agent-token VALUE --customer-token VALUE --customer2-token VALUE --stranger-token VALUE] [--duration 120 --timeout-ms 5000 --rate 200 --max-messages 100000]');
} else {
  test('contact-center local protocol E2E (real assertions, no feature skips)', { timeout: 120000 }, async (t) => {
    const options = commonOptions(args);
    const tokens = await credentials(args, true);
    const runtime = new Runtime(options, Object.values(tokens));
    runtime.onHardDeadline = () => { console.error('E2E hard duration limit; force process cleanup'); process.exit(1); };
    const interrupt = () => runtime.stop('interrupted');
    process.once('SIGINT', interrupt); process.once('SIGTERM', interrupt);
    const pool = new IdPool(runtime);
    const consultations = [];
    let agent;
    t.after(async () => {
      if (agent?.identityId) {
        await closeConsultations(agent, consultations, runtime.cleanupErrors);
        try { await agent.request('workforce.logout', {}, {}, { cleanup: true }); } catch { runtime.cleanupErrors.push('workforce.logout failed'); }
      }
      await runtime.dispose();
      process.removeListener('SIGINT', interrupt); process.removeListener('SIGTERM', interrupt);
      assert.equal(runtime.cleanupErrors.length, 0, `Cleanup failures: ${runtime.cleanupErrors.join('; ')}`);
    });

    await t.test('ID allocation: 1/256, decimal strings, uniqueness, no-store, invalid counts', async () => {
      const all = [];
      for (const count of [1, 256]) {
        const response = await runtime.fetch(`/uctp/ids?count=${count}`);
        assert.equal(response.status, 200);
        assert.match(response.headers.get('cache-control') ?? '', /no-store/i);
        all.push(...validateIds(response.json(), count));
      }
      assert.equal(new Set(all).size, all.length, 'IDs must not repeat across requests');
      for (const count of ['0', '-1', '257', '1.5', 'not-a-number', '']) {
        const response = await runtime.fetch(`/uctp/ids?count=${encodeURIComponent(count)}`);
        assert.equal(response.status, 400, 'illegal count must be rejected');
        assert.match(response.headers.get('cache-control') ?? '', /no-store/i);
      }
    });

    await t.test('invalid auth: unauthenticated command, missing challenge, invalid bearer and resume', async () => {
      const peer = new Peer(runtime, pool, 'invalid-auth');
      try {
        await peer.open();
        const unauthenticated = await peer.request('workforce.agents', {}, {}, { expected: 'error' });
        assert.equal(unauthenticated.frame.payload.code, 401);
        const premature = await peer.request('auth.response', { method: 'bearer', credential: `invalid-${randomUUID()}` }, {}, { expected: 'error' });
        assert.equal(premature.frame.payload.code, 401);
        const challenge = await peer.request('auth.hello', { auth_methods: ['bearer'] }, {}, { expected: 'auth.challenge' });
        const rejected = await peer.request('auth.response', { method: 'bearer', credential: `invalid-${randomUUID()}`, nonce: challenge.frame.payload.nonce }, { in_reply_to: challenge.frame.id }, { expected: 'error' });
        assert.equal(rejected.frame.payload.code, 401);
        const resume = await peer.request('auth.hello', { resume_token: `invalid-${randomUUID()}` }, {}, { expected: 'error' });
        assert.equal(resume.frame.payload.code, 401);
      } finally { await peer.close(); }
    });

    agent = await new Peer(runtime, pool, 'agent').open(); await agent.authenticate(tokens.agent);
    await t.test('uploads: authentication, empty/oversized reject; malicious HTML reject or safe attachment', async () => {
      const denied = await runtime.fetch('/api/upload?kind=file&filename=denied.txt', { method: 'POST', body: 'unauthorized' });
      assert.equal(denied.status, 401);
      const headers = { 'content-type': 'application/octet-stream', authorization: `Bearer ${agent.sessionToken}` };
      const empty = await runtime.fetch('/api/upload?kind=file&filename=empty.txt', { method: 'POST', headers, body: new Uint8Array() });
      assert.equal(empty.status, 400, 'empty upload must be rejected');
      const oversized = await runtime.fetch('/api/upload?kind=file&filename=oversized.bin', { method: 'POST', headers, body: new Uint8Array(10 * 1024 * 1024 + 1) });
      assert.ok([400, 413].includes(oversized.status), 'upload over 10MiB must be rejected, not accepted or 500');
      const html = '<!doctype html><script>globalThis.__upload_execution_forbidden=true</script><img src=x onerror="globalThis.__upload_execution_forbidden=true">';
      const malicious = await runtime.fetch('/api/upload?kind=file&filename=..%2F..%2Fprobe.html', { method: 'POST', headers: { ...headers, 'content-type': 'text/html' }, body: html });
      if ([400, 403, 415, 422].includes(malicious.status)) return; // rejection is a valid security outcome, not SKIP
      assert.equal(malicious.status, 200, 'HTML must be rejected or stored for safe download');
      const data = malicious.json();
      assert.equal(typeof data.url, 'string');
      const url = localResource(options.target, data.url); // no remote redirects or returned off-origin files
      assert.match(url.pathname, /^\/api\/files\/[a-zA-Z0-9._-]+$/, 'opaque file name, no path traversal');
      const file = await runtime.fetch(url.href);
      assert.equal(file.status, 200);
      assert.match(file.headers.get('content-disposition') ?? '', /^attachment\b/i, 'untrusted HTML must never be inline');
      assert.equal(file.headers.get('x-content-type-options')?.toLowerCase(), 'nosniff');
      assert.ok(!/text\/html|application\/xhtml\+xml/i.test(file.headers.get('content-type') ?? ''), 'unsafe HTML response MIME');
      // No delete-upload API exists: the accepted probe remains in local uploaded/.
    });

    const customer1 = await new Peer(runtime, pool, 'customer-1').open(); await customer1.authenticate(tokens.customer);
    const customer2 = await new Peer(runtime, pool, 'customer-2').open(); await customer2.authenticate(tokens.customer2);
    const stranger = await new Peer(runtime, pool, 'nonmember').open(); await stranger.authenticate(tokens.stranger);
    assert.equal(new Set([agent.identityId, customer1.identityId, customer2.identityId]).size, 3, 'agent and two customers must be distinct identities');
    assert.ok(![agent.identityId, customer1.identityId].includes(stranger.identityId), 'stranger must not be a member of the first CID');
    await agent.request('workforce.login', { skills: ['general'] });
    await agent.request('workforce.status', { state: 'available' });
    const cursor = agent.sequence;
    const initial = await Promise.all([enqueue(customer1, consultations), enqueue(customer2, consultations)]);
    await Promise.all(initial.map((conv) => assigned(conv, agent, cursor)));
    const [first, second] = initial;

    async function roster() {
      const entry = await agent.request('workforce.agents', {}, {}, { expected: 'workforce.roster' });
      const record = entry.frame.payload.agents.find((a) => a.identity_id === agent.identityId);
      assert.ok(record, 'agent must appear in roster');
      return record;
    }
    async function exchange(sender, receiver, cid, body, msgId) {
      const id = msgId ?? await pool.next('msgid');
      const pending = receiver.wait((f) => f.type === 'message.send' && !f.in_reply_to && f.cid === cid && f.payload.msg_id === id);
      try {
        const ack = await sender.request('message.send', { msg_id: id, to: 'all', content_type: 'text/plain', body, body_encoding: 'utf8' }, { cid });
        const delivery = await pending;
        assert.equal(ack.frame.type, 'ack', 'commit ACK is distinct from peer delivery');
        assert.equal(delivery.frame.payload.body, runtime.redact(body));
        assert.equal(delivery.frame.payload.content_type, 'text/plain');
        assert.equal(delivery.frame.cid, cid);
        return { id, delivery };
      } finally { pending.cancel?.(); }
    }
    await t.test('two customers assigned to one agent; parallel bidirectional text without auto voice', async () => {
      assert.notEqual(first.cid, second.cid);
      const record = await roster();
      for (const conv of initial) assert.ok(record.assigned_cids.includes(conv.cid), 'one agent must hold both CIDs');
      await Promise.all(initial.flatMap((conv, index) => [
        exchange(conv.customer, agent, conv.cid, `parallel-customer-${index}`),
        exchange(agent, conv.customer, conv.cid, `parallel-agent-${index}`),
      ]));
      assert.equal(agent.frames.filter((e) => initial.some((c) => c.cid === e.frame.cid) && ['session.invite', 'session.ready', 'connection.offer'].includes(e.frame.type)).length, 0, 'enqueue must not fabricate or auto-start voice');
    });

    await t.test('illegal manual agent status is 400', async () => {
      for (const state of ['ready', 'AVAILABLE', '', 3, null]) {
        const entry = await agent.request('workforce.status', { state }, {}, { expected: 'error' });
        assert.equal(entry.frame.payload.code, 400);
      }
    });

    const held = [];
    for (const state of ['busy', 'break']) {
      await t.test(`${state}: existing CIDs deliver text, a new CID stays queued`, async () => {
        await agent.request('workforce.status', { state });
        const customer = state === 'busy' ? customer1 : customer2;
        const before = customer.sequence;
        const conv = await enqueue(customer, consultations); held.push(conv);
        await Promise.all(initial.flatMap((existing) => [
          exchange(existing.customer, agent, existing.cid, `existing-${state}-customer`),
          exchange(agent, existing.customer, existing.cid, `existing-${state}-agent`),
        ]));
        await delay(350);
        assert.equal(customer.frames.filter((e) => e.seq > before && e.frame.type === 'workforce.assigned' && e.frame.cid === conv.cid).length, 0, 'manual non-available status must block new assignment');
        const record = await roster();
        assert.equal(record.state, state);
        assert.ok(!record.assigned_cids.includes(conv.cid));
        for (const existing of initial) assert.ok(record.assigned_cids.includes(existing.cid), 'busy/break must retain existing workload');
      });
    }
    await t.test('available restores all queued CIDs without displacing existing assignments', async () => {
      const before = agent.sequence;
      await agent.request('workforce.status', { state: 'available' });
      await Promise.all(held.map((conv) => assigned(conv, agent, before)));
      const record = await roster();
      for (const conv of consultations) assert.ok(record.assigned_cids.includes(conv.cid));
      await Promise.all(held.map((conv) => exchange(conv.customer, agent, conv.cid, 'after-available')));
    });

    let receiptMessage;
    await t.test('message idempotency: fresh envid/same msg_id does not redeliver; conflicting content errors', async () => {
      receiptMessage = await exchange(customer1, agent, first.cid, 'idempotent-original');
      const before = agent.sequence;
      await customer1.request('message.send', { msg_id: receiptMessage.id, to: 'all', content_type: 'text/plain', body: 'idempotent-original', body_encoding: 'utf8' }, { cid: first.cid });
      await delay(350);
      assert.equal(agent.frames.filter((e) => e.seq > before && e.frame.type === 'message.send' && !e.frame.in_reply_to && e.frame.cid === first.cid && e.frame.payload.msg_id === receiptMessage.id).length, 0, 'retry under another envid must not be delivered twice');
      const conflictBefore = agent.sequence;
      const conflict = await customer1.request('message.send', { msg_id: receiptMessage.id, to: 'all', content_type: 'text/plain', body: 'different-content', body_encoding: 'utf8' }, { cid: first.cid }, { expected: 'error' });
      assert.ok([400, 409].includes(conflict.frame.payload.code), 'same msg_id / different content must be a client conflict');
      await delay(250);
      assert.equal(agent.frames.filter((e) => e.seq > conflictBefore && e.frame.type === 'message.send' && e.frame.payload.msg_id === receiptMessage.id).length, 0, 'conflicting send must have no delivery side effect');
    });

    await t.test('same envid retries replay the ACK without a second message delivery; changed payload conflicts', async () => {
      const id = await pool.next('msgid');
      const env = await customer1.prepare('message.send', { msg_id: id, to: 'all', content_type: 'text/plain', body: 'same-envelope-retry', body_encoding: 'utf8' }, { cid: first.cid });
      const delivered = agent.wait((frame) => frame.type === 'message.send' && !frame.in_reply_to && frame.cid === first.cid && frame.payload.msg_id === id);
      const ack = customer1.wait((frame) => frame.type === 'ack' && frame.in_reply_to === env.id);
      try {
        await customer1.transmit(env);
        await Promise.all([ack, delivered]);
      } finally { ack.cancel?.(); delivered.cancel?.(); }
      const before = agent.sequence;
      const replay = customer1.wait((frame) => frame.type === 'ack' && frame.in_reply_to === env.id);
      try { await customer1.transmit(env); await replay; } finally { replay.cancel?.(); }
      const conflict = customer1.wait((frame) => frame.type === 'error' && frame.in_reply_to === env.id);
      try {
        await customer1.transmit({ ...env, payload: { ...env.payload, body: 'changed-envelope-body' } });
        assert.equal((await conflict).frame.payload.code, 409);
      } finally { conflict.cancel?.(); }
      await delay(150);
      assert.equal(agent.frames.filter((entry) => entry.seq > before && entry.frame.type === 'message.send' && !entry.frame.in_reply_to && entry.frame.payload.msg_id === id).length, 0);
    });

    await t.test('history limit=100: correlated replay precedes ACK and includes both directions once', async () => {
      const before = customer1.sequence;
      const ack = await customer1.request('message.history', { limit: 100 }, { cid: first.cid });
      await delay(25);
      const entries = customer1.frames.filter((e) => e.seq > before && e.frame.in_reply_to === ack.frame.in_reply_to && e.frame.type === 'message.send');
      assert.ok(entries.length > 0 && entries.length <= 100, 'history replay count');
      assert.ok(entries.every((e) => e.seq < ack.seq && e.frame.cid === first.cid), 'history then ACK, scoped to requested CID');
      assert.equal(new Set(entries.map((e) => e.frame.payload.msg_id)).size, entries.length, 'no duplicate logical messages in history');
      assert.ok(entries.some((e) => e.frame.payload.body === 'parallel-customer-0'));
      assert.ok(entries.some((e) => e.frame.payload.body === 'parallel-agent-0'));
    });

    await t.test('same history envid replays all correlated frames and the terminal ACK', async () => {
      const env = await customer1.prepare('message.history', { limit: 100 }, { cid: first.cid });
      const snapshots = [];
      for (let attempt = 0; attempt < 2; attempt++) {
        const before = customer1.sequence;
        const ack = customer1.wait((frame) => frame.type === 'ack' && frame.in_reply_to === env.id);
        try {
          await customer1.transmit(env);
          const terminal = await ack;
          const entries = customer1.frames.filter((entry) => entry.seq > before && entry.frame.in_reply_to === env.id && entry.frame.type === 'message.send');
          assert.ok(entries.length > 0 && entries.every((entry) => entry.seq < terminal.seq));
          snapshots.push(entries.map((entry) => entry.frame));
        } finally { ack.cancel?.(); }
      }
      assert.deepEqual(snapshots[1], snapshots[0], 'a retry must replay the original committed history snapshot');
    });

    await t.test('history limit=100 is enforced with more than 100 committed messages', async () => {
      for (let start = 0; start < 101; start += 8) {
        await Promise.all(Array.from({ length: Math.min(8, 101 - start) }, (_, offset) => {
          const index = start + offset;
          return index % 2 ? exchange(agent, customer1, first.cid, `history-bound-agent-${index}`)
            : exchange(customer1, agent, first.cid, `history-bound-customer-${index}`);
        }));
      }
      const before = customer1.sequence;
      const ack = await customer1.request('message.history', { limit: 100 }, { cid: first.cid });
      await delay(25);
      const replay = customer1.frames.filter((e) => e.seq > before && e.frame.type === 'message.send' && e.frame.in_reply_to === ack.frame.in_reply_to);
      assert.equal(replay.length, 100, 'history must honor its requested 100-message bound');
      assert.ok(replay.every((e) => e.seq < ack.seq && e.frame.cid === first.cid));
      assert.equal(new Set(replay.map((e) => e.frame.payload.msg_id)).size, 100);
      assert.ok(replay.some((e) => e.frame.payload.body.startsWith('history-bound-agent-')));
      assert.ok(replay.some((e) => e.frame.payload.body.startsWith('history-bound-customer-')));
    });

    await t.test('received is a real peer receipt, followed by read receipt', async () => {
      const id = await pool.next('msgid');
      const before = customer1.sequence;
      await exchange(customer1, agent, first.cid, 'peer-receipt-probe', id);
      await delay(250);
      assert.equal(customer1.frames.filter((e) => e.seq > before && e.frame.type === 'message.delivered' && e.frame.payload.msg_id === id).length, 0, 'server submission/dispatch must not invent peer received');
      const received = customer1.wait((f) => f.type === 'message.delivered' && f.cid === first.cid && f.payload.msg_id === id);
      try {
        await agent.request('message.received', { msg_id: id }, { cid: first.cid });
        assert.equal((await received).frame.payload.msg_id, id);
      } finally { received.cancel?.(); }
      const read = customer1.wait((f) => f.type === 'message.read' && f.cid === first.cid && f.payload.msg_id === id);
      try {
        await agent.request('message.read', { msg_id: id }, { cid: first.cid });
        const event = await read;
        assert.equal(event.frame.payload.msg_id, id);
        assert.equal(typeof event.frame.payload.by_participant, 'string');
      } finally { read.cancel?.(); }
    });

    await t.test('batch read matches the browser contract; an invalid target yields no partial receipts', async () => {
      const messages = await Promise.all([exchange(customer1, agent, first.cid, 'batch-read-one'), exchange(customer1, agent, first.cid, 'batch-read-two')]);
      const before = customer1.sequence;
      const reads = messages.map(({ id }) => customer1.wait((frame) => frame.type === 'message.read' && frame.cid === first.cid && frame.payload.msg_id === id));
      try {
        await agent.request('message.read', { msg_ids: messages.map(({ id }) => id) }, { cid: first.cid });
        await Promise.all(reads);
      } finally { reads.forEach((pending) => pending.cancel?.()); }
      assert.equal(customer1.frames.filter((entry) => entry.seq > before && entry.frame.type === 'message.read').length, 2);
      const deniedBefore = customer1.sequence;
      const error = await agent.request('message.read', { msg_ids: [messages[0].id, await pool.next('msgid')] }, { cid: first.cid }, { expected: 'error' });
      assert.equal(error.frame.payload.code, 404);
      await delay(100);
      assert.equal(customer1.frames.filter((entry) => entry.seq > deniedBefore && entry.frame.type === 'message.read').length, 0);
    });

    const sid = await pool.next('sid');
    await t.test('an accepted voice invitation can end without releasing its consultation CID (no media claim)', async () => {
      const pending = agent.wait((f) => f.type === 'session.invite' && f.cid === first.cid && f.sid === sid);
      try {
        await customer1.request('session.invite', { to: [agent.identityId], medium: 'voice' }, { cid: first.cid, sid });
        await pending;
      } finally { pending.cancel?.(); }
      await agent.request('session.accept', {}, { cid: first.cid, sid });
      await customer1.request('session.end', {}, { cid: first.cid, sid });
      assert.ok((await roster()).assigned_cids.includes(first.cid), 'session.end must not release workforce CID');
      await exchange(customer1, agent, first.cid, 'message-after-session-end');
      // No fake SDP, connection.answer/confirm or claims of real media readiness.
    });

    await t.test('nonmember cannot subscribe/query/invite/change role/send/close or observe group traffic', async () => {
      const denied = (entry) => assert.ok([403, 404].includes(entry.frame.payload.code), 'nonmember needs an authorization/not-found denial, not ACK/501');
      const liveSid = await pool.next('sid');
      await customer1.request('session.invite', { to: [agent.identityId], medium: 'voice' }, { cid: first.cid, sid: liveSid });
      await agent.request('session.accept', {}, { cid: first.cid, sid: liveSid });
      const strangerBefore = stranger.sequence;
      const attackSid = await pool.next('sid');
      denied(await stranger.request('conversation.subscribe', {}, { cid: first.cid }, { expected: 'error' }));
      denied(await stranger.request('message.history', { limit: 100 }, { cid: first.cid }, { expected: 'error' }));
      denied(await stranger.request('session.invite', { to: [agent.identityId], medium: 'voice' }, { cid: first.cid, sid: attackSid }, { expected: 'error' }));
      denied(await stranger.request('session.update', { kind: 'role-change', details: { participant_id: first.participantId, new_role: 'supervisor' } }, { cid: first.cid, sid: liveSid }, { expected: 'error' }));
      denied(await stranger.request('message.send', { msg_id: await pool.next('msgid'), to: 'all', content_type: 'text/plain', body: 'unauthorized', body_encoding: 'utf8' }, { cid: first.cid }, { expected: 'error' }));
      denied(await stranger.request('conversation.close', {}, { cid: first.cid }, { expected: 'error' }));
      await exchange(customer1, agent, first.cid, 'private-after-denied-subscription');
      const memberSid = await pool.next('sid');
      await customer1.request('session.invite', { to: [agent.identityId], medium: 'voice' }, { cid: first.cid, sid: memberSid });
      await customer1.request('session.cancel', {}, { cid: first.cid, sid: memberSid });
      await customer1.request('session.end', {}, { cid: first.cid, sid: liveSid });
      await delay(350);
      assert.equal(stranger.frames.filter((e) => e.seq > strangerBefore && e.frame.cid === first.cid && e.frame.type !== 'error').length, 0, 'denied commands must not silently subscribe the socket');
    });

    await t.test('conversation.close releases each assigned CID; closed consultation rejects later messages', async () => {
      const strangerBefore = stranger.sequence;
      for (const conv of consultations) {
        const closed = conv.customer.wait((f) => f.type === 'conversation.closed' && f.cid === conv.cid);
        try {
          await agent.request('conversation.close', { reason: 'e2e-finished' }, { cid: conv.cid }); conv.closed = true;
          await closed;
        } finally { closed.cancel?.(); }
        const entry = await conv.customer.request('message.send', { msg_id: await pool.next('msgid'), to: 'all', content_type: 'text/plain', body: 'after-close', body_encoding: 'utf8' }, { cid: conv.cid }, { expected: 'error' });
        assert.ok([404, 409, 410].includes(entry.frame.payload.code));
      }
      // Cluster notification may trail the close ACK; poll a bounded condition, not a fixed long sleep.
      let record;
      const until = Date.now() + options.timeoutMs;
      do { record = await roster(); if (!consultations.some((c) => record.assigned_cids.includes(c.cid))) break; await delay(25); } while (Date.now() < until);
      assert.ok(!consultations.some((c) => record.assigned_cids.includes(c.cid)), 'all test CIDs released from workforce');
      await delay(100);
      assert.equal(stranger.frames.filter((e) => e.seq > strangerBefore && e.frame.type === 'conversation.closed' && e.frame.cid === first.cid).length, 0, 'nonmember must not observe closed-CID group traffic');
    });
  });
}
