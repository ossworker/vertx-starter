import { writeFileSync, mkdirSync } from 'node:fs';
import { stat } from 'node:fs/promises';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import os from 'node:os';
import { randomUUID } from 'node:crypto';
import { parseArgs, commonOptions, credentials, integer, Runtime, IdPool, Peer, now, delay, percentiles, enqueue, assigned, closeConsultations } from './protocol-tools.mjs';

export function loadOptions(args) {
  const options = commonOptions(args);
  const stages = String(args.stages ?? '10,25,50').split(',').map((s) => integer(s, 'stage customers', 1, 199));
  if (!stages.length || stages.length > 8) throw new Error('Specify 1..8 stages');
  const warmupSeconds = integer(args['warmup-seconds'] ?? 2, 'warmup-seconds', 0, 20);
  const stageSeconds = integer(args['stage-seconds'] ?? 8, 'stage-seconds', 1, 100);
  if (stages.length * (warmupSeconds + stageSeconds) + 5 >= options.duration) throw new Error('Warmup + measured stages must leave setup/drain/cleanup time inside total duration');
  return { ...options, stages, warmupSeconds, stageSeconds,
    maxInflight: integer(args['max-inflight'] ?? 400, 'max-inflight', 1, 2000),
    stopErrors: integer(args['stop-errors'] ?? 20, 'stop-errors', 1, 100000),
    outputDir: resolve(args['output-dir'] ?? fileURLToPath(new URL('./results', import.meta.url))),
  };
}

// Correlate submission ACK by envid and actual delivery by (cid,msg_id).
// A message succeeds only if BOTH independent observations succeed.
export class Measurements {
  constructor(timeoutMs) {
    this.timeoutMs = timeoutMs;
    this.byEnvelope = new Map(); this.byMessage = new Map(); this.seenDeliveries = new Set();
    this.stats = { warmup: this.empty(), measured: this.empty() };
    this.duplicates = 0;
  }
  empty() { return { initiated: 0, successful: 0, errors: 0, timeouts: 0,
    ack: { successful: 0, errors: 0, timeouts: 0, samples: [] }, delivery: { successful: 0, errors: 0, timeouts: 0, samples: [] } }; }
  key(cid, msgId) { return `${cid}\u0000${msgId}`; }
  begin(env, phase, at) {
    const key = this.key(env.cid, env.payload.msg_id);
    if (this.byMessage.has(key)) throw new Error('Duplicate load message identity');
    const record = { envId: env.id, key, phase, at, ack: null, delivery: null };
    this.stats[phase].initiated++;
    this.byEnvelope.set(env.id, record); this.byMessage.set(key, record);
    return record;
  }
  finish(record, side, outcome, at) {
    if (record[side]) return;
    record[side] = outcome;
    const stats = this.stats[record.phase][side];
    stats[outcome === 'success' ? 'successful' : outcome === 'error' ? 'errors' : 'timeouts']++;
    if (outcome === 'success') stats.samples.push(at - record.at);
    if (record.ack && record.delivery) {
      const all = this.stats[record.phase];
      if (record.ack === 'success' && record.delivery === 'success') all.successful++;
      else if (record.ack === 'error' || record.delivery === 'error') all.errors++;
      else all.timeouts++;
      this.byEnvelope.delete(record.envId); this.byMessage.delete(record.key);
    }
  }
  observeAck(frame, at) {
    if (!['ack', 'error'].includes(frame.type)) return;
    const record = this.byEnvelope.get(frame.in_reply_to);
    if (!record) return;
    this.finish(record, 'ack', frame.type === 'ack' ? 'success' : 'error', at);
    if (frame.type === 'error') this.finish(record, 'delivery', 'error', at);
  }
  observeDelivery(frame, at) {
    if (frame.type !== 'message.send' || frame.in_reply_to) return; // no server "delivered" or history replay samples
    const key = this.key(frame.cid, frame.payload?.msg_id);
    if (this.seenDeliveries.has(key)) { this.duplicates++; return; }
    const record = this.byMessage.get(key);
    if (!record) return;
    this.seenDeliveries.add(key);
    this.finish(record, 'delivery', 'success', at);
  }
  expire(at, force = false) {
    for (const record of [...this.byMessage.values()]) {
      if (force || at - record.at >= this.timeoutMs) {
        this.finish(record, 'ack', 'timeout', at); this.finish(record, 'delivery', 'timeout', at);
      }
    }
  }
  failSend(record, at) { this.finish(record, 'ack', 'error', at); this.finish(record, 'delivery', 'error', at); }
  summary(phase) {
    const stats = this.stats[phase];
    return { initiated: stats.initiated, successful: stats.successful, errors: stats.errors, timeouts: stats.timeouts,
      ack: { successful: stats.ack.successful, errors: stats.ack.errors, timeouts: stats.ack.timeouts, latency_ms: percentiles(stats.ack.samples) },
      delivery: { successful: stats.delivery.successful, errors: stats.delivery.errors, timeouts: stats.delivery.timeouts, latency_ms: percentiles(stats.delivery.samples) } };
  }
}

export async function runLoad(args) {
  const options = loadOptions(args);
  const tokens = await credentials(args);
  // Verify the selected parent before creating the output directory; never touch app configuration.
  const parent = dirname(options.outputDir);
  if (!(await stat(parent)).isDirectory()) throw new Error('Report output parent must already exist');
  mkdirSync(options.outputDir, { recursive: true });
  const reportPath = resolve(options.outputDir, `load-${new Date().toISOString().replace(/[:.]/g, '-')}-${randomUUID()}.json`);
  const runtime = new Runtime(options, Object.values(tokens));
  const pool = new IdPool(runtime);
  const report = {
    schema_version: 1, started_at: new Date().toISOString(), target: options.target,
    machine: { platform: os.platform(), release: os.release(), arch: os.arch(), cpus: os.cpus().length, cpu_model: os.cpus()[0]?.model ?? null, total_memory_bytes: os.totalmem() },
    node_version: process.version,
    parameters: { stages_customers: options.stages, agent_connections: 1, duration_limit_seconds: options.duration,
      message_limit_all_outbound_frames: options.maxMessages, rate_limit_all_outbound_frames_per_second: options.rate,
      stage_seconds: options.stageSeconds, warmup_seconds: options.warmupSeconds, timeout_ms: options.timeoutMs,
      max_inflight: options.maxInflight, stop_errors: options.stopErrors, connection_limit: 200, cleanup_reserve_seconds: 5 },
    measurement: { clock: 'monotonic performance.now()', ack: 'message.send commit submission ACK correlated by in_reply_to/envid',
      delivery: 'actual customer -> agent native WebSocket message.send arrival correlated by cid/msg_id; not message.delivered, not history',
      successful: 'both commit ACK and actual peer delivery', percentile: 'nearest-rank; milliseconds; null for zero samples',
      warmup: 'tagged at send time, drained separately, excluded from measured percentiles and throughput',
      topology: 'independent persistent chat CID per customer connection, one multi-CID agent; development customer fixture may be reused across connections; no media Session or SDP' },
    stages: [], cleanup_errors: runtime.cleanupErrors, status: 'running', stop_reason: null,
  };
  let active = null;
  const writeReport = () => {
    report.finished_at = new Date().toISOString();
    report.actual_duration_seconds = (now() - runtime.startedAt) / 1000;
    report.outbound_frames = runtime.sent;
    report.peak_connections = runtime.peakConnections;
    if (active) {
      active.tracker.expire(now(), true);
      active.row.warmup = active.tracker.summary('warmup'); active.row.measured = active.tracker.summary('measured');
      active.row.actual_duration_seconds = (now() - active.stageStart) / 1000;
      active.row.status = 'failed'; active.row.failure = 'hard-duration-limit';
      active.row.unexpected_duplicate_deliveries = active.tracker.duplicates;
    }
    writeFileSync(reportPath, JSON.stringify(report, null, 2) + '\n', { mode: 0o600 });
  };
  runtime.onHardDeadline = () => {
    report.status = 'failed'; report.stop_reason = 'hard-duration-limit';
    try { writeReport(); console.error(`Local load hard deadline; partial JSON: ${reportPath}`); } finally { process.exit(1); }
  };
  const interrupt = () => runtime.stop('interrupted');
  process.once('SIGINT', interrupt); process.once('SIGTERM', interrupt);
  try {
    for (const clients of options.stages) {
      runtime.check();
      const stageStart = now();
      const row = { customer_connections_requested: clients, agent_connections_requested: 1, total_connections_requested: clients + 1, peak_connections: 0,
        actual_duration_seconds: null, measured_send_duration_seconds: null, measured_completion_duration_seconds: null,
        throughput_successful_per_second: null, throughput_commit_ack_per_second: null, throughput_peer_delivery_per_second: null,
        media_sessions_created: 0, warmup: null, measured: null, unexpected_duplicate_deliveries: 0, status: 'running' };
      report.stages.push(row);
      runtime.onPeerCount = (count) => { row.peak_connections = Math.max(row.peak_connections, count); };
      const tracked = [];
      let agent;
      const tracker = new Measurements(options.timeoutMs);
      active = { row, tracker, stageStart };
      try {
        agent = await new Peer(runtime, pool, 'load-agent').open(); await agent.authenticate(tokens.agent);
        await agent.request('workforce.login', { skills: ['general'] });
        await agent.request('workforce.status', { state: 'available' });
        // One native WS per customer, one unique consulting CID per WS, one multi-CID agent.
        // Setup retains events so fast assigned notifications cannot race later waits.
        for (let i = 0; i < clients; i++) {
          const customer = await new Peer(runtime, pool, `load-customer-${i}`).open(); await customer.authenticate(tokens.customer);
          if (customer.identityId === agent.identityId) throw new Error('Load customer and agent must be distinct authenticated identities');
          const before = agent.sequence;
          const conv = await enqueue(customer, tracked);
          await assigned(conv, agent, before);
        }
        if (new Set(tracked.map((c) => c.cid)).size !== clients) throw new Error('Load requires a unique CID per customer');
        row.distinct_customer_identities = new Set(tracked.map((c) => c.customer.identityId)).size;
        const observe = ({ frame, at }) => { tracker.observeAck(frame, at); tracker.observeDelivery(frame, at); };
        agent.listeners.add(observe); agent.retain = false; agent.frames = [];
        for (const conv of tracked) { conv.customer.listeners.add(observe); conv.customer.retain = false; conv.customer.frames = []; }
        let rotating = 0;
        const checkStop = () => {
          runtime.check(); tracker.expire(now());
          if ([agent, ...tracked.map((c) => c.customer)].some((p) => p.failure)) throw new Error('WebSocket failed during load');
          const failures = Object.values(tracker.stats).reduce((n, s) => n + s.errors + s.timeouts, 0) + tracker.duplicates;
          if (failures >= options.stopErrors) throw new Error('Error/timeout/duplicate stop threshold reached');
        };
        const drain = async () => {
          while (tracker.byMessage.size) { checkStop(); await delay(5); }
        };
        const sendPhase = async (phase, seconds) => {
          const start = now(); const until = start + seconds * 1000;
          let nextAt = start;
          while (now() < until) {
            checkStop();
            if (tracker.byMessage.size >= options.maxInflight) { await delay(2); continue; }
            if (now() < nextAt) { await delay(Math.min(5, nextAt - now())); continue; }
            const conv = tracked[rotating++ % tracked.length];
            const msgId = await pool.next('msgid');
            const env = await conv.customer.prepare('message.send', { msg_id: msgId, to: 'all', content_type: 'text/plain', body: `bounded-${phase}-${rotating}`, body_encoding: 'utf8' }, { cid: conv.cid });
            if (now() >= until) break;
            let record;
            try { await conv.customer.transmit(env, { onSent: (at) => { record = tracker.begin(env, phase, at); } }); }
            catch (error) { if (record) tracker.failSend(record, now()); throw error; }
            // Do not catch up with unbounded bursts after ID allocation or event-loop stalls.
            nextAt = Math.max(nextAt + 1000 / options.rate, now());
          }
          return { start, end: now() };
        };
        await sendPhase('warmup', options.warmupSeconds); await drain();
        const measured = await sendPhase('measured', options.stageSeconds); await drain();
        row.measured_send_duration_seconds = (measured.end - measured.start) / 1000;
        row.measured_completion_duration_seconds = (now() - measured.start) / 1000;
        row.warmup = tracker.summary('warmup'); row.measured = tracker.summary('measured');
        const seconds = row.measured_completion_duration_seconds;
        row.throughput_successful_per_second = row.measured.successful / seconds;
        row.throughput_commit_ack_per_second = row.measured.ack.successful / seconds;
        row.throughput_peer_delivery_per_second = row.measured.delivery.successful / seconds;
        row.unexpected_duplicate_deliveries = tracker.duplicates;
        row.status = row.measured.initiated > 0 && !row.measured.errors && !row.measured.timeouts && !row.warmup.errors && !row.warmup.timeouts && !tracker.duplicates ? 'passed' : 'failed';
        if (row.status !== 'passed') throw new Error('Stage has missing samples, errors, timeouts or duplicate deliveries');
      } catch (error) {
        tracker.expire(now(), true); row.warmup = tracker.summary('warmup'); row.measured = tracker.summary('measured');
        row.status = 'failed'; row.failure = runtime.redact(error.message); row.unexpected_duplicate_deliveries = tracker.duplicates;
        throw error;
      } finally {
        if (agent?.identityId) {
          await closeConsultations(agent, tracked, runtime.cleanupErrors);
          try { await agent.request('workforce.logout', {}, {}, { cleanup: true }); } catch { runtime.cleanupErrors.push('workforce.logout failed'); }
        }
        await runtime.dispose({ final: false });
        row.actual_duration_seconds = (now() - stageStart) / 1000;
        active = null;
        if (runtime.cleanupErrors.length) throw new Error('Stage cleanup failed');
      }
    }
    report.status = 'passed'; report.stop_reason = 'all-stages-completed';
  } catch (error) {
    report.status = 'failed'; report.stop_reason = runtime.stopReason ?? runtime.redact(error.message);
    process.exitCode = 1;
  } finally {
    await runtime.dispose();
    process.removeListener('SIGINT', interrupt); process.removeListener('SIGTERM', interrupt);
    if (runtime.cleanupErrors.length) { report.status = 'failed'; process.exitCode = 1; }
    writeReport();
    console.log(`Local load ${report.status}; JSON: ${reportPath}`);
  }
  return { report, reportPath };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const args = parseArgs(process.argv.slice(2), ['stages', 'warmup-seconds', 'stage-seconds', 'max-inflight', 'stop-errors', 'output-dir']);
    if (args.help) console.log('Node 24: node load.mjs --target http://127.0.0.1:8080 --stages 10,25,50 --stage-seconds 8 --warmup-seconds 2 --rate 200 --duration 120 --max-messages 100000 --timeout-ms 5000 --max-inflight 400 --stop-errors 20 --output-dir /existing/parent/results [--fixture /absolute/local/application.yml OR --agent-token VALUE --customer-token VALUE]. Local only; 1 agent + at most 199 customers; no external SMS/email or fake media.');
    else await runLoad(args);
  } catch (error) { console.error(`Local load failed: ${error instanceof Error ? error.message : 'invalid arguments'}`); process.exitCode = 1; }
}
