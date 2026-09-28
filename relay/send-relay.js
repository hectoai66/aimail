// ═══════════════════════════════════════════════════════════════════════════
// Hecto Torres SMTP Relay
// - 매 10분(기본) 워커 /api/queue/pop 폴링
// - 네이버웍스 SMTP로 실제 발송
// - 결과를 /api/queue/done 으로 리포트
// ═══════════════════════════════════════════════════════════════════════════

'use strict';

const nodemailer = require('nodemailer');

// ── 설정 ─────────────────────────────────────────────────────────────
const WORKER_BASE  = process.env.WORKER_BASE  || 'https://hecto-torres-worker.hectoai240.workers.dev';
const RELAY_TOKEN  = process.env.RELAY_TOKEN  || '';   // 워커 시크릿과 동일
const POLL_MS      = Number(process.env.POLL_MS || 10 * 60 * 1000); // 10분
const BATCH        = Number(process.env.BATCH || 20);
const PER_MAIL_MS  = Number(process.env.PER_MAIL_MS || 2500); // 발송 간 딜레이 2.5초 (스팸 회피)

if (!RELAY_TOKEN) {
  console.error('[FATAL] RELAY_TOKEN 환경변수가 필요합니다.');
  process.exit(1);
}

function ts() { return new Date().toISOString().replace('T',' ').slice(0,19); }
function log(...a) { console.log(`[${ts()}]`, ...a); }
function err(...a) { console.error(`[${ts()}]`, ...a); }
async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── 워커 API ─────────────────────────────────────────────────────────
async function popQueue() {
  const r = await fetch(`${WORKER_BASE}/api/queue/pop`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Relay-Token': RELAY_TOKEN },
    body: JSON.stringify({ batch: BATCH }),
  });
  if (!r.ok) throw new Error(`pop failed: HTTP ${r.status}`);
  return await r.json();
}

async function reportDone(results) {
  // 워커 v2.1은 { jobs: [{id, status, message_id, error}] } 를 기대
  const jobs = results.map(r => ({
    id: r.id,
    status: r.ok ? 'sent' : 'failed',
    message_id: r.message_id || null,
    error: r.error || null,
  }));
  const r = await fetch(`${WORKER_BASE}/api/queue/done`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Relay-Token': RELAY_TOKEN },
    body: JSON.stringify({ jobs }),
  });
  if (!r.ok) throw new Error(`done failed: HTTP ${r.status}`);
  return await r.json();
}

async function heartbeat(pendingJobs) {
  try {
    const os = require('os');
    await fetch(`${WORKER_BASE}/api/relay/heartbeat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Relay-Token': RELAY_TOKEN },
      body: JSON.stringify({
        version: '2.1',
        hostname: os.hostname(),
        pending_jobs: pendingJobs,
      }),
    });
  } catch (e) { /* heartbeat 실패는 조용히 */ }
}

// ── SMTP 전송 ────────────────────────────────────────────────────────
function makeTransport(smtp) {
  return nodemailer.createTransport({
    host: smtp.host,
    port: smtp.port,
    secure: !!smtp.secure,
    auth: { user: smtp.user, pass: smtp.pass },
    tls: { rejectUnauthorized: false },
  });
}

async function sendOne(transporter, sender, job) {
  const info = await transporter.sendMail({
    from: `"${sender.name}" <${sender.email}>`,
    replyTo: sender.reply_to,
    to: job.to_name ? `"${job.to_name}" <${job.to_email}>` : job.to_email,
    subject: job.subject,
    html: job.html,
    headers: {
      'X-Mailer': 'hecto-torres-relay/1.0',
    },
  });
  return info.messageId;
}

// ── 메인 루프 ────────────────────────────────────────────────────────
async function processOnce() {
  let data;
  try {
    data = await popQueue();
  } catch (e) {
    err('pop error:', e.message);
    return;
  }
  const jobs = data.jobs || [];
  // heartbeat 매 폴링마다
  await heartbeat(jobs.length);
  if (jobs.length === 0) { log('큐 비어있음.'); return; }

  log(`잡 ${jobs.length}건 수신, SMTP 발송 시작.`);
  const transporter = makeTransport(data.smtp);
  try { await transporter.verify(); }
  catch (e) {
    err('SMTP 인증 실패:', e.message);
    // 전체 실패로 리포트하여 큐가 재시도 가능하게
    await reportDone(jobs.map(j => ({ id: j.id, ok: false, error: 'smtp_verify_failed: ' + e.message })));
    return;
  }

  const results = [];
  for (const job of jobs) {
    try {
      const messageId = await sendOne(transporter, data.sender, job);
      log(`  ✓ ${job.to_email} (${messageId})`);
      results.push({ id: job.id, ok: true, message_id: messageId });
    } catch (e) {
      err(`  ✗ ${job.to_email} : ${e.message}`);
      results.push({ id: job.id, ok: false, error: e.message });
    }
    await sleep(PER_MAIL_MS);
  }
  transporter.close();

  try {
    await reportDone(results);
    const okCount = results.filter(r => r.ok).length;
    log(`리포트 완료. 성공 ${okCount}/${results.length}`);
  } catch (e) {
    err('done report error:', e.message);
  }
}

// ── --once 모드: 한 번만 큐 pop 하고 종료 (GitHub Actions cron 용) ──
const ONCE_MODE = process.argv.includes('--once') || process.env.RELAY_ONCE === '1';

async function main() {
  if (ONCE_MODE) {
    log(`[once] Hecto Torres SMTP Relay — 1회 실행 (WORKER=${WORKER_BASE})`);
    try {
      await processOnce();
      log('[once] 완료 · 종료');
      process.exit(0);
    } catch (e) {
      err('[once] fatal:', e.message);
      process.exit(1);
    }
    return;
  }
  log(`Hecto Torres SMTP Relay 시작 — WORKER=${WORKER_BASE}, 폴링=${Math.round(POLL_MS/1000)}s`);
  while (true) {
    try { await processOnce(); }
    catch (e) { err('loop error:', e); }
    await sleep(POLL_MS);
  }
}

main().catch(e => { err('fatal:', e); process.exit(1); });
