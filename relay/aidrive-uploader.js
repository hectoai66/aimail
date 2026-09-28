/**
 * AI드라이브 업로더 릴레이 (릴레이 PC 에서 상주 실행)
 *   - 워커의 /api/queue/aidrive_pop 을 30초마다 폴링
 *   - Base64 컨텐츠를 파일로 저장 후 gsk aidrive upload 실행
 *   - 완료 후 /api/queue/aidrive_done 으로 결과 보고
 *
 * 실행:  node aidrive-uploader.js
 *   또는  npm run aidrive
 *
 * 필수 환경변수 (.env):
 *   WORKER_URL=https://hecto-torres-worker.hectoai240.workers.dev
 *   RELAY_TOKEN=chSrfgT0zwD37xG9dWbyitCkNIBljXRE
 *   GSK_BIN=gsk        # gsk CLI 경로 (Windows: gsk.exe)
 */

const fs = require('fs');
const path = require('path');
const os = require('os');
const { execSync } = require('child_process');

const WORKER_URL = process.env.WORKER_URL || 'https://hecto-torres-worker.hectoai240.workers.dev';
const RELAY_TOKEN = process.env.RELAY_TOKEN || '';
const GSK_BIN = process.env.GSK_BIN || 'gsk';
const POLL_MS = 30 * 1000;

if (!RELAY_TOKEN) { console.error('RELAY_TOKEN 미설정'); process.exit(1); }

async function popJob() {
  const r = await fetch(`${WORKER_URL}/api/queue/aidrive_pop`, {
    method: 'POST',
    headers: { 'X-Relay-Token': RELAY_TOKEN, 'Content-Type': 'application/json' },
    body: '{}',
  });
  return await r.json();
}

async function reportDone(id, ok, error) {
  await fetch(`${WORKER_URL}/api/queue/aidrive_done`, {
    method: 'POST',
    headers: { 'X-Relay-Token': RELAY_TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify({ id, success: ok, error: error || '' }),
  });
}

async function uploadOne(job) {
  const tmpDir = path.join(os.tmpdir(), 'hecto-aidrive');
  fs.mkdirSync(tmpDir, { recursive: true });
  const localPath = path.join(tmpDir, job.filename);
  fs.writeFileSync(localPath, Buffer.from(job.content_b64, 'base64'));

  const drivePath = (job.aidrive_path || '/').replace(/\/$/, '') + '/' + job.filename;
  const cmd = `${GSK_BIN} aidrive upload "${localPath}" "${drivePath}"`;
  console.log(new Date().toISOString(), 'UPLOAD', cmd);
  execSync(cmd, { stdio: 'inherit' });

  try { fs.unlinkSync(localPath); } catch {}
}

async function processOnce() {
  const res = await popJob();
  if (!(res.ok && res.job)) return { empty: true };
  const { id } = res.job;
  try {
    await uploadOne(res.job);
    await reportDone(id, true);
    console.log('✓ uploaded', res.job.filename);
    return { ok: true, id };
  } catch (e) {
    console.error('✗ upload failed', e.message);
    await reportDone(id, false, e.message);
    return { ok: false, id, error: e.message };
  }
}

// --once 모드: 큐가 빌 때까지 반복 pop 하고 종료 (GitHub Actions cron 전용)
const ONCE_MODE = process.argv.includes('--once') || process.env.UPLOADER_ONCE === '1';
const MAX_ONCE  = Number(process.env.MAX_ONCE || 10); // 안전 상한

async function loop() {
  try { await processOnce(); }
  catch (e) { console.error('poll error', e.message); }
  finally { setTimeout(loop, POLL_MS); }
}

async function mainOnce() {
  console.log('[once] AI드라이브 업로더 · 1회 실행 (max', MAX_ONCE, 'jobs)');
  let count = 0;
  while (count < MAX_ONCE) {
    const r = await processOnce();
    if (r?.empty) break;
    count++;
  }
  console.log('[once] 완료 ·', count, 'jobs processed');
  process.exit(0);
}

if (ONCE_MODE) mainOnce();
else { console.log('AI드라이브 업로더 시작. 워커:', WORKER_URL); loop(); }
