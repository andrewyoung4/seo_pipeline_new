#!/usr/bin/env node
/**
 * run_lighthouse_batch_windows_v3.js
 * - Windows-safe spawning (shell:true) and npx fallback
 * - Lighthouse v12+ compatible flags:
 *     - mobile: use --form-factor=mobile (no --preset)
 *     - desktop: use --preset=desktop (Lighthouse handles desktop config)
 */
const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

const args = process.argv.slice(2);
function arg(name, defVal=null) {
  const i = args.indexOf(`--${name}`);
  if (i === -1) return defVal;
  return args[i+1] && !args[i+1].startsWith('--') ? args[i+1] : true;
}

const inFile = arg('in');
const outDir = arg('out', './data/outputs/lab');
const concurrency = parseInt(arg('concurrency', '2'), 10);
const device = (arg('device', 'mobile') || 'mobile').toLowerCase(); // 'mobile' or 'desktop'

if (!inFile || !fs.existsSync(inFile)) {
  console.error('Missing --in <urls.txt> OR file not found:', inFile);
  process.exit(1);
}
if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });

const urls = fs.readFileSync(inFile, 'utf8').split(/\r?\n/).map(s => s.trim()).filter(Boolean);

function slug(u) {
  return u.replace(/^https?:\/\//,'').replace(/[^a-z0-9]+/gi,'_').slice(0,120);
}

const queues = Array.from({length: Math.max(1, concurrency)}, () => []);
urls.forEach((u, i) => queues[i % queues.length].push(u));

function runCmd(cmdStr) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmdStr, { stdio: 'inherit', shell: true });
    child.on('close', code => resolve({ code }));
    child.on('error', err => reject(err));
  });
}

function buildArgs(url, outPath) {
  const common = [
    `"${url}"`,
    '--output=json',
    `--output-path="${outPath}"`,
    '--quiet',
    '--chrome-flags="--headless=new"',
    '--only-categories=performance,seo,accessibility,best-practices'
  ];
  if (device === 'desktop') {
    // Lighthouse desktop config
    return common.concat(['--preset=desktop']);
  } else {
    // Mobile default on LH12+: use explicit form factor
    return common.concat(['--form-factor=mobile']);
  }
}

async function runLH(url) {
  const outPath = path.join(outDir, `${slug(url)}.json`);
  const baseArgs = buildArgs(url, outPath).join(' ');

  const cmd1 = process.platform === 'win32'
    ? `cmd.exe /c lighthouse ${baseArgs}`
    : `lighthouse ${baseArgs}`;

  const cmd2 = process.platform === 'win32'
    ? `cmd.exe /c npx -y lighthouse ${baseArgs}`
    : `npx -y lighthouse ${baseArgs}`;

  try {
    let res = await runCmd(cmd1);
    if (res.code === 0) {
      console.log(`[LH] Wrote ${outPath}`);
      return;
    }
    console.warn(`[LH] lighthouse exited with code ${res.code}; trying npx`);
    res = await runCmd(cmd2);
    if (res.code === 0) {
      console.log(`[LH] (npx) Wrote ${outPath}`);
    } else {
      console.error(`[LH] (npx) Failed for ${url} with code ${res.code}`);
    }
  } catch (e) {
    console.error(`[LH] Error for ${url}:`, e.message);
  }
}

async function worker(list) {
  for (const u of list) {
    await runLH(u);
  }
}

(async () => {
  console.log(`[LH] Starting batch for ${urls.length} URLs, concurrency=${concurrency}, device=${device}`);
  await Promise.all(queues.map(worker));
  console.log('[LH] Done.');
})();
