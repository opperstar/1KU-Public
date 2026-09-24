import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fork } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { DatabaseSync } from 'node:sqlite';

const SELF = fileURLToPath(import.meta.url);

function errorInfo(error) {
  return {
    code: error?.code ?? null,
    errcode: error?.errcode ?? null,
    errstr: error?.errstr ?? null,
    message: error?.message ?? String(error),
  };
}

function pragmaValue(row, key) {
  return String(row?.[key] ?? '').toLowerCase();
}

function waitFor(child, predicate, timeoutMs = 10000) {
  const events = [];
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error('child timeout: ' + JSON.stringify(events.slice(-8))));
    }, timeoutMs);
    const onMessage = (message) => {
      events.push(message);
      if (predicate(message)) {
        cleanup();
        resolve(message);
      }
    };
    const onExit = (code, signal) => {
      cleanup();
      reject(new Error('child exited before expected event: ' + JSON.stringify({ code, signal, events })));
    };
    function cleanup() {
      clearTimeout(timeout);
      child.off('message', onMessage);
      child.off('exit', onExit);
    }
    child.on('message', onMessage);
    child.on('exit', onExit);
  });
}

async function legacyHolderMain(databasePath) {
  const database = new DatabaseSync(databasePath);
  database.exec('PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; PRAGMA busy_timeout=50');
  database.prepare('INSERT INTO ticks(origin) VALUES (?)').run('legacy-ready');

  let reads = 0;
  let writes = 0;
  const errors = [];
  const timer = setInterval(() => {
    try {
      database.prepare('SELECT compatibility_epoch FROM meta WHERE id=1').get();
      reads += 1;
    } catch (error) {
      if (errors.length < 12) errors.push({ phase: 'read', error: errorInfo(error) });
    }
    try {
      database.prepare('INSERT INTO ticks(origin) VALUES (?)').run('legacy-tick');
      writes += 1;
    } catch (error) {
      if (errors.length < 12) errors.push({ phase: 'write', error: errorInfo(error) });
    }
  }, 25);

  process.on('message', (message) => {
    if (message === 'snapshot') {
      process.send?.({ event: 'snapshot', reads, writes, errors });
    }
    if (message === 'stop') {
      clearInterval(timer);
      let closeError = null;
      try { database.close(); } catch (error) { closeError = errorInfo(error); }
      process.send?.({ event: 'stopped', reads, writes, errors, closeError }, () => process.exit(0));
    }
  });

  process.send?.({ event: 'ready' });
}

function lateLegacyMain(databasePath) {
  let database;
  const result = { event: 'late-result', open: null, read: null, write: null };
  try {
    database = new DatabaseSync(databasePath);
    database.exec('PRAGMA busy_timeout=100');
    result.open = { ok: true };
  } catch (error) {
    result.open = { ok: false, error: errorInfo(error) };
    process.send?.(result, () => process.exit(0));
    return;
  }

  try {
    result.read = {
      ok: true,
      row: database.prepare('SELECT compatibility_epoch FROM meta WHERE id=1').get(),
    };
  } catch (error) {
    result.read = { ok: false, error: errorInfo(error) };
  }

  try {
    database.prepare('INSERT INTO ticks(origin) VALUES (?)').run('late-legacy');
    result.write = { ok: true };
  } catch (error) {
    result.write = { ok: false, error: errorInfo(error) };
  }

  try { database.close(); } catch {}
  process.send?.(result, () => process.exit(0));
}

function spawnLegacyHolder(databasePath) {
  return fork(SELF, ['--legacy-holder', databasePath], {
    stdio: ['ignore', 'ignore', 'ignore', 'ipc'],
  });
}

async function runLateLegacy(databasePath) {
  const child = fork(SELF, ['--late-legacy', databasePath], {
    stdio: ['ignore', 'ignore', 'ignore', 'ipc'],
  });
  return waitFor(child, (message) => message?.event === 'late-result');
}

function openCutoverWindow(databasePath) {
  const evidence = {
    lockingMode: null,
    journalModeBefore: null,
    journalModeTransition: null,
    beginExclusive: null,
  };
  let database;
  try {
    database = new DatabaseSync(databasePath);
    database.exec('PRAGMA busy_timeout=200');

    evidence.journalModeBefore = database.prepare('PRAGMA journal_mode').get();
    evidence.lockingMode = database.prepare('PRAGMA locking_mode=EXCLUSIVE').get();

    try {
      const row = database.prepare('PRAGMA journal_mode=DELETE').get();
      evidence.journalModeTransition = { ok: true, row };
    } catch (error) {
      evidence.journalModeTransition = { ok: false, error: errorInfo(error) };
      database.close();
      return { acquired: false, evidence };
    }

    try {
      database.exec('BEGIN EXCLUSIVE');
      evidence.beginExclusive = { ok: true };
      return { acquired: true, database, evidence };
    } catch (error) {
      evidence.beginExclusive = { ok: false, error: errorInfo(error) };
      try { database.prepare('PRAGMA journal_mode=WAL').get(); } catch {}
      database.close();
      return { acquired: false, evidence };
    }
  } catch (error) {
    try { database?.close(); } catch {}
    evidence.openError = errorInfo(error);
    return { acquired: false, evidence };
  }
}

function initDatabase(databasePath) {
  const database = new DatabaseSync(databasePath);
  try {
    database.exec(
      'PRAGMA journal_mode=WAL; ' +
      'CREATE TABLE meta(id INTEGER PRIMARY KEY CHECK(id=1), compatibility_epoch INTEGER NOT NULL); ' +
      'INSERT INTO meta(id, compatibility_epoch) VALUES (1, 1); ' +
      'CREATE TABLE ticks(id INTEGER PRIMARY KEY, origin TEXT NOT NULL)'
    );
  } finally {
    database.close();
  }
}

async function run() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), '1ku-mixed-version-cutover-'));
  const databasePath = path.join(root, 'knowledge.sqlite');
  const checks = [];
  const details = {};

  function record(name, pass, detail) {
    checks.push({ name, pass, detail });
  }

  initDatabase(databasePath);

  const legacy = spawnLegacyHolder(databasePath);
  try {
    await waitFor(legacy, (message) => message?.event === 'ready');
    await new Promise((resolve) => setTimeout(resolve, 150));

    legacy.send('snapshot');
    const before = await waitFor(legacy, (message) => message?.event === 'snapshot');
    details.legacyBefore = before;

    const blockedWindow = openCutoverWindow(databasePath);
    details.whileLegacyOpen = blockedWindow.evidence;
    if (blockedWindow.acquired) {
      try { blockedWindow.database.exec('ROLLBACK'); } catch {}
      try { blockedWindow.database.prepare('PRAGMA journal_mode=WAL').get(); } catch {}
      try { blockedWindow.database.close(); } catch {}
    }

    await new Promise((resolve) => setTimeout(resolve, 150));
    legacy.send('snapshot');
    const afterProbe = await waitFor(legacy, (message) => message?.event === 'snapshot');
    details.legacyAfterBlockedProbe = afterProbe;

    record(
      'old WAL Runtime prevents target cutover window',
      !blockedWindow.acquired,
      blockedWindow.evidence,
    );
    record(
      'rejected cutover probe does not kill the old Runtime',
      afterProbe.writes > before.writes,
      { before, afterProbe },
    );

    legacy.send('stop');
    const stopped = await waitFor(legacy, (message) => message?.event === 'stopped');
    details.legacyStopped = stopped;

    const window = openCutoverWindow(databasePath);
    details.afterLegacyRuntimeStop = window.evidence;
    record(
      'target acquires cutover window after only the 1KU Runtime releases Main',
      window.acquired,
      window.evidence,
    );

    if (!window.acquired) throw new Error('cutover window not acquired after legacy Runtime stopped');

    window.database.prepare('UPDATE meta SET compatibility_epoch=2 WHERE id=1').run();
    window.database.prepare('INSERT INTO ticks(origin) VALUES (?)').run('target-fence');

    const lateDuringFence = await runLateLegacy(databasePath);
    details.lateLegacyDuringFence = lateDuringFence;
    record(
      'late old Runtime cannot enter while compatibility fence transaction is held',
      lateDuringFence.read?.ok === false && lateDuringFence.write?.ok === false,
      lateDuringFence,
    );

    window.database.exec('COMMIT');

    const walRow = window.database.prepare('PRAGMA journal_mode=WAL').get();
    const lockRow = window.database.prepare('PRAGMA locking_mode').get();
    details.targetAfterFenceCommit = { walRow, lockRow };

    const lateBeforeTargetClose = await runLateLegacy(databasePath);
    details.lateLegacyBeforeTargetClose = lateBeforeTargetClose;
    record(
      'target connection remains exclusive through WAL restoration',
      lateBeforeTargetClose.read?.ok === false && lateBeforeTargetClose.write?.ok === false,
      lateBeforeTargetClose,
    );

    window.database.close();

    const verify = new DatabaseSync(databasePath);
    let final;
    try {
      const epoch = verify.prepare('SELECT compatibility_epoch FROM meta WHERE id=1').get();
      const journal = verify.prepare('PRAGMA journal_mode').get();
      const integrity = verify.prepare('PRAGMA integrity_check(1)').get();
      final = { epoch, journal, integrity };
    } finally {
      verify.close();
    }
    details.final = final;

    record(
      'compatibility fence is durable before normal target runtime continues',
      Number(final.epoch?.compatibility_epoch) === 2,
      final,
    );
    record(
      'Main returns to WAL after one-time cutover window',
      pragmaValue(final.journal, 'journal_mode') === 'wal',
      final,
    );
    record(
      'Main remains SQLite-integrity clean',
      String(final.integrity?.integrity_check ?? '').toLowerCase() === 'ok',
      final,
    );

    const pass = checks.every((check) => check.pass);
    const evidence = {
      generatedAt: new Date().toISOString(),
      platform: process.platform,
      arch: process.arch,
      node: process.version,
      sqlite: process.versions.sqlite,
      candidate:
        'Main SQLite bootstrap cutover: locking_mode=EXCLUSIVE -> journal_mode=DELETE -> BEGIN EXCLUSIVE -> compatibility fence -> COMMIT -> restore WAL',
      productBoundary: 'Obsidian stays open; only the old 1KU Runtime/Main connection must release',
      scope: 'isolated platform qualification only; no 1KU source, Vault, Library, or Obsidian Host',
      checks,
      details,
      conclusion: pass ? 'MIXED_VERSION_CUTOVER_PRIMITIVE_PASS' : 'MIXED_VERSION_CUTOVER_PRIMITIVE_FAIL',
    };

    const outDir = path.join(path.dirname(SELF), 'mixed-version-cutover-artifacts');
    fs.mkdirSync(outDir, { recursive: true });
    const out = path.join(
      outDir,
      'mixed-version-cutover-' + process.platform + '-' + process.arch + '-' + crypto.randomUUID() + '.json',
    );
    fs.writeFileSync(out, JSON.stringify(evidence, null, 2));
    console.log(JSON.stringify({
      out,
      conclusion: evidence.conclusion,
      passed: checks.filter((check) => check.pass).length,
      total: checks.length,
    }, null, 2));

    if (!pass) process.exitCode = 1;
  } finally {
    if (legacy.exitCode === null) {
      try { legacy.send('stop'); } catch {}
      await new Promise((resolve) => setTimeout(resolve, 100));
      if (legacy.exitCode === null) legacy.kill('SIGKILL');
    }
    fs.rmSync(root, { recursive: true, force: true });
  }
}

if (process.argv[2] === '--legacy-holder') {
  await legacyHolderMain(process.argv[3]);
} else if (process.argv[2] === '--late-legacy') {
  lateLegacyMain(process.argv[3]);
} else {
  await run();
}
