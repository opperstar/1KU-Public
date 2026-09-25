import fs from 'node:fs';
import fsp from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { DatabaseSync } from 'node:sqlite';

const SELF = fileURLToPath(import.meta.url);

function scalar(row) {
  if (!row) return undefined;
  return Object.values(row)[0];
}

function pragmaScalar(db, sql) {
  return scalar(db.prepare(sql).get());
}

function errorInfo(error) {
  return {
    code: error?.code ?? null,
    errcode: error?.errcode ?? null,
    errstr: error?.errstr ?? null,
    message: error?.message ?? String(error),
  };
}

function isBusy(error) {
  const info = errorInfo(error);
  return info.errcode === 5 || info.errcode === 6 || /busy|locked/i.test(info.message);
}

async function exists(filePath) {
  try {
    await fsp.stat(filePath);
    return true;
  }
  catch (error) {
    if (error?.code === 'ENOENT') return false;
    throw error;
  }
}

function assert(condition, message, details = null) {
  if (condition) return;
  const error = new Error(message);
  error.details = details;
  throw error;
}

function contenderMain(dbPath) {
  let db = null;
  try {
    db = new DatabaseSync(dbPath, { timeout: 0 });
    db.exec('PRAGMA busy_timeout=0');
    db.exec("INSERT INTO probe(value) VALUES ('contender')");
    console.log(JSON.stringify({ result: 'WRITE_SUCCEEDED' }));
    return 0;
  }
  catch (error) {
    console.log(JSON.stringify({
      result: isBusy(error) ? 'BLOCKED_BUSY' : 'ERROR',
      error: errorInfo(error),
    }));
    return isBusy(error) ? 10 : 20;
  }
  finally {
    try { db?.close(); } catch {}
  }
}

function runContender(dbPath) {
  const result = spawnSync(process.execPath, [SELF, '--contender', dbPath], {
    encoding: 'utf8',
    timeout: 5000,
    env: process.env,
  });

  let payload = null;
  try {
    payload = JSON.parse(String(result.stdout || '').trim());
  }
  catch {}

  return {
    status: result.status,
    signal: result.signal,
    error: result.error ? errorInfo(result.error) : null,
    stdout: String(result.stdout || '').trim(),
    stderr: String(result.stderr || '').trim(),
    payload,
  };
}

async function qualificationMain() {
  const networkRoot = process.env.ROW120_NETWORK_FS_ROOT;
  if (!networkRoot) throw new Error('ROW120_NETWORK_FS_ROOT is required');

  const artifactDir = path.join(path.dirname(SELF), 'zotero-row120-artifacts');
  await fsp.mkdir(artifactDir, { recursive: true });

  const runRoot = path.join(networkRoot, `1ku-row120-${process.pid}-${Date.now()}`);
  await fsp.mkdir(runRoot, { recursive: true });

  const evidence = {
    schema: 1,
    purpose: 'Qualify Zotero row120 network-filesystem open/WAL semantics on Node 24 node:sqlite',
    referenceSemantic: [
      'macOS Gecko openNotExclusive is HOW only',
      'SQLite connection-lifetime locking_mode=EXCLUSIVE remains required',
      'target translation establishes EXCLUSIVE before first WAL access',
      'no filesystem classifier or second DB owner is introduced',
    ],
    platform: process.platform,
    arch: process.arch,
    node: process.version,
    fsKind: process.env.ROW120_NETWORK_FS_KIND ?? 'unknown-network-fs-fixture',
    networkRoot,
    startedAt: new Date().toISOString(),
  };

  let db = null;
  try {
    assert(process.platform === 'darwin', 'row120 qualification must run on macOS');

    const dbPath = path.join(runRoot, 'knowledge.sqlite');
    db = new DatabaseSync(dbPath, { timeout: 0 });
    db.exec('PRAGMA busy_timeout=0');

    const lockingMode = String(pragmaScalar(db, 'PRAGMA main.locking_mode=EXCLUSIVE')).toLowerCase();
    const journalMode = String(pragmaScalar(db, 'PRAGMA journal_mode=WAL')).toLowerCase();
    db.exec('PRAGMA synchronous=NORMAL; PRAGMA wal_autocheckpoint=0');
    db.exec('CREATE TABLE probe(id INTEGER PRIMARY KEY, value TEXT NOT NULL)');
    const insert = db.prepare('INSERT INTO probe(value) VALUES (?)');
    for (let i = 0; i < 100; i++) insert.run('owner-' + i + '-' + 'x'.repeat(128));

    const walPath = dbPath + '-wal';
    const shmPath = dbPath + '-shm';
    const walBytes = (await fsp.stat(walPath)).size;
    const shmExistsWhileOpen = await exists(shmPath);
    const contenderWhileHeld = runContender(dbPath);
    const ownerRows = Number(pragmaScalar(db, 'SELECT COUNT(*) FROM probe'));
    const checkpoint = db.prepare('PRAGMA wal_checkpoint(TRUNCATE)').get();
    const walBytesAfterCheckpoint = (await fsp.stat(walPath)).size;

    db.close();
    db = null;

    const contenderAfterRelease = runContender(dbPath);
    const reopened = new DatabaseSync(dbPath, { readOnly: true, timeout: 0 });
    const rowsAfterRelease = Number(pragmaScalar(reopened, 'SELECT COUNT(*) FROM probe'));
    const integrity = String(pragmaScalar(reopened, 'PRAGMA integrity_check(1)'));
    reopened.close();

    assert(lockingMode === 'exclusive', 'locking_mode=EXCLUSIVE was not established before WAL', { lockingMode });
    assert(journalMode === 'wal', 'WAL could not be established after EXCLUSIVE on the network filesystem', { journalMode });
    assert(walBytes > 0, 'WAL was not active after owner writes', { walBytes });
    assert(!shmExistsWhileOpen, 'EXCLUSIVE-before-WAL still created a -shm file', { shmExistsWhileOpen });
    assert(contenderWhileHeld.status === 10 && contenderWhileHeld.payload?.result === 'BLOCKED_BUSY',
      'competing writer was not denied by SQLite while owner held EXCLUSIVE', contenderWhileHeld);
    assert(ownerRows === 100, 'owner row count changed before release', { ownerRows });
    assert(walBytesAfterCheckpoint === 0, 'WAL checkpoint did not truncate before close', { checkpoint, walBytesAfterCheckpoint });
    assert(contenderAfterRelease.status === 0 && contenderAfterRelease.payload?.result === 'WRITE_SUCCEEDED',
      'writer did not succeed after owner release', contenderAfterRelease);
    assert(rowsAfterRelease === 101 && integrity === 'ok',
      'reopen/integrity failed after owner release', { rowsAfterRelease, integrity });

    evidence.result = 'PASS';
    evidence.details = {
      lockingMode,
      journalMode,
      walBytes,
      shmExistsWhileOpen,
      contenderWhileHeld,
      ownerRows,
      checkpoint,
      walBytesAfterCheckpoint,
      contenderAfterRelease,
      rowsAfterRelease,
      integrity,
    };
  }
  catch (error) {
    evidence.result = 'FAIL';
    evidence.error = {
      ...errorInfo(error),
      details: error?.details ?? null,
      stack: error?.stack ?? null,
    };
    process.exitCode = 1;
  }
  finally {
    try { db?.close(); } catch {}
    try {
      await fsp.rm(runRoot, { recursive: true, force: true });
      evidence.cleanup = 'PASS';
    }
    catch (error) {
      evidence.cleanup = 'FAIL';
      evidence.cleanupError = errorInfo(error);
      process.exitCode = 1;
    }
  }

  evidence.finishedAt = new Date().toISOString();
  const artifact = path.join(artifactDir, `zotero-row120-${process.platform}-${process.arch}.json`);
  await fsp.writeFile(artifact, JSON.stringify(evidence, null, 2));
  console.log(JSON.stringify({ result: evidence.result, artifact, details: evidence.details, error: evidence.error }, null, 2));
}

if (process.argv[2] === '--contender') {
  process.exitCode = contenderMain(process.argv[3]);
}
else {
  await qualificationMain();
}
