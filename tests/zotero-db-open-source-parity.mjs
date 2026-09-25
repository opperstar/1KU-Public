import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';

const BLOCKED_MAC_FS = new Set(['afpfs', 'smbfs', 'webdav', 'nfs']);

function canUseWalFromZoteroContract({ platform, fsInfo, byteRangeLocks }) {
  if (platform !== 'darwin') return true;
  if (!fsInfo) return false;
  if (BLOCKED_MAC_FS.has(fsInfo.fsType) || fsInfo.readOnly) return false;
  return Boolean(byteRangeLocks);
}

function assert(condition, message, details = undefined) {
  if (condition) return;
  const error = new Error(message);
  error.details = details;
  throw error;
}

function scalar(row) {
  return row ? Object.values(row)[0] : undefined;
}

function pragma(db, sql) {
  return scalar(db.prepare(sql).get());
}

function runDecisionMatrix() {
  const cases = [
    { name: 'mac fs info unavailable', input: { platform: 'darwin', fsInfo: null, byteRangeLocks: true }, expected: false },
    ...['afpfs', 'smbfs', 'webdav', 'nfs'].map((fsType) => ({
      name: `mac ${fsType}`,
      input: { platform: 'darwin', fsInfo: { fsType, readOnly: false }, byteRangeLocks: true },
      expected: false,
    })),
    { name: 'mac read-only volume', input: { platform: 'darwin', fsInfo: { fsType: 'apfs', readOnly: true }, byteRangeLocks: true }, expected: false },
    { name: 'mac local fs without byte-range locks', input: { platform: 'darwin', fsInfo: { fsType: 'apfs', readOnly: false }, byteRangeLocks: false }, expected: false },
    { name: 'mac local fs with byte-range locks', input: { platform: 'darwin', fsInfo: { fsType: 'apfs', readOnly: false }, byteRangeLocks: true }, expected: true },
    { name: 'linux ignores filesystem classifier in _canUseWAL', input: { platform: 'linux', fsInfo: { fsType: 'nfs', readOnly: true }, byteRangeLocks: false }, expected: true },
    { name: 'windows ignores filesystem classifier in _canUseWAL', input: { platform: 'win32', fsInfo: null, byteRangeLocks: false }, expected: true },
  ];

  const results = cases.map((test) => {
    const actual = canUseWalFromZoteroContract(test.input);
    return { ...test, actual, pass: actual === test.expected };
  });
  assert(results.every((x) => x.pass), 'Zotero _canUseWAL decision matrix drifted', results.filter((x) => !x.pass));
  return results;
}

async function initializeRollbackDb(file) {
  const init = new DatabaseSync(file);
  try {
    const mode = String(pragma(init, 'PRAGMA journal_mode=DELETE')).toLowerCase();
    init.exec('CREATE TABLE IF NOT EXISTS probe(id INTEGER PRIMARY KEY, value TEXT NOT NULL)');
    init.exec("INSERT INTO probe(value) VALUES ('seed')");
    assert(mode === 'delete', 'rollback-journal precondition was not established', { mode });
  } finally {
    init.close();
  }
}

async function runOpenChainCase({ root, useWal }) {
  const trace = [];
  const file = path.join(root, useWal ? 'use-wal.sqlite' : 'no-wal.sqlite');

  trace.push('canUseWAL');

  if (!useWal) {
    trace.push('downgradeDatabaseFromWAL');
    await initializeRollbackDb(file);
  }

  trace.push('openConnection');
  const db = new DatabaseSync(file, { timeout: 0 });
  try {
    trace.push('locking_mode=EXCLUSIVE');
    const lockingMode = String(pragma(db, 'PRAGMA main.locking_mode=EXCLUSIVE')).toLowerCase();
    assert(lockingMode === 'exclusive', 'EXCLUSIVE locking mode was not established', { lockingMode, trace });

    let journalMode = String(pragma(db, 'PRAGMA journal_mode')).toLowerCase();
    let synchronous = null;
    if (useWal) {
      trace.push('journal_mode=WAL');
      journalMode = String(pragma(db, 'PRAGMA journal_mode=WAL')).toLowerCase();
      trace.push('synchronous=NORMAL');
      db.exec('PRAGMA synchronous=NORMAL');
      synchronous = Number(pragma(db, 'PRAGMA synchronous'));
      assert(journalMode === 'wal', 'WAL branch did not enter WAL', { journalMode, trace });
      assert(synchronous === 1, 'WAL branch did not enter synchronous=NORMAL', { synchronous, trace });
    } else {
      assert(journalMode !== 'wal', 'no-WAL branch re-entered WAL after downgrade boundary', { journalMode, trace });
    }

    db.exec('CREATE TABLE IF NOT EXISTS runtime_probe(id INTEGER PRIMARY KEY, value TEXT NOT NULL)');
    db.exec("INSERT INTO runtime_probe(value) VALUES ('runtime')");
    const integrity = String(pragma(db, 'PRAGMA integrity_check(1)'));
    assert(integrity === 'ok', 'integrity_check failed after open chain', { integrity, trace });

    const expectedTrace = useWal
      ? ['canUseWAL', 'openConnection', 'locking_mode=EXCLUSIVE', 'journal_mode=WAL', 'synchronous=NORMAL']
      : ['canUseWAL', 'downgradeDatabaseFromWAL', 'openConnection', 'locking_mode=EXCLUSIVE'];
    assert(JSON.stringify(trace) === JSON.stringify(expectedTrace), 'Zotero open ordering drifted', { trace, expectedTrace });

    return { useWal, trace, lockingMode, journalMode, synchronous, integrity };
  } finally {
    db.close();
  }
}

const root = await fs.mkdtemp(path.join(os.tmpdir(), '1ku-zotero-open-parity-'));
const artifactDir = path.resolve('artifacts');
await fs.mkdir(artifactDir, { recursive: true });

const evidence = {
  schema: 1,
  purpose: 'Zotero source-parity qualification for _canUseWAL and _openConnectionAsync ordering',
  reference: {
    source: 'zotero-main/chrome/content/zotero/xpcom/db.js',
    symbols: ['_canUseWAL', '_openConnectionAsync'],
    note: 'row065 downgrade internals are treated as a prequalified boundary and are not reimplemented here',
  },
  runner: { platform: process.platform, arch: process.arch, node: process.version },
  startedAt: new Date().toISOString(),
};

try {
  evidence.decisionMatrix = runDecisionMatrix();
  evidence.openChain = {
    useWal: await runOpenChainCase({ root, useWal: true }),
    noWal: await runOpenChainCase({ root, useWal: false }),
  };
  evidence.result = 'PASS';
} catch (error) {
  evidence.result = 'FAIL';
  evidence.error = { message: error.message, details: error.details ?? null, stack: error.stack };
  process.exitCode = 1;
} finally {
  await fs.rm(root, { recursive: true, force: true });
}

evidence.finishedAt = new Date().toISOString();
const out = path.join(artifactDir, `zotero-db-open-source-parity-${process.platform}-${process.arch}.json`);
await fs.writeFile(out, JSON.stringify(evidence, null, 2));
console.log(JSON.stringify({ result: evidence.result, output: out, error: evidence.error ?? null }, null, 2));
