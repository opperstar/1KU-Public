import fs from 'node:fs';
import fsp from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';

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

async function exists(file) {
  try { await fsp.stat(file); return true; }
  catch (error) { if (error?.code === 'ENOENT') return false; throw error; }
}

async function readHeader(file) {
  const fd = await fsp.open(file, 'r');
  try {
    const buffer = Buffer.alloc(20);
    const { bytesRead } = await fd.read(buffer, 0, 20, 0);
    return buffer.subarray(0, bytesRead);
  } finally { await fd.close(); }
}

async function revertWalHeader(file) {
  const fd = await fsp.open(file, 'r+');
  try { await fd.write(Buffer.from([1, 1]), 0, 2, 18); }
  finally { await fd.close(); }
}

function integrityCheck(file) {
  const db = new DatabaseSync(file, { readOnly: true });
  try { return String(pragma(db, 'PRAGMA integrity_check(1)')) === 'ok'; }
  finally { db.close(); }
}

async function downgradeFromWalZoteroSemantic(file, options = {}) {
  const trace = [];
  try { file = await fsp.realpath(file); } catch {}

  let header;
  try { header = await readHeader(file); }
  catch (error) { if (error?.code === 'ENOENT') return { converted: false, trace }; throw error; }

  const isWalHeader = header.length >= 20 && header[18] === 2;
  let walSize = null;
  try { walSize = (await fsp.stat(file + '-wal')).size; }
  catch (error) { if (error?.code !== 'ENOENT') throw error; }

  trace.push({ isWalHeader, walSize });
  if (!isWalHeader && walSize === null) return { converted: false, trace };

  if (walSize > 0) {
    const tempFile = path.join(os.tmpdir(), `1ku-zotero-convert-${process.pid}-${Date.now()}-${Math.random().toString(16).slice(2)}.sqlite`);
    const swapFile = file + '.convert-tmp';
    try {
      trace.push('copy-main-to-temp');
      await fsp.copyFile(file, tempFile);
      trace.push('copy-wal-to-temp');
      await fsp.copyFile(file + '-wal', tempFile + '-wal');

      let valid = false;
      try {
        trace.push('temp-open');
        const db = new DatabaseSync(tempFile);
        try {
          trace.push('temp-journal_mode=DELETE');
          const mode = String(pragma(db, 'PRAGMA journal_mode=DELETE')).toLowerCase();
          assert(mode === 'delete', 'temp conversion did not reach rollback journal', { mode });
        } finally { db.close(); }
        trace.push('temp-integrity');
        valid = integrityCheck(tempFile);
        if (options.forceConvertedInvalid) valid = false;
      } catch (error) {
        if (!options.treatFirstConversionErrorAsCorruption) throw error;
      }

      if (!valid) {
        trace.push('fallback-main-without-wal');
        await fsp.rm(tempFile, { force: true });
        await fsp.rm(tempFile + '-wal', { force: true });
        await fsp.copyFile(file, tempFile);
        await revertWalHeader(tempFile);
        valid = integrityCheck(tempFile);
        assert(valid, 'main database is not valid without WAL');
      }

      trace.push('copy-validated-next-to-original');
      await fsp.copyFile(tempFile, swapFile);
      if (options.failBeforeSwap) throw new Error('INJECTED_BEFORE_SWAP');
      trace.push('atomic-swap');
      await fsp.rename(swapFile, file);
    } finally {
      await fsp.rm(tempFile, { force: true });
      await fsp.rm(tempFile + '-wal', { force: true });
      await fsp.rm(swapFile, { force: true });
    }
  } else if (isWalHeader) {
    trace.push('revert-header');
    await revertWalHeader(file);
  }

  trace.push('remove-original-wal-shm');
  await fsp.rm(file + '-wal', { force: true });
  await fsp.rm(file + '-shm', { force: true });
  return { converted: true, trace };
}

async function createWalFixture(root, name, extraRows = 20) {
  const source = path.join(root, `${name}-source.sqlite`);
  const target = path.join(root, `${name}.sqlite`);
  const db = new DatabaseSync(source);
  try {
    pragma(db, 'PRAGMA journal_mode=WAL');
    db.exec('PRAGMA wal_autocheckpoint=0');
    db.exec('CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT NOT NULL)');
    db.exec("INSERT INTO t(value) VALUES ('base')");
    pragma(db, 'PRAGMA wal_checkpoint(TRUNCATE)');
    for (let i = 0; i < extraRows; i++) db.prepare('INSERT INTO t(value) VALUES (?)').run(`wal-${i}`);
    const walBytes = (await fsp.stat(source + '-wal')).size;
    assert(walBytes > 0, 'fixture WAL is empty', { walBytes });
    await fsp.copyFile(source, target);
    await fsp.copyFile(source + '-wal', target + '-wal');
    return { target, walBytes };
  } finally { db.close(); }
}

function rowCount(file) {
  const db = new DatabaseSync(file, { readOnly: true });
  try { return Number(scalar(db.prepare('SELECT COUNT(*) AS n FROM t').get())); }
  finally { db.close(); }
}

function backupModeFromZoteroContract({ online, platform, isAPFS, fsType }) {
  let result = online;
  if (result && isAPFS) result = false;
  if (result && platform === 'linux' && ['cifs', 'smb', 'smb2', 'nfs'].includes(fsType)) result = false;
  return result ? 'online' : 'offline';
}

function runBackupDecisionMatrix() {
  const cases = [
    { name: 'APFS forces offline', input: { online: true, platform: 'darwin', isAPFS: true, fsType: 'apfs' }, expected: 'offline' },
    ...['cifs','smb','smb2','nfs'].map((fsType) => ({ name: `linux ${fsType} forces offline`, input: { online: true, platform: 'linux', isAPFS: false, fsType }, expected: 'offline' })),
    { name: 'linux ext4 remains online', input: { online: true, platform: 'linux', isAPFS: false, fsType: 'ext4' }, expected: 'online' },
    { name: 'mac smbfs is not the Linux backup branch', input: { online: true, platform: 'darwin', isAPFS: false, fsType: 'smbfs' }, expected: 'online' },
    { name: 'windows stays online', input: { online: true, platform: 'win32', isAPFS: false, fsType: 'smb' }, expected: 'online' },
    { name: 'already-offline stays offline', input: { online: false, platform: 'linux', isAPFS: false, fsType: 'ext4' }, expected: 'offline' },
  ];
  const results = cases.map((x) => ({ ...x, actual: backupModeFromZoteroContract(x.input) }));
  assert(results.every((x) => x.actual === x.expected), 'Zotero backup decision matrix drifted', results.filter((x) => x.actual !== x.expected));
  return results;
}

const root = await fsp.mkdtemp(path.join(os.tmpdir(), '1ku-zotero-lifecycle-'));
const artifactDir = path.resolve('artifacts');
await fsp.mkdir(artifactDir, { recursive: true });
const evidence = {
  schema: 1,
  purpose: 'Zotero source-parity qualification for WAL downgrade lifecycle and backup branch selection',
  reference: {
    source: 'zotero-main/chrome/content/zotero/xpcom/db.js',
    symbols: ['_downgradeDatabaseFromWAL', 'backupDatabase'],
  },
  runner: { platform: process.platform, arch: process.arch, node: process.version },
  startedAt: new Date().toISOString(),
};

try {
  evidence.cases = {};

  {
    const file = path.join(root, 'noop.sqlite');
    const db = new DatabaseSync(file); db.exec('CREATE TABLE t(x)'); db.close();
    const before = await fsp.readFile(file);
    const result = await downgradeFromWalZoteroSemantic(file);
    const after = await fsp.readFile(file);
    assert(result.converted === false && before.equals(after), 'no-op branch changed database');
    evidence.cases.noop = result;
  }

  {
    const file = path.join(root, 'empty-wal.sqlite');
    const db = new DatabaseSync(file); pragma(db, 'PRAGMA journal_mode=WAL'); db.exec('CREATE TABLE t(x)'); db.close();
    await fsp.writeFile(file + '-wal', Buffer.alloc(0));
    const result = await downgradeFromWalZoteroSemantic(file);
    const header = await readHeader(file);
    assert(header[18] === 1 && header[19] === 1, 'empty-WAL branch did not revert header');
    assert(!(await exists(file + '-wal')) && !(await exists(file + '-shm')), 'empty-WAL branch left journal residue');
    evidence.cases.emptyWal = result;
  }

  {
    const { target, walBytes } = await createWalFixture(root, 'replay', 20);
    const result = await downgradeFromWalZoteroSemantic(target);
    const header = await readHeader(target);
    assert(rowCount(target) === 21, 'non-empty WAL replay lost committed WAL rows');
    assert(header[18] === 1 && header[19] === 1, 'replayed DB did not end in rollback format');
    assert(integrityCheck(target), 'replayed DB failed integrity');
    assert(!(await exists(target + '-wal')) && !(await exists(target + '-shm')), 'replay branch left WAL/SHM residue');
    evidence.cases.replay = { walBytes, ...result };
  }

  {
    const { target } = await createWalFixture(root, 'fallback', 10);
    const result = await downgradeFromWalZoteroSemantic(target, { forceConvertedInvalid: true });
    assert(rowCount(target) === 1, 'fallback branch did not preserve main-file-only state');
    assert(integrityCheck(target), 'fallback main-only DB failed integrity');
    assert(result.trace.includes('fallback-main-without-wal'), 'fallback branch was not exercised');
    evidence.cases.fallback = result;
  }

  {
    const { target } = await createWalFixture(root, 'atomicity', 5);
    const mainBefore = await fsp.readFile(target);
    const walBefore = await fsp.readFile(target + '-wal');
    let injected = false;
    try { await downgradeFromWalZoteroSemantic(target, { failBeforeSwap: true }); }
    catch (error) { injected = error.message === 'INJECTED_BEFORE_SWAP'; if (!injected) throw error; }
    assert(injected, 'failure injection did not fire');
    assert(mainBefore.equals(await fsp.readFile(target)), 'original Main changed before validated swap');
    assert(walBefore.equals(await fsp.readFile(target + '-wal')), 'original WAL changed before validated swap');
    assert(!(await exists(target + '.convert-tmp')), 'swap temp residue remained after failure');
    evidence.cases.failureAtomicity = { pass: true };
  }

  evidence.backupDecisionMatrix = runBackupDecisionMatrix();
  evidence.result = 'PASS';
} catch (error) {
  evidence.result = 'FAIL';
  evidence.error = { message: error.message, details: error.details ?? null, stack: error.stack };
  process.exitCode = 1;
} finally {
  await fsp.rm(root, { recursive: true, force: true });
}

evidence.finishedAt = new Date().toISOString();
const out = path.join(artifactDir, `zotero-db-lifecycle-source-parity-${process.platform}-${process.arch}.json`);
await fsp.writeFile(out, JSON.stringify(evidence, null, 2));
console.log(JSON.stringify({ result: evidence.result, output: out, error: evidence.error ?? null }, null, 2));
