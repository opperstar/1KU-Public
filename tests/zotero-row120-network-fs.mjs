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
    deniedWhileHeld:
      (result.status === 10 && payload?.result === 'BLOCKED_BUSY')
      || (result.status === null && result.signal === 'SIGTERM' && result.error?.code === 'ETIMEDOUT'),
  };
}

// Exact source predicate carried from Zotero.DBConnection.prototype._canUseWAL().
function zoteroCanUseWAL({ platform, info, byteRangeLocks }) {
  if (platform !== 'darwin') return true;
  if (!info) return false;
  if (['afpfs', 'smbfs', 'webdav', 'nfs'].includes(info.fsType) || info.readOnly) return false;
  return !!byteRangeLocks;
}

// Exact Linux online-backup branch carried from Zotero DB backup semantics.
function zoteroBackupOnlineAfterFilesystemPolicy({ platform, requestedOnline, fsType }) {
  if (requestedOnline && platform === 'linux' && ['cifs', 'smb', 'smb2', 'nfs'].includes(fsType)) {
    return false;
  }
  return requestedOnline;
}

function decodeMountField(s) {
  return s.replace(/\\040/g, ' ').replace(/\\011/g, '\t').replace(/\\134/g, '\\');
}

function isWithin(p, mountPoint) {
  const rel = path.relative(mountPoint, p);
  return rel === '' || (!rel.startsWith('..' + path.sep) && rel !== '..' && !path.isAbsolute(rel));
}

function linuxMountInfo(target) {
  if (process.platform !== 'linux') return null;
  const text = fs.readFileSync('/proc/self/mountinfo', 'utf8');
  const rows = [];
  for (const line of text.split('\n')) {
    if (!line) continue;
    const parts = line.split(' ');
    const sep = parts.indexOf('-');
    if (sep < 0) continue;
    const mountPoint = decodeMountField(parts[4]);
    const mountOptions = parts[5].split(',');
    const fsType = parts[sep + 1];
    if (isWithin(target, mountPoint)) rows.push({ mountPoint, mountOptions, fsType });
  }
  rows.sort((a, b) => b.mountPoint.length - a.mountPoint.length);
  return rows[0] ?? null;
}

async function runWalSafeBranch(root, label) {
  const dbPath = path.join(root, `${label}-wal.sqlite`);
  let db = new DatabaseSync(dbPath, { timeout: 0 });
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
  assert(journalMode === 'wal', 'WAL could not be established after EXCLUSIVE', { journalMode });
  assert(walBytes > 0, 'WAL did not contain real data', { walBytes });
  assert(!shmExistsWhileOpen, 'EXCLUSIVE-before-WAL created -shm', { shmExistsWhileOpen });
  assert(contenderWhileHeld.deniedWhileHeld, 'competing writer was not denied while EXCLUSIVE owner was held', contenderWhileHeld);
  assert(walBytesAfterCheckpoint === 0, 'checkpoint(TRUNCATE) did not truncate WAL', { checkpoint, walBytesAfterCheckpoint });
  assert(contenderAfterRelease.status === 0 && contenderAfterRelease.payload?.result === 'WRITE_SUCCEEDED',
    'contender did not succeed after owner release', contenderAfterRelease);
  assert(rowsAfterRelease === 101 && integrity === 'ok', 'reopen/integrity failed after WAL-safe route', { rowsAfterRelease, integrity });

  return {
    lockingMode,
    journalMode,
    walBytes,
    shmExistsWhileOpen,
    contenderWhileHeld,
    checkpoint,
    walBytesAfterCheckpoint,
    contenderAfterRelease,
    rowsAfterRelease,
    integrity,
  };
}

async function runRow065RollbackBranch(root) {
  const source = path.join(root, 'row065-source.sqlite');
  const target = path.join(root, 'row065-target.sqlite');
  const temp = path.join(root, 'row065-convert.sqlite');

  const live = new DatabaseSync(source);
  live.exec('PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE probe(id INTEGER PRIMARY KEY, value TEXT NOT NULL)');
  live.prepare('PRAGMA wal_checkpoint(TRUNCATE)').get();
  const insert = live.prepare('INSERT INTO probe(value) VALUES (?)');
  for (let i = 0; i < 60; i++) insert.run('wal-row-' + i + '-' + 'x'.repeat(64));
  const sourceWalBytes = (await fsp.stat(source + '-wal')).size;
  assert(sourceWalBytes > 0, 'source WAL was empty before row065 conversion', { sourceWalBytes });

  await fsp.copyFile(source, target);
  await fsp.copyFile(source + '-wal', target + '-wal');
  live.close();

  await fsp.copyFile(target, temp);
  await fsp.copyFile(target + '-wal', temp + '-wal');

  let conversion = new DatabaseSync(temp);
  const beforeMode = String(pragmaScalar(conversion, 'PRAGMA journal_mode')).toLowerCase();
  const deleteMode = String(pragmaScalar(conversion, 'PRAGMA journal_mode=DELETE')).toLowerCase();
  conversion.close();
  conversion = null;

  const validated = new DatabaseSync(temp, { readOnly: true });
  const convertedRows = Number(pragmaScalar(validated, 'SELECT COUNT(*) FROM probe'));
  const convertedIntegrity = String(pragmaScalar(validated, 'PRAGMA integrity_check(1)'));
  validated.close();

  assert(beforeMode === 'wal' && deleteMode === 'delete', 'row065 temp replacement did not replay WAL and convert to DELETE', { beforeMode, deleteMode });
  assert(convertedRows === 60 && convertedIntegrity === 'ok', 'row065 converted replacement failed validation', { convertedRows, convertedIntegrity });

  await fsp.rm(target, { force: true });
  await fsp.rm(target + '-wal', { force: true });
  await fsp.rm(target + '-shm', { force: true });
  await fsp.rename(temp, target);
  await fsp.rm(temp + '-wal', { force: true });
  await fsp.rm(temp + '-shm', { force: true });

  let db = new DatabaseSync(target, { timeout: 0 });
  db.exec('PRAGMA busy_timeout=0');
  const lockingMode = String(pragmaScalar(db, 'PRAGMA main.locking_mode=EXCLUSIVE')).toLowerCase();
  const finalJournal = String(pragmaScalar(db, 'PRAGMA journal_mode')).toLowerCase();
  const contenderWhileHeld = runContender(target);
  const rows = Number(pragmaScalar(db, 'SELECT COUNT(*) FROM probe'));
  const integrity = String(pragmaScalar(db, 'PRAGMA integrity_check(1)'));
  db.close();
  db = null;

  assert(lockingMode === 'exclusive', 'row065 branch did not reopen with EXCLUSIVE', { lockingMode });
  assert(finalJournal !== 'wal', 'row065 branch reopened in WAL mode', { finalJournal });
  assert(contenderWhileHeld.deniedWhileHeld, 'row065 branch allowed a second writer while EXCLUSIVE held', contenderWhileHeld);
  assert(rows === 60 && integrity === 'ok', 'row065 branch lost WAL data or failed integrity', { rows, integrity });

  return {
    sourceWalBytes,
    beforeMode,
    deleteMode,
    convertedRows,
    convertedIntegrity,
    lockingMode,
    finalJournal,
    contenderWhileHeld,
    rows,
    integrity,
  };
}

async function qualificationMain() {
  const runRoot = await fsp.mkdtemp(path.join(os.tmpdir(), '1ku-row120-frozen-route-'));
  const artifactDir = path.join(path.dirname(SELF), 'zotero-row120-artifacts');
  await fsp.mkdir(artifactDir, { recursive: true });

  const evidence = {
    schema: 2,
    purpose: 'Re-audit the frozen R19BV21A row120 route without introducing a runtime fact provider or second filesystem owner',
    platform: process.platform,
    arch: process.arch,
    node: process.version,
    realNetworkFilesystemObserved: false,
    scenarios: {},
    startedAt: new Date().toISOString(),
  };

  async function scenario(name, fn) {
    try {
      evidence.scenarios[name] = { result: 'PASS', ...(await fn()) };
    }
    catch (error) {
      evidence.scenarios[name] = {
        result: 'FAIL',
        error: { ...errorInfo(error), details: error?.details ?? null, stack: error?.stack ?? null },
      };
    }
  }

  try {
    await scenario('exact_zotero_decision_table', async () => {
      const cases = [
        ['mac-missing-info', { platform: 'darwin', info: null, byteRangeLocks: true }, false],
        ['mac-afp', { platform: 'darwin', info: { fsType: 'afpfs', readOnly: false }, byteRangeLocks: true }, false],
        ['mac-smb', { platform: 'darwin', info: { fsType: 'smbfs', readOnly: false }, byteRangeLocks: true }, false],
        ['mac-webdav', { platform: 'darwin', info: { fsType: 'webdav', readOnly: false }, byteRangeLocks: true }, false],
        ['mac-nfs', { platform: 'darwin', info: { fsType: 'nfs', readOnly: false }, byteRangeLocks: true }, false],
        ['mac-readonly', { platform: 'darwin', info: { fsType: 'apfs', readOnly: true }, byteRangeLocks: true }, false],
        ['mac-no-positive-byte-lock-fact', { platform: 'darwin', info: { fsType: 'apfs', readOnly: false }, byteRangeLocks: false }, false],
        ['mac-apfs-positive-byte-lock', { platform: 'darwin', info: { fsType: 'apfs', readOnly: false }, byteRangeLocks: true }, true],
        ['linux-source-branch', { platform: 'linux', info: null, byteRangeLocks: false }, true],
        ['windows-source-branch', { platform: 'win32', info: null, byteRangeLocks: false }, true],
      ];

      for (const [name, input, expected] of cases) {
        const actual = zoteroCanUseWAL(input);
        assert(actual === expected, 'Zotero _canUseWAL decision-table mismatch', { name, input, expected, actual });
      }
      return { cases: cases.length };
    });

    await scenario('wal_safe_node_sqlite_landing', async () => {
      return await runWalSafeBranch(runRoot, process.platform);
    });

    if (process.platform === 'darwin') {
      await scenario('macos_no_positive_advlock_fact_uses_frozen_false_branch', async () => {
        const useWAL = zoteroCanUseWAL({
          platform: 'darwin',
          info: { fsType: 'apfs', readOnly: false },
          byteRangeLocks: false,
        });
        assert(useWAL === false, 'macOS missing positive byte-range-lock fact did not select useWAL=false');
        const row065 = await runRow065RollbackBranch(runRoot);
        return {
          useWAL,
          route: 'row065-preopen-validated-rollback-conversion-then-EXCLUSIVE-reopen',
          row065,
        };
      });
    }

    await scenario('linux_network_backup_branch', async () => {
      const network = ['cifs', 'smb', 'smb2', 'nfs'];
      for (const fsType of network) {
        assert(
          zoteroBackupOnlineAfterFilesystemPolicy({ platform: 'linux', requestedOnline: true, fsType }) === false,
          'Linux network filesystem did not force offline backup',
          { fsType }
        );
      }
      assert(
        zoteroBackupOnlineAfterFilesystemPolicy({ platform: 'linux', requestedOnline: true, fsType: 'ext4' }) === true,
        'Linux local filesystem unexpectedly disabled online backup'
      );
      assert(
        zoteroBackupOnlineAfterFilesystemPolicy({ platform: 'darwin', requestedOnline: true, fsType: 'nfs' }) === true,
        'Linux backup rule leaked into macOS branch'
      );

      const actualMount = process.platform === 'linux' ? linuxMountInfo(runRoot) : null;
      if (process.platform === 'linux') {
        assert(actualMount?.fsType, 'Linux /proc/self/mountinfo did not provide filesystem type for current path', { actualMount });
      }

      return {
        network,
        actualMount,
        directLinuxFactSource: process.platform === 'linux' ? '/proc/self/mountinfo' : 'NOT_APPLICABLE',
      };
    });
  }
  finally {
    await fsp.rm(runRoot, { recursive: true, force: true });
  }

  const failures = Object.entries(evidence.scenarios).filter(([, value]) => value.result !== 'PASS');
  evidence.result = failures.length ? 'FAIL' : 'PASS';
  evidence.failedScenarios = failures.map(([name]) => name);
  evidence.disposition = {
    row120Semantic: 'CLOSED',
    nodeSqliteEquivalentRoute: 'QUALIFIED',
    runtimeFactProviderAsGlobalImplementationStartGate: 'NOT_PROVED_NECESSARY_BY_THIS_ROUTE',
    realNetworkFilesystemHost: 'HOST_PENDING_FINAL_ACCEPTANCE_ONLY',
  };
  evidence.finishedAt = new Date().toISOString();

  const artifact = path.join(artifactDir, `zotero-row120-frozen-route-${process.platform}-${process.arch}.json`);
  await fsp.writeFile(artifact, JSON.stringify(evidence, null, 2));

  console.log(JSON.stringify({
    result: evidence.result,
    platform: evidence.platform,
    failedScenarios: evidence.failedScenarios,
    disposition: evidence.disposition,
    artifact,
  }, null, 2));

  if (evidence.result !== 'PASS') process.exitCode = 1;
}

if (process.argv[2] === '--contender') {
  process.exitCode = contenderMain(process.argv[3]);
}
else {
  await qualificationMain();
}
