import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fork } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { DatabaseSync } from 'node:sqlite';

const SELF = fileURLToPath(import.meta.url);
const RACE_ROUNDS = 40;

function gatePath(root, libraryId) {
  const key = crypto.createHash('sha256').update(libraryId).digest('hex').slice(0, 32);
  return path.join(root, 'library-gates', key + '.sqlite');
}

function acquireGate(root, libraryId) {
  const file = gatePath(root, libraryId);
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
  const db = new DatabaseSync(file);
  db.exec('PRAGMA busy_timeout=100');
  try {
    db.exec('BEGIN EXCLUSIVE');
    return { db, file };
  } catch (error) {
    db.close();
    throw error;
  }
}

function releaseGate(gate) {
  if (!gate) return;
  try { gate.db.exec('ROLLBACK'); } catch {}
  try { gate.db.close(); } catch {}
}

function errorInfo(error) {
  return {
    code: error?.code ?? null,
    errcode: error?.errcode ?? null,
    errstr: error?.errstr ?? null,
    message: error?.message ?? String(error)
  };
}

function isBusy(error) {
  const info = errorInfo(error);
  return info.errcode === 5 || info.errcode === 6 || /busy|locked/i.test(info.message);
}

async function childMain() {
  let gate = null;
  let viewCount = 0;
  let mainDb = null;

  process.send?.({ event: 'ready' });

  async function acquireAt(message) {
    const delay = Math.max(0, message.at - Date.now());
    if (delay) await new Promise((resolve) => setTimeout(resolve, delay));
    try {
      gate = acquireGate(message.root, message.libraryId);
      process.send?.({ event: 'response', requestId: message.requestId, ok: true, acquired: true, gatePath: gate.file });
    } catch (error) {
      process.send?.({
        event: 'response',
        requestId: message.requestId,
        ok: isBusy(error),
        acquired: false,
        busy: isBusy(error),
        error: errorInfo(error)
      });
    }
  }

  process.on('message', async (message) => {
    try {
      if (message?.cmd === 'acquireAt') {
        await acquireAt(message);
        return;
      }
      if (message?.cmd === 'release') {
        releaseGate(gate);
        gate = null;
        process.send?.({ event: 'response', requestId: message.requestId, ok: true });
        return;
      }
      if (message?.cmd === 'openView') {
        if (viewCount === 0) gate = acquireGate(message.root, message.libraryId);
        viewCount += 1;
        process.send?.({ event: 'response', requestId: message.requestId, ok: true, viewCount, gatePath: gate.file });
        return;
      }
      if (message?.cmd === 'closeView') {
        if (viewCount <= 0) throw new Error('view underflow');
        viewCount -= 1;
        if (viewCount === 0) {
          releaseGate(gate);
          gate = null;
        }
        process.send?.({ event: 'response', requestId: message.requestId, ok: true, viewCount });
        return;
      }
      if (message?.cmd === 'openMain') {
        if (mainDb) throw new Error('main already open');
        mainDb = new DatabaseSync(message.mainPath, { timeout: 0 });
        mainDb.exec('CREATE TABLE IF NOT EXISTS probe(value INTEGER)');
        process.send?.({ event: 'response', requestId: message.requestId, ok: true });
        return;
      }
      if (message?.cmd === 'closeMain') {
        if (mainDb) mainDb.close();
        mainDb = null;
        process.send?.({ event: 'response', requestId: message.requestId, ok: true });
        return;
      }
      if (message?.cmd === 'stop') {
        if (mainDb) mainDb.close();
        releaseGate(gate);
        process.send?.({ event: 'response', requestId: message.requestId, ok: true }, () => process.exit(0));
      }
    } catch (error) {
      process.send?.({ event: 'response', requestId: message?.requestId, ok: false, error: errorInfo(error) });
    }
  });
}

function spawnChild() {
  const cp = fork(SELF, ['--child'], { stdio: ['ignore', 'ignore', 'ignore', 'ipc'] });
  const events = [];
  const waiters = new Set();

  function pump() {
    for (const fn of [...waiters]) fn();
  }

  cp.on('message', (message) => {
    events.push(message);
    pump();
  });
  cp.on('error', (error) => {
    events.push({ event: 'child-error', error: errorInfo(error) });
    pump();
  });
  cp.on('exit', (code, signal) => {
    events.push({ event: 'exit', code, signal });
    pump();
  });

  function waitFor(predicate, timeoutMs = 10000) {
    const existing = events.find(predicate);
    if (existing) return Promise.resolve(existing);
    return new Promise((resolve, reject) => {
      const deadline = Date.now() + timeoutMs;
      const check = () => {
        const found = events.find(predicate);
        if (found) {
          clearInterval(timer);
          waiters.delete(check);
          resolve(found);
        } else if (Date.now() >= deadline) {
          clearInterval(timer);
          waiters.delete(check);
          reject(new Error('child wait timeout: ' + JSON.stringify(events.slice(-8))));
        }
      };
      const timer = setInterval(check, 10);
      waiters.add(check);
      check();
    });
  }

  async function request(cmd, args = {}, timeoutMs = 10000) {
    const requestId = crypto.randomUUID();
    cp.send({ cmd, requestId, ...args });
    return waitFor((event) => event.event === 'response' && event.requestId === requestId, timeoutMs);
  }

  async function stop() {
    if (cp.exitCode !== null) return;
    try { await request('stop', {}, 1500); } catch {}
    if (cp.exitCode === null) cp.kill('SIGKILL');
  }

  return { cp, events, waitFor, request, stop };
}

async function timedRace(children, root, libraryIds) {
  const at = Date.now() + 200;
  return Promise.all(children.map((child, index) =>
    child.request('acquireAt', { root, libraryId: libraryIds[index], at })
  ));
}

async function releaseWinners(children, results) {
  await Promise.all(results.map((result, index) =>
    result.acquired ? children[index].request('release') : Promise.resolve()
  ));
}

async function run() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), '1ku-single-library-gate-'));
  if (process.platform !== 'win32') fs.chmodSync(root, 0o700);
  const children = Array.from({ length: 5 }, () => spawnChild());
  const checks = [];

  function record(name, pass, detail) {
    checks.push({ name, pass, detail });
  }

  try {
    await Promise.all(children.map((child) => child.waitFor((event) => event.event === 'ready')));

    let twoRacePass = true;
    const twoRaceSamples = [];
    for (let round = 0; round < RACE_ROUNDS; round++) {
      const id = 'library-two-' + round;
      const pair = children.slice(0, 2);
      const results = await timedRace(pair, root, [id, id]);
      const winners = results.filter((result) => result.acquired).length;
      const losersBusy = results.filter((result) => !result.acquired && result.busy).length;
      if (winners !== 1 || losersBusy !== 1) twoRacePass = false;
      if (round < 3) twoRaceSamples.push({ round, winners, losersBusy, results });
      await releaseWinners(pair, results);
    }
    record('same LibraryId / 2 Obsidian contenders / exactly one winner', twoRacePass, { rounds: RACE_ROUNDS, samples: twoRaceSamples });

    let fiveRacePass = true;
    const fiveRaceSamples = [];
    for (let round = 0; round < RACE_ROUNDS; round++) {
      const id = 'library-five-' + round;
      const results = await timedRace(children, root, children.map(() => id));
      const winners = results.filter((result) => result.acquired).length;
      const losersBusy = results.filter((result) => !result.acquired && result.busy).length;
      if (winners !== 1 || losersBusy !== 4) fiveRacePass = false;
      if (round < 3) fiveRaceSamples.push({ round, winners, losersBusy });
      await releaseWinners(children, results);
    }
    record('same LibraryId / 5 Obsidian contenders / exactly one winner', fiveRacePass, { rounds: RACE_ROUNDS, samples: fiveRaceSamples });

    {
      const ids = children.map((_, index) => 'different-library-' + index);
      const results = await timedRace(children, root, ids);
      record('different LibraryIds do not block each other', results.every((result) => result.acquired), results);
      await releaseWinners(children, results);
    }

    {
      const id = 'alive-holder';
      const holder = children[0];
      const contender = children[1];
      const acquired = await holder.request('acquireAt', { root, libraryId: id, at: Date.now() + 50 });
      await new Promise((resolve) => setTimeout(resolve, 2500));
      const blocked = await contender.request('acquireAt', { root, libraryId: id, at: Date.now() + 50 });
      record('live holder remains exclusive without heartbeat/stale timeout', acquired.acquired && !blocked.acquired && blocked.busy, { acquired, blocked });
      await holder.request('release');
    }

    {
      const id = 'graceful-release';
      const holder = children[0];
      const contender = children[1];
      const first = await holder.request('acquireAt', { root, libraryId: id, at: Date.now() + 50 });
      const blocked = await contender.request('acquireAt', { root, libraryId: id, at: Date.now() + 50 });
      await holder.request('release');
      const after = await contender.request('acquireAt', { root, libraryId: id, at: Date.now() + 50 });
      record('graceful close releases Library immediately', first.acquired && blocked.busy && after.acquired, { first, blocked, after });
      if (after.acquired) await contender.request('release');
    }

    {
      const crashHolder = spawnChild();
      await crashHolder.waitFor((event) => event.event === 'ready');
      const id = 'crash-release';
      const first = await crashHolder.request('acquireAt', { root, libraryId: id, at: Date.now() + 50 });
      const blockedBefore = await children[1].request('acquireAt', { root, libraryId: id, at: Date.now() + 50 });
      crashHolder.cp.kill('SIGKILL');
      await crashHolder.waitFor((event) => event.event === 'exit');
      const started = Date.now();
      const afterCrash = await children[1].request('acquireAt', { root, libraryId: id, at: Date.now() + 20 });
      const elapsedMs = Date.now() - started;
      record('process crash releases Library without stale-file wait', first.acquired && blockedBefore.busy && afterCrash.acquired, { first, blockedBefore, afterCrash, elapsedMs });
      if (afterCrash.acquired) await children[1].request('release');
    }

    {
      const id = 'multi-view';
      const runtime = children[0];
      const contender = children[1];
      const view1 = await runtime.request('openView', { root, libraryId: id });
      const view2 = await runtime.request('openView', { root, libraryId: id });
      const blocked1 = await contender.request('acquireAt', { root, libraryId: id, at: Date.now() + 50 });
      const close1 = await runtime.request('closeView');
      const blocked2 = await contender.request('acquireAt', { root, libraryId: id, at: Date.now() + 50 });
      const close2 = await runtime.request('closeView');
      const afterLast = await contender.request('acquireAt', { root, libraryId: id, at: Date.now() + 50 });
      record('same Obsidian multi-view shares one Library gate until last view closes',
        view1.ok && view1.viewCount === 1 &&
        view2.ok && view2.viewCount === 2 &&
        blocked1.busy &&
        close1.viewCount === 1 &&
        blocked2.busy &&
        close2.viewCount === 0 &&
        afterLast.acquired,
        { view1, view2, blocked1, close1, blocked2, close2, afterLast });
      if (afterLast.acquired) await contender.request('release');
    }

    {
      const id = 'maintenance-main-close';
      const runtime = children[0];
      const contender = children[1];
      const mainPath = path.join(root, 'fake-main.sqlite');
      const openView = await runtime.request('openView', { root, libraryId: id });
      const mainOpen1 = await runtime.request('openMain', { mainPath });
      const blockedBefore = await contender.request('acquireAt', { root, libraryId: id, at: Date.now() + 50 });
      const mainClose = await runtime.request('closeMain');
      const blockedDuring = await contender.request('acquireAt', { root, libraryId: id, at: Date.now() + 50 });
      const mainOpen2 = await runtime.request('openMain', { mainPath });
      const blockedAfter = await contender.request('acquireAt', { root, libraryId: id, at: Date.now() + 50 });
      const lastClose = await runtime.request('closeView');
      const afterRelease = await contender.request('acquireAt', { root, libraryId: id, at: Date.now() + 50 });
      record('Library gate survives Main DB close/reopen maintenance window',
        openView.ok && mainOpen1.ok && blockedBefore.busy && mainClose.ok && blockedDuring.busy &&
        mainOpen2.ok && blockedAfter.busy && lastClose.viewCount === 0 && afterRelease.acquired,
        { blockedBefore, blockedDuring, blockedAfter, afterRelease });
      await runtime.request('closeMain');
      if (afterRelease.acquired) await contender.request('release');
    }

    {
      const id = 'same-library-different-device';
      const deviceA = path.join(root, 'device-a');
      const deviceB = path.join(root, 'device-b');
      const results = await Promise.all([
        children[0].request('acquireAt', { root: deviceA, libraryId: id, at: Date.now() + 100 }),
        children[1].request('acquireAt', { root: deviceB, libraryId: id, at: Date.now() + 100 })
      ]);
      record('same LibraryId on different device-local roots does not cross-lock', results.every((result) => result.acquired), results);
      await releaseWinners(children.slice(0, 2), results);
    }

    {
      const id = 'copied-folder-same-library';
      const copyA = path.join(root, 'obsidian-1', 'plugins', '1ku');
      const copyB = path.join(root, 'obsidian-2', 'plugins', '1ku');
      fs.mkdirSync(copyA, { recursive: true });
      fs.mkdirSync(copyB, { recursive: true });
      const results = await timedRace(children.slice(0, 2), root, [id, id]);
      const pass = results.filter((r) => r.acquired).length === 1 && results.filter((r) => r.busy).length === 1;
      record('different copied 1KU folders with same LibraryId still share one device gate', pass, { copyA, copyB, results });
      await releaseWinners(children.slice(0, 2), results);
    }

    const pass = checks.every((check) => check.pass);
    const evidence = {
      generatedAt: new Date().toISOString(),
      platform: process.platform,
      arch: process.arch,
      node: process.version,
      candidate: 'node:sqlite dedicated OS-local gate DB + BEGIN EXCLUSIVE',
      dependency: 'Node built-in only; no npm locking package; no native binary packaging',
      productScope: {
        sameDeviceSameLibraryId: 'max one Obsidian Runtime',
        sameRuntimeMultiView: 'allowed; one shared gate until last view closes',
        differentLibraryIds: 'independent',
        differentDevicesSameLibraryId: 'independent local gates; Yjs remains cross-device convergence',
        copied1kuFolder: 'same LibraryId still maps to same device-local gate'
      },
      gateMechanics: {
        path: 'device-local shared gate root / sha256(LibraryId).sqlite',
        lock: 'BEGIN EXCLUSIVE held for Library-session lifetime',
        busyTimeoutMs: 100,
        heartbeat: false,
        pidRegistry: false,
        staleTimeout: false,
        deleteRecreateDuringSession: false
      },
      checks,
      conclusion: pass ? 'SINGLE_LIBRARY_SQLITE_GATE_PASS' : 'SINGLE_LIBRARY_SQLITE_GATE_FAIL'
    };

    const outDir = path.join(path.dirname(SELF), 'single-library-gate-artifacts');
    fs.mkdirSync(outDir, { recursive: true });
    const out = path.join(outDir, 'single-library-gate-' + process.platform + '-' + process.arch + '.json');
    fs.writeFileSync(out, JSON.stringify(evidence, null, 2));
    console.log(JSON.stringify({ out, conclusion: evidence.conclusion, passed: checks.filter((x) => x.pass).length, total: checks.length }, null, 2));
    if (!pass) process.exitCode = 1;
  } finally {
    await Promise.all(children.map((child) => child.stop()));
  }
}

if (process.argv[2] === '--child') {
  await childMain();
} else {
  await run();
}
