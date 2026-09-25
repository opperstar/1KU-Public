import fs from 'node:fs';
import fsp from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';

const outDir = path.join(process.cwd(), 'artifacts');
await fsp.mkdir(outDir, { recursive: true });

const root = await fsp.mkdtemp(path.join(os.tmpdir(), '1ku-row120-provider-'));
const probePath = path.join(root, 'knowledge.sqlite');
await fsp.writeFile(probePath, '');

const evidence = {
  schema: 1,
  purpose: 'ROW120_RUNTIME_FACT_PROVIDER public/native capability qualification',
  platform: process.platform,
  arch: process.arch,
  node: process.version,
  constraints: {
    noNativeAddon: true,
    noHelperExecutable: true,
    noPathHeuristic: true,
    noMountNameHeuristic: true,
    noInventedSqliteLockProbe: true,
    publicNodeApiOnly: true,
  },
};

try {
  const statfs = typeof fsp.statfs === 'function' ? await fsp.statfs(probePath) : null;
  const statfsOwnKeys = statfs ? Object.keys(statfs) : [];
  const fsExports = Object.keys(fs).sort();
  const fspExports = Object.keys(fsp).sort();

  evidence.observed = {
    statfsAvailable: typeof fsp.statfs === 'function',
    statfsOwnKeys,
    statfsType: statfs?.type ?? null,
    statfsHasNamedFilesystemType: statfsOwnKeys.some(k => /fstype|filesystem.*name|typename/i.test(k)),
    statfsHasReadOnlyFlag: statfsOwnKeys.some(k => /read.?only|readonly|flags/i.test(k)),
    publicLockLikeFsExports: fsExports.filter(k => /fcntl|flock|lock/i.test(k)),
    publicLockLikeFspExports: fspExports.filter(k => /fcntl|flock|lock/i.test(k)),
  };

  evidence.requiredFacts = {
    filesystemTypeOrCategory: {
      status: statfs && 'type' in statfs ? 'PARTIAL_NUMERIC_TYPE_ONLY' : 'UNAVAILABLE',
      note: 'Zotero row120 consumes source-equivalent filesystem classification; numeric statfs.type alone is not treated as the complete macOS named fsType contract.',
    },
    readOnly: {
      status: evidence.observed.statfsHasReadOnlyFlag ? 'AVAILABLE' : 'UNAVAILABLE_FROM_NODE_STATFS_RESULT',
      note: 'A write attempt is intentionally not promoted to mount read-only truth.',
    },
    supportsByteRangeLocks: {
      status: evidence.observed.publicLockLikeFsExports.length || evidence.observed.publicLockLikeFspExports.length
        ? 'CANDIDATE_PUBLIC_API_PRESENT'
        : 'UNAVAILABLE_FROM_NODE_FS_PUBLIC_API',
      note: 'No self-invented SQLite lock probe is used.',
    },
    linuxFilesystemClass: {
      status: process.platform === 'linux' && statfs && 'type' in statfs
        ? 'NUMERIC_TYPE_AVAILABLE_REQUIRES_FROZEN_SOURCE_EQUIVALENT_MAPPING'
        : (process.platform === 'linux' ? 'UNAVAILABLE' : 'NOT_APPLICABLE'),
      note: 'No path or mount-name heuristic is used.',
    },
  };

  const complete =
    evidence.requiredFacts.filesystemTypeOrCategory.status === 'AVAILABLE' &&
    evidence.requiredFacts.readOnly.status === 'AVAILABLE' &&
    evidence.requiredFacts.supportsByteRangeLocks.status === 'AVAILABLE' &&
    (process.platform !== 'linux' || evidence.requiredFacts.linuxFilesystemClass.status === 'AVAILABLE');

  evidence.productionProviderQualified = complete;
  evidence.result = 'PASS_PROBE_COMPLETED';
}
catch (error) {
  evidence.result = 'PROBE_ERROR';
  evidence.error = {
    name: error?.name ?? null,
    code: error?.code ?? null,
    message: error?.message ?? String(error),
    stack: error?.stack ?? null,
  };
  process.exitCode = 1;
}
finally {
  await fsp.rm(root, { recursive: true, force: true });
  const file = path.join(outDir, `row120-runtime-fact-provider-${process.platform}-${process.arch}.json`);
  await fsp.writeFile(file, JSON.stringify(evidence, null, 2));
  console.log(JSON.stringify(evidence, null, 2));
}
