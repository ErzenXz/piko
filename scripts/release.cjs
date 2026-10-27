'use strict';
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const crypto = require('node:crypto');
const { spawnSync } = require('node:child_process');
const { stage, platforms } = require('./package.cjs');
const cli = require('../npm/cli/package.json');
const policy = require('./release-policy.cjs');
function run(command, args) {
  const result = spawnSync(command, args, { stdio: 'inherit' });
  if (result.status !== 0) throw new Error(`${command} failed (${result.status})`);
}
const [action, dir] = process.argv.slice(2);
if (!dir || !path.isAbsolute(dir) || !['validate', 'stage', 'finalize'].includes(action)) throw new Error('Use validate|stage|finalize ABSOLUTE_ARTIFACT_DIR');
const tag = `v${cli.version}`;
if ((process.env.RELEASE_REF || process.env.GITHUB_REF_NAME) !== tag) throw new Error(`Release must run on tag ${tag}`);
const nativeFile = (platform) => path.join(dir, `native-${platform}.tgz`);
const archiveFile = (platform) => path.join(dir, `piko-${cli.version}-${platform}.${platform.startsWith('win32') ? 'zip' : 'tar.gz'}`);
function preflight() {
  const expected = platforms.flatMap((platform) => [nativeFile(platform), archiveFile(platform)]).map((file) => path.basename(file));
  const launcher = `erzenx-piko-${cli.version}.tgz`;
  const actual = fs.readdirSync(dir).filter((name) => /\.(tgz|tar\.gz|zip)$/.test(name) && name !== launcher);
  if (actual.length !== expected.length || actual.some((name) => !expected.includes(name))) throw new Error('Expected exactly six native npm packages and their six standalone archives');
  for (const platform of platforms) {
    run('python3', ['scripts/verify-package.py', nativeFile(platform), platform]);
    run('python3', ['scripts/verify-archive.py', archiveFile(platform), nativeFile(platform), platform]);
  }
}
async function published(name, timeoutMs = 15000) {
  const url = new URL(`https://registry.npmjs.org/${encodeURIComponent(name)}/${cli.version}`);
  url.searchParams.set('visibility', crypto.randomUUID());
  const response = await fetch(url, {
    signal: AbortSignal.timeout(timeoutMs), cache: 'no-store',
    headers: { 'Cache-Control': 'no-cache', Pragma: 'no-cache' },
  });
  if (response.status === 404) return null;
  if (!response.ok) {
    const error = new Error(`Registry check failed: ${response.status}`);
    error.retryable = response.status === 429 || response.status >= 500;
    throw error;
  }
  const manifest = JSON.parse((await boundedBody(response, 2 * 1024 * 1024)).toString('utf8'));
  if (manifest.name !== name || manifest.version !== cli.version) throw new Error('Registry package name/version mismatch');
  if (!manifest.dist || typeof manifest.dist.integrity !== 'string' || typeof manifest.dist.tarball !== 'string') throw new Error(`Registry integrity missing for ${name}`);
  return manifest;
}
function integrityMatches(bytes, integrity) {
  const values = integrity.split(/\s+/).filter((value) => value.startsWith('sha512-'));
  if (!values.length) throw new Error('Registry tarball must have SHA-512 integrity');
  const actual = crypto.createHash('sha512').update(bytes).digest('base64');
  return values.some((value) => value.slice(7) === actual);
}
async function waitPublished(name, deadline) {
  let waiting = false;
  while (Date.now() < deadline) {
    try {
      const manifest = await published(name, Math.max(1, Math.min(15000, deadline - Date.now())));
      if (manifest) return manifest;
    } catch (error) {
      if (!error.retryable && !['TimeoutError', 'AbortError', 'TypeError'].includes(error.name)) throw error;
    }
    if (!waiting) { console.log(`Waiting for approved registry visibility: ${name}@${cli.version}`); waiting = true; }
    const remaining = deadline - Date.now();
    if (remaining > 0) await new Promise((resolve) => setTimeout(resolve, Math.min(3000, remaining)));
  }
  throw new Error(`Approved version not visible: ${name}; reconcile approval/propagation before retrying finalize`);
}
async function recoverPublished(manifest, file, platform) {
  if (integrityMatches(fs.readFileSync(file), manifest.dist.integrity)) return;
  const url = new URL(manifest.dist.tarball);
  if (url.origin !== 'https://registry.npmjs.org' || url.username || url.password) throw new Error('Unexpected registry tarball URL');
  const response = await fetch(url, { signal: AbortSignal.timeout(60000), redirect: 'error' });
  if (!response.ok) throw new Error(`Registry tarball download failed: ${response.status}`);
  const chunks = [];
  let size = 0;
  for await (const chunk of response.body) {
    size += chunk.length;
    if (size > 40 * 1024 * 1024) throw new Error('Registry tarball too large');
    chunks.push(chunk);
  }
  const bytes = Buffer.concat(chunks);
  if (!integrityMatches(bytes, manifest.dist.integrity)) throw new Error(`Registry tarball integrity mismatch: ${manifest.name}`);
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'piko-registry-'));
  try {
    const candidate = path.join(temp, 'package.tgz');
    fs.writeFileSync(candidate, bytes);
    run('python3', ['scripts/verify-package.py', candidate, platform]);
    fs.copyFileSync(candidate, file);
    console.log(`Recovered exact published bytes for ${manifest.name}@${cli.version}`);
  } finally {
    fs.rmSync(temp, { recursive: true, force: true });
  }
}
function checksum(file) {
  return crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
}
function githubRelease() {
  const tarball = path.join(dir, `erzenx-piko-${cli.version}.tgz`);
  run('python3', ['scripts/verify-package.py', tarball, 'cli']);
  const files = [...platforms.flatMap((platform) => [nativeFile(platform), archiveFile(platform)]), tarball].sort();
  const sumsFile = path.join(dir, 'SHA256SUMS');
  const writeSums = () => fs.writeFileSync(sumsFile, files.map((file) => `${checksum(file)}  ${path.basename(file)}\n`).join(''));
  const existing = spawnSync('gh', ['api', `repos/ErzenXz/piko/releases/tags/${tag}`], { encoding: 'utf8', maxBuffer: 4 * 1024 * 1024 });
  if (existing.status !== 0) {
    if (!/HTTP 404/.test(existing.stderr || '')) throw new Error(`Cannot inspect GitHub release: ${existing.stderr}`);
    writeSums();
    run('gh', ['release', 'create', tag, ...files, sumsFile, '--verify-tag', '--title', `Piko ${cli.version}`, '--notes-file', 'CHANGELOG.md']);
    return;
  }
  const release = JSON.parse(existing.stdout);
  if (release.tag_name !== tag) throw new Error('GitHub release tag mismatch');
  const expected = new Map([...files, sumsFile].map((file) => [path.basename(file), file]));
  const assets = new Map();
  for (const asset of release.assets) {
    if (!expected.has(asset.name) || assets.has(asset.name)) throw new Error(`Unexpected/duplicate GitHub release asset: ${asset.name}`);
    if (!Number.isSafeInteger(asset.id) || !Number.isSafeInteger(asset.size) || asset.size < 0 || asset.size > 40 * 1024 * 1024) throw new Error('Invalid existing release asset');
    if (asset.digest && !/^sha256:[0-9a-f]{64}$/.test(asset.digest)) throw new Error(`Invalid GitHub asset digest: ${asset.name}`);
    assets.set(asset.name, asset);
  }
  function readAsset(asset) {
    const data = spawnSync('gh', ['api', `repos/ErzenXz/piko/releases/assets/${asset.id}`, '-H', 'Accept: application/octet-stream', '--allow-escape-sequences'], { maxBuffer: 40 * 1024 * 1024 });
    if (data.status !== 0 || data.stdout.length !== asset.size) throw new Error(`Cannot verify GitHub asset: ${asset.name}`);
    const digest = `sha256:${crypto.createHash('sha256').update(data.stdout).digest('hex')}`;
    if (asset.digest && digest !== asset.digest) throw new Error(`GitHub asset download integrity mismatch: ${asset.name}`);
    return { bytes: data.stdout, digest };
  }
  // Old releases may have archives created before deterministic metadata. Keep
  // their immutable bytes only after checking their SHA and exact npm payload.
  for (const platform of platforms) {
    const file = archiveFile(platform);
    const asset = assets.get(path.basename(file));
    if (!asset || asset.digest === `sha256:${checksum(file)}`) continue;
    const data = readAsset(asset);
    const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'piko-github-'));
    try {
      const candidate = path.join(temp, path.basename(file));
      fs.writeFileSync(candidate, data.bytes);
      run('python3', ['scripts/verify-archive.py', candidate, nativeFile(platform), platform]);
      fs.copyFileSync(candidate, file);
      asset.digest = data.digest;
      console.log(`Preserved verified existing standalone archive: ${asset.name}`);
    } finally {
      fs.rmSync(temp, { recursive: true, force: true });
    }
  }
  // Checksums must describe recovered old archive bytes, not the fresh rebuild.
  writeSums();
  const missing = new Map(expected);
  for (const asset of assets.values()) {
    const file = expected.get(asset.name);
    const digest = asset.digest || readAsset(asset).digest;
    if (digest !== `sha256:${checksum(file)}`) throw new Error(`Existing GitHub asset differs; refusing to overwrite ${asset.name}`);
    missing.delete(asset.name);
  }
  if (missing.size) run('gh', ['release', 'upload', tag, ...missing.values()]);
  console.log(`GitHub release ${tag} assets verified.`);
}
function records() {
  return [...platforms.map((platform) => ({name:`@erzenx/piko-${platform}`,version:cli.version,platform,file:path.basename(nativeFile(platform))})),
    {name:cli.name,version:cli.version,platform:'cli',file:`erzenx-piko-${cli.version}.tgz`}];
}
function releaseSource() {
  const result = spawnSync('git', ['rev-parse', 'HEAD'], {encoding:'utf8'});
  const sourceCommit = result.stdout?.trim();
  if (result.status !== 0 || !/^[0-9a-f]{40}$/.test(sourceCommit)) throw new Error('Cannot determine release source commit');
  if (process.env.GITHUB_SHA && process.env.GITHUB_SHA !== sourceCommit) throw new Error('Checked-out release commit differs from GITHUB_SHA');
  if (process.env.GITHUB_REPOSITORY && process.env.GITHUB_REPOSITORY !== 'ErzenXz/piko') throw new Error('Unexpected release repository');
  return {repository:'ErzenXz/piko',sourceCommit,tag,version:cli.version};
}
async function verifyApproved(manifest, expected, source) {
  const url = policy.approved(manifest, expected);
  const response = await fetch(url, {signal:AbortSignal.timeout(15000),redirect:'error',cache:'no-store'});
  if (!response.ok) throw new Error(`Cannot read published provenance: ${response.status}`);
  const text = await boundedBody(response, 2 * 1024 * 1024);
  policy.provenance(JSON.parse(text.toString('utf8')), expected, source);
}
async function boundedBody(response, limit) {
  const chunks = [];
  let size = 0;
  for await (const chunk of response.body) {
    size += chunk.length;
    if (size > limit) throw new Error('Registry response exceeds limit');
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}
function packLauncher() {
  const tarball = path.join(dir, `erzenx-piko-${cli.version}.tgz`);
  if (!fs.existsSync(tarball)) {
    const out = path.join(process.env.RUNNER_TEMP || os.tmpdir(), `cli-package-${crypto.randomUUID()}`);
    try {
      stage({out,platform:'cli'});
      run('pnpm', ['--dir',out,'pack','--out',tarball]);
    } finally { fs.rmSync(out,{recursive:true,force:true}); }
  }
  run('python3', ['scripts/verify-package.py',tarball,'cli']);
}
async function main() {
  preflight();
  if (action === 'validate') return;
  const source = releaseSource();
  const file = path.join(dir,'piko-staged.json');
  if (action === 'stage') {
    packLauncher();
    const manifests = new Map();
    // Inspect/recover all existing immutable versions before uploading any stage.
    for (const record of records()) {
      const manifest = await published(record.name);
      if (!manifest) continue;
      const expected = {...record,integrity:manifest.dist.integrity};
      await verifyApproved(manifest,expected,source);
      await recoverPublished(manifest,path.join(dir,record.file),record.platform);
      manifests.set(record.name,manifest);
    }
    preflight();
    const packages = records().map((record) => ({...record,integrity:policy.integrity(fs.readFileSync(path.join(dir,record.file)))}));
    await policy.stageAll({records:packages,source,file,manifests,publish:async (record) => {
      // Match pnpm's OIDC staging recipe without inheriting setup-node's
      // token-oriented npmrc. The intent was already saved before this callback.
      const authDir = fs.mkdtempSync(path.join(os.tmpdir(), 'piko-stage-auth-'));
      try {
        const authFile = path.join(authDir, 'empty.npmrc');
        fs.writeFileSync(authFile, '', {mode:0o600,flag:'wx'});
        const result = spawnSync('pnpm', ['stage','publish',path.join(dir,record.file),'--registry=https://registry.npmjs.org/',`--npmrc-auth-file=${authFile}`,'--provenance','--access','public','--ignore-scripts','--no-git-checks','--use-stderr','--json'], {encoding:'utf8',maxBuffer:1024*1024,timeout:120000,stdio:['ignore','pipe','inherit']});
        if (result.error || result.status !== 0) throw new Error(`Staging command failed for ${record.name}; saved intent requires reconciliation, not blind republishing`);
        return result.stdout;
      } finally {
        fs.rmSync(authDir,{recursive:true,force:true});
      }
    }});
    console.log(`All seven packages staged or already approved. Receipt: ${file}`);
  } else {
    const receipt = policy.loadReceipt(file,records(),source);
    if (!receipt || receipt.packages.some((record) => !['staged','published'].includes(record.status))) throw new Error('Finalize requires a complete seven-package staging receipt');
    // Finalize never calls npm publication/staging/approval commands. Every
    // version must already be human-approved, OIDC-published, and source-bound.
    const visibilityDeadline = Date.now() + 20 * 60 * 1000;
    for (const record of receipt.packages) {
      const manifest = await waitPublished(record.name, visibilityDeadline);
      await verifyApproved(manifest,record,source);
      await recoverPublished(manifest,path.join(dir,record.file),record.platform);
    }
    preflight();
    githubRelease();
  }
}
main().catch((error) => { console.error(error.message); process.exitCode = 1; });
