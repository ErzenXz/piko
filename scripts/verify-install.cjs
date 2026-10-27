'use strict';
// Fresh public install: no global writes, credentials, trust exclusions or scripts.
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const { spawnSync } = require('node:child_process');
const cli = require('../npm/cli/package.json');
async function main() {
  const response = await fetch('https://registry.npmjs.org/@erzenx%2fpiko', {
    headers: { Accept: 'application/json', 'Cache-Control': 'no-cache' }, signal: AbortSignal.timeout(30000),
  });
  if (!response.ok) throw new Error(`Cannot inspect public latest: HTTP ${response.status}`);
  const metadata = await response.json();
  if (metadata['dist-tags']?.latest !== cli.version) throw new Error(`Public latest is not ${cli.version}`);
  const latest = metadata.versions?.[cli.version];
  if (!latest?._npmUser?.approver?.name || !latest?.dist?.attestations?.provenance) {
    throw new Error('Public latest is missing maintainer approval or provenance');
  }
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'piko-public-install-'));
  try {
    for (const dir of ['home', 'config', 'cache', 'store', 'state', 'bin']) fs.mkdirSync(path.join(root, dir));
    const authFile = path.join(root, 'empty.npmrc'); fs.writeFileSync(authFile, '');
    fs.writeFileSync(path.join(root, 'package.json'), JSON.stringify({ private: true }));
    // Leave release-age settings at pnpm defaults; exercise trust without exemptions.
    fs.writeFileSync(path.join(root, 'pnpm-workspace.yaml'),
      'packages: []\ntrustPolicy: no-downgrade\nstrictDepBuilds: true\nblockExoticSubdeps: true\nallowBuilds: {}\nupdateNotifier: false\n');
    const env = {};
    for (const name of ['PATH', 'SystemRoot', 'SYSTEMROOT', 'COMSPEC', 'PATHEXT', 'WINDIR', 'TEMP', 'TMP', 'TMPDIR', 'LANG', 'LC_ALL']) {
      if (process.env[name]) env[name] = process.env[name];
    }
    Object.assign(env, { HOME: path.join(root, 'home'), USERPROFILE: path.join(root, 'home'),
      APPDATA: path.join(root, 'config'), LOCALAPPDATA: path.join(root, 'cache'),
      XDG_CONFIG_HOME: path.join(root, 'config'), XDG_CACHE_HOME: path.join(root, 'cache'),
      PNPM_HOME: path.join(root, 'bin'), CI: 'true' });
    const script = process.env.npm_execpath;
    const js = script && /\.[cm]?js$/.test(script);
    const result = spawnSync(js ? process.execPath : (script || 'pnpm'),
      [...(js ? [script] : []), '--npmrc-auth-file', authFile,
        '--store-dir', path.join(root, 'store'), '--state-dir', path.join(root, 'state'),
        'add', cli.name, '--ignore-scripts', '--registry=https://registry.npmjs.org/'],
      { cwd: root, env, stdio: 'inherit', timeout: 180000 });
    if (result.error) throw result.error;
    if (result.status !== 0) throw new Error(`Strict public install failed (${result.status})`);
    const installed = JSON.parse(fs.readFileSync(path.join(root, 'node_modules', cli.name, 'package.json')));
    if (installed.version !== cli.version) throw new Error(`Installed ${installed.version}, expected ${cli.version}`);
    const launcher = path.join(root, 'node_modules', cli.name, 'bin/piko.cjs');
    const version = spawnSync(process.execPath, [launcher, '--version'], { cwd: root, env, encoding: 'utf8', timeout: 10000 });
    if (version.status !== 0 || version.stderr || version.stdout.trim() !== `piko ${cli.version}`) {
      throw new Error(`Installed launcher failed: ${version.stderr || version.error || version.stdout}`);
    }
    console.log(`Public latest ${cli.version} installed and ran with no-downgrade, no trust exclusions and scripts disabled.`);
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
}
main().catch((error) => { console.error(error.message); process.exitCode = 1; });
