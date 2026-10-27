'use strict';
const fs = require('node:fs');
const crypto = require('node:crypto');
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const PROVENANCE = 'https://slsa.dev/provenance/v1';
const integrity = (bytes) => `sha512-${crypto.createHash('sha512').update(bytes).digest('base64')}`;
function assert(ok, message) { if (!ok) throw new Error(message); }
function stageId(output, expected) {
  const {name, version, integrity: expectedIntegrity} = expected;
  assert(typeof output === 'string' && output.length <= 262144, 'Invalid staging JSON size');
  let value;
  try { value = JSON.parse(output); } catch { throw new Error('Invalid staging JSON; reconcile the saved staging intent before retrying'); }
  assert(value && !Array.isArray(value) && Object.keys(value).length === 1 && Object.hasOwn(value, name), 'Staging JSON package mismatch; reconcile saved intent');
  assert(value[name]?.name === name && value[name]?.version === version && value[name]?.integrity === expectedIntegrity, 'Staging JSON name/version/integrity mismatch; reconcile saved intent');
  assert(UUID.test(value[name]?.stageId), 'Staging JSON has no valid stageId; reconcile saved intent');
  return value[name].stageId;
}
function approved(manifest, expected) {
  assert(manifest?.name === expected.name && manifest.version === expected.version, 'Registry package name/version mismatch');
  assert(manifest.dist?.integrity === expected.integrity, `Published package integrity differs: ${expected.name}`);
  const publisher = manifest._npmUser?.trustedPublisher;
  assert(publisher?.id === 'github' && typeof publisher.oidcConfigId === 'string' && publisher.oidcConfigId.trim(), `GitHub OIDC publisher missing: ${expected.name}`);
  const approver = manifest._npmUser?.approver;
  assert(approver && typeof approver === 'object' && !Array.isArray(approver) && typeof approver.name === 'string' && approver.name.trim(), `Human npm approver missing: ${expected.name}`);
  assert(manifest.dist?.attestations?.provenance?.predicateType === PROVENANCE, `Provenance missing: ${expected.name}`);
  let url;
  try { url = new URL(manifest.dist.attestations.url); } catch { throw new Error('Invalid attestations URL'); }
  assert(url.origin === 'https://registry.npmjs.org' && !url.username && !url.password && !url.search && !url.hash && decodeURIComponent(url.pathname) === `/-/npm/v1/attestations/${expected.name}@${expected.version}`, 'Unexpected attestations URL');
  return url.href;
}
function provenance(document, expected, source) {
  assert(Array.isArray(document?.attestations) && document.attestations.length <= 16, 'Invalid attestations document');
  const matches = document.attestations.filter((item) => item?.predicateType === PROVENANCE);
  assert(matches.length === 1, 'Expected exactly one provenance attestation');
  const envelope = matches[0].bundle?.dsseEnvelope;
  assert(envelope?.payloadType === 'application/vnd.in-toto+json', 'Invalid DSSE payload type');
  const payload = envelope.payload;
  assert(typeof payload === 'string' && payload.length <= 1024 * 1024 && /^[A-Za-z0-9+/]+={0,2}$/.test(payload), 'Invalid provenance payload');
  const bytes = Buffer.from(payload, 'base64');
  assert(bytes.toString('base64') === payload, 'Noncanonical provenance payload');
  const statement = JSON.parse(bytes.toString('utf8'));
  const digest = Buffer.from(expected.integrity.slice(7), 'base64').toString('hex');
  const subjectName = `pkg:npm/${expected.name}@${expected.version}`;
  assert(statement._type === 'https://in-toto.io/Statement/v1' && statement.predicateType === PROVENANCE && statement.subject?.some((item) => typeof item.name === 'string' && decodeURIComponent(item.name) === subjectName && item.digest?.sha512 === digest), `Provenance subject mismatch: ${expected.name}`);
  const build = statement.predicate?.buildDefinition;
  const workflow = build?.externalParameters?.workflow;
  assert(workflow?.repository === `https://github.com/${source.repository}` && workflow.ref === `refs/tags/${source.tag}` && workflow.path === '.github/workflows/release.yml', 'Provenance workflow mismatch');
  assert(build.resolvedDependencies?.some((item) => item.uri === `git+https://github.com/${source.repository}@refs/tags/${source.tag}` && item.digest?.gitCommit === source.sourceCommit), 'Provenance source commit mismatch');
}
function receipt(value, records, source) {
  assert(value?.schemaVersion === 1 && value.repository === source.repository && value.sourceCommit === source.sourceCommit && value.tag === source.tag && value.version === source.version, 'Staging receipt release/source mismatch');
  assert(Array.isArray(value.packages) && value.packages.length === 7 && records.length === 7, 'Staging receipt must contain all seven packages');
  const ids = new Set();
  for (const [index, record] of records.entries()) {
    const saved = value.packages[index];
    assert(saved && ['name', 'version', 'platform', 'file'].every((key) => saved[key] === record[key]), 'Staging receipt package/order mismatch');
    assert(typeof saved.integrity === 'string' && /^sha512-[A-Za-z0-9+/]{86}==$/.test(saved.integrity), 'Invalid receipt integrity');
    assert(!record.integrity || saved.integrity === record.integrity, `Staging receipt artifact integrity differs: ${record.name}`);
    assert(['planned', 'staging', 'staged', 'published'].includes(saved.status), 'Invalid staging receipt status');
    assert(saved.stageId === null || typeof saved.stageId === 'string' && UUID.test(saved.stageId), 'Invalid receipt stageId');
    assert(saved.status !== 'staged' || saved.stageId, 'Staged receipt requires stageId');
    assert(!['planned', 'staging'].includes(saved.status) || saved.stageId === null, 'Unexpected stageId before successful staging');
    if (saved.stageId) { assert(!ids.has(saved.stageId), 'Duplicate receipt stageId'); ids.add(saved.stageId); }
  }
  return value;
}
function saveReceipt(file, value) {
  const temporary = `${file}.${crypto.randomUUID()}.tmp`;
  try {
    const fd = fs.openSync(temporary, 'wx', 0o600);
    try {
      fs.writeFileSync(fd, JSON.stringify(value, null, 2) + '\n');
      fs.fsyncSync(fd);
    } finally { fs.closeSync(fd); }
    fs.renameSync(temporary, file);
  } finally { fs.rmSync(temporary, { force: true }); }
}
function loadReceipt(file, records, source) {
  if (!fs.existsSync(file)) return null;
  assert(fs.statSync(file).isFile() && fs.statSync(file).size <= 65536, 'Staging receipt too large or not a regular file');
  return receipt(JSON.parse(fs.readFileSync(file, 'utf8')), records, source);
}
async function stageAll({ records, source, file, manifests, publish }) {
  let value = loadReceipt(file, records, source);
  if (!value) value = { schemaVersion: 1, ...source, packages: records.map((record) => ({ ...record, status: 'planned', stageId: null })) };
  receipt(value, records, source);
  // Inspect every receipt entry before any upload. An interrupted command may
  // have succeeded remotely; OIDC cannot list stages, so a human must reconcile it.
  for (const saved of value.packages) {
    if (manifests.has(saved.name)) { approved(manifests.get(saved.name), saved); saved.status = 'published'; }
    assert(saved.status !== 'staging', `Uncertain prior staging attempt for ${saved.name}; use authenticated pnpm stage list/view to reconcile piko-staged.json. Do not restage blindly.`);
    assert(saved.status !== 'published' || manifests.has(saved.name), `Previously published package is not visible: ${saved.name}; retry registry checks later`);
  }
  saveReceipt(file, value);
  for (const saved of value.packages) {
    if (saved.status !== 'planned') continue;
    saved.status = 'staging';
    saveReceipt(file, value); // Persist intent BEFORE the only upload attempt.
    const output = await publish(saved);
    const stagedId = stageId(output, saved);
    assert(!value.packages.some((record) => record !== saved && record.stageId === stagedId), 'Duplicate returned stageId; reconcile saved staging intent');
    saved.stageId = stagedId;
    saved.status = 'staged';
    saveReceipt(file, value);
    console.log(`Staged ${saved.name}@${saved.version}: ${saved.stageId}`);
  }
  return receipt(value, records, source);
}
module.exports = { integrity, stageId, approved, provenance, receipt, loadReceipt, stageAll };
