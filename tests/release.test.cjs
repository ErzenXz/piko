'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const {spawnSync} = require('node:child_process');
const policy = require('../scripts/release-policy.cjs');
const source = {repository:'ErzenXz/piko',sourceCommit:'a'.repeat(40),tag:'v0.7.1',version:'0.7.1'};
const records = ['linux-x64','linux-arm64','darwin-x64','darwin-arm64','win32-x64','win32-arm64','cli'].map((platform) => ({name:platform==='cli'?'@erzenx/piko':`@erzenx/piko-${platform}`,version:'0.7.1',platform,file:`${platform}.tgz`,integrity:policy.integrity(Buffer.from(platform))}));
const id = (index) => `00000000-0000-4000-8000-${String(index+1).padStart(12,'0')}`;
const summary = (record,index) => JSON.stringify({[record.name]:{name:record.name,version:record.version,integrity:record.integrity,stageId:id(index)}});
function approved(record) { return {name:record.name,version:record.version,dist:{integrity:record.integrity,attestations:{url:`https://registry.npmjs.org/-/npm/v1/attestations/${encodeURIComponent(record.name)}@${record.version}`,provenance:{predicateType:'https://slsa.dev/provenance/v1'}}},_npmUser:{approver:{name:'release-owner'},trustedPublisher:{id:'github',oidcConfigId:id(99)}}}; }
function temp(t) { const dir=fs.mkdtempSync(path.join(os.tmpdir(),'piko-release-test-'));t.after(()=>fs.rmSync(dir,{recursive:true,force:true}));return dir; }
function statement(record) { return {_type:'https://in-toto.io/Statement/v1',predicateType:'https://slsa.dev/provenance/v1',subject:[{name:`pkg:npm/${record.name.replace('@','%40')}@${record.version}`,digest:{sha512:Buffer.from(record.integrity.slice(7),'base64').toString('hex')}}],predicate:{buildDefinition:{externalParameters:{workflow:{repository:'https://github.com/ErzenXz/piko',ref:'refs/tags/v0.7.1',path:'.github/workflows/release.yml'}},resolvedDependencies:[{uri:'git+https://github.com/ErzenXz/piko@refs/tags/v0.7.1',digest:{gitCommit:source.sourceCommit}}]}}}; }
const bundle=(value)=>({attestations:[{predicateType:'https://slsa.dev/provenance/v1',bundle:{dsseEnvelope:{payloadType:'application/vnd.in-toto+json',payload:Buffer.from(JSON.stringify(value)).toString('base64')}}}]});

test('all seven packages stage while registry versions remain invisible; receipt is durable before each subprocess',async(t)=>{
  const dir=temp(t),file=path.join(dir,'piko-staged.json'),calls=[];
  await policy.stageAll({records,source,file,manifests:new Map(),publish:async(record)=>{
    const current=JSON.parse(fs.readFileSync(file));
    assert.equal(current.packages.find((item)=>item.name===record.name).status,'staging');
    assert.equal(current.packages.length,7);
    const index=records.findIndex((item)=>item.name===record.name);calls.push(record.name);
    // Actual subprocess fixture models pnpm's stdout/stderr separation.
    const child=spawnSync(process.execPath,['-e','process.stderr.write("diagnostic\\n");process.stdout.write(process.argv[1]);',summary(record,index)],{encoding:'utf8'});
    assert.equal(child.status,0);return child.stdout;
  }});
  assert.deepEqual(calls,records.map((record)=>record.name));
  const receipt=policy.loadReceipt(file,records,source);
  assert.equal(receipt.packages.filter((item)=>item.status==='staged').length,7);
  assert.equal(receipt.packages.at(-1).name,'@erzenx/piko');
  // An identical replay uses receipts; it never needs registry visibility or a second upload.
  await policy.stageAll({records,source,file,manifests:new Map(),publish:()=>assert.fail('must not restage')});
});

test('ambiguous command failure saves intent and blocks every upload on retry',async(t)=>{
  const file=path.join(temp(t),'piko-staged.json');let calls=0;
  const publish=async(record)=>{if(++calls===3)throw new Error('transport lost after upload');return summary(record,calls-1);};
  await assert.rejects(policy.stageAll({records,source,file,manifests:new Map(),publish}),/transport/);
  const saved=JSON.parse(fs.readFileSync(file));
  assert.deepEqual(saved.packages.map((item)=>item.status),['staged','staged','staging','planned','planned','planned','planned']);
  await assert.rejects(policy.stageAll({records,source,file,manifests:new Map(),publish:()=>assert.fail('blind retry')}),/Uncertain prior staging/);
  // Authenticated manual reconciliation with exact original bytes can record the missing stage ID.
  saved.packages[2].status='staged';saved.packages[2].stageId=id(2);fs.writeFileSync(file,JSON.stringify(saved));
  const resumed=[];
  await policy.stageAll({records,source,file,manifests:new Map(),publish:async(record)=>{resumed.push(record.name);return summary(record,records.findIndex((item)=>item.name===record.name));}});
  assert.deepEqual(resumed,records.slice(3).map((record)=>record.name));
});

test('staging output must bind UUID, name, version, and SHA512 to intended bytes',()=>{
  assert.equal(policy.stageId(summary(records[0],0),records[0]),id(0));
  for(const output of ['not json','{}',summary(records[1],0),JSON.stringify({[records[0].name]:{stageId:id(0)}}),summary({...records[0],integrity:records[1].integrity},0),summary({...records[0],version:'9.9.9'},0)])assert.throws(()=>policy.stageId(output,records[0]));
});

test('missing human approver, OIDC, provenance, or immutable integrity is rejected',()=>{
  assert.match(policy.approved(approved(records[0]),records[0]),/^https:/);
  for(const mutate of [m=>delete m._npmUser.approver,m=>m._npmUser.approver=null,m=>m._npmUser.approver={},m=>delete m._npmUser.trustedPublisher,m=>m._npmUser.trustedPublisher.id='token',m=>delete m.dist.attestations,m=>m.dist.integrity=records[1].integrity,m=>m.dist.attestations.url='https://example.com/attestation']){
    const manifest=approved(records[0]);mutate(manifest);assert.throws(()=>policy.approved(manifest,records[0]));
  }
});

test('already published packages require approval before any staged upload',async(t)=>{
  const file=path.join(temp(t),'piko-staged.json'),manifest=approved(records[3]);delete manifest._npmUser.approver;
  await assert.rejects(policy.stageAll({records,source,file,manifests:new Map([[manifest.name,manifest]]),publish:()=>assert.fail('must preflight all existing')}),/approver/);
  assert.equal(fs.existsSync(file),false);
});

test('receipt rejects changed artifacts, source commits, duplicate IDs, and missing packages',async(t)=>{
  const file=path.join(temp(t),'piko-staged.json');
  const value=await policy.stageAll({records,source,file,manifests:new Map(),publish:async(record)=>summary(record,records.findIndex((item)=>item.name===record.name))});
  for(const mutate of [r=>r.sourceCommit='b'.repeat(40),r=>r.packages.pop(),r=>r.packages[0].integrity=records[1].integrity,r=>r.packages[0].stageId=r.packages[1].stageId,r=>r.packages[0].file='../other.tgz']){
    const changed=structuredClone(value);mutate(changed);assert.throws(()=>policy.receipt(changed,records,source));
  }
});

test('provenance binds exact digest, source, workflow path and tag',()=>{
  policy.provenance(bundle(statement(records[0])),records[0],source);
  for(const mutate of [s=>s.subject[0].digest.sha512='0'.repeat(128),s=>s.predicate.buildDefinition.resolvedDependencies[0].digest.gitCommit='b'.repeat(40),s=>s.predicate.buildDefinition.externalParameters.workflow.path='.github/workflows/other.yml',s=>s.predicate.buildDefinition.externalParameters.workflow.ref='refs/heads/main']){
    const changed=statement(records[0]);mutate(changed);assert.throws(()=>policy.provenance(bundle(changed),records[0],source));
  }
});


test('approved immutable versions are recorded and skipped without delaying remaining stages',async(t)=>{
  const file=path.join(temp(t),'piko-staged.json'),calls=[];
  const value=await policy.stageAll({records,source,file,manifests:new Map([[records[2].name,approved(records[2])]]),publish:async(record)=>{calls.push(record.name);return summary(record,records.findIndex((item)=>item.name===record.name));}});
  assert.equal(calls.length,6);
  assert.equal(value.packages[2].status,'published');
  assert.equal(value.packages[2].stageId,null);
  assert.equal(calls.at(-1),'@erzenx/piko');
});

test('malformed successful stdout leaves an uncertain receipt rather than permitting republish',async(t)=>{
  const file=path.join(temp(t),'piko-staged.json');
  await assert.rejects(policy.stageAll({records,source,file,manifests:new Map(),publish:async()=>'{"unexpected":{}}'}),/package mismatch/);
  assert.equal(JSON.parse(fs.readFileSync(file)).packages[0].status,'staging');
  await assert.rejects(policy.stageAll({records,source,file,manifests:new Map(),publish:()=>assert.fail('no retry')}),/Uncertain prior staging/);
});
