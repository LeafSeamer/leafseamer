import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import {initializeStorage,openStorage} from '../dist/storage.mjs';
import {createPeerIdentityReceiver} from '../dist/peer-identity.mjs';
import {contractSelection as selection} from '../dist/sdk/index.mjs';
function fixture(change=()=>{},delay=0n,watermarks=null){
 let floor='1';watermarks??={current:()=>floor,observe:value=>{floor=value.revision;return floor;}};
 const requested=[];
 let tick=10000000000n,domain={id:'domain'},record={binding_id:'local-binding',revision:'1',digest:'sha256:'+'b'.repeat(64),state:'activated',peer_service_id:'controller',transcript:{parties:[
  {service_id:'seamer',database_instance_id:'database',service_kind:'seamer'},
  {service_id:'controller',issuer_id:'issuer',issuer_epoch:'epoch',binding_id:'remote-binding'}]}};
 const wall=BigInt(Date.parse('2026-11-11T00:00:00.000Z'))*1000000n,stamp=ns=>new Date(Number(ns/1000000n)).toISOString();
 const clock={pairingSample:()=>({tick_ns:String(tick),runtime_domain_ref:domain,lower_ns:String(wall+tick),upper_ns:String(wall+tick+1000n)})};
 const transport={async exchangeIntrospection(_id,packet){
  const input=JSON.parse(Buffer.from(packet.body).toString());requested.push(input.required_revision);tick+=delay;
  const snapshot={snapshot_id:'snapshot',issuer_id:'issuer',issuer_epoch:'epoch',issuer_revision:'7',audience:'seamer',principal_kind:'human',principal_id:'person',
   session_id:'session',credential_id:'credential',subject_revision:'1',session_revision:'1',policy_revision:'2',permissions:['auth.read'],scope_digest:'sha256:'+'a'.repeat(64),
   challenge_id:input.challenge_id,validity:{not_before:stamp(wall+tick-1000000000n),expires_at:stamp(wall+tick+2000000000n)},fresh_for_ms:2000,state:'active'};
  const recent_authentication={format:'issuer-recent-authentication/1',session_id:'session',session_revision:'1',not_before:stamp(wall-1000000000n),expires_at:stamp(wall)};
  const body={request_id:'request',service_id:'controller',service_boot_id:'boot',protocol_version:selection.contract_id,selection,data:{active:true,snapshot,recent_authentication}};change(body);
  return {status:'200',headers:[['content-type','application/json'],['x-leafseamer-contract',selection.contract_id],['x-leafseamer-contract-digest',selection.contract_digest]],body:Buffer.from(JSON.stringify(body))};
 }};
 const receiver=createPeerIdentityReceiver({service:{service_id:'seamer',database_instance_id:'database'},clock,pairings:{currentPending:()=>structuredClone(record)},transport,
  issuer:{service_id:'controller',issuer_id:'issuer',issuer_epoch:'epoch'},watermarks,denials:{assertAllowed(){}}});
 return {receiver,requested,prepare:()=>receiver.prepare({binding_id:'local-binding',token:'synthetic-opaque-token'}),advance:ns=>{tick+=ns;},domain:()=>{domain={id:'new-domain'};},revoke:()=>{record.state='revoked';}};
}
test('接收器使用原挑战截止，传输耗时不能重新获得两秒，过期句柄不能复活',async()=>{
 const slow=fixture(()=>{},2000000000n);await assert.rejects(slow.prepare(),/peer_identity_expired/);
 const f=fixture(()=>{},1500000000n),handle=await f.prepare();assert.equal(f.receiver.current(handle).snapshot.principal_id,'person');
 f.advance(500000000n);assert.throws(()=>f.receiver.current(handle),/peer_identity_expired/);f.advance(-500000000n);
 assert.throws(()=>f.receiver.current(handle),/peer_identity_handle/);
});

const issuer={service_id:'controller',issuer_id:'issuer',issuer_epoch:'epoch'};
function directory(t){const dir=fs.mkdtempSync(path.join(os.tmpdir(),'leafseamer-identity-watermark-'));t.after(()=>{assert.equal(path.dirname(path.resolve(dir)),path.resolve(os.tmpdir()));fs.rmSync(dir,{recursive:true,force:true,maxRetries:5,retryDelay:50});});initializeStorage(dir);return dir;}
test('已核验水位持久跨重启，后续请求不降级，较旧句柄不能继续使用',async t=>{
 const dir=directory(t);let store=openStorage(dir);t.after(()=>store.close());let revision='7';
 const first=fixture(body=>{body.data.snapshot.issuer_revision=revision;},0n,store.identityWatermarks),handle=await first.prepare();
 assert.equal(store.identityWatermarks.current(issuer),'7');revision='8';await first.prepare();
 assert.deepEqual(first.requested,['1','7']);assert.throws(()=>first.receiver.current(handle),/peer_identity_watermark/);
 first.receiver.close();store.close();store=openStorage(dir);
 const restarted=fixture(body=>{body.data.snapshot.issuer_revision='7';},0n,store.identityWatermarks);await assert.rejects(restarted.prepare(),/peer_identity_identity/);
 assert.deepEqual(restarted.requested,['8']);assert.equal(store.identityWatermarks.current(issuer),'8');
 assert.equal(store.identityWatermarks.current({...issuer,issuer_epoch:'another-epoch'}),'1');store.close();
 const db=new Database(path.join(dir,'seamer.sqlite'));try{
  assert.equal(db.prepare('SELECT count(*) FROM peer_identity_watermark').pluck().get(),2);
  assert.throws(()=>db.exec('DELETE FROM peer_identity_watermark'));
  assert.throws(()=>db.exec("UPDATE peer_identity_watermark SET revision='00000000000000000001'"));
  assert.deepEqual(db.prepare('PRAGMA foreign_key_check').raw().all(),[]);
 }finally{db.close();}
});
test('过期和错身份响应不推进水位，写入失败使接收器关闭而非返回临时成功',async t=>{
 const dir=directory(t);let store=openStorage(dir);t.after(()=>store.close());
 await assert.rejects(fixture(()=>{},2000000000n,store.identityWatermarks).prepare());
 await assert.rejects(fixture(body=>{body.data.snapshot.issuer_epoch='wrong';},0n,store.identityWatermarks).prepare());
 assert.equal(store.identityWatermarks.current(issuer),'1');store.close();
 let db=new Database(path.join(dir,'seamer.sqlite'));db.exec("CREATE TRIGGER fail_watermark BEFORE INSERT ON peer_identity_watermark BEGIN SELECT RAISE(ABORT,'injected_watermark_failure'); END");db.close();
 store=openStorage(dir);const failed=fixture(()=>{},0n,store.identityWatermarks);await assert.rejects(failed.prepare(),/injected_watermark_failure/);
 await assert.rejects(failed.prepare(),/peer_identity_capacity/);assert.equal(store.identityWatermarks.current(issuer),'1');store.close();
 db=new Database(path.join(dir,'seamer.sqlite'));assert.equal(db.prepare('SELECT count(*) FROM peer_identity_watermark').pluck().get(),0);db.close();
});
test('在途较旧响应不能覆盖并发推进后的水位，接收器不接受缺少持久提供者的装配',async()=>{
 let floor='1';const watermarks={current:()=>floor,observe:value=>{floor=value.revision;return floor;}};
 const raced=fixture(()=>{floor='9';},0n,watermarks);await assert.rejects(raced.prepare(),/peer_identity_watermark/);assert.equal(floor,'9');
 assert.throws(()=>createPeerIdentityReceiver({}),/peer_identity_watermark_required/);
});
test('核验响应拒绝错挑战、签发者、目标、水位和近期认证绑定',async()=>{
 const mutations=[b=>b.data.snapshot.challenge_id='other',b=>b.data.snapshot.issuer_epoch='other',b=>b.service_id='other',
  b=>b.data.snapshot.audience='other',b=>b.data.snapshot.issuer_revision='0',b=>b.data.recent_authentication.session_id='other',
  b=>b.data.recent_authentication.session_revision='2'];
 for(const change of mutations)await assert.rejects(fixture(change).prepare());
});
test('身份只经不透明句柄读取，原件不可改，域变化撤销和关闭均使句柄失效',async()=>{
 for(const action of ['domain','revoke','close']){
  const f=fixture(),handle=await f.prepare(),value=f.receiver.current(handle);
  assert.throws(()=>f.receiver.current({...handle}),/peer_identity_handle/);
  assert.throws(()=>{value.snapshot.permissions.push('auth.manage');},TypeError);
  // 身份仍可核验时，过期的近期认证原件不能被替换为新的管理资格。
  assert.ok(Date.parse(value.recent_authentication.expires_at)<Date.parse(value.snapshot.validity.not_before));
  if(action==='close')f.receiver.close();else f[action]();assert.throws(()=>f.receiver.current(handle));
 }
});
