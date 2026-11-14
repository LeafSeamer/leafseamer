import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import {initializeStorage} from '../dist/storage.mjs';
import {receivedSessionDenials} from '../dist/session-denial.mjs';
import {decodeSessionDenial,sessionDenialReceipt} from '../dist/runtime/peer-session-denial.mjs';
import {canonical,digest} from '../dist/runtime/auth-clock-value.mjs';
const id=()=>crypto.randomUUID();
function fixture(t){
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'leafseamer-denial-')),service=initializeStorage(dir),bootId=id(),db=new Database(path.join(dir,'seamer.sqlite'));
 db.exec('PRAGMA foreign_keys=ON');db.prepare("INSERT INTO service_boot VALUES(?,?,?,'started')").run(service.service_id,service.database_instance_id,bootId);
 t.after(()=>{db.close();assert.equal(path.dirname(path.resolve(dir)),path.resolve(os.tmpdir()));fs.rmSync(dir,{recursive:true,force:true});});
 const issuer={service_id:id(),issuer_id:id(),issuer_epoch:id()},peer={...issuer,database_instance_id:id(),service_kind:'controller'},socket={},record={revision:'1'},bindingId=id();let checks=0;
 const f={db,service,issuer,peer,record,socket,bindingId,onCheck:null};
 f.denials=receivedSessionDenials({db,service,bootId,isOpen:()=>true,issuer,pairings:{authorizeRevocationInTransaction(binding,channel){assert.ok(db.inTransaction);assert.equal(binding,bindingId);assert.equal(channel,socket);f.onCheck?.(++checks);return {peer,record:structuredClone(record)};}}});
 f.notice={format:'leafseamer.session-denial/1',issuer_scope:{authority_service_id:issuer.service_id,database_instance_id:peer.database_instance_id},issuer_id:issuer.issuer_id,issuer_epoch:issuer.issuer_epoch,
  operation_ref:{id:id(),revision:'1',digest:digest('synthetic-session-revocation')},session_id:id(),revision_floor:'2',target_scope:{authority_service_id:service.service_id,database_instance_id:service.database_instance_id}};
 f.snapshot={issuer_id:issuer.issuer_id,issuer_epoch:issuer.issuer_epoch,principal_kind:'human',session_id:f.notice.session_id,session_revision:'1'};
 f.receive=notice=>f.denials.receive(bindingId,socket,notice??f.notice);return f;
}
test('持久拒绝与原回执同事务，重复只返回原件且拒绝同操作换内容',t=>{
 const f=fixture(t);f.denials.assertAllowed(f.snapshot);const receipt=f.receive();assert.equal(receipt.identity_state,'denied');assert.equal(receipt.protection_state,'pending');
 assert.throws(()=>f.denials.assertAllowed({...f.snapshot,session_revision:'99'}),/session_denial_revoked/);
 assert.deepEqual(f.receive(),receipt);assert.equal(f.db.prepare('SELECT count(*) FROM received_session_denial').pluck().get(),1);
 assert.throws(()=>f.receive({...f.notice,session_id:id()}),/session_denial_conflict/);
 assert.doesNotThrow(()=>f.denials.assertAllowed({...f.snapshot,session_id:id()}));
 assert.throws(()=>f.db.exec('DELETE FROM received_session_denial'));assert.throws(()=>f.db.exec("UPDATE received_session_denial SET session_id='changed'"));
});
test('错签发者、目标库与回执关联均拒绝，规范JSON不接收重复键',t=>{
 const f=fixture(t);
 for(const changed of [{issuer_id:id()},{issuer_epoch:id()},{issuer_scope:{...f.notice.issuer_scope,database_instance_id:id()}},{target_scope:{...f.notice.target_scope,database_instance_id:id()}}])assert.throws(()=>f.receive({...f.notice,...changed}),/session_denial_issuer/);
 assert.equal(f.db.prepare('SELECT count(*) FROM received_session_denial').pluck().get(),0);
 const bytes=Buffer.from(canonical(f.notice));assert.deepEqual(decodeSessionDenial(bytes),f.notice);
 assert.throws(()=>decodeSessionDenial(Buffer.from('{"format":"wrong",'+bytes.subarray(1).toString())),/session_denial_canonical/);
 const receipt=f.receive();assert.throws(()=>sessionDenialReceipt({...receipt,notice_digest:digest('other')},f.notice),/receipt_binding/);
 assert.throws(()=>sessionDenialReceipt({...receipt,protection_state:'complete'},f.notice),/receipt/);
});
test('写入中止、静默忽略与末尾配对变化全部回滚，不返回接收成功',t=>{
 for(const behavior of ['ABORT','IGNORE']){
  const f=fixture(t);f.db.exec(`CREATE TRIGGER fail_denial BEFORE INSERT ON received_session_denial BEGIN SELECT RAISE(${behavior}${behavior==='ABORT'?",'injected'":''}); END`);
  assert.throws(()=>f.receive());assert.equal(f.db.prepare('SELECT count(*) FROM received_session_denial').pluck().get(),0);assert.doesNotThrow(()=>f.denials.assertAllowed(f.snapshot));
 }
 const f=fixture(t);f.onCheck=count=>{if(count===2)f.record.revision='2';};assert.throws(()=>f.receive(),/changed/);
 assert.equal(f.db.prepare('SELECT count(*) FROM received_session_denial').pluck().get(),0);
});
