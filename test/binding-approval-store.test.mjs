import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
const packageRoot=process.env.LEAFSEAMER_PACKAGE_UNDER_TEST??fileURLToPath(new URL('../',import.meta.url));assert.ok(path.isAbsolute(packageRoot));
const load=name=>import(pathToFileURL(path.join(packageRoot,'dist',name)));
import Database from 'better-sqlite3';
const {initializeStorage,migrateStorage,openStorage}=await load('storage.mjs');
const {bindingApprovalStore}=await load('binding-approval-store.mjs');
import {source,digest} from './automation-program-fixture.mjs';
const id=()=>crypto.randomUUID(),tables=['binding_approval_revision','binding_approval_head','binding_approval_operation','binding_approval_outbox'];
function fixture(t){
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'leafseamer-binding-approval-'));initializeStorage(dir);const service=migrateStorage(dir,'3'),scope={authority_service_id:service.service_id,database_instance_id:service.database_instance_id};
 const actor={issuer_id:id(),issuer_epoch:id(),principal_id:id(),principal_kind:'human'},authority={authorizeInTransaction({operation_id,definition_id,context_id}){return {scope,actor,context_id,request_epoch:service.request_epoch,operation_id,definition_id,authorization_digest:digest('synthetic-authority')};}};
 const store=openStorage(dir,{draftAuthority:authority});const draft=store.drafts.create(null,{context:{context_id:'context',idempotency_key:'create'},content:{program:source().program,selections:[],policy_ref:{kind:'policy',scope,object_id:'policy',revision:'1',digest:digest({})}},metadata:{name:'绑定批准事务',description:'',layout:{}}});store.close();
 let db,api;const state={onAuthority:null,onSource:null,onPrepare:null,prepared:0,released:0,paired:true},bundle={registration_ref:{id:'synthetic-registration'},bindings:[]},prepared={pairing_ref:{id:id(),revision:'4',digest:digest('pairing')},request:{scope:{synthetic:true}},reply:{bundle},bundle_digest:digest(bundle)};
 const proof=definition_id=>{state.onAuthority?.();return {scope,actor,definition_id,authorization_digest:digest('synthetic-current-authority')};};
 function open(){db=new Database(path.join(dir,'seamer.sqlite'));db.pragma('foreign_keys=ON');db.pragma('recursive_triggers=ON');api=bindingApprovalStore({db,service,isOpen:()=>db.open,
  authority:{bindingApprovalInTransaction(_handle,definition_id){return proof(definition_id);},bindingApprovalLookupInTransaction(_handle,definition_id){return proof(definition_id);}},
  receiver:{async prepare(request){assert.equal(db.inTransaction,false);state.prepared++;await state.onPrepare?.(request);return {};},release(){state.released++;},currentInTransaction(){state.onSource?.();return structuredClone(prepared);}},pairings:{requireActivatedInTransaction(){assert.equal(state.paired,true,'pairing_revoked');}}});}
 open();t.after(()=>{if(db.open)db.close();assert.equal(path.dirname(path.resolve(dir)),path.resolve(os.tmpdir()));fs.rmSync(dir,{recursive:true,force:true,maxRetries:5,retryDelay:50});});
 const request={request_id:id(),approval_id:id(),definition_id:draft.definition_id,expected_bundle_digest:prepared.bundle_digest};
 return {get db(){return db;},get api(){return api;},reopen(){db.close();open();},state,prepared,actor,request,query:{binding_id:id(),scope:{synthetic:true},device_id:id(),expected_registration_ref:null},counts:()=>tables.map(name=>db.prepare('SELECT count(*) FROM '+name).pluck().get())};
}
test('绑定批准、精确重放、撤销和通知持久原子保存，重开库不复活',t=>{
 const f=fixture(t),approved=f.api.approve(null,null,f.request),ref={id:approved.approval_id,revision:'1',digest:digest(approved)};
 assert.deepEqual(f.counts(),[1,1,1,1]);assert.deepEqual(f.api.approve(null,null,f.request),approved);assert.deepEqual(f.api.check(null,ref,f.request.definition_id,null),approved);
 assert.throws(()=>f.api.approve(null,null,{...f.request,expected_bundle_digest:digest('other')}),{code:'binding_approval_conflict'});
 f.reopen();assert.deepEqual(f.api.get(null,approved.approval_id),approved);f.state.paired=false;assert.throws(()=>f.api.check(null,ref,f.request.definition_id,null),/pairing_revoked/);f.state.paired=true;
 const request={request_id:id(),approval_id:approved.approval_id,expected_revision:'1'},revoked=f.api.revoke(null,request);assert.equal(revoked.state,'revoked');assert.deepEqual(f.api.revoke(null,request),revoked);assert.deepEqual(f.counts(),[2,1,2,2]);
 f.reopen();assert.throws(()=>f.api.check(null,ref,f.request.definition_id,null),{code:'binding_approval_not_current'});assert.deepEqual(f.api.approve(null,null,f.request),approved);
 assert.deepEqual(f.db.prepare('PRAGMA foreign_key_check').raw().all(),[]);assert.throws(()=>f.db.exec('DELETE FROM binding_approval_revision'));
});
for(const table of tables)test('批准写入被忽略时全部回滚：'+table,t=>{
 const f=fixture(t);f.db.exec('CREATE TRIGGER test_ignore BEFORE INSERT ON '+table+" BEGIN SELECT RAISE(IGNORE); END;");
 assert.throws(()=>f.api.approve(null,null,f.request),{code:'binding_approval_storage'});assert.deepEqual(f.counts(),[0,0,0,0]);
});
test('末尾身份或来源变化回滚，服务主体拒绝批准',t=>{
 const f=fixture(t);f.actor.principal_kind='service';assert.throws(()=>f.api.approve(null,null,f.request),{code:'binding_approval_authority'});f.actor.principal_kind='human';
 let reads=0;f.state.onAuthority=()=>{if(++reads===2)f.actor.principal_id=id();};assert.throws(()=>f.api.approve(null,null,f.request),{code:'binding_approval_authority_changed'});assert.deepEqual(f.counts(),[0,0,0,0]);f.state.onAuthority=null;
 reads=0;f.state.onSource=()=>{if(++reads===2)f.prepared.bundle_digest=digest('changed');};assert.throws(()=>f.api.approve(null,null,f.request),{code:'binding_approval_source_changed'});assert.deepEqual(f.counts(),[0,0,0,0]);
});
test('撤销head写入失败保留旧批准且不产生半条通知',t=>{
 const f=fixture(t),approved=f.api.approve(null,null,f.request);f.db.exec("CREATE TRIGGER test_ignore BEFORE UPDATE ON binding_approval_head BEGIN SELECT RAISE(IGNORE); END;");
 assert.throws(()=>f.api.revoke(null,{request_id:id(),approval_id:approved.approval_id,expected_revision:'1'}),{code:'binding_approval_storage'});assert.deepEqual(f.counts(),[1,1,1,1]);assert.deepEqual(f.api.get(null,approved.approval_id),approved);
});
test('人员查看候选后批准重新读取，读取期间无writer锁且不向调用方交付句柄',async t=>{
 const f=fixture(t),candidate=await f.api.candidate(null,f.request.definition_id,f.query);
 assert.deepEqual(candidate,{definition_id:f.request.definition_id,bundle_digest:f.prepared.bundle_digest,bundle:f.prepared.reply.bundle});assert.equal(f.state.released,1);assert.deepEqual(f.counts(),[0,0,0,0]);
 const approved=await f.api.approveCurrent(null,f.query,f.request);assert.deepEqual(approved.bundle,candidate.bundle);assert.equal(f.state.prepared,2);assert.equal(f.state.released,2);assert.deepEqual(f.counts(),[1,1,1,1]);
});
test('候选读取后来源改变拒绝批准，并释放内部句柄',async t=>{
 const f=fixture(t);await f.api.candidate(null,f.request.definition_id,f.query);f.prepared.bundle_digest=digest('new-registration');
 await assert.rejects(f.api.approveCurrent(null,f.query,f.request),{code:'binding_approval_source_changed'});assert.deepEqual(f.counts(),[0,0,0,0]);assert.equal(f.state.released,2);
});
test('远端读取前核权限，读取中权限改变拒绝提交',async t=>{
 const f=fixture(t);f.state.onAuthority=()=>{throw new Error('denied');};
 await assert.rejects(f.api.candidate(null,f.request.definition_id,f.query),/denied/);await assert.rejects(f.api.approveCurrent(null,f.query,f.request),/denied/);assert.equal(f.state.prepared,0);
 f.state.onAuthority=null;f.state.onPrepare=()=>{f.actor.principal_id=id();};
 await assert.rejects(f.api.candidate(null,f.request.definition_id,f.query),{code:'binding_approval_authority_changed'});
 await assert.rejects(f.api.approveCurrent(null,f.query,f.request),{code:'binding_approval_authority_changed'});assert.deepEqual(f.counts(),[0,0,0,0]);assert.equal(f.state.released,2);
});
test('已提交批准在来源离线时精确重放，撤销后查原仍返回原批准版本',async t=>{
 const f=fixture(t),approved=await f.api.approveCurrent(null,f.query,f.request),query={request_id:f.request.request_id,request_digest:digest(f.request),definition_id:f.request.definition_id};
 f.state.onPrepare=()=>{throw new Error('offline');};assert.deepEqual(await f.api.approveCurrent(null,f.query,f.request),approved);assert.equal(f.state.prepared,1);
 f.api.revoke(null,{request_id:id(),approval_id:approved.approval_id,expected_revision:'1'});f.reopen();assert.equal(f.api.get(null,approved.approval_id).state,'revoked');assert.deepEqual(f.api.lookup(null,query),{...query,result:approved});
 assert.throws(()=>f.api.lookup(null,{...query,request_digest:digest('other')}),{code:'binding_approval_conflict'});
 f.actor.principal_id=id();assert.equal(f.api.lookup(null,query).result,null);assert.equal(f.api.lookup(null,{...query,request_id:id()}).result,null);
});
test('异步调用固定原请求，失败读取不写入批准',async t=>{
 const f=fixture(t),original=structuredClone(f.request),query=structuredClone(f.query);
 f.state.onPrepare=received=>{assert.deepEqual(received,query);f.request.approval_id=id();f.query.device_id=id();};
 const approved=await f.api.approveCurrent(null,f.query,f.request);assert.equal(approved.approval_id,original.approval_id);
 const before=f.counts();f.state.onPrepare=()=>{throw new Error('offline');};await assert.rejects(f.api.approveCurrent(null,f.query,{...original,request_id:id(),approval_id:id()}),/offline/);assert.deepEqual(f.counts(),before);
});
