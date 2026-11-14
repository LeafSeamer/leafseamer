import test from 'node:test';import assert from 'node:assert/strict';import crypto from 'node:crypto';import fs from 'node:fs';import os from 'node:os';import path from 'node:path';import {fileURLToPath,pathToFileURL} from 'node:url';import Database from 'better-sqlite3';
const root=process.env.LEAFSEAMER_PACKAGE_UNDER_TEST??fileURLToPath(new URL('../',import.meta.url)),load=name=>import(pathToFileURL(path.join(root,'dist',name)));
const {initializeStorage,migrateStorage}=await load('storage.mjs'),{snapshotIngestStore}=await load('snapshot-ingest-store.mjs'),{digest}=await load('runtime/auth-clock-value.mjs');
const id=()=>crypto.randomUUID(),tables=['snapshot_ingest_apply','snapshot_projection_head','ingest_source_revision','ingest_source_head','snapshot_ingest_audit','snapshot_ingest_outbox','snapshot_ingest_seal'];
function fixture(t){
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'leafseamer-snapshot-ingest-'));initializeStorage(dir);const service=migrateStorage(dir,'4'),bootId=id();let db,api;
 const source_key={authority_service_id:id(),database_instance_id:id(),registration_id:id(),source_stream_id:id(),source_epoch:id()},snapshot_ref={snapshot_id:id(),manifest_digest:'a'.repeat(64),snapshot_cursor:'0'};
 const view={source_key,binding_ref:{id:'binding'},state:'synchronized',contiguous_sequence:'4',highest_seen_sequence:'5',baseline_ref:null,gaps:[{gap_id:'gap',loss_extent:'unknown',first_missing:null,last_missing:null,resolution_state:'open'}]};
 const value={subscription:{owner:{kind:'service',consumer_service_id:service.service_id,consumer_database_instance_id:service.database_instance_id,consumer_id:'events',pairing_id:id()},purpose:'canonical_trigger',binding:{subscription_id:id(),publisher_service_id:source_key.authority_service_id,publisher_database_instance_id:source_key.database_instance_id,connection_generation:'1'},snapshot_ref},context:{challenge_id:id()},projection:{manifest:{snapshot_ref},records:[{view}],source_watermarks:[{source_key}]}},state={value,onRead:null};
 function open(){db=new Database(path.join(dir,'seamer.sqlite'));db.pragma('foreign_keys=ON');db.pragma('recursive_triggers=ON');db.pragma('synchronous=FULL');api=snapshotIngestStore({db,service,bootId,isOpen:()=>db.open,receiver:{currentInTransaction(){assert.equal(db.inTransaction,true);state.onRead?.();return structuredClone(state.value);}}});}
 open();t.after(()=>{if(db.open)db.close();assert.equal(path.dirname(path.resolve(dir)),path.resolve(os.tmpdir()));fs.rmSync(dir,{recursive:true,force:true,maxRetries:5,retryDelay:50});});
 return {get db(){return db;},get api(){return api;},state,source_key,counts:()=>tables.map(name=>db.prepare('SELECT count(*) FROM '+name).pluck().get()),reopen(){db.close();open();},next(sequence='6'){state.value=structuredClone(state.value);state.value.subscription.snapshot_ref={snapshot_id:id(),manifest_digest:digest(sequence).slice(7),snapshot_cursor:'0'};state.value.projection.manifest={snapshot_ref:state.value.subscription.snapshot_ref};state.value.projection.records[0].view.contiguous_sequence=sequence;state.value.projection.records[0].view.highest_seen_sequence=sequence;return state.value.projection.records[0].view;}};
}
test('投影、拒绝下界、来源修订、审计和通知同事务提交；重放和重启返回原回执',t=>{
 const f=fixture(t),receipt=f.api.apply(null);assert.equal(receipt.state,'committed');assert.deepEqual(f.counts(),[1,1,1,1,1,1,1]);const state=f.api.sourceState(f.source_key);assert.equal(state.reject_floor,'4');assert.equal(state.highest_seen_sequence,'5');assert.equal(state.state,'baseline_required');
 assert.deepEqual(f.api.apply(null),receipt);assert.deepEqual(f.api.lookup(null),receipt);f.reopen();assert.deepEqual(f.api.receipt(receipt.apply_id),receipt);assert.deepEqual(f.api.apply(null),receipt);assert.deepEqual(f.db.pragma('foreign_key_check'),[]);
 assert.equal(JSON.parse(f.db.prepare('SELECT body_jcs FROM snapshot_ingest_outbox').pluck().get()).acknowledged,false);
});
test('提交响应丢失且原句柄过期后按原快照身份查回，重启不分配新采用身份',t=>{
 const f=fixture(t),s=f.state.value.subscription,query={owner:s.owner,subscription_id:s.binding.subscription_id,snapshot_ref:s.snapshot_ref};let committed;
 assert.throws(()=>{committed=f.api.apply(null);throw Error('response_lost');},/response_lost/);f.state.onRead=()=>{throw Error('lease_expired');};assert.throws(()=>f.api.lookup(null),/lease_expired/);f.reopen();assert.deepEqual(f.api.find(query),committed);assert.deepEqual(f.counts(),[1,1,1,1,1,1,1]);assert.throws(()=>f.api.find({...query,snapshot_ref:{...query.snapshot_ref,manifest_digest:'b'.repeat(64)}}),{code:'snapshot_ingest_missing'});
});

test('只有当前投影及精确来源修订可以确认，旧回执仍可历史查原',t=>{
 const f=fixture(t),first=f.api.apply(null);assert.deepEqual(f.api.confirmationReceipt(first.apply_id),first);f.next();const second=f.api.apply(null);assert.throws(()=>f.api.confirmationReceipt(first.apply_id),{code:'snapshot_ingest_not_current'});assert.deepEqual(f.api.receipt(first.apply_id),first);assert.deepEqual(f.api.confirmationReceipt(second.apply_id),second);f.reopen();assert.deepEqual(f.api.confirmationReceipt(second.apply_id),second);
});
test('同key基线不能低于已见上界，重基线保存旧gap及其新观察并提升floor',t=>{
 const f=fixture(t);f.api.apply(null);f.next('4');assert.throws(()=>f.api.apply(null),{code:'snapshot_ingest_baseline_regression'});assert.deepEqual(f.counts(),[1,1,1,1,1,1,1]);
 const v=f.next();v.gaps=[{...v.gaps[0],resolution_state:'baseline_replaced'}];const receipt=f.api.apply(null),state=f.api.sourceState(f.source_key);assert.equal(receipt.generation,'2');assert.equal(state.reject_floor,'6');assert.equal(state.gaps[0].resolution_state,'open');assert.equal(state.gap_history.length,2);assert.equal(state.state,'baseline_required');
 f.next('7').gaps=[];f.api.apply(null);assert.equal(f.api.sourceState(f.source_key).gaps.length,1);assert.equal(f.api.sourceState(f.source_key).gap_history.length,2);
});
test('新epoch关闭旧key但保留floor和gap，旧key不可重新采用',t=>{
 const f=fixture(t);f.api.apply(null);const next=f.next('0');next.source_key.source_epoch=id();f.api.apply(null);const closed=f.api.sourceState(f.source_key);assert.equal(closed.state,'closed');assert.equal(closed.reject_floor,'4');assert.equal(closed.gaps.length,1);
 const old=f.next('9');old.source_key=structuredClone(f.source_key);assert.throws(()=>f.api.apply(null),{code:'snapshot_ingest_source_closed_or_changed'});
});
for(const table of tables)test('采用每处缺写均回滚且无可靠回执：'+table,t=>{
 const f=fixture(t);f.db.exec('CREATE TRIGGER omit_apply BEFORE INSERT ON '+table+' BEGIN SELECT RAISE(IGNORE); END');assert.throws(()=>f.api.apply(null));assert.deepEqual(f.counts(),tables.map(()=>0));f.db.exec('DROP TRIGGER omit_apply');assert.equal(f.api.apply(null).state,'committed');
});
test('提交前资格失效回滚；同快照异正文、本机owner错配和封印后补写拒绝',t=>{
 const f=fixture(t);let reads=0;f.state.onRead=()=>{if(++reads===2)throw Error('lease_expired');};assert.throws(()=>f.api.apply(null),/lease_expired/);assert.deepEqual(f.counts(),tables.map(()=>0));f.state.onRead=null;
 const receipt=f.api.apply(null);f.state.value.projection.records[0].view.contiguous_sequence='5';assert.throws(()=>f.api.apply(null),{code:'snapshot_ingest_conflict'});f.state.value.subscription.owner.consumer_service_id=id();assert.throws(()=>f.api.apply(null),{code:'snapshot_ingest_owner'});
 const row=f.db.prepare('SELECT source_key_digest,revision,lineage_key,apply_id,body_jcs,body_digest FROM ingest_source_revision').raw().get();row[0]='new';assert.throws(()=>f.db.prepare('INSERT INTO ingest_source_revision VALUES(?,?,?,?,?,?)').run(...row),/immutable_snapshot_ingest/);assert.deepEqual(f.api.receipt(receipt.apply_id),receipt);
});
