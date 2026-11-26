import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import crypto from 'node:crypto';
import os from 'node:os';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
const packageRoot=process.env.LEAFSEAMER_PACKAGE_UNDER_TEST??fileURLToPath(new URL('../',import.meta.url));assert.ok(path.isAbsolute(packageRoot));
const load=name=>import(pathToFileURL(path.join(packageRoot,'dist',name)));
import {spawnSync} from 'node:child_process';
import Database from 'better-sqlite3';
const {initializeStorage,migrateStorage,openStorage}=await load('storage.mjs');
const {bindingApprovalStore}=await load('binding-approval-store.mjs');
import {source,digest} from './automation-program-fixture.mjs';

function directory(t){const dir=fs.mkdtempSync(path.join(os.tmpdir(),'leafseamer-migrate-'));t.after(()=>{assert.equal(path.dirname(path.resolve(dir)),path.resolve(os.tmpdir()));fs.rmSync(dir,{recursive:true,force:true,maxRetries:5,retryDelay:50});});return dir;}
function inspect(dir,run){const db=new Database(path.join(dir,'seamer.sqlite'));try{return run(db);}finally{db.close();}}
function originals(dir){return inspect(dir,db=>Object.fromEntries(['service_state','schema_migration','service_boot','definition_identity','definition_content','definition_metadata','definition_head','definition_request','definition_notification'].map(name=>[name,db.prepare('SELECT * FROM '+name).raw().all()])));}
function seeded(t){
 const dir=directory(t),identity=initializeStorage(dir),authority={authorizeInTransaction({operation_id,definition_id,context_id,scope}){return {scope,actor:{issuer_id:'issuer',issuer_epoch:'epoch',principal_id:'person',principal_kind:'human'},context_id,request_epoch:identity.request_epoch,operation_id,definition_id,authorization_digest:digest({permission:1})};}};
 const store=openStorage(dir,{draftAuthority:authority});let draft;
 try{draft=store.drafts.create(null,{context:{context_id:'context',idempotency_key:'create'},content:{program:source().program,selections:[],policy_ref:{kind:'policy',scope:{authority_service_id:identity.service_id,database_instance_id:identity.database_instance_id},object_id:'policy',revision:'1',digest:digest({})}},metadata:{name:'迁移保留草稿',description:'',layout:{}}});}finally{store.close();}
 return {dir,identity,authority,draft};
}
test('显式迁移保留服务身份、旧迁移和草稿请求原件，普通打开不自动升级',t=>{
 const {dir,identity,authority,draft}=seeded(t),before=originals(dir);
 const migrated=migrateStorage(dir,'2');assert.deepEqual(migrated,{...identity,schema_revision:'2'});
 const after=originals(dir);assert.deepEqual(after.schema_migration.slice(0,1),before.schema_migration);
 for(const name of Object.keys(before).filter(name=>!['service_state','schema_migration'].includes(name)))assert.deepEqual(after[name],before[name],name);
 assert.deepEqual(migrateStorage(dir,'2'),migrated);assert.deepEqual(originals(dir),after);
 const store=openStorage(dir,{draftAuthority:authority});try{assert.deepEqual(store.drafts.get(null,draft.definition_id),draft);
  const changed=store.drafts.updateMetadata(null,draft.definition_id,{context:{context_id:'context',idempotency_key:'rename'},expected_metadata_revision:'1',metadata:{...draft.metadata,name:'迁移后修改'}});assert.equal(changed.metadata_revision,'2');
 }finally{store.close();}
 inspect(dir,db=>{assert.deepEqual(db.prepare('PRAGMA foreign_key_check').raw().all(),[]);assert.equal(db.pragma('user_version',{simple:true}),2);
  assert.throws(()=>db.exec('DELETE FROM schema_migration'));assert.throws(()=>db.exec("UPDATE service_state SET schema_revision='1'"));
  assert.throws(()=>db.exec('DELETE FROM definition_request'));assert.throws(()=>db.exec("UPDATE definition_request SET result_digest='changed'"));
 });
});
test('运行中的排他数据库和未知目标被拒绝，源库不变',t=>{
 const {dir}=seeded(t),before=originals(dir),running=openStorage(dir);
 try{assert.throws(()=>migrateStorage(dir,'2'),e=>e.code==='database_in_use');}finally{running.close();}
 assert.throws(()=>migrateStorage(dir,'999'),e=>e.code==='migration_target');
 const after=originals(dir);assert.deepEqual(after.service_state,before.service_state);assert.deepEqual(after.schema_migration,before.schema_migration);
 const opened=openStorage(dir);try{assert.equal(opened.identity.schema_revision,'1');}finally{opened.close();}
});
test('Schema 5 显式迁移保留身份、历史和旧业务原件，冲突时回滚',t=>{
 for(const fail of [true,false]){
  const {dir,identity}=seeded(t);
  migrateStorage(dir,'4');
  if(fail)inspect(dir,db=>db.exec('CREATE TABLE subscription_reconcile_inbox(conflict TEXT)'));
  const before=originals(dir);
  if(fail){
   assert.throws(()=>migrateStorage(dir,'5'),{code:'migration_failed'});
   assert.deepEqual(originals(dir),before);
   const open=openStorage(dir);try{assert.equal(open.identity.schema_revision,'4');}finally{open.close();}
  }else{
   assert.deepEqual(migrateStorage(dir,'5'),{...identity,schema_revision:'5'});
   const after=originals(dir);
   for(const key of Object.keys(before).filter(name=>!['service_state','schema_migration'].includes(name)))assert.deepEqual(after[key],before[key],key);
   assert.deepEqual(after.schema_migration.slice(0,-1),before.schema_migration);
   inspect(dir,db=>{
    assert.deepEqual(db.pragma('foreign_key_check'),[]);
    assert.equal(db.prepare('SELECT count(*) FROM subscription_reconcile_inbox').pluck().get(),0);
    const epoch=db.prepare('SELECT service_id,database_instance_id,ingest_epoch FROM event_ingest_epoch').raw().get();
    assert.deepEqual(epoch.slice(0,2),[identity.service_id,identity.database_instance_id]);
    assert.match(epoch[2],/^[0-9a-f-]{36}$/);
    assert.throws(()=>db.prepare('UPDATE event_ingest_epoch SET ingest_epoch=?').run(crypto.randomUUID()),/immutable_ingest_epoch/);
    assert.throws(()=>db.exec('DELETE FROM event_ingest_epoch'),/immutable_ingest_epoch/);
   });
  }
 }
});
test('Schema 6 提案表迁移冲突时保留 Schema 5 的全部身份和原迁移',t=>{
 const {dir}=seeded(t);migrateStorage(dir,'5');
 inspect(dir,db=>db.exec('CREATE TABLE automation_run_approval_proposal(conflict TEXT)'));
 const before=originals(dir);
 assert.throws(()=>migrateStorage(dir,'6'),{code:'migration_failed'});
 assert.deepEqual(originals(dir),before);
 const store=openStorage(dir);try{assert.equal(store.identity.schema_revision,'5');}finally{store.close();}
});
for(const collision of ['service_state_v2','schema_migration_v2','definition_request_v2','definition_version_reservation','definition_publication','definition_publication_proof'])test('迁移中途失败原子回滚：'+collision,t=>{
 const {dir}=seeded(t);inspect(dir,db=>db.exec('CREATE TABLE '+collision+'(sentinel TEXT) STRICT'));
 const before=originals(dir);assert.throws(()=>migrateStorage(dir,'2'),e=>e.code==='migration_failed');
 assert.deepEqual(originals(dir),before);inspect(dir,db=>{assert.equal(db.pragma('user_version',{simple:true}),1);assert.deepEqual(db.prepare('PRAGMA foreign_key_check').raw().all(),[]);db.exec('DROP TABLE '+collision);});
 assert.equal(migrateStorage(dir,'2').schema_revision,'2');
});
test('迁移后的摘要或版本失配拒绝打开且不修补原件',t=>{
 const {dir}=seeded(t);migrateStorage(dir,'2');
 inspect(dir,db=>db.exec("DROP TRIGGER schema_migration_no_update; UPDATE schema_migration SET script_digest='changed' WHERE to_revision='2'"));
 assert.throws(()=>openStorage(dir),e=>e.code==='migration_mismatch');assert.throws(()=>migrateStorage(dir,'2'),e=>e.code==='migration_mismatch');
 assert.equal(originals(dir).schema_migration[1][3],'changed');
});
test('安装入口仅在显式命令与目标下迁移，不启动服务',t=>{
 const {dir}=seeded(t),cli=path.join(packageRoot,'dist/cli.mjs');
 const missing=spawnSync(process.execPath,['--disallow-code-generation-from-strings',cli,'migrate-storage','--data-dir',dir],{encoding:'utf8',windowsHide:true});assert.equal(missing.status,1);assert.equal(JSON.parse(missing.stderr).error,'migration_target');
 const result=spawnSync(process.execPath,['--disallow-code-generation-from-strings',cli,'migrate-storage','--data-dir',dir,'--to-revision','2'],{encoding:'utf8',windowsHide:true});assert.equal(result.status,0,result.stderr);const value=JSON.parse(result.stdout);assert.equal(value.event,'storage_migrated');assert.equal(value.schema_revision,'2');assert.equal(value.ready,false);
 assert.equal(originals(dir).service_boot.length,1);
});

for(const from of ['1','2'])test('显式升级到schema3保留既有草稿及全部迁移原件：'+from,t=>{
 const {dir,identity}=seeded(t);if(from==='2')migrateStorage(dir,'2');const before=originals(dir),upgraded=migrateStorage(dir,'3');
 assert.deepEqual(upgraded,{...identity,schema_revision:'3'});const after=originals(dir);
 assert.deepEqual(after.schema_migration.slice(0,Number(from)),before.schema_migration);
 for(const name of Object.keys(before).filter(name=>!['service_state','schema_migration'].includes(name)))assert.deepEqual(after[name],before[name],name);
 assert.deepEqual(migrateStorage(dir,'3'),upgraded);assert.throws(()=>migrateStorage(dir,'2'),{code:'migration_source'});
 const running=openStorage(dir);try{assert.equal(running.identity.schema_revision,'3');}finally{running.close();}
 inspect(dir,db=>{for(const name of ['binding_approval_revision','binding_approval_head','binding_approval_operation','binding_approval_outbox'])assert.equal(db.prepare('SELECT count(*) FROM '+name).pluck().get(),0);assert.deepEqual(db.prepare('PRAGMA foreign_key_check').raw().all(),[]);});
});
for(const collision of ['service_state_v3','schema_migration_v3','binding_approval_revision','binding_approval_head','binding_approval_operation','binding_approval_outbox'])test('schema3迁移中途失败原子回滚：'+collision,t=>{
 const {dir}=seeded(t);migrateStorage(dir,'2');inspect(dir,db=>db.exec('CREATE TABLE '+collision+'(sentinel TEXT) STRICT'));const before=originals(dir);
 assert.throws(()=>migrateStorage(dir,'3'),{code:'migration_failed'});assert.deepEqual(originals(dir),before);
 inspect(dir,db=>{assert.equal(db.pragma('user_version',{simple:true}),2);assert.deepEqual(db.prepare('PRAGMA foreign_key_check').raw().all(),[]);db.exec('DROP TABLE '+collision);});assert.equal(migrateStorage(dir,'3').schema_revision,'3');
});

test('schema1直升3在后段失败时连同schema2回滚',t=>{
 const {dir}=seeded(t);inspect(dir,db=>db.exec('CREATE TABLE binding_approval_outbox(sentinel TEXT) STRICT'));const before=originals(dir);
 assert.throws(()=>migrateStorage(dir,'3'),{code:'migration_failed'});assert.deepEqual(originals(dir),before);
 inspect(dir,db=>{assert.equal(db.pragma('user_version',{simple:true}),1);assert.equal(db.prepare("SELECT count(*) FROM sqlite_schema WHERE name='definition_publication'").pluck().get(),0);db.exec('DROP TABLE binding_approval_outbox');});
 assert.equal(migrateStorage(dir,'3').schema_revision,'3');
});
const allOriginals=dir=>inspect(dir,db=>Object.fromEntries(db.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT IN('service_state','schema_migration') ORDER BY name").pluck().all().map(name=>[name,db.prepare('SELECT * FROM '+name).raw().all()])));
test('Schema 7 迁移保留 Schema 6 原件，批准通知表冲突时整笔回滚',t=>{
 const {dir,identity}=seeded(t);migrateStorage(dir,'6');
 const before=allOriginals(dir),history=originals(dir).schema_migration;
 inspect(dir,db=>db.exec('CREATE TABLE automation_run_approval_outbox(conflict TEXT)'));
 assert.throws(()=>migrateStorage(dir,'7'),{code:'migration_failed'});
 inspect(dir,db=>{assert.equal(db.pragma('user_version',{simple:true}),6);db.exec('DROP TABLE automation_run_approval_outbox');});
 assert.deepEqual(allOriginals(dir),before);assert.deepEqual(originals(dir).schema_migration,history);
 const old=openStorage(dir);try{assert.equal(old.identity.schema_revision,'6');assert.equal(old.runs.approve,undefined);}finally{old.close();}
 const oldRows=allOriginals(dir),migrated=migrateStorage(dir,'7');assert.deepEqual(migrated,{...identity,schema_revision:'7'});
 const after=allOriginals(dir);for(const [name,rows] of Object.entries(oldRows))assert.deepEqual(after[name],rows,name);
 assert.deepEqual(originals(dir).schema_migration.slice(0,-1),history);
 assert.deepEqual(migrateStorage(dir,'7'),migrated);
 inspect(dir,db=>assert.deepEqual(db.pragma('foreign_key_check'),[]));
 const current=openStorage(dir);try{assert.equal(typeof current.runs.approve,'function');}finally{current.close();}
});
test('Schema 8 保护队列表冲突不改变旧库，显式升级保留原件',t=>{
 const {dir,identity}=seeded(t);migrateStorage(dir,'7');
 const before=allOriginals(dir),history=originals(dir).schema_migration;
 inspect(dir,db=>db.exec('CREATE TABLE automation_run_approval_protection_outbox(conflict TEXT)'));
 assert.throws(()=>migrateStorage(dir,'8'),{code:'migration_failed'});
 inspect(dir,db=>{assert.equal(db.pragma('user_version',{simple:true}),7);db.exec('DROP TABLE automation_run_approval_protection_outbox');});
 assert.deepEqual(allOriginals(dir),before);assert.deepEqual(originals(dir).schema_migration,history);
 assert.deepEqual(migrateStorage(dir,'8'),{...identity,schema_revision:'8'});
 const after=allOriginals(dir);for(const [name,rows]of Object.entries(before))assert.deepEqual(after[name],rows,name);
 assert.deepEqual(originals(dir).schema_migration.slice(0,-1),history);
 inspect(dir,db=>assert.deepEqual(db.pragma('foreign_key_check'),[]));
});
test('Schema 9 追加回执表且保留 Schema 8 数据，表冲突时整笔回滚',t=>{
 const {dir,identity}=seeded(t);migrateStorage(dir,'8');
 const before=allOriginals(dir),history=originals(dir).schema_migration;
 inspect(dir,db=>db.exec('CREATE TABLE automation_run_approval_protection_receipt(conflict TEXT)'));
 assert.throws(()=>migrateStorage(dir,'9'),{code:'migration_failed'});
 inspect(dir,db=>{assert.equal(db.pragma('user_version',{simple:true}),8);db.exec('DROP TABLE automation_run_approval_protection_receipt');});
 assert.deepEqual(allOriginals(dir),before);assert.deepEqual(originals(dir).schema_migration,history);
 assert.deepEqual(migrateStorage(dir,'9'),{...identity,schema_revision:'9'});
 const after=allOriginals(dir);for(const [name,rows]of Object.entries(before))assert.deepEqual(after[name],rows,name);
 assert.deepEqual(originals(dir).schema_migration.slice(0,-1),history);
 inspect(dir,db=>assert.deepEqual(db.pragma('foreign_key_check'),[]));
});
test('Schema 10 以数值顺序核验迁移，并在反向拒绝表冲突时保留旧库',t=>{
 const {dir,identity}=seeded(t);migrateStorage(dir,'9');
 const before=allOriginals(dir),history=originals(dir).schema_migration;
 inspect(dir,db=>db.exec('CREATE TABLE automation_run_endorsement_rejection(conflict TEXT)'));
 assert.throws(()=>migrateStorage(dir,'10'),{code:'migration_failed'});
 inspect(dir,db=>{assert.equal(db.pragma('user_version',{simple:true}),9);db.exec('DROP TABLE automation_run_endorsement_rejection');});
 assert.deepEqual(allOriginals(dir),before);assert.deepEqual(originals(dir).schema_migration,history);
 assert.deepEqual(migrateStorage(dir,'10'),{...identity,schema_revision:'10'});
 const after=allOriginals(dir);for(const [name,rows]of Object.entries(before))assert.deepEqual(after[name],rows,name);
 assert.deepEqual(originals(dir).schema_migration.slice(0,-1),history);
 const store=openStorage(dir);try{assert.equal(store.identity.schema_revision,'10');}finally{store.close();}
 inspect(dir,db=>assert.deepEqual(db.pragma('foreign_key_check'),[]));
});
test('Schema 11 显式迁移保留服务身份及旧原件，账本创建冲突原子回滚',t=>{
 const {dir,identity}=seeded(t);migrateStorage(dir,'10');
 const before=allOriginals(dir),history=originals(dir).schema_migration;
 inspect(dir,db=>db.exec('CREATE TABLE event_binding_approval_revision(conflict TEXT)'));
 assert.throws(()=>migrateStorage(dir,'11'),{code:'migration_failed'});
 inspect(dir,db=>{assert.equal(db.pragma('user_version',{simple:true}),10);db.exec('DROP TABLE event_binding_approval_revision');});
 assert.deepEqual(allOriginals(dir),before);assert.deepEqual(originals(dir).schema_migration,history);
 assert.deepEqual(migrateStorage(dir,'11'),{...identity,schema_revision:'11'});
 const after=allOriginals(dir);for(const [name,rows]of Object.entries(before))assert.deepEqual(after[name],rows,name);
 assert.deepEqual(after.event_binding_approval_revision,[]);
 assert.deepEqual(originals(dir).schema_migration.slice(0,-1),history);
 inspect(dir,db=>assert.deepEqual(db.pragma('foreign_key_check'),[]));
});
test('Schema 13 显式启用预检账本，冲突回滚，保留旧身份、业务和迁移原件',t=>{
 const {dir,identity}=seeded(t);migrateStorage(dir,'12');
 const before=allOriginals(dir),history=originals(dir).schema_migration;
 inspect(dir,db=>db.exec('CREATE TABLE automation_run_preflight(conflict TEXT)'));
 assert.throws(()=>migrateStorage(dir,'13'),{code:'migration_failed'});
 inspect(dir,db=>{assert.equal(db.pragma('user_version',{simple:true}),12);db.exec('DROP TABLE automation_run_preflight');});
 assert.deepEqual(allOriginals(dir),before);assert.deepEqual(originals(dir).schema_migration,history);
 const running=openStorage(dir);try{assert.equal(running.runs.recordArmPreflight,undefined);}finally{running.close();}
 assert.deepEqual(migrateStorage(dir,'13'),{...identity,schema_revision:'13'});
 const after=allOriginals(dir);for(const [name,rows] of Object.entries(before)){
  if(name!=='service_boot')assert.deepEqual(after[name],rows,name);
 }
 assert.deepEqual(after.automation_run_preflight,[]);
 assert.deepEqual(originals(dir).schema_migration.slice(0,-1),history);
 inspect(dir,db=>assert.deepEqual(db.pragma('foreign_key_check'),[]));
});
test('Schema 14 布防通知表冲突时整笔回滚，显式升级保留旧数据',t=>{
 const {dir,identity}=seeded(t);migrateStorage(dir,'13');
 const before=allOriginals(dir),history=originals(dir).schema_migration;
 inspect(dir,db=>db.exec('CREATE TABLE automation_arm_notification(conflict TEXT)'));
 assert.throws(()=>migrateStorage(dir,'14'),{code:'migration_failed'});
 inspect(dir,db=>{assert.equal(db.pragma('user_version',{simple:true}),13);db.exec('DROP TABLE automation_arm_notification');});
 assert.deepEqual(allOriginals(dir),before);assert.deepEqual(originals(dir).schema_migration,history);
 assert.deepEqual(migrateStorage(dir,'14'),{...identity,schema_revision:'14'});
 const after=allOriginals(dir);for(const [name,rows] of Object.entries(before))assert.deepEqual(after[name],rows,name);
 assert.deepEqual(after.automation_activation_slot,[]);
 assert.deepEqual(originals(dir).schema_migration.slice(0,-1),history);
 inspect(dir,db=>assert.deepEqual(db.pragma('foreign_key_check'),[]));
});
test('Schema 15 显式启用准备回执与通知，冲突时保留全部旧原件',t=>{
 const {dir,identity}=seeded(t);migrateStorage(dir,'14');
 const store=openStorage(dir);try{assert.equal(store.runs.armPreparationObservations,undefined);}finally{store.close();}
 const before=allOriginals(dir),history=originals(dir).schema_migration;
 inspect(dir,db=>db.exec('CREATE TABLE automation_arm_preparation_notification(conflict TEXT)'));
 assert.throws(()=>migrateStorage(dir,'15'),{code:'migration_failed'});
 inspect(dir,db=>{assert.equal(db.pragma('user_version',{simple:true}),14);db.exec('DROP TABLE automation_arm_preparation_notification');});
 assert.deepEqual(allOriginals(dir),before);assert.deepEqual(originals(dir).schema_migration,history);
 assert.deepEqual(migrateStorage(dir,'15'),{...identity,schema_revision:'15'});
 const after=allOriginals(dir);for(const [name,rows] of Object.entries(before))assert.deepEqual(after[name],rows,name);
 assert.deepEqual(after.automation_arm_preparation_receipt,[]);assert.deepEqual(after.automation_arm_preparation_notification,[]);
 assert.deepEqual(originals(dir).schema_migration.slice(0,-1),history);
 inspect(dir,db=>assert.deepEqual(db.pragma('foreign_key_check'),[]));
});
test('Schema 16 显式启用布防完成和封存通知，冲突时保留旧库',t=>{
 const {dir,identity}=seeded(t);migrateStorage(dir,'15');
 const running=openStorage(dir);try{assert.equal(running.runs.completeArmRun,undefined);}finally{running.close();}
 const before=allOriginals(dir),history=originals(dir).schema_migration;
 inspect(dir,db=>db.exec('CREATE TABLE automation_arm_completion_notification(conflict TEXT)'));
 assert.throws(()=>migrateStorage(dir,'16'),{code:'migration_failed'});
 inspect(dir,db=>{assert.equal(db.pragma('user_version',{simple:true}),15);db.exec('DROP TABLE automation_arm_completion_notification');});
 assert.deepEqual(allOriginals(dir),before);assert.deepEqual(originals(dir).schema_migration,history);
 assert.deepEqual(migrateStorage(dir,'16'),{...identity,schema_revision:'16'});
 const after=allOriginals(dir);for(const [name,rows] of Object.entries(before))assert.deepEqual(after[name],rows,name);
 assert.deepEqual(after.automation_arm_completion,[]);assert.deepEqual(after.automation_arm_completion_notification,[]);
 assert.deepEqual(originals(dir).schema_migration.slice(0,-1),history);
 inspect(dir,db=>assert.deepEqual(db.pragma('foreign_key_check'),[]));
});
test('Schema 29 显式启用提交账本，末段失败时以原身份和全部旧数据打开 Schema 28',t=>{
 const {dir,identity}=seeded(t);migrateStorage(dir,'28');
 const before=allOriginals(dir),history=originals(dir).schema_migration;
 inspect(dir,db=>db.exec('CREATE TABLE automation_execution_submission_adjudication(conflict TEXT)'));
 assert.throws(()=>migrateStorage(dir,'29'),{code:'migration_failed'});
 inspect(dir,db=>{
  assert.equal(db.pragma('user_version',{simple:true}),28);
  assert.equal(db.prepare("SELECT count(*) FROM sqlite_schema WHERE name='automation_execution_submission_attempt'").pluck().get(),0);
  db.exec('DROP TABLE automation_execution_submission_adjudication');
 });
 assert.deepEqual(allOriginals(dir),before);assert.deepEqual(originals(dir).schema_migration,history);
 const previous=openStorage(dir);try{assert.equal(previous.identity.schema_revision,'28');}finally{previous.close();}
 const saved=allOriginals(dir);
 assert.deepEqual(migrateStorage(dir,'29'),{...identity,schema_revision:'29'});
 const after=allOriginals(dir);for(const [name,rows] of Object.entries(saved))assert.deepEqual(after[name],rows,name);
 assert.deepEqual(originals(dir).schema_migration.slice(0,-1),history);
 for(const suffix of ['', '_attempt','_uncertain','_adjudication'])assert.deepEqual(after['automation_execution_submission'+suffix],[]);
 inspect(dir,db=>assert.deepEqual(db.pragma('foreign_key_check'),[]));
});
test('Schema 30 显式启用终态证据，末段冲突保留 Schema 29 及其原件',t=>{
 const {dir,identity}=seeded(t);
 migrateStorage(dir,'29');
 const before=allOriginals(dir),history=originals(dir).schema_migration;
 inspect(dir,db=>db.exec('CREATE TABLE automation_execution_command_terminal(conflict TEXT)'));
 assert.throws(()=>migrateStorage(dir,'30'),{code:'migration_failed'});
 inspect(dir,db=>{
  assert.equal(db.pragma('user_version',{simple:true}),29);
  db.exec('DROP TABLE automation_execution_command_terminal');
 });
 assert.deepEqual(allOriginals(dir),before);
 assert.deepEqual(originals(dir).schema_migration,history);
 const previous=openStorage(dir);try{assert.equal(previous.identity.schema_revision,'29');}finally{previous.close();}
 const saved=allOriginals(dir);
 assert.deepEqual(migrateStorage(dir,'30'),{...identity,schema_revision:'30'});
 const after=allOriginals(dir);
 for(const [name,rows] of Object.entries(saved))assert.deepEqual(after[name],rows,name);
 assert.deepEqual(originals(dir).schema_migration.slice(0,-1),history);
 assert.deepEqual(after.automation_execution_command_terminal,[]);
 inspect(dir,db=>assert.deepEqual(db.pragma('foreign_key_check'),[]));
});
test('Schema 31 显式升级依赖校验，冲突回滚保留 Schema 30 及其原件',t=>{
 const {dir,identity}=seeded(t);
 migrateStorage(dir,'30');
 const before=allOriginals(dir),history=originals(dir).schema_migration;
 inspect(dir,db=>db.exec('CREATE TABLE schema_migration_v31(conflict TEXT)'));
 assert.throws(()=>migrateStorage(dir,'31'),{code:'migration_failed'});
 inspect(dir,db=>db.exec('DROP TABLE schema_migration_v31'));
 assert.deepEqual(allOriginals(dir),before);
 assert.deepEqual(originals(dir).schema_migration,history);
 const previous=openStorage(dir);try{assert.equal(previous.identity.schema_revision,'30');}finally{previous.close();}
 const saved=allOriginals(dir);
 assert.deepEqual(migrateStorage(dir,'31'),{...identity,schema_revision:'31'});
 assert.deepEqual(allOriginals(dir),saved);
 assert.deepEqual(originals(dir).schema_migration.slice(0,-1),history);
 inspect(dir,db=>assert.deepEqual(db.pragma('foreign_key_check'),[]));
});
test('Schema 32 显式增加普通提交，冲突时保留原业务和迁移历史',t=>{
 const {dir,identity}=seeded(t);
 migrateStorage(dir,'31');
 const history=originals(dir).schema_migration,before=allOriginals(dir);
 inspect(dir,db=>db.exec('CREATE TABLE automation_execution_commit(conflict TEXT)'));
 assert.throws(()=>migrateStorage(dir,'32'),{code:'migration_failed'});
 inspect(dir,db=>{assert.equal(db.pragma('user_version',{simple:true}),31);db.exec('DROP TABLE automation_execution_commit');});
 assert.deepEqual(allOriginals(dir),before);
 assert.deepEqual(originals(dir).schema_migration,history);
 assert.deepEqual(migrateStorage(dir,'32'),{...identity,schema_revision:'32'});
 const after=allOriginals(dir);
 assert.deepEqual(after.automation_execution_commit,[]);
 for(const [name,rows] of Object.entries(before))assert.deepEqual(after[name],rows,name);
 assert.deepEqual(originals(dir).schema_migration.slice(0,-1),history);
 inspect(dir,db=>assert.deepEqual(db.pragma('foreign_key_check'),[]));
});
test('Schema 33 显式增加通知回执，冲突时完整保留 Schema 32',t=>{
 const {dir,identity}=seeded(t);
 migrateStorage(dir,'32');
 const history=originals(dir).schema_migration,before=allOriginals(dir);
 inspect(dir,db=>db.exec('CREATE TABLE automation_execution_notification_receipt(conflict TEXT)'));
 assert.throws(()=>migrateStorage(dir,'33'),{code:'migration_failed'});
 inspect(dir,db=>{assert.equal(db.pragma('user_version',{simple:true}),32);db.exec('DROP TABLE automation_execution_notification_receipt');});
 assert.deepEqual(allOriginals(dir),before);
 assert.deepEqual(originals(dir).schema_migration,history);
 assert.deepEqual(migrateStorage(dir,'33'),{...identity,schema_revision:'33'});
 const after=allOriginals(dir);
 for(const [name,rows] of Object.entries(before))assert.deepEqual(after[name],rows,name);
 assert.deepEqual(after.automation_execution_notification_receipt,[]);
 assert.deepEqual(originals(dir).schema_migration.slice(0,-1),history);
 inspect(dir,db=>assert.deepEqual(db.pragma('foreign_key_check'),[]));
});
for(const from of ['1','2','3'])test('显式schema4迁移保留全部旧表、身份和历史，包含非空批准原件：'+from,t=>{
 const {dir,identity,draft}=seeded(t);if(from!=='1')migrateStorage(dir,from);
 if(from==='3')inspect(dir,db=>{db.pragma('foreign_keys=ON');const scope={authority_service_id:identity.service_id,database_instance_id:identity.database_instance_id},bundle={registration_ref:{id:'fixture'},bindings:[]};
  // 单一不可变夹具原件用于迁移保留验证，重复读取须完全相同。
  const source={pairing_ref:{id:crypto.randomUUID()},request:{scope},reply:{bundle},bundle_digest:digest(bundle)};
  const stable=bindingApprovalStore({db,service:{...identity,schema_revision:'3'},isOpen:()=>true,authority:{bindingApprovalInTransaction(){return {scope,definition_id:draft.definition_id,actor:{principal_kind:'human'}};}},receiver:{currentInTransaction:()=>structuredClone(source)},pairings:{}});
  stable.approve(null,null,{request_id:crypto.randomUUID(),approval_id:crypto.randomUUID(),definition_id:draft.definition_id,expected_bundle_digest:source.bundle_digest});
 });
 const before=allOriginals(dir),history=originals(dir).schema_migration,upgraded=migrateStorage(dir,'4');assert.deepEqual(upgraded,{...identity,schema_revision:'4'});
 const after=allOriginals(dir);for(const [name,rows]of Object.entries(before))assert.deepEqual(after[name],rows,name);assert.deepEqual(originals(dir).schema_migration.slice(0,Number(from)),history);
 assert.deepEqual(migrateStorage(dir,'4'),upgraded);assert.throws(()=>migrateStorage(dir,'3'),{code:'migration_source'});const running=openStorage(dir);try{assert.equal(running.identity.schema_revision,'4');}finally{running.close();}
 inspect(dir,db=>{assert.deepEqual(db.pragma('foreign_key_check'),[]);assert.equal(db.pragma('user_version',{simple:true}),4);});
});
for(const collision of ['service_state_v4','schema_migration_v4','snapshot_ingest_apply','ingest_source_revision','snapshot_ingest_seal'])test('schema4中途失败保留完整旧库：'+collision,t=>{
 const {dir}=seeded(t);migrateStorage(dir,'3');inspect(dir,db=>db.exec('CREATE TABLE '+collision+'(sentinel TEXT) STRICT'));const before=allOriginals(dir),meta=originals(dir);
 assert.throws(()=>migrateStorage(dir,'4'),{code:'migration_failed'});assert.deepEqual(allOriginals(dir),before);assert.deepEqual(originals(dir),meta);
 inspect(dir,db=>{assert.equal(db.pragma('user_version',{simple:true}),3);db.exec('DROP TABLE '+collision);});assert.equal(migrateStorage(dir,'4').schema_revision,'4');
});
test('Schema 40 显式保存恢复预览，写冲突回滚到完整 Schema 39',t=>{
 const {dir,identity}=seeded(t);
 migrateStorage(dir,'39');
 const previous=openStorage(dir);try{assert.equal(previous.identity.schema_revision,'39');}finally{previous.close();}
 const before=allOriginals(dir),history=originals(dir).schema_migration;
 inspect(dir,db=>db.exec('CREATE TABLE automation_recovery_preview(conflict TEXT)'));
 assert.throws(()=>migrateStorage(dir,'40'),{code:'migration_failed'});
 inspect(dir,db=>{assert.equal(db.pragma('user_version',{simple:true}),39);db.exec('DROP TABLE automation_recovery_preview');});
 assert.deepEqual(allOriginals(dir),before);
 assert.deepEqual(originals(dir).schema_migration,history);
 assert.deepEqual(migrateStorage(dir,'40'),{...identity,schema_revision:'40'});
 const after=allOriginals(dir);
 for(const [name,rows] of Object.entries(before))assert.deepEqual(after[name],rows,name);
 assert.deepEqual(after.automation_recovery_preview,[]);
 assert.deepEqual(originals(dir).schema_migration.slice(0,-1),history);
 inspect(dir,db=>assert.deepEqual(db.pragma('foreign_key_check'),[]));
});
test('Schema 41 启用恢复受理，末段失败时保留 Schema 40',t=>{
 const {dir,identity}=seeded(t);
 migrateStorage(dir,'40');
 const before=allOriginals(dir),history=originals(dir).schema_migration;
 inspect(dir,db=>db.exec('CREATE TABLE automation_recovery_lock(conflict TEXT)'));
 assert.throws(()=>migrateStorage(dir,'41'),{code:'migration_failed'});
 inspect(dir,db=>{assert.equal(db.pragma('user_version',{simple:true}),40);db.exec('DROP TABLE automation_recovery_lock');});
 assert.deepEqual(allOriginals(dir),before);
 assert.deepEqual(originals(dir).schema_migration,history);
 assert.deepEqual(migrateStorage(dir,'41'),{...identity,schema_revision:'41'});
 const after=allOriginals(dir);
 for(const [name,rows] of Object.entries(before))assert.deepEqual(after[name],rows,name);
 for(const name of ['automation_recovery_operation','automation_recovery_execution','automation_recovery_lock'])assert.deepEqual(after[name],[]);
 assert.deepEqual(originals(dir).schema_migration.slice(0,-1),history);
 inspect(dir,db=>assert.deepEqual(db.pragma('foreign_key_check'),[]));
});
test('Schema 42 迁移原子启用恢复准备并保留原件',t=>{
 const {dir,identity}=seeded(t);
 migrateStorage(dir,'41');
 const before=allOriginals(dir),history=originals(dir).schema_migration;
 inspect(dir,db=>db.exec('CREATE TABLE automation_recovery_preparation_receipt(conflict TEXT)'));
 assert.throws(()=>migrateStorage(dir,'42'),{code:'migration_failed'});
 inspect(dir,db=>{assert.equal(db.pragma('user_version',{simple:true}),41);db.exec('DROP TABLE automation_recovery_preparation_receipt');});
 assert.deepEqual(allOriginals(dir),before);
 assert.deepEqual(originals(dir).schema_migration,history);
 assert.deepEqual(migrateStorage(dir,'42'),{...identity,schema_revision:'42'});
 const after=allOriginals(dir);
 for(const [name,rows] of Object.entries(before))assert.deepEqual(after[name],rows,name);
 for(const suffix of ['', '_target','_attempt','_receipt'])assert.deepEqual(after['automation_recovery_preparation'+suffix],[]);
 assert.deepEqual(originals(dir).schema_migration.slice(0,-1),history);
 inspect(dir,db=>assert.deepEqual(db.pragma('foreign_key_check'),[]));
});
test('Schema 43 恢复执行绑定迁移失败时保留 Schema 42 和原件',t=>{
 const {dir,identity}=seeded(t);
 migrateStorage(dir,'42');
 const before=allOriginals(dir),history=originals(dir).schema_migration;
 inspect(dir,db=>db.exec('CREATE TABLE automation_recovery_execution_binding(conflict TEXT)'));
 assert.throws(()=>migrateStorage(dir,'43'),{code:'migration_failed'});
 inspect(dir,db=>{assert.equal(db.pragma('user_version',{simple:true}),42);db.exec('DROP TABLE automation_recovery_execution_binding');});
 assert.deepEqual(allOriginals(dir),before);
 assert.deepEqual(originals(dir).schema_migration,history);
 assert.deepEqual(migrateStorage(dir,'43'),{...identity,schema_revision:'43'});
 const after=allOriginals(dir);
 for(const [name,rows] of Object.entries(before))assert.deepEqual(after[name],rows,name);
 assert.deepEqual(after.automation_recovery_execution_binding,[]);
 assert.deepEqual(originals(dir).schema_migration.slice(0,-1),history);
 inspect(dir,db=>assert.deepEqual(db.pragma('foreign_key_check'),[]));
});
test('Schema 44 恢复开始迁移失败时保留 Schema 43 和原件',t=>{
 const {dir,identity}=seeded(t);
 migrateStorage(dir,'43');
 const before=allOriginals(dir),history=originals(dir).schema_migration;
 inspect(dir,db=>db.exec('CREATE TABLE automation_recovery_start(conflict TEXT)'));
 assert.throws(()=>migrateStorage(dir,'44'),{code:'migration_failed'});
 inspect(dir,db=>{assert.equal(db.pragma('user_version',{simple:true}),43);db.exec('DROP TABLE automation_recovery_start');});
 assert.deepEqual(allOriginals(dir),before);
 assert.deepEqual(originals(dir).schema_migration,history);
 assert.deepEqual(migrateStorage(dir,'44'),{...identity,schema_revision:'44'});
 const after=allOriginals(dir);
 for(const [name,rows] of Object.entries(before))assert.deepEqual(after[name],rows,name);
 assert.deepEqual(after.automation_recovery_start,[]);
 assert.deepEqual(originals(dir).schema_migration.slice(0,-1),history);
 inspect(dir,db=>assert.deepEqual(db.pragma('foreign_key_check'),[]));
});
test('Schema 45 恢复动作迁移失败时保留 Schema 44 和原件',t=>{
 const {dir,identity}=seeded(t);migrateStorage(dir,'44');
 const before=allOriginals(dir),history=originals(dir).schema_migration;
 inspect(dir,db=>db.exec('CREATE TABLE automation_recovery_dispatch_command_terminal(conflict TEXT)'));
 assert.throws(()=>migrateStorage(dir,'45'),{code:'migration_failed'});
 inspect(dir,db=>{assert.equal(db.pragma('user_version',{simple:true}),44);db.exec('DROP TABLE automation_recovery_dispatch_command_terminal');});
 assert.deepEqual(allOriginals(dir),before);
 assert.deepEqual(originals(dir).schema_migration,history);
 assert.deepEqual(migrateStorage(dir,'45'),{...identity,schema_revision:'45'});
 const after=allOriginals(dir);
 for(const [name,rows] of Object.entries(before))assert.deepEqual(after[name],rows,name);
 assert.deepEqual(after.automation_recovery_dispatch_submission,[]);
 assert.deepEqual(originals(dir).schema_migration.slice(0,-1),history);
 inspect(dir,db=>assert.deepEqual(db.pragma('foreign_key_check'),[]));
});
test('Schema 46 恢复延时迁移失败时保留 Schema 45 和原件',t=>{
 const {dir,identity}=seeded(t);migrateStorage(dir,'45');
 const before=allOriginals(dir),history=originals(dir).schema_migration;
 inspect(dir,db=>db.exec('CREATE TABLE automation_recovery_dispatch_delay_elapsed(conflict TEXT)'));
 assert.throws(()=>migrateStorage(dir,'46'),{code:'migration_failed'});
 inspect(dir,db=>{assert.equal(db.pragma('user_version',{simple:true}),45);db.exec('DROP TABLE automation_recovery_dispatch_delay_elapsed');});
 assert.deepEqual(allOriginals(dir),before);
 assert.deepEqual(originals(dir).schema_migration,history);
 assert.deepEqual(migrateStorage(dir,'46'),{...identity,schema_revision:'46'});
 const after=allOriginals(dir);
 for(const [name,rows] of Object.entries(before))assert.deepEqual(after[name],rows,name);
 for(const suffix of ['', '_elapsed'])assert.deepEqual(after['automation_recovery_dispatch_delay'+suffix],[]);
 assert.deepEqual(originals(dir).schema_migration.slice(0,-1),history);
 inspect(dir,db=>assert.deepEqual(db.pragma('foreign_key_check'),[]));
});
test('Schema 47 恢复结算迁移失败时保留 Schema 46 和原件',t=>{
 const {dir,identity}=seeded(t);migrateStorage(dir,'46');
 const before=allOriginals(dir),history=originals(dir).schema_migration;
 inspect(dir,db=>db.exec('CREATE TRIGGER automation_recovery_execution_commit_no_delete BEFORE DELETE ON automation_recovery_execution BEGIN SELECT 1; END'));
 assert.throws(()=>migrateStorage(dir,'47'),{code:'migration_failed'});
 inspect(dir,db=>{
  assert.equal(db.pragma('user_version',{simple:true}),46);
  db.exec('DROP TRIGGER automation_recovery_execution_commit_no_delete');
 });
 assert.deepEqual(allOriginals(dir),before);
 assert.deepEqual(originals(dir).schema_migration,history);
 assert.deepEqual(migrateStorage(dir,'47'),{...identity,schema_revision:'47'});
 const after=allOriginals(dir);
 for(const [name,rows] of Object.entries(before))assert.deepEqual(after[name],rows,name);
 assert.deepEqual(after.automation_recovery_execution_commit,[]);
 assert.deepEqual(originals(dir).schema_migration.slice(0,-1),history);
 inspect(dir,db=>assert.deepEqual(db.pragma('foreign_key_check'),[]));
});
test('Schema 48 恢复完成通知迁移原子增加回执并保留历史',t=>{
 const {dir,identity}=seeded(t);migrateStorage(dir,'47');
 const before=allOriginals(dir),history=originals(dir).schema_migration;
 inspect(dir,db=>db.exec('CREATE TABLE automation_recovery_execution_notification_receipt(conflict TEXT)'));
 assert.throws(()=>migrateStorage(dir,'48'),{code:'migration_failed'});
 inspect(dir,db=>{
  assert.equal(db.pragma('user_version',{simple:true}),47);
  db.exec('DROP TABLE automation_recovery_execution_notification_receipt');
 });
 assert.deepEqual(allOriginals(dir),before);
 assert.deepEqual(originals(dir).schema_migration,history);
 assert.deepEqual(migrateStorage(dir,'48'),{...identity,schema_revision:'48'});
 const after=allOriginals(dir);
 for(const [name,rows] of Object.entries(before))assert.deepEqual(after[name],rows,name);
 assert.deepEqual(after.automation_recovery_execution_notification_receipt,[]);
 assert.deepEqual(originals(dir).schema_migration.slice(0,-1),history);
 inspect(dir,db=>assert.deepEqual(db.pragma('foreign_key_check'),[]));
});
test('schema1直升4末段失败回滚所有中间版本，schema4摘要篡改拒绝打开',t=>{
 const {dir}=seeded(t);inspect(dir,db=>db.exec('CREATE TABLE snapshot_ingest_seal(sentinel TEXT) STRICT'));const before=allOriginals(dir),meta=originals(dir);
 assert.throws(()=>migrateStorage(dir,'4'),{code:'migration_failed'});assert.deepEqual(allOriginals(dir),before);assert.deepEqual(originals(dir),meta);inspect(dir,db=>db.exec('DROP TABLE snapshot_ingest_seal'));migrateStorage(dir,'4');
 inspect(dir,db=>db.exec("DROP TRIGGER schema_migration_no_update; UPDATE schema_migration SET script_digest='changed' WHERE to_revision='4'"));assert.throws(()=>openStorage(dir),{code:'migration_mismatch'});
});
