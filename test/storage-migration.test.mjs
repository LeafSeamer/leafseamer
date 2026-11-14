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
 assert.throws(()=>migrateStorage(dir,'5'),e=>e.code==='migration_target');
 const after=originals(dir);assert.deepEqual(after.service_state,before.service_state);assert.deepEqual(after.schema_migration,before.schema_migration);
 const opened=openStorage(dir);try{assert.equal(opened.identity.schema_revision,'1');}finally{opened.close();}
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
test('schema1直升4末段失败回滚所有中间版本，schema4摘要篡改拒绝打开',t=>{
 const {dir}=seeded(t);inspect(dir,db=>db.exec('CREATE TABLE snapshot_ingest_seal(sentinel TEXT) STRICT'));const before=allOriginals(dir),meta=originals(dir);
 assert.throws(()=>migrateStorage(dir,'4'),{code:'migration_failed'});assert.deepEqual(allOriginals(dir),before);assert.deepEqual(originals(dir),meta);inspect(dir,db=>db.exec('DROP TABLE snapshot_ingest_seal'));migrateStorage(dir,'4');
 inspect(dir,db=>db.exec("DROP TRIGGER schema_migration_no_update; UPDATE schema_migration SET script_digest='changed' WHERE to_revision='4'"));assert.throws(()=>openStorage(dir),{code:'migration_mismatch'});
});
