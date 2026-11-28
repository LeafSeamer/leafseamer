import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';
import {initializeStorage,migrateStorage,openStorage,prepareProtectedRecoveryStorage} from '../dist/storage.mjs';
import {source,digest} from './automation-program-fixture.mjs';
const tables=['definition_identity','definition_content','definition_metadata','definition_head','definition_request','definition_notification'];
function directory(t){const dir=fs.mkdtempSync(path.join(os.tmpdir(),'leafseamer-draft-'));t.after(()=>{assert.equal(path.dirname(path.resolve(dir)),path.resolve(os.tmpdir()));fs.rmSync(dir,{recursive:true,force:true,maxRetries:5,retryDelay:50});});return dir;}
function provider(identity){
 const state={denied:false,denyRead:false,calls:0,changeAt:0};
 return {state,authorizeInTransaction({authentication,operation_id,definition_id,context_id,scope}){
  state.calls++;if(state.denied||authentication!=='fixture-auth'||state.denyRead&&operation_id==='getDraft')throw new Error('fixture_denied');
  return {scope,actor:{issuer_id:'controller',issuer_epoch:'issuer-1',principal_id:'fixture-person',principal_kind:'human'},context_id,request_epoch:identity.request_epoch,operation_id,definition_id,authorization_digest:digest({revision:state.changeAt&&state.calls>=state.changeAt?2:1})};
 }};
}
function request(identity,key='create'){
 const value=source();return {context:{context_id:'context-1',idempotency_key:key},content:{program:value.program,selections:[],policy_ref:{kind:'policy',scope:{authority_service_id:identity.service_id,database_instance_id:identity.database_instance_id},object_id:'policy',revision:'1',digest:digest({})}},metadata:{name:'测试定义',description:'',layout:{}}};
}
function count(dir){const db=new Database(path.join(dir,'seamer.sqlite'));try{return Object.fromEntries(tables.map(name=>[name,db.prepare('SELECT count(*) FROM '+name).pluck().get()]));}finally{db.close();}}

test('prepares protected recovery storage from a verified snapshot with draft provenance and a new identity',async t=>{
 const old=directory(t),target=directory(t),rejected=directory(t);
 initializeStorage(old);
 const identity=migrateStorage(old,'48'),body=request(identity);
 const runtime=openStorage(old,{draftAuthority:provider(identity)});
 let updated;
 try{
  const created=runtime.drafts.create('fixture-auth',body);
  const edit=structuredClone(body.content);edit.program.groups[0].steps[0].parameters.value=true;
  updated=runtime.drafts.update('fixture-auth',created.definition_id,{context:{context_id:'context-1',idempotency_key:'edit-recovery'},
   expected_draft_revision:'1',content:edit});
 }finally{runtime.close();}
 const snapshot=path.join(old,'snapshot.sqlite'),original=new Database(path.join(old,'seamer.sqlite'),{readonly:true});
 try{await original.backup(snapshot);}finally{original.close();}
 const archived=new Database(snapshot);
 try{assert.equal(archived.pragma('journal_mode=DELETE',{simple:true}),'delete');}finally{archived.close();}
 const scope={authority:'seamer',service_id:identity.service_id,database_instance_id:identity.database_instance_id,
  request_epoch:identity.request_epoch,control_epoch:null,schema_revision:'48'};
 const contents=fs.readFileSync(snapshot),receipt={state:'staged',engine:'3.53.4',backup_operation_id:crypto.randomUUID(),scope,
  bytes:contents.length,sha256:crypto.createHash('sha256').update(contents).digest('hex')};
 const nextScope={...scope,database_instance_id:crypto.randomUUID(),request_epoch:crypto.randomUUID()};
 const input={sourceFile:snapshot,receipt,targetDirectory:target,nextScope,planDigest:digest('approved plan'),dataGenerationId:crypto.randomUUID()};
 assert.throws(()=>prepareProtectedRecoveryStorage({...input,targetDirectory:rejected,nextScope:{...nextScope,request_epoch:scope.request_epoch}}),
  {code:'recovery_storage_scope'});
 assert.throws(()=>prepareProtectedRecoveryStorage({...input,targetDirectory:rejected,receipt:{...receipt,sha256:'0'.repeat(64)}}),
  {code:'backup_snapshot_digest'});
 const damaged=path.join(old,'invalid-draft.sqlite');
 fs.copyFileSync(snapshot,damaged);
 const damagedDb=new Database(damaged);
 try{damagedDb.exec("DROP TRIGGER definition_content_no_update; UPDATE definition_content SET content_jcs='{}' WHERE revision='2'");}
 finally{damagedDb.close();}
 const damagedBytes=fs.readFileSync(damaged);
 assert.throws(()=>prepareProtectedRecoveryStorage({...input,sourceFile:damaged,targetDirectory:rejected,
  receipt:{...receipt,bytes:damagedBytes.length,sha256:crypto.createHash('sha256').update(damagedBytes).digest('hex')}}),
 {code:'recovery_storage_draft_invalid'});
 assert.deepEqual(fs.readdirSync(rejected),[]);
 const preparation=prepareProtectedRecoveryStorage(input),file=path.join(target,'seamer.sqlite');
 assert.deepEqual(preparation.next_scope,nextScope);
 assert.equal(preparation.draft_count,1);
 assert.equal(preparation.draft_review_required,true);
 assert.equal(preparation.origin.plan_digest,input.planDigest);
 assert.equal(preparation.origin.snapshot_sha256,receipt.sha256);
 const prepared=fs.readFileSync(file);
 assert.equal(preparation.sha256,crypto.createHash('sha256').update(prepared).digest('hex'));
 assert.equal(preparation.bytes,prepared.length);
 assert.deepEqual(fs.readdirSync(target),['seamer.sqlite']);
 assert.deepEqual(fs.readFileSync(snapshot),contents);
 const db=new Database(file,{readonly:true,fileMustExist:true});
 try{
  for(const table of ['service_boot','definition_publication','peer_pairing_revision','local_permission_head','automation_execution','schedule_version'])
   assert.equal(db.prepare(`SELECT count(*) FROM ${table}`).pluck().get(),0,table);
  const event=JSON.parse(db.prepare('SELECT body_jcs FROM definition_notification').pluck().get());
  assert.equal(event.kind,'definitionRestored');
  assert.equal(event.provenance.draft_revision,'2');
  assert.equal(event.provenance.content_digest,updated.content_digest);
  assert.deepEqual(db.pragma('foreign_key_check'),[]);
 }finally{db.close();}
 assert.throws(()=>prepareProtectedRecoveryStorage(input),{code:'recovery_storage_destination'});
 assert.deepEqual(fs.readFileSync(file),prepared);
 const restored=openStorage(target,{draftAuthority:provider(nextScope),expectedIdentity:nextScope});
 try{
  const draft=restored.drafts.get('fixture-auth',updated.definition_id);
  assert.deepEqual(draft,{...updated,revision:'1',metadata_revision:'1',
   scope:{authority_service_id:scope.service_id,database_instance_id:nextScope.database_instance_id}});
 }finally{restored.close();}
});

test('draft content, metadata and original results survive restart with independent revisions',t=>{
 const dir=directory(t),identity=initializeStorage(dir),authority=provider(identity),body=request(identity);let store=openStorage(dir,{draftAuthority:authority});
 const created=store.drafts.create('fixture-auth',body),id=created.definition_id;
 assert.equal(created.revision,'1');assert.equal(created.metadata_revision,'1');assert.equal(created.content_digest,digest(body.content));assert.equal(created.selection_digest,digest([]));
 const updated=store.drafts.updateMetadata('fixture-auth',id,{context:{context_id:'context-1',idempotency_key:'metadata'},expected_metadata_revision:'1',metadata:{...body.metadata,name:'改名',layout:{context:{preserved:true}}}});
 assert.equal(updated.revision,'1');assert.equal(updated.metadata_revision,'2');assert.equal(updated.content_digest,created.content_digest);
 const content=structuredClone(body.content);content.program.groups[0].steps[0].parameters.value=true;
 const changed=store.drafts.update('fixture-auth',id,{context:{context_id:'context-1',idempotency_key:'edit'},expected_draft_revision:'1',content});
 assert.equal(changed.revision,'2');assert.equal(changed.metadata_revision,'2');assert.notEqual(changed.content_digest,created.content_digest);
 store.close();store=openStorage(dir,{draftAuthority:authority});
 try{
  assert.deepEqual(store.drafts.get('fixture-auth',id,'context-2'),changed);
  assert.deepEqual(store.drafts.create('fixture-auth',{...body,context:{context_id:'context-2',idempotency_key:'create'}}),created);
  assert.throws(()=>store.drafts.update('fixture-auth',id,{context:{context_id:'context-2',idempotency_key:'loser'},expected_draft_revision:'1',content}),{code:'draft_revision_conflict'});
 }finally{store.close();}
 assert.deepEqual(count(dir),{definition_identity:1,definition_content:2,definition_metadata:2,definition_head:1,definition_request:3,definition_notification:3});
});
test('idempotency binds nested content and checks current read authority before replay',t=>{
 const dir=directory(t),identity=initializeStorage(dir),authority=provider(identity),store=openStorage(dir,{draftAuthority:authority}),body=request(identity);
 try{
  const created=store.drafts.create('fixture-auth',body);const changed=structuredClone(body);changed.metadata.layout={context:{idempotency_key:'ordinary-payload'}};
  assert.throws(()=>store.drafts.create('fixture-auth',changed),{code:'draft_idempotency_conflict'});
  authority.state.denyRead=true;assert.throws(()=>store.drafts.create('fixture-auth',body),/fixture_denied/);authority.state.denyRead=false;
  authority.state.denied=true;assert.throws(()=>store.drafts.get('fixture-auth',created.definition_id,'context-1'),/fixture_denied/);authority.state.denied=false;
  assert.deepEqual(store.drafts.create('fixture-auth',body),created);
 }finally{store.close();}
 assert.equal(count(dir).definition_request,1);
});
test('authority changes at transaction end roll back all writes and leave the key reusable',t=>{
 const dir=directory(t),identity=initializeStorage(dir),authority=provider(identity),body=request(identity);authority.state.changeAt=2;
 let store=openStorage(dir,{draftAuthority:authority});assert.throws(()=>store.drafts.create('fixture-auth',body),{code:'draft_authorization_changed'});store.close();
 assert.ok(Object.values(count(dir)).every(value=>value===0));authority.state.changeAt=0;store=openStorage(dir,{draftAuthority:authority});
 try{assert.equal(store.drafts.create('fixture-auth',body).revision,'1');}finally{store.close();}
});
test('each durable write failure rolls back identity, revisions, result and notification together',t=>{
 for(const table of tables){
  const dir=directory(t),identity=initializeStorage(dir),authority=provider(identity),db=new Database(path.join(dir,'seamer.sqlite'));
  db.exec("CREATE TRIGGER injected_failure BEFORE INSERT ON "+table+" BEGIN SELECT RAISE(ABORT,'injected_failure'); END");db.close();
  const store=openStorage(dir,{draftAuthority:authority});try{assert.throws(()=>store.drafts.create('fixture-auth',request(identity)),/injected_failure/);}finally{store.close();}
  assert.ok(Object.values(count(dir)).every(value=>value===0),table);
 }
});
test('missing authority, malformed requests and duplicate selections fail closed',t=>{
 const dir=directory(t),identity=initializeStorage(dir),body=request(identity);let store=openStorage(dir);
 assert.throws(()=>store.drafts.create('fixture-auth',body),{code:'draft_authority_unavailable'});store.close();
 store=openStorage(dir,{draftAuthority:provider(identity)});
 try{
  assert.throws(()=>store.drafts.create('fixture-auth',{...body,approved:true}),{code:'automation_schema'});
  const selected={binding_id:'a',kind:'action',content_ref:{id:'binding',revision:'1',digest:digest({})}};
  assert.throws(()=>store.drafts.create('fixture-auth',{...body,content:{...body.content,selections:[selected,selected]}}),{code:'draft_duplicate_selection'});
 }finally{store.close();}
 assert.ok(Object.values(count(dir)).every(value=>value===0));
});
test('history is immutable and deferred pairing rejects a missing notification',t=>{
 const dir=directory(t),identity=initializeStorage(dir),store=openStorage(dir,{draftAuthority:provider(identity)});store.drafts.create('fixture-auth',request(identity));store.close();
 const db=new Database(path.join(dir,'seamer.sqlite'));try{
  db.exec('PRAGMA foreign_keys=ON');
  for(const table of tables)assert.throws(()=>db.exec('DELETE FROM '+table));
  assert.throws(()=>db.exec("UPDATE definition_content SET content_jcs='{}'"));
  assert.throws(()=>db.exec("UPDATE definition_metadata SET metadata_jcs='{}'"));
  assert.throws(()=>db.exec("UPDATE definition_head SET content_revision='99'"));
  db.exec('DROP TRIGGER definition_notification_no_delete; BEGIN IMMEDIATE; DELETE FROM definition_notification');assert.throws(()=>db.exec('COMMIT'),/FOREIGN KEY/);db.exec('ROLLBACK');
  assert.equal(db.prepare('SELECT count(*) FROM definition_notification').pluck().get(),1);assert.deepEqual(db.prepare('PRAGMA foreign_key_check').raw().all(),[]);
 }finally{db.close();}
});

test('authority scope, request epoch and synchronous writer boundary cannot be substituted',t=>{
 const dir=directory(t),identity=initializeStorage(dir),base=provider(identity),body=request(identity);
 for(const change of [proof=>({...proof,scope:{...proof.scope,database_instance_id:'another-database'}}),proof=>({...proof,request_epoch:'old-epoch'}),proof=>Promise.resolve(proof)]){
  const authority={authorizeInTransaction(input){return change(base.authorizeInTransaction(input));}},store=openStorage(dir,{draftAuthority:authority});
  try{assert.throws(()=>store.drafts.create('fixture-auth',body),error=>['draft_authorization','draft_authorization_binding'].includes(error.code));}finally{store.close();}
 }
 assert.ok(Object.values(count(dir)).every(value=>value===0));
});
