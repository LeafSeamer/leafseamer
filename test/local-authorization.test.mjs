import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import Database from 'better-sqlite3';
import {initializeStorage,migrateStorage,openStorage} from '../dist/storage.mjs';
import {startAuthenticationClock} from '../dist/runtime/auth-clock.mjs';
import {contractSelection as selection} from '../dist/sdk/index.mjs';
import {fixture as clockFixture} from './auth-clock-fixture.mjs';
import {seedLocalPermission} from './local-authorization-fixture.mjs';
import {source,digest} from './automation-program-fixture.mjs';
const id=()=>crypto.randomUUID(),permissions=['auth.manage','auth.read','request.create','request.read','definition.read','definition.edit'];
function fixture(t,{inject=null,managerExpires='18446744073709551615'}={}){
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'leafseamer-local-auth-')),service=initializeStorage(dir),issuer={service_id:id(),issuer_id:id(),issuer_epoch:id()},principal=id(),editor=id();
 seedLocalPermission(dir,service,issuer,principal,permissions,{expires_ns:managerExpires});
 if(inject){const db=new Database(path.join(dir,'seamer.sqlite'));try{db.exec(inject);}finally{db.close();}}
 const state={onIdentity:null},handles=new Map(),receiver={currentInTransaction(handle){state.onIdentity?.();assert.ok(handles.has(handle),'unknown handle');return handles.get(handle);}};
 let store,clock,f,authorization;
 function open(){store=openStorage(dir);f=clockFixture(store.authenticationClock.scope,null,{sampleWindowPolicy:'authentication-clock-sample/2'});clock=startAuthenticationClock(f.source,store.authenticationClock,f.provider);authorization=store.bindLocalAuthorization({clock,receiver,issuer});}
 function close(){clock.stop();store.close();}
 open();t.after(()=>{close();assert.equal(path.dirname(path.resolve(dir)),path.resolve(os.tmpdir()));fs.rmSync(dir,{recursive:true,force:true,maxRetries:5,retryDelay:50});});
 function handle(who=principal,scopes=permissions){const key={},now=clock.current(),stamp=ns=>new Date(Number(ns/1000000n)).toISOString(),snapshot={issuer_id:issuer.issuer_id,issuer_epoch:issuer.issuer_epoch,audience:service.service_id,principal_id:who,principal_kind:'human',session_id:id(),session_revision:'1',credential_id:id(),subject_revision:'1',policy_revision:'1',scope_digest:digest({who}),permissions:scopes,state:'active'};
  handles.set(key,{issuer_service_id:issuer.service_id,snapshot,token_binding_digest:digest(id()),recent_authentication:{session_id:snapshot.session_id,session_revision:'1',not_before:stamp(BigInt(now.lower_ns)-1000000000n),expires_at:stamp(BigInt(now.upper_ns)+60000000000n)}});return key;}
 const actor=handle(),editorHandle=handle(editor),request={request_id:id(),principal_id:editor,principal_kind:'human',expected_revision:null,permissions:['request.create','request.read','definition.read','definition.edit'],definition_scope:{kind:'all',definition_ids:[]},not_before_ns:'1',expires_ns:'18446744073709551615'};
 const makeContext=(h=editorHandle,max=4)=>authorization.createRequestContext(h,{audience:service.service_id,selection,operations:['createDefinition','getDraft','updateDraft','updateDefinitionMetadata'],ttl_ms:60000,max_unique_requests:max});
 const draft=c=>({context:{context_id:c.context_id,idempotency_key:id()},content:{program:source().program,selections:[],policy_ref:{kind:'policy',scope:{authority_service_id:service.service_id,database_instance_id:service.database_instance_id},object_id:'policy',revision:'1',digest:digest({})}},metadata:{name:'本地授权测试',description:'',layout:{}}});
 return {dir,service,issuer,principal,editor,actor,editorHandle,handles,handle,request,makeContext,draft,close,open,state,get store(){return store;},get auth(){return authorization;},advance:ns=>f.source.advance(ns)};
}
test('本地批准与签发方权限取交集，真实草稿同writer使用凭据绑定context和额度',t=>{
 const f=fixture(t);assert.throws(()=>f.makeContext(),{code:'local_authorization_missing'});
 const op=f.auth.replace(f.actor,f.request);assert.deepEqual(f.auth.replace(f.actor,f.request),op);
 const c=f.makeContext(f.editorHandle,1);f.advance(1000000n);const body=f.draft(c),created=f.store.drafts.create(f.editorHandle,body);
 assert.deepEqual(f.store.drafts.create(f.editorHandle,body),created);
 assert.deepEqual(f.store.drafts.get(f.editorHandle,created.definition_id,c.context_id),created);
 assert.throws(()=>f.store.drafts.create(f.editorHandle,f.draft(c)),{code:'local_authorization_context_capacity'});
 const changedToken={};f.handles.set(changedToken,{...f.handles.get(f.editorHandle),token_binding_digest:digest(id())});
 assert.throws(()=>f.store.drafts.get(changedToken,created.definition_id,c.context_id),{code:'local_authorization_context'});
 const readOnly=f.handle(f.editor,['request.create','request.read','definition.read']);assert.throws(()=>f.makeContext(readOnly),{code:'local_authorization_denied'});
 f.close();f.open();assert.throws(()=>f.store.drafts.get(f.editorHandle,created.definition_id,c.context_id),{code:'local_authorization_context'});
 assert.equal(f.auth.get(f.actor,f.editor).revision,'1');
});

test('草稿只读查原在换令牌及重启后使用原epoch，仍核本人及当前对象读取权',t=>{
 const f=fixture(t);f.auth.replace(f.actor,f.request);const c=f.makeContext();f.advance(1000000n);const body=f.draft(c),created=f.store.drafts.create(f.editorHandle,body);
 const query={...body.context,operation_id:'createDefinition'},another=f.handle(f.editor,['request.read','definition.read']);
 const first=f.store.drafts.lookup(another,query);assert.deepEqual(first.result,{kind:'DraftRecord',value:created});assert.equal(first.operation_id,'createDefinition');
 assert.throws(()=>f.store.drafts.lookup(f.handle(f.editor,['definition.read']),query),{code:'local_authorization_denied'});
 assert.throws(()=>f.store.drafts.lookup(f.actor,query),{code:'local_authorization_context'});
 assert.throws(()=>f.store.drafts.lookup(another,{...query,idempotency_key:id()}),{code:'draft_request_missing'});
 assert.deepEqual(f.store.drafts.lookup(another,query),first);
 f.close();f.open();assert.deepEqual(f.store.drafts.lookup(f.handle(f.editor,['request.read','definition.read']),query),first);
 assert.throws(()=>f.store.drafts.create(another,body));
 f.auth.replace(f.actor,{...f.request,request_id:id(),expected_revision:'1',permissions:['request.read']});
 assert.throws(()=>f.store.drafts.lookup(another,query),{code:'local_authorization_denied'});
});
test('对象范围和近期认证不能由有效身份或旧context替代，撤权不泄漏幂等结果',t=>{
 const f=fixture(t);f.auth.replace(f.actor,f.request);const c=f.makeContext();f.advance(1000000n);const body=f.draft(c),created=f.store.drafts.create(f.editorHandle,body);
 const restricted=id(),restrictedHandle=f.handle(restricted);f.auth.replace(f.actor,{...f.request,request_id:id(),principal_id:restricted,definition_scope:{kind:'selected',definition_ids:[created.definition_id]}});
 const limited=f.makeContext(restrictedHandle);f.advance(1000000n);assert.equal(f.store.drafts.get(restrictedHandle,created.definition_id,limited.context_id).definition_id,created.definition_id);
 assert.throws(()=>f.store.drafts.get(restrictedHandle,id(),limited.context_id),{code:'local_authorization_object'});
 assert.throws(()=>f.store.drafts.create(restrictedHandle,f.draft(limited)),{code:'local_authorization_object'});
 const stale={};f.handles.set(stale,{...f.handles.get(f.actor),recent_authentication:{...f.handles.get(f.actor).recent_authentication,expires_at:'2000-01-01T00:00:00.000Z'}});
 assert.throws(()=>f.auth.replace(stale,{...f.request,request_id:id()}),{code:'local_authorization_recent'});
 f.auth.replace(f.actor,{...f.request,request_id:id(),expected_revision:'1',permissions:['definition.read']});
 assert.equal(f.auth.get(f.actor,f.editor).state,'pending');assert.throws(()=>f.store.drafts.create(f.editorHandle,body),{code:'local_authorization_denied'});
 assert.throws(()=>f.store.drafts.get(f.editorHandle,created.definition_id,c.context_id),{code:'local_authorization_denied'});
 assert.throws(()=>f.auth.replace(f.actor,{...f.request,request_id:id(),expected_revision:'2'}),{code:'local_authorization_revision'});
});
test('批准逐写遗漏和草稿通知失败均回滚，不遗留授权主体或额度消费',t=>{
 for(const table of ['local_subject','local_permission_revision','local_permission_head','local_permission_operation','local_permission_outbox']){
  const f=fixture(t,{inject:'CREATE TRIGGER injected_failure BEFORE INSERT ON '+table+" BEGIN SELECT RAISE(IGNORE); END;"});
  assert.throws(()=>f.auth.replace(f.actor,f.request));assert.equal(f.auth.get(f.actor,f.editor),null);
 }
 const f=fixture(t,{inject:"CREATE TRIGGER injected_failure BEFORE INSERT ON definition_notification BEGIN SELECT RAISE(ABORT,'synthetic_write_failure'); END;"});
 f.auth.replace(f.actor,f.request);const c=f.makeContext();f.advance(1000000n);assert.throws(()=>f.store.drafts.create(f.editorHandle,f.draft(c)),/synthetic_write_failure/);
 f.close();const db=new Database(path.join(f.dir,'seamer.sqlite'));try{assert.equal(db.prepare('SELECT count(*) FROM local_context_request').pluck().get(),0);assert.equal(db.prepare('SELECT count(*) FROM definition_identity').pluck().get(),0);}finally{db.close();}f.open();
});
test('本地授权历史和通知不可拆分，CAS冲突不生成第二个批准',t=>{
 const f=fixture(t);f.auth.replace(f.actor,f.request);
 assert.throws(()=>f.auth.replace(f.actor,{...f.request,request_id:id()}),{code:'local_authorization_revision'});
 f.close();const db=new Database(path.join(f.dir,'seamer.sqlite'));try{
  db.exec('PRAGMA foreign_keys=ON');for(const table of ['local_subject','local_permission_revision','local_permission_head','local_permission_operation','local_permission_outbox'])assert.throws(()=>db.exec('DELETE FROM '+table));
  db.exec('DROP TRIGGER local_permission_outbox_no_delete; BEGIN IMMEDIATE; DELETE FROM local_permission_outbox');assert.throws(()=>db.exec('COMMIT'),/FOREIGN KEY/);db.exec('ROLLBACK');assert.deepEqual(db.pragma('foreign_key_check'),[]);
 }finally{db.close();}f.open();
});
test('管理者本地期限在提交末尾到期时回滚，服务身份不能借同名权限管理',t=>{
 const f=fixture(t,{managerExpires:'1700000000100000000'});let calls=0;
 f.state.onIdentity=()=>{if(++calls===2)f.advance(200000000n);};
 assert.throws(()=>f.auth.replace(f.actor,f.request),{code:'local_authorization_changed'});f.state.onIdentity=null;
 f.close();const db=new Database(path.join(f.dir,'seamer.sqlite'));try{
  assert.equal(db.prepare('SELECT count(*) FROM local_subject').pluck().get(),1);assert.equal(db.prepare('SELECT count(*) FROM local_permission_operation').pluck().get(),1);
 }finally{db.close();}f.open();
 const servicePrincipal=id();f.auth.replace(f.actor,{...f.request,request_id:id(),principal_id:servicePrincipal,principal_kind:'service',permissions:['auth.manage']});
 const serviceHandle=f.handle(servicePrincipal),original=f.handles.get(serviceHandle);
 f.handles.set(serviceHandle,{...original,snapshot:{...original.snapshot,principal_kind:'service',session_id:null,session_revision:null},recent_authentication:null});
 assert.throws(()=>f.auth.replace(serviceHandle,f.request),{code:'local_authorization_recent'});
});

test('发布权限取远端和本地交集，服务主体不能获准发布，缺来源不消耗额度',async t=>{
 const f=fixture(t);f.close();migrateStorage(f.dir,'2');f.open();
 const publishPermissions=[...f.request.permissions,'definition.publish'];
 assert.throws(()=>f.auth.replace(f.actor,{...f.request,request_id:id(),principal_id:id(),principal_kind:'service',permissions:publishPermissions}),{code:'local_authorization_human'});
 f.auth.replace(f.actor,{...f.request,permissions:publishPermissions});
 const req={audience:f.service.service_id,selection,operations:['publishDefinition'],ttl_ms:1000,max_unique_requests:1};
 assert.throws(()=>f.auth.createRequestContext(f.editorHandle,req),{code:'local_authorization_denied'});
 const handle=f.handle(f.editor,publishPermissions),context=f.auth.createRequestContext(handle,req),draftContext=f.makeContext(handle);f.advance(1000000n);
 const created=f.store.drafts.create(handle,f.draft(draftContext));
 await assert.rejects(f.store.publications.publish(handle,created.definition_id,{context:{context_id:context.context_id,idempotency_key:id()},expected_draft_revision:created.revision,expected_content_digest:created.content_digest,expected_selection_digest:created.selection_digest}),{code:'publication_source_unavailable'});
 f.close();const db=new Database(path.join(f.dir,'seamer.sqlite'));try{assert.equal(db.prepare('SELECT count(*) FROM local_context_request WHERE context_id=?').pluck().get(context.context_id),0);assert.equal(db.prepare('SELECT count(*) FROM definition_version_reservation').pluck().get(),0);}finally{db.close();}f.open();
});
