import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import https from 'node:https';
import {pathToFileURL} from 'node:url';
import Database from 'better-sqlite3';
import canonicalize from 'canonicalize';
import {peerCertificates} from '../../leafseamer-controller/test/fixtures/peer-certificates.mjs';
import {packageRoot,publicationMaterial} from './automation-publication-fixture.mjs';
import {fixture as clockFixture} from './auth-clock-fixture.mjs';
import {seedLocalPermission} from './local-authorization-fixture.mjs';
import {source,digest} from './automation-program-fixture.mjs';
const load=name=>import(pathToFileURL(path.join(packageRoot,'dist',name)));
const [{initializeStorage,migrateStorage,openStorage},{startAuthenticationClock},{createClient,contractSelection:selection},{startDraftHttp}]=
 await Promise.all(['storage.mjs','runtime/auth-clock.mjs','sdk/index.mjs','draft-http.mjs'].map(load));
const id=()=>crypto.randomUUID(),permissions=['auth.manage','auth.read','request.create','request.read','definition.read','definition.edit'];
function fixture(t,{inject=null,managerExpires='18446744073709551615',managerPermissions=permissions,revision='1',publicationSource=null,schedule=true}={}){
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'leafseamer-local-auth-')),service=initializeStorage(dir),issuer={service_id:id(),issuer_id:id(),issuer_epoch:id()},principal=id(),editor=id();
 if(revision!=='1')migrateStorage(dir,revision);
 seedLocalPermission(dir,service,issuer,principal,managerPermissions,{expires_ns:managerExpires});
 if(inject){const db=new Database(path.join(dir,'seamer.sqlite'));try{db.exec(inject);}finally{db.close();}}
 const state={onIdentity:null},handles=new Map(),receiver={currentInTransaction(handle){state.onIdentity?.();assert.ok(handles.has(handle),'unknown handle');return handles.get(handle);}};
 let store,clock,f,authorization;
 function open(){store=openStorage(dir,{publicationSource});f=clockFixture(store.authenticationClock.scope,null,{sampleWindowPolicy:'authentication-clock-sample/2'});clock=startAuthenticationClock(f.source,store.authenticationClock,f.provider,{schedule});authorization=store.bindLocalAuthorization({clock,receiver,issuer});}
 function close(){clock.stop();store.close();}
 open();t.after(()=>{close();assert.equal(path.dirname(path.resolve(dir)),path.resolve(os.tmpdir()));fs.rmSync(dir,{recursive:true,force:true,maxRetries:5,retryDelay:50});});
 function handle(who=principal,scopes=permissions,kind='human'){const key={},now=clock.current(),stamp=ns=>new Date(Number(ns/1000000n)).toISOString(),snapshot={issuer_id:issuer.issuer_id,issuer_epoch:issuer.issuer_epoch,audience:service.service_id,principal_id:who,principal_kind:kind,session_id:id(),session_revision:'1',credential_id:id(),subject_revision:'1',policy_revision:'1',scope_digest:digest({who}),permissions:scopes,state:'active'};
  handles.set(key,{issuer_service_id:issuer.service_id,snapshot,token_binding_digest:digest(id()),recent_authentication:{session_id:snapshot.session_id,session_revision:'1',not_before:stamp(BigInt(now.lower_ns)-1000000000n),expires_at:stamp(BigInt(now.upper_ns)+60000000000n)}});return key;}
 const actor=handle(),editorHandle=handle(editor),request={request_id:id(),principal_id:editor,principal_kind:'human',expected_revision:null,permissions:['request.create','request.read','definition.read','definition.edit'],definition_scope:{kind:'all',definition_ids:[]},not_before_ns:'1',expires_ns:'18446744073709551615'};
 const makeContext=(h=editorHandle,max=4)=>authorization.createRequestContext(h,{audience:service.service_id,selection,operations:['createDefinition','getDraft','updateDraft','updateDefinitionMetadata'],ttl_ms:60000,max_unique_requests:max});
 const draft=c=>({context:{context_id:c.context_id,idempotency_key:id()},content:{program:source().program,selections:[],policy_ref:{kind:'policy',scope:{authority_service_id:service.service_id,database_instance_id:service.database_instance_id},object_id:'policy',revision:'1',digest:digest({})}},metadata:{name:'本地授权测试',description:'',layout:{}}});
 return {dir,service,issuer,principal,editor,actor,editorHandle,handles,handle,request,makeContext,draft,close,open,state,get store(){return store;},get auth(){return authorization;},advance:ns=>f.source.advance(ns)};
}
test('恢复上下文要求人员、当前签发方权限、本地授权及近期认证',t=>{
 const scopes=[...permissions,'run.read','run.resume'];
 const f=fixture(t,{revision:'41',managerPermissions:scopes,schedule:false});
 const operator=f.handle(f.principal,scopes);
 const options={ttl_ms:30000,max_unique_requests:1};
 assert.throws(()=>f.auth.createResumeRunContext(f.actor,options),{code:'local_authorization_denied'});
 assert.throws(()=>f.auth.replace(operator,{...f.request,principal_id:id(),principal_kind:'service',permissions:['run.resume']}),
  {code:'local_authorization_human'});
 f.auth.replace(operator,{...f.request,permissions:['request.create','request.read','run.read']});
 assert.throws(()=>f.auth.createResumeRunContext(f.handle(f.editor,scopes),options),{code:'local_authorization_denied'});
 const stale=f.handle(f.principal,scopes);
 f.handles.get(stale).recent_authentication.expires_at='2000-01-01T00:00:00.000Z';
 assert.throws(()=>f.auth.createResumeRunContext(stale,options),{code:'local_authorization_recent'});
 const context=f.auth.createResumeRunContext(operator,options);
 f.advance(1000000n);
 assert.deepEqual(context.operations,['resumeRun']);
 const input={authentication:operator,operation_id:'resumeRun',definition_id:id(),context_id:context.context_id};
 assert.equal(f.auth.confirmDraftResult(input).actor.principal_kind,'human');
 assert.throws(()=>f.auth.confirmDraftResult({...input,authentication:f.handle(f.principal,scopes)}),{code:'local_authorization_context'});
});
test('HTTPS 登记与查回未布防运行，固定发布制品及本地接收基线',async t=>{
 const scopes=[...permissions,'definition.publish','run.manage','run.read'];
 let material;
 const f=fixture(t,{revision:'5',managerPermissions:scopes,schedule:false,publicationSource:{
  async resolve(){return structuredClone(material.snapshot);},
  assertCurrentInTransaction(){return digest(material.snapshot);}
 }});
 const actor=f.handle(f.principal,scopes);
 const scope={authority_service_id:f.service.service_id,database_instance_id:f.service.database_instance_id};
 material=publicationMaterial(scope);
 const prepare=(handle,operations,max=4)=>f.auth.createRequestContext(handle,{audience:f.service.service_id,selection,operations,ttl_ms:60000,max_unique_requests:max});
 const draftContext=prepare(actor,['createDefinition','publishDefinition']);f.advance(1000000n);
 const draft=f.store.drafts.create(actor,{context:{context_id:draftContext.context_id,idempotency_key:id()},content:material.content,metadata:{name:'测试运行登记',description:'',layout:{}}});
 const published=await f.store.publications.publish(actor,draft.definition_id,{context:{context_id:draftContext.context_id,idempotency_key:id()},
  expected_draft_revision:draft.revision,expected_content_digest:draft.content_digest,expected_selection_digest:draft.selection_digest});
 const servicePrincipal=id(),serviceScopes=['request.create','run.manage','run.read'];
 f.auth.replace(actor,{...f.request,request_id:id(),principal_id:servicePrincipal,principal_kind:'service',permissions:serviceScopes,definition_scope:{kind:'selected',definition_ids:[draft.definition_id]}});
 const serviceActor=f.handle(servicePrincipal,serviceScopes,'service');
 const certs=peerCertificates(),page='https://controller.invalid',humanToken='v1.'+id()+'.'+'A'.repeat(43),serviceToken='v1.'+id()+'.'+'B'.repeat(43);
 const handles=new Map([[humanToken,actor],[serviceToken,serviceActor]]);
 const diagnostic=[];
 const authorization={...f.auth,createRequestContext(...args){
  const value=f.auth.createRequestContext(...args);f.advance(1000000n);return value;
 }};
 const app=await startDraftHttp({cert:certs.leaves[0].cert,key:certs.leaves[0].key,bindingId:id(),service:f.store.identity,bootId:f.store.serviceBootId,
  receiver:{async prepare({token}){return handles.get(token);}},authorization,drafts:f.store.drafts,publications:f.store.publications,runs:f.store.runs,allowedOrigins:[page],
  onFailure(value){diagnostic.push(value);}});
 t.after(()=>app.close());
 let headersRead=[],token=humanToken,origin=page,csrf=null;
 const client=createClient('seamer',selection,packet=>new Promise((resolve,reject)=>{
  const headers=Object.fromEntries([...packet.headers,['Authorization','Bearer '+token],...(origin?[['Origin',origin]]:[]),...(csrf?[['X-CSRF-Token',csrf]]:[])]);
  const req=https.request(new URL(packet.target,app.origin),{method:packet.method,headers,ca:certs.ca,agent:false},res=>{
   const chunks=[];res.on('data',chunk=>chunks.push(chunk));res.on('end',()=>{
    headersRead=Array.from({length:res.rawHeaders.length/2},(_,i)=>[res.rawHeaders[i*2],res.rawHeaders[i*2+1]]);
    resolve({status:String(res.statusCode),headers:headersRead,body:Buffer.concat(chunks)});
   });
  });req.on('error',reject);req.end(packet.body);
 }));
 const issue=(operations=['createRun'])=>client.call('createRequestContext',{body:{audience:f.service.service_id,selection,operations,ttl_ms:30000,max_unique_requests:1}});
 const issued=await issue(['createRun','updateDefinitionMetadata']);assert.equal(issued.status,'201',JSON.stringify(diagnostic));
 const ctx=issued.body.data,ownCsrf=headersRead.find(([name])=>name.toLowerCase()==='x-csrf-token')?.[1];f.advance(1000000n);
 const input={context:{context_id:ctx.context_id,idempotency_key:id()},definition_id:draft.definition_id,definition_version:published.definition_version,expected_artifact_ref:published.artifact_ref};
 const create=(body=input)=>client.call('createRun',{body});
 assert.equal((await create()).status,'403');
 csrf=ownCsrf;
 const {status,body:run}=await create();
 assert.equal(status,'201',JSON.stringify(diagnostic));assert.equal(run.lifecycle,'disarmed');assert.equal(run.dispatch_eligible,false);
 assert.equal(run.arm_generation,'0');assert.equal(run.last_receive_seq,'0');assert.equal(run.active_execution_id,null);
 assert.deepEqual(run.artifact_ref,published.artifact_ref);
 assert.equal(run.dependency_digest,undefined);
 const dependencies=f.store.runs.dependencies(actor,run.run_id);
 assert.deepEqual(dependencies.body.artifact_ref,published.artifact_ref);
 assert.equal(dependencies.body.program_digest,published.program_digest);
 assert.equal(dependencies.body.dependencies.inputs[0].binding_id,'trigger');
 assert.deepEqual(dependencies.body.dependencies.inputs[0].uses,['trigger']);
 assert.equal(dependencies.body.dependencies.actions[0].binding_id,'action');
 const actorWithoutRunRead=f.handle(f.principal,scopes.filter(permission=>permission!=='run.read'));
 assert.throws(()=>f.store.runs.dependencies(actorWithoutRunRead,run.run_id),{code:'local_authorization_denied'});
 const duplicate=await create();assert.equal(duplicate.status,'201',JSON.stringify(diagnostic));assert.deepEqual(duplicate.body,run);
 assert.equal((await create({...input,expected_artifact_ref:{...input.expected_artifact_ref,digest:digest('wrong')}})).status,'409');
 assert.equal((await create({...input,context:{...input.context,idempotency_key:id()}})).status,'429');
 assert.throws(()=>f.store.drafts.updateMetadata(actor,draft.definition_id,{
  context:{context_id:ctx.context_id,idempotency_key:id()},expected_metadata_revision:draft.metadata_revision,metadata:{...draft.metadata,name:'额度已占用'}
 }),{code:'local_authorization_context_capacity'});
 const get=run_id=>client.call('getRun',{path:{run_id}});
 assert.deepEqual((await get(run.run_id)).body,run);
 const lookup={...input.context,operation_id:'createRun'};
 const recovered=await client.call('lookupAutomationRequest',{body:lookup});
 assert.equal(recovered.status,'200');assert.deepEqual(recovered.body.result,{kind:'RunRecord',value:run});
 assert.equal(recovered.body.request_digest,digest({version:'automation-lifecycle/0.1.0-draft.1',operation_id:'createRun',definition_id:draft.definition_id,
  request:{definition_version:published.definition_version,expected_artifact_ref:published.artifact_ref}}));
 token=serviceToken;origin=null;csrf=null;
 const serviceContext=(await issue()).body.data;f.advance(1000000n);
 const outside={...input,definition_id:id(),context:{context_id:serviceContext.context_id,idempotency_key:id()}};
 assert.equal((await create(outside)).status,'404');
 const serviceRun=await create({...input,context:{context_id:serviceContext.context_id,idempotency_key:input.context.idempotency_key}});
 assert.equal(serviceRun.status,'201');assert.equal(serviceRun.body.dispatch_eligible,false);assert.notEqual(serviceRun.body.run_id,run.run_id);
 assert.deepEqual((await get(serviceRun.body.run_id)).body,serviceRun.body);
 assert.deepEqual(f.store.runs.dependencies(serviceActor,serviceRun.body.run_id),dependencies);
 assert.equal((await client.call('lookupAutomationRequest',{body:lookup})).status,'403');
 await app.close();f.close();assert.equal(migrateStorage(f.dir,'6').schema_revision,'6');f.open();
 assert.deepEqual(f.store.runs.get(actor,run.run_id),run);
 assert.deepEqual(f.store.runs.dependencies(actor,run.run_id),dependencies);
 assert.deepEqual(f.store.runs.lookup(actor,lookup),recovered.body);
 assert.throws(()=>f.store.runs.create(actor,input),{code:'local_authorization_context'});
 f.close();
 const db=new Database(path.join(f.dir,'seamer.sqlite'));
 try{
  const rows=db.prepare('SELECT body_jcs FROM automation_run_creation').pluck().all().map(JSON.parse);
  assert.equal(rows.length,2);
  assert.equal(db.prepare('SELECT count(*) FROM definition_program_dependencies').pluck().get(),1);
  assert.equal(db.prepare('SELECT count(*) FROM automation_run_approval_proposal').pluck().get(),0);
  assert.throws(()=>db.prepare('UPDATE definition_program_dependencies SET body_digest=?').run(digest('changed')),/immutable_program_dependencies/);
  assert.throws(()=>db.prepare('DELETE FROM definition_program_dependencies').run(),/immutable_program_dependencies/);
  assert.ok(rows.every(row=>row.run.dependency_digest===dependencies.digest));
  assert.ok(rows.every(row=>row.run.last_receive_cursor.receive_sequence==='0'&&row.run.last_receive_seq===undefined));
  assert.ok(rows.every(row=>row.run.last_receive_cursor.ingest_epoch===db.prepare('SELECT ingest_epoch FROM event_ingest_epoch').pluck().get()));
 }finally{db.close();}
});

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

test('本机排期发布和激活在同一服务库内固定步骤窗口与逻辑场次',async t=>{
 const scopes=[...permissions,'definition.publish','schedule.read','schedule.publish','schedule.activate'];
 let material;
 const f=fixture(t,{revision:'35',managerPermissions:scopes,schedule:false,publicationSource:{
  async resolve(){return structuredClone(material.snapshot);},
  assertCurrentInTransaction(){return digest(material.snapshot);}
 }});
 const firstActor=f.handle(f.principal,scopes);
 const scope={authority_service_id:f.service.service_id,database_instance_id:f.service.database_instance_id};
 material=publicationMaterial(scope);
 const context=f.auth.createRequestContext(firstActor,{audience:f.service.service_id,selection,operations:['createDefinition','publishDefinition'],
  ttl_ms:60000,max_unique_requests:4});f.advance(1000000n);
 const draft=f.store.drafts.create(firstActor,{context:{context_id:context.context_id,idempotency_key:id()},content:material.content,metadata:{name:'排期测试',description:'',layout:{}}});
 const published=await f.store.publications.publish(firstActor,draft.definition_id,{context:{context_id:context.context_id,idempotency_key:id()},
  expected_draft_revision:draft.revision,expected_content_digest:draft.content_digest,expected_selection_digest:draft.selection_digest});
 assert.equal(f.store.schedules,undefined);
 f.close();const migrated=migrateStorage(f.dir,'36');
 assert.equal(migrated.service_id,f.service.service_id);assert.equal(migrated.database_instance_id,f.service.database_instance_id);
 f.open();
 const actor=f.handle(f.principal,scopes),restricted=f.handle(f.principal,scopes.filter(value=>value!=='schedule.activate'));
 const certs=peerCertificates(),fullToken='v1.'+id()+'.'+'A'.repeat(43),restrictedToken='v1.'+id()+'.'+'B'.repeat(43);
 const handles=new Map([[fullToken,actor],[restrictedToken,restricted]]),diagnostic=[];
 const createScheduleContext=(...args)=>{
  const result=f.auth.createScheduleContext(...args);f.advance(1000000n);return result;
 };
 let clockSample=null;
 const openApp=()=>startDraftHttp({cert:certs.leaves[0].cert,key:certs.leaves[0].key,bindingId:id(),service:f.store.identity,bootId:f.store.serviceBootId,
  receiver:{async prepare({token}){return handles.get(token);}},authorization:{...f.auth,createScheduleContext},drafts:f.store.drafts,publications:f.store.publications,runs:f.store.runs,
  localSchedules:f.store.schedules,scheduleClock:()=>clockSample,onFailure(value){diagnostic.push(value);}});
 let app=await openApp();
 t.after(()=>app?.close());
 const scheduleId=id(),post=(operation,request,token=fullToken)=>new Promise((resolve,reject)=>{
  const body=Buffer.from(canonicalize({operation,request}));
  const req=https.request(new URL('/auth/local-schedules/v1',app.origin),{method:'POST',headers:{
   Authorization:'Bearer '+token,'Content-Type':'application/json','Content-Length':String(body.length)},ca:certs.ca,agent:false},res=>{
   const chunks=[];res.on('data',chunk=>chunks.push(chunk));res.on('end',()=>resolve({status:res.statusCode,body:JSON.parse(Buffer.concat(chunks))}));
  });
  req.on('error',reject);req.end(body);
 });
 async function issued(operation_id){
  const reply=await post('create-context',{operation_id,ttl_ms:30000,max_unique_requests:4});
  assert.equal(reply.status,200,JSON.stringify(diagnostic));return reply.body.result.context_id;
 }
 const publishContext=await issued('publishSchedule');
 const item={item_id:'programme',one_shot_key:'edition',local_start:'2026-11-25T19:00:00.000',offset:'+09:00',
  steps:[{step_id:'first',role:'on_air',start_offset_ms:0,latest_start_offset_ms:1500}]};
 const input={context:{context_id:publishContext,idempotency_key:id()},definition_id:draft.definition_id,definition_version:published.definition_version,
  expected_artifact_ref:published.artifact_ref,expected_version:null,items:[item]};
 const publish=(version=input,schedule_id=scheduleId)=>post('publish',{schedule_id,version});
 const get=(version=null)=>post('get',{schedule_id:scheduleId,version});
 const invalidId=id();
 assert.equal((await publish({...input,context:{...input.context,idempotency_key:id()},items:[item,{...item,item_id:'bad',offset:'+14:01'}]},invalidId)).status,422);
 assert.equal((await post('get',{schedule_id:invalidId,version:null})).status,404);
 const first=await publish();assert.equal(first.status,200,JSON.stringify(diagnostic));
 assert.deepEqual(await publish(),first);
 assert.equal(first.body.result.body.items[0].step_windows[0].not_after,'2026-11-25T10:00:01.500Z');
 assert.equal(first.body.result.body.items[0].step_windows[0].action_binding_id,'action');
 assert.equal((await get()).body.result.activation,null);
 assert.equal((await post('create-context',{operation_id:'activateSchedule',ttl_ms:30000,max_unique_requests:1},restrictedToken)).status,403);
 const activateContext=await issued('activateSchedule');
 const activation={context:{context_id:activateContext,idempotency_key:id()},expected_revision:null,schedule_version_ref:first.body.result.ref};
 const activate=(input=activation,schedule_id=scheduleId)=>post('activate',{schedule_id,activation:input});
 assert.equal((await activate()).status,200);
 const moved={...input,context:{...input.context,idempotency_key:id()},expected_version:'1',items:[
  {...item,local_start:'2026-11-26T19:00:00.000'},{...item,item_id:'expired',local_start:'2026-11-24T19:00:00.000'}]};
 const second=await publish(moved);assert.equal(second.status,200,JSON.stringify(diagnostic));
 assert.equal(second.body.result.body.items[0].occurrence_id,first.body.result.body.items[0].occurrence_id);
 assert.deepEqual((await get('1')).body.result.version,first.body.result);
 assert.equal((await get('3')).status,404);
 assert.deepEqual((await get()).body.result.activation.body.schedule_version_ref,first.body.result.ref);
 const proposed={...activation,expected_revision:'1',schedule_version_ref:second.body.result.ref};
 const results=await Promise.all([activate({...proposed,context:{...activation.context,idempotency_key:id()}}),
  activate({...proposed,context:{...activation.context,idempotency_key:id()}})]);
 assert.deepEqual(results.map(result=>result.status).sort(),[200,409]);
 assert.deepEqual((await get()).body.result.activation.body.schedule_version_ref,second.body.result.ref);
 assert.equal((await publish({...moved,context:{...moved.context,idempotency_key:id()},expected_version:'2',items:[{...item,one_shot_key:'new'}]})).status,409);
 await app.close();app=null;
 f.close();
 const db=new Database(path.join(f.dir,'seamer.sqlite'),{readonly:true});
 try{
  assert.equal(db.prepare('SELECT count(*) FROM schedule_occurrence').pluck().get(),2);
  assert.equal(db.prepare('SELECT count(*) FROM schedule_request').pluck().get(),4);
  assert.equal(db.prepare('SELECT count(*) FROM automation_execution').pluck().get(),0);
  assert.deepEqual(db.pragma('foreign_key_check'),[]);
 }finally{db.close();}
 assert.equal(migrateStorage(f.dir,'37').service_id,f.service.service_id);
 f.open();
 let reader=f.handle(f.principal,scopes);
 handles.set(fullToken,reader);
 app=await openApp();
 const schedules=()=>f.store.schedules,occurrence=second.body.result.body.items[0];
 const stepStart=BigInt(occurrence.step_windows[0].window.not_before_utc_ns);
 const domain={clock_domain_id:'clock_'+'1'.repeat(64),descriptor_digest:'sha256:'+'2'.repeat(64)};
 const status=()=>schedules().get(reader,scheduleId).occurrences[0];
 function sample(tick,value=null){
  const clock={tick_ns:String(tick),domain};
  let reference=null;
  if(value!==null){
   const binding={domain:{...domain,...scope}},anchor={binding,anchor_tick_ns:'0',exchange_start_tick_ns:'0',
    utc_at_anchor:{lower_ns:String(value-10n),upper_ns:String(value)}};
   reference={service_boot_id:f.store.serviceBootId,valid_before_tick_ns:'10000',state:{binding,status:'qualified'},
    bundle:{anchor,bundle_id:'anchor',bundle_digest:digest(anchor),profile:{elapsed_error_ppm:0,span_quantization_ns:'1'},
     policy:{max_half_width_ns:'25',max_anchor_age_ns:'20000',max_exchange_age_ns:'20000',max_upstream_age_ns:'20000'},
     error:{upstream_age_upper_at_m1_ns:'0'}}};
  }
  return {clock,reference};
 }
 const drive=(tick,value)=>schedules().reconcileClock(()=>sample(tick,value));
 assert.equal(status().state,'awaiting_reference');
 drive(50n,stepStart-101n);
 assert.equal(status().window_enter_tick_ns,'100');
 const elapsed=()=>schedules().get(reader,scheduleId).occurrences[1];
 assert.equal(elapsed().state,'missed');
 const boundary=status().boundary_ref;
 drive(60n,stepStart-121n);
 assert.equal(status().window_enter_tick_ns,'100');
 drive(70n);assert.equal(status().state,'reference_unavailable');
 drive(80n,stepStart-101n);assert.equal(status().state,'tracking');
 drive(90n);
 drive(100n,stepStart-101n);
 const stopped=status();assert.equal(stopped.state,'blocked');assert.equal(stopped.stop.reason,'reference_time_lost');
 assert.deepEqual(stopped.stop.boundary_ref,boundary);
 drive(101n,stepStart-101n);assert.deepEqual(status(),stopped);
 const raceId=id(),contextFor=context_id=>({context_id,idempotency_key:id()});
 const rp=await issued('publishSchedule'),ra=await issued('activateSchedule');
 const earlyVersion=await publish({...input,context:contextFor(rp)},raceId);
 assert.equal(earlyVersion.status,200);
 const early={context:contextFor(ra),expected_revision:null,schedule_version_ref:earlyVersion.body.result.ref};
 clockSample=sample(200n,stepStart-86400000000000n-1000n);
 assert.equal((await activate(early,raceId)).status,200);
 clockSample=sample(300n,stepStart-86400000000000n-1000n);
 const beforeEntry=await activate({...early,context:contextFor(ra),expected_revision:'1'},raceId);
 assert.equal(beforeEntry.status,200,JSON.stringify(diagnostic));
 assert.equal(beforeEntry.body.result.ref.revision,'2');
 const replacementVersion=await publish({...moved,context:contextFor(rp),items:[moved.items[1]]},raceId);
 assert.equal(replacementVersion.status,200);
 const replace={context:contextFor(ra),expected_revision:'2',schedule_version_ref:replacementVersion.body.result.ref};
 const getRace=version=>post('get',{schedule_id:raceId,version});
 const firstClock=(await getRace('1')).body.result.occurrences[0];
 assert.equal(firstClock.state,'tracking');
 clockSample=null;
 const unavailable=await activate(replace,raceId);
 assert.equal(unavailable.status,503);
 assert.equal(unavailable.body.error,'CLOCK_UNTRUSTED');
 assert.equal((await getRace('1')).body.result.activation.ref.revision,'2');
 // The periodic scan has not seen this tick. The replacement itself must
 // record the previous window crossing even though it removes that item.
 clockSample=sample(1000n,stepStart-86400000000000n-1000n);
 const replaced=await activate(replace,raceId);
 assert.equal(replaced.status,200,JSON.stringify(diagnostic));
 const protectedOccurrence=(await getRace('1')).body.result.occurrences[0];
 assert.equal(protectedOccurrence.state,'blocked');
 assert.equal(protectedOccurrence.stop.reason,'previous_window_entered');
 assert.deepEqual(protectedOccurrence.stop.boundary_ref,firstClock.boundary_ref);
 clockSample=null;
 assert.deepEqual(await activate(replace,raceId),replaced);
 const reintroduced=await publish({...moved,context:contextFor(rp),expected_version:'2',items:[moved.items[0]]},raceId);
 assert.equal(reintroduced.status,200);
 clockSample=sample(1010n,stepStart-86400000000000n-1000n);
 assert.equal((await activate({...replace,context:contextFor(ra),expected_revision:'3',schedule_version_ref:reintroduced.body.result.ref},raceId)).status,200);
 assert.deepEqual((await getRace('3')).body.result.occurrences[0],protectedOccurrence);
 f.close();f.open();reader=f.handle(f.principal,scopes);
 assert.deepEqual(status(),stopped);
 assert.equal(elapsed().state,'missed');
 assert.deepEqual(schedules().get(reader,raceId).occurrences[0],protectedOccurrence);
 f.close();
 const nextSchema=migrateStorage(f.dir,'38');
 assert.equal(nextSchema.service_id,f.service.service_id);
 assert.equal(nextSchema.database_instance_id,f.service.database_instance_id);
 f.open();reader=f.handle(f.principal,scopes);
 assert.deepEqual(schedules().get(reader,raceId).occurrences[0],protectedOccurrence);
});
