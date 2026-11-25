import assert from 'node:assert/strict';
import {randomUUID as id} from 'node:crypto';
import fs from 'node:fs';
import https from 'node:https';
import path from 'node:path';
import test from 'node:test';
import {setTimeout as wait} from 'node:timers/promises';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {fork} from 'node:child_process';
import {once} from 'node:events';
import canonical from 'canonicalize';
import Database from 'better-sqlite3';
import {boolean,delay as delayStep,digest,input,reference} from './automation-program-fixture.mjs';
import {inspect,setup} from './automation-publication-fixture.mjs';
import {fixture as clockFixture} from './auth-clock-fixture.mjs';
import {peerCertificates} from '../../leafseamer-controller/test/fixtures/peer-certificates.mjs';
import {triggerReference} from './fixtures/trigger-reference.mjs';

const packageRoot=process.env.LEAFSEAMER_PACKAGE_UNDER_TEST??fileURLToPath(new URL('../',import.meta.url));
const {migrateStorage}=await import(pathToFileURL(path.join(packageRoot,'dist','storage.mjs')));
const {automationRunStore}=await import(pathToFileURL(path.join(packageRoot,'dist','automation-run-store.mjs')));
const {scheduleStore}=await import(pathToFileURL(path.join(packageRoot,'dist','schedule-store.mjs')));
const {automationPublicationStore}=await import(pathToFileURL(path.join(packageRoot,'dist','automation-publication-store.mjs')));
const {openInstalledPublicationPolicy}=await import(pathToFileURL(path.join(packageRoot,'dist','trusted-pairing-profile.mjs')));
const {snapshotIngestStore}=await import(pathToFileURL(path.join(packageRoot,'dist','snapshot-ingest-store.mjs')));
const {reconcileInbox}=await import(pathToFileURL(path.join(packageRoot,'dist','reconcile-inbox.mjs')));
const {authenticationClockStore}=await import(pathToFileURL(path.join(packageRoot,'dist','runtime','auth-clock-store.mjs')));
const {startAuthenticationClock}=await import(pathToFileURL(path.join(packageRoot,'dist','runtime','auth-clock.mjs')));
const {runApprovalStore}=await import(pathToFileURL(path.join(packageRoot,'dist','run-approval-store.mjs')));
const {runInitiatorReadReply}=await import(pathToFileURL(path.join(packageRoot,'dist','runtime','peer-run-initiator.mjs')));
const {startPeerPairingTransport}=await import(pathToFileURL(path.join(packageRoot,'dist','runtime','peer-pairing-transport.mjs')));
const {automationCommandIdentity}=await import(pathToFileURL(path.join(packageRoot,'dist','runtime','peer-automation-command.mjs')));
const {startDraftHttp}=await import(pathToFileURL(path.join(packageRoot,'dist','draft-http.mjs')));
const {createClient,contractSelection}=await import(pathToFileURL(path.join(packageRoot,'dist','sdk','index.mjs')));
const actionControllerRoot=process.env.LEAFSEAMER_ACTION_CONTROLLER_TEST_ROOT;
const controllerActions=actionControllerRoot?await import(pathToFileURL(path.join(actionControllerRoot,'dist','peer-action-admission.mjs'))):null;
const ref=(kind,scope,object_id,digest)=>({kind,scope,object_id,revision:'1',digest});

async function verifyNotificationRestart(dir,completion){
 const consumerId=id(),collector=path.join(dir,'completion-consumer.sqlite');
 const fixture=fileURLToPath(new URL('./fixtures/execution-notification-consumer.mjs',import.meta.url));
 const children=new Set();
 function start(mode){
  const child=fork(fixture,[packageRoot,dir,collector,consumerId,mode],{stdio:['ignore','ignore','pipe','ipc'],windowsHide:true});
  const messages=[];let stderr='';
  child.stderr.on('data',chunk=>{stderr=(stderr+chunk).slice(-2048);});
  child.on('message',message=>messages.push(message));
  const exited=once(child,'close');
  const member={child,exited,messages,async waitFor(predicate){
   const until=performance.now()+8000;
   while(performance.now()<until){
    const value=messages.find(predicate);
    if(value)return value;
    assert.equal(child.exitCode,null,stderr);
    await wait(25);
   }
   assert.fail('通知消费者未达到预期状态 '+stderr);
  },async close(){child.send('close');assert.deepEqual(await exited,[0,null]);}};
  children.add(member);
  void exited.then(()=>children.delete(member));
  return member;
 }
 try{
  const first=start('crash'),firstBoot=await first.waitFor(m=>m.event==='running');
  const observed=await first.waitFor(m=>m.event==='observed');
  assert.equal(observed.inserted,1);
  assert.deepEqual(observed.packet.event,completion.notification);
  assert.equal(observed.packet.ack.consumer_id,consumerId);
  assert.equal(observed.packet.ack.payload_digest,digest(completion.notification));
  assert.equal(first.child.kill(),true);await first.exited;
  const receipts=()=>inspect(dir,db=>db.prepare('SELECT body_jcs FROM automation_execution_notification_receipt').pluck().all());
  assert.deepEqual(receipts(),[]);
  const recovered=start('recover'),recoveredBoot=await recovered.waitFor(m=>m.event==='running');
  assert.notEqual(firstBoot.boot_id,recoveredBoot.boot_id);
  await recovered.waitFor(()=>recovered.messages.filter(m=>m.event==='observed').length>=2);
  for(const message of recovered.messages.filter(m=>m.event==='observed')){
   assert.equal(message.inserted,0);
   assert.deepEqual(message.packet,observed.packet);
  }
  const until=performance.now()+4000;
  do{recovered.child.send('status');await wait(25);}while(performance.now()<until&&
   !recovered.messages.some(m=>m.event==='status'&&m.status.delivered===1));
  const {status}=await recovered.waitFor(m=>m.event==='status'&&m.status.delivered===1);
  assert.equal(status.failed,1);
  await recovered.close();
  assert.equal(recovered.messages.filter(m=>m.event==='observed').length,2);
  assert.deepEqual(receipts().map(JSON.parse),[observed.packet.ack]);
  const final=start('confirmed');
  await final.waitFor(m=>m.event==='running');await wait(1200);await final.close();
  assert.equal(final.messages.filter(m=>m.event==='observed').length,0);
  const collected=new Database(collector,{readonly:true});
  try{assert.equal(collected.prepare('SELECT count(*) FROM received').pluck().get(),1);}finally{collected.close();}
  inspect(dir,db=>{
   assert.equal(db.prepare('SELECT count(*) FROM automation_execution_commit').pluck().get(),1);
   assert.throws(()=>db.exec('DELETE FROM automation_execution_notification_receipt'),/immutable_execution_notification_receipt/);
   assert.deepEqual(db.pragma('foreign_key_check'),[]);
  });
 }finally{
  for(const {child,exited} of children){child.kill();await exited;}
 }
}

function eventRegistration(){
 const publisher={authority_service_id:id(),database_instance_id:id()},registration_id=id(),device_id=id(),source_epoch=id();
 const source_key={...publisher,registration_id,source_stream_id:device_id,source_epoch};
 const original={registration_id,revision:'1',owner_service_id:publisher.authority_service_id,database_instance_id:publisher.database_instance_id,
  provider_id:id(),provider_boot_id:id(),capture_policy_id:'fixture',capture_policy_revision:'1',delivery_semantics:'discrete',
  target:{kind:'device',device_id,trigger_id:'fixture',capability_version:'1.0.0'}};
 return {publisher,source_key,original};
}
function eventStream(db,service,bootId,registration=eventRegistration(),triggerDecisions=null,withReference=false,onClockSample=null){
 const {publisher,source_key,original}=registration,{registration_id,source_epoch}=source_key;
 const binding_ref={...publisher,entity_kind:'source_binding',entity_id:registration_id,entity_epoch:source_epoch,revision:'1',content_digest:digest(original).slice(7)};
 const binding={publisher_service_id:publisher.authority_service_id,publisher_database_instance_id:publisher.database_instance_id,subscription_id:id(),connection_generation:'1'};
 const owner={kind:'service',pairing_id:id(),consumer_id:'unrelated-source',consumer_service_id:service.service_id,consumer_database_instance_id:service.database_instance_id};
 const snapshot_ref={snapshot_id:id(),manifest_digest:digest('unrelated snapshot').slice(7),snapshot_cursor:'0'};
 const projection={entity_kind:'sources',entity_id:registration_id,entity_epoch:source_epoch,entity_revision:'1',
  view:{source_key,binding_ref,state:'baseline_required',baseline_ref:null,contiguous_sequence:'0',highest_seen_sequence:'0',gaps:[]}};
 const selected={subscription:{owner,purpose:'canonical_trigger',binding,snapshot_ref},context:{},projection:{records:[projection],source_watermarks:[{}]}};
 const sources=snapshotIngestStore({db,service,bootId,isOpen:()=>db.open,receiver:{currentInTransaction:()=>selected}});
 const receipt=sources.apply(null);
 const store=authenticationClockStore(db,{service_id:service.service_id,service_boot_id:bootId});
 const base=clockFixture(store.scope),descriptor_digest=digest('trigger receiver clock'),proof=withReference?triggerReference({service,bootId,registration}):null;
 const fixture=withReference?clockFixture(store.scope,{source:base.source,descriptor:{clock_domain_id:'clock_'+descriptor_digest.slice(7),descriptor_digest}}):base;
 const runningClock=startAuthenticationClock(fixture.source,store,fixture.provider,{schedule:false});
 const clock={...runningClock,pairingSample(){
  onClockSample?.(fixture.source,db.inTransaction);
  return runningClock.pairingSample();
 }};
 triggerDecisions??=Number(service.schema_revision)>=24?automationRunStore({db,service,bootId,isOpen:()=>db.open}).triggerDecisions:null;
 const inbox=reconcileInbox({db,service,bootId,clock,isOpen:()=>db.open,triggerDecisions,referenceProvider:proof?.receiver});
 let cursor=0,lastLive=null;
 const write=()=>assert.ok(db.inTransaction);
 const originFor=(source_sequence,event_id,value)=>({source_key,binding_ref:original,event_id,source_sequence,source_time_ref:null,
  payload:{schema_ref:ref('schema',publisher,'fixture',digest('unrelated schema')),value}});
 const event=origin=>({entry_kind:'source.event',payload:{origin,origin_digest:digest(origin)}});
 const frame=(entry,mode,position=String(++cursor))=>({type:'server.data',entry,cursor:position,previous_cursor:String(Number(position)-1),
  binding,delivery_mode:mode,payload_digest:digest(entry).slice(7)});
 function upsert(sequence,mode){
  const next={...projection,entity_revision:String(Number(sequence)+1),view:{...projection.view,contiguous_sequence:sequence,highest_seen_sequence:sequence}};
  inbox.accept(receipt,frame({entry_kind:'entity.upsert',payload:next},mode),write);
 }
 return {close:clock.stop,executionSample:clock.referenceTimeSample,advanceReceiver:ns=>fixture.source.advance(ns),baseline:{apply_id:receipt.apply_id,snapshot_ref},read:eventId=>inbox.sourceEvent(source_key,eventId),
 projectLive:sequence=>upsert(sequence,'live'),
 async replayLive(){
  const never=()=>assert.fail('同一事件重投不重新取证');
  await inbox.acceptFromPeer(receipt,lastLive,write,never,never,{readSourceInTransaction:never,prepareSourceTransport:never});
  return inbox.sourceEvent(source_key,lastLive.entry.payload.origin.event_id);
 },append(source_sequence,event_id=id()){
  const origin=originFor(source_sequence,event_id,{value:true});
  inbox.accept(receipt,frame(event(origin),'reconcile',proof?undefined:source_sequence),write);
  if(proof){
   upsert(source_sequence,'reconcile');
   const marker={entry_kind:'sync.ready',payload:{baseline_snapshot_id:snapshot_ref.snapshot_id}};
   inbox.accept(receipt,{...frame(marker,'reconcile'),type:'server.sync.ready',baseline_snapshot_id:snapshot_ref.snapshot_id},write);
  }
  return inbox.sourceEvent(source_key,origin.event_id).receive_cursor;
 },async live(sequence,value,delay=0n,deferProjection=false){
  const origin=originFor(sequence,id(),value),evidence=proof.qualify(origin),delivery=frame(event(origin),'live');
  try{
   await inbox.acceptFromPeer(receipt,delivery,write,()=>{fixture.source.advance(delay);return evidence;},()=>proof.bundle,{
    readSourceInTransaction:sources.sourceStateInTransaction,async prepareSourceTransport(){
     assert.equal(db.inTransaction,false);
     const previous=db.prepare('SELECT body_digest FROM subscription_reconcile_inbox WHERE apply_id=? AND cursor=?').pluck().get(receipt.apply_id,delivery.previous_cursor);
     return {observation:{state:'ready',connection_generation:'1',ack_cursor:delivery.previous_cursor,ack_payload_digest:previous.slice(7)},assertCurrentInTransaction:write};
    }});
  }catch(error){cursor--;throw error;}
  lastLive=delivery;
  if(!deferProjection)upsert(sequence,'live');
  return inbox.sourceEvent(source_key,origin.event_id);
 }};
}

function inputFor(registration){
 const {publisher,original}=registration;
 return {authority_id:publisher.authority_service_id,registration_ref:{id:original.registration_id,revision:original.revision,digest:digest(original)},
  schema_ref:{id:'fixture',revision:'1',digest:digest('unrelated schema')}};
}

test('显式启用触发索引和决定后，摄取、全部匹配运行及原重投结果原子封存',async t=>{
 const registration=eventRegistration(),triggerBinding=inputFor(registration);
 const f=setup(t,{revision:'22',withoutActions:true,triggerBinding}),context=id();
 const contextBoot=f.store.serviceBootId;f.store.close();
 inspect(f.dir,db=>{
  const subject=id();db.prepare('INSERT INTO local_subject VALUES(?,?,?,?,?)').run(subject,id(),id(),id(),'human');
  db.prepare('INSERT INTO local_request_context VALUES(?,?,?,?,?)').run(context,subject,contextBoot,'{}',digest({}));
 });
 f.reopen();
 function create(version){return f.store.runs.create(null,{context:{context_id:context,idempotency_key:id()},definition_id:f.draft.definition_id,
  definition_version:version.definition_version,expected_artifact_ref:version.artifact_ref});}
 const first=await f.publish(),oldRun=create(first),oldBoot=f.store.serviceBootId;
 f.store.close();
 const old=inspect(f.dir,db=>{
  db.pragma('foreign_keys=ON');const events=eventStream(db,{...f.identity,schema_revision:'22'},oldBoot),eventId=id();
  try{events.append('1',eventId);return [eventId,events.read(eventId)];}finally{events.close();}
 });
 const originalRun=inspect(f.dir,db=>db.prepare('SELECT * FROM automation_run_creation WHERE run_id=?').raw().get(oldRun.run_id));
 inspect(f.dir,db=>db.exec('CREATE TABLE automation_trigger_match_set(sentinel TEXT) STRICT'));
 assert.throws(()=>migrateStorage(f.dir,'23'),{code:'migration_failed'});
 inspect(f.dir,db=>{
  assert.equal(db.pragma('user_version',{simple:true}),22);
  assert.equal(db.prepare("SELECT count(*) FROM sqlite_schema WHERE name='automation_trigger_binding'").pluck().get(),0);
  assert.deepEqual(db.prepare('SELECT * FROM automation_run_creation WHERE run_id=?').raw().get(oldRun.run_id),originalRun);
  db.exec('DROP TABLE automation_trigger_match_set');
 });
 const sourceSchema=migrateStorage(f.dir,'23');
 const historicalMatch=inspect(f.dir,db=>{
  db.pragma('foreign_keys=ON');const events=eventStream(db,sourceSchema,oldBoot),eventId=id();
  try{events.append('1',eventId);return [eventId,events.read(eventId)];}finally{events.close();}
 });
 const previousSet=inspect(f.dir,db=>{
  db.exec('CREATE TABLE automation_trigger_decision(sentinel TEXT) STRICT');
  return db.prepare('SELECT * FROM automation_trigger_match_set').raw().get();
 });
 assert.throws(()=>migrateStorage(f.dir,'24'),{code:'migration_failed'});
 inspect(f.dir,db=>{
  assert.equal(db.pragma('user_version',{simple:true}),23);
  assert.deepEqual(db.prepare('SELECT * FROM automation_trigger_match_set').raw().get(),previousSet);
  assert.equal(db.prepare("SELECT count(*) FROM sqlite_schema WHERE name='automation_trigger_decision_set'").pluck().get(),0);
  db.exec('DROP TABLE automation_trigger_decision');
 });
 const service=migrateStorage(f.dir,'24');
 assert.equal(service.service_id,f.identity.service_id);assert.equal(service.database_instance_id,f.identity.database_instance_id);
 inspect(f.dir,db=>{
  assert.deepEqual(db.prepare('SELECT * FROM automation_run_creation WHERE run_id=?').raw().get(oldRun.run_id),originalRun);
  assert.equal(db.prepare('SELECT count(*) FROM automation_trigger_binding').pluck().get(),1);
  const isOpen=()=>db.open,bootId=oldBoot;
  const {triggerDecisions}=automationRunStore({db,service,bootId,isOpen});
  const historical=reconcileInbox({db,service,bootId,clock:null,isOpen,triggerDecisions});
  assert.deepEqual(historical.sourceEvent(old[1].origin.source_key,old[0]),{...old[1],trigger_match_set:null,trigger_decision_set:null});
  assert.deepEqual(historical.sourceEvent(historicalMatch[1].origin.source_key,historicalMatch[0]),{...historicalMatch[1],trigger_decision_set:null});
 });
 f.reopen();
 const second=await f.publish({...f.request,context:{...f.request.context,idempotency_key:id()}}),newRun=create(second),sameVersion=create(second);
 const boot=f.store.serviceBootId;f.store.close();
 inspect(f.dir,db=>{
  db.pragma('foreign_keys=ON');const events=eventStream(db,service,boot,registration),eventId=id();
  const count=table=>db.prepare('SELECT count(*) FROM '+table).pluck().get();
  try{
   assert.equal(count('automation_trigger_binding'),2);
   db.exec('CREATE TEMP TRIGGER test_omit_match BEFORE INSERT ON automation_trigger_match_set BEGIN SELECT RAISE(IGNORE); END');
   assert.throws(()=>events.append('1',eventId),{code:'trigger_match_storage'});
   assert.equal(events.read(eventId),null);assert.equal(count('automation_trigger_match_set'),1);
   db.exec('DROP TRIGGER test_omit_match');
   for(const table of ['automation_trigger_decision','automation_trigger_decision_set']){
    db.exec(`CREATE TEMP TRIGGER test_omit_decision BEFORE INSERT ON ${table} BEGIN SELECT RAISE(IGNORE); END`);
    assert.throws(()=>events.append('1',eventId),{code:'trigger_decision_storage'});
    assert.equal(events.read(eventId),null);assert.equal(count('automation_trigger_match_set'),1);
    assert.equal(count('automation_trigger_decision'),0);assert.equal(count('automation_trigger_decision_set'),0);
    db.exec('DROP TRIGGER test_omit_decision');
   }
   const cursor=events.append('1',eventId),stored=events.read(eventId),{match,digest:hash}=stored.trigger_match_set;
   assert.equal(cursor.receive_sequence,'3');assert.equal(hash,digest(match));
   assert.equal(match.registry_revision,'3');assert.equal(match.targets_digest,digest(match.targets));
   assert.deepEqual(match.targets.map(target=>target.run_id),[oldRun.run_id,newRun.run_id,sameVersion.run_id].sort());
   for(const target of match.targets)assert.deepEqual(target.bindings,[{binding_id:'trigger',registration_ref:triggerBinding.registration_ref,schema_ref:triggerBinding.schema_ref}]);
   const {set,decisions}=stored.trigger_decision_set;
   assert.equal(set.match_digest,hash);assert.equal(set.trigger_disposition,'suppressed');assert.equal(digest(set.decisions),set.decisions_digest);
   assert.deepEqual(decisions.map(value=>value.run_id),match.targets.map(value=>value.run_id));
   for(const decision of decisions){
    assert.equal(decision.disposition,'suppressed_inactive');assert.equal(decision.reason,'run_disarmed');
    assert.equal(decision.ingest_revision_before,'1');assert.equal(decision.ingest_revision_after,'2');
    assert.equal(decision.run_revision_before,decision.run_revision_after);assert.equal(decision.execution_id,null);
    assert.deepEqual(decision.receive_cursor_after,cursor);
   }
   assert.deepEqual(events.append('1',eventId),cursor);assert.deepEqual(events.read(eventId),stored);
   assert.equal(count('automation_trigger_match_set'),2);assert.equal(count('automation_trigger_decision'),3);
   const other=eventStream(db,service,boot),otherId=id();
   try{
    other.append('1',otherId);const unmatched=other.read(otherId);
    assert.deepEqual(unmatched.trigger_match_set.match.targets,[]);
    assert.equal(unmatched.trigger_decision_set.set.trigger_disposition,'observed_only');assert.deepEqual(unmatched.trigger_decision_set.decisions,[]);
   }finally{other.close();}
   assert.throws(()=>db.exec('DELETE FROM automation_trigger_match_set'),/immutable_trigger_match/);
   assert.deepEqual(db.pragma('foreign_key_check'),[]);
  }finally{events.close();}
 });
 f.reopen();
 for(const run of [oldRun,newRun,sameVersion])assert.deepEqual(f.store.runs.get(null,run.run_id),{...run,last_receive_seq:'3'});
 f.store.close();
 const previousDecisions=inspect(f.dir,db=>{
  db.exec('CREATE TABLE automation_execution_step_state(sentinel TEXT) STRICT');
  return db.prepare('SELECT * FROM automation_trigger_decision ORDER BY ingest_epoch,receive_sequence,run_id').raw().all();
 });
 assert.throws(()=>migrateStorage(f.dir,'25'),{code:'migration_failed'});
 inspect(f.dir,db=>{
  assert.equal(db.pragma('user_version',{simple:true}),24);
  assert.equal(db.prepare("SELECT count(*) FROM sqlite_schema WHERE name='automation_execution'").pluck().get(),0);
  db.exec('DROP TABLE automation_execution_step_state');
 });
 assert.equal(migrateStorage(f.dir,'25').schema_revision,'25');
 inspect(f.dir,db=>{
  assert.deepEqual(db.prepare('SELECT * FROM automation_trigger_decision ORDER BY ingest_epoch,receive_sequence,run_id').raw().all(),previousDecisions);
  assert.equal(db.prepare('SELECT count(*) FROM automation_execution_head').pluck().get(),0);
  assert.deepEqual(db.pragma('foreign_key_check'),[]);
  db.exec('CREATE TABLE automation_execution_step_ready(sentinel TEXT) STRICT');
 });
 assert.throws(()=>migrateStorage(f.dir,'26'),{code:'migration_failed'});
 inspect(f.dir,db=>{
  assert.equal(db.pragma('user_version',{simple:true}),25);
  assert.equal(db.prepare("SELECT count(*) FROM sqlite_schema WHERE name='automation_execution_anchor'").pluck().get(),0);
  db.exec('DROP TABLE automation_execution_step_ready');
 });
 assert.equal(migrateStorage(f.dir,'26').schema_revision,'26');
 inspect(f.dir,db=>{
  assert.deepEqual(db.prepare('SELECT * FROM automation_trigger_decision ORDER BY ingest_epoch,receive_sequence,run_id').raw().all(),previousDecisions);
  assert.equal(db.prepare('SELECT count(*) FROM automation_execution_anchor').pluck().get(),0);
  assert.equal(db.prepare('SELECT count(*) FROM automation_execution_step_ready').pluck().get(),0);
  assert.deepEqual(db.pragma('foreign_key_check'),[]);
  db.exec('CREATE TABLE automation_execution_delay_elapsed(sentinel TEXT) STRICT');
 });
 assert.throws(()=>migrateStorage(f.dir,'27'),{code:'migration_failed'});
 inspect(f.dir,db=>{
  assert.equal(db.pragma('user_version',{simple:true}),26);
  assert.equal(db.prepare("SELECT count(*) FROM sqlite_schema WHERE name='automation_execution_delay'").pluck().get(),0);
  db.exec('DROP TABLE automation_execution_delay_elapsed');
 });
 assert.equal(migrateStorage(f.dir,'27').schema_revision,'27');
 inspect(f.dir,db=>{
  assert.deepEqual(db.prepare('SELECT * FROM automation_trigger_decision ORDER BY ingest_epoch,receive_sequence,run_id').raw().all(),previousDecisions);
  assert.equal(db.prepare('SELECT count(*) FROM automation_execution_delay').pluck().get(),0);
  assert.deepEqual(db.pragma('foreign_key_check'),[]);
  db.exec('CREATE TABLE automation_execution_action_parameters(sentinel TEXT) STRICT');
 });
 assert.throws(()=>migrateStorage(f.dir,'28'),{code:'migration_failed'});
 inspect(f.dir,db=>{
  assert.equal(db.pragma('user_version',{simple:true}),27);
  db.exec('DROP TABLE automation_execution_action_parameters');
 });
 assert.equal(migrateStorage(f.dir,'28').schema_revision,'28');
 inspect(f.dir,db=>{
  assert.deepEqual(db.prepare('SELECT * FROM automation_trigger_decision ORDER BY ingest_epoch,receive_sequence,run_id').raw().all(),previousDecisions);
  assert.equal(db.prepare('SELECT count(*) FROM automation_execution_action_parameters').pluck().get(),0);
  assert.deepEqual(db.pragma('foreign_key_check'),[]);
 });
 f.reopen();
 for(const run of [oldRun,newRun,sameVersion])assert.equal(f.store.runs.get(null,run.run_id).last_receive_seq,'3');
});

function seedRunApproval(db,scope,run,contextId,target,deviceActions,validity={not_before:'2026-01-01T00:00:00.000Z',not_after:'2026-01-02T00:00:00.000Z'},parameterConstraints=[]){
 const targets=Array.isArray(target)?target:target?[target]:[];
 const proposalId=id(),approvalId=id(),operationId=id(),requestDigest=digest(id()),requestIdentity=digest(id());
 const audit={context_id:contextId,request_digest:requestDigest},auditDigest=digest(audit);
 const dependencyDigest=db.prepare('SELECT dependency_digest FROM automation_run_creation WHERE run_id=?').pluck().get(run.run_id);
 const shared={scope,run_id:run.run_id,artifact_ref:run.artifact_ref,
  dependency_footprint_ref:ref('program_dependencies',scope,run.definition_id,dependencyDigest),
  program_digest:digest('program'),bindings_digest:digest('bindings'),service_principal:{issuer_id:id(),issuer_epoch:id(),principal_id:id()},
  source_service_approval_ref:ref('service_approval',targets[0]??scope,id(),digest('service approval')),
  validity};
 const proposal={...shared,proposal_id:proposalId,definition_id:run.definition_id,definition_version:run.definition_version,
  controller_targets:targets.map(controller_scope=>({controller_scope,...(deviceActions&&{device_actions:deviceActions,parameter_constraints:parameterConstraints})})),
  created_authentication_ref:ref('authentication',scope,proposalId,auditDigest)};
 const proposal_ref=ref('run_approval_proposal',scope,proposalId,digest(proposal));
 const approval={...shared,proposal_ref,approval_id:approvalId,revision:'1',controller_endorsement_refs:targets.map(target=>ref('run_approval_endorsement',target,id(),digest('endorsement'))),
  approval_operation_id:operationId,created_authentication_ref:ref('authentication',scope,approvalId,auditDigest)};
 const approvalRef=ref('run_approval',scope,approvalId,digest(approval));
 const result={kind:'approve',state:'applied',operation_id:operationId,run_id:run.run_id,request_identity:requestIdentity,request_digest:requestDigest,
  proposal_ref,approval_ref:approvalRef,result_ref:approvalRef};
 db.prepare('INSERT INTO automation_run_approval_proposal VALUES(?,?,?,?,?,?,?,?,?)')
  .run(proposalId,run.run_id,contextId,digest(id()),requestDigest,canonical(proposal),digest(proposal),canonical(audit),auditDigest);
 db.prepare('INSERT INTO automation_run_approval VALUES(?,?,?,?,?,?,?,?,?,?)')
  .run(approvalId,run.run_id,proposalId,contextId,requestIdentity,requestDigest,canonical(approval),digest(approval),canonical(audit),auditDigest);
 db.prepare('INSERT INTO automation_run_approval_operation VALUES(?,?,?,?)').run(operationId,approvalId,canonical(result),digest(result));
 db.prepare("INSERT INTO automation_run_approval_head VALUES(?,?,'2')").run(run.run_id,approvalId);
 db.prepare("INSERT INTO automation_run_approval_state VALUES(?,1,'active')").run(approvalId);
 return approvalId;
}

for(const [label,schema_revision,delayCase=null,parameterCase=null,commandCase=null] of [
 ['零目标','16'],['设备目标','24'],['升级保留未锚定的原计划','25'],['完整触发计划与提交后就绪','26'],['提交后就绪失败保持保护','26'],
 ['延迟释放直接后继','27','elapsed'],['首次延迟等于截止时保护','27','late'],['后继就绪漏写回滚延迟到期','27','rollback'],
 ['延迟期间失去来源保护','27','source'],['延迟期间来源换代保护','27','generation'],
 ['就绪动作封存事件参数','28',null,'validated'],['缺少原参数投影保护','28',null,'unknown'],
 ['等待当前触发事件的来源投影后才封存动作参数','28',null,'validated'],
 ['动作守卫为假时保护','28',null,'guard'],['物化后 RunGrant 失效保持保护','28',null,'grant'],
 ['参数超出 RunGrant 授权范围时保护','28',null,'constraint'],
 ['动作参数写入被拦截时保护','28',null,'storage'],
 ['延迟后动作封存参数和直接窗口','28','elapsed','validated'],
 ['动作提交已受理且不会推进成功后继','29',null,'validated','accepted'],
 ['动作提交永久拒收后保留原裁决','29',null,'validated','rejected'],
 ['动作提交回包丢失并在新运行域按原请求追认','29',null,'validated','lost'],
 ['动作收到其他主体的裁决时保留未决','29',null,'validated','wrong_subject'],
 ['动作提交尝试回滚时不调用 Controller','29',null,'validated','rollback'],
 ['配对读取已证实终态并按原命令落账','30',null,'validated','terminal_success'],
 ['配对读取未知终态时结算与保护同事务','30',null,'validated','terminal_unknown'],
 ['同一运行域采用成功终态后解锁原依赖，重开读回证据','31',null,'validated','terminal_dependency'],
 ['来源失步后查回成功终态只作历史证据','31',null,'validated','terminal_after_protection'],
 ['R5 规则提交并释放活动执行，下一事件可受理','32',null,'validated','r5_rule'],
 ['排期与事件共享空闲活动槽，受理与版本绑定同事务','38',null,'validated','r5_schedule'],
 ['Schema 39 paired schedule runtime','39',null,'validated','r5_scheduled_runtime'],
 ['R5 状态机原子提交目标状态，后续按新状态匹配','32',null,'validated','r5_state'],
 ['R5 可选独立动作确定失败且资源已释放时完成','32',null,'validated','r5_optional'],
 ['R5 提交前目标资格撤回时保留原状态和终态','32',null,'validated','r5_grant_revoked'],
 ['R5 释放写入失败全量回滚并保留对账资格','32',null,'validated','r5_rollback'],
 ['R5 通知已落库未确认时重启并重送原事件','32',null,'validated','r5_notification']
])test('布防受理、回执采用和最终提交保留同一操作：'+label,async t=>{
 const registration=eventRegistration(),hasTarget=schema_revision!=='16',executionPlanning=Number(schema_revision)>=25;
 const withTerminals=Number(schema_revision)>=30;
 const r5=commandCase?.startsWith('r5_'),r5Rollback=commandCase==='r5_rollback',r5Optional=commandCase==='r5_optional';
 const r5GrantRevoked=commandCase==='r5_grant_revoked',r5Protected=r5Rollback||r5GrantRevoked;
 const scheduledRuntime=commandCase==='r5_scheduled_runtime',scheduleCase=scheduledRuntime||commandCase==='r5_schedule';
 const stateMachine=['r5_state','r5_notification'].includes(commandCase)||r5Protected;
 const withAnchors=Number(schema_revision)>=26,omitReady=label==='提交后就绪失败保持保护';
 const triggerBinding=inputFor(registration),limits={event_age_limits:[{authority_id:triggerBinding.authority_id,registration_ref:triggerBinding.registration_ref,
  capture_semantics:'event_occurrence',max_age_ms:1001}],max_condition_nodes:20};
 const compare=left=>({kind:'compare',operator:'eq',value_type:boolean,left,right:{kind:'literal',value:true}});
 const condition=scheduleCase?{kind:'literal',value:true}:{kind:'any',items:[compare(reference()),{kind:'all',items:[
  compare({...reference(),pointer:'/request_observation'}),compare(reference('observation','observation'))]}]};
 const projections=[...input().projections,{pointer:'/request_observation',value_type:boolean,reads:[]}];
 const triggerReview=hasTarget?{condition,policyDigest:digest(limits),extraInputs:[input('observation','observation')],
  ...(stateMachine&&{stateMachine:true}),
  ...(delayCase&&{beforeSteps:[{...delayStep('before-a'),delay_ms:5,max_lateness_ms:10},{...delayStep('before-b'),delay_ms:15,max_lateness_ms:20}]}),
  ...(parameterCase&&{actionParameters:{...(r5Optional&&{required:false,failure_policy:'continue_if_independent'}),parameter_bindings:[{target_pointer:'/value',
   value:scheduleCase?{kind:'literal',value:true}:parameterCase==='unknown'?{...reference(),pointer:'/request_observation'}:reference()}],
   ...(parameterCase==='guard'&&{guard:{kind:'literal',value:false}})}}),
  ...(executionPlanning&&{extraSteps:[delayStep('after-action')]})}:null;
 const f=setup(t,{revision:schema_revision,withoutActions:!hasTarget,triggerBinding:{...triggerBinding,projections},triggerReview}),{scope,draft}=f,contextId=id(),bootId=f.store.serviceBootId;
 const actor={issuer_id:id(),issuer_epoch:id(),principal_id:id(),principal_kind:'human'},authEvidence={origin:'fixture-authentication'};
 f.store.close();
 inspect(f.dir,db=>{
  const subject=id();db.prepare('INSERT INTO local_subject VALUES(?,?,?,?,?)').run(subject,actor.issuer_id,actor.issuer_epoch,actor.principal_id,'human');
  const body=scheduleCase?{context:{operations:['activateSchedule','publishSchedule']}}:{};
  db.prepare('INSERT INTO local_request_context VALUES(?,?,?,?,?)').run(contextId,subject,bootId,canonical(body),digest(body));
 });
 f.reopen();const boot=f.store.serviceBootId,published=await f.publish();
 const create=()=>f.store.runs.create(null,{context:{context_id:contextId,idempotency_key:id()},definition_id:draft.definition_id,
  definition_version:published.definition_version,expected_artifact_ref:published.artifact_ref});
 const run=create(),other=create();f.store.close();
 const filename=path.join(f.dir,'seamer.sqlite');let db=new Database(filename);db.pragma('foreign_keys=ON');
 try{
  const target=hasTarget?{authority_service_id:id(),database_instance_id:id()}:null;
  const action={device_id:id(),action_id:'simulator.value.set',capability_version:'1.0.0'};
  const sampledAt=Date.now(),epoch=BigInt(sampledAt)*1000000n;
  const validity={not_before:new Date(sampledAt-60000).toISOString(),not_after:new Date(sampledAt+60000).toISOString()};
  const pairing={binding_id:id(),revision:'1',digest:digest('current peer pairing'),peer_service_id:target?.authority_service_id,transcript:{pair_attempt_id:id(),parties:[
   {service_id:scope.authority_service_id,permissions:['automation.prepare.read','automation.protect']},
   {service_id:target?.authority_service_id,database_instance_id:target?.database_instance_id,service_kind:'controller',permissions:['automation.prepare.read']}]}};
  db.transaction(()=>{
   const constraints=parameterCase?[{...action,parameter_pointer:'/value',rule:{kind:'equals',value:parameterCase!=='constraint'}}]:[];
   seedRunApproval(db,scope,run,contextId,target,[action],validity,constraints);
   seedRunApproval(db,scope,other,contextId,target,[action],validity);
   if(hasTarget){
    db.prepare("INSERT INTO peer_pairing_revision VALUES(?,1,?,?,?,'activated',?,?,?)")
     .run(pairing.binding_id,id(),pairing.peer_service_id,boot,digest(pairing.transcript),canonical(pairing),digest(pairing));
    db.prepare('INSERT INTO peer_pairing_head VALUES(?,1)').run(pairing.binding_id);
   }
  }).immediate();
  let tick=100n,principalKind='human',limited=false,revokeOnWrite=false,currentInitiator=true,peerDatabase=target?.database_instance_id;
  let sourceChanged=false,transportChanged=false,expireOnCompletionWrite=false,executionGrantDenied=false,expireOnPlanWrite=false;
  let pendingSource=null;
  const domain={clock_domain_id:id()};
  const wallTime=()=>({tick:expireOnCompletionWrite&&db.prepare('SELECT 1 FROM automation_arm_completion').pluck().get()?tick+2000000000n:tick,
   domain,lower:epoch,upper:expireOnPlanWrite&&db.prepare('SELECT 1 FROM automation_execution_seal').pluck().get()?epoch+250000000n:epoch});
  const proof={id:id(),revision:'1',digest:digest('reviewed test profile')};
  pairing.expires_ns=String(epoch+3600000000000n);pairing.transcript_digest=digest(pairing.transcript);
  authEvidence.grant_ref=proof;
  authEvidence.credential={issuer_service_id:target?.authority_service_id??id(),token_id:id(),session_id:id(),session_revision:'1',credential_id:id(),
   policy_revision:'1',scope_digest:digest('issuer scope'),recent_before_ns:pairing.expires_ns};
  const counts={rows:'40',logical_bytes:scheduledRuntime?'56000':commandCase?'48000':delayCase||parameterCase?'26000':executionPlanning?'12000':'8000',queue_slots:'1',work_units:'60'};
  const source={state:'synchronized',source:{baseline_ref:{apply_id:id()},continuity:{connection_generation:'1',delivery_cursor:'2'}}};
  const settings={approval_file:path.join(f.dir,'publication-approval.json')};
  const installationApproval={format:'leafseamer.publication-policy-approval/2',policy_ref:f.content.policy_ref,not_before_ns:'0',expires_ns:'18446744073709551615',...limits};
  const policyBlob=canonical(installationApproval);
  settings.approval_sha256=digest(installationApproval);
  fs.writeFileSync(settings.approval_file,policyBlob);
  const installed=hasTarget?openInstalledPublicationPolicy(settings,{identity:f.identity,clock:{current:()=>({lower_ns:String(epoch),upper_ns:String(epoch)})}}):null;
  let ackCursor='2';
  const control={control_ref:{id:id(),revision:'1',digest:digest('device control original')},control_epoch:id(),effective_version:'0',
   mode:'automatic',gate_state:'protected',pending_operation_id:null};
  const policy=ref('run_endorsement_policy',target,id(),digest('target policy original'));
  let seamerTransport,observeActionBinding,observedAfterReceipt,observedActionPreparation,remoteProtection,revocationId,scheduledActionRead,observedScheduled;
  let commandSubmits=0,commandLookups=0,commandFound=false,sentCommand=null,savedAdjudication=null,terminalReply=null,terminalReads=0;
  const assembly={db,service:{...f.identity,schema_revision},bootId:boot,isOpen:()=>db.open,
   publications:automationPublicationStore({db,service:{...f.identity,schema_revision},isOpen:()=>db.open}),eventAgePolicy:installed?.eventAgePolicy,
   publicationBindingsCurrentInTransaction:()=>true,
   authority:{authorizeInTransaction({operation_id,definition_id,context_id}){return {operation_id,definition_id,context_id,scope,
    request_epoch:f.identity.request_epoch,actor:{...actor,principal_kind:principalKind},authorization_digest:digest(authEvidence),authentication_evidence:authEvidence};},
   assertRunCapacityInTransaction(){},identifyRunApprovalActorInTransaction:()=>({...actor,principal_kind:principalKind}),runApprovalTimeInTransaction:wallTime,
   assertArmInitiatorInTransaction(initiator,definition,grant){assert.deepEqual(initiator,actor);assert.equal(definition,draft.definition_id);assert.deepEqual(grant,proof);
    if(!currentInitiator)throw Object.assign(Error('initiator withdrawn'),{code:'test_arm_initiator_withdrawn'});return epoch+500000000n;}},
   resolveAutomationSourcesInTransaction:()=>[sourceChanged?{...source,state:'desynchronized'}:
    pendingSource?{state:'baseline_required',source:pendingSource}:source],
   prepareSourceTransport:async()=>({observation:{state:'ready',connection_generation:transportChanged?'2':'1',ack_cursor:ackCursor},assertCurrentInTransaction(){}}),
   resolveDeviceActionsInTransaction(_id,actions){assert.equal(actions.length,1);return [{binding_id:actions[0].binding_id,controller_scope:target,device_action:action}];},
   currentPairingInTransaction(reference){assert.equal(reference.id,pairing.binding_id);return pairing;},
   sendArmPreparation(...args){return seamerTransport.submitRunPreparation(...args);},
   lookupArmPreparation(...args){return seamerTransport.lookupRunPreparation(...args);},
   observeArmPreparation(...args){return seamerTransport.prepareRunPreparationEligibility(...args);},
   submitAutomationCommand(...args){return seamerTransport.submitAutomationCommand(...args);},
   lookupAutomationCommand(...args){return seamerTransport.lookupAutomationCommand(...args);},
   readAutomationCommandResult(...args){return seamerTransport.readAutomationCommandResult(...args);},
   sendApprovalProtection(...args){return seamerTransport.deliverRunApprovalProtection(...args);},
   authorizeControllerPeerInTransaction(binding,socket){assert.equal(binding,pairing.binding_id);if(socket)assert.equal(socket.authorized,true);
    return {record:pairing,peer:{service_id:target.authority_service_id,database_instance_id:peerDatabase,service_kind:'controller'}};},
   prepareEndorsement:async(_binding,object_ref)=>({original:{object_ref,endorsement:{authorization_basis_refs:[policy]}},
    pairing_ref:{id:pairing.binding_id,revision:pairing.revision,digest:pairing.digest},devices:[{device_id:action.device_id,control}],assertCurrentInTransaction(){}}),
   prepareRunActionObservations:async()=>()=>{},prepareServiceApproval:async(_binding,_principal,approval_ref)=>({pairing_ref:proof,
    original:{issuer_scope:target??scope,approval_ref,approval:{not_before_ns:'0',expires_ns:String(epoch+86400000000000n),automation_origin:{seamer_scope:scope}}},assertCurrentInTransaction(){}}),
   armAdmissionPolicy:{inspectInTransaction(){assert.ok(db.inTransaction);
    if(revokeOnWrite&&db.prepare('SELECT 1 FROM automation_arm_budget').pluck().get())return null;
    return {profile_ref:proof,qualification_ref:proof,capacity:{...counts,rows:limited?'39':'40'},required:counts};}}};
  const schedulesFor=(data,bootId)=>scheduleStore({db:data,service:assembly.service,bootId,isOpen:()=>data.open,
   authority:assembly.authority,publications:assembly.publications});
  if(scheduleCase)assembly.schedules=schedulesFor(db,boot);
  const runs=automationRunStore(assembly);
  t.after(()=>runs.stopExecutionTimers?.());
  let preCommitTick=null,postCommitTick=null;
  const verified=hasTarget?eventStream(db,assembly.service,boot,registration,runs.triggerDecisions,true,withAnchors?(clock,inTransaction)=>{
   if(!db.prepare('SELECT 1 FROM automation_execution_seal').pluck().get()||postCommitTick!==null)return;
   if(inTransaction)preCommitTick=clock.tick();
   else{
    assert.notEqual(preCommitTick,null);
    inspect(f.dir,other=>assert.equal(other.prepare('SELECT count(*) FROM automation_execution_seal').pluck().get(),1));
    clock.advance(100n);postCommitTick=clock.tick();
   }
  }:null):null;
  if(verified){source.source.baseline_ref=verified.baseline;t.after(verified.close);}
  ackCursor='1';assert.equal((await runs.recordArmPreflight(null,run.run_id,'2')).preflight.observation.can_arm,false);ackCursor='2';
  transportChanged=true;assert.equal((await runs.recordArmPreflight(null,run.run_id,'2')).preflight.observation.can_arm,false);transportChanged=false;
  limited=true;assert.equal((await runs.recordArmPreflight(null,run.run_id,'2')).preflight.observation.can_arm,false);limited=false;
  const first=await runs.recordArmPreflight(null,run.run_id,'2'),second=await runs.recordArmPreflight(null,other.run_id,'2');
  assert.equal(first.preflight.observation.can_arm,true);
  const request={context:{context_id:contextId,idempotency_key:id()},preflight_ref:first.preflight_ref};
  principalKind='service';await assert.rejects(runs.armRun(null,run.run_id,request),{code:'run_arm_authorization'});principalKind='human';
  db.exec("CREATE TRIGGER test_arm_ignore BEFORE INSERT ON automation_arm_notification BEGIN SELECT RAISE(IGNORE); END");
  await assert.rejects(runs.armRun(null,run.run_id,request),{code:'run_arm_storage'});
  assert.equal(runs.getActivationSlot(null,draft.definition_id).active_run_id,null);
  assert.equal(runs.get(null,run.run_id).revision,'2');
  assert.equal(db.prepare('SELECT count(*) FROM automation_arm_budget').pluck().get(),0);
  db.exec('DROP TRIGGER test_arm_ignore');
  revokeOnWrite=true;
  await assert.rejects(runs.armRun(null,run.run_id,request),{code:'run_arm_policy_changed'});
  assert.equal(runs.getActivationSlot(null,draft.definition_id).active_run_id,null);
  assert.equal(runs.get(null,run.run_id).revision,'2');
  revokeOnWrite=false;
  const [saved,replay]=await Promise.all([runs.armRun(null,run.run_id,request),runs.armRun(null,run.run_id,request)]);
  assert.deepEqual(saved,replay);assert.equal(saved.operation.targets.length,Number(hasTarget));
  const intents=runs.armPreparationIntents(saved.operation_ref);assert.equal(intents.length,Number(hasTarget));
  const observe=()=>runs.armPreparationObservations(saved.operation_ref);
  assert.deepEqual(observe(),saved.operation.targets.map(({controller_scope,preparation_id})=>({controller_scope,preparation_id,
   state:'pending',receipt_ref:null,target_grant_ref:null})));
  if(hasTarget)await assert.rejects(runs.completeArmRun(null,saved.operation_ref),{code:'run_preparation_receipts_incomplete'});
  else assert.deepEqual((await runs.prepareArmCompletionEvidence(saved.operation_ref)).original.targets,[]);
  if(hasTarget){
   const {controller_scope,device_actions,preparation_id,state}=saved.operation.targets[0];
   assert.deepEqual(controller_scope,target);assert.deepEqual(device_actions,[action]);assert.equal(state,'pending');
   assert.equal(db.prepare('SELECT preparation_id FROM automation_arm_target').pluck().get(),preparation_id);
   const {object_id:operation_id,digest:operation_digest}=saved.operation_ref,request={operation_id,operation_digest,challenge_id:id()};
   const read=(query=request)=>runs.readArmInitiatorForPeer(pairing.binding_id,null,query);
   const original=read();
   assert.deepEqual(original.initiator,actor);assert.equal(original.preparation_id,preparation_id);assert.equal(original.arm_generation,'1');
   assert.equal(original.eligible_before_ns,String(epoch+500000000n));
   assert.equal(original.initiator_authorization_ref.digest,digest(authEvidence));
   const reply={format:'leafseamer.run-initiator-response/1',challenge_id:request.challenge_id,pair_attempt_id:pairing.transcript.pair_attempt_id,
    transcript_digest:pairing.transcript_digest,body:original};
   assert.deepEqual(runInitiatorReadReply(reply,request,pairing,scope,target),original);
   assert.throws(()=>runInitiatorReadReply({...reply,challenge_id:id()},request,pairing,scope,target),{code:'peer_pairing_run_initiator_response'});
   assert.throws(()=>read({...request,operation_digest:digest('wrong proof')}),{code:'run_initiator_missing'});
   peerDatabase=id();assert.throws(read,{code:'run_initiator_scope'});peerDatabase=target.database_instance_id;
   currentInitiator=false;assert.throws(read,{code:'test_arm_initiator_withdrawn'});currentInitiator=true;
   const clockId=domain.clock_domain_id;domain.clock_domain_id=id();
   assert.throws(read,{code:'run_initiator_expired'});domain.clock_domain_id=clockId;
   const [intent]=intents,plan=intent.request.plan_ref;
   assert.equal(intent.binding_id,pairing.binding_id);assert.equal(plan.object_id,preparation_id);
   assert.deepEqual(runs.armPreparationLookupIntent(saved.operation_ref,target),{binding_id:intent.binding_id,lookup:intent.lookup});
   assert.equal(intent.request.grant_intent.validity.authorization_policy_revision,policy.revision);
   const planRequest={preparation_id,plan_digest:plan.digest,challenge_id:id()};
   const release=(query=planRequest)=>runs.prepareArmPlanForPeer(pairing.binding_id,null,query);
   const prepared=await release();assert.deepEqual(prepared.beforeSend(),prepared.body);
   assert.deepEqual(prepared.body.plan_ref,plan);assert.equal(digest(prepared.body.original),plan.digest);
   assert.deepEqual({...prepared.body.original.preparation,plan_ref:plan},intent.request);
   assert.deepEqual(prepared.body.original.controls,[{device_id:action.device_id,control_ref:control.control_ref}]);
   assert.equal(prepared.body.eligible_before_ns,original.eligible_before_ns);
   await assert.rejects(release({...planRequest,plan_digest:digest('different plan')}),{code:'run_preparation_plan_reference'});
   peerDatabase=id();await assert.rejects(release(),{code:'run_preparation_plan_target'});peerDatabase=target.database_instance_id;
   transportChanged=true;const replaced=await release();assert.throws(replaced.beforeSend,{code:'run_preparation_plan_source'});transportChanged=false;
   sourceChanged=true;assert.throws(prepared.beforeSend,{code:'run_preparation_plan_source'});sourceChanged=false;
   assert.throws(prepared.beforeSend,{code:'run_preparation_plan_changed'});
   const beforeWithdrawal=await release();currentInitiator=false;
   assert.throws(beforeWithdrawal.beforeSend,{code:'test_arm_initiator_withdrawn'});currentInitiator=true;

   const {leaves:[aMaterial,bMaterial],ca}=peerCertificates();
   let tickOffset=0n;
   const clock={pairingSample:()=>({lower_ns:String(epoch),upper_ns:String(epoch),tick_ns:String(process.hrtime.bigint()+tickOffset),runtime_domain_ref:domain})};
   async function peer(serviceScope,service_kind,material,options){
    const identity={service_id:serviceScope.authority_service_id,database_instance_id:serviceScope.database_instance_id,
     service_kind,issuer_id:id(),issuer_epoch:id(),credential_id:id()};
    let record;
    const pairings={bindConfiguration(){},currentPending(binding){assert.equal(binding,record.binding_id);return record;},
     requireActivatedInTransaction(reference){assert.deepEqual(reference,{id:record.binding_id,revision:'1',digest:record.digest});return record;}};
    const transport=await startPeerPairingTransport({pairings,clock,identity,...material,ca,...options});t.after(()=>transport.close());
    return {transport,record(value){record=value;},describe(){return {identity,cert:material.cert,origin:transport.origin,
     server_name:'127.0.0.1',trust_bundle_digest:transport.configuration.trust_bundle_digest};}};
   }
   const endorsement=first.preflight.basis.authority.endorsements[0].endorsement_ref;
   let remoteReceipt=null,lookupOverride=null,loseReply=true,eligible=true,indeterminate=false,reportMissing=false,submitCalls=0;
   function makeRecord(endorsement_ref){
    const {grant_intent,approval_ref,definition_version,arm_generation,run_id,origin_scope}=intent.request;
    const {principal,device_actions,parameter_constraints,validity}=grant_intent,request_digest=intent.lookup.request_digest;
    const grant={grant_id:id(),revision:'1',origin_scope,run_id,definition_version,arm_generation,principal,issuer_id:principal.issuer_id,
     issuer_epoch:principal.issuer_epoch,device_actions,parameter_constraints,validity,state:'active',approval_ref,endorsement_ref};
    const receipt={receipt_id:id(),revision:'1',controller_scope:target,preparation_id,request_digest,grant,
     grant_ref:ref('grant',target,grant.grant_id,digest(grant)),commit_ref:ref('controller_commit',target,id(),digest('commit'))};
    return {request:intent.request,request_digest,revision:'2',state:'applied',applied_receipt:{...receipt,receipt_digest:digest(receipt)},protection_ref:null};
   }
   let submittingSocket,submittedAdjudication;
   const queriedSockets=new WeakSet();
   const commands=commandCase?{
    async submit(_binding,socket,request){
     assert.ok(++commandSubmits<=(scheduledRuntime?2:1));
     assert.equal(db.inTransaction,false);
     inspect(f.dir,other=>{
      const saved=other.prepare(`SELECT s.body_jcs FROM automation_execution_submission s
       JOIN automation_execution_submission_attempt a USING(execution_id,step_id)
       WHERE s.execution_id=? AND s.step_id=?`).pluck().get(request.action.execution_ref.object_id,request.action.step_id);
      assert.deepEqual(JSON.parse(saved).request,request);
     });
     const observed=await observeActionBinding(request.action);
     assert.deepEqual(observed.original.parameters.body.parameters,{value:true});
     if(observed.original.execution.origin_kind==='schedule'){
      assert.equal(scheduledRuntime,true);
      const adoption=controllerActions?.actionAdmissionBinding(observed.original,controllerActions.actionAdmissionRequest(request.action,target),observedActionPreparation);
      observedScheduled={binding:observed.original,adoption};
     }
     assert.deepEqual(request.target,action);
     sentCommand=request;
     const principal=remoteReceipt.applied_receipt.grant.principal;
     const decision=commandCase==='rejected'?'rejected':'accepted';
     const original={contract_version:'command-admission/0.1.0-draft.1',revision:'1',adjudication_id:id(),decision,
      identity:automationCommandIdentity(request,principal),command_ref:decision==='accepted'?ref('command',target,id(),digest('queued action')):null,
      commit_ref:ref('controller_commit',target,id(),digest('action adjudication'))};
     savedAdjudication={...original,adjudication_digest:digest(original)};
     submittingSocket=socket;
     submittedAdjudication=savedAdjudication;
     if(commandCase==='lost')socket.destroy();
     if(commandCase==='wrong_subject'){
      const other={...original,identity:automationCommandIdentity(request,{...principal,principal_id:id()})};
      submittedAdjudication={...other,adjudication_digest:digest(other)};
     }
     return {created:true,adjudication:submittedAdjudication};
    },lookup(_binding,socket,request){
     assert.deepEqual(request,sentCommand);
     if(socket===submittingSocket)return {state:'found',adjudication:submittedAdjudication};
     if(!queriedSockets.has(socket)){queriedSockets.add(socket);commandLookups++;}
     return commandFound?{state:'found',adjudication:savedAdjudication}:{state:'not_found',adjudication:null};
    },result(_binding,socket,request){
     assert.equal(socket.authorized,true);
     assert.deepEqual(request,sentCommand);terminalReads++;
     return {adjudication:savedAdjudication,terminal:terminalReply};
    }
   }:null;
   const [a,b]=await Promise.all([peer(target,'controller',aMaterial,{automationCommands:commands,runApprovalProtection(_binding,socket,notice,options){
    assert.equal(socket.authorized,true);assert.equal(options.completion,true);
    assert.equal(digest(notice),(remoteProtection.rejection??remoteProtection).request_digest);return remoteProtection;
   },runPreparations:{prepare(_binding,socket,submitted){
    assert.deepEqual(submitted,intent.request);
    submitCalls++;
    const created=remoteReceipt===null;remoteReceipt??=makeRecord(endorsement);
    if(loseReply)socket.destroy();return {created,record:remoteReceipt};
   },lookup(_binding,_socket,query){
    assert.deepEqual(query,intent.lookup);
    if(indeterminate)return {state:'indeterminate',record:null};
    if(reportMissing)return {state:'not_found',record:null};
    return remoteReceipt?{state:'found',record:lookupOverride??remoteReceipt}:{state:'not_found',record:null};
   },prepareEligibility(_binding,_socket,request){
    const receipt=remoteReceipt.applied_receipt;
    assert.deepEqual(request,{lookup:intent.lookup,grant_ref:receipt.grant_ref,receipt_ref:ref('preparation_receipt',target,receipt.receipt_id,digest(receipt))});
    if(!eligible||executionGrantDenied||parameterCase==='grant'&&postCommitTick!==null)throw Error('test target no longer eligible');
    const body={request,state:'eligible',eligible_before_ns:String(epoch+250000000n)};return {body,beforeSend:()=>body};
   }}}),peer(scope,'seamer',bMaterial,{runArmPlan:runs.prepareArmPlanForPeer,runActionBinding(...args){
    return scheduledActionRead?scheduledActionRead(...args):runs.prepareActionBindingForPeer(...args);
   }})]);
   a.transport.bindPeers([b.describe()]);b.transport.bindPeers([a.describe()]);
   const ap={...a.transport.configuration,binding_id:id(),permissions:['automation.prepare.read']};
   const bp={...b.transport.configuration,binding_id:pairing.binding_id,permissions:['automation.prepare','automation.prepare.read','automation.protect']};
   const transcript={pair_attempt_id:id(),parties:[ap,bp]},transcript_digest=digest(transcript);
   const active={revision:'1',state:'activated',transcript,transcript_digest,expires_ns:pairing.expires_ns,digest:digest('TLS pairing')};
   a.record({...active,binding_id:ap.binding_id,peer_service_id:bp.service_id});
   const seamerRecord={...active,binding_id:bp.binding_id,peer_service_id:ap.service_id,digest:pairing.digest};
   Object.assign(pairing,seamerRecord);b.record(seamerRecord);
   seamerTransport=b.transport;
   if(a.transport.prepareRunActionBinding)observeActionBinding=request=>a.transport.prepareRunActionBinding(ap.binding_id,request);
   const received=await a.transport.prepareRunArmPlan(ap.binding_id,plan);
   assert.deepEqual(received.original,prepared.body.original);assert.equal(received.assertCurrentInTransaction(),true);
   tickOffset=2000000000n;assert.throws(received.assertCurrentInTransaction,{code:'peer_pairing_preparation_expired'});tickOffset=0n;
   assert.throws(received.assertCurrentInTransaction,{code:'peer_pairing_preparation_expired'});
   await assert.rejects(a.transport.prepareRunArmPlan(ap.binding_id,{...plan,digest:digest('bad plan')}),{code:'peer_pairing_response'});
   sourceChanged=true;
   await assert.rejects(a.transport.prepareRunArmPlan(ap.binding_id,plan),{code:'peer_pairing_response'});sourceChanged=false;

   const lookup=()=>runs.lookupArmPreparation(saved.operation_ref,target),submit=()=>runs.submitArmPreparation(saved.operation_ref,target);
   const scan=after=>runs.scanArmPreparation({after,limit:1});
   const job={preparation_id,operation_ref:saved.operation_ref,target};
   assert.deepEqual(scan(null),{items:[job],next:null});
   assert.deepEqual(scan(preparation_id),{items:[],next:null});
   const restartedBeforeReceipt=automationRunStore({...assembly,bootId:id()});
   assert.deepEqual(restartedBeforeReceipt.scanArmPreparation({after:null,limit:1}),{items:[],next:null});
   await assert.rejects(restartedBeforeReceipt.deliverArmPreparation(job),{code:'run_initiator_ineligible'});
   await assert.rejects(runs.deliverArmPreparation({...job,preparation_id:id()}),{code:'run_preparation_plan_target'});
   currentInitiator=false;await assert.rejects(runs.deliverArmPreparation(job),{code:'test_arm_initiator_withdrawn'});currentInitiator=true;
   indeterminate=true;
   assert.deepEqual(await runs.deliverArmPreparation(job),{state:'pending',receipt_ref:null});
   indeterminate=false;assert.equal(submitCalls,0);
   assert.deepEqual(await lookup(),{state:'not_found',record:null});
   await assert.rejects(runs.deliverArmPreparation(job),{code:'ECONNRESET'});
   assert.equal(submitCalls,1);assert.equal(observe()[0].state,'pending');loseReply=false;
   assert.deepEqual(scan(null),{items:[job],next:null});
   reportMissing=true;
   assert.deepEqual(await runs.deliverArmPreparation(job),{state:'pending',receipt_ref:null});
   await assert.rejects(submit(),{code:'run_preparation_submission_unknown'});
   assert.equal(submitCalls,1);reportMissing=false;
   currentInitiator=false;sourceChanged=true;
   lookupOverride=makeRecord({...endorsement,object_id:id()});
   await assert.rejects(lookup(),{code:'run_preparation_receipt_binding'});lookupOverride=null;
   db.exec('CREATE TEMP TRIGGER omit_receipt_notice BEFORE INSERT ON automation_arm_preparation_notification BEGIN SELECT RAISE(IGNORE); END');
   await assert.rejects(lookup(),{code:'run_preparation_receipt_storage'});
   db.exec('DROP TRIGGER omit_receipt_notice');assert.equal(observe()[0].state,'pending');
   const delivered=await runs.deliverArmPreparation(job);
   const adopted=await lookup();assert.equal(adopted.state,'found');assert.deepEqual(adopted.record,remoteReceipt);
   observedActionPreparation=adopted.record;
   assert.deepEqual(delivered,{state:'delivered',receipt_ref:adopted.observation.receipt_ref});
   assert.deepEqual(await runs.deliverArmPreparation(job),{state:'acknowledged',receipt_ref:adopted.observation.receipt_ref});
   assert.deepEqual(scan(null),{items:[],next:null});assert.equal(submitCalls,1);
   observedAfterReceipt=observe();assert.equal(observedAfterReceipt[0].state,'receipt_observed');
   assert.deepEqual(observedAfterReceipt[0].target_grant_ref,remoteReceipt.applied_receipt.grant_ref);
   assert.deepEqual(await lookup(),adopted);
   lookupOverride=makeRecord(endorsement);await assert.rejects(lookup(),{code:'run_preparation_receipt_conflict'});lookupOverride=null;
   assert.deepEqual(observe(),observedAfterReceipt);
   currentInitiator=true;sourceChanged=false;
   assert.deepEqual(await submit(),{created:false,record:remoteReceipt,observation:adopted.observation});
   const evidence=await runs.prepareArmCompletionEvidence(saved.operation_ref);
   assert.equal(evidence.original.eligible_before_ns,String(epoch+250000000n));
   assert.deepEqual(evidence.original.targets[0].request.grant_ref,adopted.observation.target_grant_ref);
   assert.deepEqual(db.transaction(evidence.assertCurrentInTransaction).immediate(),evidence.original);
   const originalTick=tick;tick+=2000000000n;
   assert.throws(()=>db.transaction(evidence.assertCurrentInTransaction).immediate(),{code:'run_preparation_evidence_expired'});
   tick=originalTick;assert.throws(()=>db.transaction(evidence.assertCurrentInTransaction).immediate(),{code:'run_preparation_evidence_unavailable'});
   eligible=false;await assert.rejects(runs.prepareArmCompletionEvidence(saved.operation_ref),{code:'peer_pairing_response'});eligible=true;
   transportChanged=true;
   await assert.rejects(runs.prepareArmCompletionEvidence(saved.operation_ref),{code:'run_preparation_plan_source'});transportChanged=false;
   const beforeSourceChange=await runs.prepareArmCompletionEvidence(saved.operation_ref);sourceChanged=true;
   assert.throws(()=>db.transaction(beforeSourceChange.assertCurrentInTransaction).immediate(),{code:'run_preparation_plan_source'});sourceChanged=false;
   assert.throws(()=>db.transaction(beforeSourceChange.assertCurrentInTransaction).immediate(),{code:'run_preparation_evidence_unavailable'});
   assert.equal(db.prepare('SELECT count(*) FROM automation_arm_preparation_receipt').pluck().get(),1);
   assert.equal(db.prepare('SELECT count(*) FROM automation_arm_preparation_notification').pluck().get(),1);
  }
  assert.equal(saved.operation.initiator_authorization_ref.digest,digest(authEvidence));
  assert.equal(saved.operation.state,'reserved');
  assert.equal(runs.getActivationSlot(null,draft.definition_id).active_run_id,run.run_id);
  const reservedRun=runs.get(null,run.run_id);assert.equal(reservedRun.revision,'3');assert.equal(reservedRun.arm_generation,'1');
  assert.equal(reservedRun.active_operation_id,saved.operation.operation_id);assert.equal(reservedRun.lifecycle,'disarmed');assert.equal(reservedRun.dispatch_eligible,false);
  await assert.rejects(runs.armRun(null,other.run_id,{context:{context_id:contextId,idempotency_key:id()},preflight_ref:second.preflight_ref}),{code:'run_preflight_changed'});
  tick+=2000000000n;assert.deepEqual(await runs.armRun(null,run.run_id,request),saved);
  await assert.rejects(runs.armRun(null,run.run_id,{...request,preflight_ref:second.preflight_ref}),{code:'run_arm_conflict'});
  assert.equal(db.prepare('SELECT count(*) FROM automation_run_arm').pluck().get(),1);
  const complete=()=>runs.completeArmRun(null,saved.operation_ref);
  const checkpoints=['automation_arm_completion','automation_arm_completion_notification'];
  function checkPending(){
   for(const table of checkpoints)assert.equal(db.prepare('SELECT count(*) FROM '+table).pluck().get(),0);
   assert.deepEqual(runs.get(null,run.run_id),reservedRun);
   assert.equal(runs.getActivationSlot(null,draft.definition_id).active_operation_id,saved.operation.operation_id);
  }
  const tokenId=authEvidence.credential.token_id;authEvidence.credential.token_id=id();
  await assert.rejects(complete(),{code:'run_arm_completion_authorization'});checkPending();authEvidence.credential.token_id=tokenId;
  if(hasTarget){
   const evidence=(await runs.prepareArmCompletionEvidence(saved.operation_ref)).original;
   evidence.targets[0].request.lookup.controller_scope.database_instance_id=id();
   assert.throws(()=>db.transaction(()=>{
    const body={state:'ready',completed_run_revision:'4',evidence},operationId=saved.operation.operation_id;
    db.prepare('INSERT INTO automation_arm_completion VALUES(?,?,?,?,?,?,?)')
     .run(operationId,run.run_id,saved.operation.approval_ref.object_id,'3','4',canonical(body),digest(body));
    db.prepare('INSERT INTO automation_arm_completion_notification VALUES(?,?,?)').run(operationId,'{}',digest({}));
   }).immediate(),/arm_completion_precondition/);checkPending();
  }
  for(const table of checkpoints){
   db.exec(`CREATE TEMP TRIGGER omit_arm_completion BEFORE INSERT ON ${table} BEGIN SELECT RAISE(IGNORE); END`);
   await assert.rejects(complete(),{code:'run_arm_completion_storage'});checkPending();db.exec('DROP TRIGGER omit_arm_completion');
  }
  db.exec("CREATE TEMP TRIGGER omit_arm_head BEFORE UPDATE ON automation_run_approval_head WHEN NEW.run_revision='4' BEGIN SELECT RAISE(IGNORE); END");
  await assert.rejects(complete(),/arm_completion_revision/);checkPending();db.exec('DROP TRIGGER omit_arm_head');
  expireOnCompletionWrite=true;
  await assert.rejects(complete(),{code:'run_preparation_evidence_expired'});checkPending();expireOnCompletionWrite=false;
  const unrelated=eventStream(db,assembly.service,boot);
  t.after(()=>unrelated.close());
  const latest=unrelated.append('1');
  assert.equal(first.preflight.basis.observed_receive_cursor.receive_sequence,'0');
  assert.equal(latest.receive_sequence,'1');
  const [ready,duplicate]=await Promise.all([complete(),complete()]);assert.deepEqual(duplicate,ready);
  assert.equal(ready.completion.state,'ready');assert.equal(ready.completion.completed_run_revision,'4');
  assert.equal(ready.completion.evidence.targets.length,Number(hasTarget));
  assert.deepEqual(ready.completion.ingest_baseline,{revision:'2',receive_cursor:latest,source_baseline_refs:[source.source.baseline_ref]});
  assert.equal(unrelated.append('2').receive_sequence,'2');
  unrelated.close();
  const armedAtBaseline=runs.get(null,run.run_id);
  assert.deepEqual(armedAtBaseline,{...reservedRun,revision:'4',lifecycle:'armed',active_operation_id:null,last_receive_seq:'1'});
  const occupied=runs.getActivationSlot(null,draft.definition_id);
  assert.equal(occupied.active_run_id,run.run_id);assert.equal(occupied.active_operation_id,null);
  assert.equal(db.prepare('SELECT count(*) FROM automation_arm_budget').pluck().get(),1);
  for(const table of checkpoints)assert.equal(db.prepare('SELECT count(*) FROM '+table).pluck().get(),1);
  let expected=armedAtBaseline;
  let originalExecution=null;
  if(hasTarget){
   const matchedId=id();
   let conditionLatch;
   try{
    assert.equal(verified.append('1',matchedId).receive_sequence,'3');
    const {decisions}=verified.read(matchedId).trigger_decision_set,decision=decisions.find(value=>value.run_id===run.run_id);
    assert.equal(decision.disposition,'protected');assert.equal(decision.reason,'reconciled_delivery');
    assert.deepEqual(decision.arm_completion_ref,ready.completion_ref);
    assert.equal(decision.ingest_revision_before,'2');assert.equal(decision.ingest_revision_after,'3');
    assert.equal(decision.run_revision_before,'4');assert.equal(decision.run_revision_after,'4');assert.equal(decision.execution_id,null);
    if(scheduleCase)executionGrantDenied=true;
    const exact=await verified.live('2',{value:false,request_observation:false});
    const result=event=>event.trigger_decision_set.decisions.find(value=>value.run_id===run.run_id);
    if(scheduleCase){
     assert.equal(result(exact).reason,'execution_grant_unavailable');
     executionGrantDenied=false;
    }else{
     assert.equal(result(exact).disposition,'no_match');assert.equal(result(exact).evaluation.outcome,'no_match');
    }
    assert.equal(exact.trigger_decision_set.set.live_evidence.upper_age_ms,1001);
    if(executionPlanning){
     executionGrantDenied=true;
     const unavailable=await verified.live('3',{value:true});
     assert.equal(result(unavailable).reason,'execution_grant_unavailable');assert.equal(result(unavailable).execution_id,null);
     executionGrantDenied=false;
     const before=runs.get(null,run.run_id);
     for(const table of ['automation_execution','automation_execution_step_plan','automation_execution_step_state','automation_execution_seal','automation_execution_head']){
      db.exec(`CREATE TEMP TRIGGER test_omit_plan BEFORE INSERT ON ${table} BEGIN SELECT RAISE(IGNORE); END`);
      await assert.rejects(verified.live('4',{value:true}),{code:'execution_plan_storage'});
      assert.deepEqual(runs.get(null,run.run_id),before);
      assert.equal(db.prepare('SELECT count(*) FROM automation_execution').pluck().get(),0);
      db.exec('DROP TRIGGER test_omit_plan');
     }
     expireOnPlanWrite=true;
     await assert.rejects(verified.live('4',{value:true}),{code:'execution_grant_changed'});
     assert.deepEqual(runs.get(null,run.run_id),before);
     assert.equal(db.prepare('SELECT count(*) FROM automation_execution').pluck().get(),0);
     expireOnPlanWrite=false;
     if(omitReady)db.exec('CREATE TEMP TRIGGER test_omit_ready BEFORE INSERT ON automation_execution_step_ready BEGIN SELECT RAISE(IGNORE); END');
     const deferProjection=label==='等待当前触发事件的来源投影后才封存动作参数';
     const accepted=await verified.live('4',{value:true},0n,deferProjection),decision=result(accepted);
     if(omitReady)db.exec('DROP TRIGGER test_omit_ready');
     assert.equal(decision.disposition,'accepted');assert.equal(decision.evaluation.selected_branch,stateMachine?'start':'rule');
     originalExecution=runs.getExecution(null,decision.execution_id);
     if(deferProjection){
      pendingSource={baseline_ref:source.source.baseline_ref,source_key:accepted.origin.source_key,
       source_revision:accepted.trigger_decision_set.set.live_evidence.predecessor.source_revision};
      await wait(60);
      const waiting=runs.getExecution(null,decision.execution_id);
      assert.equal(waiting.protection,null);
      assert.equal(waiting.steps[0].prepared_parameters,null);
      verified.projectLive('4');pendingSource=null;
     }
     assert.equal(originalExecution.execution.decision_ref.object_id,decision.decision_id);
     assert.deepEqual(originalExecution.execution.receive_cursor,accepted.receive_cursor);
     assert.deepEqual(originalExecution.execution.grant_basis.map(value=>value.grant_ref),[ready.completion.evidence.targets[0].request.grant_ref]);
     const originalSteps=originalExecution.steps,actionStep=originalSteps.at(-2),waitingDelay=originalSteps.at(-1);
     assert.deepEqual(originalSteps.map(({plan,state})=>[plan.step_id,plan.payload.kind,state.state]),[
      ...(delayCase?[['before-a','delay','waiting'],['before-b','delay','waiting']]:[]),['first','action','waiting'],['after-action','delay','waiting']]);
     assert.equal(actionStep.plan.payload.binding.grant_ref.digest,originalExecution.execution.grant_basis[0].grant_ref.digest);
     assert.deepEqual(waitingDelay.plan.dependencies,[{from_step_id:'first',to_step_id:'after-action',edge_kind:'sequence'}]);
     if(withAnchors){
      assert.equal(postCommitTick,preCommitTick+100n);
      assert.equal(originalExecution.steps[1].ready,null);
     }
     if(omitReady){
      assert.equal(originalExecution.anchor,null);assert.equal(originalExecution.steps[0].ready,null);
      assert.equal(db.prepare('SELECT count(*) FROM automation_execution_step_ready').pluck().get(),0);
     }else if(withAnchors){
      const {anchor,steps:[{ready,state}]}=originalExecution;
      assert.equal(anchor.body.clock.tick_ns,String(postCommitTick));
      assert.deepEqual(anchor.body.execution_ref,originalExecution.execution_ref);
      assert.deepEqual(anchor.body.origin_ref,originalExecution.execution.decision_ref);
      assert.deepEqual(ready.body.execution_anchor_ref,anchor.ref);
      assert.equal(ready.body.step_plan_digest,state.plan_digest);
      assert.deepEqual(ready.body.clock.runtime_domain_ref,anchor.body.clock.runtime_domain_ref);
      assert.ok(BigInt(ready.body.clock.tick_ns)>=postCommitTick);
      assert.throws(()=>db.exec('UPDATE automation_execution_anchor SET body_digest=body_digest'),/immutable_execution_anchor/);
      assert.throws(()=>db.exec('DELETE FROM automation_execution_step_ready'),/immutable_execution_step_ready/);
     }
     let busy=null;
     const poll=async condition=>{
       const until=performance.now()+5000;
       while(performance.now()<until){
        const current=runs.getExecution(null,decision.execution_id);
        if(condition(current))return current;
        await wait(10);
       }
       assert.fail('本机执行没有推进到预期状态');
     };
     async function checkParameters(current){
      const {ready,prepared_parameters}=current.steps.at(-2);
      const {body}=prepared_parameters;
      assert.deepEqual(body.parameters,{value:true});
      assert.equal(body.parameters_digest,digest({value:true}));
      assert.equal(body.origin_digest,decision.evaluation.origin_digest);
      assert.deepEqual(body.step_ready_ref,ready.ref);
      assert.deepEqual(body.delay_window_refs,ready.body.delay_window_refs??[]);
      assert.deepEqual(body.grant_ref,current.execution.grant_basis[0].grant_ref);
      assert.equal(runs.get(null,run.run_id).dispatch_eligible,false);
      assert.throws(()=>db.exec('DELETE FROM automation_execution_action_parameters'),/immutable_execution_action_parameters/);
      const {lookup,grant_ref,receipt_ref}=current.execution.grant_basis[0];
      const request={preparation:{lookup,grant_ref,receipt_ref},execution_ref:current.execution_ref,step_id:ready.body.step_id,
       parameters_ref:prepared_parameters.ref};
      const view=runs.prepareActionBindingForPeer(pairing.binding_id,{authorized:true},request);
      assert.deepEqual(view.body.parameters,prepared_parameters);assert.deepEqual(view.beforeSend(),view.body);
      if(actionControllerRoot)assert.equal(typeof observeActionBinding,'function');
      const verifiedAction=observeActionBinding?await observeActionBinding(request):null;
      if(verifiedAction){
       assert.deepEqual(verifiedAction.original,view.body);
       assert.equal(db.transaction(verifiedAction.assertCurrentInTransaction).immediate(),true);
      }
      if(controllerActions){
       const normalized=controllerActions.actionAdmissionRequest(request,target);
       const adopted=controllerActions.actionAdmissionBinding(verifiedAction.original,normalized,observedActionPreparation);
       assert.deepEqual(adopted.parameters,body.parameters);assert.deepEqual(adopted.delay_window_refs,body.delay_window_refs);
       assert.deepEqual(adopted.grant_ref,body.grant_ref);assert.deepEqual(adopted.target,action);
      }
      assert.throws(()=>runs.prepareActionBindingForPeer(pairing.binding_id,{authorized:true},
       {...request,parameters_ref:{...request.parameters_ref,digest:digest('other parameters')}}),{code:'execution_action_binding'});
      if(observeActionBinding)await assert.rejects(observeActionBinding({...request,parameters_ref:{...request.parameters_ref,digest:digest('other parameters')}}),
       {code:'peer_pairing_response'});
      peerDatabase=id();
      assert.throws(view.beforeSend,{code:'execution_action_peer'});
      if(observeActionBinding)await assert.rejects(observeActionBinding(request),{code:'peer_pairing_response'});
      peerDatabase=target.database_instance_id;
      assert.throws(view.beforeSend,{code:'execution_action_binding'});
     }
     if(delayCase){
      const [initial]=originalSteps;
      assert.equal(initial.elapsed,null);
      assert.equal(initial.window.body.due_tick_ns,String(postCommitTick+5000000n));
      assert.equal(initial.window.body.deadline_tick_ns,String(postCommitTick+15000000n));
      assert.equal(actionStep.ready,null);
      if(delayCase==='elapsed'){
       verified.advanceReceiver(5000000n);
       const advanced=await poll(value=>value.steps[1].window);
       const [first,second]=advanced.steps;
       assert.equal(first.elapsed.body.clock.tick_ns,first.window.body.due_tick_ns);
       assert.deepEqual(second.ready.body.delay_window_refs,[first.window.ref]);
       assert.equal(second.window.body.due_tick_ns,String(postCommitTick+15000000n));
       verified.advanceReceiver(10000000n);
       const ready=await poll(value=>value.steps[2].ready);
       assert.deepEqual(ready.steps[2].ready.body.delay_window_refs,[second.window.ref]);
       assert.equal(ready.steps[3].ready,null);
       assert.equal(runs.get(null,run.run_id).dispatch_eligible,false);
       if(parameterCase)await checkParameters(await poll(value=>value.steps[2].prepared_parameters));
       verified.advanceReceiver(20000000n);
      }else{
       if(delayCase==='source')sourceChanged=true;
       if(delayCase==='generation')source.source.continuity.connection_generation='2';
       if(delayCase==='rollback')db.exec("CREATE TEMP TRIGGER test_omit_successor BEFORE INSERT ON automation_execution_step_ready WHEN NEW.step_id='before-b' BEGIN SELECT RAISE(IGNORE); END");
       verified.advanceReceiver(delayCase==='late'?15000000n:5000000n);
      }
      originalExecution=await poll(value=>value.protection);
      assert.equal(originalExecution.protection,['elapsed','late'].includes(delayCase)?'delay_window_expired':'execution_runtime_unavailable');
      if(delayCase!=='elapsed'){
       assert.equal(originalExecution.steps[0].elapsed,null);assert.equal(originalExecution.steps[1].ready,null);
      }
      if(delayCase==='rollback')db.exec('DROP TRIGGER test_omit_successor');
      sourceChanged=false;
      if(delayCase==='generation')source.source.continuity.connection_generation='1';
     }else if(parameterCase){
      assert.equal(actionStep.prepared_parameters,null);
      if(commandCase){
       const readStep=value=>value.steps[0];
       const table='automation_execution_submission_attempt';
       if(commandCase==='rollback')db.exec(`CREATE TEMP TRIGGER test_omit_attempt BEFORE INSERT ON ${table} BEGIN SELECT RAISE(IGNORE); END`);
       originalExecution=await poll(value=>commandCase==='accepted'||withTerminals?readStep(value).submission?.state==='accepted':value.protection);
       assert.equal(readStep(originalExecution).state.state,'waiting');
       assert.equal(originalExecution.steps[1].ready,null);
       assert.equal(runs.get(null,run.run_id).dispatch_eligible,false);
       if(commandCase==='rollback'){
        db.exec('DROP TRIGGER test_omit_attempt');
        assert.equal(readStep(originalExecution).submission.state,'planned');
        assert.equal(db.prepare(`SELECT count(*) FROM ${table}`).pluck().get(),0);
        assert.equal(commandSubmits,0);assert.equal(commandLookups,0);
       }else{
        const readDb=new Database(filename);
        const recovered=automationRunStore({...assembly,db:readDb,bootId:id(),isOpen:()=>readDb.open});
        try{
         const first=readStep(originalExecution).submission;
         const unresolved=['lost','wrong_subject'].includes(commandCase);
         if(unresolved){
          for(let n=0;commandLookups<1&&n<80;n++)await wait(10);
          assert.equal(first.state,'indeterminate');assert.equal(first.adjudication,null);
          await assert.rejects(async()=>{
           const denied=automationRunStore({...assembly,db:readDb,bootId:id(),isOpen:()=>readDb.open,
            authority:{...assembly.authority,authorizeInTransaction(){throw Error('denied read');}}});
           await denied.reconcileAutomationSubmission(null,decision.execution_id,'first');
          },/denied read/);
          commandFound=true;
         }
         if(commandCase==='lost'){
          recovered.startSubmissionRecovery();
          for(let n=0;n<65;n++){
           const current=recovered.getExecution(null,decision.execution_id);
           if(readStep(current).submission.state==='accepted')break;
           await wait(100);
          }
          assert.equal(readStep(recovered.getExecution(null,decision.execution_id)).submission.state,'accepted');
         }else assert.deepEqual(await recovered.reconcileAutomationSubmission(null,decision.execution_id,'first'),savedAdjudication);
         assert.deepEqual(await recovered.reconcileAutomationSubmission(null,decision.execution_id,'first'),savedAdjudication);
         originalExecution=runs.getExecution(null,decision.execution_id);
         const final=readStep(originalExecution).submission;
         assert.deepEqual(final.attempt,first.attempt);assert.deepEqual(final.adjudication,savedAdjudication);
         assert.equal(final.state,commandCase==='rejected'?'rejected':'accepted');assert.equal(commandSubmits,1);
         assert.equal(commandLookups,unresolved?2:0);
         assert.equal(originalExecution.protection,commandCase==='accepted'||withTerminals?null:'execution_runtime_unavailable');
         if(commandCase==='accepted'||withTerminals){
          const reread=await observeActionBinding(sentCommand.action);
          assert.deepEqual(reread.original.parameters,readStep(originalExecution).prepared_parameters);
         }else await assert.rejects(observeActionBinding(sentCommand.action));
         if(withTerminals){
          const readResult=()=>seamerTransport.readAutomationCommandResult(pairing.binding_id,sentCommand);
          assert.deepEqual(await readResult(),{adjudication:savedAdjudication,terminal:null});
          const activeDependency=commandCase==='terminal_dependency'||r5;
          const afterProtection=commandCase==='terminal_after_protection';
          const suppliedSuccess=commandCase!=='terminal_unknown'&&!r5Optional,command_ref=savedAdjudication.command_ref;
          const evidence=kind=>({id:command_ref.object_id+'.'+kind,revision:'1',digest:digest(kind)});
          const state={command_id:command_ref.object_id,scope:target,revision:'5',phase:'finished',
           outcome:suppliedSuccess?'succeeded':r5Optional?'failed':'unknown',effect_status:suppliedSuccess?'verified':r5Optional?'not_sent':'possible',
           resource_status:suppliedSuccess||r5Optional?'clear':'unresolved',automation_continuation:suppliedSuccess||r5Optional?'eligible':'paused_latched',
           reason_code:suppliedSuccess?null:r5Optional?'controller_command_failed':'evidence_unresolved',terminal_revision:'5',resolution_ref:null,attempt_ids:[id()],
           completion_evidence_refs:suppliedSuccess?[evidence('decision')]:[],resource_evidence_refs:suppliedSuccess||r5Optional?[evidence('release')]:[]};
          const terminal={format:'leafseamer.automation-command-terminal/1',command_ref,state,
           record_ref:{...command_ref,revision:'5',digest:digest('terminal record')},
           state_ref:{id:command_ref.object_id+'_state_5',revision:'5',digest:digest(state)},
           completion_achieved:suppliedSuccess?'effect_verified':null};
          terminalReply={...terminal,state_ref:{...terminal.state_ref,digest:digest('different state')}};
          await assert.rejects(readResult);
          if(afterProtection){
           sourceChanged=true;
           await poll(value=>value.protection==='execution_runtime_unavailable');
           sourceChanged=false;
          }
          if(r5Rollback)db.exec('CREATE TEMP TRIGGER omit_execution_release BEFORE DELETE ON automation_execution_head BEGIN SELECT RAISE(IGNORE); END');
          if(r5GrantRevoked)executionGrantDenied=true;
          terminalReply=terminal;
          const waitForTerminal=async condition=>{
           const until=performance.now()+8500;
           do{
            const current=(activeDependency?runs:recovered).getExecution(null,decision.execution_id);
            if(condition(current))return current;
            await wait(50);
           }while(performance.now()<until);
           assert.fail('终态查原未达到预期状态');
          };
          if(commandCase==='terminal_unknown'){
           readDb.exec('CREATE TEMP TRIGGER omit_terminal_protection BEFORE INSERT ON automation_execution_protection BEGIN SELECT RAISE(IGNORE); END');
           const before=terminalReads;
           recovered.startSubmissionRecovery();
           await waitForTerminal(()=>terminalReads>=before+2);
           await wait(150);
           recovered.stopExecutionTimers();
           assert.equal(readStep(recovered.getExecution(null,decision.execution_id)).submission.terminal,null);
           readDb.exec('DROP TRIGGER omit_terminal_protection');
          }
          if(!activeDependency)recovered.startSubmissionRecovery();
          const observed=await waitForTerminal(value=>r5?(r5Protected?value.reconciliation_required:value.completion):
           activeDependency?value.steps[1].elapsed:readStep(value).submission.terminal);
          assert.deepEqual(readStep(observed).submission.terminal,terminal);
          assert.equal(observed.protection,(suppliedSuccess||r5Optional)&&!afterProtection&&!r5Protected?null:'execution_runtime_unavailable');
          if(activeDependency){
           assert.deepEqual(observed.steps[1].ready.body.command_terminal_refs,[{step_id:'first',terminal_digest:digest(terminal)}]);
           assert.deepEqual(observed.steps[1].window.body.execution_anchor_ref,observed.anchor.ref);
           assert.equal(observed.steps[1].window.body.anchor_tick_ns,observed.anchor.body.clock.tick_ns);
           if(!r5)runs.stopExecutionTimers();
          }else assert.equal(observed.steps[1].ready,null);
          assert.equal(commandSubmits,1);
          assert.throws(()=>db.exec('DELETE FROM automation_execution_command_terminal'),/immutable_execution_command_terminal/);
          originalExecution=observed;
         }
         assert.throws(()=>db.exec(`DELETE FROM ${table}`),/immutable_execution_submission_attempt/);
        }finally{recovered.stopExecutionTimers();readDb.close();}
       }
       if(r5){
        const done=runs.get(null,run.run_id);
        assert.deepEqual(done.business_state,r5Protected?{state_id:'idle',revision:'1'}:stateMachine?{state_id:'live',revision:'2'}:null);
        assert.equal(done.revision,r5Protected?'5':'6');
        assert.equal(done.active_execution_id,r5Protected?decision.execution_id:null);
        assert.equal(done.last_execution_id,r5Protected?null:decision.execution_id);
        assert.equal(done.lifecycle,r5Protected?'paused':'armed');
        assert.equal(runs.getActivationSlot(null,draft.definition_id).revision,'2');
        if(commandCase==='r5_rule'){
         const {leaves:[listener],ca}=peerCertificates();
         const app=await startDraftHttp({cert:listener.cert,key:listener.key,bindingId:id(),service:assembly.service,bootId:boot,
          receiver:{async prepare(){return null;}},authorization:{},drafts:{},runs});
         try{
          const client=createClient('seamer',contractSelection,packet=>new Promise((resolve,reject)=>{
           const headers=Object.fromEntries([...packet.headers,['Authorization','Bearer v1.'+id()+'.'+'A'.repeat(43)]]);
           const request=https.request(new URL(packet.target,app.origin),{method:packet.method,headers,ca,agent:false},response=>{
            const body=[];response.on('data',chunk=>body.push(chunk));response.on('error',reject);
            response.on('end',()=>resolve({status:String(response.statusCode),headers:Object.entries(response.headers),body:Buffer.concat(body)}));
           });request.on('error',reject);request.end(packet.body);
          }));
          const slot=await client.call('getActivationSlot',{path:{definition_id:draft.definition_id}});
          assert.equal(slot.status,'200');
          assert.deepEqual(slot.body,{scope,definition_id:draft.definition_id,revision:'2',active_run_id:run.run_id,definition_version:published.definition_version});
         }finally{await app.close();}
        }
        assert.equal(db.prepare('SELECT count(*) FROM automation_execution_commit').pluck().get(),r5Protected?0:1);
        assert.equal(result(await verified.replayLive()).execution_id,decision.execution_id);
        if(r5Protected){
         if(r5Rollback)db.exec('DROP TRIGGER omit_execution_release');
         assert.equal(originalExecution.completion,null);
         assert.equal(originalExecution.reconciliation_required,true);
        }else{
         const {completion}=originalExecution;
         assert.equal(completion.notification.event_id,decision.execution_id+'.completed');
         assert.equal(completion.body.slot_revision,'2');
         assert.equal(completion.body.run_revision_before,'5');
         assert.equal(completion.body.run_revision_after,'6');
         assert.deepEqual(completion.body.business_state_before,stateMachine?{state_id:'idle',revision:'1'}:null);
         assert.deepEqual(completion.body.business_state_after,done.business_state);
         assert.equal(completion.body.steps[0].outcome,r5Optional?'failed':'succeeded');
         assert.throws(()=>db.exec('DELETE FROM automation_execution_commit'),/immutable_execution_commit/);
        }
        // 验证下一事件的状态和槽选择，仅观察受理。
        if(!scheduledRuntime)runs.stopExecutionTimers();
        if(scheduleCase){
         const schedules=assembly.schedules,scheduleId=id(),context=()=>({context_id:contextId,idempotency_key:id()});
         const item={item_id:'programme',one_shot_key:'show',local_start:new Date(sampledAt).toISOString().slice(0,-1),offset:'+00:00',
          steps:[{step_id:'first',role:'on_air',start_offset_ms:0,latest_start_offset_ms:1500}]};
         const input={context:context(),definition_id:draft.definition_id,definition_version:published.definition_version,
          expected_artifact_ref:published.artifact_ref,expected_version:null,items:[item,{...item,item_id:'overlap'}]};
         const version=schedules.publish(null,scheduleId,input);
         const runtimeSample=scheduledRuntime?verified.executionSample():null,tickBase=runtimeSample?BigInt(runtimeSample.tick_ns):0n;
         const tick=offset=>String(tickBase+BigInt(offset));
         const clockDomain=runtimeSample?.runtime_domain_ref??{clock_domain_id:'clock_'+'1'.repeat(64),descriptor_digest:digest('schedule domain')};
         const binding={domain:{...scope,...clockDomain}},anchor={binding,anchor_tick_ns:tick(0),exchange_start_tick_ns:tick(0),
          utc_at_anchor:{lower_ns:String(epoch-1010n),upper_ns:String(epoch-1000n)}};
         const reference={service_boot_id:boot,valid_before_tick_ns:tick(10000),state:{binding,status:'qualified'},
          bundle:{anchor,bundle_id:id(),bundle_digest:digest(anchor),profile:{elapsed_error_ppm:0,span_quantization_ns:'1'},
           policy:{max_half_width_ns:'25',max_anchor_age_ns:'20000',max_exchange_age_ns:'20000',max_upstream_age_ns:'20000'},
           error:{upstream_age_upper_at_m1_ns:'0'}}};
         const sample=offset=>({clock:{tick_ns:tick(offset),domain:clockDomain},reference});
         const activation=schedules.activate(null,scheduleId,{context:context(),expected_revision:null,schedule_version_ref:version.ref},()=>sample('500'));
         const occurrence=version.body.items[0],source={schedule_id:scheduleId,occurrence_id:occurrence.occurrence_id,activation_ref:activation.ref,run_id:run.run_id};
         const admit=(readClock=()=>sample('1020'),fixed=source)=>runs.admitScheduledOccurrence(fixed,readClock,scheduledRuntime?verified.executionSample:undefined);
         if(scheduledRuntime)verified.advanceReceiver(1020n);
         const viewAt=tick=>schedules.get(null,scheduleId,null,{readClock:()=>sample(tick),inspect:runs.inspectScheduleInTransaction}).occurrences[0];
         assert.equal(viewAt('999').qualification,undefined);
         assert.deepEqual(viewAt('1020').qualification,{state:'ready_for_preparation',selected_branch:'rule',first_step_id:'first',observed_tick_ns:tick(1020)});
         const {leaves:[listener],ca}=peerCertificates();
         const app=await startDraftHttp({cert:listener.cert,key:listener.key,bindingId:id(),service:assembly.service,bootId:boot,
          receiver:{async prepare(){return null;}},authorization:{},drafts:{},runs,localSchedules:schedules,scheduleClock:()=>sample('1020')});
         try{
          const body=Buffer.from(canonical({operation:'get',request:{schedule_id:scheduleId,version:null}}));
          const reply=await new Promise((resolve,reject)=>{
           const request=https.request(new URL('/auth/local-schedules/v1',app.origin),{method:'POST',ca,agent:false,
            headers:{Authorization:'Bearer v1.'+id()+'.'+'A'.repeat(43),'Content-Type':'application/json','Content-Length':body.length}},response=>{
            const chunks=[];response.on('data',chunk=>chunks.push(chunk));response.on('error',reject);
            response.on('end',()=>resolve({status:response.statusCode,body:JSON.parse(Buffer.concat(chunks))}));
           });
           request.on('error',reject);request.end(body);
          });
          assert.equal(reply.status,200);
          assert.deepEqual(reply.body.result.occurrences[0],viewAt('1020'));
         }finally{await app.close();}
         const untouched=()=>{
          assert.equal(db.prepare('SELECT count(*) FROM automation_execution').pluck().get(),1);
          assert.equal(db.prepare('SELECT count(*) FROM schedule_execution_admission').pluck().get(),0);
          assert.equal(db.prepare('SELECT count(*) FROM automation_execution_head').pluck().get(),0);
          assert.deepEqual(runs.get(null,run.run_id),done);
         };
         await assert.rejects(admit(()=>sample('999')),{code:'schedule_admission_window'});
         await assert.rejects(admit(undefined,{...source,activation_ref:{...activation.ref,digest:digest('stale activation')}}),{code:'schedule_activation_conflict'});
         executionGrantDenied=true;
         await assert.rejects(admit(),{code:'execution_schedule_grant'});
         executionGrantDenied=false;
         let reads=0;
         await assert.rejects(admit(()=>sample(reads++>=2?'10000':'1020')),{code:'schedule_admission_clock'});
         assert.equal(reads,3);
         untouched();
         db.exec("CREATE TEMP TRIGGER omit_schedule_claim BEFORE INSERT ON schedule_execution_admission BEGIN SELECT RAISE(IGNORE); END");
         await assert.rejects(admit(),{code:'schedule_storage'});
         db.exec('DROP TRIGGER omit_schedule_claim');
         untouched();
         terminalReply=null;
         const accepted=await admit();
         assert.equal(accepted.admission.occurrence_id,occurrence.occurrence_id);
         assert.deepEqual(accepted.admission.activation_ref,activation.ref);
         assert.deepEqual(accepted.admission.schedule_version_ref,version.ref);
         assert.deepEqual(accepted.admission.artifact_ref,published.artifact_ref);
         const execution=runs.getExecution(null,accepted.execution_ref.object_id);
         assert.deepEqual(execution.admission,accepted.admission);
         assert.deepEqual(execution.execution_ref,accepted.execution_ref);
         assert.equal(execution.execution.origin_kind,'schedule');
         assert.equal(execution.execution.receive_cursor,undefined);
         if(scheduledRuntime){
          assert.deepEqual(execution.anchor.body.origin_ref,execution.execution.admission_ref);
          assert.equal(execution.anchor.body.clock.tick_ns,tick(1020));
          assert.deepEqual(execution.steps[0].ready.body.execution_anchor_ref,execution.anchor.ref);
         }else assert.equal(execution.anchor,null);
         assert.equal(execution.steps[0].plan.step_id,accepted.admission.first_step_id);
         assert.equal(runs.get(null,run.run_id).active_execution_id,accepted.execution_ref.object_id);
         assert.equal(runs.get(null,run.run_id).revision,'7');
         await assert.rejects(admit(),{code:'execution_schedule_run'});
         assert.equal(result(await verified.live('5',{value:true})).disposition,'suppressed_busy');
         schedules.reconcileClock(()=>sample('1020'),runs.isScheduleSlotBusyInTransaction);
         const overlap=schedules.get(null,scheduleId).occurrences[1];
         assert.equal(overlap.state,'blocked');
         assert.equal(overlap.stop.reason,'run_busy');
         const next=schedules.publish(null,scheduleId,{...input,context:context(),expected_version:'1',
          items:[{...item,local_start:new Date(sampledAt+60000).toISOString().slice(0,-1)}]});
         schedules.activate(null,scheduleId,{context:context(),expected_revision:'1',schedule_version_ref:next.ref});
         schedules.reconcileClock(()=>null);
         const occurrenceView={occurrence_id:occurrence.occurrence_id,state:'accepted',admission:accepted.admission};
         assert.deepEqual(schedules.get(null,scheduleId).occurrences,[occurrenceView]);
         assert.deepEqual(schedules.get(null,scheduleId,'1').occurrences,[occurrenceView,overlap]);
         const step=execution.steps[0].plan,readSchedule=db.transaction(()=>schedules.readActionScheduleInTransaction(execution.execution,step));
         assert.deepEqual(readSchedule(),{admission:accepted.admission,step_window:occurrence.step_windows[0],
          window_index:occurrence.step_windows.map(({step_id,step_window_digest})=>({step_id,step_window_digest}))});
         assert.throws(()=>db.transaction(()=>schedules.readActionScheduleInTransaction(execution.execution,{...step,step_id:'other'}))(),
          {code:'schedule_admission_step_window'});
         if(scheduledRuntime){
          const canStart=reading=>db.transaction(()=>schedules.canStartActionInTransaction(execution.execution,step,reading))();
          assert.equal(canStart(sample(1020)),true);
          assert.throws(()=>canStart(sample(1019)),{code:'schedule_execution_clock'});
          const changed=structuredClone(sample(1021));
          changed.reference.bundle.anchor.binding.generation='2';
          changed.reference.state.binding=changed.reference.bundle.anchor.binding;
          assert.throws(()=>canStart(changed),{code:'schedule_execution_clock'});
          const elapsed=structuredClone(sample(1021));
          const end=BigInt(occurrence.step_windows[0].window.not_after_utc_ns);
          elapsed.reference.bundle.anchor.utc_at_anchor={lower_ns:String(end-1033n),upper_ns:String(end-1022n)};
          assert.throws(()=>canStart(elapsed),{code:'schedule_execution_window_elapsed'});
         }
         if(controllerActions&&!scheduledRuntime){
          const grant=execution.execution.grant_basis[0],eligible_before_ns=execution.execution.approved_before_ns;
          // Readiness and parameter fixtures isolate the transport. The occurrence and execution are the committed Seamer originals.
          const readyBody={format:'leafseamer.step-ready/1',ready_id:id(),execution_ref:execution.execution_ref,step_id:step.step_id,
           step_plan_digest:digest(step)};
          const step_ready={ref:ref('step_ready',scope,readyBody.ready_id,digest(readyBody)),body:readyBody};
          const params={value:true},parametersBody={format:'leafseamer.action-parameters/1',execution_ref:execution.execution_ref,
           step_id:step.step_id,step_plan_digest:digest(step),step_ready_ref:step_ready.ref,origin_digest:execution.execution.admission_ref.digest,
           grant_ref:grant.grant_ref,parameter_schema_ref:step.payload.binding.parameter_schema_ref,delay_window_refs:[],
           parameters:params,parameters_digest:digest(params),eligible_before_ns};
          const parameters={ref:ref('action_parameters',scope,step.step_id,digest(parametersBody)),body:parametersBody};
          const request={preparation:{lookup:grant.lookup,grant_ref:grant.grant_ref,receipt_ref:grant.receipt_ref},
           execution_ref:execution.execution_ref,step_id:step.step_id,parameters_ref:parameters.ref};
          const unsigned={execution:execution.execution,step_plan:step,step_ready,parameters,eligible_before_ns};
          scheduledActionRead=(bindingId,socket,query)=>{
           assert.equal(bindingId,pairing.binding_id);assert.equal(socket.authorized,true);assert.deepEqual(query,request);
           const read=()=>({...unsigned,schedule:readSchedule()});
           return {body:read(),beforeSend:read};
          };
          const received=await observeActionBinding(request);
          assert.equal(db.transaction(received.assertCurrentInTransaction).immediate(),true);
          assert.deepEqual(received.original.schedule,readSchedule());
          const checked=controllerActions.actionAdmissionRequest(request,target);
          const adopted=controllerActions.actionAdmissionBinding(received.original,checked,observedActionPreparation);
          const window=occurrence.step_windows[0];
          assert.deepEqual(adopted.schedule_binding,{format:'leafseamer.schedule-step-binding/1',execution_ref:execution.execution_ref,
           occurrence_ref:{id:occurrence.occurrence_id,revision:'1',digest:digest(occurrence)},
           admission_ref:{id:occurrence.occurrence_id,revision:'1',digest:digest(accepted.admission)},
           definition_artifact_ref:{id:published.artifact_ref.object_id,revision:published.artifact_ref.revision,digest:published.artifact_ref.digest},
           schedule_version:'1',activation_ref:activation.ref,step_id:step.step_id,
           step_window_ref:{id:window.step_window_id,revision:'1',digest:window.step_window_digest},window:window.window});
          const tampered=structuredClone(received.original),{step_window_digest,...changed}=tampered.schedule.step_window;
          changed.window.not_after_utc_ns=String(BigInt(changed.window.not_after_utc_ns)+1000000n);
          tampered.schedule.step_window={...changed,step_window_digest:digest(changed)};
          assert.throws(()=>controllerActions.actionAdmissionBinding(tampered,checked,observedActionPreparation),{code:'peer_pairing_action_schedule_step'});
          tampered.schedule.window_index[0].step_window_digest=digest(changed);
          assert.throws(()=>controllerActions.actionAdmissionBinding(tampered,checked,observedActionPreparation),{code:'peer_pairing_action_schedule_occurrence'});
          const old=structuredClone(received.original);delete old.schedule;
          assert.throws(()=>controllerActions.actionAdmissionBinding(old,checked,observedActionPreparation),{code:'peer_pairing_input'});
          scheduledActionRead=null;
         }
         let finalExecution=execution;
         if(scheduledRuntime){
          const until=performance.now()+7000;
          do{
           finalExecution=runs.getExecution(null,accepted.execution_ref.object_id);
           assert.equal(finalExecution.protection,null);
           if(finalExecution.steps[0].submission?.state==='accepted')break;
           await wait(25);
          }while(performance.now()<until);
          const first=finalExecution.steps[0];
          assert.equal(first.submission?.state,'accepted');
          assert.deepEqual(observedScheduled.binding.parameters,first.prepared_parameters);
          assert.deepEqual(observedScheduled.binding.step_ready,first.ready);
          assert.deepEqual(observedScheduled.binding.schedule,readSchedule());
          if(controllerActions)assert.deepEqual(observedScheduled.adoption.schedule_binding.window,occurrence.step_windows[0].window);
          const command_ref=savedAdjudication.command_ref,evidence=kind=>({id:command_ref.object_id+'.'+kind,revision:'1',digest:digest(kind)});
          const state={command_id:command_ref.object_id,scope:target,revision:'5',phase:'finished',outcome:'succeeded',effect_status:'verified',
           resource_status:'clear',automation_continuation:'eligible',reason_code:null,terminal_revision:'5',resolution_ref:null,attempt_ids:[id()],
           completion_evidence_refs:[evidence('decision')],resource_evidence_refs:[evidence('release')]};
          terminalReply={format:'leafseamer.automation-command-terminal/1',command_ref,state,
           record_ref:{...command_ref,revision:'5',digest:digest('scheduled terminal')},
           state_ref:{id:command_ref.object_id+'_state_5',revision:'5',digest:digest(state)},completion_achieved:'effect_verified'};
          const completeBefore=performance.now()+7000;
          do{
           finalExecution=runs.getExecution(null,accepted.execution_ref.object_id);
           assert.equal(finalExecution.protection,null);
           if(finalExecution.completion)break;
           await wait(25);
          }while(performance.now()<completeBefore);
          assert.deepEqual(finalExecution.steps[0].submission.terminal,terminalReply);
          assert.equal(finalExecution.completion?.body.run_revision_after,'8');
          assert.equal(runs.get(null,run.run_id).active_execution_id,null);
          runs.stopExecutionTimers();
         }
         assert.equal(commandSubmits,scheduledRuntime?2:1);
         assert.deepEqual(db.pragma('foreign_key_check'),[]);
         inspect(f.dir,reopened=>{
          const bootId=id(),schedules=schedulesFor(reopened,bootId);
          const historical=automationRunStore({...assembly,db:reopened,bootId,isOpen:()=>reopened.open,schedules});
          assert.deepEqual(historical.getExecution(null,accepted.execution_ref.object_id),finalExecution);
          assert.deepEqual(schedules.get(null,scheduleId).occurrences,[occurrenceView]);
         });
         return;
        }
        const following=result(await verified.live('5',{value:true}));
        assert.equal(following.disposition,r5Protected?'suppressed_busy':stateMachine?'no_match':'accepted');
        if(commandCase==='r5_rule'){
         const next=runs.getExecution(null,following.execution_id);
         assert.equal(next.execution.activation_slot.revision,'2');
         assert.equal(next.execution.activation_slot_revision,'3');
         assert.notEqual(following.execution_id,decision.execution_id);
        }
        assert.equal(commandSubmits,1);
        assert.deepEqual(db.pragma('foreign_key_check'),[]);
        inspect(f.dir,reopened=>{
         const historical=automationRunStore({...assembly,db:reopened,bootId:id(),isOpen:()=>reopened.open});
         assert.deepEqual(historical.getExecution(null,decision.execution_id),originalExecution);
        });
        if(commandCase==='r5_notification'){
         db.close();
         assert.equal(migrateStorage(f.dir,'33').schema_revision,'33');
         await verifyNotificationRestart(f.dir,originalExecution.completion);
        }
        if(commandCase==='r5_rule'){
         db.close();
         const service=migrateStorage(f.dir,'37');
         const executions=inspect(f.dir,reopened=>reopened.prepare(
          'SELECT execution_id,body_jcs,body_digest FROM automation_execution ORDER BY execution_id').raw().all());
         assert.equal(executions.length,2);
         inspect(f.dir,reopened=>reopened.exec('CREATE TABLE schedule_execution_admission(sentinel TEXT) STRICT'));
         assert.throws(()=>migrateStorage(f.dir,'38'),{code:'migration_failed'});
         inspect(f.dir,reopened=>{
          assert.equal(reopened.pragma('user_version',{simple:true}),37);
          assert.deepEqual(reopened.prepare('SELECT execution_id,body_jcs,body_digest FROM automation_execution ORDER BY execution_id').raw().all(),executions);
          reopened.exec('DROP TABLE schedule_execution_admission');
         });
         assert.equal(migrateStorage(f.dir,'38').database_instance_id,service.database_instance_id);
         assert.equal(migrateStorage(f.dir,'39').database_instance_id,service.database_instance_id);
         inspect(f.dir,reopened=>{
          assert.deepEqual(reopened.prepare('SELECT execution_id,body_jcs,body_digest FROM automation_execution ORDER BY execution_id').raw().all(),executions);
          assert.deepEqual(reopened.pragma('foreign_key_check'),[]);
          const historical=automationRunStore({...assembly,db:reopened,service:{...service,schema_revision:'39'},bootId:id(),
           isOpen:()=>reopened.open});
          assert.deepEqual(historical.getExecution(null,decision.execution_id),originalExecution);
         });
        }
        return;
       }
      }else if(parameterCase==='validated'){
       busy=await verified.live('5',{value:false});
       originalExecution=await poll(value=>value.steps[0].prepared_parameters);
       await checkParameters(originalExecution);
      }else{
       if(parameterCase==='storage')db.exec('CREATE TEMP TRIGGER test_omit_action_parameters BEFORE INSERT ON automation_execution_action_parameters BEGIN SELECT RAISE(IGNORE); END');
       originalExecution=await poll(value=>value.protection);
       if(parameterCase==='storage')db.exec('DROP TRIGGER test_omit_action_parameters');
       assert.equal(originalExecution.protection,'execution_runtime_unavailable');
       assert.equal(originalExecution.steps[0].prepared_parameters,null);
       assert.equal(db.prepare('SELECT count(*) FROM automation_execution_action_parameters').pluck().get(),0);
      }
     }
     if(!busy)assert.deepEqual(await verified.replayLive(),accepted);
     assert.equal(db.prepare('SELECT count(*) FROM automation_execution').pluck().get(),1);
     busy??=await verified.live('5',{value:true});assert.equal(result(busy).disposition,'suppressed_busy');
     assert.deepEqual(runs.getExecution(null,decision.execution_id),originalExecution);
    }else{
    const matched=await verified.live('3',{value:true});
    assert.equal(result(matched).reason,'execution_admission_unavailable');assert.equal(result(matched).evaluation.selected_branch,'rule');
    assert.equal(result(matched).execution_id,null);
    const expired=await verified.live('4',{value:true},2000000n);
    assert.equal(result(expired).reason,'event_age_limit_exceeded');assert.equal(result(expired).evaluation,undefined);
    const beforePause=runs.get(null,run.run_id);
    db.exec("CREATE TEMP TRIGGER test_omit_pause BEFORE UPDATE ON automation_run_approval_head WHEN NEW.run_revision='5' BEGIN SELECT RAISE(IGNORE); END");
    await assert.rejects(verified.live('5',{value:false,request_observation:true}),{code:'trigger_pause_revision'});
    assert.deepEqual(runs.get(null,run.run_id),beforePause);
    db.exec('DROP TRIGGER test_omit_pause');
    const unknown=await verified.live('5',{value:false,request_observation:true});
    assert.equal(result(unknown).disposition,'evaluation_unknown');assert.equal(result(unknown).evaluation.outcomes[0].value,'unknown');
    assert.equal(result(unknown).run_revision_before,'4');assert.equal(result(unknown).run_revision_after,'5');
    conditionLatch=result(unknown).decision_id;
    const later=await verified.live('6',{value:true});
    assert.equal(result(later).disposition,'suppressed_inactive');assert.equal(result(later).reason,'run_paused');
    }
   }finally{verified.close();}
   const armed=runs.get(null,run.run_id);
   const executionProtected=omitReady||Boolean(originalExecution?.protection);
   assert.deepEqual(armed,executionPlanning?
    {...armedAtBaseline,last_receive_seq:'7',revision:'5',lifecycle:executionProtected?'paused':'running',active_execution_id:originalExecution.execution.execution_id,
     ...(executionProtected&&{pause_latches:[originalExecution.execution.execution_id]})}:
    {...armedAtBaseline,last_receive_seq:'8',revision:'5',lifecycle:'paused',pause_latches:[conditionLatch]});
   const revoked=runs.revokeApproval(null,saved.operation.approval_ref.object_id,{context:{context_id:contextId,idempotency_key:id()},
    approval_ref:saved.operation.approval_ref,expected_status_revision:'1',reason:'测试撤销已布防运行批准'});
   revocationId=revoked.operation_id;
   expected={...armed,revision:'6',lifecycle:'paused',pause_latches:[revoked.operation_id,...armed.pause_latches]};
   assert.deepEqual(runs.get(null,run.run_id),expected);
   const {lookup}=intents[0],preparation={preparation_id:lookup.preparation_id,operation_ref:saved.operation_ref,
    request_digest:lookup.request_digest,run_id:run.run_id,arm_generation:'1'};
   const [text,checksum]=db.prepare('SELECT body_jcs,body_digest FROM automation_run_approval_protection_outbox WHERE operation_id=?')
    .raw().get(revoked.operation_id),notice=JSON.parse(text);
   assert.equal(digest(notice),checksum);
   assert.deepEqual(notice.preparations,[preparation]);
   assert.deepEqual(notice.grants,[{preparation_id:lookup.preparation_id,grant_ref:observedAfterReceipt[0].target_grant_ref}]);
   assert.deepEqual(notice.arm_generations,[{run_id:run.run_id,arm_generation:'1'}]);
   const rejection={format:'leafseamer.run-approval-rejection/1',receipt_id:id(),target_scope:target,approval_ref:notice.approval_ref,
    endorsement_ref:notice.endorsement_ref,revocation_operation_ref:notice.revocation_operation_ref,request_digest:checksum,
    identity_state:'denied',protection_state:'pending'};
   const deliver=()=>runs.deliverApprovalProtection(revocationId,target);
   remoteProtection=rejection;
   assert.deepEqual((await deliver()).receipt,rejection);
   assert.equal(runs.getApprovalOperation(null,revocationId).state,'applying');
   assert.deepEqual(runs.scanApprovalProtection({after:null,limit:1}).items,[{operation_id:revocationId,target_scope:target}]);
   const final={format:'leafseamer.run-approval-protection/1',receipt_id:id(),rejection,run_id:run.run_id,deny_through_generation:'1',
    proof_digest:digest('fixture protection evidence'),device_ids:[action.device_id],protection_state:'applied'};
   remoteProtection={...final,deny_through_generation:null};
   await assert.rejects(deliver(),{code:'peer_pairing_response'});
   remoteProtection={...final,device_ids:[id()]};
   await assert.rejects(deliver(),{code:'run_approval_protection_completion'});
   remoteProtection=final;
   db.exec('CREATE TEMP TRIGGER omit_final_protection BEFORE INSERT ON automation_run_approval_protection_completion BEGIN SELECT RAISE(IGNORE); END');
   await assert.rejects(deliver(),{code:'run_approval_storage'});
   db.exec('DROP TRIGGER omit_final_protection');assert.equal(runs.getApprovalOperation(null,revocationId).state,'applying');
   assert.deepEqual(await deliver(),{state:'delivered',receipt:final});
   assert.deepEqual(await deliver(),{state:'acknowledged',receipt:final});
   assert.equal(runs.getApprovalOperation(null,revocationId).state,'applied');
   assert.deepEqual(runs.getApprovalOperation(null,revocationId).targets,[{...revoked.targets[0],state:'applied',
    receipt_ref:ref('run_approval_protection_receipt',target,final.receipt_id,digest(final))}]);
   assert.deepEqual(db.prepare('SELECT state,status_revision FROM automation_run_approval_state WHERE approval_id=?').raw().get(notice.approval_ref.object_id),['revoked',3]);
   assert.deepEqual(runs.scanApprovalProtection({after:null,limit:1}),{items:[],next:null});
  }
  assert.deepEqual(await complete(),ready);
  assert.deepEqual(db.pragma('foreign_key_check'),[]);
  currentInitiator=false;sourceChanged=true;
  db.close();
  const service=schema_revision==='25'?migrateStorage(f.dir,'26'):
   schema_revision==='26'&&!omitReady?migrateStorage(f.dir,'27'):
    schema_revision==='27'&&delayCase==='elapsed'?migrateStorage(f.dir,'28'):
    schema_revision==='30'&&commandCase==='terminal_success'?migrateStorage(f.dir,'31'):assembly.service;
  db=new Database(filename);db.pragma('foreign_keys=ON');
  const restarted=automationRunStore({...assembly,service,db,bootId:id(),publications:automationPublicationStore({db,service,isOpen:()=>db.open})});
  assert.deepEqual(await restarted.completeArmRun(null,saved.operation_ref),ready);
  if(originalExecution){
   const historical=schema_revision==='25'?
    {...originalExecution,anchor:null,steps:originalExecution.steps.map(step=>({...step,ready:null}))}:originalExecution;
   assert.deepEqual(restarted.getExecution(null,originalExecution.execution.execution_id),historical);
   if(!expected.pause_latches.includes(originalExecution.execution.execution_id))
    expected={...expected,pause_latches:[...expected.pause_latches,originalExecution.execution.execution_id]};
  }
  assert.deepEqual(restarted.get(null,run.run_id),expected);
  if(hasTarget){
   assert.equal(restarted.getApprovalOperation(null,revocationId).state,'applied');
   const [{binding_id,lookup}]=intents;
   assert.deepEqual(restarted.armPreparationLookupIntent(saved.operation_ref,target),{binding_id,lookup});
   assert.deepEqual(restarted.armPreparationObservations(saved.operation_ref),observedAfterReceipt);
   assert.throws(()=>restarted.armPreparationIntents(saved.operation_ref),{code:'run_initiator_ineligible'});
   assert.throws(()=>restarted.armPreparationLookupIntent(saved.operation_ref,{...target,database_instance_id:id()}),{code:'run_preparation_plan_target'});
  }
 }finally{db.close();}
});

test('升级保留原拒绝回执，两个目标全部完成后才提交撤销',async t=>{
 let db;t.after(()=>{if(db?.open)db.close();});
 const f=setup(t,{revision:'16',withoutActions:true}),{scope,draft}=f,contextId=id(),boot=f.store.serviceBootId;
 const actor={issuer_id:id(),issuer_epoch:id(),principal_id:id(),principal_kind:'human'};
 f.store.close();
 inspect(f.dir,db=>{
  const subject=id();db.prepare('INSERT INTO local_subject VALUES(?,?,?,?,?)').run(subject,actor.issuer_id,actor.issuer_epoch,actor.principal_id,'human');
  db.prepare('INSERT INTO local_request_context VALUES(?,?,?,?,?)').run(contextId,subject,boot,'{}',digest({}));
 });
 f.reopen();const published=await f.publish(),run=f.store.runs.create(null,{context:{context_id:contextId,idempotency_key:id()},definition_id:draft.definition_id,
  definition_version:published.definition_version,expected_artifact_ref:published.artifact_ref});const preparationBoot=f.store.serviceBootId;f.store.close();
 const scopes=[0,1].map(()=>({authority_service_id:id(),database_instance_id:id()})),action={device_id:id(),action_id:'simulator.value.set',capability_version:'1.0.0'};
 const pairings=scopes.map(target=>({binding_id:id(),revision:'1',digest:digest(target),peer_service_id:target.authority_service_id,transcript:{parties:[
  {service_id:scope.authority_service_id,permissions:['automation.protect']},
  {service_id:target.authority_service_id,database_instance_id:target.database_instance_id,service_kind:'controller',permissions:['automation.prepare.read']}]}}));
 const filename=path.join(f.dir,'seamer.sqlite'),open=()=>{const db=new Database(filename);db.pragma('foreign_keys=ON');return db;};
 db=open();
 const approvalId=db.transaction(()=>{
  for(const record of pairings){
   db.prepare("INSERT INTO peer_pairing_revision VALUES(?,1,?,?,?,'activated',?,?,?)")
    .run(record.binding_id,id(),record.peer_service_id,preparationBoot,digest(record.transcript),canonical(record),digest(record));
   db.prepare('INSERT INTO peer_pairing_head VALUES(?,1)').run(record.binding_id);
  }
  return seedRunApproval(db,scope,run,contextId,scopes,[action]);
 }).immediate();
 const replies=new Map(),requests=new Map();
 function store(canCompleteProtection){
  return runApprovalStore({db,scope,requestEpoch:f.identity.request_epoch,writer:work=>db.transaction(work).immediate(),
   authority:{identifyRunApprovalActorInTransaction:()=>actor,assertRunCapacityInTransaction(){}},authorize:()=>({actor,authentication_evidence:{fixture:true}}),
   runOriginal:()=>run,getProposal(reference){const proposal=JSON.parse(db.prepare('SELECT body_jcs FROM automation_run_approval_proposal WHERE proposal_id=?').pluck().get(reference.object_id));
    assert.equal(reference.digest,digest(proposal));return {saved:{proposal}};},requestIdentity:(_proof,key)=>digest(key),
   currentPairingInTransaction:reference=>pairings.find(record=>record.binding_id===reference.id),canRevoke:true,canAdoptProtection:true,canCompleteProtection,
   sendApprovalProtection(binding,notice,options){
    assert.equal(options.completion,canCompleteProtection);assert.equal(db.inTransaction,false);requests.set(binding,notice);return replies.get(binding);
   }});
 }
 const firstStore=store(false),approval=JSON.parse(db.prepare('SELECT body_jcs FROM automation_run_approval WHERE approval_id=?').pluck().get(approvalId));
 const operation=firstStore.revokeApproval(null,approvalId,{context:{context_id:contextId,idempotency_key:id()},
  approval_ref:ref('run_approval',scope,approvalId,digest(approval)),expected_status_revision:'1',reason:'测试两个 Controller'});
 const rejection=scopes.map((target,index)=>{
  const {notice}=firstStore.prepareApprovalProtection(operation.operation_id,target),receipt={format:'leafseamer.run-approval-rejection/1',receipt_id:id(),
   target_scope:target,approval_ref:notice.approval_ref,endorsement_ref:notice.endorsement_ref,revocation_operation_ref:notice.revocation_operation_ref,
   request_digest:digest(notice),identity_state:'denied',protection_state:'pending'};
  replies.set(pairings[index].binding_id,receipt);return receipt;
 });
 await firstStore.deliverApprovalProtection(operation.operation_id,scopes[0]);assert.equal(requests.size,1);
 const snapshot=db.prepare('SELECT * FROM automation_run_approval_protection_receipt').raw().all();
 db.exec('CREATE TABLE automation_run_approval_protection_completion(conflict TEXT)');db.close();
 assert.throws(()=>migrateStorage(f.dir,'17'),{code:'migration_failed'});db=open();
 assert.equal(db.pragma('user_version',{simple:true}),16);assert.deepEqual(db.prepare('SELECT * FROM automation_run_approval_protection_receipt').raw().all(),snapshot);
 db.exec('DROP TABLE automation_run_approval_protection_completion');db.close();
 assert.equal(migrateStorage(f.dir,'17').schema_revision,'17');db=open();
 assert.deepEqual(db.prepare('SELECT * FROM automation_run_approval_protection_receipt').raw().all(),snapshot);
 const secondStore=store(true);
 assert.equal(secondStore.scanApprovalProtection({after:null,limit:16}).items.length,2);
 for(const [index,target] of scopes.entries()){
  const final={format:'leafseamer.run-approval-protection/1',receipt_id:id(),rejection:rejection[index],run_id:run.run_id,deny_through_generation:null,
   proof_digest:digest('fixture proof '+index),device_ids:[action.device_id],protection_state:'applied'};
  replies.set(pairings[index].binding_id,final);
  await secondStore.deliverApprovalProtection(operation.operation_id,target);
  const current=secondStore.getApprovalOperation(null,operation.operation_id);
  assert.equal(current.state,index===0?'applying':'applied');
  assert.deepEqual(current.targets.map(entry=>entry.state),index===0?['applied','pending']:['applied','applied']);
 }
 assert.equal(db.prepare('SELECT status_revision FROM automation_run_approval_state WHERE approval_id=?').pluck().get(approvalId),3);
 assert.deepEqual(db.pragma('foreign_key_check'),[]);
});

for(const kind of ['event','device'])test('迁移前撤销的精确绑定批准进入保护队列，事务失败及重启可恢复：'+kind,async t=>{
 const f=setup(t,{revision:'11'}),contextId=id(),subjectId=id(),approvalId=id(),target={authority_service_id:id(),database_instance_id:id()};
 const {scope,draft,snapshot}=f,actor={principal_kind:'human',issuer_id:id(),issuer_epoch:id(),principal_id:id()};
 const candidate={definition_id:draft.definition_id,binding:snapshot.bindings[0]};
 const original={scope,format:'leafseamer.event-binding-approval/1',approval_id:approvalId,definition_id:draft.definition_id,revision:'1',state:'approved',
  request:{request_id:id()},candidate:{...candidate,candidate_digest:digest(candidate)},authorization:{context_id:contextId,actor},previous_ref:null};
 const originalRef={id:approvalId,revision:'1',digest:digest(original)};
 const revoked={...original,revision:'2',state:'revoked',request:{request_id:id()},previous_ref:originalRef};
 const revokedRef={id:approvalId,revision:'2',digest:digest(revoked)};
 const bootId=f.store.serviceBootId;
 f.store.close();
 inspect(f.dir,db=>{
  db.prepare('INSERT INTO local_subject VALUES(?,?,?,?,?)').run(subjectId,actor.issuer_id,actor.issuer_epoch,actor.principal_id,'human');
  db.prepare('INSERT INTO local_request_context VALUES(?,?,?,?,?)').run(contextId,subjectId,bootId,'{}',digest({}));
  if(kind==='event'){
   const insert=db.prepare('INSERT INTO event_binding_approval_revision VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)');
   for(const body of [original,revoked])insert.run(approvalId,Number(body.revision),draft.definition_id,candidate.binding.binding_id,contextId,
    digest(actor),body.request.request_id,digest(body.request),body.state,canonical(body),digest(body),body.previous_ref?1:null,body.previous_ref?.digest??null);
  }else db.transaction(()=>{
   const revision=db.prepare('INSERT INTO binding_approval_revision VALUES(?,?,?,?,?,?,?)');
   const request=db.prepare('INSERT INTO binding_approval_operation VALUES(?,?,?,?,?,?)');
   const notice=db.prepare('INSERT INTO binding_approval_outbox VALUES(?,?,?)');
   for(const body of [original,revoked]){
    const transactionId=id(),number=Number(body.revision);
    revision.run(approvalId,number,draft.definition_id,body.state,canonical(body),digest(body),transactionId);
    request.run(transactionId,digest(actor),body.request.request_id,digest(body.request),approvalId,number);
    notice.run(transactionId,'{}',digest({}));
    if(number===1)db.prepare('INSERT INTO binding_approval_head VALUES(?,1)').run(approvalId);
    else db.prepare('UPDATE binding_approval_head SET revision=2 WHERE approval_id=?').run(approvalId);
   }
  }).immediate();
 });
 f.reopen();
 function setPins(approvalRef){
  snapshot.review.binding_approval_refs=[{binding_id:candidate.binding.binding_id,kind,approval_ref:approvalRef},
   {binding_id:snapshot.bindings[1].binding_id,kind:'device',approval_ref:kind==='device'?approvalRef:{id:id(),revision:'1',digest:digest('other device')}}];
  snapshot.review_ref.digest=digest(snapshot.review);
 }
 const runFor=published=>f.store.runs.create(null,{context:{context_id:contextId,idempotency_key:id()},definition_id:draft.definition_id,
  definition_version:published.definition_version,expected_artifact_ref:published.artifact_ref});
 setPins(originalRef);const run=runFor(await f.publish());
 setPins({...originalRef,digest:digest('different original')});
 const unrelated=runFor(await f.publish({...f.request,context:{...f.request.context,idempotency_key:id()}}));
 f.store.close();
 const affected=inspect(f.dir,db=>db.transaction(()=>{
  seedRunApproval(db,scope,unrelated,contextId,target);return seedRunApproval(db,scope,run,contextId,target);
 }).immediate());
 if(kind==='event'){
  inspect(f.dir,db=>db.exec('CREATE VIEW revoked_publication_binding AS SELECT 1'));
  assert.throws(()=>migrateStorage(f.dir,'12'),{code:'migration_failed'});
  inspect(f.dir,db=>{
   assert.equal(db.pragma('user_version',{simple:true}),11);
   assert.equal(db.prepare("SELECT count(*) FROM sqlite_schema WHERE name='definition_publication_binding_pin'").pluck().get(),0);
   assert.equal(db.prepare('SELECT count(*) FROM automation_run_approval').pluck().get(),2);
   db.exec('DROP VIEW revoked_publication_binding');
  });
 }
 migrateStorage(f.dir,'12');
 const reopen=()=>{
  f.reopen();
  f.store.bindPeerPairings({issuer:{service_id:target.authority_service_id,issuer_id:actor.issuer_id,issuer_epoch:actor.issuer_epoch}});
  f.store.bindBindingApprovals({receiver:{}});
 };
 const scan=()=>{
  f.store.runs.adoptBindingRevocations();
  return f.store.runs.scanApprovalProtection({after:null,limit:16});
 };
 inspect(f.dir,db=>db.exec(`CREATE TRIGGER test_protection_failure BEFORE INSERT ON automation_run_approval_protection_outbox
  BEGIN SELECT RAISE(IGNORE); END`));
 reopen();
 assert.throws(scan,{code:'run_approval_storage'});
 assert.equal(f.store.runs.get(null,run.run_id).revision,'2');
 f.store.close();
 inspect(f.dir,db=>{
  assert.equal(db.prepare('SELECT count(*) FROM automation_run_approval_revocation').pluck().get(),0);
  db.exec('DROP TRIGGER test_protection_failure');
 });
 reopen();
 const queued=scan(),operationId=queued.items[0].operation_id;
 assert.deepEqual(queued,{items:[{operation_id:operationId,target_scope:target}],next:null});
 assert.deepEqual(scan(),queued);
 assert.equal(f.store.runs.get(null,run.run_id).revision,'3');
 assert.equal(f.store.runs.get(null,unrelated.run_id).revision,'2');
 f.store.close();
 inspect(f.dir,db=>{
  assert.deepEqual(db.pragma('foreign_key_check'),[]);
  const row=db.prepare('SELECT approval_id,context_id,audit_jcs FROM automation_run_approval_revocation').raw().get();
  assert.deepEqual(row.slice(0,2),[affected,null]);
  assert.deepEqual(JSON.parse(row[2]).binding_revocation,{kind,approval_ref:originalRef,revocation_ref:revokedRef});
 });
 reopen();assert.deepEqual(scan(),queued);
});
