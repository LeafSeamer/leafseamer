import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import Database from 'better-sqlite3';
import {fixture as clockFixture} from '../../leafseamer-controller/test/auth-clock-fixture.mjs';
import {referenceBundleAuthority} from '../../leafseamer-controller/test/fixtures/reference-bundle-authority.mjs';

const root=process.env.LEAFSEAMER_PACKAGE_UNDER_TEST??fileURLToPath(new URL('../',import.meta.url));
const load=name=>import(pathToFileURL(path.join(root,'dist',name)));
const {initializeStorage,migrateStorage}=await load('storage.mjs');
const {snapshotIngestStore}=await load('snapshot-ingest-store.mjs');
const {reconcileInbox}=await load('reconcile-inbox.mjs');
const {automationRunStore}=await load('automation-run-store.mjs');
const {startAuthenticationClock}=await load('runtime/auth-clock.mjs');
const {authenticationClockStore}=await load('runtime/auth-clock-store.mjs');
const {digest}=await load('runtime/auth-clock-value.mjs');

test('布防输入绑定只能使用当前采用的准确来源，换代和离开范围使旧证据失效',t=>{
 const directory=fs.mkdtempSync(path.join(os.tmpdir(),'leafseamer-source-'));
 let db;
 t.after(()=>{
  db?.close();
  assert.equal(path.dirname(path.resolve(directory)),path.resolve(os.tmpdir()));
  fs.rmSync(directory,{recursive:true,force:true,maxRetries:5,retryDelay:50});
 });
 initializeStorage(directory);
 const service=migrateStorage(directory,'5');db=new Database(path.join(directory,'seamer.sqlite'));
 db.pragma('foreign_keys=ON');
 const authorityId=randomUUID(),databaseId=randomUUID(),bootId=randomUUID(),registrationId=randomUUID();
 const owner={kind:'service',consumer_id:'canonical',consumer_service_id:service.service_id,consumer_database_instance_id:service.database_instance_id};
 let selected;
 const store=snapshotIngestStore({db,service,bootId,isOpen:()=>db.open,receiver:{currentInTransaction(){assert.ok(db.inTransaction);return selected;}}});
 const inspect=inputs=>db.transaction(()=>store.resolveAutomationSourcesInTransaction(inputs)).immediate();
 function record(id,epoch){
  const source_key={authority_service_id:authorityId,database_instance_id:databaseId,registration_id:id,source_stream_id:randomUUID(),source_epoch:epoch};
  const binding_ref={entity_id:id,entity_kind:'source_binding',entity_epoch:epoch,revision:'1',content_digest:digest(source_key).slice(7)};
  return {entity_id:id,entity_epoch:epoch,entity_revision:'1',view:{source_key,binding_ref,state:'synchronized',baseline_ref:null,
   contiguous_sequence:'1',highest_seen_sequence:'1',gaps:[]}};
 }
 function adopt(source){
  selected={subscription:{owner,purpose:'canonical_trigger',binding:{publisher_service_id:authorityId,publisher_database_instance_id:databaseId,
   subscription_id:randomUUID()},snapshot_ref:{snapshot_id:randomUUID(),manifest_digest:digest(source).slice(7)}},
   context:{},projection:{records:[source],source_watermarks:[{}]}};
  return store.apply({});
 }
 const first=record(registrationId,randomUUID());
 const input={binding_id:'trigger',source:'event',authority_id:authorityId,
  registration_ref:{id:registrationId,revision:'1',digest:'sha256:'+first.view.binding_ref.content_digest}};
 assert.equal(inspect([input])[0].state,'missing');
 const applied=adopt(first);
 const [baseline]=inspect([input]);
 assert.equal(baseline.state,'baseline_required');
 assert.deepEqual(baseline.source.source_key,first.view.source_key);
 assert.deepEqual(baseline.source.binding_ref,first.view.binding_ref);
 assert.equal(baseline.source.baseline_ref.apply_id,applied.apply_id);
 assert.equal(inspect([{...input,registration_ref:{...input.registration_ref,revision:'2'}}])[0].state,'missing');
 assert.equal(inspect([{...input,authority_id:randomUUID()}])[0].state,'missing');
 assert.equal(inspect([{...input,registration_ref:{...input.registration_ref,digest:digest('another binding')}}])[0].state,'missing');
 adopt(record(randomUUID(),randomUUID()));
 assert.equal(inspect([input])[0].state,'projection_changed');
 const replacement=record(registrationId,randomUUID());
 const next=adopt(replacement);
 const nextInput={...input,registration_ref:{...input.registration_ref,digest:'sha256:'+replacement.view.binding_ref.content_digest}};
 const checks=inspect([input,nextInput,{...nextInput,binding_id:'guard',source:'observation'}]);
 assert.equal(checks[0].state,'closed');
 assert.equal(checks[1].state,'baseline_required');
 assert.equal(checks[1].source.baseline_ref.apply_id,next.apply_id);
 assert.deepEqual(checks[1].source,checks[2].source);
 assert.throws(()=>store.resolveAutomationSourcesInTransaction([input]),{code:'snapshot_ingest_source_transaction'});
});

test('来源连续性须覆盖当前封存增量、投影和同代检查点',async t=>{
 const id=randomUUID,directory=fs.mkdtempSync(path.join(os.tmpdir(),'leafseamer-continuity-'));
 let db,clock;
 t.after(()=>{
  clock?.stop();db?.close();
  assert.equal(path.dirname(path.resolve(directory)),path.resolve(os.tmpdir()));
  fs.rmSync(directory,{recursive:true,force:true,maxRetries:5,retryDelay:50});
 });
 initializeStorage(directory);const service=migrateStorage(directory,'24'),bootId=id();
 db=new Database(path.join(directory,'seamer.sqlite'));db.pragma('foreign_keys=ON');
 db.prepare("INSERT INTO service_boot VALUES(?,?,?,'started')").run(service.service_id,service.database_instance_id,bootId);
 const authenticationClock=authenticationClockStore(db,{service_id:service.service_id,service_boot_id:bootId});
 const seed=clockFixture(authenticationClock.scope),descriptor_digest=digest('fixture receiver clock domain');
 const fixture=clockFixture(authenticationClock.scope,{source:seed.source,descriptor:{clock_domain_id:'clock_'+descriptor_digest.slice(7),descriptor_digest}});
 clock=startAuthenticationClock(fixture.source,authenticationClock,fixture.provider,{schedule:false});
 const authority_service_id=id(),database_instance_id=id(),registration_id=id(),device_id=id(),source_epoch=id();
 const source_key={authority_service_id,database_instance_id,registration_id,source_stream_id:device_id,source_epoch};
 const originalBinding={registration_id,revision:'1',owner_service_id:authority_service_id,database_instance_id,
  provider_id:id(),provider_boot_id:id(),capture_policy_id:'fixture',capture_policy_revision:'1',delivery_semantics:'discrete',
  target:{kind:'device',device_id,trigger_id:'fixture',capability_version:'1.0.0'}};
 const binding_ref={authority_service_id,database_instance_id,entity_kind:'source_binding',entity_id:registration_id,
  entity_epoch:source_epoch,revision:'1',content_digest:digest(originalBinding).slice(7)};
 const snapshot_ref={snapshot_id:id(),manifest_digest:digest('snapshot').slice(7),snapshot_cursor:'0'};
 const binding={publisher_service_id:authority_service_id,publisher_database_instance_id:database_instance_id,subscription_id:id(),connection_generation:'1'};
 const owner={kind:'service',pairing_id:id(),consumer_id:'canonical',consumer_service_id:service.service_id,consumer_database_instance_id:service.database_instance_id};
 const projection=sequence=>({entity_kind:'sources',entity_id:registration_id,entity_epoch:source_epoch,entity_revision:String(Number(sequence)+1),
  view:{source_key,binding_ref,state:'baseline_required',baseline_ref:null,contiguous_sequence:sequence,highest_seen_sequence:sequence,gaps:[]}});
 const selected={subscription:{owner,purpose:'canonical_trigger',binding,snapshot_ref},context:{},projection:{records:[projection('0')],source_watermarks:[{}]}};
 const openSources=bootId=>snapshotIngestStore({db,service,bootId,isOpen:()=>db.open,receiver:{currentInTransaction:()=>selected}});
 let captureReceiver=null,receiverCurrent=true;
 const referenceProvider={readFirst(input){assert.equal(db.inTransaction,false);return captureReceiver?.(input);},
  isCurrent(input){assert.ok(db.inTransaction);assert.ok(BigInt(input.first_tick_ns)<=BigInt(input.tick_ns));return receiverCurrent;}};
 const isOpen=()=>db.open,{triggerDecisions}=automationRunStore({db,service,bootId,isOpen});
 const sources=openSources(bootId),receipt=sources.apply(null),inbox=reconcileInbox({db,service,bootId,clock,isOpen,referenceProvider,triggerDecisions});
 const state=()=>sources.sourceState(source_key).state,marker={entry_kind:'sync.ready',payload:{baseline_snapshot_id:snapshot_ref.snapshot_id}};
 let cursor=0;
 function delivery(entry,connection_generation,delivery_mode){
  const info={cursor:String(++cursor),previous_cursor:String(cursor-1),binding:{...binding,connection_generation},delivery_mode,payload_digest:digest(entry).slice(7)};
  return entry.entry_kind==='sync.ready'?{...info,type:'server.sync.ready',baseline_snapshot_id:snapshot_ref.snapshot_id}:{...info,type:'server.data',entry};
 }
 function accept(entry,connection_generation,delivery_mode){
  const frame=delivery(entry,connection_generation,delivery_mode);
  inbox.accept(receipt,frame,()=>assert.ok(db.inTransaction));return frame;
 }
 function timed(sequence){
  const {payload:{origin}}=event(sequence),ref={id:id(),revision:'1',digest:digest('capture')};
  const sourceTime={id:id(),revision:'1',source_key,binding_ref:originalBinding,event_id:origin.event_id,source_sequence:sequence,
   payload_digest:digest(origin.payload),capture_semantics:'event_occurrence',capture_evidence_ref:ref,capture_domain:{authority_service_id,database_instance_id,
    provider_id:originalBinding.provider_id,provider_boot_id:originalBinding.provider_boot_id,runtime_binding_ref:ref,
    clock_domain_id:'clock_'+digest('source domain').slice(7),descriptor_digest:digest('source domain'),clock_generation:'1'},
   capture_start_tick_ns:'100',capture_end_tick_ns:'200',elapsed_profile_ref:ref,reference_context_ref:null,utc_interval:null};
  const evidence={...sourceTime,digest:digest(sourceTime)};
  origin.source_time_ref={kind:'source_time_evidence',scope:{authority_service_id,database_instance_id},object_id:evidence.id,revision:evidence.revision,digest:evidence.digest};
  return {entry:{entry_kind:'source.event',payload:{origin,origin_digest:digest(origin)}},evidence};
 }
 function event(source_sequence){
  const origin={source_key,binding_ref:originalBinding,event_id:id(),source_sequence,source_time_ref:null,
   payload:{schema_ref:{kind:'schema',scope:{authority_service_id,database_instance_id},object_id:'fixture',revision:'1',digest:digest('schema')},value:{value:7}}};
  return {entry_kind:'source.event',payload:{origin,origin_digest:digest(origin)}};
 }
 const upsert=sequence=>({entry_kind:'entity.upsert',payload:projection(sequence)});
 accept(marker,'1','reconcile');assert.equal(state(),'baseline_required');
 const first=event('1'),firstFrame=accept(first,'1','live');assert.equal(state(),'baseline_required');
 accept(upsert('1'),'1','live');assert.equal(state(),'synchronized');
 const observed=inbox.sourceEvent(source_key,first.payload.origin.event_id),observation=observed.first_observation,age=observation.age_evidence;
 assert.equal(observation.disposition.state,'observed_only');assert.equal(age.basis,'unproven');assert.equal(age.lower_age_ms,null);assert.equal(age.upper_age_ms,null);
 assert.equal(age.first_receive_tick_ns,observation.received_tick_ns);assert.equal(age.receiver_domain.provider_boot_id,bootId);
 assert.equal(age.origin_digest,first.payload.origin_digest);
 assert.deepEqual(observation.disposition.age_evidence_ref,{kind:'age_evidence',scope:observed.receive_cursor.scope,object_id:age.id,revision:age.revision,digest:age.digest});
 const publisher={binding_id:owner.pairing_id,service_id:authority_service_id,database_instance_id},reference={id:age.id,revision:age.revision,digest:age.digest};
 assert.deepEqual(inbox.ageEvidenceForPublisher(reference,publisher),age);
 assert.equal(inbox.ageEvidenceForPublisher(reference,{...publisher,binding_id:id()}),null);
 assert.equal(inbox.ageEvidenceForPublisher({...reference,digest:digest('wrong')},publisher),null);
 fixture.source.advance(200000n);inbox.accept(receipt,firstFrame,()=>assert.ok(db.inTransaction));
 assert.deepEqual(inbox.sourceEvent(source_key,first.payload.origin.event_id),observed);
 assert.equal(openSources(id()).sourceState(source_key).state,'baseline_required');
 const second=timed('2'),secondFrame=delivery(second.entry,'1','live'),writer=()=>assert.ok(db.inTransaction);
 let reads=0,afterLookup;
 await inbox.acceptFromPeer(receipt,secondFrame,writer,reference=>{
  assert.equal(db.inTransaction,false);assert.deepEqual(reference,second.entry.payload.origin.source_time_ref);++reads;
  fixture.source.advance(1000000n);afterLookup=clock.pairingSample().tick_ns;return second.evidence;
 });
 const retained=inbox.sourceEvent(source_key,second.entry.payload.origin.event_id);
 assert.deepEqual(retained.first_observation.source_time_evidence,second.evidence);
 assert.equal(retained.first_observation.age_evidence.basis,'unproven');
 assert.deepEqual(retained.first_observation.disposition.reason_codes,['source_reference_missing']);
 assert.ok(BigInt(retained.first_observation.received_tick_ns)<BigInt(afterLookup));
 await inbox.acceptFromPeer(receipt,secondFrame,writer,()=>{++reads;return null;});
 assert.equal(reads,1);assert.deepEqual(inbox.sourceEvent(source_key,second.entry.payload.origin.event_id),retained);
 assert.equal(state(),'baseline_required');
 accept(upsert('2'),'1','live');assert.equal(state(),'synchronized');
 const third=timed('3');
 await inbox.acceptFromPeer(receipt,delivery(third.entry,'2','reconcile'),writer,()=>({...third.evidence,payload_digest:digest('wrong payload')}));
 const downgraded=inbox.sourceEvent(source_key,third.entry.payload.origin.event_id).first_observation;
 assert.equal(downgraded.source_time_evidence,undefined);
 assert.deepEqual(downgraded.disposition.reason_codes,['source_time_unverified','reconciled_delivery']);
 accept(upsert('3'),'2','reconcile');assert.equal(state(),'baseline_required');
 accept(marker,'2','reconcile');assert.equal(state(),'synchronized');
 assert.equal(sources.sourceState(source_key).continuity.delivery_cursor,'8');
 const fourth=timed('4'),{capture_domain}=fourth.evidence;
 const fixtureRef=id=>({id,revision:'1',digest:digest(id)});
 const authority=referenceBundleAuthority({serviceId:authority_service_id,configuration:fixtureRef('config'),timeBounds:{lower_ns:'0',upper_ns:'2000000000'}});
 const runtime={service_id:authority_service_id,database_instance_id,launch_intent:{member_id:capture_domain.provider_id},
  admission_receipt:{member_boot_id:capture_domain.provider_boot_id},runtime_binding_id:'runtime',clock_domain_descriptor:capture_domain};
 const timestamp={seconds:'2208988801',fraction:'0',era:'0'};
 const packet={request_size:48,response_size:48,ntp_version:4,ntp_mode:4,leap:0,stratum:1,precision:0,root_delay_word:'0',root_dispersion_word:'0',
  request_digest:digest('request'),response_digest:digest('response'),response_deadline_tick_ns:'1000',t2:timestamp,t3:timestamp};
 const anchor={anchor_id:id(),poll_ref:fixtureRef('poll'),capture_ref:fixtureRef('capture'),authorization:{endpoint_ref:fixtureRef('endpoint')},
  runtime_digest:digest('runtime'),m1_tick_ns:'1',m4_tick_ns:'50',lower_utc_ns:'999999000',upper_utc_ns:'1000002049'};
 const {bundle,state:publisherState}=authority.readBundleOriginals({anchor,runtime,packet,assessment:{tick_ns:'50',lower_utc_ns:'999999000',upper_utc_ns:'1000003049',
  anchor_age_ns:'1000',exchange_age_ns:'1049',upstream_age_ns:'2049'}});
 const {digest:_digest,...capture}=fourth.evidence;
 const claim={...capture,elapsed_profile_ref:authority.references.profile_ref,
  reference_context_ref:{id:bundle.bundle_id,revision:bundle.revision,digest:bundle.bundle_digest},
  utc_interval:{lower_ns:'999999000',upper_ns:'1000003200'}};
 const evidence={...claim,digest:digest(claim)},origin=fourth.entry.payload.origin;
 origin.source_time_ref.digest=evidence.digest;fourth.entry.payload.origin_digest=digest(origin);
 await inbox.acceptFromPeer(receipt,delivery(fourth.entry,'2','live'),writer,()=>evidence,()=>bundle);
 const badInterval=inbox.sourceEvent(source_key,origin.event_id).first_observation;
 assert.deepEqual(badInterval.source_time_evidence,evidence);
 assert.equal(badInterval.source_reference_bundle,undefined);
 assert.deepEqual(badInterval.disposition.reason_codes,['source_reference_unverified']);
 accept(upsert('4'),'2','live');
 const receiverAuthority=referenceBundleAuthority({serviceId:service.service_id,configuration:fixtureRef('config.receiver'),timeBounds:{lower_ns:'0',upper_ns:'3000000000'}});
 let captured;
 function localReference({tick_ns,runtime_domain_ref},seconds='2208988802'){
  const tick=BigInt(tick_ns),m1=tick-200n,m4=tick-100n,utc=(BigInt(seconds)-2208988800n)*1000000000n;
  const receiverRuntime={service_id:service.service_id,database_instance_id:service.database_instance_id,
   launch_intent:{member_id:'reference.receiver'},admission_receipt:{member_boot_id:id()},runtime_binding_id:'runtime.receiver',clock_domain_descriptor:runtime_domain_ref};
  const receiverAnchor={...anchor,anchor_id:id(),m1_tick_ns:String(m1),m4_tick_ns:String(m4),lower_utc_ns:String(utc-1000n),upper_utc_ns:String(utc+2100n)};
  const timestamp={seconds,fraction:'0',era:'0'};
  captured={service_boot_id:bootId,valid_before_tick_ns:String(tick+100000000n),...receiverAuthority.readBundleOriginals({anchor:receiverAnchor,
   runtime:receiverRuntime,packet:{...packet,t2:timestamp,t3:timestamp,response_deadline_tick_ns:String(tick+1000n)},
   assessment:{tick_ns:String(m4),lower_utc_ns:String(utc-1000n),upper_utc_ns:String(utc+3100n),anchor_age_ns:'1000',exchange_age_ns:'1100',upstream_age_ns:'2100'}})};
  return captured;
 }
 async function trustedDelivery(sequence,duringFetch=()=>{},liveTransport=null){
  const candidate=timed(sequence),origin=candidate.entry.payload.origin;
  const {digest:_digest,...fields}=candidate.evidence;
  const corrected={...fields,elapsed_profile_ref:claim.elapsed_profile_ref,reference_context_ref:claim.reference_context_ref,
   utc_interval:{lower_ns:'999999000',upper_ns:'1000003199'}};
  const evidence={...corrected,digest:digest(corrected)};
  origin.source_time_ref.digest=evidence.digest;candidate.entry.payload.origin_digest=digest(origin);
  const frame=delivery(candidate.entry,'2','live');
  await inbox.acceptFromPeer(receipt,frame,writer,()=>{duringFetch();return evidence;},()=>bundle,liveTransport);
  accept(upsert(sequence),'2','live');
  return {origin,frame,observation:inbox.sourceEvent(source_key,origin.event_id).first_observation};
 }
 captureReceiver=localReference;
 const {origin:trustedOrigin,frame:trustedFrame,observation:trusted}=await trustedDelivery('5',()=>{
  assert.ok(captured);fixture.source.advance(2000000n);captureReceiver=()=>assert.fail('after lookup reference read');
 });
 assert.equal(trusted.age_basis,'trusted_utc');
 assert.deepEqual(trusted.age_evidence.first_receive_utc,{lower_ns:'1999999000',upper_ns:'2000003200'});
 assert.deepEqual(trusted.age_evidence.initial_age_ns,{lower_ns:'999995801',upper_ns:'1000004200'});
 assert.deepEqual([trusted.age_evidence.lower_age_ms,trusted.age_evidence.upper_age_ms],[999,1001]);
 assert.equal(trusted.age_evidence.receiver_domain.authority_service_id,service.service_id);
 assert.equal(trusted.age_evidence.receiver_domain.provider_id,'reference.receiver');
 assert.equal(trusted.disposition.state,'observed_only');
 await inbox.acceptFromPeer(receipt,trustedFrame,writer,()=>assert.fail('replay source lookup'));
 assert.deepEqual(inbox.sourceEvent(source_key,trustedOrigin.event_id).first_observation,trusted);
 const request={source_key,event_id:trustedOrigin.event_id,origin_digest:digest(trustedOrigin),first_age_ref:trusted.disposition.age_evidence_ref,
  policy:{max_age_ms:1003,capture_semantics:'event_occurrence'}};
 let transportCurrent=true;
 const dependencies={readSourceInTransaction:sources.sourceStateInTransaction,async prepareSourceTransport(applyId){
  assert.equal(db.inTransaction,false);assert.equal(applyId,receipt.apply_id);
  return {observation:{state:'ready',connection_generation:'2',ack_cursor:String(cursor)},
   assertCurrentInTransaction(){assert.ok(db.inTransaction);assert.ok(transportCurrent);}};
 }};
 const prepared=await inbox.prepareAge(request,dependencies);
 assert.throws(()=>prepared.evaluateInTransaction(),{code:'reconcile_inbox_age_transaction'});
 assert.throws(()=>db.transaction(()=>{assert.equal(prepared.evaluateInTransaction().state,'eligible');throw new Error('rollback');}).immediate(),/rollback/);
 assert.equal(db.prepare('SELECT count(*) FROM event_age_revision').pluck().get(),0);
 const exact=prepared.commit();assert.equal(exact.state,'eligible');assert.equal(exact.age_evidence_ref.revision,'2');
 assert.deepEqual(prepared.commit(),exact);
 const reference2={id:exact.age_evidence_ref.object_id,revision:'2',digest:exact.age_evidence_ref.digest};
 const revision2=inbox.ageEvidenceForPublisher(reference2,publisher);
 assert.deepEqual([revision2.lower_age_ms,revision2.upper_age_ms],[1001,1003]);
 assert.equal(revision2.first_receive_tick_ns,trusted.received_tick_ns);
 assert.equal(inbox.ageEvidenceForPublisher(reference2,{...publisher,binding_id:id()}),null);
 fixture.source.advance(1000000n);
 const stale=(await inbox.prepareAge(request,dependencies)).commit();
 assert.equal(stale.reason,'age_limit_exceeded');assert.equal(stale.age_evidence_ref.revision,'3');
 const regressed=reconcileInbox({db,service,bootId,clock:{pairingSample:()=>({...clock.pairingSample(),tick_ns:trusted.received_tick_ns})},
  isOpen,referenceProvider,triggerDecisions});
 const reversed=await regressed.prepareAge(request,dependencies);
 assert.throws(()=>reversed.commit(),{code:'reconcile_inbox_age_revision_order'});
 transportCurrent=false;assert.equal(prepared.commit().reason,'source_transport_changed');transportCurrent=true;
 const wrongCapture=await inbox.prepareAge({...request,policy:{...request.policy,capture_semantics:'sampled_observation'}},dependencies);
 assert.equal(wrongCapture.commit().reason,'capture_semantics_changed');
 const restarted=reconcileInbox({db,service,bootId:id(),clock,isOpen,referenceProvider,triggerDecisions});
 const oldBoot=await restarted.prepareAge(request,{...dependencies,prepareSourceTransport:()=>assert.fail('old boot transport query')});
 assert.equal(oldBoot.commit().reason,'receiver_boot_changed');
 const reconciled=await inbox.prepareAge({source_key,event_id:third.entry.payload.origin.event_id,origin_digest:third.entry.payload.origin_digest,
  first_age_ref:downgraded.disposition.age_evidence_ref,policy:request.policy},
 {...dependencies,prepareSourceTransport:()=>assert.fail('reconciled transport query')});
 assert.equal(reconciled.commit().reason,'reconciled_delivery');
 captureReceiver=input=>localReference(input,'2208988800');
 const future=await trustedDelivery('6');
 assert.equal(future.observation.age_basis,'unproven');
 assert.deepEqual(future.observation.disposition.reason_codes,['reference_future_conflict']);
 captureReceiver=localReference;
 const revoked=await trustedDelivery('7',()=>{receiverCurrent=false;});
 assert.equal(revoked.observation.age_basis,'unproven');
 assert.equal(revoked.observation.receiver_reference,undefined);
 assert.deepEqual(revoked.observation.disposition.reason_codes,['receiver_reference_unavailable']);
 assert.deepEqual(inbox.sourceEvent(source_key,trustedOrigin.event_id).first_observation,trusted);
 assert.equal(prepared.commit().reason,'receiver_reference_unavailable');
 assert.deepEqual(inbox.ageEvidenceForPublisher(reference2,publisher),revision2);
 receiverCurrent=true;
 const expired=await trustedDelivery('8',()=>fixture.source.advance(100000000n));
 assert.equal(expired.observation.age_basis,'unproven');
 assert.equal(expired.observation.receiver_reference,undefined);
 captureReceiver=null;
 const late=await trustedDelivery('9',()=>{captureReceiver=localReference;});
 assert.equal(late.observation.age_basis,'unproven');
 assert.equal(late.observation.receiver_reference,undefined);
 captureReceiver=({tick_ns})=>({service_boot_id:bootId,valid_before_tick_ns:String(BigInt(tick_ns)+100000000n),bundle,state:publisherState});
 const wrongAuthority=await trustedDelivery('10');
 assert.equal(wrongAuthority.observation.age_basis,'unproven');
 assert.equal(wrongAuthority.observation.receiver_reference,undefined);
 assert.deepEqual(wrongAuthority.observation.source_reference_bundle,bundle);
 assert.equal(prepared.commit().reason,'receiver_reference_unavailable');
 captureReceiver=localReference;
 let preparedChecks=0;
 const live=(offset=0,assertCurrent=()=>{})=>({readSourceInTransaction:sources.sourceStateInTransaction,async prepareSourceTransport(applyId){
  assert.equal(db.inTransaction,false);assert.equal(applyId,receipt.apply_id);assert.ok(captured);
  const previous=db.prepare('SELECT body_digest FROM subscription_reconcile_inbox WHERE apply_id=? AND cursor=?').pluck().get(applyId,cursor-1);
  return {observation:{state:'ready',connection_generation:'2',ack_cursor:String(cursor-1+offset),ack_payload_digest:previous.slice(7)},
   assertCurrentInTransaction(){assert.ok(db.inTransaction);preparedChecks++;assertCurrent();}};
 }});
 const qualified=await trustedDelivery('11',()=>fixture.source.advance(2000000n),live());
 const readDecision=event=>inbox.sourceEvent(source_key,event.origin.event_id).trigger_decision_set;
 const decision=readDecision(qualified),liveEvidence=decision.set.live_evidence;
 assert.equal(liveEvidence.state,'verified');assert.equal(liveEvidence.predecessor.previous_source_sequence,'10');
 assert.equal(liveEvidence.predecessor.previous_origin_digest,digest(wrongAuthority.origin));
 assert.equal(liveEvidence.predecessor.ack_cursor,qualified.frame.previous_cursor);assert.equal(liveEvidence.age_evidence_ref.revision,'2');
 assert.equal(liveEvidence.upper_age_ms,1003);assert.equal(preparedChecks,2);
 await inbox.acceptFromPeer(receipt,qualified.frame,writer,()=>assert.fail('replay source lookup'),null,{prepareSourceTransport(){assert.fail('replay query');}});
 assert.deepEqual(readDecision(qualified),decision);
 const ahead=await trustedDelivery('12',()=>{},live(1));
 assert.deepEqual(readDecision(ahead).set.live_evidence,{state:'unavailable',reason:'source_predecessor_unconfirmed'});
 const behind=await trustedDelivery('13',()=>{},live(-1));
 assert.deepEqual(readDecision(behind).set.live_evidence,{state:'unavailable',reason:'source_predecessor_unconfirmed'});
 const changed=await trustedDelivery('14',()=>{},live(0,()=>{throw new Error('retired transport');}));
 assert.deepEqual(readDecision(changed).set.live_evidence,{state:'unavailable',reason:'source_transport_changed'});
 const counts=()=>['subscription_reconcile_inbox','event_age_revision','automation_trigger_decision_set']
  .map(table=>db.prepare('SELECT count(*) FROM '+table).pluck().get());
 const before=counts();let checks=0;
 await assert.rejects(trustedDelivery('15',()=>fixture.source.advance(2000000n),live(0,()=>{
  if(++checks===2)throw new Error('transport retired at commit');
 })),/transport retired at commit/);
 assert.equal(checks,2);assert.deepEqual(counts(),before);
 cursor--;
 assert.throws(()=>accept(event('16'),'2','live'),{code:'reconcile_inbox_source_gap'});
 assert.equal(state(),'desynchronized');assert.deepEqual(db.pragma('foreign_key_check'),[]);
});
