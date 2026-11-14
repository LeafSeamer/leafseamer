import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
const root=process.env.LEAFSEAMER_PACKAGE_UNDER_TEST??fileURLToPath(new URL('../',import.meta.url));assert.ok(path.isAbsolute(root));
const load=name=>import(pathToFileURL(path.join(root,'dist',name)));
const {createPeerEventReceiver}=await load('peer-event-receiver.mjs'),{digest}=await load('runtime/auth-clock-value.mjs');
const {deviceEventSourceRef}=await load('runtime/peer-device-events.mjs');const id=()=>crypto.randomUUID();
function fixture(t){
 const controller={service_id:id(),database_instance_id:id()},seamer={service_id:id()},device=id(),registration=id(),epoch=id();
 const scope={format:'leafseamer.device-binding-scope/1',authority_service_id:controller.service_id,database_instance_id:controller.database_instance_id,device_ids:[device]},owner={authority_service_id:controller.service_id,database_instance_id:controller.database_instance_id};
 const key={...owner,registration_id:registration,source_stream_id:device,source_epoch:epoch},binding={registration_id:registration,revision:'1',owner_service_id:controller.service_id,database_instance_id:controller.database_instance_id,
  provider_id:'worker.member',provider_boot_id:id(),capture_policy_id:'policy',capture_policy_revision:'1',delivery_semantics:'discrete',target:{kind:'device',device_id:device,trigger_id:'written',capability_version:'1'}};
 const schema={type:'integer'},schemaRef={kind:'schema',scope:owner,object_id:'schema',revision:'1',digest:digest(schema)},source={format:'leafseamer-device-event-source/1',registration_id:registration,revision:'1',source_key:key,binding_ref:binding,
  qualification:{device_id:device,capture:{capture_id:epoch}},canonical_delivery:'ordered_subscription',payload_schema_ref:schemaRef,payload_schema:schema};
 const origin={source_key:key,binding_ref:binding,event_id:id(),source_sequence:'1',payload:{schema_ref:schemaRef,value:7},source_time_ref:null};
 const bundle={registration:source,highest_committed_sequence:'1',fault:null,events:[{origin,origin_digest:digest(origin)}]};
 const record={binding_id:id(),revision:'4',digest:digest('pairing'),state:'activated',peer_service_id:controller.service_id,transcript_digest:digest('transcript'),transcript:{parties:[{...controller,service_kind:'controller'},
  {...seamer,service_kind:'seamer',permissions:['events.internal.read'],object_scope_digest:digest(scope)}]}};
 const state={tick:1n,domain:{clock_domain_id:id()},delay:null,mutate:null};
 const receiver=createPeerEventReceiver({service:seamer,pairings:{currentPending:()=>structuredClone(record),requireActivatedInTransaction:()=>structuredClone(record)},
  clock:{pairingSample:()=>({tick_ns:String(state.tick),runtime_domain_ref:structuredClone(state.domain)})},transport:{async readDeviceEvents(_,request){if(state.delay)await state.delay;
   const reply={format:'leafseamer.device-events-response/1',challenge_id:request.challenge_id,controller_id:controller.service_id,seamer_id:seamer.service_id,transcript_digest:record.transcript_digest,bundle_digest:digest(bundle),bundle:structuredClone(bundle)};state.mutate?.(reply);return reply;}}});t.after(()=>receiver.close());
 return {receiver,record,state,bundle,query:{binding_id:record.binding_id,scope,device_id:device}};
}
test('保存独立原件副本和读取区间，原域及2秒截止不因读取刷新',async t=>{
 const f=fixture(t),handle=await f.receiver.prepare(f.query),value=f.receiver.current(handle);assert.equal(value.reply.bundle.events[0].origin.source_time_ref,null);assert.deepEqual(value.source_ref,deviceEventSourceRef(f.bundle.registration));
 value.reply.bundle.events[0].origin.payload.value=99;assert.equal(f.receiver.currentInTransaction(handle).reply.bundle.events[0].origin.payload.value,7);
 f.state.tick=2000000000n;assert.ok(f.receiver.current(handle));f.state.tick=2000000001n;assert.throws(()=>f.receiver.current(handle),{code:'peer_event_expired'});
 const next=await f.receiver.prepare(f.query);f.state.domain={clock_domain_id:id()};assert.throws(()=>f.receiver.current(next),{code:'peer_event_expired'});
});
test('错误用途、对象范围、来源引用及重算摘要后的缺页拒绝',async t=>{
 for(const variant of ['purpose','scope','reference','missing','gap','payload-schema','origin-schema']){
  const f=fixture(t);if(variant==='purpose')f.record.transcript.parties[1].permissions=['automation.bindings.read'];
  if(variant==='scope')f.record.transcript.parties[1].object_scope_digest=digest('other');
  if(variant==='reference')f.query.expected_source_ref={...deviceEventSourceRef(f.bundle.registration),digest:digest('other')};
  f.state.mutate=reply=>{const b=reply.bundle;if(variant==='missing')b.events=[];if(variant==='gap')b.events[0].origin.source_sequence='2';
   if(variant==='payload-schema')b.registration.payload_schema={type:'string'};if(variant==='origin-schema')b.events[0].origin.extra=true;
   for(const item of b.events)item.origin_digest=digest(item.origin);reply.bundle_digest=digest(b);};await assert.rejects(f.receiver.prepare(f.query));
 }
});
test('配对撤销、往返超期及关闭后的迟到响应不能保留可用句柄',async t=>{
 const f=fixture(t),handle=await f.receiver.prepare(f.query);f.record.state='revoked';assert.throws(()=>f.receiver.current(handle),{code:'peer_event_expired'});f.record.state='activated';
 f.state.mutate=()=>{f.state.tick+=2000000000n;};await assert.rejects(f.receiver.prepare(f.query),{code:'peer_event_expired'});f.state.mutate=null;
 let release;f.state.delay=new Promise(resolve=>{release=resolve;});const pending=f.receiver.prepare(f.query);f.receiver.close();release();await assert.rejects(pending,{code:'peer_event_closed'});
});
test('读取预算同时限制并行请求和保留句柄，显式释放不影响其他句柄',async t=>{
 const f=fixture(t);let release;f.state.delay=new Promise(resolve=>{release=resolve;});const jobs=Array.from({length:4},()=>f.receiver.prepare(f.query));await assert.rejects(f.receiver.prepare(f.query),{code:'peer_event_capacity'});release();const handles=await Promise.all(jobs);f.state.delay=null;
 for(let i=4;i<64;i++)handles.push(await f.receiver.prepare(f.query));await assert.rejects(f.receiver.prepare(f.query),{code:'peer_event_capacity'});
 f.receiver.release(handles[0]);assert.throws(()=>f.receiver.current(handles[0]),{code:'peer_event_handle'});assert.ok(f.receiver.current(handles[1]));assert.ok(await f.receiver.prepare(f.query));
});
