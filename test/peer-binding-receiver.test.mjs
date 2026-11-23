import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
const packageRoot=process.env.LEAFSEAMER_PACKAGE_UNDER_TEST??fileURLToPath(new URL('../',import.meta.url));assert.ok(path.isAbsolute(packageRoot));
const load=name=>import(pathToFileURL(path.join(packageRoot,'dist',name)));
const {createPeerBindingReceiver}=await load('peer-binding-receiver.mjs');
import {digest} from '../dist/runtime/auth-clock-value.mjs';
import {registered} from '../../leafseamer-controller/test/fixtures/device-bindings.mjs';
const id=()=>crypto.randomUUID();
function fixture(t){
 const controller={service_id:id(),database_instance_id:id()},seamer={service_id:id()},device=id(),f=registered(t,{service:controller,device}),bundle=f.bindings();
 const scope={format:'leafseamer.device-binding-scope/1',authority_service_id:controller.service_id,database_instance_id:controller.database_instance_id,device_ids:[device]};
 const record={binding_id:id(),revision:'4',digest:digest('pairing'),state:'activated',peer_service_id:controller.service_id,transcript_digest:digest('transcript'),transcript:{parties:[{...controller,service_kind:'controller'},
  {...seamer,service_kind:'seamer',permissions:['automation.bindings.read'],object_scope_digest:digest(scope)}]}};
 const state={tick:1n,domain:{clock_domain_id:id()},delay:null,mutate:null},pairings={currentPending(){return structuredClone(record);},requireActivatedInTransaction(ref){assert.deepEqual(ref,{id:record.binding_id,revision:record.revision,digest:record.digest});return structuredClone(record);}};
 const receiver=createPeerBindingReceiver({service:seamer,pairings,clock:{pairingSample:()=>({tick_ns:String(state.tick),runtime_domain_ref:structuredClone(state.domain)})},transport:{async readDeviceBindings(_id,request){if(state.delay)await state.delay;state.mutate?.();return {format:'leafseamer.device-bindings-response/1',challenge_id:request.challenge_id,controller_id:controller.service_id,seamer_id:seamer.service_id,transcript_digest:record.transcript_digest,bundle_digest:digest(bundle),bundle:structuredClone(bundle)};}}});t.after(()=>receiver.close());
 return {receiver,state,record,query:{binding_id:record.binding_id,scope,device_id:device}};
}
test('读取句柄只在原单调域与原截止内可用，外部副本不改变句柄',async t=>{
 const f=fixture(t),handle=await f.receiver.prepare(f.query),first=f.receiver.current(handle);first.reply.bundle.bindings.length=0;assert.equal(f.receiver.currentInTransaction(handle).reply.bundle.bindings.length,2);
 f.state.tick=2000000000n;assert.ok(f.receiver.current(handle));f.state.tick=2000000001n;assert.throws(()=>f.receiver.current(handle),{code:'peer_binding_expired'});
 const second=await f.receiver.prepare(f.query);f.state.domain={clock_domain_id:id()};assert.throws(()=>f.receiver.current(second),{code:'peer_binding_expired'});
});
test('配对修订变化、读取往返超期及关闭后的迟到响应均不能产生可用句柄',async t=>{
 const f=fixture(t),handle=await f.receiver.prepare(f.query);f.record.revision='5';assert.throws(()=>f.receiver.current(handle),{code:'peer_binding_expired'});
 f.state.mutate=()=>{f.state.tick+=2000000000n;};await assert.rejects(f.receiver.prepare(f.query),{code:'peer_binding_expired'});f.state.mutate=null;
 let release;f.state.delay=new Promise(resolve=>{release=resolve;});const pending=f.receiver.prepare(f.query);f.receiver.close();release();await assert.rejects(pending,{code:'peer_binding_closed'});
});
test('显式释放仅销毁对应句柄并归还容量，不能重用原读取身份',async t=>{
 const f=fixture(t),handles=[];for(let i=0;i<64;i++)handles.push(await f.receiver.prepare(f.query));
 await assert.rejects(f.receiver.prepare(f.query),{code:'peer_binding_capacity'});f.receiver.release(handles[0]);f.receiver.release(handles[0]);
 assert.throws(()=>f.receiver.current(handles[0]),{code:'peer_binding_handle'});assert.ok(f.receiver.current(handles[1]));
 const replacement=await f.receiver.prepare(f.query);assert.notEqual(replacement,handles[0]);assert.ok(f.receiver.current(replacement));
});
test('批准正文匹配读取沿用原截止及配对，过期后不恢复',async t=>{
 const f=fixture(t),handle=await f.receiver.prepare(f.query),value=f.receiver.current(handle);
 const expected={pairing_ref:value.pairing_ref,bundle_digest:value.bundle_digest};
 const query={...f.query,expected_registration_ref:value.reply.bundle.registration_ref};
 await assert.rejects(f.receiver.prepareMatch(query,{...expected,bundle_digest:digest('other')}),{code:'peer_binding_changed'});
 const verify=await f.receiver.prepareMatch(query,expected),second=await f.receiver.prepareMatch(query,expected);
 f.record.revision='5';assert.throws(()=>second(false),{code:'peer_binding_expired'});f.record.revision='4';
 f.state.tick=2000000000n;assert.doesNotThrow(()=>verify(true));
 f.state.tick=2000000001n;assert.throws(()=>verify(true),{code:'peer_binding_expired'});
 f.state.tick=2000000000n;assert.throws(()=>verify(true),{code:'peer_binding_expired'});
});
