import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {bindingMaterial} from './device-bindings-fixture.mjs';
import {canonical,digest} from '../dist/runtime/auth-clock-value.mjs';
const root=process.env.LEAFSEAMER_PACKAGE_UNDER_TEST??fileURLToPath(new URL('../',import.meta.url));assert.ok(path.isAbsolute(root));
const {deviceBindingsRequest,authorizeDeviceBindings,deviceBindingsReply,decodeDeviceBindings}=await import(pathToFileURL(path.join(root,'dist/runtime/peer-device-bindings.mjs')));
const id=()=>crypto.randomUUID();
function fixture(){
 const controller=id(),seamer=id(),database=id(),device=id(),scope={format:'leafseamer.device-binding-scope/1',authority_service_id:controller,database_instance_id:database,device_ids:[device]};
 const request={format:'leafseamer.device-bindings-request/1',challenge_id:id(),scope,device_id:device,expected_registration_ref:null};
 const record={state:'activated',transcript_digest:digest('transcript'),transcript:{parties:[{service_id:controller,database_instance_id:database,service_kind:'controller'},
  {service_id:seamer,service_kind:'seamer',permissions:['automation.bindings.read'],object_scope_digest:digest(scope)}]}};
 const bundle=bindingMaterial({authority_service_id:controller,database_instance_id:database},device),reply={format:'leafseamer.device-bindings-response/1',challenge_id:request.challenge_id,
  controller_id:controller,seamer_id:seamer,transcript_digest:record.transcript_digest,bundle_digest:digest(bundle),bundle};
 return {controller,seamer,record,request,reply,check:value=>deviceBindingsReply(value,request,record,controller,seamer)};
}
test('绑定读取具名格式、规范字节与设备范围严格匹配',()=>{
 const f=fixture();assert.deepEqual(f.check(decodeDeviceBindings(Buffer.from(canonical(f.reply)))),f.reply);
 assert.throws(()=>decodeDeviceBindings(Buffer.from(JSON.stringify(f.reply))),{code:'peer_pairing_bindings_canonical'});
 assert.throws(()=>decodeDeviceBindings(Buffer.alloc(262145)),{code:'peer_pairing_bindings_capacity'});
 for(const patch of [{format:'future'},{device_id:id()},{unexpected:true}])assert.throws(()=>deviceBindingsRequest({...f.request,...patch}));
 for(const patch of [{database_instance_id:id()},{device_ids:[...f.request.scope.device_ids,...f.request.scope.device_ids]}])assert.throws(()=>authorizeDeviceBindings(f.record,{...f.request,scope:{...f.request.scope,...patch}},f.controller,f.seamer));
});
test('错误对端、用途、配对状态与旧挑战均不能取得绑定资料',()=>{
 const f=fixture();for(const patch of [{challenge_id:id()},{controller_id:id()},{seamer_id:id()},{transcript_digest:digest('other')}])assert.throws(()=>f.check({...f.reply,...patch}),{code:'peer_pairing_bindings_response'});
 f.record.state='proved';assert.throws(()=>f.check(f.reply),{code:'peer_pairing_purpose'});f.record.state='activated';f.record.transcript.parties[1].permissions=['auth.introspect'];assert.throws(()=>f.check(f.reply),{code:'peer_pairing_purpose'});
});
test('即使外层摘要重算，注册、内容、Schema及别名错配仍拒绝',()=>{
 const f=fixture(),changes=[b=>b.registration.device_id=id(),b=>b.bindings[0].value.authority_id=id(),b=>b.input_schemas[0].document.type='string',b=>b.alias_table.entries.push({from:'x',to:'y'})];
 for(const change of changes){const reply=structuredClone(f.reply);change(reply.bundle);reply.bundle_digest=digest(reply.bundle);assert.throws(()=>f.check(reply));}
 f.request.expected_registration_ref={...f.reply.bundle.registration_ref,digest:digest('old')};assert.throws(()=>f.check(f.reply),{code:'peer_pairing_bindings_registration'});
});
