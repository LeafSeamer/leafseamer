// 实际受监督模块与独立Seamer进程的集成协调器；首次配对和管理员仍由测试装配。
import assert from 'node:assert/strict';
import fs from 'node:fs';
import crypto from 'node:crypto';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {fileURLToPath} from 'node:url';
import {setTimeout as delay} from 'node:timers/promises';
import {digest} from '../dist/runtime/auth-clock-value.mjs';
const id=()=>crypto.randomUUID();
export async function verifyNativeBindingApproval({frames,core,config,materials}){
 const child=spawn(process.execPath,['--disallow-code-generation-from-strings',fileURLToPath(new URL('./pairing-process-fixture.mjs',import.meta.url))],{stdio:['ignore','ignore','pipe','ipc'],windowsHide:true});
 const pending=new Map(),exited=once(child,'exit');let stopped=false,stderr='';child.stderr.on('data',chunk=>{stderr=(stderr+chunk).slice(-4096);});
 child.on('message',message=>{const item=pending.get(message.id);if(!item)return;pending.delete(message.id);clearTimeout(item.timer);message.ok?item.resolve(message.value):item.reject(Object.assign(new Error(message.error.message),{code:message.error.code}));});
 const failed=error=>{for(const item of pending.values()){clearTimeout(item.timer);item.reject(error);}pending.clear();};
 child.on('error',failed);child.on('exit',code=>{stopped=true;failed(new Error('seamer_fixture_exit:'+code+':'+stderr));});
 const seamer=(command,value)=>new Promise((resolve,reject)=>{assert.equal(stopped,false);const key=id(),timer=setTimeout(()=>{pending.delete(key);reject(new Error('seamer_fixture_timeout:'+command));},30000);pending.set(key,{resolve,reject,timer});child.send({id:key,command,value},error=>{if(error){clearTimeout(timer);pending.delete(key);reject(error);}});});
 async function wait(check){const deadline=performance.now()+60000;for(;;){const value=check();if(value)return value;assert.equal(core.exitCode,null,'native_core_exited');
  const failure=frames.find(f=>f.event==='unavailable');assert.equal(failure,undefined,JSON.stringify(failure));assert.ok(performance.now()<deadline,'native_binding_timeout');await delay(20);}}
 const phase=name=>wait(()=>frames.find(f=>f.event==='binding-peer-phase'&&f.phase===name));
 async function controller(command,value){const key=id();assert.equal(fs.existsSync(config.request),false);assert.equal(fs.existsSync(config.response),false);fs.writeFileSync(config.request,JSON.stringify({id:key,command,value}));
  await wait(()=>fs.existsSync(config.response));const result=JSON.parse(fs.readFileSync(config.response,'utf8'));fs.unlinkSync(config.response);assert.equal(result.id,key);if(!result.ok)throw Object.assign(new Error(result.error.message),{code:result.error.code});return result.value;}
 try{
  const first=await phase('connected'),issuer={service_id:first.identity.service_id,issuer_id:first.identity.issuer_id,issuer_epoch:first.identity.issuer_epoch};
  const second=await seamer('initialize',{role:'seamer',material:materials.leaves[1],ca:materials.ca,issuer,principal_id:first.principal_id,bindingRead:true});
  assert.notEqual(first.pid,second.pid);assert.notEqual(second.pid,process.pid);
  await controller('bind',[{identity:second.identity,cert:materials.leaves[1].cert,origin:second.origin,server_name:'127.0.0.1',trust_bundle_digest:second.configuration.trust_bundle_digest}]);
  await seamer('bind',[{identity:first.identity,cert:materials.leaves[0].cert,origin:first.origin,server_name:'127.0.0.1',trust_bundle_digest:first.configuration.trust_bundle_digest}]);
  const scope={format:'leafseamer.device-binding-scope/1',authority_service_id:first.identity.service_id,database_instance_id:first.identity.database_instance_id,device_ids:[first.device_id]};
  const begin=BigInt(Date.now())*1000000n-1000000000n,parties=[first,second].map(p=>({...p.configuration,binding_id:p.bindingId??id(),permissions:p===second?['auth.introspect','automation.bindings.read']:['auth.revoke'],object_scope_digest:p===second?digest(scope):digest({devices:[]}),not_before_ns:String(begin),expires_ns:String(begin+120000000000n)}));
  const transcript={format:'leafseamer.peer-pairing/1',pair_attempt_id:id(),parties},left=await controller('stage',{request_id:id(),transcript}),right=await seamer('stage',{request_id:id(),transcript});
  await controller('prove',left.binding_id);await seamer('prove',right.binding_id);
  for(const [call,record]of [[controller,left],[seamer,right]]){const ready=await call('get',record.binding_id);await call('activate',{request_id:id(),binding_id:record.binding_id,expected_revision:ready.revision,transcript_digest:record.transcript_digest});}
  const issued=await controller('issueIdentity',left.binding_id);let identity;
  const refresh=async()=>{identity=(await seamer('prepareIdentity',{binding_id:right.binding_id,token:issued.token})).handle_id;};await refresh();
  const grant=await seamer('localGrant',{handle_id:identity,principal_id:issued.principal_id});
  await seamer('replaceLocalGrant',{handle_id:identity,request:{request_id:id(),principal_id:issued.principal_id,principal_kind:'human',expected_revision:grant.revision,permissions:[...grant.permissions,'definition.edit'],definition_scope:grant.definition_scope,not_before_ns:grant.not_before_ns,expires_ns:grant.expires_ns}});
  const context=await seamer('createDraftContext',identity),draft=await seamer('createDraft',{handle_id:identity,context_id:context.context_id,key:id()});
  const query={binding_id:right.binding_id,scope,device_id:first.device_id,expected_registration_ref:null};
  const prepared=await seamer('prepareBindings',query),original=frames.find(f=>f.event==='automation-bindings-connected').bindings;assert.deepEqual(prepared.value.reply.bundle,original);
  const candidate=await seamer('bindingCandidate',{identity,definition_id:draft.definition_id,query});assert.deepEqual(candidate.bundle,original);
  const approvalRequest={request_id:id(),approval_id:id(),definition_id:draft.definition_id,expected_bundle_digest:candidate.bundle_digest};
  const approved=await seamer('approveCurrentBindings',{identity,query:{...query,expected_registration_ref:original.registration_ref},request:approvalRequest});
  assert.deepEqual(approved.bundle,original);assert.equal(approved.authorization.actor.principal_id,issued.principal_id);assert.equal(approved.state,'approved');
  const approvalRef={id:approved.approval_id,revision:approved.revision,digest:digest(approved)};
  assert.deepEqual(await seamer('checkBindingApproval',{identity,approval_ref:approvalRef,definition_id:draft.definition_id,handle:prepared.handle_id}),approved);
  frames.push({event:'native-binding-approved',approval:approved,seamer_pid:second.pid,controller_pid:first.pid});await controller('finish');
  await phase('disconnected');await assert.rejects(seamer('prepareBindings',{...query,expected_registration_ref:original.registration_ref}),{code:'peer_pairing_response'});
  await refresh();const lookupRequest={request_id:approvalRequest.request_id,request_digest:digest(approvalRequest),definition_id:draft.definition_id};
  assert.deepEqual((await seamer('lookupBindingApproval',{identity,request:lookupRequest})).result,approved);
  assert.deepEqual(await seamer('approveCurrentBindings',{identity,query,request:approvalRequest}),approved);
  frames.push({event:'native-binding-disconnected',read_denied:true,committed_lookup_verified:true});await controller('finish');
  await phase('reconnected');await assert.rejects(seamer('prepareBindings',{...query,expected_registration_ref:original.registration_ref}),{code:'peer_pairing_response'});
  await refresh();const next=await seamer('prepareBindings',query),fresh=frames.find(f=>f.event==='automation-bindings-reconnected').bindings;assert.deepEqual(next.value.reply.bundle,fresh);assert.notDeepEqual(fresh.registration_ref,original.registration_ref);
  await assert.rejects(seamer('checkBindingApproval',{identity,approval_ref:approvalRef,definition_id:draft.definition_id,handle:next.handle_id}),{code:'binding_approval_source_changed'});
  const replacement=await seamer('approveCurrentBindings',{identity,query:{...query,expected_registration_ref:fresh.registration_ref},request:{request_id:id(),approval_id:id(),definition_id:draft.definition_id,expected_bundle_digest:digest(fresh)}});assert.deepEqual(replacement.bundle,fresh);
  const revoked=await seamer('revokeBindingApproval',{identity,request:{request_id:id(),approval_id:approved.approval_id,expected_revision:'1'}});assert.equal(revoked.state,'revoked');
  assert.deepEqual((await seamer('lookupBindingApproval',{identity,request:lookupRequest})).result,approved);
  frames.push({event:'native-binding-reapproved',approval:replacement,old_approval:revoked});await controller('finish');
  await phase('withdrawn');await assert.rejects(seamer('prepareBindings',{...query,expected_registration_ref:fresh.registration_ref}),{code:'peer_pairing_response'});frames.push({event:'native-binding-withdrawn',read_denied:true});await controller('finish');
 }finally{
  if(!stopped){try{await seamer('shutdown');}catch{child.kill();}}
  await exited;
 }
}
