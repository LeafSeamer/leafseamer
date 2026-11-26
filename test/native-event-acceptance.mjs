// 比对独立 Seamer 与原生模拟器的配对来源。完整场景使用真实首次授权和本机导入，环境资格使用受控测试材料。
import assert from 'node:assert/strict';
import fs from 'node:fs';
import https from 'node:https';
import crypto from 'node:crypto';
import path from 'node:path';
import {spawn,spawnSync} from 'node:child_process';
import {once} from 'node:events';
import {fileURLToPath} from 'node:url';
import {setTimeout as delay} from 'node:timers/promises';
import {canonical,digest} from '../dist/runtime/auth-clock-value.mjs';
import {createClient,contractSelection} from '../dist/sdk/index.mjs';
import {verifyEventOrigin} from '../dist/sdk/event-time.mjs';
const id=()=>crypto.randomUUID();
function requestHttps(origin,ca,{method,target,headers,body,credential}){
 return new Promise((resolve,reject)=>{
  const req=https.request(new URL(target,origin),{ca,method,headers:Object.fromEntries(headers),agent:false,...credential},res=>{
   const chunks=[];
   res.on('data',part=>chunks.push(part));res.on('error',reject);
   res.on('end',()=>resolve({status:String(res.statusCode),headers:Array.from({length:res.rawHeaders.length/2},(_,i)=>[res.rawHeaders[2*i],res.rawHeaders[2*i+1]]),
    body:Buffer.concat(chunks)}));
  });
  req.on('error',reject);req.setTimeout(8000,()=>req.destroy(new Error('acceptance_https_timeout')));req.end(body);
 });
}
async function verifyScheduledAdmission({seamer,frames,origin,ca,bearer,armed,grantRef,program,refresh,dispatch,withdrawReference}){
 async function request(operation,input){
  const body=Buffer.from(canonical({operation,request:input}));
  const response=await requestHttps(origin,ca,{target:'/auth/local-schedules/v1',method:'POST',body,
   headers:[bearer,['content-type','application/json'],['content-length',String(body.length)]]});
  assert.equal(response.status,'200',response.body.toString());
  return JSON.parse(response.body).result;
 }
 const context=async operation_id=>({context_id:(await request('create-context',{operation_id,ttl_ms:30000,max_unique_requests:1})).context_id,
  idempotency_key:id()});
 const schedule_id=id(),publishContext=await context('publishSchedule'),activateContext=await context('activateSchedule');
 const {current}=await seamer('nativeReferenceStatus');
 assert.equal(current?.status,'conditional');
 const local_start=new Date(Number(BigInt(current.upper_utc_ns)/1000000n)+5000).toISOString().slice(0,-1);
 const steps=program.program.groups.flatMap(({steps})=>steps.filter(step=>step.kind==='action')).map(({step_id})=>
  ({step_id,role:'on_air',start_offset_ms:0,latest_start_offset_ms:12000}));
 assert.equal(steps.length,1);
 const published=await request('publish',{schedule_id,version:{context:publishContext,definition_id:armed.definition_id,
  definition_version:armed.definition_version,expected_artifact_ref:armed.artifact_ref,expected_version:null,
  items:[{item_id:'show',one_shot_key:'once',local_start,offset:'+00:00',steps}]}});
 const activation=await request('activate',{schedule_id,activation:{context:activateContext,expected_revision:null,schedule_version_ref:published.ref}});
 const read=()=>request('get',{schedule_id,version:null});
 const tracked=await read();
 assert.equal(tracked.occurrences[0].state,'tracking');
 assert.equal(frames.some(f=>f.event==='automation-target-accepted'),false);
 const until=performance.now()+22000;
 let accepted;
 do{
  const failure=frames.find(f=>f.event==='peer-transport-failure'&&f.method==='submitAutomationCommand');
  assert.equal(failure,undefined,JSON.stringify(failure));
  accepted=frames.find(f=>f.event==='automation-target-accepted');
  if(accepted)break;
  await delay(50);
 }while(performance.now()<until);
 const view=await read(),occurrence=view.occurrences[0];
 assert.equal(occurrence.state,'accepted',JSON.stringify({state:occurrence.state,reason:occurrence.stop?.reason??occurrence.qualification?.reason}));
 assert.ok(accepted,'Controller did not durably accept the scheduled command');
 const {admission}=occurrence;
 assert.deepEqual(admission.activation_ref,activation.ref);
 let terminal=null,resource=null;
 if(dispatch){
  const deadline=performance.now()+20000;
  let settled,failed;
  do{
   const failure=frames.find(f=>f.event==='automation-target-failed');
   if(withdrawReference&&failure){
    const withdrawal=frames.find(f=>f.event==='scheduled-reference-withdrawn');
    const stages=frames.filter(f=>['automation-target-stage','automation-target-stage-failed'].includes(f.event))
     .map(({stage,present,disposition,code})=>({stage,present,disposition,code}));
    assert.equal(withdrawal?.command_id,accepted.reference.object_id,JSON.stringify({failure,stages}));
    assert.deepEqual(failure.reference,accepted.reference);
    failed=failure;
    break;
   }
   assert.equal(failure,undefined,JSON.stringify({failure,stages:frames.filter(f=>f.event.startsWith('automation-target-'))
    .map(({event,stage,present,disposition,code,cause,constraint,reported_ms,remaining_ms})=>({event,stage,present,disposition,code,cause,constraint,reported_ms,remaining_ms}))}));
   settled=frames.find(f=>f.event==='automation-target-settled');
   if(settled)break;
   await delay(25);
  }while(performance.now()<deadline);
  if(withdrawReference)assert.ok(failed&&!settled,'Controller did not refuse the write after reference withdrawal');
  else assert.deepEqual(settled?.reference,accepted.reference,'Scheduled command did not settle in Controller');
  const adoptionDeadline=performance.now()+12000;
  do{
   const current=await seamer('nativeTriggerExecution',{identity:await refresh(),execution_id:admission.execution_id});
   const submitted=current.steps[0].submission;
   if(withdrawReference&&submitted?.terminal){
    assert.deepEqual(submitted.adjudication.command_ref,accepted.reference);
    assert.equal(submitted.terminal.state.outcome,'blocked');
    assert.equal(submitted.terminal.state.reason_code,'execution_closed');
    assert.equal(current.completion,null);
    terminal=submitted.terminal;
    break;
   }
   if(current.completion){
    assert.equal(withdrawReference,false,'A withdrawn scheduled command completed');
    assert.deepEqual(submitted.adjudication.command_ref,accepted.reference);
    assert.equal(submitted.terminal.state.outcome,'succeeded');
    assert.equal(submitted.terminal.completion_achieved,'effect_verified');
    assert.equal(current.completion.notification.event_id,admission.execution_id+'.completed');
    terminal=submitted.terminal;
    break;
   }
   if(!withdrawReference)assert.equal(current.protection,null,JSON.stringify({protection:current.protection,state:submitted?.state}));
   await delay(200);
  }while(performance.now()<adoptionDeadline);
  assert.ok(terminal,'Scheduled execution did not consume the Controller terminal receipt');
  const resources=await seamer('nativeAutomationCommandResource',{identity:await refresh(),execution_id:admission.execution_id,
   step_id:admission.first_step_id});
  assert.deepEqual(resources.adjudication.command_ref,accepted.reference);
  assert.deepEqual(resources.terminal,terminal);
  resource=resources.resource;
  assert.equal(resource?.status,'released');
  assert.ok(resource.hold_ref&&resource.resource_commit_ref);
  assert.deepEqual(resource.resource_evidence_refs,terminal.state.resource_evidence_refs);
  const finished=await seamer('getRun',{identity:await refresh(),run_id:armed.run_id});
  if(withdrawReference){
   assert.equal(finished.lifecycle,'paused');
   assert.equal(finished.active_execution_id,admission.execution_id);
   const body=Buffer.from(canonical({operation:'preflight-recovery',request:{run_id:armed.run_id,execution_id:admission.execution_id}}));
   const response=await requestHttps(origin,ca,{target:'/auth/local-run-approvals/v1',method:'POST',body,
    headers:[bearer,['content-type','application/json'],['content-length',String(body.length)]]});
   assert.equal(response.status,'200',response.body.toString());
   const {result}=JSON.parse(response.body);
   assert.equal(result.current_protection.state,'unconfirmed');
   assert.equal(result.current_resources.state,'verified');
   assert.equal(result.recovery_evidence_state,'unconfirmed');
   assert.equal(result.current_resources.steps.length,1);
   assert.equal(result.current_resources.steps[0].state,'released');
   assert.deepEqual(result.current_resources.steps[0].resource,resource);
   assert.equal(result.steps[0].terminal.outcome,'blocked');
   const previewRequest=Buffer.from(canonical({operation:'preview-recovery',request:{run_id:armed.run_id,
    execution_id:admission.execution_id,expected_revision:finished.revision}}));
   const saved=await requestHttps(origin,ca,{target:'/auth/local-run-approvals/v1',method:'POST',body:previewRequest,
    headers:[bearer,['content-type','application/json'],['content-length',String(previewRequest.length)]]});
   assert.equal(saved.status,'200',saved.body.toString());
   const {preview_ref,preview}=JSON.parse(saved.body).result;
   assert.equal(preview_ref.digest,digest(preview));
   assert.equal(preview.snapshot.execution_ref.object_id,admission.execution_id);
   assert.deepEqual(preview.snapshot.current_resources,result.current_resources);
   assert.equal(preview.plan.mode,'blocked');
   assert.equal(preview.plan.items[0].reason,'scheduled_occurrence_not_replayed');
   const recoveryContext=Buffer.from(canonical({operation:'create-resume-context',request:{ttl_ms:30000,max_unique_requests:1}}));
   const denied=await requestHttps(origin,ca,{target:'/auth/local-run-approvals/v1',method:'POST',body:recoveryContext,
    headers:[bearer,['content-type','application/json'],['content-length',String(recoveryContext.length)]]});
   assert.equal(denied.status,'403',denied.body.toString());
  }else{
   assert.equal(finished.lifecycle,'armed');
   assert.equal(finished.active_execution_id,null);
   assert.equal(finished.last_execution_id,admission.execution_id);
  }
 }
 const execution=await seamer('nativeTriggerExecution',{identity:await refresh(),execution_id:admission.execution_id});
 assert.equal(execution.execution.origin_kind,'schedule');
 assert.deepEqual(execution.admission,admission);
 const step=execution.steps[0];
 assert.deepEqual(step.prepared_parameters.body.parameters,{value:12});
 assert.equal(step.plan.step_id,admission.first_step_id);
 frames.push({event:'native-scheduled-target-admission',reference:accepted.reference,grant_ref:grantRef,admission,terminal,resource,
  receiver_reference:'native'});
}
export async function verifyNativeEventRead({frames,core,config,materials}){
 const scheduledAdmission=config.scheduled_admission===true;
 const nativeReceiver=config.source_reference_capture&&(scheduledAdmission||process.env.LEAFSEAMER_EVENT_RECEIVER_NATIVE_TEST==='1');
 const nativeReferenceAssets=nativeReceiver?process.env.LEAFSEAMER_SUPERVISOR_DIST:null;
 if(nativeReceiver)assert.ok(config.first_enrollment&&nativeReferenceAssets&&path.isAbsolute(nativeReferenceAssets));
 let referenceWitness=null;
 const child=spawn(process.execPath,['--disallow-code-generation-from-strings',fileURLToPath(new URL('./pairing-process-fixture.mjs',import.meta.url))],{stdio:['ignore','ignore','pipe','ipc'],windowsHide:true});
 const pending=new Map(),exited=once(child,'exit');let stopped=false,stderr='';child.stderr.on('data',chunk=>{stderr=(stderr+chunk).slice(-4096);});
 child.on('message',message=>{const item=pending.get(message.id);if(!item)return;pending.delete(message.id);clearTimeout(item.timer);message.ok?item.resolve(message.value):item.reject(Object.assign(new Error(message.error.message),{code:message.error.code,remoteStack:message.error.stack,caller:item.caller}));});
 const failed=error=>{for(const item of pending.values()){clearTimeout(item.timer);item.reject(error);}pending.clear();};child.on('error',failed);child.on('exit',code=>{stopped=true;failed(new Error('event_peer_exit:'+code+':'+stderr));});
 const seamer=(command,value)=>new Promise((resolve,reject)=>{assert.equal(stopped,false);const key=id(),timer=setTimeout(()=>{pending.delete(key);reject(new Error('event_peer_timeout:'+command));},30000);pending.set(key,{resolve,reject,timer,caller:new Error(command).stack});child.send({id:key,command,value},error=>{if(error){clearTimeout(timer);pending.delete(key);reject(error);}});});
 async function wait(check){const end=performance.now()+60000;for(;;){const value=check();if(value)return value;assert.equal(core.exitCode,null);assert.equal(frames.find(f=>f.event==='unavailable'),undefined,JSON.stringify(frames.find(f=>f.event==='unavailable')));assert.ok(performance.now()<end,'native_event_timeout');await delay(20);}}
 async function controller(command,value){const key=id();assert.equal(fs.existsSync(config.request),false);assert.equal(fs.existsSync(config.response),false);fs.writeFileSync(config.request,JSON.stringify({id:key,command,value}));await wait(()=>fs.existsSync(config.response));const result=JSON.parse(fs.readFileSync(config.response,'utf8'));fs.unlinkSync(config.response);assert.equal(result.id,key);if(!result.ok)throw Object.assign(new Error(result.error.message),{code:result.error.code,remoteStack:result.error.stack});return result.value;}
 try{
  const first=await wait(()=>frames.find(f=>f.event==='binding-peer-phase'&&f.phase==='events')),issuer={service_id:first.identity.service_id,issuer_id:first.identity.issuer_id,issuer_epoch:first.identity.issuer_epoch};
  const browser=config.publication_gui,gui=browser?await wait(()=>frames.find(f=>f.event==='gui-endpoint')):null;
  let executionSource=null,referenceTimeline=null;
  const armedDispatch=config.verify_armed_target_dispatch===true;
  const managedSubscriptions=config.first_enrollment&&config.incremental_source_event&&process.env.LEAFSEAMER_MANAGED_SOURCE_SUBSCRIPTION_TEST==='1';
  const focused=config.focused_dispatch===true;
  if(focused)assert.ok(armedDispatch&&nativeReceiver&&!browser);
  if(armedDispatch){
   assert.ok(config.source_reference_capture);
   const original=await wait(()=>frames.find(f=>f.event==='event-source-published'));
   const {origin}=original.published.events[0],binding=origin.binding_ref;
   executionSource={authority_id:origin.source_key.authority_service_id,registration_ref:{id:binding.registration_id,revision:binding.revision,digest:digest(binding)}};
   if(nativeReceiver){
    const {current}=await wait(()=>frames.find(f=>f.event==='command-reference-anchor'));
    referenceTimeline={tick_ns:current.tick_ns,utc_ns:current.upper_utc_ns};
   }
  }
  const second=await seamer('initialize',{role:'seamer',material:materials.leaves[1],ca:materials.ca,issuer,principal_id:first.principal_id,
   allowedOrigins:gui?[gui.origin]:[],reconcileIngest:config.verifyEventChannel===true,eventApprovals:config.incremental_source_event===true,
   receiverReference:config.source_reference_capture===true&&!nativeReceiver,nativeReferenceAssets,referenceTimeline,
   firstEnrollment:config.first_enrollment===true,armTarget:config.arm_target,executionSource,managedSubscriptions,scheduledAdmission});assert.notEqual(first.pid,second.pid);
  if(browser){
   const directory=path.dirname(config.request),siteFile=path.join(directory,'publication-site.json'),assets=path.join(directory,'publication-gui');
   fs.writeFileSync(siteFile,canonical({site_label:'首次配对发布联调',service_id:first.identity.service_id,database_instance_id:first.identity.database_instance_id,
    seamer:{service_id:second.identity.service_id,database_instance_id:second.identity.database_instance_id,base_url:second.apiOrigin,service_label:'本机 Seamer'}}));
   const built=spawnSync(process.execPath,['scripts/build.mjs','--config',siteFile,'--out-dir',assets],
    {cwd:browser.root,encoding:'utf8',windowsHide:true,timeout:20000});
   assert.equal(built.status,0,[built.stderr,built.stdout,built.error?.code].join('\n'));
   await controller('configurePublicationGui',assets);
  }
  await controller('bind',[{identity:second.identity,cert:materials.leaves[1].cert,origin:second.origin,server_name:'127.0.0.1',trust_bundle_digest:second.configuration.trust_bundle_digest}]);
  await seamer('bind',[{identity:first.identity,cert:materials.leaves[0].cert,origin:first.origin,server_name:'127.0.0.1',trust_bundle_digest:first.configuration.trust_bundle_digest}]);
  const armTarget=config.arm_target,actionDevice=armTarget??first.device_id;
  const deviceIds=armTarget?[first.device_id,armTarget].sort():[first.device_id];
  const scope={format:'leafseamer.device-binding-scope/1',authority_service_id:first.identity.service_id,database_instance_id:first.identity.database_instance_id,device_ids:deviceIds};
  let issued=null;
  const operator=async()=>{
   const selected=issued;
   return (await seamer('prepareIdentity',{binding_id:second.bindingId,token:selected.token})).handle_id;
  };
  async function activate(permissions,controllerPermissions=[],seamerBindingId=id(),enroll=false){const begin=BigInt(Date.now())*1000000n-1000000000n;
   const parties=[first,second].map(p=>({...p.configuration,binding_id:p===second?seamerBindingId:id(),permissions:p===second?permissions:controllerPermissions,object_scope_digest:p===second?digest(scope):digest({devices:[]}),not_before_ns:String(begin),expires_ns:String(begin+120000000000n)}));
   const transcript={format:'leafseamer.peer-pairing/1',pair_attempt_id:id(),parties},left=await controller('stage',{request_id:id(),transcript});
   const request={request_id:id(),transcript};
   let envelope=null,right;
   if(enroll){
    envelope=await controller('exportFirstPairing',{binding_id:left.binding_id,transcript_digest:left.transcript_digest});
    assert.equal(envelope.authorization.administrator.principal_id,first.principal_id);
    right=await seamer('importFirstPairing',{envelope,controllerCertificate:materials.leaves[0].cert});
    assert.equal(right.state,'staged');
   }else right=issued?await seamer('stageWithIdentity',{identity:await operator(),request}):await seamer('stage',request);
   assert.equal(right.binding_id,seamerBindingId);
   await controller('prove',left.binding_id);await seamer('prove',right.binding_id);
   const activation=record=>({request_id:id(),binding_id:record.binding_id,expected_revision:record.revision,transcript_digest:record.transcript_digest});
   await controller('activate',activation(await controller('get',left.binding_id)));
   if(enroll){
    const authorizationId=envelope.authorization.authorization_id,completed=await seamer('completeFirstPairing',authorizationId);
    assert.equal(completed.administrator.principal_id,first.principal_id);assert.equal(completed.pairing_ref.id,right.binding_id);
    assert.deepEqual(await seamer('completeFirstPairing',authorizationId),completed);
    issued=await controller('issueIdentity',left.binding_id);
    const grant=await seamer('localGrant',{handle_id:await operator(),principal_id:first.principal_id});
    assert.deepEqual(grant.permissions,['auth.manage','auth.read']);
   }else if(issued){
    const ready=await seamer('getWithIdentity',{identity:await operator(),binding_id:right.binding_id});
    await seamer('activateWithIdentity',{identity:await operator(),request:activation(ready)});
   }else await seamer('activate',activation(await seamer('get',right.binding_id)));
   return right;
  }
  const purposes=['event.internal.subscribe','events.internal.read'];
  if(config.source_reference_capture)purposes.push('clock.internal.read');
  if(config.incremental_source_event)purposes.push('auth.introspect','automation.bindings.read','automation.prepare','automation.prepare.read','automation.protect');
  const active=await activate(purposes.sort(),config.incremental_source_event?['automation.prepare.read','events.internal.read']:[],second.bindingId,config.first_enrollment===true);
  const denied=focused?null:await activate(['automation.bindings.read']);
  if(denied)await assert.rejects(seamer('prepareEvents',{binding_id:denied.binding_id,scope,device_id:first.device_id}),{code:'peer_pairing_purpose'});
  const query={binding_id:active.binding_id,scope,device_id:first.device_id},page=await seamer('prepareEvents',{...query,limit:1});
  const original=frames.find(f=>f.event==='event-source-published').published,baselineSequence=config.incremental_source_event?'1':'2';assert.deepEqual(page.value.reply.bundle.registration,original.registration);assert.deepEqual(page.value.reply.bundle.events,[original.events[0]]);
  assert.ok(original.events.every(e=>(e.origin.source_time_ref!==null)===Boolean(config.source_reference_capture)));
  if(!focused){
   assert.deepEqual(await seamer('currentEvents',page.handle_id),page.value);
   const secondPage=await seamer('prepareEvents',{...query,after_sequence:'1',expected_source_ref:page.value.source_ref});assert.deepEqual(secondPage.value.reply.bundle.events,original.events.slice(1));
   assert.deepEqual((await seamer('prepareEvents',{...query,expected_source_ref:page.value.source_ref})).value.reply.bundle,original);
   await assert.rejects(seamer('prepareEvents',{...query,expected_source_ref:{...page.value.source_ref,digest:digest('wrong')}}),{code:'peer_pairing_response'});
  }
  await seamer('releaseEvents',page.handle_id);
  if(!focused)await assert.rejects(seamer('currentEvents',page.handle_id),{code:'peer_event_handle'});
  let stopScope=null,eventApproval=null,approvalProtection=null,closeSubscription=null,finishConsumer=null,grantRef=null,priorMatchSet=null;
  if(config.verifyEventScopes!==false){
  const leftId=active.transcript.parties.find(p=>p.service_id===first.identity.service_id).binding_id,left=await controller('get',leftId);
  const scopeRequest={request_id:id(),device_ids:[first.device_id]},registered=await controller('registerEventScope',scopeRequest);
  if(!focused)assert.deepEqual(await controller('registerEventScope',scopeRequest),registered);
  const pairing_ref={id:left.binding_id,revision:left.revision,digest:left.digest},allowed_scope_ref=registered.scope_ref;
  const readSourceTime=(reference,selection=contractSelection,binding=left.binding_id)=>requestHttps(first.origin,materials.ca,{
   method:'GET',target:`/internal/events/source-times/${reference.object_id}/${reference.revision}?digest=${encodeURIComponent(reference.digest)}`,
   headers:[['X-LeafSeamer-Pairing',binding],['X-LeafSeamer-Contract',selection.contract_id],['X-LeafSeamer-Contract-Digest',selection.contract_digest]],
   credential:{...materials.leaves[1],minVersion:'TLSv1.3',maxVersion:'TLSv1.3'}});
  const readBundle=(reference,binding=left.binding_id)=>requestHttps(first.origin,materials.ca,{
   method:'GET',target:`/internal/reference-time/bundles/${reference.id}/${reference.revision}?digest=${encodeURIComponent(reference.digest)}`,
   headers:[['X-LeafSeamer-Pairing',binding],['X-LeafSeamer-Contract',contractSelection.contract_id],['X-LeafSeamer-Contract-Digest',contractSelection.contract_digest]],
   credential:{...materials.leaves[1],minVersion:'TLSv1.3',maxVersion:'TLSv1.3'}});
  const bundleRef=config.source_reference_capture?frames.find(f=>f.event==='command-reference-anchor')?.current?.bundle_ref:null;
  if(config.source_reference_capture&&!focused){
   assert.ok(bundleRef);
   assert.equal((await readSourceTime(original.events[0].origin.source_time_ref)).status,'403');
   assert.equal((await readBundle(bundleRef)).status,'403');
  }
  assert.deepEqual(registered.registration.sources.map(s=>s.source_key),[original.registration.source_key]);
  assert.deepEqual(registered.registration.sources.map(s=>s.registration_sequence),[baselineSequence]);
  if(!focused)assert.deepEqual((await controller('currentEventScope',allowed_scope_ref)).registration,registered.registration);
  const slot={publisher_service_id:first.identity.service_id,consumer_service_id:second.identity.service_id,consumer_database_instance_id:second.identity.database_instance_id,
   consumer_id:'canonical_events',purpose:'canonical_trigger',allowed_scope_ref,max_subscriptions:1,pairing_ref};
  const consumerQuery={pairing_ref,consumer_id:slot.consumer_id,purpose:slot.purpose,allowed_scope_ref};
  if(!focused)await assert.rejects(controller('currentConsumer',consumerQuery),{code:'service_identity_consumer_unavailable'});
  const principal=await controller('createConsumer'),approvalInput={request_id:id(),principal_id:principal.id,expected_revision:principal.revision,audiences:[first.identity.service_id],
   permissions:['event.internal.subscribe','status.read'],device_ids:deviceIds,not_before_ns:left.not_before_ns,expires_ns:left.expires_ns,consumer_slots:[slot]};
  const deniedLeft=focused?null:await controller('get',denied.transcript.parties.find(p=>p.service_id===first.identity.service_id).binding_id);
  if(deniedLeft){
   await assert.rejects(controller('approveConsumer',{...approvalInput,consumer_slots:[{...slot,pairing_ref:{id:deniedLeft.binding_id,revision:deniedLeft.revision,digest:deniedLeft.digest}}]}),{code:'service_consumer_pairing'});
   await assert.rejects(controller('approveConsumer',{...approvalInput,permissions:['event.internal.subscribe']}),{code:'service_identity_consumer_scope'});
   await assert.rejects(controller('approveConsumer',{...approvalInput,consumer_slots:[{...slot,allowed_scope_ref:{...allowed_scope_ref,content_digest:'b'.repeat(64)}}]}),{code:'event_scope_reference'});
  }
  const approved=await controller('approveConsumer',approvalInput);assert.equal(approved.approval.format,'local-service-approval/2');
  if(!focused)assert.deepEqual(await controller('approveConsumer',approvalInput),approved);
  const consumer=await controller('currentConsumer',consumerQuery);assert.equal(consumer.owner.principal_id,principal.id);assert.equal(consumer.owner.consumer_database_instance_id,second.identity.database_instance_id);
  if(config.verifySubscriptions!==false){
  if(config.verifySourceProjection){const cut=await controller('sourceSnapshotCut',allowed_scope_ref);assert.equal(cut.records.length,1);assert.equal(cut.records[0].view.contiguous_sequence,baselineSequence);assert.equal(cut.records[0].view.state,'baseline_required');assert.equal(cut.records[0].view.baseline_ref,null);assert.deepEqual(cut.records[0].view.source_key,registered.registration.sources[0].source_key);assert.equal(cut.source_watermarks[0].baseline_sequence,baselineSequence);assert.deepEqual(await controller('sourceSnapshotCut',allowed_scope_ref),cut);frames.push({event:'native-source-projection-cut',cut});}
  const subscriptionRequest={request_id:id(),consumer_id:slot.consumer_id,purpose:slot.purpose,scope:registered.registration.scope},createSubscription=request=>seamer('createSubscription',{binding_id:active.binding_id,request});
  if(!focused)await assert.rejects(createSubscription(subscriptionRequest),{code:'forbidden'});
  const policyRequest={request_id:id(),principal_id:principal.id,subject_ref:{id:approved.principal.id,revision:approved.principal.revision,digest:approved.principal.digest},approval_ref:{id:approved.approval.id,revision:approved.approval.revision,digest:approved.approval.digest},
   audience:first.identity.service_id,permissions:approvalInput.permissions,device_ids:approvalInput.device_ids,not_before_ns:approvalInput.not_before_ns,expires_ns:approvalInput.expires_ns,consumer_slots:[slot]};
  const policy=await controller('approveConsumerPolicy',policyRequest);assert.equal(policy.format,'local-service-receiver-policy/2');
  if(!focused)assert.deepEqual(await controller('approveConsumerPolicy',policyRequest),policy);
  const subscription=await createSubscription(subscriptionRequest);assert.equal(subscription.status,202);assert.equal(subscription.view.state,'building');assert.equal(subscription.view.snapshot_ref,null);assert.deepEqual(subscription.view.owner,consumer.owner);
  const subQuery={binding_id:active.binding_id,subscription_id:subscription.view.binding.subscription_id};
  let currentSubscription=subscription;
  if(config.verifySnapshots){const deadline=Date.now()+5000;do{currentSubscription=await seamer('getSubscription',subQuery);if(currentSubscription.view.state!=='building')break;await new Promise(resolve=>setTimeout(resolve,50));}while(Date.now()<deadline);
   assert.equal(currentSubscription.view.state,'snapshot_ready');assert.equal(currentSubscription.view.revision,'2');assert.ok(currentSubscription.view.snapshot_ref);assert.equal(currentSubscription.view.ack_payload_digest,'0'.repeat(64));frames.push({event:'native-service-snapshot-ready',view:currentSubscription.view});}
  if(!armedDispatch)assert.deepEqual(await createSubscription(subscriptionRequest),{status:200,view:currentSubscription.view});
  if(config.verifyEventChannel){
   if(!armedDispatch)await assert.rejects(seamer('subscriptionSnapshot',{...subQuery,generation:'0'}),{code:'conflict'});
   const connected=await seamer('attachSubscription',{binding_id:active.binding_id,view:currentSubscription.view});assert.equal(connected.attached.binding.connection_generation,'1');assert.deepEqual(connected.ready.channel.owner,consumer.owner);
   const manifest=(await seamer('subscriptionSnapshot',{...subQuery,generation:'1'})).view;
   let page;
   if(!armedDispatch){
    page=(await seamer('subscriptionPage',{...subQuery,generation:'1',snapshot_id:manifest.binding.snapshot_id,page_index:0})).view;
    assert.equal(page.delivery_binding.connection_generation,'1');assert.equal(page.content.records[0].view.contiguous_sequence,baselineSequence);assert.equal(page.manifest_digest,manifest.manifest_digest);
    const staged=await seamer('prepareSnapshot',{binding_id:active.binding_id,connection_attempt_id:connected.connection_attempt_id});assert.deepEqual(staged.projection.manifest,manifest);assert.deepEqual(staged.projection.records,page.content.records);assert.equal(staged.projection.records[0].view.state,'baseline_required');assert.equal(staged.context.publisher_service_boot_id,connected.ready.channel.service_boot_id);frames.push({event:'native-service-snapshot-staged',snapshot_ref:staged.subscription.snapshot_ref,records:staged.projection.records.length,durable_ingest:false});
   }
   const adoption={binding_id:active.binding_id,connection_attempt_id:connected.connection_attempt_id};
   const adopted=await seamer('adoptSnapshot',{...adoption,confirm:armedDispatch,transportReady:armedDispatch});
   assert.equal(adopted.receipt.state,'committed');assert.deepEqual(adopted.receipt.snapshot_ref,currentSubscription.view.snapshot_ref);assert.equal(adopted.sources.length,1);assert.equal(adopted.sources[0].reject_floor,baselineSequence);assert.equal(adopted.sources[0].state,'baseline_required');
   if(!armedDispatch)assert.equal((await seamer('getSubscription',subQuery)).view.ack_payload_digest,'0'.repeat(64));
   frames.push({event:'native-service-snapshot-adopted',...adopted,acknowledged:armedDispatch});
   let replacement=connected;
   if(!armedDispatch){
   const beforeReplace=(await seamer('getSubscription',subQuery)).view;await assert.rejects(seamer('attachSubscription',{binding_id:active.binding_id,view:beforeReplace}));
   await assert.rejects(seamer('attachSubscription',{binding_id:active.binding_id,view:currentSubscription.view,options:{connection_attempt_id:connected.connection_attempt_id}}));
   replacement=await seamer('attachSubscription',{binding_id:active.binding_id,view:beforeReplace,options:{replace_existing:true}});assert.equal(replacement.attached.binding.connection_generation,'2');
   await assert.rejects(seamer('subscriptionSnapshot',{...subQuery,generation:'1'}),{code:'conflict'});
   assert.equal((await seamer('subscriptionAttempt',{...subQuery,attempt_id:connected.connection_attempt_id})).view.current,false);
   const again=(await seamer('subscriptionSnapshot',{...subQuery,generation:'2'})).view;assert.deepEqual(again,manifest);const page2=(await seamer('subscriptionPage',{...subQuery,generation:'2',snapshot_id:manifest.binding.snapshot_id,page_index:0})).view;assert.deepEqual(page2.content,page.content);assert.equal(page2.delivery_binding.connection_generation,'2');
   }
   const confirmedGeneration=replacement.attached.binding.connection_generation;
   const confirmed=armedDispatch?adopted:await seamer('adoptSnapshot',{...adoption,connection_attempt_id:replacement.connection_attempt_id,confirm:true,transportReady:true});assert.deepEqual(confirmed.receipt,adopted.receipt);assert.equal(confirmed.confirmation.state,'confirmed');assert.equal(confirmed.confirmation.transport_ready,true);assert.equal(confirmed.confirmation.view.ack_cursor,'1');assert.equal(confirmed.confirmation.view.state,'live');assert.equal(confirmed.confirmation.view.binding.connection_generation,confirmedGeneration);assert.equal(confirmed.sources[0].reject_floor,baselineSequence);assert.equal(confirmed.sources[0].state,'baseline_required');assert.notEqual(confirmed.confirmation.view.ack_payload_digest,manifest.manifest_digest);frames.push({event:'native-service-snapshot-confirmed',...confirmed});
   let checkpoint=confirmed.confirmation.view;
   if(managedSubscriptions&&!armedDispatch){
    await seamer('detachSubscription',replacement.connection_attempt_id);
    const target='/auth/local-source-subscriptions/v1',{purpose:_,...request}=subscriptionRequest,body=Buffer.from(canonical({operation:'connect',request}));
    const post=headers=>requestHttps(second.apiOrigin,materials.ca,{method:'POST',target,body,
     headers:[['Content-Type','application/json'],['Content-Length',String(body.length)],...headers]});
    assert.equal((await post([])).status,'401');
    const auth=[['Authorization','Bearer '+issued.token]];
    assert.equal((await post([...auth,['X-Forwarded-For','127.0.0.1']])).status,'403');
    let joined,confirmation;
    const deadline=performance.now()+5000;
    do{
     const response=await post(auth);
     assert.equal(response.status,'200',response.body.toString());
     joined=JSON.parse(response.body).result;
     assert.equal(joined.subscription_id,subQuery.subscription_id);
     assert.equal(joined.apply_id,adopted.receipt.apply_id);
     if(joined.transport_ready)break;
     assert.ok(performance.now()<deadline,'managed source subscription did not become live');
     await delay(40);
    }while(true);
    const {view}=await seamer('getSubscription',subQuery);
    assert.equal(joined.state,'confirmed');
    assert.equal(view.binding.connection_generation,String(BigInt(confirmedGeneration)+1n));
    assert.equal(view.ack_cursor,'2');assert.equal(view.state,'live');
    confirmation={state:joined.state,apply_id:joined.apply_id,transport_ready:joined.transport_ready,view};
    frames.push({event:'native-service-snapshot-resumed',confirmation,receipt:adopted.receipt});
    frames.push({event:'native-managed-source-subscription',result:joined});checkpoint=view;
   }else if(!focused){
    const resumed=await seamer('resumeSnapshot',{binding_id:active.binding_id,apply_id:adopted.receipt.apply_id,previous_attempt_id:replacement.connection_attempt_id,transportReady:true,options:{replace_existing:true}});
    assert.equal(resumed.connection.attached.binding.connection_generation,String(BigInt(confirmedGeneration)+1n));
    assert.equal(resumed.connection.attached.state,'catching_up');assert.equal(resumed.confirmation.state,'confirmed');assert.equal(resumed.confirmation.transport_ready,true);assert.equal(resumed.confirmation.view.ack_cursor,'2');assert.equal(resumed.confirmation.view.state,'live');assert.deepEqual(resumed.receipt,adopted.receipt);
    if(!armedDispatch)assert.equal((await seamer('subscriptionAttempt',{...subQuery,attempt_id:replacement.connection_attempt_id})).view.current,false);
    frames.push({event:'native-service-snapshot-resumed',...resumed});checkpoint=resumed.confirmation.view;
   }
   const generation=checkpoint.binding.connection_generation,eventCursor=String(BigInt(checkpoint.ack_cursor)+1n),projectionCursor=String(BigInt(eventCursor)+1n);
   const transport=await seamer('snapshotSourceTransport',{apply_id:adopted.receipt.apply_id});
   assert.deepEqual(transport,{state:'ready',connection_generation:generation,ack_cursor:checkpoint.ack_cursor,ack_payload_digest:checkpoint.ack_payload_digest});
   if(config.incremental_source_event){
    const sourceKey=confirmed.sources[0].source_key;
    assert.equal((await seamer('localSourceState',sourceKey)).state,'baseline_required');
    const localReference=nativeReceiver?await (async()=>{
     const until=performance.now()+6000;
     for(;;){
      const sampled=await seamer('nativeReferenceStatus');
      if(sampled.current?.bundle_ref&&sampled.status.every(source=>source.state==='waiting'))return sampled;
      assert.ok(!sampled.status?.some(source=>source.state==='blocked')&&performance.now()<until,JSON.stringify(sampled));await delay(30);
     }
    })():null;
    const published=await controller('publishBufferedEvent'),publishedEvent=published.events[1];
    assert.equal(published.events.length,2);assert.equal(publishedEvent.origin.source_sequence,'2');
    const sourceTimeRef=publishedEvent.origin.source_time_ref;
    assert.equal(sourceTimeRef!==null,Boolean(config.source_reference_capture));
    const liveScope=await controller('currentEventScope',allowed_scope_ref);
    assert.equal(liveScope.current_frontiers[0].registration_sequence,'2');
    const lookup={source_key:publishedEvent.origin.source_key,event_id:publishedEvent.origin.event_id};
    const until=performance.now()+5000;
    let current;
    do{current=(await seamer('getSubscription',subQuery)).view;if(current.ack_cursor===projectionCursor)break;
     assert.equal(current.state,'live');await delay(40);}while(performance.now()<until);
    assert.equal(current.ack_cursor,projectionCursor);assert.equal(current.state,'live');
    const stored=await seamer('localSourceEvent',lookup);
    assert.deepEqual(stored.origin,publishedEvent.origin);assert.equal(stored.origin_digest,publishedEvent.origin_digest);
    assert.deepEqual(stored.first_delivery,{apply_id:adopted.receipt.apply_id,cursor:eventCursor,connection_generation:generation});
    assert.equal(stored.receive_cursor.receive_sequence,'1');
    if(armTarget||nativeReceiver){
     const {match,digest:hash}=stored.trigger_match_set;
     assert.deepEqual(match.receive_cursor,stored.receive_cursor);assert.equal(match.origin_digest,stored.origin_digest);
     assert.equal(hash,digest(match));assert.equal(match.registry_revision,'0');assert.deepEqual(match.targets,[]);
     const decision=stored.trigger_decision_set;
     assert.equal(decision.set.match_digest,hash);assert.equal(decision.set.trigger_disposition,'observed_only');
     assert.deepEqual(decision.decisions,[]);
     if(nativeReceiver){
      assert.equal(decision.set.live_evidence.state,'unavailable');
      assert.ok(['source_predecessor_changed','source_predecessor_unavailable'].includes(decision.set.live_evidence.reason));
     }
     priorMatchSet={lookup,match:stored.trigger_match_set,decision};
    }
    assert.equal(stored.first_observation.receiver_boot_id,adopted.receipt.service_boot_id);
    assert.equal(stored.first_observation.delivery_mode,'live');
    const expectedBasis=sourceTimeRef?'trusted_utc':'unproven';
    assert.equal(stored.first_observation.age_basis,expectedBasis);
    const {age_evidence:age,disposition}=stored.first_observation;
    assert.deepEqual(disposition,{state:'observed_only',age_evidence_ref:{kind:'age_evidence',
     scope:adopted.receipt.scope,object_id:age.id,revision:age.revision,digest:age.digest},reason_codes:[sourceTimeRef?'initial_reference_age_observed':'source_time_missing']});
    assert.equal(age.basis,expectedBasis);
    if(sourceTimeRef){
     assert.equal(age.receiver_domain.authority_service_id,second.identity.service_id);
     if(nativeReceiver){
      const receiver=stored.first_observation.receiver_reference;
      assert.ok(localReference.packet_count>0);
      assert.equal(stored.first_observation.receiver_boot_id,localReference.boot);
      assert.equal(age.receiver_domain.database_instance_id,second.identity.database_instance_id);
      assert.equal(age.receiver_domain.runtime_binding_ref.digest,localReference.binding.runtime_digest);
      assert.deepEqual(age.receiver_domain,receiver.bundle.anchor.binding.domain);
      assert.ok(BigInt(receiver.bundle.anchor.anchor_tick_ns)<BigInt(age.first_receive_tick_ns));
      referenceWitness={anchor_id:receiver.bundle.anchor.anchor_id,event_id:publishedEvent.origin.event_id,stored,age};
     }else assert.equal(age.receiver_domain.provider_id,'fixture.receiver.clock');
     assert.equal(age.receiver_reference_context_ref.digest,stored.first_observation.receiver_reference.bundle.bundle_digest);
     assert.notEqual(age.receiver_reference_context_ref.digest,age.source_reference_context_ref.digest);
     assert.ok(age.lower_age_ms>0&&age.upper_age_ms>=age.lower_age_ms);
    }else{assert.equal(age.lower_age_ms,null);assert.equal(age.upper_age_ms,null);}
    assert.equal(age.origin_digest,stored.origin_digest);assert.equal(age.first_receive_tick_ns,stored.first_observation.received_tick_ns);
    if(!focused){
    const readAge=(binding,selectedDigest,selection=contractSelection,revision=age.revision)=>new Promise((resolve,reject)=>{
     const url=`${second.origin}/internal/events/age-evidence/${age.id}/${revision}?digest=${encodeURIComponent(selectedDigest)}`;
     const req=https.get(url,{...materials.leaves[0],ca:materials.ca,agent:false,minVersion:'TLSv1.3',headers:{
      'X-LeafSeamer-Pairing':binding,'X-LeafSeamer-Contract':selection.contract_id,'X-LeafSeamer-Contract-Digest':selection.contract_digest}},res=>{
      const chunks=[];res.on('data',part=>chunks.push(part));res.on('error',reject);
      res.on('end',()=>resolve({status:res.statusCode,headers:res.headers,text:Buffer.concat(chunks).toString('utf8')}));
     });req.setTimeout(3000,()=>req.destroy(Error('read_age_timeout')));req.on('error',reject);
    });
    const exact=await readAge(active.binding_id,age.digest);
    assert.equal(exact.status,200);assert.equal(exact.headers['cache-control'],'no-store');assert.deepEqual(JSON.parse(exact.text),age);
    assert.equal((await readAge(active.binding_id,digest('wrong age digest'))).status,403);
    assert.equal((await readAge(denied.binding_id,age.digest)).status,403);
    assert.equal((await readAge(active.binding_id,age.digest,{...contractSelection,contract_id:'leafseamer.api.v1.product8'})).status,403);
    if(nativeReceiver&&!armedDispatch){
     const request={...lookup,origin_digest:stored.origin_digest,first_age_ref:disposition.age_evidence_ref,
      policy:{max_age_ms:age.upper_age_ms+5000,capture_semantics:stored.first_observation.source_time_evidence.capture_semantics}};
     const advanced=await seamer('evaluateNativeAge',request);
     assert.equal(advanced.state,'eligible',JSON.stringify(advanced));
     const ref=advanced.age_evidence_ref;
     assert.equal(ref.object_id,age.id);assert.equal(ref.revision,'2');
     const revision=await readAge(active.binding_id,ref.digest,contractSelection,ref.revision);
     assert.equal(revision.status,200,revision.text);
     const updated=JSON.parse(revision.text);
     assert.equal(updated.first_receive_tick_ns,age.first_receive_tick_ns);
     assert.ok(BigInt(updated.evaluated_tick_ns)>BigInt(age.evaluated_tick_ns));
     assert.ok(updated.upper_age_ms>=age.upper_age_ms);assert.deepEqual(updated.first_receive_utc,age.first_receive_utc);
     assert.equal((await readAge(denied.binding_id,ref.digest,contractSelection,ref.revision)).status,403);
     const stale=await seamer('evaluateNativeAge',{...request,policy:{...request.policy,max_age_ms:0}});
     assert.equal(stale.reason,'age_limit_exceeded');assert.equal(stale.age_evidence_ref.revision,'3');
     referenceWitness.advanced=updated;
     referenceWitness.stale=stale.age_evidence_ref;
    }
    if(sourceTimeRef){
     const reply=await readSourceTime(sourceTimeRef);assert.equal(reply.status,'200',reply.body.toString('utf8'));
     assert.equal(Object.fromEntries(reply.headers.map(([key,value])=>[key.toLowerCase(),value]))['cache-control'],'no-store');
     const evidence=JSON.parse(reply.body.toString('utf8'));
     await verifyEventOrigin(stored.origin,evidence);
     assert.deepEqual(stored.first_observation.source_time_evidence,evidence);
     assert.deepEqual(evidence.reference_context_ref,bundleRef);
     assert.ok(BigInt(evidence.utc_interval.lower_ns)<=BigInt(evidence.utc_interval.upper_ns));
     if(nativeReceiver){
      const nonnegative=value=>String(value<0n?0n:value);
      const remote=evidence.utc_interval,local=age.first_receive_utc;
      assert.deepEqual(age.initial_age_ns,{lower_ns:nonnegative(BigInt(local.lower_ns)-BigInt(remote.upper_ns)),
       upper_ns:nonnegative(BigInt(local.upper_ns)-BigInt(remote.lower_ns))});
     }
     const originalBundle=await readBundle(bundleRef);assert.equal(originalBundle.status,'200');
     const envelope=JSON.parse(originalBundle.body.toString('utf8'));
     assert.equal(envelope.service_id,first.identity.service_id);
     assert.deepEqual(stored.first_observation.source_reference_bundle,envelope.data);
     assert.equal(envelope.data.bundle_digest,bundleRef.digest);
     assert.equal((await readBundle({...bundleRef,digest:digest('different reference bundle')})).status,'403');
     assert.equal((await readBundle(bundleRef,deniedLeft.binding_id)).status,'403');
     assert.equal((await readSourceTime({...sourceTimeRef,digest:digest('wrong source time digest')})).status,'403');
     assert.equal((await readSourceTime(sourceTimeRef,{...contractSelection,contract_id:'leafseamer.api.v1.product8'})).status,'403');
     assert.equal((await readSourceTime(sourceTimeRef,contractSelection,deniedLeft.binding_id)).status,'403');
    }
    }
    assert.deepEqual(await seamer('localSourceEvent',lookup),stored);
    const source=await seamer('localSourceState',sourceKey);
    assert.equal(source.state,'synchronized');assert.equal(source.reject_floor,'1');
    assert.deepEqual(source.continuity,{connection_generation:generation,delivery_cursor:projectionCursor,source_sequence:'2',origin_digest:publishedEvent.origin_digest,
     projection_digest:'sha256:'+current.ack_payload_digest,checkpoint_digest:'sha256:'+checkpoint.ack_payload_digest});
    frames.push({event:'native-service-source-event-ingested',cursor:current.ack_cursor,hash:current.ack_payload_digest});
   }
   currentSubscription=await seamer('getSubscription',subQuery);frames.push({event:'native-service-event-channel',manifest,page,connected,replacement,transport});
  }
  if(!focused){
   await assert.rejects(createSubscription({...subscriptionRequest,request_id:id()}),{code:'conflict'});
   assert.deepEqual((await seamer('getSubscription',subQuery)).view,currentSubscription.view);
  }
  closeSubscription=async()=>{
  await assert.rejects(seamer('closeSubscription',{...subQuery,expected_revision:config.verifySnapshots?'1':'2'}),{code:'conflict'});
  const closed=await seamer('closeSubscription',{...subQuery,expected_revision:currentSubscription.view.revision});assert.equal(closed.view.state,'closed');assert.deepEqual(await createSubscription(subscriptionRequest),closed);
  const secondSubscription=await createSubscription({...subscriptionRequest,request_id:id()});assert.equal(secondSubscription.status,202);
  await controller('revokeConsumerPolicy',{request_id:id(),policy_ref:{id:policy.id,revision:policy.revision,digest:policy.digest}});
  await assert.rejects(seamer('getSubscription',{binding_id:active.binding_id,subscription_id:secondSubscription.view.binding.subscription_id}),{code:'forbidden'});
  if(config.source_reference_capture){
   assert.equal((await readSourceTime(original.events[0].origin.source_time_ref)).status,'403');
   assert.equal((await readBundle(bundleRef)).status,'403');
  }
  frames.push({event:'native-service-subscription',created:subscription.view,closed:closed.view,local_policy_revoked:true});
  };
  if(!armTarget)await closeSubscription();
  }
  finishConsumer=async()=>{
  await assert.rejects(controller('currentConsumer',{...consumerQuery,allowed_scope_ref:{...allowed_scope_ref,revision:'2'}}),{code:'service_identity_consumer_scope'});
  const duplicate=await controller('createConsumer');await assert.rejects(controller('approveConsumer',{...approvalInput,request_id:id(),principal_id:duplicate.id}),{code:'service_identity_consumer_slot_conflict'});
  await controller('revokeConsumer',{request_id:id(),principal_id:principal.id,expected_revision:approved.principal.revision});
  await assert.rejects(controller('currentConsumer',consumerQuery),{code:'service_identity_consumer_unavailable'});
  assert.deepEqual(await controller('approveConsumer',approvalInput),approved);
  await assert.rejects(controller('currentConsumer',consumerQuery),{code:'service_identity_consumer_unavailable'});
  const replacement=await controller('approveConsumer',{...approvalInput,request_id:id(),principal_id:duplicate.id});assert.equal(replacement.principal.state,'active');
  await controller('revokeEventScope',allowed_scope_ref);
  await assert.rejects(controller('currentConsumer',consumerQuery),{code:'event_scope_inactive'});
  assert.deepEqual(await controller('registerEventScope',scopeRequest),registered);
  await assert.rejects(controller('currentEventScope',allowed_scope_ref),{code:'event_scope_inactive'});
  stopScope=await controller('registerEventScope',{request_id:id(),device_ids:[first.device_id]});
  frames.push({event:'native-consumer-approval',approved:true,duplicate_denied:true,revoked_denied:true,scope_original:registered,subscription_created:config.verifySubscriptions!==false});
  };
  if(!armTarget)await finishConsumer();
  }
  if(config.incremental_source_event){
   const left=active.transcript.parties.find(p=>p.service_id===first.identity.service_id);
   issued??=await controller('issueIdentity',left.binding_id);
   const refresh=operator;
   const bearer=['authorization','Bearer '+issued.token];
   const eventRequest=async(operation,request,headers=[bearer])=>{
    const body=Buffer.from(canonical({operation,request}));
    return requestHttps(second.apiOrigin,materials.ca,{target:'/auth/local-event-bindings/v1',method:'POST',body,
     headers:[...headers,['content-type','application/json'],['content-length',String(body.length)]]});
   };
   const eventResult=async(operation,request)=>{
    const result=await eventRequest(operation,request);
    assert.equal(result.status,'200',result.body.toString());
    return JSON.parse(result.body).result;
   };
   let identity=await refresh();
   const grant=await seamer('localGrant',{handle_id:identity,principal_id:issued.principal_id});
   await seamer('replaceLocalGrant',{handle_id:identity,request:{request_id:id(),principal_id:issued.principal_id,principal_kind:'human',expected_revision:grant.revision,
    permissions:[...new Set([...grant.permissions,'request.create','request.read','definition.read','definition.edit','definition.publish','run.approve','run.arm','run.manage','run.read',
     ...(scheduledAdmission?['schedule.read','schedule.publish','schedule.activate']:[])])],
    definition_scope:grant.definition_scope,not_before_ns:grant.not_before_ns,expires_ns:grant.expires_ns}});
   identity=await refresh();
   const context=await seamer('createDraftContext',identity),draft=await seamer('createDraft',{handle_id:identity,context_id:context.context_id,key:id()});
   const request={definition_id:draft.definition_id,query:{...query,expected_source_ref:page.value.source_ref,expected_registration_ref:null}};
   if(!focused){
    assert.equal((await eventRequest('candidate',request,[])).status,'401');
    assert.equal((await eventRequest('candidate',request,[bearer,['origin',second.apiOrigin]])).status,'403');
   }
   const candidate=await eventResult('candidate',request),input=candidate.binding.value;
   const projected=frames.find(f=>f.event==='native-source-projection-cut').cut.records[0].view.binding_ref;
   assert.deepEqual(candidate.source_ref,page.value.source_ref);assert.deepEqual(candidate.source_key,original.registration.source_key);
   assert.equal(input.source,'event');assert.deepEqual(input.registration_ref,{id:projected.entity_id,revision:projected.revision,digest:'sha256:'+projected.content_digest});
   assert.equal(candidate.binding.content_ref.digest,digest(input));assert.equal(candidate.schema.schema_ref.digest,digest(candidate.schema.document));
   assert.deepEqual(input.projections.map(p=>p.pointer),['/value','/write_sequence']);assert.ok(input.projections.every(p=>p.reads.length>0));
   if(!focused)assert.equal((await eventRequest('candidate',{...request,definition_id:id()})).status,'404');
   const writeContext=await eventResult('create-context',{ttl_ms:30000,max_unique_requests:2});
   const approval={request_id:id(),approval_id:id(),definition_id:draft.definition_id,expected_candidate_digest:candidate.candidate_digest};
   const approvalRequest={query:request.query,approval,context_id:writeContext.context_id};
   if(!focused){
    assert.equal((await eventRequest('approve',{...approvalRequest,approval:{...approval,expected_candidate_digest:digest('other candidate')}})).status,'409');
    assert.equal((await eventRequest('approve',{...approvalRequest,context_id:id()})).status,'403');
   }
   const accepted=await eventResult('approve',approvalRequest);
   assert.equal(accepted.state,'approved');assert.deepEqual(accepted.candidate,candidate);assert.equal(accepted.previous_ref,null);
   assert.equal(Object.hasOwn(accepted,'authorization'),false);
   if(!focused)assert.deepEqual(await eventResult('approve',approvalRequest),accepted);
   const definition_id=draft.definition_id,deviceQuery={...query,device_id:actionDevice,expected_registration_ref:armTarget?null:candidate.device_registration_ref};
   const deviceCandidate=await seamer('bindingCandidate',{identity:await refresh(),definition_id,query:deviceQuery});
   const deviceContext=await seamer('createEventBindingContext',{identity:await refresh(),max_unique_requests:1});
   const deviceApproval=await seamer('approveCurrentBindings',{identity:await refresh(),query:deviceQuery,
    request:{request_id:id(),approval_id:id(),definition_id,expected_bundle_digest:deviceCandidate.bundle_digest},
    context:{context_id:deviceContext.context_id,operation_id:'approveDefinitionBindings'}});
   const action=deviceApproval.bundle.bindings.find(binding=>binding.kind==='action');
   let csrf=null,lastHeaders=[];
   const client=createClient('seamer',contractSelection,async packet=>{
    const response=await requestHttps(second.apiOrigin,materials.ca,{...packet,headers:[...packet.headers,bearer,
     ['origin',second.apiOrigin],...(csrf?[['x-csrf-token',csrf]]:[])]});
    lastHeaders=response.headers;
    return response;
   });
   const publicationContext=await client.call('createRequestContext',{body:{audience:second.identity.service_id,selection:contractSelection,
    operations:['updateDraft','publishDefinition'],ttl_ms:30000,max_unique_requests:3}});
   assert.equal(publicationContext.status,'201');
   csrf=new Headers(lastHeaders).get('x-csrf-token');assert.match(csrf,/^[A-Za-z0-9_-]{43}$/);
   const publishRequest=await seamer('prepareEventPublication',{identity:await refresh(),definition_id,event:candidate.binding,action,
    context_id:publicationContext.body.data.context_id});
   const publishedHttp=await client.call('publishDefinition',{path:{definition_id},body:publishRequest});
   assert.equal(publishedHttp.status,'201',JSON.stringify(publishedHttp.body));const published=publishedHttp.body;
   const readProgram=identity=>seamer('readPublishedEventProgram',{identity,definition_id,version:published.definition_version});
   const program=await readProgram(await refresh());
   assert.deepEqual(program.input_bindings,[input]);assert.deepEqual(program.action_bindings,[action.value]);
   assert.equal(program.program.trigger_binding_id,input.binding_id);assert.equal(digest(program),published.program_digest);
   if(browser){
    const seamerSpki=crypto.createHash('sha256').update(new crypto.X509Certificate(materials.leaves[1].cert).publicKey.export({type:'spki',format:'der'})).digest('base64');
    const browserConfig=path.join(path.dirname(config.request),'publication-browser.json');
    fs.writeFileSync(browserConfig,canonical({...browser,origin:gui.origin,seamerOrigin:second.apiOrigin,spkis:[gui.spki,seamerSpki],published,program}));
    const driver=spawn(process.execPath,[path.join(browser.root,'test/seamer-first-pairing-publication-browser.mjs'),browserConfig],
     {stdio:['ignore','pipe','pipe'],windowsHide:true,timeout:25000});
    let stdout='',stderr='';driver.stdout.setEncoding('utf8').on('data',chunk=>stdout+=chunk);driver.stderr.setEncoding('utf8').on('data',chunk=>stderr+=chunk);
    const [code]=await once(driver,'close');assert.equal(code,0,stderr+'\n'+stdout);
    const outcome=JSON.parse(stdout);assert.equal(outcome.bindings_digest,published.bindings_digest);assert.ok(BigInt(outcome.version)>BigInt(published.definition_version));
    frames.push({event:'native-first-pairing-browser-publication',outcome});
   }
   const run=await seamer('createAnotherPublishedRun',{identity:await refresh(),run:published});
   const preflight=async()=>seamer('inspectArmPrerequisites',{identity:await refresh(),run_id:run.run_id,revision:run.revision});
   if(!focused)await assert.rejects(preflight(),{code:'run_preflight_approval'});
   const controllerScope={authority_service_id:first.identity.service_id,database_instance_id:first.identity.database_instance_id};
   const pair=await controller('get',left.binding_id),pairing_ref={id:pair.binding_id,revision:pair.revision,digest:pair.digest};
   const principal=await controller('createAutomationPrincipal'),service_principal={issuer_id:principal.issuer_id,issuer_epoch:principal.issuer_epoch,principal_id:principal.id};
   const interval={not_before_ns:pair.not_before_ns,expires_ns:pair.expires_ns};
   const service=await controller('approveAutomationPrincipal',{request_id:id(),principal_id:principal.id,expected_revision:principal.revision,
    audiences:[first.identity.service_id],permissions:['automation.prepare'],device_ids:[actionDevice],...interval,
    automation_origin:{seamer_scope:run.scope,pairing_ref,device_scope:scope}});
   const targets=await seamer('runDeviceTargets',{identity:await refresh(),run_id:run.run_id});
   const [target]=targets.targets;assert.deepEqual(target.controller_scope,controllerScope);
   await controller('replaceEndorsementPolicy',{request_id:id(),expected_policy_ref:null,
    human:{issuer_id:issuer.issuer_id,issuer_epoch:issuer.issuer_epoch,principal_id:issued.principal_id},service_principal,service_approval_ref:service.principal.approval_ref,
    device_actions:target.device_actions,parameter_constraints:[],...interval});
   const proposalIdentity=await refresh(),proposalContext=await seamer('createRunApprovalContext',proposalIdentity);
   const proposalRequest={context:{context_id:proposalContext.context_id,idempotency_key:id()},artifact_ref:run.artifact_ref,expected_run_revision:run.revision,
    service_principal,source:{binding_id:active.binding_id,approval_ref:service.principal.approval_ref},
    validity:{not_before:new Date(Number((BigInt(interval.not_before_ns)+999999n)/1000000n)).toISOString(),
     not_after:new Date(Number(BigInt(interval.expires_ns)/1000000n)-1000).toISOString()},controller_constraints:[]};
   const proposal=await seamer('createRunApprovalProposal',{identity:proposalIdentity,run_id:run.run_id,request:proposalRequest});
   const endorsement=await controller('endorseRun',{binding_id:left.binding_id,proposal_ref:proposal.proposal_ref});
   const approver=await refresh(),approveContext=await seamer('createApproveRunContext',{handle_id:approver,max_unique_requests:1});
   const orgApproval=await seamer('approveRun',{identity:approver,run_id:run.run_id,request:{context:{context_id:approveContext.context_id,idempotency_key:id()},
    controller_endorsement_refs:[endorsement.endorsement_ref],expected_prior_approval_ref:null,expected_run_revision:run.revision,proposal_ref:proposal.proposal_ref}});
   const remoteApproval={binding_id:left.binding_id,approval_ref:orgApproval.approval_ref};
   const mode=armTarget?'automatic':'manual';
   let armPreflight=null;
   if(!focused){
    const eligible=await controller('checkRunApproval',remoteApproval);assert.deepEqual(eligible.endorsement_ref,endorsement.endorsement_ref);
    const controlRequest={binding_id:active.binding_id,endorsement_ref:endorsement.endorsement_ref};
    await assert.rejects(seamer('readPeerControlObservations',{...controlRequest,binding_id:denied.binding_id}),{code:'peer_pairing_purpose'});
    const controlRead=await seamer('readPeerControlObservations',controlRequest);
    assert.deepEqual(controlRead.endorsement_read.object_ref,endorsement.endorsement_ref);
    assert.equal(controlRead.devices.length,1);assert.equal(controlRead.devices[0].device_id,actionDevice);
    assert.equal(controlRead.devices[0].control.mode,mode);
    armPreflight=await seamer('inspectArmPrerequisites',{identity:await refresh(),run_id:run.run_id,revision:'2'});
    assert.deepEqual(armPreflight.control_observations,[{controller_scope:controllerScope,endorsement_ref:endorsement.endorsement_ref,
     devices:[{...controlRead.devices[0],state:mode}]}]);
    assert.ok(armPreflight.sources.some(source=>source.transport?.state===(armTarget?'ready':'unavailable')));
    assert.ok(armPreflight.sources.every(source=>source.state==='synchronized'));
    assert.equal(armPreflight.can_arm,false);
   }
   const recordPreflight=async()=>seamer('recordArmPreflight',{identity:await refresh(),run_id:run.run_id,revision:'2'});
   const readPreflight=async(command,reference)=>seamer(command,{identity:await refresh(),reference});
   const armContext=armedDispatch?null:await seamer('createArmRunContext',await refresh());
   const requestAndArm=armedDispatch?await seamer('recordAndArmRun',{identity_request:{binding_id:second.bindingId,token:issued.token},
    run_id:run.run_id,revision:'2'}):null;
   const recorded=requestAndArm?.recorded??await recordPreflight();
   if(armPreflight)assert.deepEqual(recorded.preflight.observation,{...armPreflight,can_arm:Boolean(armTarget)});
   else{
    const observed=recorded.preflight.observation;
    assert.equal(observed.can_arm,true);assert.deepEqual(observed.approval_ref,orgApproval.approval_ref);
    assert.equal(observed.control_observations[0].devices[0].control.mode,mode);
    assert.equal(observed.control_observations[0].devices[0].device_id,actionDevice);
    assert.ok(observed.sources.every(source=>source.state==='synchronized'&&source.transport?.state==='ready'));
   }
   assert.equal(recorded.preflight_ref.digest,digest(recorded.preflight));
    if(requestAndArm?.expiredPreflight){
     const original=requestAndArm.expiredPreflight;
     assert.notDeepEqual(original.preflight_ref,recorded.preflight_ref);
     assert.deepEqual(await readPreflight('getArmPreflight',original.preflight_ref),original);
    }
   const requestArm=identity=>seamer('armRun',{identity,run_id:run.run_id,request:{
    context:{context_id:armContext.context_id,idempotency_key:id()},preflight_ref:recorded.preflight_ref}});
   let revocable;
   if(armTarget){
    const issuedArm=requestAndArm?.issued??await requestArm(await refresh());
    assert.equal(issuedArm.operation.target_arm_generation,'1');
    let target;
    for(let attempt=0;attempt<50;attempt++){
     [target]=await seamer('armPreparationObservations',issuedArm.operation_ref);
     if(target.state==='receipt_observed')break;
     await delay(100);
    }
    assert.equal(target?.state,'receipt_observed');grantRef=target.target_grant_ref;
    const completed=await seamer('completeArmRun',{identity:await refresh(),reference:issuedArm.operation_ref});
    const {ingest_baseline:baseline}=completed.completion;
    assert.deepEqual(baseline.receive_cursor,recorded.preflight.basis.observed_receive_cursor);
    assert.equal(baseline.receive_cursor.receive_sequence,'1');
    assert.equal(baseline.revision,'2');
    assert.deepEqual(baseline.source_baseline_refs,[recorded.preflight.observation.sources[0].source.baseline_ref]);
    const armed=await seamer('getRun',{identity:await refresh(),run_id:run.run_id});
    assert.equal(armed.lifecycle,'armed');assert.equal(armed.dispatch_eligible,false);
    assert.equal(armed.last_receive_seq,baseline.receive_cursor.receive_sequence);
    const retained=await seamer('localSourceEvent',priorMatchSet.lookup);
    assert.deepEqual(retained.trigger_match_set,priorMatchSet.match);
    assert.deepEqual(retained.trigger_decision_set,priorMatchSet.decision);
    if(scheduledAdmission){
     await verifyScheduledAdmission({seamer,frames,origin:second.apiOrigin,ca:materials.ca,bearer,armed,grantRef,program,refresh,
      dispatch:config.scheduled_dispatch===true,withdrawReference:config.scheduled_reference_withdrawal===true});
     await controller('finish');return;
    }
    if(armedDispatch){
     let publishedSource;
     try{publishedSource=await controller('publishArmedEvent');}
     catch(error){
      const start=frames.findLastIndex(frame=>frame.event==='successor-stage'&&frame.stage==='trigger-source-start');
       const sourceFrames=frames.slice(start<0?frames.length:start);
       error.sourceDispatch=sourceFrames.filter(frame=>frame.event==='successor-stage')
        .map(({stage,remaining_ms,reported_ms,disposition})=>({stage,remaining_ms,reported_ms,disposition}));
       error.sourceDispatchFaults=sourceFrames.filter(frame=>['worker_gate_failure','unavailable','core-source-lost'].includes(frame.event))
        .map(({event,code,reason,reported_ms,permitted,alive})=>({event,code,reason,reported_ms,permitted,alive}));
      throw error;
     }
     const {sourceEvent}=publishedSource;
     assert.equal(sourceEvent.origin.source_sequence,'3');
     const lookup={source_key:sourceEvent.origin.source_key,event_id:sourceEvent.origin.event_id},until=performance.now()+12000;
     let settled;
     while(performance.now()<until){
      const failure=frames.find(f=>f.event==='automation-target-failed');
      if(failure){
       const trace=frames.filter(frame=>['automation-target-ready','automation-target-accepted','automation-target-stage'].includes(frame.event));
       assert.fail(JSON.stringify({code:failure.code,cause:failure.cause,close_reason:failure.close_reason,stages:trace.map(({event,stage,remaining_ms,disposition,reported_ms})=>
        ({event,stage,remaining_ms,disposition,reported_ms}))}));
      }
      settled=frames.find(f=>f.event==='automation-target-settled');if(settled)break;
      await delay(30);
     }
     const stored=await seamer('localSourceEvent',lookup);
     assert.ok(stored,'触发事件未进入 Seamer 持久事件账本');
     assert.deepEqual(stored.origin,sourceEvent.origin);
     assert.equal(stored.first_observation.age_basis,'trusted_utc',JSON.stringify(stored.trigger_decision_set));
     assert.equal(stored.trigger_decision_set.set.live_evidence.state,'verified',JSON.stringify(stored.trigger_decision_set));
     const decision=stored.trigger_decision_set.decisions.find(item=>item.run_id===armed.run_id);
     assert.equal(decision?.disposition,'accepted');
      if(!settled){
       const execution=await seamer('nativeTriggerExecution',{identity:await refresh(),execution_id:decision.execution_id});
       assert.fail(JSON.stringify({protection:execution.protection,reconciliation_required:execution.reconciliation_required,
        steps:execution.steps.map(({plan,ready,prepared_parameters,submission})=>({step_id:plan.step_id,ready:Boolean(ready),
         parameters_prepared:Boolean(prepared_parameters),submission_state:submission?.state})),
        controller:frames.filter(f=>['automation-target-ready','automation-target-accepted'].includes(f.event))
         .map(({event,remaining_ms,reported_ms})=>({event,remaining_ms,reported_ms}))}));
      }
     const target=frames.find(f=>f.event==='automation-target-ready');
     assert.equal(target?.control.device_id,armTarget);assert.equal(target.control.gate_state,'ready');
     const adoptionUntil=performance.now()+16000;
     let submission,execution,readIdentity,refreshAt=0;
     do{
      if(performance.now()>=refreshAt){readIdentity=await refresh();refreshAt=performance.now()+500;}
      execution=await seamer('nativeTriggerExecution',{identity:readIdentity,execution_id:decision.execution_id});
      submission=execution.steps.find(({submission})=>submission?.adjudication?.command_ref?.object_id===settled.reference.object_id)?.submission;
      if(execution.completion||execution.reconciliation_required)break;
      await delay(200);
     }while(performance.now()<adoptionUntil);
     assert.deepEqual(submission?.adjudication?.command_ref,settled.reference);
     assert.equal(submission?.terminal?.state.outcome,'succeeded');
     assert.ok(execution.completion,JSON.stringify({protection:execution.protection,reconciliation_required:execution.reconciliation_required}));
     assert.equal(execution.completion.body.execution_ref.object_id,decision.execution_id);
     assert.equal(execution.completion.notification.event_id,decision.execution_id+'.completed');
     const finished=await seamer('getRun',{identity:await refresh(),run_id:armed.run_id});
     assert.equal(finished.lifecycle,'armed');assert.equal(finished.active_execution_id,null);
     assert.equal(finished.last_execution_id,decision.execution_id);
     frames.push({event:'native-automation-target-dispatch',reference:settled.reference,grant_ref:grantRef,
      terminal:submission.terminal,execution_completion:execution.completion,receiver_reference:nativeReceiver?'native':'fixture'});
     await controller('finish');return;
    }
    if(config.verify_armed_target_ready){
     const ready=await controller('openArmTarget');
     assert.equal(ready.control.device_id,armTarget);assert.equal(ready.control.gate_state,'ready');
     await controller('finish');return;
    }
   }else{
    assert.deepEqual(await readPreflight('revalidateArmPreflight',recorded.preflight_ref),recorded);
    await assert.rejects(requestArm(await refresh()),{code:'run_arm_preflight_ineligible'});
   const slot=await seamer('getActivationSlot',{identity:await refresh(),definition_id});
   assert.equal(slot.active_run_id,null);assert.equal(slot.revision,'0');
   const missingOperation={kind:'run_arm_operation',scope:run.scope,object_id:id(),revision:'1',digest:digest('not an accepted arm')};
   await assert.rejects(controller('readRunInitiator',{binding_id:left.binding_id,operation_ref:missingOperation}),{code:'peer_pairing_response'});
   await assert.rejects(readPreflight('getArmPreflight',{...recorded.preflight_ref,digest:digest('wrong preflight digest')}),{code:'run_preflight_missing'});
   await delay(2100);
   await assert.rejects(readPreflight('revalidateArmPreflight',recorded.preflight_ref),{code:'run_preflight_expired'});
    revocable=await recordPreflight();
   }
   const revocation={request_id:id(),definition_id:draft.definition_id,approval_ref:accepted.approval_ref};
   const revokeRequest={approval:revocation,context_id:writeContext.context_id};
   const revoked=await eventResult('revoke',revokeRequest);
   assert.equal(revoked.state,'revoked');assert.equal(revoked.revision,'2');assert.equal(revoked.protection,'pending');
   if(revocable){
    await assert.rejects(readPreflight('revalidateArmPreflight',revocable.preflight_ref),error=>
     ['run_publication_binding_unapproved','run_preflight_revision'].includes(error.code));
    assert.deepEqual(await readPreflight('getArmPreflight',revocable.preflight_ref),revocable);
   }
   assert.deepEqual(revoked.previous_ref,accepted.approval_ref);assert.deepEqual(await eventResult('revoke',revokeRequest),revoked);
   assert.deepEqual(await eventResult('inspect',{approval_id:approval.approval_id}),revoked);
   assert.deepEqual(await eventResult('approve',approvalRequest),accepted);
   assert.equal((await eventRequest('approve',{...approvalRequest,approval:{...approval,approval_id:id(),request_id:id()}})).status,'429');
   const blocked=await client.call('publishDefinition',{path:{definition_id},
    body:{...publishRequest,context:{...publishRequest.context,idempotency_key:id()}}});
   assert.equal(blocked.status,'403');assert.equal(blocked.body.code,'PERMISSION_DENIED');
   const replay=await client.call('publishDefinition',{path:{definition_id},body:publishRequest});
   assert.equal(replay.status,'201');assert.deepEqual(replay.body,published);
   const history=await client.call('getCompiledProgram',{path:{definition_id,definition_version:published.definition_version}});
   assert.equal(history.status,'200');assert.deepEqual(history.body,program);
   await assert.rejects(controller('checkRunApproval',remoteApproval),{code:'run_endorsement_approval_ineligible'});
   let protection;
   for(let attempt=0;attempt<50;attempt++){
    const identity=await refresh(),current=await seamer('getCurrentRunApproval',{identity,run_id:run.run_id});
    if(current.revocation_operation_ref){
     const operationId=current.revocation_operation_ref.object_id;
     protection=await seamer('getRunApprovalOperation',{identity,operation_id:operationId});
     if(protection.state==='applied'){
      assert.equal((await seamer('getCurrentRunApproval',{identity,run_id:run.run_id})).state,'revoked');
      approvalProtection={operationId,runId:run.run_id,receiptRef:protection.targets[0].receipt_ref};break;
     }
    }
    await delay(100);
   }
   assert.equal(protection?.state,'applied');assert.deepEqual(protection.approval_ref,orgApproval.approval_ref);
   assert.deepEqual(protection.targets[0].receipt_ref.scope,controllerScope);
   const currentIdentity=await refresh(),nextContext=await seamer('createRunApprovalContext',currentIdentity);
   const currentRun=await seamer('getRun',{identity:currentIdentity,run_id:run.run_id});
   assert.equal(currentRun.lifecycle,armTarget?'paused':'disarmed');assert.equal(currentRun.dispatch_eligible,false);
   if(!armTarget)await assert.rejects(seamer('inspectArmPrerequisites',{identity:currentIdentity,run_id:run.run_id,revision:currentRun.revision}),
    {code:'run_publication_binding_unapproved'});
   await assert.rejects(seamer('createRunApprovalProposal',{identity:currentIdentity,run_id:run.run_id,request:{...proposalRequest,
    context:{context_id:nextContext.context_id,idempotency_key:id()},expected_run_revision:currentRun.revision}}),{code:'run_publication_binding_unapproved'});
   eventApproval={id:approval.approval_id,refs:[accepted.approval_ref,revoked.approval_ref],run_id:run.run_id,preflights:revocable?[recorded.preflight,revocable.preflight]:[recorded.preflight],
    publication:{definition_id,version:published.definition_version,
    approvals:[{binding_id:input.binding_id,kind:'event',approval_ref:accepted.approval_ref},{binding_id:action.binding_id,kind:'device',
     approval_ref:{id:deviceApproval.approval_id,revision:deviceApproval.revision,digest:digest(deviceApproval)}}]}};
   frames.push({event:'native-source-event-binding-candidate',binding_id:input.binding_id});
  }
  if(armTarget){await closeSubscription();await finishConsumer();}
  frames.push({event:'native-events-read',count:2,seamer_pid:second.pid,controller_pid:first.pid,source_ref:page.value.source_ref});await controller('finish');
  await wait(()=>frames.find(f=>f.event==='binding-peer-phase'&&f.phase==='events-stopped'));await assert.rejects(seamer('prepareEvents',query),{code:'peer_pairing_response'});
  if(stopScope)await assert.rejects(controller('currentEventScope',stopScope.scope_ref));frames.push({event:'native-events-stopped',read_denied:true});await controller('finish');
  await wait(()=>frames.find(f=>f.event==='binding-peer-phase'&&f.phase==='withdrawn'));await assert.rejects(seamer('prepareEvents',query),{code:'peer_pairing_response'});frames.push({event:'native-events-withdrawn',read_denied:true});await controller('finish');
  if(eventApproval){
   await seamer('close');assert.deepEqual(await seamer('readDurableEventBindingApproval',eventApproval.id),eventApproval.refs);
   if(referenceWitness){
    const {event_id,anchor_id,stored,age}=referenceWitness,archive=await seamer('nativeReferenceArchive',{event_id,anchor_id});
    assert.deepEqual(archive.observation,stored.first_observation);
    assert.equal(archive.anchor.reference_bundle.digest,age.receiver_reference_context_ref.digest);
    assert.equal(archive.runtime.launch_intent.member_id,age.receiver_domain.provider_id);
    assert.equal(archive.runtime.service_boot_id,stored.first_observation.receiver_boot_id);
    assert.equal(archive.ack.receipt.capture_ref.id,archive.anchor.capture_ref.id);
    assert.deepEqual(archive.ages[0],referenceWitness.advanced);
    assert.equal(archive.ages[1].digest,referenceWitness.stale.digest);
    frames.push({event:'native-service-reference-age-verified',origin_digest:stored.origin_digest,age_digest:age.digest});
   }
   assert.deepEqual(await seamer('readDurablePublicationApprovals',eventApproval.publication),eventApproval.publication.approvals);
   const durable=await seamer('readDurableArmPreflights',eventApproval.run_id);
   assert.deepEqual(durable,eventApproval.preflights.sort((a,b)=>a.preflight_id.localeCompare(b.preflight_id)));
   const receipt=await seamer('readDurableRunApprovalProtection',approvalProtection.operationId);
   assert.equal(receipt.run_id,approvalProtection.runId);assert.equal(receipt.deny_through_generation,armTarget?'1':null);
   assert.equal(receipt.receipt_id,approvalProtection.receiptRef.object_id);assert.equal(digest(receipt),approvalProtection.receiptRef.digest);
   assert.deepEqual(receipt.device_ids,[actionDevice]);
   frames.push({event:'native-run-binding-protection',receipt,grant_ref:grantRef});
  }
 }finally{if(!stopped){try{await seamer('shutdown');}catch{child.kill();}}await exited;}
}
