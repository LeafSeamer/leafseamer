// 实际受监督模拟器来源通过配对TLS交付独立Seamer；环境资格及首次配对仍为测试装配。
import assert from 'node:assert/strict';
import fs from 'node:fs';
import crypto from 'node:crypto';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {fileURLToPath} from 'node:url';
import {setTimeout as delay} from 'node:timers/promises';
import {digest} from '../dist/runtime/auth-clock-value.mjs';
const id=()=>crypto.randomUUID();
export async function verifyNativeEventRead({frames,core,config,materials}){
 const child=spawn(process.execPath,['--disallow-code-generation-from-strings',fileURLToPath(new URL('./pairing-process-fixture.mjs',import.meta.url))],{stdio:['ignore','ignore','pipe','ipc'],windowsHide:true});
 const pending=new Map(),exited=once(child,'exit');let stopped=false,stderr='';child.stderr.on('data',chunk=>{stderr=(stderr+chunk).slice(-4096);});
 child.on('message',message=>{const item=pending.get(message.id);if(!item)return;pending.delete(message.id);clearTimeout(item.timer);message.ok?item.resolve(message.value):item.reject(Object.assign(new Error(message.error.message),{code:message.error.code,remoteStack:message.error.stack,caller:item.caller}));});
 const failed=error=>{for(const item of pending.values()){clearTimeout(item.timer);item.reject(error);}pending.clear();};child.on('error',failed);child.on('exit',code=>{stopped=true;failed(new Error('event_peer_exit:'+code+':'+stderr));});
 const seamer=(command,value)=>new Promise((resolve,reject)=>{assert.equal(stopped,false);const key=id(),timer=setTimeout(()=>{pending.delete(key);reject(new Error('event_peer_timeout:'+command));},30000);pending.set(key,{resolve,reject,timer,caller:new Error(command).stack});child.send({id:key,command,value},error=>{if(error){clearTimeout(timer);pending.delete(key);reject(error);}});});
 async function wait(check){const end=performance.now()+60000;for(;;){const value=check();if(value)return value;assert.equal(core.exitCode,null);assert.equal(frames.find(f=>f.event==='unavailable'),undefined,JSON.stringify(frames.find(f=>f.event==='unavailable')));assert.ok(performance.now()<end,'native_event_timeout');await delay(20);}}
 async function controller(command,value){const key=id();assert.equal(fs.existsSync(config.request),false);assert.equal(fs.existsSync(config.response),false);fs.writeFileSync(config.request,JSON.stringify({id:key,command,value}));await wait(()=>fs.existsSync(config.response));const result=JSON.parse(fs.readFileSync(config.response,'utf8'));fs.unlinkSync(config.response);assert.equal(result.id,key);if(!result.ok)throw Object.assign(new Error(result.error.message),{code:result.error.code});return result.value;}
 try{
  const first=await wait(()=>frames.find(f=>f.event==='binding-peer-phase'&&f.phase==='events')),issuer={service_id:first.identity.service_id,issuer_id:first.identity.issuer_id,issuer_epoch:first.identity.issuer_epoch};
  const second=await seamer('initialize',{role:'seamer',material:materials.leaves[1],ca:materials.ca,issuer,principal_id:first.principal_id,snapshotIngest:config.verifyEventChannel===true});assert.notEqual(first.pid,second.pid);
  await controller('bind',[{identity:second.identity,cert:materials.leaves[1].cert,origin:second.origin,server_name:'127.0.0.1',trust_bundle_digest:second.configuration.trust_bundle_digest}]);
  await seamer('bind',[{identity:first.identity,cert:materials.leaves[0].cert,origin:first.origin,server_name:'127.0.0.1',trust_bundle_digest:first.configuration.trust_bundle_digest}]);
  const scope={format:'leafseamer.device-binding-scope/1',authority_service_id:first.identity.service_id,database_instance_id:first.identity.database_instance_id,device_ids:[first.device_id]};
  async function activate(permissions){const begin=BigInt(Date.now())*1000000n-1000000000n;
   const parties=[first,second].map(p=>({...p.configuration,binding_id:id(),permissions:p===second?permissions:[],object_scope_digest:p===second?digest(scope):digest({devices:[]}),not_before_ns:String(begin),expires_ns:String(begin+120000000000n)}));
   const transcript={format:'leafseamer.peer-pairing/1',pair_attempt_id:id(),parties},left=await controller('stage',{request_id:id(),transcript}),right=await seamer('stage',{request_id:id(),transcript});
   await controller('prove',left.binding_id);await seamer('prove',right.binding_id);
   for(const [call,record]of [[controller,left],[seamer,right]]){const ready=await call('get',record.binding_id);await call('activate',{request_id:id(),binding_id:record.binding_id,expected_revision:ready.revision,transcript_digest:record.transcript_digest});}return right;
  }
  const denied=await activate(['automation.bindings.read']);await assert.rejects(seamer('prepareEvents',{binding_id:denied.binding_id,scope,device_id:first.device_id}),{code:'peer_pairing_purpose'});
  const active=await activate(['event.internal.subscribe','events.internal.read']),query={binding_id:active.binding_id,scope,device_id:first.device_id},page=await seamer('prepareEvents',{...query,limit:1});
  const original=frames.find(f=>f.event==='event-source-published').published;assert.deepEqual(page.value.reply.bundle.registration,original.registration);assert.deepEqual(page.value.reply.bundle.events,[original.events[0]]);
  assert.deepEqual(await seamer('currentEvents',page.handle_id),page.value);const secondPage=await seamer('prepareEvents',{...query,after_sequence:'1',expected_source_ref:page.value.source_ref});assert.deepEqual(secondPage.value.reply.bundle.events,[original.events[1]]);
  assert.deepEqual((await seamer('prepareEvents',{...query,expected_source_ref:page.value.source_ref})).value.reply.bundle,original);assert.ok(original.events.every(e=>e.origin.source_time_ref===null));
  await assert.rejects(seamer('prepareEvents',{...query,expected_source_ref:{...page.value.source_ref,digest:digest('wrong')}}),{code:'peer_pairing_response'});
  await seamer('releaseEvents',page.handle_id);await assert.rejects(seamer('currentEvents',page.handle_id),{code:'peer_event_handle'});
  let stopScope=null;
  if(config.verifyEventScopes!==false){
  const leftId=active.transcript.parties.find(p=>p.service_id===first.identity.service_id).binding_id,left=await controller('get',leftId);
  const scopeRequest={request_id:id(),device_ids:[first.device_id]},registered=await controller('registerEventScope',scopeRequest);
  assert.deepEqual(await controller('registerEventScope',scopeRequest),registered);
  const pairing_ref={id:left.binding_id,revision:left.revision,digest:left.digest},allowed_scope_ref=registered.scope_ref;
  assert.deepEqual(registered.registration.sources.map(s=>s.source_key),[original.registration.source_key]);
  assert.deepEqual(registered.registration.sources.map(s=>s.registration_sequence),['2']);
  assert.deepEqual((await controller('currentEventScope',allowed_scope_ref)).registration,registered.registration);
  const slot={publisher_service_id:first.identity.service_id,consumer_service_id:second.identity.service_id,consumer_database_instance_id:second.identity.database_instance_id,
   consumer_id:'canonical_events',purpose:'canonical_trigger',allowed_scope_ref,max_subscriptions:1,pairing_ref};
  const consumerQuery={pairing_ref,consumer_id:slot.consumer_id,purpose:slot.purpose,allowed_scope_ref};
  await assert.rejects(controller('currentConsumer',consumerQuery),{code:'service_identity_consumer_unavailable'});
  const principal=await controller('createConsumer'),approvalInput={request_id:id(),principal_id:principal.id,expected_revision:principal.revision,audiences:[first.identity.service_id],
   permissions:['event.internal.subscribe','status.read'],device_ids:[first.device_id],not_before_ns:left.not_before_ns,expires_ns:left.expires_ns,consumer_slots:[slot]};
  const deniedLeft=await controller('get',denied.transcript.parties.find(p=>p.service_id===first.identity.service_id).binding_id);
  await assert.rejects(controller('approveConsumer',{...approvalInput,consumer_slots:[{...slot,pairing_ref:{id:deniedLeft.binding_id,revision:deniedLeft.revision,digest:deniedLeft.digest}}]}),{code:'service_consumer_pairing'});
  await assert.rejects(controller('approveConsumer',{...approvalInput,permissions:['event.internal.subscribe']}),{code:'service_identity_consumer_scope'});
  await assert.rejects(controller('approveConsumer',{...approvalInput,consumer_slots:[{...slot,allowed_scope_ref:{...allowed_scope_ref,content_digest:'b'.repeat(64)}}]}),{code:'event_scope_reference'});
  const approved=await controller('approveConsumer',approvalInput);assert.equal(approved.approval.format,'local-service-approval/2');
  assert.deepEqual(await controller('approveConsumer',approvalInput),approved);
  const consumer=await controller('currentConsumer',consumerQuery);assert.equal(consumer.owner.principal_id,principal.id);assert.equal(consumer.owner.consumer_database_instance_id,second.identity.database_instance_id);
  if(config.verifySubscriptions!==false){
  if(config.verifySourceProjection){const cut=await controller('sourceSnapshotCut',allowed_scope_ref);assert.equal(cut.records.length,1);assert.equal(cut.records[0].view.contiguous_sequence,'2');assert.equal(cut.records[0].view.state,'baseline_required');assert.equal(cut.records[0].view.baseline_ref,null);assert.deepEqual(cut.records[0].view.source_key,registered.registration.sources[0].source_key);assert.equal(cut.source_watermarks[0].baseline_sequence,'2');assert.deepEqual(await controller('sourceSnapshotCut',allowed_scope_ref),cut);frames.push({event:'native-source-projection-cut',cut});}
  const subscriptionRequest={request_id:id(),consumer_id:slot.consumer_id,purpose:slot.purpose,scope:registered.registration.scope},createSubscription=request=>seamer('createSubscription',{binding_id:active.binding_id,request});
  await assert.rejects(createSubscription(subscriptionRequest),{code:'forbidden'});
  const policyRequest={request_id:id(),principal_id:principal.id,subject_ref:{id:approved.principal.id,revision:approved.principal.revision,digest:approved.principal.digest},approval_ref:{id:approved.approval.id,revision:approved.approval.revision,digest:approved.approval.digest},
   audience:first.identity.service_id,permissions:approvalInput.permissions,device_ids:approvalInput.device_ids,not_before_ns:approvalInput.not_before_ns,expires_ns:approvalInput.expires_ns,consumer_slots:[slot]};
  const policy=await controller('approveConsumerPolicy',policyRequest);assert.equal(policy.format,'local-service-receiver-policy/2');
  assert.deepEqual(await controller('approveConsumerPolicy',policyRequest),policy);
  const subscription=await createSubscription(subscriptionRequest);assert.equal(subscription.status,202);assert.equal(subscription.view.state,'building');assert.equal(subscription.view.snapshot_ref,null);assert.deepEqual(subscription.view.owner,consumer.owner);
  const subQuery={binding_id:active.binding_id,subscription_id:subscription.view.binding.subscription_id};
  let currentSubscription=subscription;
  if(config.verifySnapshots){const deadline=Date.now()+5000;do{currentSubscription=await seamer('getSubscription',subQuery);if(currentSubscription.view.state!=='building')break;await new Promise(resolve=>setTimeout(resolve,50));}while(Date.now()<deadline);
   assert.equal(currentSubscription.view.state,'snapshot_ready');assert.equal(currentSubscription.view.revision,'2');assert.ok(currentSubscription.view.snapshot_ref);assert.equal(currentSubscription.view.ack_payload_digest,'0'.repeat(64));frames.push({event:'native-service-snapshot-ready',view:currentSubscription.view});}
  assert.deepEqual(await createSubscription(subscriptionRequest),{status:200,view:currentSubscription.view});
  if(config.verifyEventChannel){
   await assert.rejects(seamer('subscriptionSnapshot',{...subQuery,generation:'0'}),{code:'conflict'});
   const connected=await seamer('attachSubscription',{binding_id:active.binding_id,view:currentSubscription.view});assert.equal(connected.attached.binding.connection_generation,'1');assert.deepEqual(connected.ready.channel.owner,consumer.owner);
   const manifest=(await seamer('subscriptionSnapshot',{...subQuery,generation:'1'})).view,page=(await seamer('subscriptionPage',{...subQuery,generation:'1',snapshot_id:manifest.binding.snapshot_id,page_index:0})).view;
   assert.equal(page.delivery_binding.connection_generation,'1');assert.equal(page.content.records[0].view.contiguous_sequence,'2');assert.equal(page.manifest_digest,manifest.manifest_digest);
   const staged=await seamer('prepareSnapshot',{binding_id:active.binding_id,connection_attempt_id:connected.connection_attempt_id});assert.deepEqual(staged.projection.manifest,manifest);assert.deepEqual(staged.projection.records,page.content.records);assert.equal(staged.projection.records[0].view.state,'baseline_required');assert.equal(staged.context.publisher_service_boot_id,connected.ready.channel.service_boot_id);frames.push({event:'native-service-snapshot-staged',snapshot_ref:staged.subscription.snapshot_ref,records:staged.projection.records.length,durable_ingest:false});
   const adopted=await seamer('adoptSnapshot',{binding_id:active.binding_id,connection_attempt_id:connected.connection_attempt_id});assert.equal(adopted.receipt.state,'committed');assert.deepEqual(adopted.receipt.snapshot_ref,currentSubscription.view.snapshot_ref);assert.equal(adopted.sources.length,1);assert.equal(adopted.sources[0].reject_floor,'2');assert.equal(adopted.sources[0].state,'baseline_required');assert.equal((await seamer('getSubscription',subQuery)).view.ack_payload_digest,'0'.repeat(64));frames.push({event:'native-service-snapshot-adopted',...adopted,acknowledged:false});
   const beforeReplace=(await seamer('getSubscription',subQuery)).view;await assert.rejects(seamer('attachSubscription',{binding_id:active.binding_id,view:beforeReplace}));
   await assert.rejects(seamer('attachSubscription',{binding_id:active.binding_id,view:currentSubscription.view,options:{connection_attempt_id:connected.connection_attempt_id}}));
   const replacement=await seamer('attachSubscription',{binding_id:active.binding_id,view:beforeReplace,options:{replace_existing:true}});assert.equal(replacement.attached.binding.connection_generation,'2');
   await assert.rejects(seamer('subscriptionSnapshot',{...subQuery,generation:'1'}),{code:'conflict'});
   assert.equal((await seamer('subscriptionAttempt',{...subQuery,attempt_id:connected.connection_attempt_id})).view.current,false);
   const again=(await seamer('subscriptionSnapshot',{...subQuery,generation:'2'})).view;assert.deepEqual(again,manifest);const page2=(await seamer('subscriptionPage',{...subQuery,generation:'2',snapshot_id:manifest.binding.snapshot_id,page_index:0})).view;assert.deepEqual(page2.content,page.content);assert.equal(page2.delivery_binding.connection_generation,'2');
   const confirmed=await seamer('adoptSnapshot',{binding_id:active.binding_id,connection_attempt_id:replacement.connection_attempt_id,confirm:true});assert.deepEqual(confirmed.receipt,adopted.receipt);assert.equal(confirmed.confirmation.state,'confirmed');assert.equal(confirmed.confirmation.view.state,'catching_up');assert.equal(confirmed.confirmation.view.binding.connection_generation,'2');assert.equal(confirmed.sources[0].reject_floor,'2');assert.equal(confirmed.sources[0].state,'baseline_required');assert.notEqual(confirmed.confirmation.view.ack_payload_digest,manifest.manifest_digest);frames.push({event:'native-service-snapshot-confirmed',...confirmed});
   currentSubscription=await seamer('getSubscription',subQuery);frames.push({event:'native-service-event-channel',manifest,page,connected,replacement});
  }
  await assert.rejects(createSubscription({...subscriptionRequest,request_id:id()}),{code:'conflict'});
  assert.deepEqual((await seamer('getSubscription',subQuery)).view,currentSubscription.view);
  await assert.rejects(seamer('closeSubscription',{...subQuery,expected_revision:config.verifySnapshots?'1':'2'}),{code:'conflict'});
  const closed=await seamer('closeSubscription',{...subQuery,expected_revision:currentSubscription.view.revision});assert.equal(closed.view.state,'closed');assert.deepEqual(await createSubscription(subscriptionRequest),closed);
  const secondSubscription=await createSubscription({...subscriptionRequest,request_id:id()});assert.equal(secondSubscription.status,202);
  await controller('revokeConsumerPolicy',{request_id:id(),policy_ref:{id:policy.id,revision:policy.revision,digest:policy.digest}});
  await assert.rejects(seamer('getSubscription',{binding_id:active.binding_id,subscription_id:secondSubscription.view.binding.subscription_id}),{code:'forbidden'});
  frames.push({event:'native-service-subscription',created:subscription.view,closed:closed.view,local_policy_revoked:true});
  }
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
  }
  frames.push({event:'native-events-read',count:2,seamer_pid:second.pid,controller_pid:first.pid,source_ref:page.value.source_ref});await controller('finish');
  await wait(()=>frames.find(f=>f.event==='binding-peer-phase'&&f.phase==='events-stopped'));await assert.rejects(seamer('prepareEvents',query),{code:'peer_pairing_response'});
  if(stopScope)await assert.rejects(controller('currentEventScope',stopScope.scope_ref));frames.push({event:'native-events-stopped',read_denied:true});await controller('finish');
  await wait(()=>frames.find(f=>f.event==='binding-peer-phase'&&f.phase==='withdrawn'));await assert.rejects(seamer('prepareEvents',query),{code:'peer_pairing_response'});frames.push({event:'native-events-withdrawn',read_denied:true});await controller('finish');
 }finally{if(!stopped){try{await seamer('shutdown');}catch{child.kill();}}await exited;}
}
