import crypto from 'node:crypto';
import {canonical,digest,same} from './runtime/auth-clock-value.mjs';
import {deviceBindingsBundle} from './runtime/peer-device-bindings.mjs';
import {validateAutomationModel} from './sdk/automation-program.mjs';
const need=(ok,code)=>{if(!ok)throw Object.assign(new Error('binding_approval_'+code),{code:'binding_approval_'+code});};
const fields=(v,keys)=>need(v&&Object.keys(v).sort().join(',')===keys.split(',').sort().join(','),'request');
const uuid=v=>need(typeof v==='string'&&/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(v),'request');
const hash=v=>need(typeof v==='string'&&/^sha256:[0-9a-f]{64}$/.test(v),'request');
const put=result=>need(result.changes===1,'storage');
function eventBindingCandidate(event,device){
 const {registration, fault}=event.reply.bundle,{binding_ref:binding,qualification:qualified,payload_schema_ref:schema,payload_schema:document}=registration;
 const bundle=device.reply.bundle,catalog=bundle.registration,review=qualified.review;
 need(!fault&&same(event.pairing_ref,device.pairing_ref)&&same(event.request.scope,device.request.scope)&&same(event.source_ref.scope,catalog.scope)&&
  binding.target.kind==='device'&&binding.target.device_id===catalog.device_id&&qualified.device_id===catalog.device_id&&
  qualified.device_session_id===catalog.device_session_id&&qualified.channel_id===catalog.channel_id&&same(qualified.runtime_binding_ref,catalog.runtime_binding_ref),
  'event_source_changed');
 need(qualified.format==='leafseamer-device-event-qualification/1'&&review?.format==='leafseamer-fixed-event-review/1'&&
  review.review_id==='leafseamer.simulator.control.events'&&review.revision==='1'&&review.capture_api==='leafseamer-device-event-capture/1'&&
  review.capture_boundary==='fixed_virtual_device_successful_memory_write'&&review.value_minimum===-2147483648&&review.value_maximum===2147483647&&
  qualified.review_ref?.id===review.review_id&&qualified.review_ref.revision===review.revision&&qualified.review_ref.digest===digest(review)&&
  review.event_type==='leafseamer.simulator.value.written'&&qualified.capture.event_type===review.event_type&&binding.target.trigger_id==='simulator_value_written'&&
  review.delivery_semantics==='discrete'&&binding.delivery_semantics===review.delivery_semantics&&
  ['module_digest','protocol_io_policy_digest','template_digest'].every(key=>catalog.qualification?.[key]===review[key]),'event_review');
 need(same(document,{type:'object',additionalProperties:false,properties:{value:{type:'integer',minimum:review.value_minimum,maximum:review.value_maximum},
  write_sequence:{type:'string',pattern:'^[1-9][0-9]{0,19}$(?![\\s\\S])'}},required:['value','write_sequence']}),'event_schema');
 const actions=bundle.bindings.filter(item=>item.kind==='action'),action=actions[0]?.value;
 need(actions.length===1&&action.footprint.complete&&action.footprint.reads.length>0&&same(action.footprint.evidence_ref,catalog.footprint_ref),'event_footprint');
 const binding_id=registration.registration_id+'.event',schema_ref={id:schema.object_id,revision:schema.revision,digest:schema.digest};
 const value={binding_id,source:'event',authority_id:binding.owner_service_id,
  registration_ref:{id:binding.registration_id,revision:binding.revision,digest:digest(binding)},schema_ref,producer_step_id:null,
  projections:[{pointer:'/value',value_type:{kind:'binary64',unit:null},reads:action.footprint.reads},
   {pointer:'/write_sequence',value_type:{kind:'integer_string',unit:null},reads:action.footprint.reads}]};
 validateAutomationModel('InputBinding',value);
 return {binding:{kind:'input',binding_id,content_ref:{id:binding_id,revision:'1',digest:digest(value)},value},
  schema:{schema_ref,document},source_ref:event.source_ref,source_key:registration.source_key,device_registration_ref:bundle.registration_ref};
}
export function bindingApprovalStore({db,service,isOpen,authority,receiver,eventReceiver=null,pairings}){
 const scope={authority_service_id:service.service_id,database_instance_id:service.database_instance_id};
 const inside=()=>need(isOpen()&&db.open&&db.inTransaction&&Number(service.schema_revision)>=3,'transaction');
 const writer=work=>{need(isOpen()&&db.open&&!db.inTransaction,'closed');need(Number(service.schema_revision)>=3,'migration_required');return db.transaction(work).immediate();};
 const reference=value=>({id:value.approval_id,revision:value.revision,digest:digest(value)});
 function read(id,revision=null){inside();uuid(id);const row=revision===null?db.prepare('SELECT r.body_jcs,r.body_digest,r.revision,r.definition_id,r.state FROM binding_approval_revision r JOIN binding_approval_head h USING(approval_id,revision) WHERE approval_id=?').raw().get(id):db.prepare('SELECT body_jcs,body_digest,revision,definition_id,state FROM binding_approval_revision WHERE approval_id=? AND revision=?').raw().get(id,Number(revision));
  need(row,'missing');const value=JSON.parse(row[0]);need(canonical(value)===row[0]&&digest(value)===row[1]&&value.approval_id===id&&value.revision===String(row[2])&&value.definition_id===row[3]&&value.state===row[4]&&same(value.scope,scope),'integrity');return value;
 }
 const authorize=(authentication,definition,write=true,context=null)=>{inside();const proof=structuredClone(authority.bindingApprovalInTransaction(authentication,definition,write,context));need(same(proof.scope,scope)&&proof.definition_id===definition&&(!write||proof.actor.principal_kind==='human'),'authority');return proof;};
 function prior(actor,request){const row=db.prepare('SELECT request_digest,approval_id,revision FROM binding_approval_operation WHERE actor_digest=? AND request_id=?').raw().get(digest(actor),request.request_id);if(!row)return null;need(row[0]===digest(request),'conflict');return read(row[1],String(row[2]));}
 function save(value,proof,request){
  authority.bindingApprovalCapacityInTransaction?.(proof);const transaction=crypto.randomUUID(),body=canonical(value);need(Buffer.byteLength(body)<=524288,'capacity');
  put(db.prepare('INSERT INTO binding_approval_revision VALUES(?,?,?,?,?,?,?)').run(value.approval_id,Number(value.revision),value.definition_id,value.state,body,digest(value),transaction));
  if(value.revision==='1')put(db.prepare('INSERT INTO binding_approval_head VALUES(?,1)').run(value.approval_id));
  else put(db.prepare('UPDATE binding_approval_head SET revision=2 WHERE approval_id=? AND revision=1').run(value.approval_id));
  put(db.prepare('INSERT INTO binding_approval_operation VALUES(?,?,?,?,?,?)').run(transaction,digest(proof.actor),request.request_id,digest(request),value.approval_id,Number(value.revision)));
  const notice={format:'leafseamer.binding-approval-notice/1',scope,transaction_id:transaction,approval_ref:reference(value),definition_id:value.definition_id,state:value.state,protection:'pending'};
  put(db.prepare('INSERT INTO binding_approval_outbox VALUES(?,?,?)').run(transaction,canonical(notice),digest(notice)));need(same(read(value.approval_id),value),'integrity');return value;
 }
 function current(approvalRef,definitionId,handle){
   inside();fields(approvalRef,'id,revision,digest');const value=read(approvalRef.id);need(value.state==='approved'&&same(reference(value),approvalRef)&&value.definition_id===definitionId,'not_current');
   pairings.requireActivatedInTransaction(value.pairing_ref);const source=structuredClone(receiver.currentInTransaction(handle));
   need(same(source.pairing_ref,value.pairing_ref)&&same(source.request.scope,value.source_scope)&&same(source.reply.bundle.registration_ref,value.registration_ref)&&source.bundle_digest===value.bundle_digest&&same(source.reply.bundle,value.bundle),'source_changed');
   return value;
  }
 function resolveRunActionsInTransaction(definitionId,actions){
  inside();
  const byId=new Map(actions.map(action=>[action.binding_id,action])),resolved=new Map();
  const registrationIds=[...new Set(actions.map(action=>action.registration_ref.id))];
  const approvals=db.prepare(`SELECT r.approval_id FROM binding_approval_revision r JOIN binding_approval_head h USING(approval_id,revision)
   WHERE r.definition_id=? AND r.state='approved' AND json_extract(r.body_jcs,'$.registration_ref.object_id') IN (SELECT value FROM json_each(?))`).raw();
  for(const [id] of approvals.all(definitionId,canonical(registrationIds))){
   const value=read(id),bundle=value.bundle,registration=bundle.registration;
   for(const binding of bundle.bindings){
    const action=byId.get(binding.binding_id);
    if(!action||binding.kind!=='action'||binding.content_ref.digest!==action.binding_digest)continue;
    need(!resolved.has(action.binding_id),'run_target_ambiguous');
    const registrationRef=value.registration_ref;
    deviceBindingsBundle(bundle,{scope:value.source_scope,device_id:action.device_id,expected_registration_ref:registrationRef});
    need(value.bundle_digest===digest(bundle)&&same(action.registration_ref,{id:registrationRef.object_id,revision:registrationRef.revision,digest:registrationRef.digest})&&
     registration.scope.authority_service_id===action.controller_id&&registration.provider_id===action.provider_id&&registration.capability_id===action.capability_id&&
     same(registration.catalog_ref,action.catalog_ref)&&same(registration.footprint_ref,action.footprint_ref)&&
     same(registration.parameter_schema_ref,action.parameter_schema_ref)&&same(registration.result_schema_ref,action.result_schema_ref),'run_target_mismatch');
    const pairing=pairings.requireActivatedInTransaction(value.pairing_ref);
    const controller=pairing.transcript.parties.find(p=>p.service_kind==='controller'&&p.service_id===action.controller_id);
    need(controller&&controller.service_id===pairing.peer_service_id&&controller.database_instance_id===registration.scope.database_instance_id,'run_target_mismatch');
    const version=registration.capability_version;
    need(/^[A-Za-z0-9._-]{1,128}$(?![\s\S])/.test(action.capability_id)&&typeof version==='string'&&version.length<=128&&
     /^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?$(?![\s\S])/.test(version),'run_target_version');
    resolved.set(action.binding_id,{binding_id:action.binding_id,approval_ref:reference(value),pairing_ref:value.pairing_ref,registration_ref:registrationRef,
     source_scope:value.source_scope,bundle_digest:value.bundle_digest,controller_scope:registration.scope,
     device_action:{device_id:action.device_id,action_id:action.capability_id,capability_version:version}});
   }
  }
  need(resolved.size===actions.length,'run_target_unapproved');
  return actions.map(action=>resolved.get(action.binding_id));
 }
 async function prepareRunActionObservations(evidence){
  need(isOpen()&&db.open&&!db.inTransaction,'transaction');
  const candidates=[...new Map(evidence.map(item=>[item.approval_ref.id,item])).values()],checks=[];
  for(let offset=0;offset<candidates.length;offset+=4){
   if(checks.length)checks[0](false);
   const batch=await Promise.allSettled(candidates.slice(offset,offset+4).map(item=>receiver.prepareMatch({
    binding_id:item.pairing_ref.id,scope:item.source_scope,device_id:item.device_action.device_id,expected_registration_ref:item.registration_ref
   },{pairing_ref:item.pairing_ref,bundle_digest:item.bundle_digest})));
   for(const result of batch){
    if(result.status==='rejected')throw result.reason;
    checks.push(result.value);
   }
  }
  return ()=>{
   inside();
   for(const check of checks)check(true);
   // 最先发出的挑战最早到期，整组核验结束时仍须处于它的原期限。
   if(checks.length>1)checks[0](true);
  };
 }
 function approvalRequest(request){
  fields(request,'request_id,approval_id,definition_id,expected_bundle_digest');uuid(request.request_id);uuid(request.approval_id);uuid(request.definition_id);hash(request.expected_bundle_digest);return structuredClone(request);
 }
 function queryRequest(request){
  fields(request,'binding_id,scope,device_id,expected_registration_ref');uuid(request.binding_id);uuid(request.device_id);return structuredClone(request);
 }
 function candidateProof(authentication,definition){
  const proof=authorize(authentication,definition,false);
  need(db.prepare('SELECT 1 FROM definition_identity WHERE definition_id=?').pluck().get(definition),'definition_missing');
  return proof;
 }
 function eventQuery(request){
  fields(request,'binding_id,scope,device_id,expected_source_ref,expected_registration_ref');
  const {expected_source_ref,...deviceRequest}=structuredClone(request);
  return {...queryRequest(deviceRequest),expected_source_ref};
 }
 async function prepareEventSources(fixed){
  need(eventReceiver,'event_unavailable');
  const {expected_source_ref,...deviceRequest}=fixed;
  const [event,device]=await Promise.allSettled([
   eventReceiver.prepare({binding_id:fixed.binding_id,scope:fixed.scope,device_id:fixed.device_id,expected_source_ref,limit:1}),receiver.prepare(deviceRequest)
  ]);
  const release=()=>{
   if(event.status==='fulfilled')eventReceiver.release(event.value);
   if(device.status==='fulfilled')receiver.release(device.value);
  };
  const failed=[event,device].find(result=>result.status==='rejected');if(failed){release();throw failed.reason;}
  return {release,current:()=>[eventReceiver.currentInTransaction(event.value),receiver.currentInTransaction(device.value)]};
 }
 async function readEventCandidate(definitionId,fixed,visit){
  const observation=await prepareEventSources(fixed);
  try{
   return writer(()=>{
    const originals=observation.current(),[origin,catalog]=originals,body={definition_id:definitionId,...eventBindingCandidate(origin,catalog)};
    const result=visit({...body,candidate_digest:digest(body)},origin.pairing_ref);
    need(same(observation.current(),originals),'event_source_changed');return result;
   });
  }finally{observation.release();}
 }
 const eventColumns='SELECT approval_id,revision,definition_id,binding_id,context_id,actor_digest,request_id,request_digest,state,body_jcs,body_digest,previous_digest FROM event_binding_approval_revision';
 function decodeEventApproval(row){
  need(row,'event_approval_missing');
  const [id,revision,definition,bindingId,context,actor,requestId,requestDigest,state,text,hash,previous]=row;
  const value=JSON.parse(text),{scope:owner,request,candidate,authorization,previous_ref}=value;
  const {candidate_digest,...content}=candidate;
  need(canonical(value)===text&&digest(value)===hash&&same(owner,scope)&&value.format==='leafseamer.event-binding-approval/1'&&
   value.approval_id===id&&value.revision===String(revision)&&value.state===state&&definition===candidate.definition_id&&
   digest(content)===candidate_digest&&candidate.binding.binding_id===bindingId&&authorization.context_id===context&&digest(authorization.actor)===actor&&
   request.request_id===requestId&&digest(request)===requestDigest&&same(previous_ref,previous===null?null:{id,revision:'1',digest:previous}),'event_approval_integrity');
  return {...value,approval_ref:{id,revision:String(revision),digest:hash}};
 }
 const eventHead=id=>decodeEventApproval(db.prepare(eventColumns+' WHERE approval_id=? ORDER BY revision DESC LIMIT 1').raw().get(id));
 function priorEvent(actor,request){
  const row=db.prepare(eventColumns+' WHERE actor_digest=? AND request_id=?').raw().get(digest(actor),request.request_id);
  if(!row)return null;
  need(row[7]===digest(request),'event_approval_conflict');return decodeEventApproval(row);
 }
 function eventWriteProof(authentication,definition,context,operation){
  fields(context,'context_id,operation_id');uuid(context.context_id);need(context.operation_id===operation,'event_approval_context');
  return authorize(authentication,definition,true,context);
 }
 function saveEventApproval(value){
  const {authorization:proof,candidate,request,previous_ref}=value;
  authority.bindingApprovalCapacityInTransaction(proof);
  need(db.prepare('SELECT count(*) FROM event_binding_approval_revision').pluck().get()<4096,'event_approval_capacity');
  const body=canonical(value),hash=digest(value);
  put(db.prepare('INSERT INTO event_binding_approval_revision VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)').run(value.approval_id,Number(value.revision),
   candidate.definition_id,candidate.binding.binding_id,proof.context_id,digest(proof.actor),request.request_id,digest(request),value.state,
   body,hash,previous_ref?Number(previous_ref.revision):null,previous_ref?.digest??null));
  return decodeEventApproval(db.prepare(eventColumns+' WHERE approval_id=? AND revision=?').raw().get(value.approval_id,Number(value.revision)));
 }
 function publicationMaterials(draft,policy){
  inside();const definition=draft.definition_id;
  need(policy.assertCurrentInTransaction(draft.content.policy_ref)===true,'publication_policy');
  const selections=new Map(draft.content.selections.map(item=>[item.kind+':'+item.binding_id,item]));
  const selected=new Map(),devices=new Map(),events=new Map();
  function match(binding){
   const key=binding.kind+':'+binding.binding_id,item=selections.get(key);
   if(!item||!same(item.content_ref,binding.content_ref))return null;
   need(!selected.has(key),'publication_ambiguous');return key;
  }
  const deviceRows=db.prepare(`SELECT r.approval_id FROM binding_approval_revision r JOIN binding_approval_head USING(approval_id,revision)
   WHERE r.definition_id=? AND r.state='approved'`).pluck().all(definition);
  for(const id of deviceRows){
   const value=read(id),bundle=value.bundle,approval_ref=reference(value);
   for(const binding of bundle.bindings){
    const key=match(binding);if(!key)continue;
    const schema=binding.kind==='action'?bundle.schemas.find(item=>item.binding_id===binding.binding_id):null;
    need(binding.kind!=='action'||schema&&bundle.registration.parameter_scope==='all_allowed_parameters'&&binding.value.footprint.complete,'publication_footprint');
    selected.set(key,{binding,schema,approval_ref,kind:'device',aliases:bundle.alias_table.entries});devices.set(id,{approval_ref,value});
   }
  }
  const eventRows=db.prepare(eventColumns+` AS a WHERE definition_id=? AND revision=1
   AND NOT EXISTS(SELECT 1 FROM event_binding_approval_revision r WHERE r.approval_id=a.approval_id AND r.revision=2)`).raw().all(definition);
  for(const row of eventRows){
   const value=decodeEventApproval(row),binding=value.candidate.binding,key=match(binding);if(!key)continue;
   selected.set(key,{binding,schema:null,approval_ref:value.approval_ref,kind:'event',aliases:[]});events.set(value.approval_id,value);
  }
  need(selected.size===selections.size,'publication_binding_unapproved');
  const ordered=[...selections.keys()].map(key=>selected.get(key)),entries=ordered[0]?.aliases??[];
  need(ordered.every(value=>same(value.aliases,entries)),'publication_aliases');
  const bindings=ordered.map(value=>value.binding),schemas=ordered.flatMap(value=>value.schema?[value.schema]:[]);
  const alias_table={ref:{id:'approved-selection-aliases',revision:'1',digest:digest(entries)},entries};
  const review={scope,selection_digest:draft.selection_digest,alias_table_digest:digest(alias_table),policy_ref:draft.content.policy_ref,
   parameter_schema_refs:schemas.map(({binding_id,schema_ref})=>({binding_id,schema_ref})),parameter_scope:'all_allowed_parameters',
   binding_approval_refs:ordered.map(({binding,kind,approval_ref})=>({binding_id:binding.binding_id,kind,approval_ref}))};
  const review_ref={id:'approved-binding-publication',revision:'1',digest:digest(review)};
  return {snapshot:{bindings,schemas,alias_table,review,review_ref},devices,events};
 }
 function publicationBindingsCurrentInTransaction(publication,policy){
  inside();
  const {draft,snapshot:{bindings,review}}=publication;
  if(policy?.assertCurrentInTransaction(review.policy_ref)!==true)return false;
  const selected=new Map(bindings.map(binding=>[binding.kind+':'+binding.binding_id,binding]));
  return review.binding_approval_refs.every(({kind,approval_ref},index)=>{
   const selection=draft.content.selections[index],binding=selected.get(selection.kind+':'+selection.binding_id);
   if(kind==='event'){
    const current=eventHead(approval_ref.id);
    return current.state==='approved'&&same(current.approval_ref,approval_ref)&&
     current.candidate.definition_id===draft.definition_id&&same(current.candidate.binding,binding);
   }
   const current=read(approval_ref.id);
   return current.state==='approved'&&same(reference(current),approval_ref)&&current.definition_id===draft.definition_id&&
    current.bundle.bindings.some(candidate=>same(candidate,binding));
  });
 }
 function publicationBindingRevocationInTransaction({draft,snapshot:{review}}){
  inside();
  for(const {kind,approval_ref} of review.binding_approval_refs??[]){
   if(kind==='event'){
    const revoked=eventHead(approval_ref.id);
    if(revoked.state==='revoked'&&revoked.candidate.definition_id===draft.definition_id&&same(revoked.previous_ref,approval_ref)){
     return {kind,approval_ref,revocation_ref:revoked.approval_ref};
    }
   }else{
    const revoked=read(approval_ref.id);
    if(revoked.state==='revoked'&&revoked.definition_id===draft.definition_id&&same(reference(read(approval_ref.id,'1')),approval_ref)){
     return {kind,approval_ref,revocation_ref:reference(revoked)};
    }
   }
  }
  return null;
 }
 function preparePublication(draft,policy){
  const original=writer(()=>publicationMaterials(draft,policy));
  return {snapshot:original.snapshot,async observe(){
   const checks=[],releases=[],tasks=[];
   for(const {approval_ref,value} of original.devices.values())tasks.push(async()=>{
    const handle=await receiver.prepare({binding_id:value.pairing_ref.id,scope:value.source_scope,device_id:value.bundle.registration.device_id,
     expected_registration_ref:value.registration_ref});
    releases.push(()=>receiver.release(handle));checks.push(()=>current(approval_ref,draft.definition_id,handle));
   });
   for(const approval of original.events.values())tasks.push(async()=>{
    const source=await prepareEventSources(approval.request.source);releases.push(source.release);
    checks.push(()=>{
     const [event,device]=source.current(),{candidate_digest,...body}=approval.candidate;
     need(same(event.pairing_ref,approval.pairing_ref)&&same(body,{definition_id:draft.definition_id,...eventBindingCandidate(event,device)}),'event_source_changed');
    });
   });
   const release=()=>{for(const close of releases)close();};
   try{
    // 各接收器原并发上限为四，最终事务继续要求最早取得的证据有效。
    for(let offset=0;offset<tasks.length;offset+=4){
     const batch=await Promise.allSettled(tasks.slice(offset,offset+4).map(task=>task()));
     const failed=batch.find(result=>result.status==='rejected');if(failed)throw failed.reason;
    }
    return {release,assertCurrentInTransaction(){
     const latest=publicationMaterials(draft,policy);
     need(same(latest.snapshot,original.snapshot),'publication_source_changed');
     for(const check of checks)check();
     return digest(latest.snapshot);
    }};
   }catch(error){release();throw error;}
  }};
 }
 const eventApprovals=Number(service.schema_revision)>=11?{
  async approveEventCurrent(authentication,query,input,context){
   fields(input,'approval_id,definition_id,expected_candidate_digest,request_id');
   for(const key of ['approval_id','definition_id','request_id'])uuid(input[key]);hash(input.expected_candidate_digest);
   context=structuredClone(context);const source=eventQuery(query),request={operation_id:'approveEventBinding',...structuredClone(input),source};
   const definition=request.definition_id,verify=()=>eventWriteProof(authentication,definition,context,'approveDefinitionBindings');
   const prepared=writer(()=>{const proof=verify(),prior=priorEvent(proof.actor,request);candidateProof(authentication,definition);return {proof,prior};});
   if(prepared.prior)return prepared.prior;
   return readEventCandidate(definition,source,(candidate,pairing_ref)=>{
    const proof=verify();need(same(proof,prepared.proof),'authority_changed');
    const prior=priorEvent(proof.actor,request);if(prior)return prior;
    need(candidate.candidate_digest===request.expected_candidate_digest,'event_source_changed');
    need(!db.prepare('SELECT 1 FROM event_binding_approval_revision WHERE approval_id=?').pluck().get(request.approval_id),'event_approval_exists');
    need(!db.prepare(`SELECT 1 FROM event_binding_approval_revision a WHERE a.definition_id=? AND a.binding_id=? AND a.revision=1
     AND NOT EXISTS(SELECT 1 FROM event_binding_approval_revision r WHERE r.approval_id=a.approval_id AND r.revision=2)`).pluck().get(definition,candidate.binding.binding_id),'event_approval_exists');
    const result=saveEventApproval({format:'leafseamer.event-binding-approval/1',scope,approval_id:request.approval_id,revision:'1',state:'approved',
     candidate,pairing_ref,authorization:proof,request,previous_ref:null,protection:null});
    need(same(proof,verify()),'authority_changed');return result;
   });
  },
  revokeEventApproval(authentication,input,context){
   fields(input,'approval_ref,definition_id,request_id');fields(input.approval_ref,'id,revision,digest');
   uuid(input.approval_ref.id);uuid(input.definition_id);uuid(input.request_id);hash(input.approval_ref.digest);
   need(input.approval_ref.revision==='1','event_approval_revision');
   context=structuredClone(context);const request={operation_id:'revokeEventBinding',...structuredClone(input)},definition=request.definition_id;
   const verify=()=>eventWriteProof(authentication,definition,context,'revokeBindingApproval');
   return writer(()=>{
    const proof=verify(),old=priorEvent(proof.actor,request);if(old)return old;
    const {approval_ref,...current}=eventHead(request.approval_ref.id);
    need(current.candidate.definition_id===definition&&current.state==='approved'&&same(approval_ref,request.approval_ref),'event_approval_revision');
    const result=saveEventApproval({...current,revision:'2',state:'revoked',authorization:proof,request,previous_ref:approval_ref,protection:'pending'});
    need(same(proof,verify()),'authority_changed');return result;
   });
  },
  getEventApproval(authentication,id){
   uuid(id);return writer(()=>{const value=eventHead(id),definition=value.candidate.definition_id;
    const proof=authorize(authentication,definition,false);need(same(proof,authorize(authentication,definition,false)),'authority_changed');return value;});
  }
 }:{};
 function approve(authentication,handle,request,expectedProof=null,context=null){
   request=approvalRequest(request);
   return writer(()=>{
    const proof=authorize(authentication,request.definition_id,true,context);need(expectedProof===null||same(proof,expectedProof),'authority_changed');
    const old=prior(proof.actor,request);if(old){need(same(proof,authorize(authentication,request.definition_id,true,context)),'authority_changed');return old;}
    need(db.prepare('SELECT 1 FROM definition_identity WHERE definition_id=?').pluck().get(request.definition_id),'definition_missing');
    need(!db.prepare('SELECT 1 FROM binding_approval_head WHERE approval_id=?').pluck().get(request.approval_id),'exists');need(db.prepare('SELECT count(*) FROM binding_approval_revision').pluck().get()<2048,'capacity');
    const source=structuredClone(receiver.currentInTransaction(handle));need(source.bundle_digest===request.expected_bundle_digest,'source_changed');
    const value={format:'leafseamer.binding-approval/1',scope,approval_id:request.approval_id,revision:'1',definition_id:request.definition_id,state:'approved',purpose:'definition_compile',
     pairing_ref:source.pairing_ref,source_scope:source.request.scope,registration_ref:source.reply.bundle.registration_ref,bundle_digest:source.bundle_digest,bundle:source.reply.bundle,authorization:proof};
    save(value,proof,request);need(same(source,receiver.currentInTransaction(handle)),'source_changed');need(same(proof,authorize(authentication,request.definition_id,true,context)),'authority_changed');return value;
   });
  }
 return Object.freeze({
  prepare:request=>receiver.prepare(request),inspect:handle=>receiver.current(handle),approve,
  // 候选正文供人员查看；不把本地短期读取句柄交给交互界面持有。
  async candidate(authentication,definitionId,request){
   uuid(definitionId);request=queryRequest(request);
   const proof=writer(()=>candidateProof(authentication,definitionId));
   const handle=await receiver.prepare(request);
   try{return writer(()=>{need(same(proof,authorize(authentication,definitionId,false)),'authority_changed');const source=structuredClone(receiver.currentInTransaction(handle));
    need(same(proof,authorize(authentication,definitionId,false)),'authority_changed');return {definition_id:definitionId,bundle_digest:source.bundle_digest,bundle:source.reply.bundle};});}
   finally{receiver.release(handle);}
  },
  async eventCandidate(authentication,definitionId,request){
   uuid(definitionId);const fixed=eventQuery(request),proof=writer(()=>candidateProof(authentication,definitionId));
   return readEventCandidate(definitionId,fixed,result=>{
    need(same(proof,authorize(authentication,definitionId,false)),'authority_changed');return result;
   });
  },
  ...eventApprovals,
  ...(Number(service.schema_revision)>=11&&{preparePublication,publicationBindingsCurrentInTransaction,publicationBindingRevocationInTransaction}),
  async approveCurrent(authentication,sourceRequest,request,context=null){
   context=structuredClone(context);
   request=approvalRequest(request);sourceRequest=queryRequest(sourceRequest);
   const prepared=writer(()=>{const proof=authorize(authentication,request.definition_id,true,context),old=prior(proof.actor,request);
    need(db.prepare('SELECT 1 FROM definition_identity WHERE definition_id=?').pluck().get(request.definition_id),'definition_missing');
    need(same(proof,authorize(authentication,request.definition_id,true,context)),'authority_changed');return {proof,old};});
   if(prepared.old)return prepared.old;
   const handle=await receiver.prepare(sourceRequest);
   try{return approve(authentication,handle,request,prepared.proof,context);}finally{receiver.release(handle);}
  },
  lookup(authentication,request){
   fields(request,'request_id,request_digest,definition_id');uuid(request.request_id);uuid(request.definition_id);hash(request.request_digest);request=structuredClone(request);
   return writer(()=>{const proof=structuredClone(authority.bindingApprovalLookupInTransaction(authentication,request.definition_id));
    need(same(proof.scope,scope)&&proof.definition_id===request.definition_id,'authority');
    const row=db.prepare('SELECT request_digest,approval_id,revision FROM binding_approval_operation WHERE actor_digest=? AND request_id=?').raw().get(digest(proof.actor),request.request_id);
    let result=null;if(row){need(row[0]===request.request_digest,'conflict');result=read(row[1],String(row[2]));need(result.definition_id===request.definition_id,'missing');}
    need(same(proof,authority.bindingApprovalLookupInTransaction(authentication,request.definition_id)),'authority_changed');return {...request,result};});
  },
  revoke(authentication,request,context=null,definitionId=null){context=structuredClone(context);fields(request,'request_id,approval_id,expected_revision');uuid(request.request_id);uuid(request.approval_id);need(request.expected_revision==='1','revision');request=structuredClone(request);
   return writer(()=>{const current=read(request.approval_id);need(definitionId===null||current.definition_id===definitionId,'missing');const proof=authorize(authentication,current.definition_id,true,context),old=prior(proof.actor,request);if(old){need(same(proof,authorize(authentication,current.definition_id,true,context)),'authority_changed');return old;}
    need(current.state==='approved'&&current.revision===request.expected_revision,'revision');const result=save({...current,revision:'2',state:'revoked',revocation:proof},proof,request);
    need(same(proof,authorize(authentication,current.definition_id,true,context)),'authority_changed');return result;});
  },
  get(authentication,id){return writer(()=>{const value=read(id),proof=authorize(authentication,value.definition_id,false);need(same(proof,authorize(authentication,value.definition_id,false)),'authority_changed');return value;});},
  check(authentication,approvalRef,definitionId,handle){return writer(()=>{const proof=authorize(authentication,definitionId,false),value=current(approvalRef,definitionId,handle);need(same(proof,authorize(authentication,definitionId,false)),'authority_changed');return value;});},
  currentInTransaction:current,resolveRunActionsInTransaction,prepareRunActionObservations
 });
}
