import {canonical,checked,digest,ref,same} from './runtime/auth-clock-value.mjs';
import {clockInteger} from './runtime/native-clock.mjs';
import {validateEventTimeModel} from './sdk/event-time.mjs';
import {automationTriggerMatchStore} from './automation-trigger-match-store.mjs';

const need=(ok,reason)=>{
 if(!ok)throw Object.assign(new Error('reconcile_inbox_'+reason),{code:'reconcile_inbox_'+reason});
};
const stripGeneration=({connection_generation,...binding})=>binding;
const protection={baseline_required:0,synchronized:0,desynchronized:1,quarantined:2,closed:3};
function eventDisposition(origin,mode,age_evidence_ref=null,sourceTime=null,sourceReference=null,referenceResult=null){
 const reason=referenceResult?.reason??(sourceReference?'receiver_reference_unavailable':sourceTime?(sourceTime.reference_context_ref?'source_reference_unverified':'source_reference_missing')
  :(origin.source_time_ref===null?'source_time_missing':'source_time_unverified'));
 const reasons=[reason];
 if(mode==='reconcile')reasons.push('reconciled_delivery');
 return {state:'observed_only',age_evidence_ref,reason_codes:reasons};
}

function sourceTimeOriginal(origin,evidence){
 validateEventTimeModel('SourceTimeEvidence',evidence);
 const reference=origin.source_time_ref,{id,revision,digest:actual,...fields}=evidence;
 need(reference?.object_id===id&&reference.revision===revision&&reference.digest===actual&&digest({id,revision,...fields})===actual&&
  same(evidence.source_key,origin.source_key)&&same(evidence.binding_ref,origin.binding_ref)&&evidence.event_id===origin.event_id&&
  evidence.source_sequence===origin.source_sequence&&evidence.payload_digest===digest(origin.payload),'source_time_integrity');
 return evidence;
}

const ppmScale=1000000n,ntpFraction=1n<<32n;
const floor=(n,d)=>n<0n?(n-d+1n)/d:n/d;
const ceil=(n,d)=>-floor(-n,d);
const millis=1000000n;
function elapsedBounds(profile,delta){
 const rate=BigInt(profile.elapsed_error_ppm),quantum=BigInt(profile.span_quantization_ns)*ppmScale;
 need(rate>=0n&&rate<ppmScale&&quantum>=0n&&delta>=0n,'source_reference_elapsed');
 const lower=delta*(ppmScale-rate)-quantum;
 return {lower:lower<0n?0n:lower,upper:delta*(ppmScale+rate)+quantum};
}
const stamp=({era,seconds,fraction})=>((BigInt(era)*ntpFraction+BigInt(seconds)-2208988800n)*ntpFraction+BigInt(fraction))*1000000000n;
function timeReference(body,name,digestField){
 const {[digestField]:hash,...fields}=body;
 need(digest(fields)===hash,'source_reference_integrity');
 return {id:body[name],revision:body.revision,digest:hash};
}
function referenceInterval(bundle,domain,before,after){
 const {anchor,exchange,request,profile,policy,error,assessment}=bundle,{binding}=anchor;
 const domainFields=['authority_service_id','database_instance_id','clock_domain_id','descriptor_digest','clock_generation'];
 need(domainFields.every(key=>binding.domain[key]===domain[key])&&binding.time_generation==='1'&&
  same(binding.profile_ref,timeReference(profile,'profile_id','profile_digest'))&&
  same(binding.policy_ref,timeReference(policy,'policy_id','policy_digest'))&&same(anchor.exchange_ref,timeReference(exchange,'exchange_id','exchange_digest'))&&
  same(exchange.request_ref,timeReference(request,'request_id','request_digest'))&&same(exchange.error_ref,timeReference(error,'error_id','error_digest'))&&
  [exchange,request,error,assessment].every(value=>same(value.binding,binding))&&
  same(assessment.anchor_ref,timeReference(anchor,'anchor_id','anchor_digest'))&&assessment.decision==='conditional'&&
  profile.model_id==='reference-bound/1'&&profile.elapsed_model==='observed-delta-relative/1'&&profile.time_scale==='posix-utc'&&
  profile.leap_policy==='normal-only'&&profile.smear_policy==='forbidden','source_reference_binding');
 const m1=clockInteger(request.send_before_tick_ns),m4=clockInteger(exchange.receive_after_tick_ns);
 need(m1<=m4&&m4<=before&&before<=after&&anchor.exchange_start_tick_ns===String(m1)&&anchor.anchor_tick_ns===String(m4)&&
  m4<BigInt(request.response_deadline_tick_ns)&&exchange.leap===0,'source_reference_order');
 const elapsed=delta=>elapsedBounds(profile,delta);
 const span=elapsed(m4-m1).upper,t2=stamp(exchange.t2),t3=stamp(exchange.t3);
 need(t2<=t3&&span<=BigInt(policy.max_exchange_span_ns)*ppmScale,'source_reference_span');
 const lower=floor(t3-BigInt(error.t3_error_ns)*ntpFraction,ntpFraction);
 const upper=ceil((t2+BigInt(error.t2_error_ns)*ntpFraction)*ppmScale+span*ntpFraction,ppmScale*ntpFraction);
 need(lower<=upper&&same(anchor.utc_at_anchor,{lower_ns:String(lower),upper_ns:String(upper)}),'source_reference_anchor');
 const interval=tick=>{
  const bounds=elapsed(tick-m4),anchorAge=ceil(bounds.upper,ppmScale),exchangeAge=ceil(elapsed(tick-m1).upper,ppmScale);
  const lo=lower+floor(bounds.lower,ppmScale),hi=upper+anchorAge;
  need(anchorAge<BigInt(policy.max_anchor_age_ns)&&exchangeAge<BigInt(policy.max_exchange_age_ns)&&
   BigInt(error.upstream_age_upper_at_m1_ns)+exchangeAge<BigInt(policy.max_upstream_age_ns)&&hi-lo<=2n*BigInt(policy.max_half_width_ns),'source_reference_bounds');
  return {lower_ns:String(lo),upper_ns:String(hi)};
 };
 return {lower_ns:interval(before).lower_ns,upper_ns:interval(after).upper_ns};
}

function sourceReferenceOriginal(evidence,bundle){
 need(Buffer.byteLength(canonical(bundle))<=32768,'source_reference_capacity');
 const original=timeReference(bundle,'bundle_id','bundle_digest');
 need(same(evidence.reference_context_ref,original)&&same(evidence.elapsed_profile_ref,bundle.anchor.binding.profile_ref),'source_reference_integrity');
 need(same(evidence.utc_interval,referenceInterval(bundle,evidence.capture_domain,clockInteger(evidence.capture_start_tick_ns),
  clockInteger(evidence.capture_end_tick_ns))),'source_reference_interval');
 return bundle;
}

function receiverReferenceOriginal(scope,observation,reference){
 const {service_boot_id,valid_before_tick_ns,bundle,state}=reference;
 const {runtime_domain_ref,received_tick_ns}=observation,tick=clockInteger(received_tick_ns);
 const {anchor,source,approval,profile,assessment}=bundle,{binding}=anchor;
 const bundleRef=timeReference(bundle,'bundle_id','bundle_digest');
 const anchorRef=timeReference(anchor,'anchor_id','anchor_digest');
 const approvalRef=timeReference(approval,'approval_id','approval_digest');
 const sourceRef=timeReference(source,'source_id','source_digest');
 need(Buffer.byteLength(canonical(reference))<=32768&&service_boot_id===observation.receiver_boot_id&&
  clockInteger(assessment.assessed_tick_ns)>=clockInteger(anchor.anchor_tick_ns)&&clockInteger(assessment.assessed_tick_ns)<=tick&&
  tick<clockInteger(valid_before_tick_ns)&&bundle.bundle_id===anchor.anchor_id&&
  same(bundle.state_ref,timeReference(state,'state_id','state_digest'))&&state.status==='qualified'&&state.invalidation_ref===null&&
  same(state.binding,binding)&&same(state.current_anchor_ref,anchorRef)&&same(binding.source_ref,sourceRef)&&
  same(binding.approval_ref,approvalRef)&&approval.status==='approved'&&approval.authority_service_id===scope.authority_service_id&&
  same(approval.source_ref,sourceRef)&&same(approval.profile_ref,binding.profile_ref),'receiver_reference_binding');
 const domain={...scope,clock_domain_id:runtime_domain_ref.clock_domain_id,descriptor_digest:runtime_domain_ref.descriptor_digest,clock_generation:'1'};
 const interval=referenceInterval(bundle,domain,tick,tick);
 return {bundleRef,interval,domain:binding.domain,qualificationRefs:[approvalRef,approval.qualification_ref,profile.platform_qualification_ref,bundle.error.qualification_ref]};
}

function referenceAge(scope,entry,observation,sourceTime,sourceReference,receiverReference){
 const receiver=receiverReferenceOriginal(scope,observation,receiverReference);
 const {lower_ns:ls,upper_ns:us}=sourceTime.utc_interval,{lower_ns:lr,upper_ns:ur}=receiver.interval;
 if(BigInt(ls)>BigInt(ur))return {reason:'reference_future_conflict'};
 const nonnegative=value=>value>0n?value:0n;
 const lower=nonnegative(BigInt(lr)-BigInt(us)),upper=nonnegative(BigInt(ur)-BigInt(ls));
 const lowerMs=lower/millis,upperMs=ceil(upper,millis);
 if(upperMs>BigInt(Number.MAX_SAFE_INTEGER))return {reason:'reference_age_out_of_range'};
 const {binding}=sourceReference.anchor;
 const qualifications=[binding.approval_ref,sourceReference.approval.qualification_ref,sourceReference.profile.platform_qualification_ref,
  sourceReference.error.qualification_ref,...receiver.qualificationRefs];
 return {reason:'initial_reference_age_observed',fields:{receiver_domain:receiver.domain,basis:'trusted_utc',
  source_time_ref:entry.payload.origin.source_time_ref,
  source_reference_context_ref:sourceTime.reference_context_ref,receiver_reference_context_ref:receiver.bundleRef,first_receive_utc:receiver.interval,
  initial_age_ns:{lower_ns:String(lower),upper_ns:String(upper)},elapsed_profile_ref:receiverReference.bundle.anchor.binding.profile_ref,
  lower_age_ms:Number(lowerMs),upper_age_ms:Number(upperMs),qualification_refs:[...new Map(qualifications.map(value=>[canonical(value),value])).values()]}};
}

function initialEvidence(scope,entry,observation,acquisition,epoch,sequence,referenceResult=null){
 const {clock_domain_id,descriptor_digest,service_boot_id,runtime_ref}=acquisition;
 // 旧认证域只有 UUID 时保留原观察。此区间来自同一启动态，时钟失信锁存，不在原绑定内换代。
 if(clock_domain_id!=='clock_'+descriptor_digest.slice(7))return null;
 const {origin,origin_digest}=entry.payload;
 const body={id:`age.${epoch}.${sequence}`,revision:'1',origin_ref:{source_key:origin.source_key,event_id:origin.event_id},origin_digest,
  receiver_domain:{...scope,provider_id:scope.authority_service_id,provider_boot_id:service_boot_id,runtime_binding_ref:runtime_ref,
   clock_domain_id,descriptor_digest,clock_generation:'1'},
  first_receive_tick_ns:observation.received_tick_ns,evaluated_tick_ns:observation.received_tick_ns,basis:'unproven',
  elapsed_profile_ref:null,lower_age_ms:null,upper_age_ms:null,qualification_refs:[],
  ...referenceResult?.fields,
  reason_codes:eventDisposition(origin,observation.delivery_mode,null,observation.source_time_evidence,observation.source_reference_bundle,referenceResult).reason_codes};
 const value={...body,digest:digest(body)};
 validateEventTimeModel('AgeEvidence',value);return value;
}

const ageReference=(scope,{id,revision,digest})=>({kind:'age_evidence',scope,object_id:id,revision,digest});
function advancedAge(observation,tick,revision){
 const first=observation.age_evidence,{bundle}=observation.receiver_reference;
 const {lower,upper}=elapsedBounds(bundle.profile,tick-clockInteger(first.first_receive_tick_ns));
 const scale=millis*ppmScale;
 const lowerMs=floor(BigInt(first.initial_age_ns.lower_ns)*ppmScale+lower,scale);
 const upperMs=ceil(BigInt(first.initial_age_ns.upper_ns)*ppmScale+upper,scale);
 if(upperMs>BigInt(Number.MAX_SAFE_INTEGER))return null;
 const {digest:_firstDigest,...fixed}=first,body={...fixed,revision:String(revision),evaluated_tick_ns:String(tick),
  lower_age_ms:Number(lowerMs),upper_age_ms:Number(upperMs),reason_codes:['reference_age_advanced']};
 const evidence={...body,digest:digest(body)};
 validateEventTimeModel('AgeEvidence',evidence);return evidence;
}

export function latestReconcileProjection(db,applyId,key){
 const row=db.prepare(`SELECT body_jcs,body_digest FROM subscription_reconcile_inbox
  WHERE apply_id=? AND json_extract(body_jcs,'$.entry_kind')='entity.upsert'
  AND json_extract(body_jcs,'$.payload.entity_id')=? ORDER BY cursor DESC LIMIT 1`).raw().get(applyId,key.registration_id);
 if(!row)return null;
 const entry=JSON.parse(row[0]);
 need(canonical(entry)===row[0]&&digest(entry)===row[1]&&same(entry.payload.view.source_key,key),'projection_integrity');
 return entry.payload;
}

// 连续性只依赖已摄取的序号与检查点。事件年龄仍由各自的首次观察决定。
export function readReconcileContinuity(db,baseline,projection,bootId){
 const {source_key,binding_ref,contiguous_sequence,highest_seen_sequence,state,gaps}=projection.view;
 if(!['baseline_required','synchronized'].includes(state)||gaps.length||contiguous_sequence!==highest_seen_sequence||
  BigInt(contiguous_sequence)<=BigInt(baseline.reject_floor))return null;
 const {apply_id,snapshot_ref}=baseline.baseline_ref;
 const projectionDigest=digest({entry_kind:'entity.upsert',payload:projection});
 const selected=db.prepare(`SELECT e.cursor,e.body_jcs,e.body_digest,e.observation_jcs,e.observation_digest,
  p.cursor,p.connection_generation,p.receiver_clock_domain_id,e.receiver_clock_domain_id
  FROM subscription_reconcile_inbox e JOIN subscription_reconcile_inbox p
   ON p.apply_id=e.apply_id AND p.connection_generation=e.connection_generation AND p.receiver_boot_id=e.receiver_boot_id
  WHERE e.apply_id=? AND e.receiver_boot_id=? AND e.source_key_digest=? AND e.source_sequence=?
   AND json_extract(p.body_jcs,'$.entry_kind')='entity.upsert' AND json_extract(p.body_jcs,'$.payload.entity_id')=?
   AND p.body_digest=? AND p.cursor>e.cursor AND NOT EXISTS(
    SELECT 1 FROM subscription_reconcile_inbox newer WHERE newer.apply_id=e.apply_id AND newer.source_key_digest=e.source_key_digest AND newer.cursor>e.cursor)
  `).raw().get(apply_id,bootId,digest(source_key),contiguous_sequence,projection.entity_id,projectionDigest);
 if(!selected)return null;
 const [eventCursor,eventText,eventDigest,observationText,observationDigest,projectionCursor,connection_generation,projectionDomain,eventDomain]=selected;
 const event=JSON.parse(eventText),observation=JSON.parse(observationText),origin=event.payload.origin;
 need(canonical(event)===eventText&&digest(event)===eventDigest&&event.entry_kind==='source.event'&&same(origin.source_key,source_key)&&
  origin.source_sequence===contiguous_sequence&&digest(origin)===event.payload.origin_digest&&
  digest(origin.binding_ref)==='sha256:'+binding_ref.content_digest&&eventDomain===projectionDomain&&
  canonical(observation)===observationText&&digest(observation)===observationDigest&&observation.receiver_boot_id===bootId&&
  observation.runtime_domain_ref.clock_domain_id===eventDomain&&['reconcile','live'].includes(observation.delivery_mode),'continuity_integrity');
 const marker={entry_kind:'sync.ready',payload:{baseline_snapshot_id:snapshot_ref.snapshot_id}};
 const checkpoint=db.prepare(`SELECT cursor,body_jcs,body_digest FROM subscription_reconcile_inbox
  WHERE apply_id=? AND receiver_boot_id=? AND connection_generation=? AND receiver_clock_domain_id=?
   AND json_extract(body_jcs,'$.entry_kind')='sync.ready'
   AND (CASE WHEN ?='live' THEN cursor<? ELSE cursor>? END) ORDER BY cursor DESC LIMIT 1`)
  .raw().get(apply_id,bootId,connection_generation,eventDomain,observation.delivery_mode,eventCursor,projectionCursor);
 if(!checkpoint)return null;
 const [checkpointCursor,text,hash]=checkpoint;
 need(canonical(marker)===text&&digest(marker)===hash,'continuity_integrity');
 return {connection_generation,delivery_cursor:String(Math.max(projectionCursor,checkpointCursor)),source_sequence:contiguous_sequence,
  origin_digest:event.payload.origin_digest,projection_digest:projectionDigest,checkpoint_digest:hash};
}

export function reconcileSourceFaults(db,key){
 const rows=db.prepare(`SELECT fault_key,body_jcs,body_digest,apply_id,receiver_boot_id,receiver_clock_domain_id,acquisition_id FROM ingest_source_fault
  WHERE source_key_digest=? ORDER BY fault_key`).raw().all(digest(key));
 return rows.map(([faultKey,text,hash,applyId,boot,domain,acquisition])=>{
  const fault=JSON.parse(text);
  need(canonical(fault)===text&&digest(fault)===hash&&digest(fault.identity)===faultKey&&same(fault.identity.source_key,key)&&
   fault.identity.apply_id===applyId&&fault.observation.receiver_boot_id===boot&&
   fault.observation.runtime_domain_ref.clock_domain_id===domain&&fault.observation.acquisition_ref.id===acquisition,'fault_integrity');
  return fault;
 });
}

export function readReceiveCursor(db,scope){
 const row=db.prepare(`SELECT e.ingest_epoch,coalesce((SELECT max(receive_sequence) FROM subscription_reconcile_inbox
  WHERE ingest_epoch=e.ingest_epoch),0) FROM event_ingest_epoch e WHERE e.singleton=1 AND e.service_id=? AND e.database_instance_id=?`)
  .raw().get(scope.authority_service_id,scope.database_instance_id);
 need(row,'ingest_epoch');
 return {scope:{...scope},ingest_epoch:row[0],receive_sequence:String(row[1])};
}

export function reconcileInbox({db,service,bootId,clock,isOpen,referenceProvider=null,triggerDecisions=null}){
 const available=()=>need(isOpen()&&db.open&&Number(service.schema_revision)>=5,'closed');
 const scope={authority_service_id:service.service_id,database_instance_id:service.database_instance_id};
 const withAgeRevisions=Number(service.schema_revision)>=22;
 const triggerMatches=Number(service.schema_revision)>=23?automationTriggerMatchStore(db,scope):null;
 need(Number(service.schema_revision)<24||triggerDecisions,'trigger_decision_dependencies');
 function receiveCursor(){available();return readReceiveCursor(db,scope);}
 function current(receipt){
  available();
  const row=db.prepare(`SELECT a.receipt_jcs,a.receipt_digest,a.body_jcs,a.body_digest FROM snapshot_ingest_apply a
   JOIN snapshot_projection_head h ON h.apply_id=a.apply_id AND h.slot_key=a.slot_key AND h.generation=a.generation
   JOIN snapshot_ingest_seal s ON s.apply_id=a.apply_id AND s.receipt_digest=a.receipt_digest WHERE a.apply_id=?`).raw().get(receipt.apply_id);
  need(row&&canonical(receipt)===row[0]&&digest(receipt)===row[1],'projection');
  const source=db.prepare(`SELECT r.body_digest FROM ingest_source_revision r
   JOIN ingest_source_head h USING(source_key_digest,revision) WHERE source_key_digest=? AND revision=?`);
  for(const ref of receipt.source_refs){
   need(source.pluck().get(digest(ref.source_key),ref.revision)===ref.digest,'source_changed');
  }
  return row;
 }
 function checkEntry(applyId,entry,projection){
  const payload=entry.payload,origin=entry.entry_kind==='source.event'?payload.origin:null;
  const key=origin?.source_key??payload.view?.source_key;
  const source=projection.records.find(record=>same(record.view.source_key,key));
  need(source,'source_scope');
  const latest=latestReconcileProjection(db,applyId,key)??source;
  const sourceKey=digest(key);
  const previous=db.prepare(`SELECT source_sequence FROM subscription_reconcile_inbox
   WHERE apply_id=? AND source_key_digest=? ORDER BY cursor DESC LIMIT 1`).pluck().get(applyId,sourceKey)??source.view.contiguous_sequence;
  need(latest.view.state!=='closed','source_closed');
  if(origin){
   validateEventTimeModel('Origin',origin);
   const binding=source.view.binding_ref,original=origin.binding_ref;
   need(digest(origin)===payload.origin_digest&&binding.entity_id===original.registration_id&&binding.revision===original.revision&&
    binding.content_digest===digest(original).slice(7),'origin');
   const expected=BigInt(previous)+1n,observed=BigInt(origin.source_sequence);
   const byId=sourceEvent(key,origin.event_id),indexedId=db.prepare(`SELECT event_id FROM subscription_reconcile_inbox
    WHERE source_key_digest=? AND source_sequence=?`).pluck().get(sourceKey,origin.source_sequence);
   if(byId||indexedId){
    need(byId?.origin_digest!==payload.origin_digest,'event_identity');
    return {fault:'event_identity',expected:String(expected),projectionRevision:latest.entity_revision};
   }
   if(observed>expected)return {fault:'source_gap',expected:String(expected),projectionRevision:latest.entity_revision};
   need(observed===expected,'origin');
   return {key:sourceKey,sequence:origin.source_sequence,event:origin.event_id};
  }
  const view=payload.view;
  const gaps=new Set(view.gaps.map(gap=>canonical(gap)));
  need(entry.entry_kind==='entity.upsert'&&payload.entity_kind==='sources'&&payload.entity_id===source.entity_id&&
   payload.entity_epoch===source.entity_epoch&&BigInt(payload.entity_revision)>BigInt(latest.entity_revision)&&
   same(view.binding_ref,source.view.binding_ref)&&same(view.baseline_ref,source.view.baseline_ref)&&
   view.contiguous_sequence===previous&&BigInt(view.highest_seen_sequence)>=BigInt(previous)&&
   BigInt(view.highest_seen_sequence)>=BigInt(latest.view.highest_seen_sequence)&&
   protection[view.state]>=protection[latest.view.state]&&latest.view.gaps.every(gap=>gaps.has(canonical(gap))),'projection_update');
  return null;
 }
 function checkObservation(observation){
  const {receiver_boot_id,runtime_domain_ref,acquisition_ref,received_tick_ns}=observation;
  need(runtime_domain_ref&&acquisition_ref,'clock_acquisition');
  const row=db.prepare(`SELECT body FROM authentication_clock_acquisition
   WHERE service_boot_id=? AND clock_domain_id=? AND id=? AND revision=? AND digest=?`).pluck()
   .get(receiver_boot_id,runtime_domain_ref.clock_domain_id,acquisition_ref.id,acquisition_ref.revision,acquisition_ref.digest);
  need(row,'clock_acquisition');
  const raw=row.toString('utf8'), acquisition=checked(JSON.parse(raw));
  const received=clockInteger(received_tick_ns);
  need(canonical(acquisition)===raw&&same(ref(acquisition),acquisition_ref)&&acquisition.service_id===service.service_id&&
   acquisition.service_boot_id===receiver_boot_id&&acquisition.clock_domain_id===runtime_domain_ref.clock_domain_id&&
   acquisition.descriptor_digest===runtime_domain_ref.descriptor_digest&&received>=clockInteger(acquisition.b)&&
   received<clockInteger(acquisition.valid_before_tick_ns),'clock_acquisition');
  return acquisition;
 }
 function entryAt(applyId,cursor){
  const row=db.prepare(`SELECT body_jcs,body_digest,observation_jcs,observation_digest,
   receiver_boot_id,receiver_clock_domain_id,acquisition_id,connection_generation,ingest_epoch,receive_sequence
   FROM subscription_reconcile_inbox WHERE apply_id=? AND cursor=?`)
   .raw().get(applyId,cursor);
  if(!row)return null;
  const [body,hash,rawObservation,observationHash,boot,domain,acquisitionId,generation,epoch,sequence]=row;
  const entry=JSON.parse(body),observation=JSON.parse(rawObservation);
  const receiverQualified=observation.format==='leafseamer-reconcile-observation/6';
  need(canonical(entry)===body&&digest(entry)===hash&&canonical(observation)===rawObservation&&
   digest(observation)===observationHash&&/^leafseamer-reconcile-observation\/[1-6]$/.test(observation.format)&&
   (receiverQualified?['unproven','trusted_utc'].includes(observation.age_basis):observation.age_basis==='unproven')&&
   receiverQualified===Object.hasOwn(observation,'receiver_reference')&&
   observation.receiver_boot_id===boot&&observation.runtime_domain_ref?.clock_domain_id===domain&&
   observation.acquisition_ref?.id===acquisitionId&&['reconcile','live'].includes(observation.delivery_mode),'integrity');
  if(observation.format==='leafseamer-reconcile-observation/2')need(entry.entry_kind==='source.event'&&
   same(observation.disposition,eventDisposition(entry.payload.origin,observation.delivery_mode)),'disposition_integrity');
  const acquisition=checkObservation(observation);
  if(/^leafseamer-reconcile-observation\/[3-6]$/.test(observation.format)){
   need(entry.entry_kind==='source.event'&&epoch&&Number.isSafeInteger(sequence),'disposition_integrity');
   let sourceTime=null,sourceReference=null;
   if(observation.format!=='leafseamer-reconcile-observation/3')sourceTime=sourceTimeOriginal(entry.payload.origin,observation.source_time_evidence);
   else need(!Object.hasOwn(observation,'source_time_evidence'),'disposition_integrity');
   if(receiverQualified||observation.format==='leafseamer-reconcile-observation/5')sourceReference=sourceReferenceOriginal(sourceTime,observation.source_reference_bundle);
   else need(!Object.hasOwn(observation,'source_reference_bundle'),'disposition_integrity');
   const referenceResult=receiverQualified?referenceAge(scope,entry,observation,sourceTime,sourceReference,observation.receiver_reference):null;
   const expected=initialEvidence(scope,entry,observation,acquisition,epoch,sequence,referenceResult);
   need(expected&&expected.basis===observation.age_basis&&same(observation.age_evidence,expected)&&same(observation.disposition,
    eventDisposition(entry.payload.origin,observation.delivery_mode,ageReference(scope,expected),sourceTime,sourceReference,referenceResult)),'disposition_integrity');
  }
  return {entry,hash,observation,observationHash,generation,epoch,sequence};
 }
 function confirmedEntry(receipt,cursor,hash){
  current(receipt);
  const stored=entryAt(receipt.apply_id,cursor);
  return stored?.hash==='sha256:'+hash;
 }
 function hasCheckpoint(receipt,generation,cursor){
  const checkpoint=db.prepare(`SELECT cursor FROM subscription_reconcile_inbox WHERE apply_id=? AND connection_generation=? AND cursor<=?
   AND json_extract(body_jcs,'$.entry_kind')='sync.ready' ORDER BY cursor DESC LIMIT 1`).pluck().get(receipt.apply_id,generation,cursor);
  const marker={entry_kind:'sync.ready',payload:{baseline_snapshot_id:receipt.snapshot_ref.snapshot_id}};
  return checkpoint!==undefined&&entryAt(receipt.apply_id,checkpoint)?.hash===digest(marker);
 }
 function sourceEvent(key,eventId){
  available();
  const identity=db.prepare(`SELECT apply_id,cursor,source_sequence FROM subscription_reconcile_inbox
   WHERE source_key_digest=? AND event_id=?`).raw().get(digest(key),eventId);
  if(!identity)return null;
  const [applyId,cursor,sequence]=identity,stored=entryAt(applyId,cursor);
  need(stored?.entry.entry_kind==='source.event','event_integrity');
  const {origin,origin_digest}=stored.entry.payload;
  validateEventTimeModel('Origin',origin);
  need(same(origin.source_key,key)&&origin.event_id===eventId&&origin.source_sequence===sequence&&digest(origin)===origin_digest,'event_integrity');
  const frontier=receiveCursor();
  need(stored.epoch===frontier.ingest_epoch&&Number.isSafeInteger(stored.sequence)&&stored.sequence>=1&&
   BigInt(stored.sequence)<=BigInt(frontier.receive_sequence),'event_integrity');
  const receive_cursor={scope:{...scope},ingest_epoch:stored.epoch,receive_sequence:String(stored.sequence)};
  const match=triggerMatches?.read({origin,origin_digest},receive_cursor)??null;
  return {origin,origin_digest,first_observation:stored.observation,first_observation_digest:stored.observationHash,
   first_delivery:{apply_id:applyId,cursor:String(cursor),connection_generation:stored.generation},
   receive_cursor,...(triggerMatches&&{trigger_match_set:match}),...(triggerDecisions&&{trigger_decision_set:match?triggerDecisions.read(match,stored.observationHash):null})};
 }
 function readAgeRevision(observation,row){
  if(!row)return null;
  const [revision,acquisitionId,padded,firstDigest,text,hash]=row;
  const evidence=JSON.parse(text),tick=clockInteger(evidence.evaluated_tick_ns);
  const acquisition=db.prepare('SELECT revision,digest FROM authentication_clock_acquisition WHERE id=?').raw().get(acquisitionId);
  need(acquisition&&tick>clockInteger(observation.received_tick_ns)&&padded===String(tick).padStart(20,'0')&&
   firstDigest===observation.age_evidence.digest&&canonical(evidence)===text&&evidence.digest===hash&&
   same(evidence,advancedAge(observation,tick,revision)),'age_revision_integrity');
  checkObservation({...observation,received_tick_ns:String(tick),acquisition_ref:{id:acquisitionId,revision:acquisition[0],digest:acquisition[1]}});
  return evidence;
 }
 const ageRevisionColumns='revision,acquisition_id,evaluated_tick,first_digest,body_jcs,body_digest';
 function appendAge(event,sample,tick){
  const {first_observation:observation,first_delivery:{apply_id,cursor}}=event,first=observation.age_evidence;
  const prior=db.prepare(`SELECT ${ageRevisionColumns} FROM event_age_revision WHERE apply_id=? AND cursor=? ORDER BY revision DESC LIMIT 1`).raw().get(apply_id,cursor);
  const previous=readAgeRevision(observation,prior)??first,previousTick=clockInteger(previous.evaluated_tick_ns);
  need(tick>=previousTick,'age_revision_order');
  if(tick===previousTick)return previous;
  const revision=Number(previous.revision)+1,age=advancedAge(observation,tick,revision);
  if(!age)return null;
  need(revision<=65536&&db.prepare('SELECT count(*) FROM event_age_revision').pluck().get()<65536,'age_revision_capacity');
  checkObservation({...observation,received_tick_ns:String(tick),acquisition_ref:sample.acquisition_ref});
  need(db.prepare('INSERT INTO event_age_revision VALUES(?,?,?,?,?,?,?,?,?)').run(apply_id,cursor,revision,sample.acquisition_ref.id,
   String(tick).padStart(20,'0'),first.digest,previous.digest,canonical(age),age.digest).changes===1,'age_revision_storage');
  return age;
 }
 function currentReceiverSample(observation){
  const {runtime_domain_ref,received_tick_ns,receiver_reference}=observation;
  try{
   const sample=clock.pairingSample(),tick=clockInteger(sample.tick_ns);
   if(!same(runtime_domain_ref,sample.runtime_domain_ref)||tick<clockInteger(received_tick_ns)||
    tick>=clockInteger(receiver_reference.valid_before_tick_ns)||referenceProvider.isCurrent({first_tick_ns:received_tick_ns,
     tick_ns:sample.tick_ns,reference:structuredClone(receiver_reference)})!==true)return null;
   return {sample,tick};
  }catch{return null;}
 }
 function livePredecessor(receipt,frame,origin,prepared){
  if(!prepared)return {state:'unavailable',reason:'source_predecessor_unavailable'};
  const {transport,readSourceInTransaction}=prepared,{observation:ack}=transport;
  const source=readSourceInTransaction(origin.source_key),continuity=source?.continuity;
  if(source?.state!=='synchronized'||source.baseline_ref.apply_id!==receipt.apply_id||source.gaps.length!==0||
   !continuity||continuity.connection_generation!==frame.binding.connection_generation||
   BigInt(source.contiguous_sequence)+1n!==BigInt(origin.source_sequence)||continuity.source_sequence!==source.contiguous_sequence||
   BigInt(source.reject_floor)>=BigInt(source.contiguous_sequence)||source.highest_seen_sequence!==source.contiguous_sequence||
   source.binding_ref.content_digest!==digest(origin.binding_ref).slice(7))return {state:'unavailable',reason:'source_predecessor_changed'};
  if(ack.state!=='ready'||ack.connection_generation!==continuity.connection_generation||
   BigInt(ack.ack_cursor)<BigInt(continuity.delivery_cursor)||BigInt(ack.ack_cursor)>BigInt(frame.previous_cursor))
   return {state:'unavailable',reason:'source_predecessor_unconfirmed'};
  try{transport.assertCurrentInTransaction();}catch{return {state:'unavailable',reason:'source_transport_changed'};}
  return {state:'confirmed',source_revision:source.revision,source_state_digest:digest(source),baseline_ref:source.baseline_ref,
   previous_source_sequence:continuity.source_sequence,previous_origin_digest:continuity.origin_digest,
   previous_delivery_cursor:continuity.delivery_cursor,connection_generation:continuity.connection_generation,
   ack_cursor:ack.ack_cursor,ack_payload_digest:ack.ack_payload_digest};
 }
 function prepareAge(request,{readSourceInTransaction,prepareSourceTransport}){
  available();need(withAgeRevisions&&!db.inTransaction,'age_preparation');
  const input=JSON.parse(canonical(request)),{source_key,event_id,origin_digest,first_age_ref,policy}=input;
  need(policy&&Number.isSafeInteger(policy.max_age_ms)&&policy.max_age_ms>=0&&
   ['event_occurrence','sampled_observation'].includes(policy.capture_semantics),'age_policy');
  const fixed=sourceEvent(source_key,event_id);
  const observed=(reason,age_evidence_ref=null)=>({state:'observed_only',reason,age_evidence_ref});
  need(fixed&&fixed.origin_digest===origin_digest&&same(fixed.first_observation.disposition?.age_evidence_ref,first_age_ref),'age_origin');
  return (async()=>{
   const {first_observation:observation,first_delivery:delivery}=fixed;
   const {receiver_boot_id}=observation;
   let transport=null;
   if(observation.delivery_mode==='live'&&observation.age_basis==='trusted_utc'&&referenceProvider&&receiver_boot_id===bootId&&
    observation.source_time_evidence.capture_semantics===policy.capture_semantics){
    try{transport=await prepareSourceTransport(delivery.apply_id);}catch{ /* 查原失败时拒绝推进年龄。 */ }
   }
   function evaluateInTransaction(){
    available();need(db.inTransaction,'age_transaction');
    if(observation.delivery_mode!=='live')return observed('reconciled_delivery');
    if(observation.age_basis!=='trusted_utc'||!referenceProvider)return observed('reference_age_unproven');
    if(receiver_boot_id!==bootId)return observed('receiver_boot_changed');
    if(observation.source_time_evidence.capture_semantics!==policy.capture_semantics)return observed('capture_semantics_changed');
    if(transport?.observation.state!=='ready'||transport.observation.connection_generation!==delivery.connection_generation||
     BigInt(transport.observation.ack_cursor)<BigInt(delivery.cursor))return observed('source_transport_unavailable');
    const event=sourceEvent(source_key,event_id);
    need(event&&event.origin_digest===origin_digest&&event.first_observation_digest===fixed.first_observation_digest,'age_origin_changed');
    const source=readSourceInTransaction(source_key);
    if(source?.state!=='synchronized'||source.baseline_ref.apply_id!==delivery.apply_id||source.gaps.length||
     BigInt(source.reject_floor)>=BigInt(event.origin.source_sequence)||BigInt(source.contiguous_sequence)<BigInt(event.origin.source_sequence))return observed('source_not_current');
    try{transport.assertCurrentInTransaction();}catch{return observed('source_transport_changed');}
    const clock=currentReceiverSample(observation);
    if(!clock)return observed('receiver_reference_unavailable');
    const age=appendAge(event,clock.sample,clock.tick);
    if(!age)return observed('reference_age_out_of_range');
    const reference=ageReference(scope,age);
    return age.upper_age_ms<=policy.max_age_ms?{state:'eligible',age_evidence_ref:reference}:observed('age_limit_exceeded',reference);
   }
   return Object.freeze({evaluateInTransaction,commit(){
    need(!db.inTransaction,'age_transaction');return db.transaction(evaluateInTransaction).immediate();
   }});
  })();
 }
 function ageEvidenceForPublisher(reference,publisher){
  available();
  const identity=/^age\.([0-9a-f-]{36})\.([1-9][0-9]{0,4})$/.exec(reference.id);
  if(!identity||typeof reference.revision!=='string'||!/^[1-9][0-9]{0,4}$/.test(reference.revision)||
   Number(reference.revision)>65536||reference.revision!=='1'&&!withAgeRevisions)return null;
  return db.transaction(()=>{
   const row=db.prepare(`SELECT i.apply_id,i.cursor,a.receipt_jcs,a.receipt_digest FROM subscription_reconcile_inbox i
    JOIN snapshot_ingest_apply a USING(apply_id) JOIN snapshot_ingest_seal s ON s.apply_id=a.apply_id AND s.receipt_digest=a.receipt_digest
    WHERE i.ingest_epoch=? AND i.receive_sequence=?`).raw().get(identity[1],Number(identity[2]));
   if(!row)return null;
   const [applyId,cursor,text,hash]=row,receipt=JSON.parse(text);
   need(canonical(receipt)===text&&digest(receipt)===hash&&receipt.apply_id===applyId&&same(receipt.scope,scope),'projection');
   if(receipt.owner.pairing_id!==publisher.binding_id||receipt.delivery_binding.publisher_service_id!==publisher.service_id||
    receipt.delivery_binding.publisher_database_instance_id!==publisher.database_instance_id)return null;
   const stored=entryAt(applyId,cursor),age=reference.revision==='1'?stored.observation.age_evidence:
    readAgeRevision(stored.observation,db.prepare(`SELECT ${ageRevisionColumns} FROM event_age_revision WHERE apply_id=? AND cursor=? AND revision=?`)
     .raw().get(applyId,cursor,reference.revision));
   if(!age||age.id!==reference.id||age.revision!==reference.revision||age.digest!==reference.digest)return null;
   const key=age.origin_ref.source_key;
   need(key.authority_service_id===publisher.service_id&&key.database_instance_id===publisher.database_instance_id,'projection');
   return age;
  }).immediate();
 }
 function saveFault(receipt,frame,source,observation){
  const {origin,origin_digest}=frame.entry.payload;
  const identity={apply_id:receipt.apply_id,cursor:frame.cursor,source_key:origin.source_key,reason:source.fault,
   publisher_projection_revision:source.projectionRevision,expected_sequence:source.expected,
   event_id:origin.event_id,source_sequence:origin.source_sequence,origin_digest};
  const key=digest(identity),sourceDigest=digest(origin.source_key);
  if(db.prepare('SELECT 1 FROM ingest_source_fault WHERE fault_key=?').pluck().get(key))return;
  need(db.prepare('SELECT count(*) FROM ingest_source_fault').pluck().get()<4096&&
   db.prepare('SELECT count(*) FROM ingest_source_fault WHERE source_key_digest=?').pluck().get(sourceDigest)<256,'fault_capacity');
  const gap=source.fault==='source_gap'?{gap_id:key.slice(7),loss_extent:'known',first_missing:source.expected,
   last_missing:String(BigInt(origin.source_sequence)-1n),resolution_state:'open'}:null;
  const fault={format:'leafseamer-reconcile-source-fault/1',scope:receipt.scope,identity,gap,
   connection_generation:frame.binding.connection_generation,observation};
  const inserted=db.prepare('INSERT INTO ingest_source_fault VALUES(?,?,?,?,?,?,?,?)').run(key,sourceDigest,receipt.apply_id,canonical(fault),digest(fault),
   observation.receiver_boot_id,observation.runtime_domain_ref.clock_domain_id,observation.acquisition_ref.id);
  need(inserted.changes===1,'fault_storage');
 }
 function readDelivery(receipt,frame){
   const checkpoint=frame.type==='server.sync.ready';
   const entry=checkpoint?{entry_kind:'sync.ready',payload:{baseline_snapshot_id:frame.baseline_snapshot_id}}:frame.entry;
   need((checkpoint?frame.baseline_snapshot_id===receipt.snapshot_ref.snapshot_id:frame.type==='server.data'&&entry.entry_kind!=='sync.ready')&&
    (frame.delivery_mode==='reconcile'||(!checkpoint&&frame.delivery_mode==='live'))&&same(stripGeneration(frame.binding),stripGeneration(receipt.delivery_binding))&&
    typeof frame.cursor==='string'&&/^[1-9][0-9]{0,4}$/.test(frame.cursor)&&frame.previous_cursor===String(Number(frame.cursor)-1)&&
    digest(entry).slice(7)===frame.payload_digest,'binding');
   return {checkpoint,entry,body:canonical(entry),hash:'sha256:'+frame.payload_digest};
 }
 function accept(receipt,frame,checkInTransaction,first,{checkpoint,entry,body,hash},sourceTime=null,sourceReference=null,receiverReference=null,prepared=null){
   need(Number(service.schema_revision)<26||!db.inTransaction,'execution_admission_transaction');
   const committed=db.transaction(()=>{
    let recheckLive=null,afterCommit=null;
    checkInTransaction();
    const row=current(receipt);
    if(frame.delivery_mode==='live')need(hasCheckpoint(receipt,frame.binding.connection_generation,frame.previous_cursor),'live_without_checkpoint');
    const existing=entryAt(receipt.apply_id,frame.cursor);
    if(existing){
     need(canonical(existing.entry)===body&&existing.hash===hash,'replay_conflict');
     return;
    }
    const lastCursor=db.prepare('SELECT coalesce(max(cursor),0) FROM subscription_reconcile_inbox WHERE apply_id=?').pluck().get(receipt.apply_id);
    need(Number(frame.cursor)===lastCursor+1,'cursor');
    const adopted=JSON.parse(row[2]);
    need(canonical(adopted)===row[2]&&digest(adopted)===row[3],'integrity');
    const source=checkpoint?null:checkEntry(receipt.apply_id,entry,adopted.projection);
    const event=source&&!source.fault;
    const observation={format:event?'leafseamer-reconcile-observation/2':'leafseamer-reconcile-observation/1',receiver_boot_id:bootId,
     runtime_domain_ref:first.runtime_domain_ref,acquisition_ref:first.acquisition_ref,received_tick_ns:first.tick_ns,
     delivery_mode:frame.delivery_mode,age_basis:'unproven',
     ...(event&&{disposition:eventDisposition(entry.payload.origin,frame.delivery_mode)})};
    const acquisition=checkObservation(observation);
    if(source?.fault)saveFault(receipt,frame,source,observation);
    else{
     need(db.prepare('SELECT count(*) FROM subscription_reconcile_inbox').pluck().get()<65537,'capacity');
     const frontier=source?receiveCursor():null;
     const sequence=frontier===null?null:Number(frontier.receive_sequence)+1;
     if(event){
      if(sourceTime)observation.source_time_evidence=sourceTime;
      if(sourceReference)observation.source_reference_bundle=sourceReference;
      let referenceResult=null,age=null;
      if(sourceReference&&receiverReference){
       try{
        const now=clock.pairingSample();
        if(same(first.runtime_domain_ref,now.runtime_domain_ref)&&clockInteger(now.tick_ns)>=clockInteger(first.tick_ns)&&
         clockInteger(now.tick_ns)<clockInteger(receiverReference.valid_before_tick_ns)&&
         referenceProvider.isCurrent({first_tick_ns:first.tick_ns,tick_ns:now.tick_ns,reference:JSON.parse(canonical(receiverReference))})===true){
         referenceResult=referenceAge(scope,entry,observation,sourceTime,sourceReference,receiverReference);
         age=initialEvidence(scope,entry,observation,acquisition,frontier.ingest_epoch,sequence,referenceResult);
        }
       }catch{referenceResult=null;}
      }
      if(!age){
       referenceResult=null;
       age=initialEvidence(scope,entry,observation,acquisition,frontier.ingest_epoch,sequence);
      }
      if(age){
       observation.format=referenceResult?'leafseamer-reconcile-observation/6':sourceReference?'leafseamer-reconcile-observation/5':sourceTime?'leafseamer-reconcile-observation/4':'leafseamer-reconcile-observation/3';
       if(referenceResult)observation.receiver_reference=receiverReference;
       observation.age_basis=age.basis;
       observation.age_evidence=age;
       observation.disposition=eventDisposition(entry.payload.origin,frame.delivery_mode,ageReference(scope,age),sourceTime,sourceReference,referenceResult);
      }else{
       delete observation.source_time_evidence;
       delete observation.source_reference_bundle;
      }
     }
     // 当前事件尚未入库，连续性必须由已确认的前驱证明，不能借用随后才会发送的 ACK。
     const predecessor=event&&triggerDecisions&&frame.delivery_mode==='live'&&observation.age_basis==='trusted_utc'?
      livePredecessor(receipt,frame,entry.payload.origin,prepared):null;
     const inserted=db.prepare('INSERT INTO subscription_reconcile_inbox VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)')
      .run(receipt.apply_id,frame.cursor,frame.binding.connection_generation,body,hash,source?.key??null,source?.sequence??null,source?.event??null,
       canonical(observation),digest(observation),bootId,first.runtime_domain_ref.clock_domain_id,first.acquisition_ref.id,
       frontier?.ingest_epoch??null,sequence);
     need(inserted.changes===1,'storage');
     if(event&&triggerMatches){
      const match=triggerMatches.record(entry.payload,{scope,ingest_epoch:frontier.ingest_epoch,receive_sequence:String(sequence)});
      let liveEvidence=predecessor;
      if(predecessor?.state==='confirmed'){
       const current=currentReceiverSample(observation);
       const advanced=current&&appendAge({first_observation:observation,first_delivery:{apply_id:receipt.apply_id,cursor:frame.cursor}},current.sample,current.tick);
       liveEvidence=advanced?{state:'verified',predecessor,age_evidence_ref:ageReference(scope,advanced),
        upper_age_ms:advanced.upper_age_ms,capture_semantics:sourceTime.capture_semantics}:
        {state:'unavailable',reason:current?'reference_age_out_of_range':'receiver_reference_unavailable'};
       if(advanced)recheckLive=()=>{
        prepared.transport.assertCurrentInTransaction();
        need(currentReceiverSample(observation),'live_reference_changed');
       };
      }
      afterCommit=triggerDecisions?.record(match,observation,liveEvidence,entry.payload.origin,prepared?.programs);
     }
    }
    checkInTransaction();
    recheckLive?.();
    const last=clock.pairingSample();
    need(same(first.runtime_domain_ref,last.runtime_domain_ref)&&BigInt(last.tick_ns)>=BigInt(first.tick_ns),'domain');
    return {failure:source?.fault,afterCommit,last};
   }).immediate();
   need(!committed?.failure,committed?.failure);
   committed?.afterCommit?.(committed.last,()=>clock.pairingSample(),()=>{
    checkInTransaction();
    prepared.transport.assertCurrentInTransaction();
   },checkInTransaction,(cursor,predecessor=null)=>{
    need(db.inTransaction&&same(cursor.scope,scope),'execution_action_origin');
    const stored=entryAt(receipt.apply_id,frame.cursor);
    need(stored?.entry.entry_kind==='source.event'&&stored.hash===hash&&stored.epoch===cursor.ingest_epoch&&
     String(stored.sequence)===cursor.receive_sequence&&stored.observation.delivery_mode==='live','execution_action_origin');
    const {origin,origin_digest}=stored.entry.payload;
    need(digest(origin)===origin_digest,'execution_action_origin');
    if(!predecessor)return {origin,origin_digest};
    const projected=latestReconcileProjection(db,receipt.apply_id,origin.source_key)?.view;
    const projectionPending=Boolean(projected&&['baseline_required','synchronized'].includes(projected.state)&&projected.gaps.length===0&&
     projected.contiguous_sequence===predecessor.previous_source_sequence&&projected.highest_seen_sequence===projected.contiguous_sequence);
    return {origin,origin_digest,projectionPending};
   });
 }
 return Object.freeze({
  sourceEvent,ageEvidenceForPublisher,receiveCursor,...(withAgeRevisions&&{prepareAge}),
  confirmed(receipt,cursor,hash){return confirmedEntry(receipt,cursor,hash);},
  ready(receipt,cursor,hash,generation){
   return confirmedEntry(receipt,cursor,hash)&&hasCheckpoint(receipt,generation,cursor);
  },
  accept(receipt,frame,checkInTransaction){
   accept(receipt,frame,checkInTransaction,clock.pairingSample(),readDelivery(receipt,frame));
  },
  async acceptFromPeer(receipt,frame,checkInTransaction,loadSourceTime,loadReferenceBundle,liveTransport=null){
   const first=clock.pairingSample(),delivery=readDelivery(receipt,frame);
   const origin=delivery.entry.entry_kind==='source.event'?delivery.entry.payload.origin:null;
   let sourceTime=null,sourceReference=null,receiverReference=null,prepared=null;
   if(origin?.source_time_ref&&!entryAt(receipt.apply_id,frame.cursor)){
    validateEventTimeModel('Origin',origin);
    need(digest(origin)===delivery.entry.payload.origin_digest&&origin.source_key.authority_service_id===frame.binding.publisher_service_id&&
     origin.source_key.database_instance_id===frame.binding.publisher_database_instance_id,'source_time_binding');
    if(referenceProvider){
     try{
      const captured=canonical(referenceProvider.readFirst({tick_ns:first.tick_ns,runtime_domain_ref:structuredClone(first.runtime_domain_ref)}));
      if(Buffer.byteLength(captured)<=32768)receiverReference=JSON.parse(captured);
     }catch{ /* 接收参考原件只能在远端查询之前取得。 */ }
    }
    try{sourceTime=sourceTimeOriginal(origin,await loadSourceTime(origin.source_time_ref));}
    catch{ /* 无法核验来源原件时仅保存本机观察。 */ }
    if(sourceTime?.reference_context_ref&&loadReferenceBundle){
     const needTransport=triggerDecisions&&receiverReference&&frame.delivery_mode==='live'&&liveTransport?.readSourceInTransaction;
     const [reference,transport]=await Promise.allSettled([
      Promise.resolve().then(()=>loadReferenceBundle(sourceTime.reference_context_ref)).then(bundle=>sourceReferenceOriginal(sourceTime,bundle)),
      needTransport?Promise.resolve().then(()=>liveTransport.prepareSourceTransport(receipt.apply_id)):null
     ]);
     if(reference.status==='fulfilled')sourceReference=reference.value;
     if(sourceReference&&transport.status==='fulfilled'&&transport.value){
      prepared={transport:transport.value,readSourceInTransaction:liveTransport.readSourceInTransaction};
     }
    }
    if(prepared?.transport.observation.state==='ready')prepared.programs=await triggerDecisions.prepare(
     ()=>triggerMatches.programVersions(origin),()=>triggerMatches.runs(origin));
   }
   accept(receipt,frame,checkInTransaction,first,delivery,sourceTime,sourceReference,receiverReference,prepared);
  }
 });
}
