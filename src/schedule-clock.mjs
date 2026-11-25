import {canonical,digest,same} from './runtime/auth-clock-value.mjs';

const million=1000000n;
const maxTick=(1n<<64n)-1n;
const pad=value=>String(value).padStart(20,'0');
const ceil=(value,divisor)=>(value+divisor-1n)/divisor;
const minimum=(...items)=>items.reduce((a,b)=>a<b?a:b);
const need=(ok,code='schedule_clock_integrity')=>{if(!ok)throw Object.assign(new Error(code),{code});};
const put=result=>need(result.changes===1);
const reference=(id,digest)=>({id,revision:'1',digest});
function original(text,checksum){
 const body=JSON.parse(text);need(canonical(body)===text&&digest(body)===checksum);return body;
}
function firstUpperTick(base,required,rate,bias){
 const numerator=(required-1n)*million-bias;
 return numerator<0n?base:base+numerator/rate+1n;
}
function qualify(sample,scope,boot){
 const {clock,reference:verified}=sample??{};
 if(!clock||!verified||verified.service_boot_id!==boot)return null;
 const {bundle,state}=verified,{anchor,profile,policy,error}=bundle;
 const {clock_domain_id,descriptor_digest,authority_service_id,database_instance_id}=anchor.binding.domain;
 if(!same(clock.domain,{clock_domain_id,descriptor_digest})||!same(scope,{authority_service_id,database_instance_id})||
    !same(state.binding,anchor.binding)||state.status!=='qualified')return null;
 const t=BigInt(clock.tick_ns),m1=BigInt(anchor.exchange_start_tick_ns),m4=BigInt(anchor.anchor_tick_ns);
 const atLower=BigInt(anchor.utc_at_anchor.lower_ns),atUpper=BigInt(anchor.utc_at_anchor.upper_ns);
 const rate=BigInt(profile.elapsed_error_ppm),q=BigInt(profile.span_quantization_ns),bias=q*million;
 if(m1>m4||m4>t||t>maxTick||atLower>atUpper||rate<0n||rate>=million||q<=0n)return null;
 const ageUpper=ceil((t-m4)*(million+rate)+bias,million);
 const ageLower=((t-m4)*(million-rate)-bias)/million;
 const lower=atLower+(ageLower>0n?ageLower:0n),upper=atUpper+ageUpper;
 const ageExpiry=(base,threshold)=>firstUpperTick(base,threshold,million+rate,bias);
 const width=2n*BigInt(policy.max_half_width_ns),remainingWidth=width-(atUpper-atLower)-2n*q;
 // Two directed roundings contribute at most one additional nanosecond to interval width.
 const widthExpiry=remainingWidth<0n?m4:rate===0n?maxTick:firstUpperTick(m4,remainingWidth,2n*rate,0n);
 const before=minimum(BigInt(verified.valid_before_tick_ns),ageExpiry(m4,BigInt(policy.max_anchor_age_ns)),
  ageExpiry(m1,BigInt(policy.max_exchange_age_ns)),
  ageExpiry(m1,BigInt(policy.max_upstream_age_ns)-BigInt(error.upstream_age_upper_at_m1_ns)),widthExpiry);
 if(t>=before||upper-lower>width)return null;
 return {lower,upper,before,
  firstAt:start=>firstUpperTick(m4,start-atUpper,million+rate,bias),
  bundle_ref:reference(bundle.bundle_id,bundle.bundle_digest)};
}

export function scheduleClockStore(db,scope,boot,wasAdmitted){
 function clockRecord(occurrence){
  const row=db.prepare(`SELECT activation_revision,body_jcs,body_digest,window_enter_tick,observed_tick,qualified_before_tick,fault
   FROM schedule_occurrence_clock WHERE occurrence_id=? ORDER BY activation_revision DESC LIMIT 1`).raw().get(occurrence);
  if(!row)return null;
  const [revision,text,checksum,enter,observed,before,fault]=row,body=original(text,checksum);
  need(body.occurrence_id===occurrence&&pad(body.activation_ref.revision)===revision&&same(body.scope,scope)&&BigInt(enter)<=BigInt(body.window_enter_tick_ns));
  return {revision,body,ref:reference(body.boundary_id,checksum),enter:BigInt(enter),observed:BigInt(observed),before:BigInt(before),fault:fault===1};
 }
 function finalize(occurrence,activation,step,prior,clock,disposition,reason){
  const {schedule_id}=activation.body,occurrence_id=occurrence.occurrence_id;
  const body={format:'leafseamer.schedule-clock-stop/1',scope,occurrence_id,schedule_id,activation_ref:activation.ref,
   boundary_ref:prior?.ref??null,step_window_ref:reference(step.step_window_id,step.step_window_digest),
   service_boot_id:boot,clock,disposition,reason};
  put(db.prepare('INSERT INTO schedule_occurrence_stop VALUES(?,?,?,?,?,?)')
   .run(occurrence_id,schedule_id,pad(activation.ref.revision),disposition,canonical(body),digest(body)));
 }
 function firstWindow(occurrence){
  return occurrence.step_windows.reduce((a,b)=>{
   const aStart=BigInt(a.window.not_before_utc_ns),bStart=BigInt(b.window.not_before_utc_ns);
   if(aStart!==bStart)return aStart<bStart?a:b;
   const aEnd=BigInt(a.window.not_after_utc_ns),bEnd=BigInt(b.window.not_after_utc_ns);
   return aEnd<bEnd||aEnd===bEnd&&a.step_id<b.step_id?a:b;
  });
 }
 return Object.freeze({
  advance(activation,version,sample,{replacing=false}={}){
   need(db.inTransaction&&same(activation.body.schedule_version_ref,version.ref));
   const proof=qualify(sample,scope,boot),clock=sample?.clock??null;
   const revision=pad(activation.ref.revision),now=clock?BigInt(clock.tick_ns):null;
   for(const item of version.body.items){
    if(wasAdmitted?.(item.occurrence_id))continue;
    if(db.prepare('SELECT 1 FROM schedule_occurrence_stop WHERE occurrence_id=?').pluck().get(item.occurrence_id))continue;
    // An activation can remove the item from subsequent scans. Keep the old
    // version active if this transaction cannot establish where its window stands.
    need(!replacing||proof,'schedule_replacement_reference');
    const prior=clockRecord(item.occurrence_id),step=firstWindow(item);
    const stop=(state,reason)=>finalize(item,activation,step,prior,clock,state,reason);
    if(prior&&(!clock||prior.body.service_boot_id!==boot||!same(prior.body.domain,clock.domain)||now<prior.observed)){
     stop('blocked','clock_domain_unavailable');continue;
    }
    const start=BigInt(step.window.not_before_utc_ns),end=BigInt(step.window.not_after_utc_ns);
    if(prior&&now>=prior.enter&&(!proof||prior.fault||now>=prior.before)){
     stop('blocked','reference_time_lost');continue;
    }
    if(!proof){
     if(prior&&!prior.fault)put(db.prepare('UPDATE schedule_occurrence_clock SET observed_tick=?,fault=1 WHERE occurrence_id=? AND activation_revision=?')
      .run(pad(now),item.occurrence_id,prior.revision));
     continue;
    }
    const enter=minimum(proof.firstAt(start),maxTick);
    if(prior?.revision===revision){
     if(proof.lower>=end){stop('missed','step_window_elapsed');continue;}
     const fixed=minimum(prior.enter,enter);
     if(now>=fixed&&(prior.fault||now>=prior.before)){stop('blocked','reference_time_lost');continue;}
     if(replacing&&now>=fixed){stop('blocked','previous_window_entered');continue;}
     put(db.prepare(`UPDATE schedule_occurrence_clock SET window_enter_tick=?,observed_tick=?,qualified_before_tick=?,fault=0
       WHERE occurrence_id=? AND activation_revision=?`).run(pad(fixed),pad(now),pad(proof.before),item.occurrence_id,revision));
     continue;
    }
    if(prior&&now>=prior.enter){stop('blocked','previous_window_entered');continue;}
    if(proof.lower>=end){stop('missed','step_window_elapsed');continue;}
    if(proof.upper>=start){stop('blocked','window_entry_unproven');continue;}
    const {schedule_id}=activation.body;
    const body={format:'leafseamer.schedule-clock-boundary/1',scope,schedule_id,occurrence_id:item.occurrence_id,activation_ref:activation.ref,
     boundary_id:item.occurrence_id+'.'+activation.ref.revision,step_window_ref:reference(step.step_window_id,step.step_window_digest),
     service_boot_id:boot,domain:clock.domain,window_enter_tick_ns:String(enter),origin_bundle_ref:proof.bundle_ref};
    put(db.prepare('INSERT INTO schedule_occurrence_clock VALUES(?,?,?,?,?,?,?,?,?)').run(item.occurrence_id,schedule_id,revision,canonical(body),
     digest(body),pad(enter),pad(now),pad(proof.before),0));
   }
  },
  admit(activation,item,sample){
   need(db.inTransaction,'schedule_admission_transaction');
   const {occurrence_id}=item,prior=clockRecord(occurrence_id),step=firstWindow(item);
   const step_window_ref=reference(step.step_window_id,step.step_window_digest);
   const proof=qualify(sample,scope,boot);
   need(proof&&prior&&!prior.fault&&same(prior.body.activation_ref,activation.ref)&&prior.body.service_boot_id===boot&&
    same(prior.body.domain,sample.clock.domain)&&same(prior.body.step_window_ref,step_window_ref)&&
    !db.prepare('SELECT 1 FROM schedule_occurrence_stop WHERE occurrence_id=?').pluck().get(occurrence_id),'schedule_admission_clock');
   const now=BigInt(sample.clock.tick_ns);
   need(now>=prior.observed&&now>=prior.enter&&now<prior.before&&
    proof.lower>=BigInt(step.window.not_before_utc_ns)&&proof.upper<BigInt(step.window.not_after_utc_ns),'schedule_admission_window');
   return {first_step_id:step.step_id,step_window_ref,boundary_ref:prior.ref,clock:sample.clock,reference_bundle_ref:proof.bundle_ref,
    utc_interval:{lower_ns:String(proof.lower),upper_ns:String(proof.upper)}};
  },
  blockBusy(activation,item,time){
   const prior=clockRecord(item.occurrence_id),step=firstWindow(item);
   need(db.inTransaction&&prior&&same(time.boundary_ref,prior.ref)&&same(prior.body.activation_ref,activation.ref));
   finalize(item,activation,step,prior,time.clock,'blocked','run_busy');
  },
  status(occurrenceId){
   const stopped=db.prepare('SELECT body_jcs,body_digest FROM schedule_occurrence_stop WHERE occurrence_id=?').raw().get(occurrenceId);
   if(stopped){const stop=original(...stopped);return {state:stop.disposition,stop};}
   const clock=clockRecord(occurrenceId);
   if(!clock)return {state:'awaiting_reference'};
   return {state:clock.fault||clock.body.service_boot_id!==boot?'reference_unavailable':'tracking',
    boundary_ref:clock.ref,window_enter_tick_ns:String(clock.enter)};
  }
 });
}
