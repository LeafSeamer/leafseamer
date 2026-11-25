import {randomUUID} from 'node:crypto';
import {canonical,digest,same} from './runtime/auth-clock-value.mjs';
import {freeze} from './automation-expression.mjs';
import {scheduleClockStore} from './schedule-clock.mjs';

const need=(ok,code)=>{if(!ok)throw Object.assign(new Error('schedule_'+code),{code:'schedule_'+code});};
const fields=(value,names)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).sort().join(',')===names;
const id=value=>typeof value==='string'&&/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$(?![\s\S])/.test(value);
const revision=value=>typeof value==='string'&&/^[1-9][0-9]{0,15}$(?![\s\S])/.test(value);
const nullableRevision=value=>value===null||revision(value);
const pad=value=>value.padStart(20,'0');
const ref=(id,revision,digest)=>({id,revision,digest});
const put=result=>need(result.changes===1,'storage');
const calendarProfile={format:'leafseamer.calendar-fixed-offset/1',calendar:'iso8601',time_scale:'posix-utc',precision:'milliseconds'};
const calendarRef=freeze(ref('calendar.fixed-offset','1',digest(calendarProfile)));
const limit=86400000;
const windowIndex=item=>item.step_windows.map(({step_id,step_window_digest})=>({step_id,step_window_digest}));

function original(text,checksum){
 const value=JSON.parse(text);
 need(canonical(value)===text&&digest(value)===checksum,'integrity');
 return value;
}
function date(value){
 need(typeof value==='string'&&/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}$(?![\s\S])/.test(value),'local_time');
 const ms=Date.parse(value+'Z');
 need(Number.isSafeInteger(ms)&&new Date(ms).toISOString()===value+'Z','local_time');
 return ms;
}
function resolveOffset(value){
 need(typeof value==='string'&&/^[+-](?:0[0-9]|1[0-4]):[0-5][0-9]$(?![\s\S])/.test(value)&&value!=='-00:00','offset');
 const minutes=Number(value.slice(1,3))*60+Number(value.slice(4));
 need(minutes<=14*60,'offset');
 return value[0]==='-'?-minutes:minutes;
}
function actionSteps(publication){
 const {program}=publication.program;
 const steps=program.kind==='rule'?program.groups.flatMap(group=>group.steps):
  program.transitions.flatMap(transition=>[...transition.exit,...transition.transition,...transition.enter]);
 return new Map(steps.filter(step=>step.kind==='action').map(step=>[step.step_id,step.action_binding_id]));
}
function timestamp(ms){
 const value=new Date(ms).toISOString();
 need(value.length===24,'utc_range');
 return value;
}

export function scheduleStore({db,service,bootId,isOpen,authority,publications}){
 const scope=freeze({authority_service_id:service.service_id,database_instance_id:service.database_instance_id});
 const admissions=Number(service.schema_revision)>=38?db.prepare('SELECT body_jcs,body_digest FROM schedule_execution_admission WHERE occurrence_id=?').raw():null;
 const clock=Number(service.schema_revision)>=37?scheduleClockStore(db,scope,bootId,admissions&&((id)=>Boolean(admissions.get(id)))):null;
 const insertAdmission=admissions&&db.prepare('INSERT INTO schedule_execution_admission VALUES(?,?,?,?,?,?)');
 let cursor='';
 function writer(work){need(isOpen()&&db.open&&!db.inTransaction,'writer');return db.transaction(work).immediate();}
 function authorize(authentication,operation_id,definition_id,context_id){
  const proof=authority.authorizeInTransaction({authentication,operation_id,definition_id,context_id,scope});
  need(proof&&same(proof.scope,scope)&&proof.request_epoch===service.request_epoch&&proof.operation_id===operation_id&&
   proof.definition_id===definition_id&&proof.context_id===context_id&&(context_id===null||proof.actor.principal_kind==='human'),'authorization');
  return proof;
 }
 function readVersion(scheduleId,version=null){
  const row=(version===null?db.prepare('SELECT version,body_jcs,body_digest FROM schedule_version WHERE schedule_id=? ORDER BY version DESC LIMIT 1').raw().get(scheduleId):
   db.prepare('SELECT version,body_jcs,body_digest FROM schedule_version WHERE schedule_id=? AND version=?').raw().get(scheduleId,pad(version)));
  if(!row)return null;
  const [column,text,checksum]=row,body=original(text,checksum);
  need(body.format==='leafseamer.schedule-version/1'&&same(body.scope,scope)&&body.schedule_id===scheduleId&&
   body.version===String(BigInt(column))&&body.plan_digest===digest(body.items)&&same(body.calendar_profile_ref,calendarRef),'integrity');
  return {ref:ref(scheduleId,body.version,checksum),body};
 }
 function readActivation(scheduleId,revision=null){
  const row=(revision===null?db.prepare('SELECT revision,body_jcs,body_digest FROM schedule_activation WHERE schedule_id=? ORDER BY revision DESC LIMIT 1').raw().get(scheduleId):
   db.prepare('SELECT revision,body_jcs,body_digest FROM schedule_activation WHERE schedule_id=? AND revision=?').raw().get(scheduleId,pad(revision)));
  if(!row)return null;
  const [column,text,checksum]=row,body=original(text,checksum),version=readVersion(scheduleId,body.schedule_version_ref.revision);
  need(body.format==='leafseamer.schedule-activation/1'&&same(body.scope,scope)&&body.schedule_id===scheduleId&&
   body.revision===String(BigInt(column))&&same(body.schedule_version_ref,version?.ref),'integrity');
  return {ref:ref(body.activation_id,body.revision,checksum),body};
 }
 function definition(scheduleId){return db.prepare('SELECT definition_id FROM schedule_identity WHERE schedule_id=?').pluck().get(scheduleId);}
 function next(value){
  const result=String(BigInt(value??'0')+1n);need(revision(result),'revision_exhausted');return result;
 }
 function occurrence(scheduleId,itemId,key){
  const logical_key={authority_id:scope.authority_service_id,schedule_id:scheduleId,item_id:itemId,one_shot_key:key};
  const logical_key_digest=digest(logical_key),existing=db.prepare('SELECT one_shot_key,occurrence_id,logical_key_digest FROM schedule_occurrence WHERE schedule_id=? AND item_id=?').raw().get(scheduleId,itemId);
  if(existing){
   need(existing[0]===key&&existing[2]===logical_key_digest,'occurrence_identity');
   return {occurrence_id:existing[1],logical_key_digest};
  }
  const occurrence_id=randomUUID();
  put(db.prepare('INSERT INTO schedule_occurrence VALUES(?,?,?,?,?)').run(scheduleId,itemId,key,occurrence_id,logical_key_digest));
  return {occurrence_id,logical_key_digest};
 }
 function resolveItem(item,actions,scheduleId){
  need(fields(item,'item_id,local_start,offset,one_shot_key,steps')&&id(item.item_id)&&id(item.one_shot_key)&&
   Array.isArray(item.steps)&&item.steps.length===actions.size,'item');
  const nominal=date(item.local_start)-resolveOffset(item.offset)*60000,seen=new Set();
  const windows=item.steps.map(step=>{
   need(fields(step,'latest_start_offset_ms,role,start_offset_ms,step_id')&&actions.has(step.step_id)&&!seen.has(step.step_id)&&
    ['prepare','on_air','follow_up'].includes(step.role),'step');
   seen.add(step.step_id);
   const {start_offset_ms,latest_start_offset_ms}=step;
   need([start_offset_ms,latest_start_offset_ms].every(x=>Number.isSafeInteger(x)&&Math.abs(x)<=limit)&&
    start_offset_ms<latest_start_offset_ms&&(step.role==='prepare'?start_offset_ms<0:start_offset_ms>=0),'window');
   const start=nominal+start_offset_ms,end=nominal+latest_start_offset_ms;
   const body={step_window_id:randomUUID(),...step,action_binding_id:actions.get(step.step_id),
    not_before:timestamp(start),not_after:timestamp(end),
    window:{not_before_utc_ns:String(BigInt(start)*1000000n),not_after_utc_ns:String(BigInt(end)*1000000n)}};
   return {...body,step_window_digest:digest(body)};
  });
  return {...occurrence(scheduleId,item.item_id,item.one_shot_key),item_id:item.item_id,one_shot_key:item.one_shot_key,
   local_start:item.local_start,offset:item.offset,nominal_utc:timestamp(nominal),step_windows:windows};
 }
 function request(authentication,scheduleId,input,operation,definitionId,write){
  need(id(scheduleId)&&id(definitionId)&&fields(input.context,'context_id,idempotency_key')&&id(input.context.context_id)&&id(input.context.idempotency_key),'request');
  const {context:{context_id,idempotency_key},...body}=input;
  const proof=authorize(authentication,operation,definitionId,context_id);
  const key=digest({scope,request_epoch:proof.request_epoch,actor:proof.actor,operation,idempotency_key}),requestDigest=digest({schedule_id:scheduleId,request:body});
  const prior=db.prepare('SELECT request_digest,schedule_id,result_revision,result_digest FROM schedule_request WHERE request_key=?').raw().get(key);
  if(prior){
   need(prior[0]===requestDigest&&prior[1]===scheduleId,'request_conflict');
   const result=(operation==='publishSchedule'?readVersion:readActivation)(scheduleId,String(BigInt(prior[2])));
   need(result?.ref.digest===prior[3]&&same(proof,authorize(authentication,operation,definitionId,context_id)),'integrity');
   return freeze(result);
  }
  authority.assertRunCapacityInTransaction(proof);
  const result=write(proof),{revision:resultRevision,digest:resultDigest}=result.ref;
  put(db.prepare('INSERT INTO schedule_request VALUES(?,?,?,?,?,?,?)').run(key,context_id,operation,requestDigest,scheduleId,pad(resultRevision),resultDigest));
  need(same(proof,authorize(authentication,operation,definitionId,context_id)),'authorization_changed');
  return freeze(result);
 }
 function timeFor(source,sample){
  need(admissions&&db.inTransaction,'admission_transaction');
  const {schedule_id,occurrence_id,activation_ref}=source;
  need(!admissions.get(occurrence_id),'occurrence_claimed');
  const activation=readActivation(schedule_id);
  need(activation&&same(activation.ref,activation_ref),'activation_conflict');
  const version=readVersion(schedule_id,activation.body.schedule_version_ref.revision);
  const item=version.body.items.find(item=>item.occurrence_id===occurrence_id);
  need(item,'admission_occurrence');
  return {activation,version,item,time:clock.admit(activation,item,sample)};
 }
 function openWindow(activation,item,sample){
  try{return clock.admit(activation,item,sample);}
  catch(error){if(['schedule_admission_window','schedule_admission_clock'].includes(error?.code))return null;throw error;}
 }
 return Object.freeze({
  ...(admissions&&{timeForAdmissionInTransaction(source,sample){return freeze(timeFor(source,sample).time);},
   readActionScheduleInTransaction(execution,step){
    need(db.inTransaction&&execution.origin_kind==='schedule'&&same(execution.scope,scope),'admission_transaction');
    const claim=execution.admission_ref,row=admissions.get(claim.object_id);
    need(row&&row[1]===claim.digest,'admission_integrity');
    const admission=original(...row),{schedule_id,occurrence_id}=admission;
    const activated=readActivation(schedule_id,admission.activation_ref.revision),version=readVersion(schedule_id,admission.schedule_version_ref.revision);
    need(activated&&version&&admission.execution_id===execution.execution_id&&admission.run_id===execution.run_id&&
     same(admission.scope,scope)&&same(execution.artifact_ref,admission.artifact_ref)&&same(admission.artifact_ref,version.body.definition_artifact_ref)&&
     same(activated.ref,admission.activation_ref)&&same(activated.body.schedule_version_ref,version.ref)&&
     same(admission.schedule_version_ref,version.ref),'admission_integrity');
    const occurrence=version.body.items.find(item=>item.occurrence_id===occurrence_id);
    const window=occurrence?.step_windows.find(window=>window.step_id===step.step_id);
    need(occurrence&&admission.occurrence_plan_digest===digest(occurrence)&&window&&
     window.action_binding_id===step.payload.binding.action_binding_ref.id,'admission_step_window');
    const window_index=windowIndex(occurrence);
    need(admission.step_window_index_digest===digest(window_index),'admission_integrity');
    return freeze({admission,step_window:window,window_index});
   },
   prepareAdmissionInTransaction(source,run,execution_id,steps,readClock){
   need(db.inTransaction&&typeof readClock==='function','admission_transaction');
   const {schedule_id,occurrence_id,activation_ref}=source;
   const {activation,version,item,time}=timeFor(source,readClock());
   need(same(run.scope,scope)&&run.definition_id===version.body.definition_id&&run.definition_version===version.body.definition_version&&
    same(run.artifact_ref,version.body.definition_artifact_ref),'admission_artifact');
   const byStep=new Map(item.step_windows.map(step=>[step.step_id,step]));
   const first=steps.find(({step})=>step.step_id===time.first_step_id);
   const actions=steps.filter(({step})=>step.kind==='action');
   need(first?.step.kind==='action'&&first.dependencies.length===0&&actions.every(({step,action})=>
    byStep.get(step.step_id)?.action_binding_id===action.action_binding_ref.id),'admission_steps');
   const body=freeze({format:'leafseamer.schedule-admission/1',scope,service_boot_id:bootId,occurrence_id,schedule_id,execution_id,
    logical_key_digest:item.logical_key_digest,occurrence_plan_digest:digest(item),step_window_index_digest:digest(windowIndex(item)),
    run_id:run.run_id,artifact_ref:run.artifact_ref,activation_ref:activation.ref,
    schedule_version_ref:version.ref,...time});
   const text=canonical(body),checksum=digest(body);
   need(Buffer.byteLength(text)<=8192,'admission_capacity');
   return Object.freeze({body,text,checksum,sealInTransaction(){
    const current=clock.admit(activation,item,readClock());
    need(BigInt(current.clock.tick_ns)>=BigInt(body.clock.tick_ns)&&same(current.boundary_ref,body.boundary_ref),'admission_clock_changed');
    put(insertAdmission.run(occurrence_id,execution_id,schedule_id,pad(activation.ref.revision),text,checksum));
   }});
  }}),
  ...(clock&&{reconcileClock(readClock,isBusy=null){
   need(typeof readClock==='function','clock_reader');
   const result=writer(()=>{
    const rows=db.prepare('SELECT DISTINCT schedule_id FROM schedule_activation WHERE schedule_id>? ORDER BY schedule_id COLLATE BINARY LIMIT 8').pluck().all(cursor);
    for(const id of rows){
     const activation=readActivation(id),version=readVersion(id,activation.body.schedule_version_ref.revision);
     const sample=readClock();
     clock.advance(activation,version,sample);
     if(admissions&&sample&&isBusy?.(version.body.definition_id)){
      for(const item of version.body.items){
       if(admissions.get(item.occurrence_id))continue;
       const time=openWindow(activation,item,sample);
       if(time)clock.blockBusy(activation,item,time);
      }
     }
    }
    return rows;
   });
   cursor=result.length===8?result.at(-1):'';
  }}),
  publish(authentication,scheduleId,value){
   const input=structuredClone(value);
   need(fields(input,'context,definition_id,definition_version,expected_artifact_ref,expected_version,items')&&revision(input.definition_version)&&
    nullableRevision(input.expected_version)&&Array.isArray(input.items)&&input.items.length>0&&input.items.length<=256,'request');
   return writer(()=>request(authentication,scheduleId,input,'publishSchedule',input.definition_id,proof=>{
    const prior=readVersion(scheduleId),bound=definition(scheduleId);
    need((prior?.body.version??null)===input.expected_version&&(!bound||bound===input.definition_id),'version_conflict');
    const publication=publications.versionForRunInTransaction(input.definition_id,input.definition_version),artifact=publication.result.artifact_ref;
    need(same(artifact,input.expected_artifact_ref),'artifact');
    const actions=actionSteps(publication);
    need(actions.size>0&&actions.size<=256&&actions.size*input.items.length<=2048&&
     new Set(input.items.map(item=>item?.item_id)).size===input.items.length,'capacity');
    if(!bound)put(db.prepare('INSERT INTO schedule_identity VALUES(?,?)').run(scheduleId,input.definition_id));
    const items=input.items.map(item=>resolveItem(item,actions,scheduleId)),version=next(prior?.body.version);
    const body={format:'leafseamer.schedule-version/1',scope,schedule_id:scheduleId,version,definition_id:input.definition_id,
     definition_version:input.definition_version,definition_artifact_ref:artifact,calendar_profile_ref:calendarRef,items,plan_digest:digest(items),authorization:proof};
    const text=canonical(body),checksum=digest(body);
    need(Buffer.byteLength(text)<=1048576,'capacity');
    put(db.prepare('INSERT INTO schedule_version VALUES(?,?,?,?,?,?)').run(scheduleId,pad(version),input.definition_id,input.definition_version,text,checksum));
    return {body,ref:ref(scheduleId,version,checksum)};
   }));
  },
  activate(authentication,scheduleId,value,readClock=null){
   const input=structuredClone(value);
   need(fields(input,'context,expected_revision,schedule_version_ref')&&nullableRevision(input.expected_revision)&&id(scheduleId)&&
    fields(input.schedule_version_ref,'digest,id,revision')&&input.schedule_version_ref.id===scheduleId&&revision(input.schedule_version_ref.revision),'request');
   return writer(()=>request(authentication,scheduleId,input,'activateSchedule',definition(scheduleId),proof=>{
    const version=readVersion(scheduleId,input.schedule_version_ref.revision),prior=readActivation(scheduleId);
    need((prior?.body.revision??null)===input.expected_revision,'activation_conflict');
    need(version&&same(version.ref,input.schedule_version_ref),'version');
    const sample=clock&&readClock?readClock():null;
    if(clock&&prior)clock.advance(prior,readVersion(scheduleId,prior.body.schedule_version_ref.revision),sample,{replacing:true});
    const body={format:'leafseamer.schedule-activation/1',scope,activation_id:randomUUID(),schedule_id:scheduleId,revision:next(prior?.body.revision),
     schedule_version_ref:version.ref,prior_ref:prior?.ref??null,authorization:proof};
    const checksum=digest(body);
    put(db.prepare('INSERT INTO schedule_activation VALUES(?,?,?,?,?)').run(scheduleId,pad(body.revision),pad(version.ref.revision),canonical(body),checksum));
    const activation={body,ref:ref(body.activation_id,body.revision,checksum)};
    if(clock&&sample)clock.advance(activation,version,sample);
    return activation;
   }));
  },
  get(authentication,scheduleId,version=null,{readClock,inspect}={}){
   need(id(scheduleId)&&nullableRevision(version),'request');
   return writer(()=>{
    const bound=definition(scheduleId);need(bound,'missing');
    const proof=authorize(authentication,'getSchedule',bound,null);
    const result={version:readVersion(scheduleId,version),activation:readActivation(scheduleId)};
    need(result.version,'missing');
    const canInspect=admissions&&readClock&&inspect&&same(result.activation?.body.schedule_version_ref,result.version.ref);
    let sample;
    if(clock)result.occurrences=result.version.body.items.map(item=>{
     const {occurrence_id}=item;
     const row=admissions?.get(occurrence_id);
     if(row)return {occurrence_id,state:'accepted',admission:original(...row)};
     const status={occurrence_id,...clock.status(occurrence_id)};
     if(!canInspect||status.state!=='tracking')return status;
     if(sample===undefined)sample=readClock()??null;
     const time=sample&&openWindow(result.activation,item,sample);
     if(!time)return status;
     const {definition_id,definition_version,definition_artifact_ref}=result.version.body;
     const qualification=inspect({definition_id,definition_version,definition_artifact_ref,first_step_id:time.first_step_id});
     return {...status,qualification:{first_step_id:time.first_step_id,observed_tick_ns:time.clock.tick_ns,
      ...qualification}};
    });
    need(same(proof,authorize(authentication,'getSchedule',bound,null)),'authorization_changed');
    return freeze(result);
   });
  }
 });
}
