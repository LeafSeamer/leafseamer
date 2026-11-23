import crypto from 'node:crypto';
import {canonical,digest,same} from './runtime/auth-clock-value.mjs';
import {checked,need} from './automation-publication.mjs';

const dimensions=['logical_bytes','queue_slots','rows','work_units'];
const keys=(value,names)=>value&&typeof value==='object'&&Object.keys(value).sort().join(',')===names;
const put=result=>need(result.changes===1,'run_arm_storage');
function units(value){
 need(keys(value,dimensions.join(',')),'run_arm_budget');
 return dimensions.map(key=>{
  const amount=value[key];
  need(typeof amount==='string'&&/^(0|[1-9][0-9]{0,19})$(?![\s\S])/.test(amount),'run_arm_budget');
  return BigInt(amount);
 });
}

export function armAdmissionStore({db,scope,policy}){
 const inside=()=>need(db.inTransaction,'run_arm_transaction');
 const select='SELECT operation_id,run_id,definition_id,preflight_id,context_id,request_identity,request_digest,body_jcs,body_digest FROM automation_run_arm';
 const reference=(object_id,digest)=>({kind:'run_arm_operation',scope,object_id,revision:'1',digest});
 function decode(text,hash){
  const value=JSON.parse(text);need(canonical(value)===text&&digest(value)===hash,'run_arm_integrity');return value;
 }
 function read(row){
  const [id,runId,definition,preflight,context,identity,request,text,hash]=row,body=decode(text,hash);
  need(body.operation_id===id&&body.run_id===runId&&body.definition_id===definition&&body.preflight_ref.object_id===preflight&&
   body.context_id===context&&body.request_identity===identity&&body.request_digest===request&&same(body.scope,scope)&&
   body.state==='reserved'&&body.initiator.principal_kind==='human'&&body.dispatch_eligible===false,'run_arm_integrity');
  const budget=db.prepare('SELECT body_jcs,body_digest FROM automation_arm_budget WHERE operation_id=?').raw().get(id);
  const notice=db.prepare('SELECT body_jcs,body_digest FROM automation_arm_notification WHERE operation_id=?').raw().get(id);
  const targets=db.prepare('SELECT controller_service_id,controller_database_instance_id,preparation_id FROM automation_arm_target WHERE operation_id=? ORDER BY controller_service_id,controller_database_instance_id').raw().all(id);
  need(budget&&notice&&same(decode(...budget),body.budget)&&same(decode(...notice),{kind:'run_arm_reserved',operation_ref:reference(id,hash)})&&
   same(targets,body.targets.map(({controller_scope,preparation_id})=>[controller_scope.authority_service_id,controller_scope.database_instance_id,preparation_id])),'run_arm_integrity');
  return {operation_ref:reference(id,hash),operation:body};
 }
 function slot(definition_id){
  inside();const row=db.prepare(`SELECT s.run_id,s.revision,s.operation_id,c.definition_version
   FROM automation_activation_slot s JOIN automation_run_creation c USING(run_id) WHERE s.definition_id=?`).raw().get(definition_id);
  return {scope,definition_id,revision:row?.[1]??'0',active_run_id:row?.[0]??null,active_operation_id:row?.[2]??null,definition_version:row?.[3]??null};
 }
 function budgetFits(capacity,required){
  const limit=units(capacity),needed=units(required),used=dimensions.map(()=>0n);
  for(const row of db.prepare('SELECT body_jcs,body_digest FROM automation_arm_budget').raw().iterate()){
   const amounts=units(decode(...row).required);amounts.forEach((value,index)=>{used[index]+=value;});
  }
  return needed.every((value,index)=>value>0n&&used[index]+value<=limit[index]);
 }
 function policyOffer(observation,basis){
  const {admission,...evidence}=basis;
  const result=policy.inspectInTransaction(structuredClone({scope,observation:{...observation,can_arm:false},basis:evidence}));
  if(result===null)return null;
  need(keys(result,'capacity,profile_ref,qualification_ref,required'),'run_arm_policy');
  for(const ref of [result.profile_ref,result.qualification_ref])checked('ContentRef',ref);
  return structuredClone(result);
 }
 function assess(observation,basis){
  inside();
  if(!policy||basis.slot.active_run_id!==null||observation.sources.some(({state,source,transport})=>{
   const continuity=source?.continuity;
   return state!=='synchronized'||!continuity||transport?.state!=='ready'||transport.connection_generation!==continuity.connection_generation||
    transport.ack_cursor==null||BigInt(transport.ack_cursor)<BigInt(continuity.delivery_cursor);
  })||
   observation.control_observations.some(target=>!target.devices||target.devices.some(device=>device.state!=='automatic'||device.control.gate_state!=='protected')))return null;
  const result=policyOffer(observation,basis);
  return result&&budgetFits(result.capacity,result.required)?result:null;
 }
 function assertPolicy(observation,basis){
  inside();need(policy&&basis.admission&&same(policyOffer(observation,basis),basis.admission),'run_arm_policy_changed');
 }
 function reserve({run,preflight,context,proof,key,requestDigest}){
  inside();const {observation,basis,preflight_id}=preflight.preflight;
  const budget=basis.admission,slotBefore=basis.slot;
  need(observation.can_arm===true&&budget&&same(slot(run.definition_id),slotBefore)&&slotBefore.active_run_id===null&&
   run.arm_generation==='0'&&run.active_operation_id===null&&run.lifecycle==='disarmed'&&!run.dispatch_eligible,'run_arm_slot_changed');
  need(proof.actor.principal_kind==='human'&&proof.authentication_evidence&&budgetFits(budget.capacity,budget.required),'run_arm_authorization');
  const operation_id=crypto.randomUUID(),revision=String(BigInt(run.revision)+1n);checked('LifecycleRevision',revision);
  const targets=observation.targets.map(target=>({...target,preparation_id:crypto.randomUUID(),state:'pending'}));
  const initiator_authorization_ref={id:operation_id,revision:'1',digest:digest(proof.authentication_evidence)};
  const body={scope,operation_id,kind:'arm',run_id:run.run_id,definition_id:run.definition_id,definition_version:run.definition_version,
   target_arm_generation:'1',committed_run_revision:revision,committed_slot_revision:'1',state:'reserved',dispatch_eligible:false,
   preflight_ref:preflight.preflight_ref,approval_ref:observation.approval_ref,budget,targets,context_id:context.context_id,
   request_identity:key,request_digest:requestDigest,initiator:proof.actor,initiator_authorization_ref,initiator_authentication:proof.authentication_evidence};
  const text=canonical(body),hash=digest(body);need(Buffer.byteLength(text)<=1048576,'run_arm_capacity');
  const notice={kind:'run_arm_reserved',operation_ref:reference(operation_id,hash)};
  put(db.prepare('INSERT INTO automation_run_arm VALUES(?,?,?,?,?,?,?,?,?)')
   .run(operation_id,run.run_id,run.definition_id,preflight_id,context.context_id,key,requestDigest,text,hash));
  put(db.prepare("INSERT INTO automation_activation_slot VALUES(?,?,?,'1')").run(run.definition_id,run.run_id,operation_id));
  put(db.prepare('INSERT INTO automation_arm_budget VALUES(?,?,?)').run(operation_id,canonical(budget),digest(budget)));
  const insert=db.prepare('INSERT INTO automation_arm_target VALUES(?,?,?,?)');
  for(const {controller_scope,preparation_id} of targets)put(insert.run(operation_id,controller_scope.authority_service_id,controller_scope.database_instance_id,preparation_id));
  put(db.prepare('INSERT INTO automation_arm_notification VALUES(?,?,?)').run(operation_id,canonical(notice),digest(notice)));
  put(db.prepare('UPDATE automation_run_approval_head SET run_revision=? WHERE run_id=? AND approval_id=? AND run_revision=?')
   .run(revision,run.run_id,observation.approval_ref.object_id,run.revision));
  assertPolicy(observation,basis);
  return read(db.prepare(select+' WHERE operation_id=?').raw().get(operation_id));
 }
 return Object.freeze({slot,assess,assertPolicy,reserve,operation(id,hash){
  inside();const row=db.prepare(select+' WHERE operation_id=?').raw().get(id);
  need(row&&row[8]===hash,'run_initiator_missing');return read(row);
 },operationForPreparation(id){
  inside();const operationId=db.prepare('SELECT operation_id FROM automation_arm_target WHERE preparation_id=?').pluck().get(id);
  need(operationId,'run_preparation_plan_missing');return read(db.prepare(select+' WHERE operation_id=?').raw().get(operationId));
 },forRun(run){
  inside();const row=db.prepare(select+' WHERE run_id=?').raw().get(run.run_id);return row?read(row):null;
 },prior(key){inside();const row=db.prepare(select+' WHERE request_identity=?').raw().get(key);return row?read(row):null;}});
}
