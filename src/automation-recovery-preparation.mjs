import {canonical,digest,same} from './runtime/auth-clock-value.mjs';
import {armPlanOriginal} from './runtime/peer-run-arm-plan.mjs';
import {armPreparationRecord,preparationResponseLimit} from './runtime/peer-run-preparation.mjs';
import {need} from './automation-publication.mjs';

export function automationRecoveryPreparationStore(db,{withBinding=false}={}){
 const written=result=>need(result.changes===1,'run_recovery_preparation_storage');
 const registration=db.prepare('SELECT context_id,body_jcs,body_digest FROM automation_recovery_preparation WHERE operation_id=?').raw();
 const targets=db.prepare('SELECT preparation_id,controller_service_id,controller_database_instance_id FROM automation_recovery_preparation_target WHERE operation_id=?').raw();
 const attempt=db.prepare('SELECT 1 FROM automation_recovery_preparation_attempt WHERE preparation_id=?').pluck();
 const receipt=db.prepare('SELECT body_jcs,body_digest FROM automation_recovery_preparation_receipt WHERE preparation_id=?').raw();
 const capacity=table=>db.prepare(`SELECT count(*),coalesce(sum(length(CAST(body_jcs AS BLOB))),0) FROM ${table}`).raw();
 const preparationsSize=capacity('automation_recovery_preparation'),receiptsSize=capacity('automation_recovery_preparation_receipt');
 const bindingsSize=withBinding?capacity('automation_recovery_execution_binding'):null;
 function decode(text,hash){
  const body=JSON.parse(text);
  need(canonical(body)===text&&digest(body)===hash,'run_recovery_preparation_integrity');
  return body;
 }
 function insertSize(statement,text){
  const [count,total]=statement.get();
  need(count<16384&&total+Buffer.byteLength(text)<=67108864,'run_recovery_preparation_capacity');
 }
 function read(original){
  need(db.inTransaction,'run_recovery_transaction');
  const {operation,operation_ref}=original,row=registration.get(operation.operation_id);
  if(!row)return null;
  const body=decode(row[1],row[2]),indexed=targets.all(operation.operation_id);
  need(body.format==='leafseamer.recovery-preparation/1'&&same(body.operation_ref,operation_ref)&&body.context_id===row[0]&&
   same(body.plans.map(item=>item.request.controller_scope),operation.targets.map(item=>item.controller_scope))&&indexed.length===body.plans.length,
   'run_recovery_preparation_integrity');
  for(const plan of body.plans){
   const request=armPlanOriginal(plan.original,plan.plan_ref,plan.request.controller_scope,{recovery:true});
   const {controller_scope,preparation_id}=request;
   need(same(request,plan.request)&&same(request.operation_ref,operation_ref)&&request.execution_id===operation.new_execution_id&&
    request.arm_generation===operation.recovery_generation&&same(request.approval_ref,body.approval_ref)&&same(request.artifact_ref,body.artifact_ref)&&
    indexed.some(item=>same(item,[preparation_id,controller_scope.authority_service_id,controller_scope.database_instance_id])),
    'run_recovery_preparation_integrity');
  }
  return body;
 }
 function lookup(plan){
  const {preparation_id,controller_scope,origin_scope}=plan.request;
  return {preparation_id,controller_scope,origin_scope,request_digest:digest(plan.request)};
 }
 function describe(plan,record){
  armPreparationRecord(record,lookup(plan),{recovery:true});
  const applied=record.applied_receipt;
  need(same(record.request,plan.request)&&same(applied.grant.endorsement_ref,plan.endorsement_ref),'run_recovery_preparation_receipt');
  return {receipt_ref:{kind:'preparation_receipt',scope:applied.controller_scope,object_id:applied.receipt_id,revision:'1',digest:digest(applied)},
   grant_ref:applied.grant_ref};
 }
 function received(plan){
  const row=receipt.get(plan.request.preparation_id);
  if(!row)return null;
  const original=decode(...row);
  need(same(original.pairing_ref,plan.pairing_ref)&&['submit','lookup'].includes(original.via),'run_recovery_preparation_integrity');
  return {original,...describe(plan,original.record)};
 }
 function grants(body){
  need(db.inTransaction&&body.plans.length>0,'run_recovery_transaction');
  return body.plans.map(plan=>{
   const evidence=received(plan);
   need(evidence,'run_preparation_receipts_incomplete');
   return {lookup:lookup(plan),receipt_ref:evidence.receipt_ref,grant_ref:evidence.grant_ref,pairing_ref:plan.pairing_ref,
    endorsement_ref:plan.endorsement_ref};
  });
 }
 function binding(original,body=read(original)){
  const row=db.prepare('SELECT context_id,body_jcs,body_digest FROM automation_recovery_execution_binding WHERE operation_id=?')
   .raw().get(original.operation.operation_id);
  if(!row)return null;
  const [contextId,text,hash]=row,value=decode(text,hash),{operation,operation_ref,execution_ref}=original;
  need(body&&value.format==='leafseamer.recovery-execution-binding/1'&&value.context_id===contextId&&
   same(value.operation_ref,operation_ref)&&same(value.execution_ref,execution_ref)&&same(value.approval_ref,body.approval_ref)&&
   value.run_revision===body.run_revision&&value.arm_generation===operation.recovery_generation&&value.dispatch_eligible===false&&
   value.registration_digest===digest(body)&&value.plan_digest===operation.plan_digest&&same(value.grants,grants(body)),
   'run_recovery_preparation_integrity');
  return {binding_ref:{kind:'recovery_execution_binding',scope:operation.scope,object_id:operation.new_execution_id,revision:'1',digest:hash},binding:value};
 }
 return Object.freeze({read,lookup,received,
  ...(withBinding&&{grants,binding,bind(original,body,value){
   need(db.inTransaction,'run_recovery_transaction');
   const text=canonical(value);insertSize(bindingsSize,text);
   written(db.prepare('INSERT INTO automation_recovery_execution_binding VALUES(?,?,?,?)')
    .run(original.operation.operation_id,value.context_id,text,digest(value)));
   return binding(original,body);
  }}),
  operationForPreparation:id=>db.prepare('SELECT operation_id FROM automation_recovery_preparation_target WHERE preparation_id=?').pluck().get(id),
  register(original,body){
   need(db.inTransaction,'run_recovery_transaction');
   const id=original.operation.operation_id,text=canonical(body);
   insertSize(preparationsSize,text);
   written(db.prepare('INSERT INTO automation_recovery_preparation VALUES(?,?,?,?)').run(id,body.context_id,text,digest(body)));
   const insert=db.prepare('INSERT INTO automation_recovery_preparation_target VALUES(?,?,?,?)');
   for(const {request:{preparation_id,controller_scope}} of body.plans)
    written(insert.run(preparation_id,id,controller_scope.authority_service_id,controller_scope.database_instance_id));
   return read(original);
  },
  attempted:id=>attempt.get(id)===1,
  claim(id){
   need(db.inTransaction,'run_recovery_transaction');
   return db.prepare('INSERT INTO automation_recovery_preparation_attempt VALUES(?) ON CONFLICT DO NOTHING').run(id).changes===1;
  },
  adopt(plan,record,via,now,bootId){
   need(db.inTransaction,'run_recovery_transaction');
   describe(plan,record);
   const prior=received(plan);
   if(prior){
    need(same(prior.original.record,record),'run_recovery_preparation_receipt');
    return prior;
   }
   const value={record,via,pairing_ref:plan.pairing_ref,service_boot_id:bootId,domain:now.domain,tick_ns:String(now.tick)};
   const text=canonical(value);
   need(Buffer.byteLength(text)<=preparationResponseLimit,'run_recovery_preparation_capacity');
   insertSize(receiptsSize,text);
   written(db.prepare('INSERT INTO automation_recovery_preparation_receipt VALUES(?,?,?)').run(plan.request.preparation_id,text,digest(value)));
   return received(plan);
  }
 });
}
