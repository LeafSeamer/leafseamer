import {randomUUID} from 'node:crypto';
import {canonical,digest,same} from './runtime/auth-clock-value.mjs';
import {need} from './automation-publication.mjs';
import {nextExecutionRevision} from './automation-execution-commit.mjs';

const ref=(kind,scope,object_id,hash)=>({kind,scope,object_id,revision:'1',digest:hash});
const decode=(text,hash)=>{
 const value=JSON.parse(text);
 need(canonical(value)===text&&digest(value)===hash,'run_recovery_operation_integrity');
 return value;
};
export const recoveryReceipt=({operation_ref,operation,notification})=>({operation_ref,operation,notification});
const nextGeneration=(snapshot,targets)=>nextExecutionRevision(String(targets.reduce((floor,target)=>{
 const deny=BigInt(target.deny_through_generation);
 return deny>floor?deny:floor;
},BigInt(snapshot.arm_generation))));

export function automationRecoveryOperationStore({db,scope,previews}){
 const select=`SELECT o.operation_id,o.run_id,o.preview_id,o.context_id,o.request_identity,o.new_execution_id,o.body_jcs,o.body_digest,
  o.audit_jcs,o.notification_jcs,e.body_jcs,e.body_digest,l.run_id,l.execution_id
  FROM automation_recovery_operation o JOIN automation_recovery_execution e USING(operation_id) JOIN automation_recovery_lock l USING(operation_id)`;
 const byId=db.prepare(select+' WHERE o.operation_id=?').raw();
 const byKey=db.prepare(select+' WHERE o.request_identity=?').raw();
 const byRun=db.prepare(select+' WHERE l.run_id=? AND l.released_by IS NULL').raw();
 const byExecution=db.prepare(select+' WHERE e.execution_id=?').raw();
 const insert=db.prepare('INSERT INTO automation_recovery_operation VALUES(?,?,?,?,?,?,?,?,?,?)');
 const insertExecution=db.prepare('INSERT INTO automation_recovery_execution VALUES(?,?,?,?)');
 const lock=db.prepare('INSERT INTO automation_recovery_lock(operation_id,run_id,execution_id) VALUES(?,?,?)');
 const advance=db.prepare('UPDATE automation_run_approval_head SET run_revision=? WHERE run_id=? AND approval_id=? AND run_revision=?');
 const count=db.prepare('SELECT count(*) FROM automation_recovery_operation').pluck();
 const notification=operation=>({event_id:operation.operation_id+'.reserved',kind:'recovery_reserved',
  run_id:operation.run_id,run_revision:operation.committed_run_revision,execution_id:operation.new_execution_id});
 const auditOf=(operation,hash,authentication)=>({kind:'recovery_reserved',operation_id:operation.operation_id,operation_digest:hash,authentication});
 function targetsFrom(snapshot){
  return snapshot.target_scopes.map(controller_scope=>{
   const basis=snapshot.observed_protection.targets.find(target=>same(target.target_scope,controller_scope));
   need(basis?.receipt_ref&&basis.deny_through_generation!==null,'run_recovery_ineligible');
   return {controller_scope,protection_ref:basis.receipt_ref,deny_through_generation:basis.deny_through_generation,device_ids:basis.device_ids,state:'pending'};
  });
 }
 function decodeOperation(row){
  need(db.inTransaction,'run_recovery_transaction');
  if(!row)return null;
  const [id,runId,previewId,contextId,key,newId,text,hash,auditText,noticeText,executionText,executionHash,lockedRun,lockedExecution]=row;
  const operation=decode(text,hash),operation_ref=ref('recovery_operation',scope,id,hash);
  const {preview}=previews.read(operation.preview_ref),{snapshot}=preview;
  const execution=decode(executionText,executionHash),audit=JSON.parse(auditText);
  need(operation.format==='leafseamer.recovery-operation/1'&&same(operation.scope,scope)&&operation.operation_id===id&&
   operation.run_id===runId&&snapshot.run_id===runId&&operation.context_id===contextId&&operation.request_identity===key&&
   operation.preview_ref.object_id===previewId&&same(operation.source_execution_ref,snapshot.execution_ref)&&operation.plan_digest===preview.plan_digest&&
   operation.baseline_snapshot_digest===preview.basis_digest&&same(operation.targets,targetsFrom(snapshot))&&operation.new_execution_id===newId&&
   operation.state==='reserved'&&operation.consumed===true&&same(operation.initiator,preview.reader.actor)&&
   same(operation.new_command_ids,[])&&operation.previous_run_revision===snapshot.run_revision&&
   operation.committed_run_revision===nextExecutionRevision(operation.previous_run_revision)&&
   operation.committed_slot_revision===nextExecutionRevision(snapshot.slot.revision)&&
   operation.recovery_generation===nextGeneration(snapshot,operation.targets)&&lockedRun===runId&&lockedExecution===newId&&
   execution.format==='leafseamer.recovery-execution-reservation/1'&&same(execution.operation_ref,operation_ref)&&same(execution.scope,scope)&&
   execution.execution_id===newId&&execution.run_id===runId&&execution.definition_id===snapshot.definition_id&&
   execution.arm_generation===operation.recovery_generation&&execution.slot_revision===operation.committed_slot_revision&&
   execution.mode===preview.plan.mode&&execution.plan_digest===preview.plan_digest&&execution.dispatch_eligible===false&&
   same(operation.accepted_at.domain,preview.window.domain)&&
   BigInt(operation.accepted_at.tick_ns)>=BigInt(preview.window.issued_tick_ns)&&BigInt(operation.accepted_at.tick_ns)<BigInt(preview.window.expires_tick_ns)&&
   operation.authentication_digest===digest(audit.authentication)&&canonical(auditOf(operation,hash,audit.authentication))===auditText&&
   canonical(notification(operation))===noticeText,'run_recovery_operation_integrity');
  return {operation_ref,operation,execution,execution_ref:ref('execution',scope,newId,executionHash),plan:preview.plan,
   notification:notification(operation),audit};
 }
 function reserve({saved,proof,key,requestDigest,contextId,now}){
  need(db.inTransaction,'run_recovery_transaction');
  const {preview_ref,preview}=saved,{snapshot,plan}=preview,run_id=snapshot.run_id;
  need(count.get()<16384,'run_recovery_preview_capacity');
  need(!byRun.get(run_id),'run_recovery_operation_conflict');
  const targets=targetsFrom(snapshot);
  const operation_id=randomUUID(),new_execution_id=randomUUID(),recovery_generation=nextGeneration(snapshot,targets);
  const operation={format:'leafseamer.recovery-operation/1',scope,operation_id,run_id,preview_ref,context_id:contextId,request_identity:key,request_digest:requestDigest,
   initiator:proof.actor,authentication_digest:digest(proof.authentication_evidence),source_execution_ref:snapshot.execution_ref,previous_run_revision:snapshot.run_revision,
   committed_run_revision:nextExecutionRevision(snapshot.run_revision),committed_slot_revision:nextExecutionRevision(snapshot.slot.revision),
   recovery_generation,state:'reserved',new_execution_id,new_command_ids:[],consumed:true,plan_digest:preview.plan_digest,
   baseline_snapshot_digest:preview.basis_digest,targets,accepted_at:{domain:now.domain,tick_ns:String(now.tick),acquisition_ref:now.acquisition_ref}};
  const hash=digest(operation),operation_ref=ref('recovery_operation',scope,operation_id,hash);
  const execution={format:'leafseamer.recovery-execution-reservation/1',scope,operation_ref,execution_id:new_execution_id,run_id,
   definition_id:snapshot.definition_id,arm_generation:recovery_generation,slot_revision:operation.committed_slot_revision,
   mode:plan.mode,plan_digest:preview.plan_digest,dispatch_eligible:false};
  const store=result=>need(result.changes===1,'run_recovery_operation_storage');
  store(insert.run(operation_id,run_id,preview_ref.object_id,contextId,key,new_execution_id,canonical(operation),hash,
   canonical(auditOf(operation,hash,proof.authentication_evidence)),canonical(notification(operation))));
  store(insertExecution.run(new_execution_id,operation_id,canonical(execution),digest(execution)));
  store(lock.run(operation_id,run_id,new_execution_id));
  need(advance.run(operation.committed_run_revision,run_id,snapshot.approval_ref.object_id,snapshot.run_revision).changes===1,'run_recovery_execution_conflict');
  return decodeOperation(byId.get(operation_id));
 }
 return Object.freeze({reserve,active:id=>decodeOperation(byRun.get(id)),read:id=>decodeOperation(byId.get(id)),
  prior:key=>decodeOperation(byKey.get(key)),execution:id=>decodeOperation(byExecution.get(id))});
}
