import {randomUUID} from 'node:crypto';
import {canonical,digest,same} from './runtime/auth-clock-value.mjs';
import {checked,need} from './automation-publication.mjs';

const reference=(scope,object_id,hash)=>({kind:'recovery_preview',scope,object_id,revision:'1',digest:hash});
const closedWithoutDispatch=new Set(['not_submitted','rejected','never_acquired']);

export function recoveryPlan(execution,steps,resources){
 const byStep=new Map(resources.steps.map(resource=>[resource.step_id,resource]));
 return steps.map(({plan,elapsed,submission,prepared_parameters})=>{
  const current=byStep.get(plan.step_id),terminal=submission?.terminal;
  const retained=plan.payload.kind==='delay'?Boolean(elapsed):terminal?.state.outcome==='succeeded'&&current?.state==='released';
  const notSent=plan.payload.kind==='delay'||closedWithoutDispatch.has(current?.state)||
   current?.state==='released'&&terminal?.state.effect_status==='not_sent';
  let decision='stop',reason='effect_unresolved';
  if(retained){decision='retain_success';reason='previous_success';}
  else if(execution.origin_kind==='schedule')reason='scheduled_occurrence_not_replayed';
  else if(execution.runtime_revision==='3'&&notSent){decision='execute_new';reason='no_previous_write';}
  return {step_id:plan.step_id,source_execution_id:execution.execution_id,source_command_ref:submission?.adjudication?.command_ref??null,
   decision,reason,plan_digest:digest(plan),parameters_digest:prepared_parameters?.ref.digest??digest(plan.payload),
   completion_evidence_refs:retained?(elapsed?[elapsed.ref]:terminal.state.completion_evidence_refs):[]};
 });
}

export function automationRecoveryPreviewStore({db,scope,bootId,requestEpoch}){
 const insert=db.prepare('INSERT INTO automation_recovery_preview VALUES(?,?,?,?,?,?)');
 const pick=db.prepare('SELECT run_id,execution_id,service_boot_id,body_jcs,body_digest FROM automation_recovery_preview WHERE preview_id=?').raw();
 const count=db.prepare('SELECT count(*) FROM automation_recovery_preview').pluck();
 function read(ref){
  need(db.inTransaction,'run_recovery_transaction');
  need(ref&&Object.keys(ref).sort().join(',')==='digest,kind,object_id,revision,scope'&&
   ref.kind==='recovery_preview'&&ref.revision==='1'&&same(ref.scope,scope),'run_recovery_preview_reference');
  checked('LifecycleId',ref.object_id);checked('LifecycleDigest',ref.digest);
  const row=pick.get(ref.object_id);
  need(row&&row[4]===ref.digest,'run_recovery_preview_missing');
  const [runId,executionId,ownerBoot,text,hash]=row,preview=JSON.parse(text);
  need(canonical(preview)===text&&digest(preview)===hash&&preview.format==='leafseamer.recovery-preview/1'&&
   preview.preview_id===ref.object_id&&preview.snapshot.run_id===runId&&preview.snapshot.execution_ref.object_id===executionId&&
   same(preview.snapshot.scope,scope)&&preview.request_epoch===requestEpoch&&preview.window.service_boot_id===ownerBoot&&
   preview.basis_digest===digest(preview.snapshot)&&preview.plan_digest===digest(preview.plan),'run_recovery_preview_integrity');
  return {preview_ref:reference(scope,ref.object_id,hash),preview};
 }
 function record(snapshot,plan,window,proof){
  need(db.inTransaction&&window.service_boot_id===bootId,'run_recovery_transaction');
  const preview_id=randomUUID(),preview={format:'leafseamer.recovery-preview/1',preview_id,request_epoch:requestEpoch,
   window,reader:{actor:proof.actor,authorization_digest:proof.authorization_digest},
   snapshot,plan,basis_digest:digest(snapshot),plan_digest:digest(plan)};
  const text=canonical(preview),hash=digest(preview);
  need(Buffer.byteLength(text)<=1048576&&count.get()<16384,'run_recovery_preview_capacity');
  need(insert.run(preview_id,snapshot.run_id,snapshot.execution_ref.object_id,bootId,text,hash).changes===1,'run_recovery_preview_storage');
  return {preview_ref:reference(scope,preview_id,hash),preview};
 }
 return Object.freeze({record,read});
}
