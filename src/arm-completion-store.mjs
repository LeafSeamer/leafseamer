import {canonical,digest,same} from './runtime/auth-clock-value.mjs';
import {checked,need} from './automation-publication.mjs';

export function armCompletionStore({db,bootId,now,readIngestBaseline}){
 const select=`SELECT c.run_id,c.approval_id,c.previous_run_revision,c.completed_run_revision,c.body_jcs,c.body_digest,n.body_jcs,n.body_digest
  FROM automation_arm_completion c JOIN automation_arm_completion_notification n USING(operation_id) WHERE c.operation_id=?`;
 const reference=(operation,hash)=>({kind:'run_arm_completion',scope:operation.scope,object_id:operation.operation_id,revision:'1',digest:hash});
 const after=before=>String(BigInt(before)+1n);
 const put=result=>need(result.changes===1,'run_arm_completion_storage');
 function decode(text,hash){
  const value=JSON.parse(text);need(canonical(value)===text&&digest(value)===hash,'run_arm_completion_integrity');return value;
 }
 function read({operation_ref,operation}){
  need(db.inTransaction,'run_arm_completion_transaction');
  const row=db.prepare(select).raw().get(operation.operation_id);if(!row)return null;
  const [runId,approvalId,before,revision,text,hash,noticeText,noticeHash]=row,completion=decode(text,hash),completion_ref=reference(operation,hash);
  const notice=decode(noticeText,noticeHash);
  const baseline=completion.ingest_baseline;
  need(runId===operation.run_id&&approvalId===operation.approval_ref.object_id&&before===operation.committed_run_revision&&revision===after(before)&&
   same(completion.operation_ref,operation_ref)&&same(completion.approval_ref,operation.approval_ref)&&completion.run_id===runId&&
   completion.previous_run_revision===before&&completion.completed_run_revision===revision&&completion.arm_generation===operation.target_arm_generation&&
   completion.slot_revision===operation.committed_slot_revision&&completion.state==='ready'&&completion.dispatch_eligible===false&&
   same(completion.evidence.operation_ref,operation_ref)&&same(notice,{kind:'run_arm_completed',completion_ref,run_id:runId,run_revision:revision}),
   'run_arm_completion_integrity');
  // 旧完成原件仍可查回。没有基线的原件不能为后续触发提供接收下界。
  need(baseline===undefined||baseline?.receive_cursor&&same(baseline,readIngestBaseline(operation,baseline.receive_cursor)),
   'run_arm_completion_integrity');
  return {completion_ref,completion};
 }
 function complete(registered,verify){
  need(db.inTransaction,'run_arm_completion_transaction');
  const previous=read(registered);if(previous)return previous;
  const {operation_ref,operation}=registered,evidence=verify(),ingestBaseline=readIngestBaseline(operation),observed=now();
  const before=operation.committed_run_revision,revision=after(before);
  checked('LifecycleRevision',revision);
  need(same(evidence.operation_ref,operation_ref)&&observed.upper<BigInt(evidence.eligible_before_ns),'run_arm_completion_evidence');
  const completion={operation_ref,run_id:operation.run_id,approval_ref:operation.approval_ref,
   arm_generation:operation.target_arm_generation,slot_revision:operation.committed_slot_revision,previous_run_revision:before,completed_run_revision:revision,
   state:'ready',dispatch_eligible:false,service_boot_id:bootId,clock_domain_ref:observed.domain,observed_tick_ns:String(observed.tick),
   ingest_baseline:ingestBaseline,evidence};
  const text=canonical(completion),hash=digest(completion),completion_ref=reference(operation,hash);
  need(Buffer.byteLength(text)<=1048576,'run_arm_completion_capacity');
  put(db.prepare('INSERT INTO automation_arm_completion VALUES(?,?,?,?,?,?,?)')
   .run(operation.operation_id,operation.run_id,operation.approval_ref.object_id,before,revision,text,hash));
  // 只有通知封存后才推进运行修订。此处仍可按原受理状态复核全部证据。
  need(same(verify(),evidence),'run_arm_completion_evidence');
  need(same(readIngestBaseline(operation),ingestBaseline),'run_arm_ingest_baseline_changed');
  const notice={kind:'run_arm_completed',completion_ref,run_id:operation.run_id,run_revision:revision};
  put(db.prepare('INSERT INTO automation_arm_completion_notification VALUES(?,?,?)').run(operation.operation_id,canonical(notice),digest(notice)));
  return read(registered);
 }
 return Object.freeze({read,complete});
}
