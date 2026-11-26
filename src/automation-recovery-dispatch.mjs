import {randomUUID} from 'node:crypto';
import {canonical,digest,same} from './runtime/auth-clock-value.mjs';
import {need} from './automation-publication.mjs';
import {automationSubmissionStore} from './automation-submission-store.mjs';

const reference=(kind,scope,object_id,body)=>({kind,scope,object_id,revision:'1',digest:digest(body)});
const decode=(text,hash)=>{
 const body=JSON.parse(text);
 need(canonical(body)===text&&digest(body)===hash,'run_recovery_dispatch_integrity');
 return body;
};

export function automationRecoveryDispatchStore(db){
 const submissions=automationSubmissionStore(db,{withTerminals:true,recovery:true});
 const originals=db.prepare('SELECT step_id,body_jcs,body_digest,ready_jcs,ready_digest FROM automation_recovery_dispatch_action_parameters WHERE execution_id=?').raw();
 const protection=db.prepare('SELECT reason FROM automation_recovery_dispatch_protection WHERE execution_id=?').pluck();
 const insert=db.prepare('INSERT INTO automation_recovery_dispatch_action_parameters VALUES(?,?,?,?,?,?,?)');
 const usage=db.prepare('SELECT count(*),coalesce(sum(length(CAST(body_jcs AS BLOB))+length(CAST(ready_jcs AS BLOB))),0) FROM automation_recovery_dispatch_action_parameters').raw();
 const executionUsage=db.prepare('SELECT count(*),coalesce(sum(length(CAST(body_jcs AS BLOB))+length(CAST(ready_jcs AS BLOB))),0) FROM automation_recovery_dispatch_action_parameters WHERE execution_id=?').raw();
 function checkClock(value,step,clock,first=step.prepared_parameters.body.clock){
  const {start}=value.started;
  const tick=BigInt(clock.tick_ns);
  need(same(clock.runtime_domain_ref,start.observed_at.domain)&&same(clock.runtime_domain_ref,first.runtime_domain_ref)&&
   tick>=BigInt(start.observed_at.tick_ns)&&tick>=BigInt(first.tick_ns)&&tick<BigInt(clock.valid_before_tick_ns)&&
   BigInt(clock.upper_ns)<BigInt(step.prepared_parameters.body.eligible_before_ns),'run_recovery_dispatch_expired');
 }
 function read(value){
  const {execution_ref,execution}=value,{execution_id,scope}=execution;
  const records=new Map(originals.all(execution_id).map(([stepId,...data])=>[stepId,data]));
  const steps=value.steps.map(step=>{
   const {plan,item}=step,record=records.get(plan.step_id);
   if(!record)return {...step,ready:null,prepared_parameters:null};
   const [text,hash,readyText,readyHash]=record,body=decode(text,hash),original=decode(readyText,readyHash);
   const ready={ref:reference('step_ready',scope,original.ready_id,original),body:original};
   const prepared_parameters={ref:reference('action_parameters',scope,plan.step_id,body),body};
   need(item.decision==='execute_new'&&item.grant_ref&&original.format==='leafseamer.step-ready/1'&&
    body.format==='leafseamer.action-parameters/1'&&same(original.execution_ref,execution_ref)&&same(body.execution_ref,execution_ref)&&
    body.step_id===plan.step_id&&original.step_id===plan.step_id&&digest(plan)===body.step_plan_digest&&
    original.step_plan_digest===body.step_plan_digest&&same(body.step_ready_ref,ready.ref)&&
    same(body.grant_ref,item.grant_ref)&&body.origin_digest===execution.origin_digest&&same(body.clock,original.clock)&&
    same(body.parameter_schema_ref,plan.payload.binding.parameter_schema_ref)&&digest(body.parameters)===body.parameters_digest&&
    BigInt(body.eligible_before_ns)<=BigInt(execution.approved_before_ns),'run_recovery_dispatch_integrity');
   checkClock(value,{prepared_parameters},body.clock);
   return {...step,ready,prepared_parameters};
  });
  need(steps.filter(step=>step.ready).length===records.size,'run_recovery_dispatch_integrity');
  return {...value,protection:protection.get(execution_id)??null,steps:submissions.read({...value,steps},
   (step,clock,first)=>checkClock(value,step,clock,first))};
 }
 return Object.freeze({read,submissions,checkClock,protection:id=>protection.get(id)??null,reserve(id,budget,bytes,rows){
  const [actions,actionsBytes]=executionUsage.get(id),reserved=submissions.reservedUsage(id);
  need(BigInt(actionsBytes+reserved.bytes+bytes)<=BigInt(budget.logical_bytes)&&
   BigInt(actions*2+reserved.rows+rows)<=BigInt(budget.rows)&&
   BigInt(actions*2+reserved.work+rows)<=BigInt(budget.work_units)&&BigInt(budget.queue_slots)>=1n,'execution_plan_capacity');
 },
  protect(id,reason){
   need(db.inTransaction,'run_recovery_transaction');
   db.prepare('INSERT INTO automation_recovery_dispatch_protection VALUES(?,?) ON CONFLICT DO NOTHING').run(id,reason);
  },
  seal(value,step,result,deadline,clock,command_terminal_refs){
   need(db.inTransaction&&!step.ready&&!value.protection,'run_recovery_dispatch_changed');
   const {execution_ref,execution}=value,{plan}=step,{scope,execution_id}=execution;
   const ready={format:'leafseamer.step-ready/1',ready_id:randomUUID(),revision:'1',scope,execution_ref,step_id:plan.step_id,
    step_plan_digest:digest(plan),dependencies_digest:digest(plan.dependencies),recovery_start_ref:execution.start_ref,command_terminal_refs,clock};
   const body={format:'leafseamer.action-parameters/1',execution_ref,step_id:plan.step_id,step_ready_ref:reference('step_ready',scope,ready.ready_id,ready),
    step_plan_digest:ready.step_plan_digest,origin_digest:execution.origin_digest,grant_ref:step.item.grant_ref,delay_window_refs:[],
    parameter_schema_ref:result.parameter_schema_ref,parameters:result.parameters,parameters_digest:result.parameters_digest,eligible_before_ns:deadline,clock};
   checkClock(value,{prepared_parameters:{body}},clock);
   const text=canonical(body),original=canonical(ready),[count,bytes]=usage.get();
   need(count<16384&&bytes+Buffer.byteLength(text)+Buffer.byteLength(original)<=67108864,'run_recovery_dispatch_capacity');
   need(insert.run(execution_id,plan.step_id,execution_ref.digest,text,digest(body),original,digest(ready)).changes===1,'run_recovery_dispatch_storage');
  }
 });
}
