import {randomUUID} from 'node:crypto';
import {canonical,digest,same} from './runtime/auth-clock-value.mjs';
import {need} from './automation-publication.mjs';
import {automationSubmissionStore} from './automation-submission-store.mjs';
import {executionDelayBounds,inDelayWindow} from './automation-execution-store.mjs';

const reference=(kind,scope,object_id,body)=>({kind,scope,object_id,revision:'1',digest:digest(body)});
const decode=(text,hash)=>{
 const body=JSON.parse(text);
 need(canonical(body)===text&&digest(body)===hash,'run_recovery_dispatch_integrity');
 return body;
};

export function recoveryStepEvidence(steps,step,clock){
 const terminals=[],windows=[],tick=BigInt(clock.tick_ns);
 for(const {from_step_id} of step.plan.dependencies){
  const before=steps.find(item=>item.plan.step_id===from_step_id);
  if(before?.item.decision==='retain_success'){
   terminals.push({step_id:from_step_id,completion_evidence_refs:before.item.completion_evidence_refs});
   continue;
  }
  if(before?.window){
   need(tick<BigInt(before.window.body.deadline_tick_ns),'delay_window_expired');
   if(!before.elapsed||tick<BigInt(before.elapsed.body.clock.tick_ns)||!inDelayWindow(before.window,tick))return null;
   windows.push(before.window.ref);continue;
  }
  const terminal=before?.submission?.terminal;
  if(terminal?.state.outcome!=='succeeded')return null;
  terminals.push({step_id:from_step_id,terminal_digest:digest(terminal)});
 }
 return {terminals,windows};
}

export function automationRecoveryDispatchStore(db,{withDelays=false}={}){
 const submissions=automationSubmissionStore(db,{withTerminals:true,recovery:true});
 const originals=db.prepare('SELECT step_id,body_jcs,body_digest,ready_jcs,ready_digest FROM automation_recovery_dispatch_action_parameters WHERE execution_id=?').raw();
 const protection=db.prepare('SELECT reason FROM automation_recovery_dispatch_protection WHERE execution_id=?').pluck();
 const insert=db.prepare('INSERT INTO automation_recovery_dispatch_action_parameters VALUES(?,?,?,?,?,?,?)');
 const usage=db.prepare('SELECT count(*),coalesce(sum(length(CAST(body_jcs AS BLOB))+length(CAST(ready_jcs AS BLOB))),0) FROM automation_recovery_dispatch_action_parameters').raw();
 const executionUsage=db.prepare('SELECT count(*),coalesce(sum(length(CAST(body_jcs AS BLOB))+length(CAST(ready_jcs AS BLOB))),0) FROM automation_recovery_dispatch_action_parameters WHERE execution_id=?').raw();
 const delayRows=withDelays&&db.prepare(`SELECT d.step_id,d.body_jcs,d.body_digest,e.body_jcs,e.body_digest
  FROM automation_recovery_dispatch_delay d LEFT JOIN automation_recovery_dispatch_delay_elapsed e USING(execution_id,step_id) WHERE d.execution_id=?`).raw();
 const delayUsage=withDelays&&db.prepare(`SELECT count(d.step_id)+count(e.step_id),coalesce(sum(length(CAST(d.body_jcs AS BLOB))+
  coalesce(length(CAST(e.body_jcs AS BLOB)),0)),0) FROM automation_recovery_dispatch_delay d
  LEFT JOIN automation_recovery_dispatch_delay_elapsed e USING(execution_id,step_id) WHERE d.execution_id=?`).raw();
 function checkExecutionClock(value,clock,first=clock,deadline=value.execution.approved_before_ns){
  const {start}=value.started;
  const tick=BigInt(clock.tick_ns);
  need(same(clock.runtime_domain_ref,start.observed_at.domain)&&same(clock.runtime_domain_ref,first.runtime_domain_ref)&&
   tick>=BigInt(start.observed_at.tick_ns)&&tick>=BigInt(first.tick_ns)&&tick<BigInt(clock.valid_before_tick_ns)&&
   BigInt(clock.upper_ns)<BigInt(deadline),'run_recovery_dispatch_expired');
  return tick;
 }
 function checkClock(value,step,clock,first=step.prepared_parameters.body.clock){
  const body=step.prepared_parameters.body,tick=checkExecutionClock(value,clock,first,body.eligible_before_ns);
  for(const ref of body.delay_window_refs){
   const before=value.steps.find(candidate=>candidate.plan.step_id===ref.object_id);
   need(before?.elapsed&&same(before.window.ref,ref)&&inDelayWindow(before.window,tick)&&
    tick>=BigInt(before.elapsed.body.clock.tick_ns),'delay_window_expired');
  }
 }
 function delayBody(value,step,clock,evidence){
  const {started,execution_ref}=value,{plan,item}=step;
  return {format:'leafseamer.recovery-delay-window/1',execution_ref,step_id:plan.step_id,source_plan_digest:item.plan_digest,
   recovery_start_ref:started.start_ref,clock,dependencies:evidence,window_scope:'direct_successor',anchor:plan.payload.anchor,
   ...executionDelayBounds(started.start.observed_at.tick_ns,plan.payload)};
 }
 function readDelay(value,step,record){
  if(!record)return {window:null,elapsed:null};
  const [text,hash,elapsedText,elapsedHash]=record,{execution_ref,execution:{scope}}=value,{plan,item}=step;
  const body=decode(text,hash),window={ref:reference('delay_window',scope,plan.step_id,body),body};
  need(plan.payload.kind==='delay'&&item.decision==='execute_new'&&same(body,delayBody(value,step,body.clock,body.dependencies)),
   'run_recovery_dispatch_integrity');
  need(checkExecutionClock(value,body.clock)<BigInt(body.deadline_tick_ns),'run_recovery_dispatch_integrity');
  if(elapsedText===null)return {window,elapsed:null};
  const elapsed=decode(elapsedText,elapsedHash),tick=checkExecutionClock(value,elapsed.clock,body.clock);
  need(elapsed.format==='leafseamer.recovery-delay-elapsed/1'&&same(elapsed.execution_ref,execution_ref)&&
   elapsed.step_id===plan.step_id&&same(elapsed.window_ref,window.ref)&&inDelayWindow(window,tick),'run_recovery_dispatch_integrity');
  return {window,elapsed:{ref:reference('delay_elapsed',scope,plan.step_id,elapsed),body:elapsed}};
 }
 function read(value){
  const {execution_ref,execution}=value,{execution_id,scope}=execution;
  const records=new Map(originals.all(execution_id).map(([stepId,...data])=>[stepId,data]));
  const delays=new Map((withDelays?delayRows.all(execution_id):[]).map(([stepId,...data])=>[stepId,data]));
  const steps=value.steps.map(step=>{
   const {plan,item}=step,record=records.get(plan.step_id);
   if(!record)return {...step,...readDelay(value,step,delays.get(plan.step_id)),ready:null,prepared_parameters:null};
   const [text,hash,readyText,readyHash]=record,body=decode(text,hash),original=decode(readyText,readyHash);
   const ready={ref:reference('step_ready',scope,original.ready_id,original),body:original};
   const prepared_parameters={ref:reference('action_parameters',scope,plan.step_id,body),body};
   need(item.decision==='execute_new'&&item.grant_ref&&original.format==='leafseamer.step-ready/1'&&
    body.format==='leafseamer.action-parameters/1'&&same(original.execution_ref,execution_ref)&&same(body.execution_ref,execution_ref)&&
    body.step_id===plan.step_id&&original.step_id===plan.step_id&&digest(plan)===body.step_plan_digest&&
    original.step_plan_digest===body.step_plan_digest&&same(body.step_ready_ref,ready.ref)&&
    same(body.grant_ref,item.grant_ref)&&body.origin_digest===execution.origin_digest&&same(body.clock,original.clock)&&
    same(body.parameter_schema_ref,plan.payload.binding.parameter_schema_ref)&&digest(body.parameters)===body.parameters_digest&&
    same(body.delay_window_refs,original.delay_window_refs??[])&&
    BigInt(body.eligible_before_ns)<=BigInt(execution.approved_before_ns),'run_recovery_dispatch_integrity');
   return {...step,window:null,elapsed:null,ready,prepared_parameters};
  });
  need(steps.filter(step=>step.ready).length===records.size&&steps.filter(step=>step.window).length===delays.size,'run_recovery_dispatch_integrity');
  const hydrated={...value,steps};
  const originalsWithSubmissions=submissions.read(hydrated,(step,clock,first)=>checkClock(hydrated,step,clock,first));
  for(const step of originalsWithSubmissions){
   const original=step.ready?.body??step.window?.body;
   if(!original)continue;
   const expected=recoveryStepEvidence(originalsWithSubmissions,step,original.clock);
   const recorded=step.window?original.dependencies:{terminals:original.command_terminal_refs,windows:original.delay_window_refs??[]};
   need(same(recorded,expected),'run_recovery_dispatch_integrity');
   if(step.ready)checkClock(hydrated,step,original.clock);
   if(step.elapsed)need(recoveryStepEvidence(originalsWithSubmissions,step,step.elapsed.body.clock)!==null,'run_recovery_dispatch_integrity');
  }
  return {...value,protection:protection.get(execution_id)??null,steps:originalsWithSubmissions};
 }
 function reserve(id,budget,bytes,rows){
  const [actions,actionsBytes]=executionUsage.get(id),reserved=submissions.reservedUsage(id);
  const [delays,delayBytes]=withDelays?delayUsage.get(id):[0,0];
  need(BigInt(actionsBytes+reserved.bytes+delayBytes+bytes)<=BigInt(budget.logical_bytes)&&
   BigInt(actions*2+reserved.rows+delays+rows)<=BigInt(budget.rows)&&
   BigInt(actions*2+reserved.work+delays+rows)<=BigInt(budget.work_units)&&BigInt(budget.queue_slots)>=1n,'execution_plan_capacity');
 }
 return Object.freeze({read,submissions,checkClock,protection:id=>protection.get(id)??null,reserve,
  ...(withDelays&&{writeDelay(value,step,clock,evidence,budget){
   need(db.inTransaction&&step.plan.payload.kind==='delay'&&step.item.decision==='execute_new'&&!step.elapsed&&!value.protection,'run_recovery_dispatch_changed');
   const id=value.execution.execution_id,stepId=step.plan.step_id,body=step.window?.body??delayBody(value,step,clock,evidence);
   const tick=checkExecutionClock(value,clock,body.clock),end=BigInt(body.deadline_tick_ns);
   need(tick<end,'delay_window_expired');
   const hash=digest(body),window={body,ref:reference('delay_window',value.execution.scope,stepId,body)};
   if(!step.window){
    const text=canonical(body);reserve(id,budget,Buffer.byteLength(text),1);
    need(db.prepare('INSERT INTO automation_recovery_dispatch_delay VALUES(?,?,?,?)').run(id,stepId,text,hash).changes===1,'run_recovery_dispatch_storage');
   }
   if(!inDelayWindow(window,tick))return BigInt(body.due_tick_ns)-tick;
   const outcome={format:'leafseamer.recovery-delay-elapsed/1',execution_ref:value.execution_ref,step_id:stepId,window_ref:window.ref,clock};
   const text=canonical(outcome);reserve(id,budget,Buffer.byteLength(text),1);
   need(db.prepare('INSERT INTO automation_recovery_dispatch_delay_elapsed VALUES(?,?,?,?,?)').run(id,stepId,hash,text,digest(outcome)).changes===1,'run_recovery_dispatch_storage');
   return 0n;
  }}),
  protect(id,reason){
   need(db.inTransaction,'run_recovery_transaction');
   db.prepare('INSERT INTO automation_recovery_dispatch_protection VALUES(?,?) ON CONFLICT DO NOTHING').run(id,reason);
  },
  seal(value,step,result,deadline,clock,{terminals,windows}){
   need(db.inTransaction&&!step.ready&&!value.protection,'run_recovery_dispatch_changed');
   const {execution_ref,execution}=value,{plan}=step,{scope,execution_id}=execution;
   const ready={format:'leafseamer.step-ready/1',ready_id:randomUUID(),revision:'1',scope,execution_ref,step_id:plan.step_id,
    step_plan_digest:digest(plan),dependencies_digest:digest(plan.dependencies),recovery_start_ref:execution.start_ref,
    command_terminal_refs:terminals,...(windows.length&&{delay_window_refs:windows}),clock};
   const body={format:'leafseamer.action-parameters/1',execution_ref,step_id:plan.step_id,step_ready_ref:reference('step_ready',scope,ready.ready_id,ready),
    step_plan_digest:ready.step_plan_digest,origin_digest:execution.origin_digest,grant_ref:step.item.grant_ref,delay_window_refs:windows,
    parameter_schema_ref:result.parameter_schema_ref,parameters:result.parameters,parameters_digest:result.parameters_digest,eligible_before_ns:deadline,clock};
   checkClock(value,{prepared_parameters:{body}},clock);
   const text=canonical(body),original=canonical(ready),[count,bytes]=usage.get();
   need(count<16384&&bytes+Buffer.byteLength(text)+Buffer.byteLength(original)<=67108864,'run_recovery_dispatch_capacity');
   need(insert.run(execution_id,plan.step_id,execution_ref.digest,text,digest(body),original,digest(ready)).changes===1,'run_recovery_dispatch_storage');
  }
 });
}
