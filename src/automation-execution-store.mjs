import {randomUUID} from 'node:crypto';
import {canonical,checked,digest,ref,same} from './runtime/auth-clock-value.mjs';
import {clockInteger} from './runtime/native-clock.mjs';
import {mayContinueAfterFailure,nextExecutionRevision} from './automation-execution-commit.mjs';

const maxBytes=1048576;
const need=(ok,code)=>{if(!ok)throw Object.assign(new Error(code),{code});};
const reference=(kind,scope,id,hash)=>({kind,scope,object_id:id,revision:'1',digest:hash});
const originReference=execution=>execution.admission_ref??execution.decision_ref;

export function selectedExecutionSteps(compiled,run,selected){
 const {program}=compiled.compiled;
 let groups;
 if(program.kind==='rule'){
  need(selected==='rule','execution_plan_branch');
  groups=program.groups.map(({mode,steps},index)=>({phase:'rule',group_id:'rule.'+index,group_mode:mode,steps}));
 }else{
  const branch=program.transitions.find(item=>item.transition_id===selected&&item.from_state===run.business_state?.state_id);
  need(branch,'execution_plan_branch');
  groups=['exit','transition','enter'].map(phase=>({phase,group_id:selected+'.'+phase,group_mode:'sequence',steps:branch[phase]}));
 }
 const records=groups.flatMap(({steps,...group})=>steps.map(step=>({...group,step})));
 const stepIds=new Set(records.map(({step})=>step.step_id)),edges=new Map(records.map(({step})=>[step.step_id,[]]));
 need(stepIds.size===records.length,'execution_plan_branch');
 for(const edge of compiled.graph.edges){
  if(!stepIds.has(edge.to_step_id))continue;
  need(stepIds.has(edge.from_step_id),'execution_plan_dependency');edges.get(edge.to_step_id).push(edge);
 }
 return records.map(({step,...group},ordinal)=>({...group,ordinal,step,dependencies:edges.get(step.step_id)}));
}

export function automationExecutionStore(db,scope,bootId,{withAnchors,withDelays,withActionParameters,withCommandDependencies,withRunCommit,submissions=null,schedules=null,withScheduledRuntime=false}){
 const writeExecution=db.prepare('INSERT INTO automation_execution(execution_id,run_id,decision_id,arm_operation_id,body_jcs,body_digest) VALUES(?,?,?,?,?,?)');
 const writeScheduled=schedules&&db.prepare('INSERT INTO automation_execution(execution_id,run_id,occurrence_id,arm_operation_id,body_jcs,body_digest) VALUES(?,?,?,?,?,?)');
 const advanceScheduledRun=schedules&&db.prepare('UPDATE automation_run_approval_head SET run_revision=? WHERE run_id=? AND run_revision=?');
 const writePlan=db.prepare('INSERT INTO automation_execution_step_plan VALUES(?,?,?,?,?)');
 const writeStep=db.prepare('INSERT INTO automation_execution_step_state VALUES(?,?,?,?)');
 const writeSeal=db.prepare('INSERT INTO automation_execution_seal VALUES(?,?,?)');
 const writeHead=db.prepare('INSERT INTO automation_execution_head VALUES(?,?)');
 const select=`SELECT e.execution_id,e.run_id,e.decision_id,e.arm_operation_id,e.body_jcs,e.body_digest,s.body_jcs,s.body_digest,d.body_digest
  ${schedules?',e.occurrence_id,a.body_jcs,a.body_digest':''}
  FROM automation_execution e JOIN automation_execution_seal s USING(execution_id) LEFT JOIN automation_trigger_decision d USING(decision_id)
  LEFT JOIN automation_trigger_decision_set t ON t.ingest_epoch=d.ingest_epoch AND t.receive_sequence=d.receive_sequence
  ${schedules?'LEFT JOIN schedule_execution_admission a ON a.occurrence_id=e.occurrence_id AND a.execution_id=e.execution_id':''}`;
 const sealedSource=schedules?'(t.ingest_epoch IS NOT NULL OR a.occurrence_id IS NOT NULL)':'t.ingest_epoch IS NOT NULL';
 const byId=db.prepare(select+' WHERE e.execution_id=? AND '+sealedSource).raw();
 const current=db.prepare(select+' JOIN automation_execution_head h USING(execution_id) WHERE h.definition_id=? AND '+sealedSource).raw();
 const allSteps=db.prepare(`SELECT p.step_id,p.ordinal,p.body_jcs,p.body_digest,w.initial_jcs,w.initial_digest
  FROM automation_execution_step_plan p JOIN automation_execution_step_state w USING(execution_id,step_id)
  WHERE p.execution_id=? ORDER BY p.ordinal`).raw();
 const anchors=withAnchors&&db.prepare('SELECT body_jcs,body_digest FROM automation_execution_anchor WHERE execution_id=?').raw();
 const readySteps=withAnchors&&db.prepare('SELECT step_id,body_jcs,body_digest FROM automation_execution_step_ready WHERE execution_id=?').raw();
 const writeAnchor=withAnchors&&db.prepare('INSERT INTO automation_execution_anchor VALUES(?,?,?,?,?,?)');
 const writeReady=withAnchors&&db.prepare('INSERT INTO automation_execution_step_ready VALUES(?,?,?,?,?,?,?)');
 const delays=withDelays&&db.prepare('SELECT step_id,body_jcs,body_digest FROM automation_execution_delay WHERE execution_id=?').raw();
 const elapsed=withDelays&&db.prepare('SELECT step_id,body_jcs,body_digest FROM automation_execution_delay_elapsed WHERE execution_id=?').raw();
 const writeDelay=withDelays&&db.prepare('INSERT INTO automation_execution_delay VALUES(?,?,?,?)');
 const writeElapsed=withDelays&&db.prepare('INSERT INTO automation_execution_delay_elapsed VALUES(?,?,?,?,?,?,?)');
 const protection=withDelays&&db.prepare('SELECT reason FROM automation_execution_protection WHERE execution_id=?').pluck();
 const writeProtection=withDelays&&db.prepare('INSERT INTO automation_execution_protection VALUES(?,?)');
 const actionParameters=withActionParameters&&db.prepare('SELECT step_id,body_jcs,body_digest FROM automation_execution_action_parameters WHERE execution_id=?').raw();
 const writeActionParameters=withActionParameters&&db.prepare('INSERT INTO automation_execution_action_parameters VALUES(?,?,?,?,?,?,?)');
 const acquisition=withAnchors&&db.prepare(`SELECT body FROM authentication_clock_acquisition
  WHERE service_boot_id=? AND clock_domain_id=? AND id=? AND revision=? AND digest=?`).pluck();
 const put=result=>need(result.changes===1,'execution_plan_storage');
 function decode(text,hash){
  const value=JSON.parse(text);need(canonical(value)===text&&digest(value)===hash,'execution_plan_integrity');return value;
 }
 function original(row){
  const [id,runId,decisionId,operationId,text,hash,noticeText,noticeHash,decisionHash,occurrenceId,admissionText,admissionHash]=row;
  const execution=decode(text,hash),execution_ref=reference('execution',scope,id,hash),notice=decode(noticeText,noticeHash);
  const common={kind:'execution_plan_sealed',execution_ref,run_id:runId,revision:execution.run_revision};
  need(same(execution.scope,scope)&&execution.execution_id===id&&execution.run_id===runId&&execution.arm_completion_ref.object_id===operationId,'execution_plan_integrity');
  if(occurrenceId){
   const admission=decode(admissionText,admissionHash),admission_ref=reference('schedule_admission',scope,occurrenceId,admissionHash);
   need(execution.format==='leafseamer.scheduled-execution/1'&&execution.origin_kind==='schedule'&&same(execution.admission_ref,admission_ref)&&
    admission.format==='leafseamer.schedule-admission/1'&&same(admission.scope,scope)&&admission.occurrence_id===occurrenceId&&
    admission.execution_id===id&&admission.run_id===runId&&admission.service_boot_id===execution.service_boot_id&&
    (execution.runtime_revision===undefined||execution.runtime_revision==='3'&&execution.origin_digest===admissionHash&&admission.reference_binding)&&
    same(admission.artifact_ref,execution.artifact_ref)&&same(notice,{...common,admission_ref}),'execution_plan_integrity');
   return {execution_ref,execution,admission};
  }
  need(execution.format==='leafseamer.trigger-execution/1'&&execution.origin_kind==='trigger'&&
   same(execution.decision_ref,reference('trigger_decision',scope,decisionId,decisionHash))&&
   same(notice,{...common,decision_ref:execution.decision_ref}),'execution_plan_integrity');
  return {execution_ref,execution};
 }
 function record(decision,decisionDigest,plan){
  need(db.inTransaction&&decision.disposition==='accepted','execution_plan_transaction');
  const {execution_id,run_id,decision_id,run_revision_after,arm_completion_ref,receive_cursor,artifact_ref,arm_generation}=decision;
  const decision_ref=reference('trigger_decision',scope,decision_id,decisionDigest);
  const original={format:'leafseamer.trigger-execution/1',
   ...(withActionParameters?{runtime_revision:'3',origin_digest:decision.evaluation.origin_digest}:withDelays?{runtime_revision:'2'}:{}),
   origin_kind:'trigger',decision_ref,receive_cursor,evaluation_digest:digest(decision.evaluation),selected_branch:decision.evaluation.selected_branch};
  return persist({execution_id,run_id,arm_completion_ref,artifact_ref,arm_generation,run_revision:run_revision_after},original,
   {decision_ref},plan,writeExecution,decision_id);
 }
 function recordScheduled(run,source,selectedBranch,plan,readClock){
  need(db.inTransaction&&run.lifecycle==='armed'&&run.active_execution_id===null&&run.run_arm_completion_ref&&
   same(run.scope,scope)&&plan.activation_slot.active_run_id===run.run_id&&plan.activation_slot.definition_id===run.definition_id&&
   plan.activation_slot.active_operation_id===run.run_arm_completion_ref.object_id,'execution_schedule_run');
  const {run_id,artifact_ref,arm_generation,run_arm_completion_ref:arm_completion_ref}=run,execution_id=randomUUID(),
   run_revision=nextExecutionRevision(run.revision);
  const admission=schedules.prepareAdmissionInTransaction(source,run,execution_id,plan.steps,readClock);
  const admission_ref=reference('schedule_admission',scope,admission.body.occurrence_id,admission.checksum);
  const sealed=persist({execution_id,run_id,artifact_ref,arm_generation,arm_completion_ref,run_revision},
   {format:'leafseamer.scheduled-execution/1',origin_kind:'schedule',admission_ref,selected_branch:selectedBranch,
    ...(withScheduledRuntime&&{runtime_revision:'3',origin_digest:admission.checksum})},
   {admission_ref},plan,writeScheduled,admission.body.occurrence_id,Buffer.byteLength(admission.text),1);
  put(advanceScheduledRun.run(run_revision,run_id,run.revision));
  plan.assertCurrentInTransaction();
  admission.sealInTransaction();
  return {...sealed,admission:admission.body};
 }
 function persist(identity,original,source,plan,insert,sourceId,sourceBytes=0,sourceRows=0){
  const {execution_id,run_id,arm_completion_ref,run_revision}=identity;
  const {steps,budget,activation_slot,program_digest,graph_digest,grant_basis,policy_ref,business_state,approved_before_ns,target_state_id}=plan;
  const execution={...original,...identity,scope,definition_id:activation_slot.definition_id,service_boot_id:bootId,
   program_digest,graph_digest,activation_slot,business_state,grant_basis,policy_ref,approved_before_ns,
   ...(withRunCommit&&{activation_slot_revision:nextExecutionRevision(activation_slot.revision),target_state_id}),
   steps:[]};
  let bytes=sourceBytes,work=0;
  const rows=steps.map(({phase,group_id,group_mode,ordinal,step,dependencies,action})=>{
   const payload=step.kind==='delay'?{kind:'delay',anchor:'execution_admitted',delay_ms:step.delay_ms,max_lateness_ms:step.max_lateness_ms}:
    {kind:'action',binding:action,guard:step.guard,parameters:step.parameters,parameter_bindings:step.parameter_bindings};
   const plan={execution_id,step_id:step.step_id,phase,group_id,group_mode,ordinal,required:step.required,failure_policy:step.failure_policy,dependencies,payload};
   const body=canonical(plan),planDigest=digest(plan),initial={execution_id,step_id:step.step_id,plan_digest:planDigest,revision:'1',state:'waiting',outcome:'pending'};
   const initialText=canonical(initial),initialDigest=digest(initial);
   bytes+=Buffer.byteLength(body)+Buffer.byteLength(initialText);work+=1+dependencies.length;
   need(bytes<=maxBytes&&BigInt(bytes)<=BigInt(budget.logical_bytes)&&BigInt(work)<=BigInt(budget.work_units),'execution_plan_capacity');
   execution.steps.push({step_id:step.step_id,plan_digest:planDigest,state_digest:initialDigest});
   return {step_id:step.step_id,ordinal,body,planDigest,initialText,initialDigest};
  });
  const text=canonical(execution),hash=digest(execution),execution_ref=reference('execution',scope,execution_id,hash);
  const notice={kind:'execution_plan_sealed',execution_ref,...source,run_id,revision:run_revision};
  bytes+=Buffer.byteLength(text)+Buffer.byteLength(canonical(notice));
  const rowCount=rows.length*2+3+sourceRows;
  need(bytes<=maxBytes&&BigInt(bytes)<=BigInt(budget.logical_bytes)&&BigInt(rowCount)<=BigInt(budget.rows)&&BigInt(budget.queue_slots)>=1n,'execution_plan_capacity');
  put(insert.run(execution_id,run_id,sourceId,arm_completion_ref.object_id,text,hash));
  for(const row of rows){
   put(writePlan.run(execution_id,row.step_id,row.ordinal,row.body,row.planDigest));
   put(writeStep.run(execution_id,row.step_id,row.initialText,row.initialDigest));
  }
  put(writeSeal.run(execution_id,canonical(notice),digest(notice)));
  put(writeHead.run(activation_slot.definition_id,execution_id));
  return {execution_ref,planUsage:{bytes,rows:rowCount,work},budget};
 }
 function checkClock(clock,boot,deadline,domain){
  const {acquisition_ref:acquisitionRef,runtime_domain_ref:runtime,tick_ns,upper_ns,valid_before_tick_ns}=clock;
  const tick=clockInteger(tick_ns);
  need(same(runtime,domain)&&tick<clockInteger(valid_before_tick_ns)&&clockInteger(upper_ns)<clockInteger(deadline),'execution_clock_changed');
  const raw=acquisition.get(boot,runtime.clock_domain_id,acquisitionRef.id,acquisitionRef.revision,acquisitionRef.digest);
  need(raw,'execution_clock_acquisition');
  const text=raw.toString('utf8'),record=checked(JSON.parse(text));
  need(canonical(record)===text&&same(ref(record),acquisitionRef)&&record.service_boot_id===boot&&
   record.clock_domain_id===runtime.clock_domain_id&&record.descriptor_digest===runtime.descriptor_digest&&tick>=clockInteger(record.b)&&
   clockInteger(valid_before_tick_ns)<=clockInteger(record.valid_before_tick_ns),'execution_clock_acquisition');
  return tick;
 }
 function loadAnchor(value){
  const {execution,execution_ref}=value,row=anchors.get(execution.execution_id);
  if(!row)return null;
  const body=decode(...row);
  need(body.format==='leafseamer.execution-anchor/1'&&same(body.scope,scope)&&same(body.execution_ref,execution_ref)&&
   same(body.origin_ref,originReference(execution))&&body.service_boot_id===execution.service_boot_id&&body.revision==='1','execution_plan_integrity');
  checkClock(body.clock,body.service_boot_id,execution.approved_before_ns,body.clock.runtime_domain_ref);
  return {ref:reference('execution_anchor',scope,body.anchor_id,row[1]),body};
 }
 function readInitialReadiness(value,steps,anchor){
  const {execution,execution_ref}=value;
  const records=new Map(readySteps.all(execution.execution_id).map(([stepId,text,hash])=>[stepId,{body:decode(text,hash),hash}]));
  const delayRuntime=withDelays&&['2','3'].includes(execution.runtime_revision);
  const roots=anchor?steps.filter(({plan})=>plan.dependencies.length===0).length:0;
  need(delayRuntime?records.size>=roots:records.size===roots,'execution_plan_integrity');
  return steps.map(step=>{
   const {plan,state}=step,item=records.get(plan.step_id);
   if(!anchor||plan.dependencies.length&&(!delayRuntime||!item)){
    need(!item,'execution_plan_integrity');return {...step,ready:null};
   }
   const body=item?.body,anchorClock=anchor.body.clock;
   need(body&&body.format==='leafseamer.step-ready/1'&&same(body.scope,scope)&&same(body.execution_ref,execution_ref)&&
    same(body.execution_anchor_ref,anchor.ref)&&body.step_id===plan.step_id&&body.step_plan_digest===state.plan_digest&&
    body.dependencies_digest===digest(plan.dependencies)&&body.revision==='1','execution_plan_integrity');
   const tick=checkClock(body.clock,execution.service_boot_id,execution.approved_before_ns,anchorClock.runtime_domain_ref);
   need(tick>=clockInteger(anchorClock.tick_ns),'execution_plan_integrity');
   return {...step,ready:{ref:reference('step_ready',scope,body.ready_id,item.hash),body}};
  });
 }
 function originals(statement,id,kind){
  return new Map(statement.all(id).map(([stepId,text,hash])=>[stepId,{ref:reference(kind,scope,stepId,hash),body:decode(text,hash)}]));
 }
 function timeWindow(value,step,ready){
  const {execution_ref,anchor}=value,{step_id,payload}=step.plan;
  const anchorTick=clockInteger(anchor.body.clock.tick_ns);
  const due=clockInteger(String(anchorTick+BigInt(payload.delay_ms)*1000000n));
  const deadline=clockInteger(String(due+BigInt(payload.max_lateness_ms)*1000000n));
  return {format:'leafseamer.delay-window/1',execution_ref,step_id,step_ready_ref:ready.ref,
   execution_anchor_ref:anchor.ref,clock_domain_ref:anchor.body.clock.runtime_domain_ref,anchor_tick_ns:String(anchorTick),
   due_tick_ns:String(due),deadline_tick_ns:String(deadline),window_scope:'direct_successor',anchor:payload.anchor};
 }
 const inWindow=(window,tick)=>tick>=clockInteger(window.body.due_tick_ns)&&tick<clockInteger(window.body.deadline_tick_ns);
 function dependencyEvidence(plan,byStep,tick,{historical=false}={}){
  const windows=new Map(),terminals=new Map();
  for(const {from_step_id,edge_kind} of plan.dependencies){
   const predecessor=byStep.get(from_step_id);
   if(predecessor?.elapsed){
    if(clockInteger(predecessor.elapsed.body.clock.tick_ns)>tick||!inWindow(predecessor.window,tick))return null;
    windows.set(from_step_id,predecessor.window);
    continue;
   }
   const terminal=predecessor?.submission?.terminal;
   if(!withCommandDependencies||!terminal||!['sequence','requires_success'].includes(edge_kind))return null;
   if(terminal.state.outcome!=='succeeded'&&!(withRunCommit&&edge_kind==='sequence'&&mayContinueAfterFailure(predecessor.plan,terminal.state,{historical})))return null;
   terminals.set(from_step_id,{step_id:from_step_id,terminal_digest:digest(terminal)});
  }
  return {windows:[...windows.values()],terminals:[...terminals.values()]};
 }
 function delayProgress(value,steps){
  const {execution,execution_ref,anchor}=value,id=execution.execution_id;
  const windows=originals(delays,id,'delay_window'),outcomes=originals(elapsed,id,'delay_elapsed');
  const byStep=new Map(steps.map(step=>{
   const {plan,ready}=step,window=windows.get(plan.step_id)??null,outcome=outcomes.get(plan.step_id)??null;
   const requiresWindow=ready&&plan.payload.kind==='delay';
   need(Boolean(window)===Boolean(requiresWindow)&&(!outcome||window),'execution_plan_integrity');
   if(window)need(same(window.body,timeWindow(value,step,ready)),'execution_plan_integrity');
   if(outcome){
    const {body}=outcome,tick=checkClock(body.clock,execution.service_boot_id,execution.approved_before_ns,anchor.body.clock.runtime_domain_ref);
    need(body.format==='leafseamer.delay-elapsed/1'&&same(body.execution_ref,execution_ref)&&
     body.step_id===plan.step_id&&same(body.window_ref,window.ref)&&
     tick>=clockInteger(ready.body.clock.tick_ns)&&inWindow(window,tick),'execution_plan_integrity');
   }
   return [plan.step_id,{...step,window,elapsed:outcome}];
  }));
  return [...byStep.values()];
 }
 function checkReadyDependencies(value){
  const byStep=new Map(value.steps.map(step=>[step.plan.step_id,step]));
  for(const {plan,ready} of value.steps){
   if(!ready)continue;
   // 已持久化的就绪原件沿用记录时的证据判据。
   const evidence=dependencyEvidence(plan,byStep,clockInteger(ready.body.clock.tick_ns),{historical:true});
   need(evidence&&same(ready.body.delay_window_refs??[],evidence.windows.map(window=>window.ref))&&
    same(ready.body.command_terminal_refs??[],evidence.terminals),'execution_plan_integrity');
  }
  return value;
 }
 function read(id){
  const row=byId.get(id);if(!row)return null;
  const value=original(row),{execution}=value;
  const steps=allSteps.all(id).map(([stepId,ordinal,body,hash,initial,initialHash],index)=>{
   const plan=decode(body,hash),state=decode(initial,initialHash);
   need(plan.step_id===stepId&&plan.execution_id===id&&ordinal===index&&plan.ordinal===index&&
    same(execution.steps[index],{step_id:stepId,plan_digest:hash,state_digest:initialHash})&&
    same(state,{execution_id:id,step_id:stepId,plan_digest:hash,revision:'1',state:'waiting',outcome:'pending'}),'execution_plan_integrity');
   return {plan,state};
  });
  need(steps.length===execution.steps.length,'execution_plan_integrity');
  if(!anchors)return {...value,steps};
  const anchor=loadAnchor(value);
  const readiness=readInitialReadiness(value,steps,anchor);
  if(!withDelays||!['2','3'].includes(execution.runtime_revision))return {...value,anchor,steps:readiness};
  const progressed={...value,anchor,steps:delayProgress({...value,anchor},readiness),protection:protection.get(id)??null};
  if(!withActionParameters||execution.runtime_revision!=='3')return checkReadyDependencies(progressed);
  const originalsByStep=originals(actionParameters,id,'action_parameters');
  const prepared={...progressed,steps:progressed.steps.map(step=>{
   const prepared_parameters=originalsByStep.get(step.plan.step_id)??null;
   if(prepared_parameters)checkPreparedParameters(progressed,step,prepared_parameters.body);
   return {...step,prepared_parameters};
  })};
  if(!submissions)return checkReadyDependencies(prepared);
  return checkReadyDependencies({...prepared,steps:submissions.read(prepared,(step,clock,planned=step.prepared_parameters.body.clock)=>{
   const deadline=step.prepared_parameters.body.eligible_before_ns;
   need(assertActionWindow(prepared,step,clock,deadline)>=assertActionWindow(prepared,step,planned,deadline),'execution_clock_changed');
  })});
 }
 function active(run){
  const row=current.get(run.definition_id);if(!row)return null;
  const result=original(row),{execution}=result;
  need(execution.run_id===run.run_id&&execution.arm_generation===run.arm_generation&&same(execution.artifact_ref,run.artifact_ref)&&
   same(execution.arm_completion_ref,run.run_arm_completion_ref),'execution_plan_integrity');
  return anchors?{...result,anchor:loadAnchor(result),...(withDelays&&{protection:protection.get(execution.execution_id)??null})}:result;
 }
 function reserve(planUsage,budget,already=[]){
  const byteLimit=Math.min(maxBytes,Number(budget.logical_bytes));
  let {bytes,rows,work}=planUsage;
  for(const record of already){bytes+=Buffer.byteLength(canonical(record));rows++;work++;}
  return (text=null)=>{
   if(text!==null){bytes+=Buffer.byteLength(text);rows++;work++;}
   need(bytes<=byteLimit&&BigInt(rows)<=BigInt(budget.rows)&&BigInt(work)<=BigInt(budget.work_units),'execution_plan_capacity');
  };
 }
 function runningOriginals(value){
  return [value.anchor.body,...value.steps.flatMap(step=>[step.ready?.body,step.window?.body,step.elapsed?.body,step.prepared_parameters?.body].filter(Boolean))];
 }
 function runningBudget(value,item){
  const reserved=submissions?.reservedUsage(value.execution.execution_id);
  if(!reserved)return item.planUsage;
  const {bytes,rows,work}=item.planUsage;
  return {bytes:bytes+reserved.bytes,rows:rows+reserved.rows,work:work+reserved.work};
 }
 function reserveSubmission(value,item,bytes,rows){
  const used=runningBudget(value,item);
  reserve({bytes:used.bytes+bytes,rows:used.rows+rows,work:used.work+rows},item.budget,runningOriginals(value))();
 }
 function assertActionWindow(value,step,clock,deadline){
  const {execution,anchor,steps}=value,domain=anchor.body.clock.runtime_domain_ref;
  const tick=checkClock(clock,execution.service_boot_id,deadline,domain);
  need(step.ready&&tick>=clockInteger(step.ready.body.clock.tick_ns),'execution_clock_changed');
  const windows=new Map(steps.filter(item=>item.window).map(item=>[item.plan.step_id,item.window]));
  for(const ref of step.ready.body.delay_window_refs??[]){
   const window=windows.get(ref.object_id);
   need(window&&same(window.ref,ref)&&inWindow(window,tick),'delay_window_expired');
  }
  return tick;
 }
 function checkPreparedParameters(value,step,body){
  const {execution,execution_ref}=value,{plan,state,ready}=step;
  need(plan.payload.kind==='action'&&ready&&body.format==='leafseamer.action-parameters/1'&&
   same(body.execution_ref,execution_ref)&&body.step_id===plan.step_id&&same(body.step_ready_ref,ready.ref)&&
   body.step_plan_digest===state.plan_digest&&body.origin_digest===execution.origin_digest&&
   same(body.parameter_schema_ref,plan.payload.binding.parameter_schema_ref)&&same(body.grant_ref,plan.payload.binding.grant_ref)&&
   same(body.delay_window_refs,ready.body.delay_window_refs??[])&&digest(body.parameters)===body.parameters_digest&&
   clockInteger(body.eligible_before_ns)<=clockInteger(execution.approved_before_ns),'execution_action_parameters_integrity');
  return assertActionWindow(value,step,body.clock,body.eligible_before_ns);
 }
 function assertActionCurrent(value,step,clock,deadline){
  need(db.inTransaction&&value.execution.service_boot_id===bootId&&value.execution.runtime_revision==='3'&&
   !value.protection&&!step.prepared_parameters,'execution_action_inactive');
  return assertActionWindow(value,step,clock,deadline);
 }
 function sealParameters(value,step,item,result,deadline,clock,recheck,sample){
  const {execution_ref,execution}=value,{plan,state,ready}=step;
  const body={format:'leafseamer.action-parameters/1',execution_ref,step_id:plan.step_id,step_ready_ref:ready.ref,
   step_plan_digest:state.plan_digest,origin_digest:execution.origin_digest,grant_ref:plan.payload.binding.grant_ref,
   delay_window_refs:ready.body.delay_window_refs??[],parameter_schema_ref:result.parameter_schema_ref,parameters:result.parameters,
   parameters_digest:result.parameters_digest,eligible_before_ns:deadline,clock};
  assertActionCurrent(value,step,clock,deadline);
  const at=checkPreparedParameters(value,step,body),text=canonical(body);
  reserve(runningBudget(value,item),item.budget,runningOriginals(value))(text);
  put(writeActionParameters.run(execution.execution_id,plan.step_id,bootId,clock.runtime_domain_ref.clock_domain_id,clock.acquisition_ref.id,text,digest(body)));
  recheck();
  need(assertActionWindow(value,step,sample(),deadline)>=at,'execution_clock_changed');
 }
 function saveReady(value,step,clock,windows,charge,terminals=[]){
  const {execution_ref,execution,anchor}=value,{plan,state}=step;
  const ready={format:'leafseamer.step-ready/1',ready_id:randomUUID(),revision:'1',scope,execution_ref,step_id:plan.step_id,
   step_plan_digest:state.plan_digest,dependencies_digest:digest(plan.dependencies),execution_anchor_ref:anchor.ref,clock,
   ...(windows.length&&{delay_window_refs:windows.map(window=>window.ref)}),
   ...(terminals.length&&{command_terminal_refs:terminals})};
  const text=canonical(ready),hash=digest(ready);
  charge(text);
  put(writeReady.run(execution.execution_id,plan.step_id,bootId,clock.runtime_domain_ref.clock_domain_id,clock.acquisition_ref.id,text,hash));
  if(withDelays&&plan.payload.kind==='delay'){
   const window=timeWindow(value,step,{ref:reference('step_ready',scope,ready.ready_id,hash),body:ready}),body=canonical(window);
   charge(body);
   put(writeDelay.run(execution.execution_id,plan.step_id,body,digest(window)));
  }
 }
 function anchorAdmission({execution_ref,execution,steps,anchor:previous},{planUsage,budget},floor,clock,sample){
  need(anchors&&db.inTransaction,'execution_anchor_transaction');
  need(execution.service_boot_id===bootId&&!previous,'execution_anchor_identity');
  const {runtime_domain_ref:domain}=floor,deadline=execution.approved_before_ns;
  const at=checkClock(clock,bootId,deadline,domain);
  need(at>=clockInteger(floor.tick_ns),'execution_clock_changed');
  const anchor={format:'leafseamer.execution-anchor/1',anchor_id:randomUUID(),revision:'1',scope,execution_ref,
   origin_ref:originReference(execution),service_boot_id:bootId,clock};
  const anchorDigest=digest(anchor),anchorText=canonical(anchor),charge=reserve(planUsage,budget);
  const roots=steps.filter(({plan})=>plan.dependencies.length===0);
  charge(anchorText);
  put(writeAnchor.run(execution.execution_id,bootId,domain.clock_domain_id,clock.acquisition_ref.id,anchorText,anchorDigest));
  const readyClock=sample();
  need(checkClock(readyClock,bootId,deadline,domain)>=at,'execution_clock_changed');
  const anchored={execution_ref,execution,anchor:{ref:reference('execution_anchor',scope,anchor.anchor_id,anchorDigest),body:anchor}};
  for(const step of roots)saveReady(anchored,step,readyClock,[],charge);
  const last=sample();
  need(checkClock(last,bootId,deadline,domain)>=clockInteger(readyClock.tick_ns),'execution_clock_changed');
  return withRunCommit||(withActionParameters?roots.length>0:roots.some(({plan})=>plan.payload.kind==='delay'));
 }
 function advanceDelays(value,item,sample,recheckAuthority){
  need(db.inTransaction&&withDelays&&value.execution.service_boot_id===bootId&&value.anchor&&!value.protection,'execution_delay_transaction');
  const {steps,execution_ref,execution,anchor}=value,id=execution.execution_id,domain=anchor.body.clock.runtime_domain_ref;
  const clock=sample(),tick=checkClock(clock,bootId,execution.approved_before_ns,domain);
  const byStep=new Map(steps.map(step=>[step.plan.step_id,step]));
  const needsWindow=new Set(steps.filter(step=>!step.ready||step.plan.payload.kind==='action'&&!(withRunCommit&&step.submission?.terminal))
   .flatMap(({plan})=>plan.dependencies.map(edge=>edge.from_step_id)));
  const due=[],recheck=[];
  let next=null;
  function wakeAt(at){if(next===null||at<next)next=at;}
  for(const step of steps){
   const {window,elapsed:done,ready}=step;
   need(!ready||tick>=clockInteger(ready.body.clock.tick_ns),'execution_clock_changed');
   need(!done||tick>=clockInteger(done.body.clock.tick_ns),'execution_clock_changed');
   if(!window||done&&!needsWindow.has(step.plan.step_id))continue;
   const start=clockInteger(window.body.due_tick_ns),end=clockInteger(window.body.deadline_tick_ns);
   need(tick<end,'delay_window_expired');
   if(!done&&tick>=start)due.push(step);
   else wakeAt(done?end:start);
  }
  if(!due.length&&(!withCommandDependencies||!steps.some(step=>!step.ready&&dependencyEvidence(step.plan,byStep,tick))))
   return next===null?null:next-tick;
  const charge=reserve(runningBudget(value,item),item.budget,runningOriginals(value));
  for(const step of due){
   const {window,plan}=step;
   const result={format:'leafseamer.delay-elapsed/1',execution_ref,step_id:plan.step_id,window_ref:window.ref,clock};
   const text=canonical(result),hash=digest(result);
   charge(text);
   put(writeElapsed.run(id,plan.step_id,bootId,domain.clock_domain_id,clock.acquisition_ref.id,text,hash));
   byStep.set(plan.step_id,{...step,elapsed:{ref:reference('delay_elapsed',scope,plan.step_id,hash),body:result}});
   recheck.push(window);
  }
  for(const step of steps){
   if(step.ready)continue;
   const evidence=dependencyEvidence(step.plan,byStep,tick);
   if(!evidence)continue;
   const {windows,terminals}=evidence;
   saveReady(value,step,clock,windows,charge,terminals);
   recheck.push(...windows);
  }
  recheckAuthority();
  const last=checkClock(sample(),bootId,execution.approved_before_ns,domain);
  need(last>=tick,'execution_clock_changed');
  need(recheck.every(window=>inWindow(window,last)),'delay_window_expired');
  return 0n;
 }
 function protect(id,reason){
  need(db.inTransaction,'execution_delay_transaction');
  if(!protection.get(id))put(writeProtection.run(id,reason));
 }
 function completionClock(value,clock){
  const {execution,anchor}=value;
  need(execution.service_boot_id===bootId&&anchor&&!value.protection,'execution_commit_inactive');
  const tick=checkClock(clock,bootId,execution.approved_before_ns,anchor.body.clock.runtime_domain_ref);
  need(tick>=clockInteger(anchor.body.clock.tick_ns),'execution_clock_changed');
  return tick;
 }
 return Object.freeze({record,read,active,...(schedules&&{recordScheduled}),...(anchors&&{anchorAdmission}),...(withDelays&&{advanceDelays,protect}),
  ...(withActionParameters&&{assertActionCurrent,assertActionWindow,sealParameters}),...(submissions&&{reserveSubmission}),...(withRunCommit&&{completionClock})});
}
