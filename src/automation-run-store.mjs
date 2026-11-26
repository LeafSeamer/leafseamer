import crypto from 'node:crypto';
import {canonical,digest,same} from './runtime/auth-clock-value.mjs';
import {checked,need} from './automation-publication.mjs';
import {readReceiveCursor} from './reconcile-inbox.mjs';
import {assertRunGrantParameters,constrainRunTargets} from './run-approval-parameters.mjs';
import {peerScope,proposalTarget,proposalWindowEnd} from './runtime/peer-run-approval-proposal.mjs';
import {runApprovalStore} from './run-approval-store.mjs';
import {armAdmissionStore,hasCurrentSourceContinuity} from './arm-admission-store.mjs';
import {armCompletionStore} from './arm-completion-store.mjs';
import {armPreparationReceiptStore} from './arm-preparation-receipt-store.mjs';
import {pairingReference} from './runtime/peer-pairing-value.mjs';
import {armPlanOriginal} from './runtime/peer-run-arm-plan.mjs';
import {automationTriggerDecisionStore} from './automation-trigger-decision-store.mjs';
import {evaluateExpression,evaluateValue} from './automation-expression.mjs';
import {evaluateScheduledCondition,evaluateTriggerCondition,eventInputSnapshots} from './automation-trigger-condition.mjs';
import {automationExecutionStore,selectedExecutionSteps} from './automation-execution-store.mjs';
import {automationSubmissionStore} from './automation-submission-store.mjs';
import {automationExecutionCommit,completionEvidence,mayContinueAfterFailure,nextExecutionRevision} from './automation-execution-commit.mjs';
import {automationRecoveryPreviewStore,recoveryPlan} from './automation-recovery-preview.mjs';
import {automationRecoveryOperationStore,recoveryReceipt} from './automation-recovery-operation.mjs';
import {automationRecoveryPreparationStore} from './automation-recovery-preparation.mjs';
import {automationRecoveryDispatchStore} from './automation-recovery-dispatch.mjs';

const fields=(value,names)=>value&&Object.keys(value).sort().join(',')===names;
const view=({last_receive_cursor,ingest_revision,dependency_digest,run_arm_completion_ref,arm_source_baselines,trigger_pause_revision,...run})=>({...run,last_receive_seq:last_receive_cursor.receive_sequence});
const compare=(a,b)=>a<b?-1:a>b?1:0;
const orderAction=(a,b)=>compare(a.device_id,b.device_id)||compare(a.action_id,b.action_id)||compare(a.capability_version,b.capability_version);
const parameterSchemas=publication=>new Map(publication.snapshot.schemas.map(({binding_id,schema_ref,document})=>[binding_id,{schema_ref,document}]));
const proposalOperation='createRunApprovalProposal';
const resumeOperation='resumeRun';
const preflightWindowNs=2000000000n;
const recoveryPreparationWindowNs=5000000000n;
const utc=value=>{
 const millis=Date.parse(value);
 need(typeof value==='string'&&Number.isSafeInteger(millis)&&millis>=0&&new Date(millis).toISOString()===value,'run_proposal_time');
 return BigInt(millis)*1000000n;
};

export function automationRunStore({db,service,bootId,isOpen,authority,publications,schedules=null,armAdmissionPolicy,eventAgePolicy=null,publicationBindingsCurrentInTransaction,publicationBindingRevocationInTransaction,resolveAutomationSourcesInTransaction,prepareSourceTransport,resolveDeviceActionsInTransaction,prepareRunActionObservations,prepareServiceApproval,authorizeControllerPeerInTransaction,prepareEndorsement,currentPairingInTransaction,sendApprovalProtection,observeApprovalProtection,readRevokedEndorsement,authorizeEndorsementSender,sendArmPreparation,lookupArmPreparation,observeArmPreparation,sendRecoveryPreparation,lookupRecoveryPreparation,observeRecoveryPreparation,readRecoveryDispatchOrigin,submitAutomationCommand,lookupAutomationCommand,readAutomationCommandResult,readAutomationCommandResource}){
 const scope=Object.freeze({authority_service_id:service.service_id,database_instance_id:service.database_instance_id});
 const writer=work=>{
  need(isOpen()&&db.open&&!db.inTransaction&&Number(service.schema_revision)>=5,'run_unavailable');
  return db.transaction(work).immediate();
 };
 function authorize(authentication,operation_id,definition_id,context_id){
  const proof=authority.authorizeInTransaction({authentication,operation_id,definition_id,context_id,scope});
  need(proof&&same(proof.scope,scope)&&proof.operation_id===operation_id&&proof.definition_id===definition_id&&
   proof.context_id===context_id&&proof.request_epoch===service.request_epoch&&
   ['human','service'].includes(proof.actor?.principal_kind),'run_authorization');
  checked('LifecycleDigest',proof.authorization_digest);
  for(const name of ['issuer_id','issuer_epoch','principal_id'])checked('LifecycleId',proof.actor[name]);
  return proof;
 }
 const select='SELECT body_jcs,body_digest,run_id,definition_id,definition_version,ingest_epoch,receive_sequence,context_id,request_digest,dependency_digest FROM automation_run_creation';
 const selectProposal='SELECT proposal_id,run_id,context_id,request_digest,body_jcs,body_digest,audit_jcs,audit_digest FROM automation_run_approval_proposal';
 const dependencyRow=(id,version)=>db.prepare('SELECT body_jcs,body_digest FROM definition_program_dependencies WHERE definition_id=? AND definition_version=?').raw().get(id,version);
 const requestIdentity=(proof,idempotency_key,operation_id='createRun')=>digest({scope,request_epoch:proof.request_epoch,actor:proof.actor,operation_id,idempotency_key});
 function original(row){
  need(row,'run_missing');
  const [text,hash,id,definition,version,epoch,sequence,context,requestDigest,dependencyDigest]=row;
  const saved=JSON.parse(text),run=saved.run,baseline=run.last_receive_cursor;
  need(canonical(saved)===text&&digest(saved)===hash&&run.run_id===id&&run.definition_id===definition&&run.definition_version===version&&
   same(run.scope,scope)&&same(baseline.scope,scope)&&baseline.ingest_epoch===epoch&&baseline.receive_sequence===String(sequence)&&
   saved.authorization.context_id===context&&saved.request_digest===requestDigest&&run.dependency_digest===dependencyDigest&&run.lifecycle==='disarmed'&&run.dispatch_eligible===false,
   'run_integrity');
  return {run,requestDigest};
 }
 function pinDependencies(published){
  const {definition_id,definition_version,artifact_ref,program_digest,bindings_digest}=published.result;
  const body={format:'leafseamer-program-dependencies/1',scope,definition_id,definition_version,artifact_ref,program_digest,bindings_digest,
   dependencies:published.dependencies};
  const text=canonical(body),hash=digest(body),prior=dependencyRow(definition_id,definition_version);
  if(prior)need(prior[0]===text&&prior[1]===hash,'run_dependency_integrity');
  else need(db.prepare('INSERT INTO definition_program_dependencies VALUES(?,?,?,?)').run(definition_id,definition_version,text,hash).changes===1,'run_dependency_storage');
  return hash;
 }
 function readDependencies(run){
  const row=dependencyRow(run.definition_id,run.definition_version);
  need(row,'run_dependency_integrity');
  const [text,hash]=row,body=JSON.parse(text);
  need(canonical(body)===text&&digest(body)===hash&&hash===run.dependency_digest&&same(body.scope,scope)&&
   body.definition_id===run.definition_id&&body.definition_version===run.definition_version&&same(body.artifact_ref,run.artifact_ref),'run_dependency_integrity');
  return {digest:hash,body};
 }
 function readWithDependencies(authentication,id,read,verify=null,finish=null){
  checked('LifecycleId',id);
  return writer(()=>{
   const {run}=original(db.prepare(select+' WHERE run_id=?').raw().get(id));
   const access=authorize(authentication,'getRun',run.definition_id,null);
   const result=read(run,readDependencies(run),access);
   need(same(access,authorize(authentication,'getRun',run.definition_id,null)),'run_authorization_changed');
   verify?.();
   return finish?finish(result,access):result;
  });
 }
 function deviceTargetPlan(run,dependencies,authorization){
  const actions=dependencies.body.dependencies.actions;
  const evidence=actions.length===0?[]:resolveDeviceActionsInTransaction(run.definition_id,actions);
  const groups=new Map();
  for(const {controller_scope,device_action} of evidence){
   const key=canonical(controller_scope),group=groups.get(key)??{controller_scope,actions:new Map()};
   group.actions.set(canonical(device_action),device_action);
   groups.set(key,group);
   need(groups.size<=256&&group.actions.size<=256,'run_target_capacity');
  }
  const targets=[...groups.values()].sort((a,b)=>compare(a.controller_scope.authority_service_id,b.controller_scope.authority_service_id)||
   compare(a.controller_scope.database_instance_id,b.controller_scope.database_instance_id)).map(({controller_scope,actions})=>
   ({controller_scope,device_actions:[...actions.values()].sort(orderAction)}));
  return {authorization,result:{scope,run_id:run.run_id,dependency_digest:dependencies.digest,targets,evidence}};
 }
 function readProposal(row,run){
  need(row,'run_proposal_missing');
  const [id,runId,contextId,requestDigest,text,hash,auditText,auditHash]=row;
  const body=JSON.parse(text),audit=JSON.parse(auditText);
  need(canonical(body)===text&&digest(body)===hash&&canonical(audit)===auditText&&digest(audit)===auditHash&&
   id===body.proposal_id&&runId===body.run_id&&runId===run.run_id&&same(body.scope,scope)&&same(body.artifact_ref,run.artifact_ref)&&
   body.definition_id===run.definition_id&&body.definition_version===run.definition_version&&body.dependency_footprint_ref.digest===run.dependency_digest&&
   same(body.created_authentication_ref,{kind:'authentication',scope,object_id:id,revision:'1',digest:auditHash})&&
   audit.context_id===contextId&&audit.request_digest===requestDigest,'run_proposal_integrity');
  return {proposal:body,proposal_ref:{kind:'run_approval_proposal',scope,object_id:id,revision:'1',digest:hash}};
 }
 function proposalRun(runId){return original(db.prepare(select+' WHERE run_id=?').raw().get(runId)).run;}
 function publicationBasisCurrent(run,published=publications.versionForRunInTransaction(run.definition_id,run.definition_version)){
  need(same(published.result.artifact_ref,run.artifact_ref),'run_dependency_integrity');
  return !Object.hasOwn(published.snapshot.review,'binding_approval_refs')||publicationBindingsCurrentInTransaction(published);
 }
 const approvals=Number(service.schema_revision)>=7?runApprovalStore({db,scope,requestEpoch:service.request_epoch,writer,authority,authorize,
  runOriginal:proposalRun,publicationBasisCurrent,requestIdentity,prepareServiceApproval,prepareEndorsement,currentPairingInTransaction,authorizeControllerPeerInTransaction,canRevoke:Number(service.schema_revision)>=8,
  canAdoptProtection:Number(service.schema_revision)>=9,canCompleteProtection:Number(service.schema_revision)>=17,
  sendApprovalProtection,readRevokedEndorsement,authorizeEndorsementSender,protectionTargets,
  recoveryApprovalBasis,
  bindingRevocationForRun:publicationBindingRevocationInTransaction?run=>{
   const publication=publications.versionForRunInTransaction(run.definition_id,run.definition_version);
   need(same(publication.result.artifact_ref,run.artifact_ref),'run_dependency_integrity');
   return publicationBindingRevocationInTransaction(publication);
  }:null,
  runForProposal(id){const found=db.prepare('SELECT run_id FROM automation_run_approval_proposal WHERE proposal_id=?').pluck().get(id);need(found,'run_proposal_missing');return proposalRun(found);},
  getProposal(reference,run){
   const row=db.prepare(selectProposal+' WHERE proposal_id=?').raw().get(reference.object_id),saved=readProposal(row,run);
   need(same(reference,saved.proposal_ref),'run_approval_proposal_changed');
   readDependencies(run);
   return {saved,audit:JSON.parse(row[6])};
  }}):null;
 const arm=Number(service.schema_revision)>=14?armAdmissionStore({db,scope,policy:armAdmissionPolicy,artifactSize(observation){
  const run=proposalRun(observation.run_id),published=publications.versionForRunInTransaction(run.definition_id,run.definition_version);
  need(same(published.result.artifact_ref,observation.artifact_ref)&&same(run.artifact_ref,observation.artifact_ref),'run_dependency_integrity');
  return Buffer.byteLength(canonical(published.program));
 }}):null;
 const withExecutionAnchors=Number(service.schema_revision)>=26;
 const withDelays=Number(service.schema_revision)>=27;
 const withActionParameters=Number(service.schema_revision)>=28;
 const withCommandDependencies=Number(service.schema_revision)>=31;
 const withRunCommit=Number(service.schema_revision)>=32;
 const withScheduledRuntime=Number(service.schema_revision)>=39&&schedules!==null;
 const executionCommits=withRunCommit?automationExecutionCommit(db):null;
 const recoveryPreviews=Number(service.schema_revision)>=40?automationRecoveryPreviewStore({db,scope,bootId,requestEpoch:service.request_epoch}):null;
 const recoveries=Number(service.schema_revision)>=41?automationRecoveryOperationStore({db,scope,previews:recoveryPreviews}):null;
 const recoveryPreparations=Number(service.schema_revision)>=42?
  automationRecoveryPreparationStore(db,{withBinding:Number(service.schema_revision)>=43,withStart:Number(service.schema_revision)>=44}):null;
 const recoveryDispatch=Number(service.schema_revision)>=45?automationRecoveryDispatchStore(db):null;
 const submissions=Number(service.schema_revision)>=29?automationSubmissionStore(db,{withTerminals:Number(service.schema_revision)>=30}):null;
 const executions=Number(service.schema_revision)>=25?automationExecutionStore(db,scope,bootId,{withAnchors:withExecutionAnchors,withDelays,withActionParameters,withCommandDependencies,withRunCommit,submissions,schedules,withScheduledRuntime}):null;
 const activeSlotRevision=withRunCommit&&db.prepare(`SELECT json_extract(e.body_jcs,'$.activation_slot_revision') FROM automation_execution_head h
  JOIN automation_execution e USING(execution_id) WHERE h.definition_id=?`).pluck();
 const busySlot=schedules&&db.prepare('SELECT 1 FROM automation_execution_head WHERE definition_id=?').pluck();
 function slotFor(definitionId){
  const slot=arm.slot(definitionId);
  if(!executionCommits||slot.active_run_id===null)return slot;
  const revision=activeSlotRevision.get(definitionId)??executionCommits.latest(slot.active_run_id)?.body.slot_revision??slot.revision;
  return {...slot,revision};
 }
 const delayTimers=new Map(),delayFailures=new Set();
 const actionReaders=new Map(),submitting=new Set();
 const submissionSlot=(executionId,stepId)=>executionId+'\0'+stepId;
 let submissionRecovery=null,executionScheduling=true;
 const triggerDecisions=Number(service.schema_revision)>=24?automationTriggerDecisionStore(db,executions):null;
 const armCompletion=Number(service.schema_revision)>=16?armCompletionStore({db,bootId,now:()=>authority.runApprovalTimeInTransaction(),readIngestBaseline}):null;
 function readIngestProgress(run){
  if(!triggerDecisions)return null;
  const frontier=readReceiveCursor(db,scope);
  return frontier.ingest_epoch===run.last_receive_cursor.ingest_epoch?triggerDecisions.latest(run.run_id,frontier):null;
 }
 function withIngestProgress(run,progress=readIngestProgress(run)){
  if(!progress||BigInt(progress.revision)<=BigInt(run.ingest_revision))return run;
  need(BigInt(progress.receive_cursor.receive_sequence)>=BigInt(run.last_receive_cursor.receive_sequence),'trigger_decision_progress');
  const pause_latches=[...new Set([...run.pause_latches,...progress.pause_latches])];
  return {...run,ingest_revision:progress.revision,last_receive_cursor:progress.receive_cursor,pause_latches,trigger_pause_revision:progress.trigger_pause_revision,
   ...(run.lifecycle==='armed'&&pause_latches.length&&{lifecycle:'paused',dispatch_eligible:false})};
 }
 function currentRun(original){
  const run=approvals?.currentRun(original)??original,registered=arm?.forRun(run);
  if(!registered)return withIngestProgress(run);
  const reserved=registered.operation,ready=armCompletion?.read(registered);
  need(BigInt(run.revision)>=BigInt(reserved.committed_run_revision),'run_arm_integrity');
  if(ready){
   const latch=approvals.armRevocationLatch(reserved.approval_ref),revision=ready.completion.completed_run_revision;
   const baseline=ready.completion.ingest_baseline;
   const committed=executionCommits?.latest(run.run_id)?.body;
   if(committed)need(committed.arm_generation===reserved.target_arm_generation&&same(committed.artifact_ref,run.artifact_ref),'execution_commit_integrity');
   const projected={...run,lifecycle:latch?'paused':'armed',active_operation_id:null,arm_generation:reserved.target_arm_generation,
    ...(committed&&{business_state:committed.business_state_after,last_execution_id:committed.execution_ref.object_id}),
    ...(baseline&&{last_receive_cursor:baseline.receive_cursor,ingest_revision:baseline.revision,run_arm_completion_ref:ready.completion_ref,
     arm_source_baselines:baseline.source_baseline_refs}),
    pause_latches:latch?[latch]:run.pause_latches,dispatch_eligible:false};
   const executionHead=executions?.active(projected),active=executionHead?.execution;
   if(active){
    const protectedExecution=active.service_boot_id!==bootId||(withExecutionAnchors&&!executionHead.anchor)||
     executionHead.protection||delayFailures.has(active.execution_id);
    projected.active_execution_id=active.execution_id;
    projected.lifecycle=latch||protectedExecution?'paused':'running';
    if(protectedExecution)projected.pause_latches=[...projected.pause_latches,active.execution_id];
   }
   const progress=readIngestProgress(projected),currentRevision=progress?.trigger_pause_revision??active?.run_revision??committed?.run_revision_after??revision;
   need(BigInt(currentRevision)>=BigInt(revision)&&(latch?BigInt(run.revision)>BigInt(currentRevision):run.revision===currentRevision),'run_arm_completion_integrity');
   const current=withIngestProgress(projected,progress),recovery=recoveries?.active(run.run_id);
   if(!recovery)return current;
   const {operation}=recovery;
   need(current.lifecycle==='paused'&&same(executionHead?.execution_ref,operation.source_execution_ref)&&
    BigInt(current.revision)>=BigInt(operation.committed_run_revision),'run_recovery_operation_integrity');
   return {...current,active_operation_id:operation.operation_id,active_execution_id:operation.new_execution_id,arm_generation:operation.recovery_generation,
    lifecycle:recoveryDispatch&&recoveryCanRun(recovery,current.revision)?'running':'paused'};
  }
  return withIngestProgress({...run,active_operation_id:reserved.operation_id,arm_generation:reserved.target_arm_generation});
 }
 const triggerProgramKey=(id,version)=>canonical([id,version]);
 async function prepareTriggerPrograms(versions,runIds){
  const policy=eventAgePolicy?.();
  if(!policy)return null;
  need(!db.inTransaction,'trigger_program_transaction');
  const programs=new Map();let bytes=0;
  for(const [id,version,size] of versions()){
   bytes+=size;
   need(bytes<=67108864&&programs.size<policy.max_condition_nodes,'trigger_program_capacity');
   programs.set(triggerProgramKey(id,version),publications.prepareExecutionProgram(id,version));
  }
  const grants=executions&&programs.size?await prepareExecutionGrants(runIds):null;
  return {policy,programs,grants};
 }
 function executionGrantInputs(run){
  const registered=arm.forRun(run),ready=armCompletion.read(registered),{operation}=registered;
  need(ready&&same(ready.completion_ref,run.run_arm_completion_ref),'execution_arm_integrity');
  const preflight=originalArmPreflight(operation);
  const grants=operation.targets.map(target=>{
   const plan=historicalPlan(registered,target,preflight),lookup=preparationLookup(plan),found=preparationReceipts.read({...plan,lookup});
   need(found,'execution_arm_integrity');
   const request={lookup,receipt_ref:found.receipt_ref,grant_ref:found.target_grant_ref};
   need(ready.completion.evidence.targets.some(value=>same(value.request,request)),'execution_arm_integrity');
   return {request,pairing_ref:plan.pairing_ref,grant:found.record.applied_receipt.grant};
  });
  return {preflight,operation,grants};
 }
 async function prepareExecutionGrants(runIds){
  const runs=[...runIds()];
  const captured=writer(()=>runs.flatMap(id=>{
   const run=currentRun(proposalRun(id));
   if(run.lifecycle!=='armed'||run.active_execution_id!==null||!run.run_arm_completion_ref)return [];
   const inputs=executionGrantInputs(run);
   need(BigInt(inputs.grants.length)<=BigInt(inputs.operation.budget.required.work_units),'execution_grant_capacity');
   return inputs.grants.map(value=>({runId:id,...value}));
  }));
  return observeExecutionGrants(captured);
 }
 async function observeExecutionGrants(captured){
  const observed=await inBatches(captured,async item=>{
   const request={...item.request,...(item.schedule_device_ids&&{schedule_device_ids:item.schedule_device_ids})};
   let reply;
   try{reply=await observeArmPreparation(item.pairing_ref.id,structuredClone(request));}
   catch{reply=null;}
   return {item,request,reply};
  });
  return new Map(observed.map(result=>[result.request.lookup.preparation_id,result]));
 }
 function executionActions(run,publication,steps,preflight,grants){
  const ids=new Set(steps.filter(({step})=>step.kind==='action').map(({step})=>step.action_binding_id));
  const deps=publication.dependencies.actions.filter(({binding_id})=>ids.has(binding_id));
  need(deps.length===ids.size,'execution_plan_binding');
  const bindings=deps.length?resolveDeviceActionsInTransaction(run.definition_id,deps):[];
  need(bindings.length===deps.length,'execution_plan_binding');
  return bindings.map((binding,index)=>{
   const dep=deps[index];
   need(binding.binding_id===dep.binding_id&&preflight.basis.device_bindings.some(value=>same(value,binding)),'execution_plan_binding');
   const grant=grants.find(item=>same(item.request.lookup.controller_scope,binding.controller_scope));
   need(grant&&grant.grant.device_actions.some(value=>same(value,binding.device_action)),'execution_plan_grant');
   return {dep,binding,grant};
  });
 }
 function scheduledGrantDevices(targets){
  const groups=new Map();
  for(const {binding,grant} of targets){
   if(!groups.has(grant))groups.set(grant,new Set());
   groups.get(grant).add(binding.device_action.device_id);
  }
  return new Map([...groups].map(([grant,ids])=>[grant,[...ids].sort(compare)]));
 }
 function planExecution(run,publication,approval,selectedBranch,prepared){
  if(!executions)return null;
  const {preflight,operation,grants}=executionGrantInputs(run),slot=slotFor(run.definition_id);
  need(slot.active_run_id===run.run_id&&slot.active_operation_id===operation.operation_id&&(withRunCommit||slot.revision===operation.committed_slot_revision)&&
   same(operation.approval_ref,approval.approval_ref),'execution_slot_changed');
  arm.assertPolicy(preflight.observation,preflight.basis);
  const steps=selectedExecutionSteps(publication.compiled,run,selectedBranch);
  const targets=executionActions(run,publication,steps,preflight,grants);
  const scheduled=prepared.requireScheduled?scheduledGrantDevices(targets):null;
  const grantBasis=new Map(),actions=new Map(),recheckGrants=[];
  let deadline=proposalWindowEnd(approval.saved.proposal);
  for(const {dep,binding,grant} of targets){
   const id=grant.request.lookup.preparation_id;
   if(!grantBasis.has(id)){
    const observed=prepared.grants?.get(id);
    if(!observed?.reply||observed.item.runId!==run.run_id||!same(observed.item.request,grant.request)||!same(observed.item.pairing_ref,grant.pairing_ref))return null;
    const expected=scheduled?{...grant.request,schedule_device_ids:scheduled.get(grant)}:grant.request;
    if(!same(observed.request,expected))return null;
    const {original,assertCurrentInTransaction}=observed.reply;
    try{if(original.state!=='eligible'||!same(original.request,expected)||assertCurrentInTransaction()!==true)return null;}
    catch{return null;}
    const pair=currentPairingInTransaction(grant.pairing_ref);
    const limit=BigInt(original.eligible_before_ns),expires=BigInt(pair.expires_ns);
    if(limit<deadline)deadline=limit;if(expires<deadline)deadline=expires;
    grantBasis.set(id,{...grant.request,pairing_ref:grant.pairing_ref,eligible_before_ns:String(limit)});
    recheckGrants.push(()=>{
     need(assertCurrentInTransaction()===true,'execution_grant_changed');
     currentPairingInTransaction(grant.pairing_ref);
    });
   }
   actions.set(dep.binding_id,{action_binding_ref:{id:dep.binding_id,revision:'1',digest:dep.binding_digest},target:binding,
    catalog_ref:dep.catalog_ref,module_ref:dep.module_ref,parameter_schema_ref:dep.parameter_schema_ref,result_schema_ref:dep.result_schema_ref,
    grant_ref:grant.request.grant_ref});
  }
  if(authority.runApprovalTimeInTransaction().upper>=deadline)return null;
  return {steps:steps.map(item=>item.step.kind==='action'?{...item,action:actions.get(item.step.action_binding_id)}:item),
   budget:operation.budget.required,activation_slot:slot,grant_basis:[...grantBasis.values()].sort((a,b)=>compare(a.lookup.preparation_id,b.lookup.preparation_id)),
   program_digest:publication.result.program_digest,graph_digest:publication.program.graph_digest,policy_ref:publication.draft.content.policy_ref,
   business_state:run.business_state,target_state_id:targetState(publication,run.business_state,selectedBranch),
   approved_before_ns:String(deadline),assertCurrentInTransaction(){
    for(const recheck of recheckGrants)recheck();
    need(authority.runApprovalTimeInTransaction().upper<deadline&&publicationBasisCurrent(run,publication),'execution_grant_changed');
   }};
 }
 function targetState(publication,state,selected){
  const {program}=publication.compiled.compiled;
  if(program.kind==='rule'){need(state===null&&selected==='rule','execution_commit_state');return null;}
  const branch=program.transitions.find(item=>item.transition_id===selected&&item.from_state===state?.state_id);
  need(branch,'execution_commit_state');
  return branch.to_state;
 }
 function scheduleSelection(run,publication,firstStepId){
  let remaining=BigInt(executionGrantInputs(run).operation.budget.required.work_units);
  const charge=amount=>{remaining-=BigInt(amount);need(remaining>=0n,'execution_schedule_capacity');};
  const {compiled}=publication;
  const evaluation=evaluateScheduledCondition(compiled,run,charge);
  if(evaluation.outcome!=='matched')return {state:'unqualified',reason:evaluation.outcome};
  const first=selectedExecutionSteps(compiled,run,evaluation.selected_branch).find(({step})=>step.step_id===firstStepId);
  if(first?.step.kind!=='action'||first.dependencies.length)return {state:'unqualified',reason:'first_step_not_ready'};
  const {guard,parameters}=compiled.stepPlan(firstStepId),snapshots=new Map();
  charge(guard.nodes+parameters.reduce((total,{value})=>total+value.nodes,0));
  const guardValue=evaluateExpression(guard,snapshots).value;
  if(guardValue!==true)return {state:'unqualified',reason:guardValue===false?'first_step_guard_false':'first_step_guard_unknown'};
  if(parameters.some(({value})=>evaluateValue(value,snapshots).state!=='available'))return {state:'unqualified',reason:'first_step_parameters_unknown'};
  return {state:'ready_for_preparation',selected_branch:evaluation.selected_branch};
 }
 function inspectScheduleInTransaction({definition_id,definition_version,definition_artifact_ref,first_step_id}){
  need(db.inTransaction,'execution_schedule_transaction');
  const slot=slotFor(definition_id);
  if(slot.active_run_id===null)return {state:'unqualified',reason:'run_not_armed'};
  const run=currentRun(proposalRun(slot.active_run_id));
  if(run.active_execution_id!==null)return {state:'unqualified',reason:'run_busy'};
  if(run.lifecycle!=='armed'||run.pause_latches.length)return {state:'unqualified',reason:'run_not_armed'};
  if(run.definition_version!==definition_version||!same(run.artifact_ref,definition_artifact_ref))return {state:'unqualified',reason:'run_version_changed'};
  try{approvals.currentAuthorityInTransaction(run);}
  catch(error){if(error?.code==='run_preflight_approval')return {state:'unqualified',reason:'run_approval_unavailable'};throw error;}
  const publication=publications.versionForRunInTransaction(definition_id,definition_version,{withEvaluator:true});
  if(!Object.hasOwn(publication.snapshot.review,'binding_approval_refs')||!publicationBasisCurrent(run,publication))
   return {state:'unqualified',reason:'publication_unapproved'};
  return scheduleSelection(run,publication,first_step_id);
 }
 function triggerEvaluator(prepared,origin,liveEvidence){
  let work=0;
  const protect=reason=>({disposition:'protected',reason});
  return (run,target)=>{
   if(!prepared)return protect('event_age_policy_unconfigured');
   const {policy,programs}=prepared,compiled=programs.get(triggerProgramKey(run.definition_id,run.definition_version));
   if(!compiled)return protect('trigger_program_unavailable');
   const {publication}=compiled;
   compiled.assertOriginalInTransaction();
   if(!Object.hasOwn(publication.snapshot.review,'binding_approval_refs')||!publicationBasisCurrent(run,publication))return protect('publication_binding_unapproved');
   if(!run.arm_source_baselines.some(value=>same(value,liveEvidence.predecessor.baseline_ref)))return protect('arm_source_baseline_changed');
   let approval;
   try{approval=approvals.currentAuthorityInTransaction(run);}
   catch(error){if(error.code==='run_preflight_approval')return protect('run_approval_unavailable');throw error;}
   const reference=publication.draft.content.policy_ref,registration=target.bindings[0].registration_ref;
   const limit=policy.limitInTransaction(reference,origin.source_key.authority_service_id,registration);
   if(!limit)return protect('event_age_policy_unavailable');
   const captureSemantics={discrete:'event_occurrence',sampled:'sampled_observation'}[origin.binding_ref.delivery_semantics];
   if(limit.capture_semantics!==liveEvidence.capture_semantics||limit.capture_semantics!==captureSemantics)return protect('event_capture_semantics_changed');
   if(liveEvidence.upper_age_ms>limit.max_age_ms)return protect('event_age_limit_exceeded');
   const result=evaluateTriggerCondition(publication.compiled,run,target,origin,amount=>{
    work+=amount;need(work<=policy.max_condition_nodes,'trigger_condition_capacity');
   });
   if(!result)return protect('event_payload_schema_changed');
   const evaluation={...result,origin_digest:digest(origin),age_evidence_ref:liveEvidence.age_evidence_ref,policy_ref:reference,
    approval_digest:limit.approval_digest,max_age_ms:limit.max_age_ms};
   if(result.outcome==='matched'){
    const plan=planExecution(run,publication,approval,evaluation.selected_branch,prepared);
    return plan?{disposition:'accepted',reason:'execution_plan_sealed',evaluation,plan}:
     {...protect(executions?'execution_grant_unavailable':'execution_admission_unavailable'),evaluation};
   }
   return {disposition:result.outcome,reason:result.outcome,evaluation};
  };
 }
 function recordedSubmission(executionId,stepId){
  const value=executions.read(executionId)??(recoveryDispatch&&readRecoveryDispatch(executionId));
  const submission=value?.steps.find(({plan})=>plan.step_id===stepId)?.submission;
  need(submission?.attempt,'execution_submission_missing');
  const recovered=value.execution.origin_kind==='recovery';
  return {value,submission,ledger:recovered?recoveryDispatch.submissions:submissions,owner:recovered?recoveryDispatch:executions};
 }
 function adoptSubmission(original,reply){
  const {execution_ref,step_id}=original.body.request.action;
  const {submission,ledger,owner}=recordedSubmission(execution_ref.object_id,step_id);
  need(submission.digest===original.digest&&same(submission.attempt,original.attempt),'execution_submission_changed');
  const adopted=ledger.adopt(submission,reply);
  if(adopted?.decision!=='accepted')owner.protect(execution_ref.object_id,'execution_runtime_unavailable');
  return adopted;
 }
 async function lookupSubmission(original){
  const {body}=original;
  writer(()=>currentPairingInTransaction(body.pairing_ref));
  const result=await lookupAutomationCommand(body.pairing_ref.id,structuredClone(body.request));
  need(result?.state==='found'||result?.state==='not_found'&&result.adjudication===null,'execution_submission_lookup');
  need(result.state!=='found'||result.adjudication,'execution_submission_lookup');
  return result.adjudication;
 }
 function adoptLookup(original,reply){
  currentPairingInTransaction(original.body.pairing_ref);
  return adoptSubmission(original,reply);
 }
 async function readAndAdoptTerminal(executionId,stepId,running){
  const slot=submissionSlot(executionId,stepId);
  if(submitting.has(slot))return;
  submitting.add(slot);
  try{
   const original=writer(()=>{
    const {submission}=recordedSubmission(executionId,stepId);
    if(submission.terminal)return null;
    currentPairingInTransaction(submission.body.pairing_ref);
    return submission;
   });
   if(!original)return;
   const {pairing_ref,request}=original.body,reply=await readAutomationCommandResult(pairing_ref.id,structuredClone(request));
   if(!running())return;
   const result=writer(()=>{
    const {value,submission,ledger,owner}=recordedSubmission(executionId,stepId);
    currentPairingInTransaction(pairing_ref);
    need(submission.digest===original.digest&&same(submission.adjudication,original.adjudication),'execution_submission_changed');
    const observed=ledger.adoptTerminal(submission,reply);
    if(!observed)return false;
    const {plan}=value.steps.find(step=>step.plan.step_id===stepId);
    const {outcome}=observed.state;
    const dependent=value.steps.some(step=>step.plan.dependencies.some(edge=>edge.from_step_id===stepId&&edge.edge_kind!=='sequence'));
    const recovered=owner===recoveryDispatch;
    if(outcome!=='succeeded'&&(recovered||!mayContinueAfterFailure(plan,observed.state)||dependent))
     owner.protect(executionId,'execution_runtime_unavailable');
    return {recovered,success:outcome==='succeeded'};
   });
   if(result?.recovered&&result.success)queueRecoveryDispatch(executionId);
   return Boolean(result);
  }catch{
   // 配对变化、连接中断或落账失败均保留待核对项。查原不重发原动作。
  }finally{submitting.delete(slot);}
 }
 function startSubmissionRecovery(){
  need(!submissionRecovery,'execution_submission_recovery_started');
  let active=true,timer;
  const ledgers=[submissions,...(recoveryDispatch?[recoveryDispatch.submissions]:[])].map(ledger=>({ledger,cursor:['',''],terminalCursor:['','']}));
  const running=()=>active&&isOpen()&&db.open;
  async function recoverTerminals(state){
   if(!state.ledger.adoptTerminal)return;
   const page=writer(()=>state.ledger.pendingTerminalAfter(state.terminalCursor));
   state.terminalCursor=page.at(-1)??['',''];
   for(const [executionId,stepId] of page){
    if(!running())return;
    await readAndAdoptTerminal(executionId,stepId,running);
   }
  }
  async function recoverAttempts(state){
   const page=writer(()=>state.ledger.pendingAfter(state.cursor));
   state.cursor=page.at(-1)??['',''];
   for(const [executionId,stepId] of page){
    if(!running())return;
    const slot=submissionSlot(executionId,stepId);
    if(submitting.has(slot))continue;
    submitting.add(slot);
    try{
     const original=writer(()=>{
      const {submission}=recordedSubmission(executionId,stepId);
      if(submission.adjudication)return null;
      adoptSubmission(submission,null);
      return submission;
     });
     if(!original)continue;
     const found=await lookupSubmission(original);
     if(found&&running())writer(()=>adoptLookup(original,found));
    }catch{
     // A later scan can query this same attempt again after transport or storage recovers.
    }finally{submitting.delete(slot);}
   }
  }
  async function scan(){
   try{
    for(const state of ledgers){
     if(!running())return;
     await recoverTerminals(state);
     if(!running())return;
     await recoverAttempts(state);
    }
   }catch{
    // A failed scan leaves all unresolved attempts available for the next pass.
   }finally{
    if(active&&isOpen()&&db.open)timer=setTimeout(scan,5000).unref();
   }
  }
  submissionRecovery=()=>{active=false;clearTimeout(timer);submissionRecovery=null;};
  timer=setTimeout(scan,5000).unref();
 }
 function scheduleExecution(item,{sample,checkConnection,predecessor,readEvent,recheckDelivery,scheduleClock,admissionGrants=null,firstParameters=null}){
  if(!executionScheduling)return;
  const id=item.execution_ref.object_id;
  const running=()=>executionScheduling&&isOpen()&&db.open;
  const scheduled=scheduleClock!==undefined;
  const canStart=(execution,step)=>!scheduled||schedules.canStartActionInTransaction(execution,step.plan,scheduleClock());
  let lastTick=null,terminalPoll=0,awaitingInitialProjection=!scheduled;
  function currentSample(){
   const result=sample(),tick=BigInt(result.tick_ns);
   need(lastTick===null||tick>=lastTick,'execution_clock_changed');
   lastTick=tick;
   return result;
  }
  function assertCurrent(execution,publication,allowPendingProjection=false){
   const run=currentRun(proposalRun(execution.run_id)),approval=approvals.currentAuthorityInTransaction(run);
   const inputs=executionGrantInputs(run),{preflight,operation}=inputs;
   need(!delayFailures.has(id)&&run.active_execution_id===id&&run.lifecycle==='running'&&run.revision===execution.run_revision&&
    run.pause_latches.length===0&&same(operation.approval_ref,approval.approval_ref)&&publicationBasisCurrent(run,publication),
    'execution_delay_inactive');
   arm.assertPolicy(preflight.observation,preflight.basis);
   for(const {pairing_ref} of execution.grant_basis)currentPairingInTransaction(pairing_ref);
   const sources=resolveAutomationSourcesInTransaction(readDependencies(run).body.dependencies.inputs);
   need(execution.origin_kind===(scheduled?'schedule':'trigger'),'execution_action_origin');
   const onArmedBaseline=source=>source&&run.arm_source_baselines.some(baseline=>same(source.baseline_ref,baseline));
   const synchronized=({state,source})=>state==='synchronized'&&source?.continuity&&onArmedBaseline(source);
   if(scheduled){
    need(sources.every(synchronized),'execution_delay_source');
    return {...inputs,run,sources};
   }
   checkConnection();
   const sameConnection=source=>source.continuity.connection_generation===predecessor.connection_generation;
   const synchronizedTrigger=sources.some(item=>synchronized(item)&&same(item.source.baseline_ref,predecessor.baseline_ref)&&sameConnection(item.source));
   if(allowPendingProjection&&sources.some(item=>item.state==='baseline_required')){
    // 来源事件已经落库，订阅随后交付同一来源的投影。等待期间不求值、不提交目标动作。
    const observed=readEvent(execution.receive_cursor,predecessor);
    const pending=({state,source})=>state==='baseline_required'&&onArmedBaseline(source)&&
     same(source.baseline_ref,predecessor.baseline_ref)&&same(source.source_key,observed.origin.source_key)&&
     source.source_revision===predecessor.source_revision;
    if(observed.projectionPending&&observed.origin_digest===execution.origin_digest&&sources.some(pending)&&sources.every(item=>synchronized(item)||pending(item))){
     recheckDelivery();
     need(BigInt(currentSample().upper_ns)<BigInt(execution.approved_before_ns),'execution_delay_source');
     return {projectionPending:true};
    }
   }
   need(sources.every(synchronized)&&synchronizedTrigger,'execution_delay_source');
   return {...inputs,run,sources};
  }
  async function commitExecution(){
   const initial=writer(()=>{
    const value=executions.read(id),state=assertCurrent(value.execution);
    executions.completionClock(value,currentSample());
    const grants=value.execution.grant_basis.map(basis=>{
     const grant=state.grants.find(item=>same(item.request.lookup,basis.lookup));
     need(grant&&same(grant.pairing_ref,basis.pairing_ref)&&same(grant.request.grant_ref,basis.grant_ref)&&
      same(grant.request.receipt_ref,basis.receipt_ref),'execution_commit_grant');
     return grant;
    });
    return {run:state.run,sources:state.sources,grants};
   });
   const observed=await inBatches(initial.grants,async grant=>{
    const result=await observeArmPreparation(grant.pairing_ref.id,structuredClone(grant.request));
    need(result?.original.state==='eligible'&&same(result.original.request,grant.request),'execution_commit_grant');
    return result;
   });
   if(!running())return;
   const bundle=publications.prepareExecutionProgram(initial.run.definition_id,initial.run.definition_version);
   writer(()=>{
    const value=executions.read(id),{execution,execution_ref,anchor,steps}=value;
    const {run,sources,operation}=assertCurrent(execution,bundle.publication);
    bundle.assertOriginalInTransaction();
    need(same(execution_ref,item.execution_ref)&&same(run,initial.run)&&same(sources,initial.sources)&&
     same(run.business_state,execution.business_state)&&same(execution.artifact_ref,bundle.publication.result.artifact_ref),'execution_commit_changed');
    const slot=slotFor(run.definition_id),evidence=completionEvidence(steps);
    need(evidence&&same(slot,{...execution.activation_slot,revision:execution.activation_slot_revision}),'execution_commit_evidence');
    const target=targetState(bundle.publication,execution.business_state,execution.selected_branch);
    need(target===execution.target_state_id,'execution_commit_state');
    const business_state_after=target===null?null:{state_id:target,revision:nextExecutionRevision(execution.business_state.revision)};
    const recheckGrants=()=>{for(const result of observed)need(result.assertCurrentInTransaction()===true,'execution_commit_grant');};
    recheckGrants();
    const clock=currentSample();
    executions.completionClock(value,clock);
    need(observed.every(result=>BigInt(clock.upper_ns)<BigInt(result.original.eligible_before_ns)),'execution_commit_grant');
    executionCommits.write({format:'leafseamer.execution-commit/1',execution_ref,run_id:run.run_id,artifact_ref:run.artifact_ref,
     arm_generation:run.arm_generation,approval_ref:operation.approval_ref,anchor_ref:anchor.ref,slot_revision:slot.revision,
     run_revision_before:run.revision,run_revision_after:nextExecutionRevision(run.revision),
     business_state_before:run.business_state,business_state_after,source_digest:digest(sources),
     receive_cursor:run.last_receive_cursor,ingest_revision:run.ingest_revision,clock,steps:evidence},
    bytes=>executions.reserveSubmission(value,item,bytes,1));
    need(publicationBasisCurrent(run,bundle.publication)&&
     same(sources,resolveAutomationSourcesInTransaction(readDependencies(run).body.dependencies.inputs)),'execution_commit_changed');
    checkConnection?.();recheckGrants();
    const finalClock=currentSample();
    executions.completionClock(value,finalClock);
    need(observed.every(result=>BigInt(finalClock.upper_ns)<BigInt(result.original.eligible_before_ns)),'execution_commit_grant');
   });
   actionReaders.delete(id);
  }
  async function prepareAction(stepId){
   const value=executions.read(id);
   const runId=value.execution.run_id,{definition_id,artifact_ref}=value.execution;
   const run=proposalRun(runId),bundle=publications.prepareExecutionProgram(definition_id,run.definition_version);
   const {publication}=bundle;
   need(same(publication.result.artifact_ref,artifact_ref),'execution_action_publication');
   function snapshot(deadline,expectedParameters=null){
    const current=executions.read(id),step=current.steps.find(item=>item.plan.step_id===stepId);
    need(same(current.execution_ref,item.execution_ref)&&step?.ready,'execution_action_inactive');
    const {execution}=current,binding=step.plan.payload.binding;
    bundle.assertOriginalInTransaction();
    const {grants}=assertCurrent(execution,publication);
    const observed=scheduled?null:readEvent(execution.receive_cursor);
    if(scheduled)need(canStart(execution,step),'schedule_execution_window');
    else need(observed.origin_digest===execution.origin_digest,'execution_action_origin');
    const grant=grants.find(candidate=>same(candidate.request.grant_ref,binding.grant_ref));
    const approved=execution.grant_basis.find(candidate=>same(candidate.grant_ref,binding.grant_ref));
    need(grant&&approved&&same(grant.request.lookup,approved.lookup)&&same(grant.request.receipt_ref,approved.receipt_ref)&&
     same(grant.pairing_ref,approved.pairing_ref)&&same(binding.target.controller_scope,approved.lookup.controller_scope),'execution_action_grant');
    const clock=currentSample();
    if(expectedParameters){
     need(!current.protection&&same(step.prepared_parameters?.ref,expectedParameters),'execution_action_changed');
     executions.assertActionWindow(current,step,clock,deadline);
    }else executions.assertActionCurrent(current,step,clock,deadline);
    return {current,step,grant,observed,clock};
   }
   const originalDeadline=value.execution.approved_before_ns;
   const captured=writer(()=>snapshot(originalDeadline));
   const {compiled}=publication,stepPlan=compiled.stepPlan(stepId);
   need(stepPlan,'execution_action_plan');
   const snapshots=captured.observed?eventInputSnapshots(compiled.compiled.input_bindings,captured.observed.origin):new Map();
   need(evaluateExpression(stepPlan.guard,snapshots).value===true,'execution_action_guard');
   const prevalidated=firstParameters?.step_id===stepId&&same(firstParameters.artifact_ref,artifact_ref)?firstParameters.result:null;
   const {grant}=captured;
   const prior=stepId===item.admission?.first_step_id?admissionGrants?.get(grant.request.lookup.preparation_id):null;
   const observe=async()=>{
    // 受理时的 Controller 观察已经核验同一首步目标。复用仍须核对原配对和期限。
    if(prior?.reply&&same(prior.item.request,grant.request)&&same(prior.item.pairing_ref,grant.pairing_ref)&&
       prior.request.schedule_device_ids?.includes(captured.step.plan.payload.binding.target.device_action.device_id)){
     try{
      if(writer(()=>prior.reply.assertCurrentInTransaction()===true))return {proof:prior.reply,request:prior.request};
     }catch(error){if(error.code!=='peer_pairing_preparation_expired')throw error;}
    }
    const request=structuredClone(grant.request);
    return {proof:await observeArmPreparation(grant.pairing_ref.id,request),request};
   };
   const [parameters,eligibility]=await Promise.allSettled([
    prevalidated??compiled.parameterExecution({snapshots,schemas:parameterSchemas(publication)}).resolve(stepId),observe()
   ]);
   if(parameters.status==='rejected')throw parameters.reason;
   const result=parameters.value;
   need(result.status==='validated'&&result.action_binding_id===captured.step.plan.payload.binding.action_binding_ref.id,'execution_action_parameters');
   assertRunGrantParameters(captured.grant.grant,captured.step.plan.payload.binding.target.device_action,result.parameters);
   if(!isOpen()||!db.open)return;
   if(eligibility.status==='rejected')throw eligibility.reason;
   const {proof,request}=eligibility.value,returned=proof?.original;
   need(returned?.state==='eligible'&&same(returned.request,request),'execution_action_grant');
   const deadline=BigInt(returned.eligible_before_ns)<BigInt(originalDeadline)?returned.eligible_before_ns:originalDeadline;
   const recheck=({current,grant})=>{
    bundle.assertOriginalInTransaction();
    assertCurrent(current.execution,publication);
    need(same(grant,captured.grant)&&proof.assertCurrentInTransaction()===true,'execution_action_grant');
   };
   const parametersRef=writer(()=>{
    const latest=snapshot(deadline);
    need(same(latest.step.ready.ref,captured.step.ready.ref)&&same(latest.grant,captured.grant),'execution_action_changed');
    recheck(latest);
    executions.sealParameters(latest.current,latest.step,item,result,deadline,latest.clock,()=>recheck(latest),currentSample);
    if(submissions)return executions.read(id).steps.find(({plan})=>plan.step_id===stepId).prepared_parameters.ref;
   });
   if(!submissions)return;
   const capture=()=>{
    const latest=snapshot(deadline,parametersRef);
    recheck(latest);
    return latest;
   };
   const submissionDigest=writer(()=>{
    const {current,step,grant,clock}=capture();
    return submissions.plan(current,step,grant,clock,(bytes,rows)=>executions.reserveSubmission(current,item,bytes,rows));
   });
   const original=writer(()=>{
    const {current,step,clock}=capture();
    need(step.submission?.digest===submissionDigest,'execution_submission_changed');
    return {...step.submission,attempt:submissions.attempt(current,step,clock)};
   });
   // This continuation is the only owner of the committed ATTEMPT result.
   const slot=submissionSlot(id,stepId);
   submitting.add(slot);
   try{
    writer(()=>{
     const {step}=capture();
     need(step.submission?.digest===original.digest&&same(step.submission.attempt,original.attempt),'execution_submission_changed');
    });
    const {body}=original;
    const reply=await submitAutomationCommand(body.pairing_ref.id,structuredClone(body.request));
    if(isOpen()&&db.open)writer(()=>adoptSubmission(original,reply));
   }catch{
    if(!isOpen()||!db.open)return;
    writer(()=>adoptSubmission(original,null));
    let found;
    try{found=await lookupSubmission(original);}
    catch{return;}
    if(found&&isOpen()&&db.open)writer(()=>adoptLookup(original,found));
   }finally{submitting.delete(slot);}
  }
  async function wake(){
   delayTimers.delete(id);
   if(!running())return;
   try{
    let {next,actions,pending,completed}=writer(()=>{
     const value=executions.read(id);
     need(value&&same(value.execution_ref,item.execution_ref),'execution_delay_inactive');
     const state=assertCurrent(value.execution,undefined,awaitingInitialProjection);
     if(state.projectionPending)return {next:20000000n,actions:[],pending:[],completed:false};
     awaitingInitialProjection=false;
     let next=executions.advanceDelays(value,item,currentSample,()=>assertCurrent(value.execution));
     const actions=[];
     if(withActionParameters)for(const step of value.steps){
      if(step.plan.payload.kind!=='action'||!step.ready||step.prepared_parameters)continue;
      if(canStart(value.execution,step))actions.push(step);
      else if(next===null||next>50000000n)next=50000000n;
     }
     const pending=withCommandDependencies?value.steps.filter(step=>step.submission?.state==='accepted'&&!step.submission.terminal):[];
     return {next,actions,pending,completed:withRunCommit&&completionEvidence(value.steps)!==null};
    });
    if(completed){
     await commitExecution();
     return;
    }else if(actions.length){
     await prepareAction(actions[0].plan.step_id);
     if(withCommandDependencies||next!==null||actions.length>1)next=0n;
    }else if(pending.length&&next!==0n){
     const step=pending[terminalPoll++%pending.length];
     const adopted=await readAndAdoptTerminal(id,step.plan.step_id,running);
     const wait=adopted?0n:100000000n;
     if(next===null||next>wait)next=wait;
    }
    if(next!==null&&running())armTimer(next);
   }catch(error){
    if(!running())return;
    const reason=error.code==='delay_window_expired'?error.code:'execution_runtime_unavailable';
    try{writer(()=>executions.protect(id,reason));}
    catch{delayFailures.add(id);}
    return;
   }
  }
  function armTimer(wait){
   const milliseconds=(wait+999999n)/1000000n;
   const timer=setTimeout(wake,Number(milliseconds<1000n?milliseconds:1000n));
   timer.unref();
   delayTimers.set(id,timer);
  }
  if(withActionParameters)actionReaders.set(id,(bindingId,socket,request)=>{
   const verify=()=>writer(()=>{
    const value=executions.read(id),step=value.steps.find(item=>item.plan.step_id===request.step_id);
    need(value.execution.service_boot_id===bootId&&!value.protection&&same(value.execution_ref,request.execution_ref)&&
     step?.prepared_parameters&&same(step.prepared_parameters.ref,request.parameters_ref),'execution_action_binding');
    if(submissions)need(['attempt_recorded','accepted'].includes(step.submission?.state)&&
     same(step.submission.body.request.action,request),'execution_action_binding');
    assertCurrent(value.execution);
    const paired=authorizeControllerPeerInTransaction(bindingId,socket),target=peerScope(paired.peer);
    const grant=value.execution.grant_basis.find(candidate=>same(candidate.grant_ref,step.plan.payload.binding.grant_ref));
    need(grant&&paired.record.state==='activated'&&paired.peer.service_kind==='controller'&&
     same(pairingReference(paired.record),grant.pairing_ref)&&same(target,step.plan.payload.binding.target.controller_scope)&&
     same(request.preparation,{lookup:grant.lookup,grant_ref:grant.grant_ref,receipt_ref:grant.receipt_ref}),'execution_action_peer');
    const eligible_before_ns=step.prepared_parameters.body.eligible_before_ns;
    executions.assertActionWindow(value,step,currentSample(),eligible_before_ns);
    need(canStart(value.execution,step),'schedule_execution_window');
    return {execution:value.execution,step_plan:step.plan,step_ready:step.ready,parameters:step.prepared_parameters,eligible_before_ns,
     ...(value.execution.origin_kind==='schedule'&&{schedule:schedules.readActionScheduleInTransaction(value.execution,step.plan)})};
   });
   const body=verify();
   let current=true;
   return Object.freeze({body:structuredClone(body),beforeSend(){
    need(current,'execution_action_binding');
    try{
     const checked=verify();
     need(same(checked,body),'execution_action_binding');
     return checked;
    }catch(error){current=false;throw error;}
   }});
  });
  armTimer(0n);
 }
 function priorProposal(identity,run,requestDigest,authentication){
  const row=db.prepare(selectProposal+' WHERE identity_digest=?').raw().get(identity);
  if(!row)return null;
  const access=authorize(authentication,'getRun',run.definition_id,null);
  need(row[1]===run.run_id&&row[3]===requestDigest,'run_proposal_conflict');
  const saved=readProposal(row,run);
  need(same(access,authorize(authentication,'getRun',run.definition_id,null)),'run_authorization_changed');
  return saved;
 }
 function checkWindow(before,after,source){
  const {lower,upper}=authority.runApprovalTimeInTransaction();
  need(lower>=BigInt(source.approval.not_before_ns)&&upper<after&&before>=BigInt(source.approval.not_before_ns)&&after<=BigInt(source.approval.expires_ns),'run_proposal_time');
 }
 async function observeArmPrerequisites(authentication,id,expectedRevision,finish=null){
  checked('LifecycleRevision',expectedRevision);
  function snapshot(original,dependencies,authorization){
   const run=currentRun(original);
   need(run.revision===expectedRevision,'run_preflight_revision');
   need(run.lifecycle==='disarmed'&&run.active_execution_id===null&&run.active_operation_id===null&&
    run.pause_latches.length===0&&!run.dispatch_eligible,'run_preflight_transition');
   need(publicationBasisCurrent(run),'run_publication_binding_unapproved');
   const approval=approvals.currentAuthorityInTransaction(run);
   const plan=deviceTargetPlan(run,dependencies,authorization).result;
   const proposalTargets=approval.saved.proposal.controller_targets.map(({controller_scope,device_actions})=>({controller_scope,device_actions}));
   need(same(plan.targets,proposalTargets),'run_preflight_targets');
   const sources=resolveAutomationSourcesInTransaction(dependencies.body.dependencies.inputs);
   const {domain}=authority.runApprovalTimeInTransaction();
   return {run,approval,plan,sources,authorization,domain,observed_receive_cursor:finish?readReceiveCursor(db,scope):null,
    slot:finish&&arm?arm.slot(run.definition_id):null};
  }
  const {initial,start}=readWithDependencies(authentication,id,(run,dependencies,authorization)=>
   ({initial:snapshot(run,dependencies,authorization),start:finish?authority.runApprovalTimeInTransaction():null}));
  const applyIds=[...new Set(initial.sources.filter(item=>item.source&&item.state!=='closed').map(item=>item.source.baseline_ref.apply_id))];
  const [verifyTargets,approvalEvidence,transportEntries]=await Promise.all([
   prepareRunActionObservations(initial.plan.evidence),approvals.observeAuthority(initial.approval,{includeControls:true}),
   Promise.all(applyIds.map(async id=>[id,await prepareSourceTransport(id)]))
  ]);
  const transports=new Map(transportEntries);
  const control_observations=approvalEvidence.controls.map(({devices,...target})=>({...target,devices:devices?.map(({device_id,control})=>({device_id,control,
   state:control===null?'unavailable':control.pending_operation_id!==null||control.gate_state==='fencing'?'transition':control.mode}))??null}));
  return readWithDependencies(authentication,id,(run,dependencies,authorization)=>{
   const current=snapshot(run,dependencies,authorization);
   need(same(initial,current),'run_preflight_changed');
   const sources=current.sources.map(item=>({...item,transport:transports.get(item.source?.baseline_ref.apply_id)?.observation??null}));
   const observation={scope,run_id:id,run_revision:current.run.revision,artifact_ref:current.run.artifact_ref,
    dependency_digest:dependencies.digest,approval_ref:current.approval.approval_ref,approval_status_revision:current.approval.status_revision,
    targets:current.plan.targets,sources,control_observations,can_arm:false};
   if(!finish)return observation;
   const {arm_generation,ingest_revision,last_receive_cursor,business_state}=current.run;
   const basis={arm_generation,ingest_revision,last_receive_cursor,business_state,observed_receive_cursor:current.observed_receive_cursor,
    authority:approvalEvidence.basis,device_bindings:current.plan.evidence};
   if(arm){
    basis.slot=current.slot;
    basis.admission=arm.assess(observation,basis);
    observation.can_arm=basis.admission!==null;
   }
   return {observation,basis,start,run:current.run};
  },()=>{verifyTargets();approvalEvidence.assertCurrentInTransaction();for(const value of transports.values())value.assertCurrentInTransaction();verifyTargets();},finish);
 }
 const preflightReference=(id,hash)=>({kind:'run_preflight',scope,object_id:id,revision:'1',digest:hash});
 function checkPreflightWindow(window,now=authority.runApprovalTimeInTransaction()){
  need(window.service_boot_id===bootId&&same(now.domain,window.domain)&&now.tick>=BigInt(window.issued_tick_ns)&&
   now.tick<BigInt(window.expires_tick_ns),'run_preflight_expired');
 }
 function checkPreflightReference(reference){
  need(fields(reference,'digest,kind,object_id,revision,scope')&&same(reference,preflightReference(reference.object_id,reference.digest)),'run_preflight_reference');
  checked('LifecycleId',reference.object_id);checked('LifecycleDigest',reference.digest);
 }
 function getArmPreflight(authentication,reference){
  checkPreflightReference(reference);
  return writer(()=>{
   const row=db.prepare('SELECT run_id,service_boot_id,body_jcs,body_digest FROM automation_run_preflight WHERE preflight_id=?').raw().get(reference.object_id);
   need(row&&row[3]===reference.digest,'run_preflight_missing');
   const [runId,ownerBoot,text,hash]=row,run=proposalRun(runId),proof=authorize(authentication,'getRun',run.definition_id,null),preflight=JSON.parse(text);
   const {observation,window,basis}=preflight;
   need(canonical(preflight)===text&&digest(preflight)===hash&&preflight.format==='leafseamer.run-arm-preflight/1'&&preflight.preflight_id===reference.object_id&&
    observation.run_id===runId&&same(observation.scope,scope)&&same(observation.artifact_ref,run.artifact_ref)&&observation.dependency_digest===run.dependency_digest&&
    observation.can_arm===Boolean(basis.admission)&&window.service_boot_id===ownerBoot&&preflight.basis_digest===digest({observation,basis}),'run_preflight_integrity');
   need(same(proof,authorize(authentication,'getRun',run.definition_id,null)),'run_authorization_changed');
   return {preflight_ref:preflightReference(reference.object_id,hash),preflight};
  });
 }
 async function reserveArm(authentication,id,{context,preflight_ref,expected_run_revision},requestDigest){
  function lookup(){
   const run=proposalRun(id),actor=authority.identifyRunApprovalActorInTransaction(authentication);
   need(actor.principal_kind==='human','run_arm_authorization');
   const key=requestIdentity({actor,request_epoch:service.request_epoch},context.idempotency_key,'armRun'),prior=arm.prior(key);
   if(prior){
    authorize(authentication,'getRun',run.definition_id,null);
    need(prior.operation.run_id===id&&prior.operation.request_digest===requestDigest,'run_arm_conflict');
   }
   return {run,key,prior};
  }
  const prepared=writer(()=>{
   const result=lookup();
   if(result.prior)return result;
   return {...result,proof:authorize(authentication,'armRun',result.run.definition_id,context.context_id)};
  });
  if(prepared.prior)return prepared.prior;
  const saved=preflight_ref?getArmPreflight(authentication,preflight_ref):null;
  if(saved){
   const {observation,window}=saved.preflight;
   need(observation.run_id===id&&observation.can_arm===true,'run_arm_preflight_ineligible');
   writer(()=>checkPreflightWindow(window));
  }
  const revision=saved?.preflight.observation.run_revision??expected_run_revision;
  try{
   return await observeArmPrerequisites(authentication,id,revision,(current,reader)=>{
    const {prior,key}=lookup();if(prior)return prior;
    if(saved)checkPreflightWindow(saved.preflight.window);
    const proof=authorize(authentication,'armRun',current.run.definition_id,context.context_id);
    need(key===prepared.key&&same(proof,prepared.proof),'run_authorization_changed');
    if(saved)need(current.observation.can_arm===true&&
     digest({observation:current.observation,basis:current.basis})===saved.preflight.basis_digest,'run_preflight_changed');
    else need(current.observation.can_arm===true,'run_arm_preflight_ineligible');
    authority.assertRunCapacityInTransaction(proof);
    const preflight=saved??writeArmPreflight(current,reader);
    const result=arm.reserve({run:current.run,preflight,context,proof,key,requestDigest});
    need(same(proof,authorize(authentication,'armRun',current.run.definition_id,context.context_id)),'run_authorization_changed');
    checkPreflightWindow(preflight.preflight.window);
    return result;
   });
  }catch(error){
   const replay=writer(lookup).prior;
   if(replay)return replay;
   throw error;
  }
 }
 async function armRun(authentication,id,request){
  checked('LifecycleId',id);
  need(fields(request,'context,preflight_ref')&&fields(request.context,'context_id,idempotency_key'),'run_arm_request');
  const fixed=structuredClone(request);
  for(const value of Object.values(fixed.context))checked('LifecycleId',value);
  checkPreflightReference(fixed.preflight_ref);
  return reserveArm(authentication,id,fixed,digest({operation_id:'armRun',run_id:id,preflight_ref:fixed.preflight_ref}));
 }
 async function observeAndArmRun(authentication,id,request){
  checked('LifecycleId',id);
  need(fields(request,'context,expected_run_revision')&&fields(request.context,'context_id,idempotency_key'),'run_arm_request');
  const fixed=structuredClone(request);
  for(const value of Object.values(fixed.context))checked('LifecycleId',value);
  checked('LifecycleRevision',fixed.expected_run_revision);
  return reserveArm(authentication,id,fixed,digest({operation_id:'observeAndArmRun',run_id:id,expected_run_revision:fixed.expected_run_revision}));
 }
 function writeArmPreflight({observation,basis,start},proof,armContext=null){
  const now=authority.runApprovalTimeInTransaction();
  const window={service_boot_id:bootId,domain:start.domain,issued_tick_ns:String(now.tick),expires_tick_ns:String(start.tick+preflightWindowNs)};
  need(now.tick>=start.tick,'run_preflight_expired');checkPreflightWindow(window,now);
  const preflight_id=crypto.randomUUID(),preflight={format:'leafseamer.run-arm-preflight/1',preflight_id,window,observation,basis,
   basis_digest:digest({observation,basis}),reader:{actor:proof.actor,authorization_digest:proof.authorization_digest,
    ...(armContext!==null&&{arm_context_id:armContext})}};
  const text=canonical(preflight),hash=digest(preflight);
  need(Buffer.byteLength(text)<=1048576&&db.prepare('SELECT count(*) FROM automation_run_preflight').pluck().get()<16384,'run_preflight_capacity');
  need(db.prepare('INSERT INTO automation_run_preflight VALUES(?,?,?,?,?)').run(preflight_id,observation.run_id,bootId,text,hash).changes===1,'run_preflight_storage');
  return {preflight_ref:preflightReference(preflight_id,hash),preflight};
 }
 function recordArmPreflight(authentication,id,expectedRevision,armContext=null){
  if(armContext!==null)checked('LifecycleId',armContext);
  return observeArmPrerequisites(authentication,id,expectedRevision,(current,proof)=>{
   const armProof=armContext===null?null:authorize(authentication,'armRun',proof.definition_id,armContext);
   if(armProof)authority.assertRunCapacityInTransaction(armProof);
   const saved=writeArmPreflight(current,proof,armContext);
   if(current.basis.admission)arm.assertPolicy(current.observation,current.basis);
   if(armProof)need(same(armProof,authorize(authentication,'armRun',proof.definition_id,armContext)),'run_authorization_changed');
   need(same(proof,authorize(authentication,'getRun',proof.definition_id,null)),'run_authorization_changed');checkPreflightWindow(saved.preflight.window);
   return saved;
  });
 }
 async function revalidateArmPreflight(authentication,reference){
  const saved=getArmPreflight(authentication,reference),{window,observation,basis_digest}=saved.preflight;
  writer(()=>checkPreflightWindow(window));
  return observeArmPrerequisites(authentication,observation.run_id,observation.run_revision,current=>{
   need(digest({observation:current.observation,basis:current.basis})===basis_digest,'run_preflight_changed');
   checkPreflightWindow(window);
   return saved;
  });
 }
 function originalArmPreflight(operation){
  const [owner,text,hash,runId]=db.prepare('SELECT service_boot_id,body_jcs,body_digest,run_id FROM automation_run_preflight WHERE preflight_id=?')
   .raw().get(operation.preflight_ref.object_id)??[];
  need(hash===operation.preflight_ref.digest&&runId===operation.run_id,'run_preflight_integrity');
  const preflight=JSON.parse(text);
  need(canonical(preflight)===text&&digest(preflight)===hash&&preflight.window.service_boot_id===owner,'run_preflight_integrity');
  return preflight;
 }
 function readIngestBaseline(operation,receive_cursor=readReceiveCursor(db,scope)){
  const {basis,observation}=originalArmPreflight(operation),preflightCursor=basis.observed_receive_cursor;
  need(fields(receive_cursor,'ingest_epoch,receive_sequence,scope')&&typeof receive_cursor.receive_sequence==='string'&&
   /^(0|[1-9][0-9]{0,19})$/.test(receive_cursor.receive_sequence)&&same(receive_cursor.scope,preflightCursor.scope)&&
   receive_cursor.ingest_epoch===preflightCursor.ingest_epoch&&
   BigInt(receive_cursor.receive_sequence)>=BigInt(preflightCursor.receive_sequence),'run_arm_ingest_baseline_changed');
  const last=triggerDecisions?.latest(operation.run_id,receive_cursor);
  const before=last&&BigInt(last.revision)>BigInt(basis.ingest_revision)?last.revision:basis.ingest_revision;
  const revision=String(BigInt(before)+1n);
  checked('LifecycleRevision',revision);
  const refs=new Map(observation.sources.map(({source})=>[canonical(source.baseline_ref),source.baseline_ref]));
  const source_baseline_refs=[...refs.keys()].sort(compare).map(key=>refs.get(key));
  return {revision,receive_cursor,source_baseline_refs};
 }
 function armReleaseState(registered){
  const {operation}=registered,{initiator_authentication:authentication,initiator_authorization_ref,initiator}=operation;
  need(digest(authentication)===initiator_authorization_ref.digest,'run_arm_integrity');
  const preflight=originalArmPreflight(operation);
  need(preflight.window.service_boot_id===bootId,'run_initiator_ineligible');
  const run=currentRun(proposalRun(operation.run_id));
  need(run.active_operation_id===operation.operation_id&&run.arm_generation===operation.target_arm_generation&&
   run.revision===operation.committed_run_revision&&run.lifecycle==='disarmed'&&!run.dispatch_eligible,'run_initiator_ineligible');
  need(publicationBasisCurrent(run),'run_publication_binding_unapproved');
  const approval=approvals.currentAuthorityInTransaction(run);
  need(same(approval.approval_ref,operation.approval_ref),'run_initiator_approval');
  const grantEnd=authority.assertArmInitiatorInTransaction(initiator,run.definition_id,authentication.grant_ref);
  const now=authority.runApprovalTimeInTransaction();
  const deadline=[authentication.credential?.recent_before_ns??'0',proposalWindowEnd(approval.saved.proposal),grantEnd].map(BigInt).reduce((a,b)=>a<b?a:b);
  need(same(now.domain,preflight.window.domain)&&now.tick>=BigInt(preflight.window.issued_tick_ns)&&now.upper<deadline,'run_initiator_expired');
  return {...registered,run,approval,preflight,now,deadline};
 }
 function pairedDeadline(state,record){
  const end=BigInt(record.expires_ns),value=end<state.deadline?end:state.deadline;
  need(state.now.upper<value,'run_initiator_expired');return String(value);
 }
 function fixedArmPlan(state,target){
  const {run,approval,operation_ref,preflight}=state,{controller_scope,preparation_id}=target;
  const {observation,basis}=preflight,proposal=approval.saved.proposal;
  const observationTarget=observation.control_observations.find(item=>same(item.controller_scope,controller_scope));
  const authorityTarget=basis.authority.endorsements.find(item=>same(item.endorsement_ref.scope,controller_scope));
  const policy=authorityTarget?.target_policy_ref;
  need(target.state==='pending'&&policy?.kind==='run_endorsement_policy'&&same(policy.scope,controller_scope)&&
   same(authorityTarget.endorsement_ref,observationTarget?.endorsement_ref)&&Array.isArray(observationTarget.devices),'run_preparation_plan_evidence');
  const {device_actions,parameter_constraints}=proposalTarget(proposal,controller_scope);
  need(same(device_actions,target.device_actions),'run_preparation_plan_target');
  const controls=observationTarget.devices.map(({device_id,control,state})=>{
   need(state==='automatic'&&control?.gate_state==='protected'&&control.pending_operation_id===null,'run_preparation_plan_control');
   return {device_id,expected_control:{epoch:control.control_epoch,version:control.effective_version}};
  });
  const original={format:'leafseamer.run-arm-plan/1',definition_id:run.definition_id,preparation:{
   operation_id:operation_ref.object_id,preparation_id,controller_scope,origin_scope:scope,run_id:run.run_id,definition_version:run.definition_version,
   arm_generation:run.arm_generation,mode:'arm',execution_id:null,operation_ref,artifact_ref:run.artifact_ref,approval_ref:approval.approval_ref,
   authorization_digest:digest({approval_ref:approval.approval_ref,basis:basis.authority}),source_digest:digest(observation.sources),controls,
   baseline_grant_ref:null,grant_intent:{principal:{...proposal.service_principal,principal_kind:'service'},device_actions,parameter_constraints,
    validity:{...proposal.validity,authorization_policy_revision:policy.revision}}},
   controls:observationTarget.devices.map(({device_id,control})=>({device_id,control_ref:control.control_ref}))};
  const plan_ref={kind:'arm_plan',scope,object_id:preparation_id,revision:'1',digest:digest(original)};
  const request=armPlanOriginal(original,plan_ref,controller_scope);
  return {original,plan_ref,request,pairing_ref:authorityTarget.pairing_ref,endorsement_ref:authorityTarget.endorsement_ref};
 }
 function assertArmPlanSources(state){
  const {preflight,run}=state,dependencies=readDependencies(run);
  const expected=preflight.observation.sources.map(({transport,...source})=>source);
  need(same(resolveAutomationSourcesInTransaction(dependencies.body.dependencies.inputs),expected)&&
   expected.every(item=>item.state==='synchronized'&&item.source),'run_preparation_plan_source');
  arm.assertPolicy(preflight.observation,preflight.basis);
 }
 function armPlanPair(plan){
  const pair=currentPairingInTransaction(plan.pairing_ref);
  const peer=pair.transcript.parties.find(item=>item.service_id===pair.peer_service_id);
  need(peer?.service_kind==='controller'&&same(peerScope(peer),plan.request.controller_scope),'run_preparation_plan_target');
  return pair;
 }
 function preparationLookup(plan){
  const {controller_scope,origin_scope,preparation_id}=plan.request;
  return {controller_scope,origin_scope,preparation_id,request_digest:digest(plan.request)};
 }
 function registeredArm(reference){
  const result=arm.operation(reference.object_id,reference.digest);
  need(same(result.operation_ref,reference),'run_preparation_plan_reference');return result;
 }
 const preparationReceipts=Number(service.schema_revision)>=15?armPreparationReceiptStore({db,bootId}):null;
 function historicalPlan(registered,target,preflight=originalArmPreflight(registered.operation)){
  const {operation}=registered,run={...proposalRun(operation.run_id),arm_generation:operation.target_arm_generation};
  const approval=approvals.originalAuthorityInTransaction(operation.approval_ref,run);
  return fixedArmPlan({...registered,run,preflight,approval},target);
 }
 function protectionTargets(run,approvalRef,targetScope){
  const preparations=[],grants=[],arm_generations=[],targets={preparations,grants,arm_generations};
  const recovery=recoveryPreparations&&recoveries.active(run.run_id);
  const ready=recovery&&recoveryPreparations.read(recovery);
  if(ready&&same(ready.approval_ref,approvalRef)){
   const plan=ready.plans.find(item=>same(item.request.controller_scope,targetScope));
   if(!plan)return targets;
   const {run_id,arm_generation,operation_ref,preparation_id}=plan.request;
   const {request_digest}=recoveryPreparations.lookup(plan);
   const received=recoveryPreparations.received(plan);
   arm_generations.push({run_id,arm_generation});
   preparations.push({run_id,arm_generation,operation_ref,preparation_id,request_digest});
   if(received)grants.push({preparation_id,grant_ref:received.grant_ref});
   return targets;
  }
  const registered=arm?.forRun(run);
  if(!registered||!same(registered.operation.approval_ref,approvalRef))return targets;
  const {operation,operation_ref}=registered,target=operation.targets.find(item=>same(item.controller_scope,targetScope));
  if(!target)return targets;
  const generation={run_id:operation.run_id,arm_generation:operation.target_arm_generation};
  const preflight=originalArmPreflight(operation);
  const basis=preflight.basis.authority.endorsements.find(item=>same(item.endorsement_ref.scope,targetScope));
  let request_digest=null;
  if(basis?.target_policy_ref){
   const plan=historicalPlan(registered,target,preflight),lookup=preparationLookup(plan);
   request_digest=lookup.request_digest;
   const received=preparationReceipts?.read({...plan,lookup});
   if(received)grants.push({preparation_id:target.preparation_id,grant_ref:received.target_grant_ref});
  }
  arm_generations.push(generation);
  preparations.push({...generation,preparation_id:target.preparation_id,operation_ref,request_digest});
  return targets;
 }
 function preparationTarget(registered,scope){
  const target=registered.operation.targets.find(item=>same(item.controller_scope,scope));
  need(target,'run_preparation_plan_target');return target;
 }
 // 自动提交只在受理布防的启动域运行。离开该启动域后只允许按原身份查原。
 const attemptedPreparations=new Set();
 // 只持久记录配对 Controller 观察到的事实。当前布防资格由后续状态事务另行核验。
 async function exchangeArmPreparation(reference,targetScope,via){
  const fixed=structuredClone(reference),scope=structuredClone(targetScope);
  function select(forSubmit){
   const registered=registeredArm(fixed),target=preparationTarget(registered,scope);
   if(!forSubmit)return historicalPlan(registered,target);
   const state=armReleaseState(registered);assertArmPlanSources(state);
   const plan=fixedArmPlan(state,target);pairedDeadline(state,armPlanPair(plan));return plan;
  }
  const prior=writer(()=>{
   const plan=select(via==='submit'),pair=armPlanPair(plan),lookup=preparationLookup(plan);
   return {plan,pair,lookup,existing:via==='submit'?preparationReceipts.read({...plan,lookup}):null};
  });
  const observation=({receipt_ref,target_grant_ref})=>({receipt_ref,target_grant_ref});
  if(prior.existing)return {created:false,record:prior.existing.record,observation:observation(prior.existing)};
  const {preparation_id}=prior.lookup;
  if(via==='submit'){
   need(!attemptedPreparations.has(preparation_id),'run_preparation_submission_unknown');
   attemptedPreparations.add(preparation_id);
  }
  const result=via==='submit'?await sendArmPreparation(prior.pair.binding_id,structuredClone(prior.plan.request)):
   await lookupArmPreparation(prior.pair.binding_id,structuredClone(prior.lookup));
  if(via==='lookup'&&result.state!=='found'){
   need(['not_found','indeterminate'].includes(result.state)&&result.record===null,'run_preparation_receipt_response');return result;
  }
  const observed=writer(()=>{
   const plan=select(false),pair=armPlanPair(plan);
   need(same(plan,prior.plan)&&same(pair,prior.pair),'run_preparation_receipt_pairing');
   const received=preparationReceipts.adopt({...plan,lookup:prior.lookup},result.record,via,authority.runApprovalTimeInTransaction());
   need(same(armPlanPair(plan),pair),'run_preparation_receipt_pairing');
   return {...result,observation:observation(received)};
  });
  attemptedPreparations.delete(preparation_id);
  return observed;
 }
 function scanArmPreparation({after,limit}){
  if(after!==null)checked('LifecycleId',after);
  need(Number.isSafeInteger(limit)&&limit>=1&&limit<=64,'run_preparation_scan');
  return writer(()=>{
   const rows=db.prepare(`SELECT t.preparation_id,a.operation_id,a.body_digest,t.controller_service_id,t.controller_database_instance_id
    FROM automation_arm_target t JOIN automation_run_arm a USING(operation_id)
    JOIN automation_run_preflight p USING(preflight_id) JOIN automation_run_approval_head h ON h.run_id=a.run_id
    JOIN automation_run_approval_state s ON s.approval_id=h.approval_id
    WHERE t.preparation_id>? AND p.service_boot_id=? AND s.state='active'
     AND h.run_revision=json_extract(a.body_jcs,'$.committed_run_revision')
     AND h.approval_id=json_extract(a.body_jcs,'$.approval_ref.object_id')
     AND NOT EXISTS(SELECT 1 FROM automation_arm_preparation_receipt r WHERE r.preparation_id=t.preparation_id)
    ORDER BY t.preparation_id LIMIT ?`).raw().all(after??'',bootId,limit+1);
   const items=rows.slice(0,limit).map(([preparation_id,object_id,digest,authority_service_id,database_instance_id])=>({preparation_id,
    operation_ref:{kind:'run_arm_operation',scope,object_id,revision:'1',digest},target:{authority_service_id,database_instance_id}}));
   return {items,next:rows.length>limit?items.at(-1).preparation_id:null};
  });
 }
 async function deliverArmPreparation({operation_ref,target,preparation_id}){
  const existing=writer(()=>{
   const registered=registeredArm(operation_ref),member=preparationTarget(registered,target);
   need(member.preparation_id===preparation_id,'run_preparation_plan_target');
   if(preparationReceipts.has(preparation_id)){
    const plan=historicalPlan(registered,member);
    return preparationReceipts.read({...plan,lookup:preparationLookup(plan)});
   }
   if(!attemptedPreparations.has(preparation_id))armReleaseState(registered);
   return null;
  });
  if(existing)return {state:'acknowledged',receipt_ref:existing.receipt_ref};
  const lookup=await exchangeArmPreparation(operation_ref,target,'lookup');
  if(lookup.state!=='found'&&(lookup.state==='indeterminate'||attemptedPreparations.has(preparation_id)))return {state:'pending',receipt_ref:null};
  const result=lookup.state==='found'?lookup:await exchangeArmPreparation(operation_ref,target,'submit');
  return {state:'delivered',receipt_ref:result.observation.receipt_ref};
 }
 async function inBatches(values,read){
  const result=[];
  for(let offset=0;offset<values.length;offset+=4)result.push(...await Promise.all(values.slice(offset,offset+4).map(read)));
  return result;
 }
 async function readArmSourceTransports(sources){
  const ids=[...new Set(sources.map(item=>item.source.baseline_ref.apply_id))];
  return new Map(await inBatches(ids,async id=>[id,await prepareSourceTransport(id)]));
 }
 function assertArmSourceTransports(sources,transports){
  for(const source of sources){
   const evidence=transports.get(source.source.baseline_ref.apply_id);
   need(evidence?.observation.state==='ready'&&same(evidence.observation,source.transport),'run_preparation_plan_source');
  }
  for(const evidence of transports.values())evidence.assertCurrentInTransaction();
 }
 let pendingCompletionChecks=0;
 async function prepareArmCompletionEvidence(reference){
  const fixed=structuredClone(reference);
  function snapshot(){
   const registered=registeredArm(fixed),state=armReleaseState(registered);assertArmPlanSources(state);
   const slot=arm.slot(state.run.definition_id);
   need(slot.active_run_id===state.run.run_id&&slot.active_operation_id===registered.operation.operation_id&&
    slot.revision===registered.operation.committed_slot_revision,'run_arm_slot_changed');
   const plan=deviceTargetPlan(state.run,readDependencies(state.run),null).result;
   need(same(plan.targets,state.preflight.observation.targets)&&same(plan.evidence,state.preflight.basis.device_bindings),'run_preparation_plan_target');
   const targets=registered.operation.targets.map(target=>{
    const plan=fixedArmPlan(state,target),pair=armPlanPair(plan),lookup=preparationLookup(plan);
    const found=preparationReceipts.read({...plan,lookup});need(found,'run_preparation_receipts_incomplete');
    return {pair,request:{lookup,receipt_ref:found.receipt_ref,grant_ref:found.target_grant_ref},deadline:pairedDeadline(state,pair)};
   });
   return {targets,deadline:String(state.deadline),now:state.now,sources:state.preflight.observation.sources};
  }
  need(pendingCompletionChecks<4,'run_preparation_plan_capacity');
  const captured=writer(snapshot);pendingCompletionChecks++;
  try{
   const [remote,transports]=await Promise.all([
    inBatches(captured.targets,item=>observeArmPreparation(item.pair.binding_id,structuredClone(item.request))),
    readArmSourceTransports(captured.sources)
   ]);
   let active=true;
   function verify(){
    need(db.inTransaction&&active,'run_preparation_evidence_unavailable');
    try{
     const latest=snapshot();
     need(same(latest.targets,captured.targets)&&same(latest.sources,captured.sources)&&latest.deadline===captured.deadline,'run_preparation_evidence_changed');
     assertArmSourceTransports(latest.sources,transports);
     const deadlines=[BigInt(captured.deadline)];
     const targets=remote.map(({original,assertCurrentInTransaction},index)=>{
      const expected=captured.targets[index];
      need(original.state==='eligible'&&same(original.request,expected.request)&&assertCurrentInTransaction()===true,'run_preparation_evidence_ineligible');
      deadlines.push(BigInt(original.eligible_before_ns),BigInt(expected.deadline));
      return original;
     });
     const end=deadlines.reduce((a,b)=>a<b?a:b),now=authority.runApprovalTimeInTransaction();
     need(same(now.domain,captured.now.domain)&&now.tick>=captured.now.tick&&
      now.tick<captured.now.tick+preflightWindowNs&&now.upper<end,'run_preparation_evidence_expired');
     return {operation_ref:structuredClone(fixed),targets,eligible_before_ns:String(end)};
    }catch(error){active=false;throw error;}
   }
   const original=writer(verify);
   return Object.freeze({get original(){return structuredClone(original);},assertCurrentInTransaction:verify});
  }finally{pendingCompletionChecks--;}
 }
 async function completeArmRun(authentication,reference){
  const fixed=structuredClone(reference);
  need(fields(fixed,'digest,kind,object_id,revision,scope')&&fixed.kind==='run_arm_operation'&&fixed.revision==='1'&&
   same(fixed.scope,scope),'run_arm_request');
  checked('LifecycleId',fixed.object_id);checked('LifecycleDigest',fixed.digest);
  function authorizeCompletion(registered,write){
   const operation=registered.operation;
   const proof=authorize(authentication,write?'armRun':'getRun',operation.definition_id,write?operation.context_id:null);
   need(same(proof.actor,operation.initiator),'run_arm_completion_authorization');
   if(write){
    const original=operation.initiator_authentication,current=proof.authentication_evidence;
    need(current&&same(current.grant_ref,original.grant_ref)&&same(current.credential,original.credential),'run_arm_completion_authorization');
   }
  }
  const read=write=>writer(()=>{
   const registered=registeredArm(fixed),ready=armCompletion.read(registered);
   authorizeCompletion(registered,write&&!ready);return ready;
  });
  const existing=read(true);if(existing)return existing;
  try{
   const evidence=await prepareArmCompletionEvidence(fixed);
   return writer(()=>{
    const registered=registeredArm(fixed),ready=armCompletion.read(registered);
    if(ready){authorizeCompletion(registered,false);return ready;}
    return armCompletion.complete(registered,()=>{
     authorizeCompletion(registered,true);return evidence.assertCurrentInTransaction();
    });
   });
  }catch(error){
   const committed=read(false);if(committed)return committed;throw error;
  }
 }
 let pendingPlans=0;
 async function prepareArmPlanForPeer(bindingId,socket,request){
  function snapshot(){
   const paired=authorizeControllerPeerInTransaction(bindingId,socket),scope=peerScope(paired.peer);
   const registered=arm.operationForPreparation(request.preparation_id);
   const target=registered.operation.targets.find(item=>item.preparation_id===request.preparation_id&&same(item.controller_scope,scope));
   need(target,'run_preparation_plan_target');
   const state=armReleaseState(registered),plan=fixedArmPlan(state,target);
   need(plan.plan_ref.digest===request.plan_digest&&same(pairingReference(paired.record),plan.pairing_ref),'run_preparation_plan_reference');
   assertArmPlanSources(state);
   return {paired,now:state.now,sources:state.preflight.observation.sources,
    body:{plan_ref:plan.plan_ref,original:plan.original,eligible_before_ns:pairedDeadline(state,paired.record)}};
  }
  need(pendingPlans<4,'run_preparation_plan_capacity');
  const captured=writer(snapshot);pendingPlans++;
  try{
   const transports=await readArmSourceTransports(captured.sources);
   let eligible=true;
   function beforeSend(){
    need(eligible,'run_preparation_plan_changed');
    try{return writer(()=>{
     const latest=snapshot();
     need(same(latest.paired,captured.paired)&&same(latest.body,captured.body)&&same(latest.sources,captured.sources)&&
      same(latest.now.domain,captured.now.domain)&&latest.now.tick>=captured.now.tick&&latest.now.tick<captured.now.tick+preflightWindowNs,'run_preparation_plan_changed');
     assertArmSourceTransports(captured.sources,transports);
     const final=authority.runApprovalTimeInTransaction();
     need(same(final.domain,captured.now.domain)&&final.tick>=latest.now.tick&&final.tick<captured.now.tick+preflightWindowNs&&
      final.upper<BigInt(captured.body.eligible_before_ns)&&same(authorizeControllerPeerInTransaction(bindingId,socket),captured.paired),'run_preparation_plan_changed');
     return structuredClone(captured.body);
    });}catch(error){eligible=false;throw error;}
   }
   return {body:structuredClone(captured.body),beforeSend};
  }finally{pendingPlans--;}
 }
 function recoveryEvidenceInTransaction(authentication,runId,executionId,includeResources=false,previewRevision=null,reservation=null){
  const original=proposalRun(runId),proof=authorize(authentication,'getRun',original.definition_id,null);
  need(proof.actor.principal_kind==='human','local_authorization_human');
  const run=currentRun(original);
  need(run.lifecycle==='paused'&&run.active_execution_id===(reservation?.operation.new_execution_id??executionId),'run_recovery_execution_conflict');
  const value=executions.read(executionId),registered=arm.forRun(run);
  const sourceGeneration=reservation?value?.execution.arm_generation:run.arm_generation;
  if(reservation)need(same(reservation.operation_ref,recoveries.active(runId)?.operation_ref)&&
   same(value?.execution_ref,reservation.operation.source_execution_ref)&&run.active_operation_id===reservation.operation.operation_id&&
   run.arm_generation===reservation.operation.recovery_generation,'run_recovery_execution_conflict');
  need(value&&registered&&value.execution.run_id===runId&&value.execution.arm_generation===sourceGeneration&&
   value.execution.activation_slot.active_operation_id===registered.operation.operation_id&&!executionCommits.read(executionId),
   'run_recovery_execution_conflict');
  const approval_ref=registered.operation.approval_ref;
  const protection=approvals.recoveryProtectionInTransaction(run,approval_ref,sourceGeneration);
  let previewBasis;
  if(previewRevision!==null){
   need(run.revision===previewRevision,'run_recovery_execution_conflict');
   const slot=slotFor(run.definition_id),dependencies=readDependencies(run);
   need(slot.active_run_id===runId&&slot.revision===value.execution.activation_slot_revision,
    'run_recovery_execution_conflict');
   previewBasis={scope,definition_id:run.definition_id,definition_version:run.definition_version,artifact_ref:run.artifact_ref,
    slot,business_state:run.business_state,dependency_digest:dependencies.digest,ingest_revision:run.ingest_revision,
    receive_cursor:readReceiveCursor(db,scope),run_receive_cursor:run.last_receive_cursor,
    trigger_pause_revision:run.trigger_pause_revision??null,latch_set_digest:digest(run.pause_latches),
    publication_current:publicationBasisCurrent(run),source_baselines:run.arm_source_baselines??[],
    target_scopes:registered.operation.targets.map(({controller_scope})=>controller_scope),
    sources:resolveAutomationSourcesInTransaction(dependencies.body.dependencies.inputs),
    unresolved_latch_ids:run.pause_latches.filter(id=>id!==executionId&&id!==protection?.operation_ref.object_id)};
  }
  const commands=[];
  const steps=value.steps.flatMap(({plan,submission})=>{
   if(plan.payload.kind!=='action')return [];
   const {controller_scope,device_action}=plan.payload.binding.target;
   const terminal=submission?.terminal;
   if(includeResources){
    let paired=false;
    if(submission?.attempt)try{paired=Boolean(currentPairingInTransaction(submission.body.pairing_ref));}catch{}
    commands.push({step_id:plan.step_id,controller_scope,device_id:device_action.device_id,submission,paired});
   }
   return [{step_id:plan.step_id,controller_scope,device_id:device_action.device_id,submission_digest:submission?.digest??null,
    submission_state:submission?.state??'not_recorded',command_ref:submission?.adjudication?.command_ref??null,
    terminal:terminal?{state_ref:terminal.state_ref,outcome:terminal.state.outcome,resource_status:terminal.state.resource_status,
     resource_evidence_refs:terminal.state.resource_evidence_refs}:null}];
  });
  need(same(proof,authorize(authentication,'getRun',run.definition_id,null)),'run_authorization_changed');
  return {proof,evidence:{run_id:runId,run_revision:reservation?.operation.previous_run_revision??run.revision,execution_ref:value.execution_ref,arm_generation:sourceGeneration,
   pause_latches:run.pause_latches,approval_ref,observed_protection:protection,steps},...(includeResources&&{commands}),
   ...(previewBasis&&{previewBasis,execution:value.execution,executionSteps:value.steps})};
 }
 function recoveryCommandResource(command,reply){
  const {step_id,controller_scope,device_id,submission}=command;
  const step={step_id,controller_scope,device_id,resource:null};
  if(!submission?.attempt)return {...step,state:'not_submitted'};
  if(!reply)return {...step,state:'unavailable'};
  const {adjudication,terminal,resource}=reply;
  if(!adjudication||!same(adjudication.identity,submission.body.identity)||
   submission.adjudication&&!same(adjudication,submission.adjudication)||submission.terminal&&!same(terminal,submission.terminal))
   return {...step,state:'unconfirmed'};
  if(adjudication.decision==='rejected')return {...step,state:'rejected',adjudication_ref:adjudication.commit_ref};
  return resource?{...step,state:resource.status,resource}:{...step,state:'unconfirmed'};
 }
 async function observeRecoveryProtection(authentication,runId,executionId,{includeResources=false,previewRevision=null,finish=null,reservation=null}={}){
  checked('LifecycleId',runId);checked('LifecycleId',executionId);
  function capture(){
   const snapshot=recoveryEvidenceInTransaction(authentication,runId,executionId,includeResources,previewRevision,reservation),protection=snapshot.evidence.observed_protection;
   const requests=protection?.targets.map(({target_scope})=>approvals.prepareRecoveryProtectionInTransaction(protection.operation_ref,target_scope))??[];
   return {...snapshot,requests};
  }
  const {initial,start}=writer(()=>({initial:capture(),start:authority.runApprovalTimeInTransaction()}));
  function recheckTime(){
   const now=authority.runApprovalTimeInTransaction();
   need(same(now.domain,start.domain)&&now.tick>=start.tick&&now.tick<start.tick+preflightWindowNs&&
    now.tick<start.validBeforeTick,'run_recovery_protection_expired');
   return now;
  }
  const observe=async(read,pairingRef,request)=>{
   writer(recheckTime);
   if(!pairingRef)return null;
   try{return await read(pairingRef.id,structuredClone(request));}
   catch{return null;}
  };
  const reads=initial.requests.map(({notice,pairing_ref})=>()=>observe(observeApprovalProtection,pairing_ref,notice));
  for(const command of initial.commands??[]){
   const body=command.submission?.body;
   reads.push(()=>observe(readAutomationCommandResource,command.paired?body.pairing_ref:null,body?.request));
  }
  const replies=await inBatches(reads,read=>read());
  const readySourceIds=[...new Set((initial.previewBasis?.sources??[])
   .filter(item=>item.state==='synchronized').map(item=>item.source.baseline_ref.apply_id))];
  const sourceTransports=new Map(await inBatches(readySourceIds,async id=>{
   writer(recheckTime);
   try{return [id,await prepareSourceTransport(id)];}catch{return [id,null];}
  }));
  return writer(()=>{
   const current=capture();
   need(same(initial,current),'run_recovery_execution_conflict');recheckTime();
   const targets=(current.evidence.observed_protection?.targets??[]).map(({target_scope,receipt_ref},index)=>{
    const reply=replies[index];
    if(!reply)return {target_scope,state:'unavailable'};
    const matched=receipt_ref?.kind==='run_approval_protection_receipt'&&reply.completion?.receipt_id===receipt_ref.object_id&&
     digest(reply.completion)===receipt_ref.digest;
    const state=reply.state==='verified'&&!matched?'unconfirmed':reply.state;
    return {target_scope,state};
   });
   const protectionVerified=targets.length>0&&targets.every(item=>item.state==='verified');
   const result={...current.evidence,current_protection:{state:protectionVerified?'verified':'unconfirmed',targets}};
   if(!includeResources)return result;
   const steps=current.commands.map((command,index)=>recoveryCommandResource(command,replies[targets.length+index]));
   const protectionTargets=current.evidence.observed_protection?.targets??[];
   const resourcesVerified=steps.every(step=>['not_submitted','rejected','never_acquired','released'].includes(step.state));
   const allCovered=steps.every(step=>protectionTargets.some(target=>same(target.target_scope,step.controller_scope)&&target.device_ids?.includes(step.device_id)));
   const evidence={...result,current_resources:{state:resourcesVerified?'verified':'unconfirmed',steps},
    recovery_evidence_state:protectionVerified&&resourcesVerified&&allCovered?'verified':'unconfirmed'};
   if(!finish)return evidence;
   const sources=current.previewBasis.sources.map(source=>{
    const transport=sourceTransports.get(source.source?.baseline_ref.apply_id);
    if(transport)try{
     transport.assertCurrentInTransaction();
     return {...source,transport:transport.observation};
    }catch{}
    return {...source,transport:null};
   });
   const snapshot={...current.previewBasis,...evidence,sources,source_pause_resolution_refs:[]};
   const items=recoveryPlan(current.execution,current.executionSteps,evidence.current_resources);
   const protectedScopes=targets.map(({target_scope})=>target_scope);
   const ready=evidence.recovery_evidence_state==='verified'&&snapshot.publication_current&&snapshot.unresolved_latch_ids.length===0&&
    protectedScopes.length===snapshot.target_scopes.length&&snapshot.target_scopes.every(scope=>protectedScopes.some(target=>same(target,scope)))&&
    sources.every(hasCurrentSourceContinuity)&&items.every(item=>item.decision!=='stop');
   const mode=!ready?'blocked':items.every(item=>item.decision==='retain_success')?'reconcile_commit':'execute';
   return finish(snapshot,{mode,items,target_scopes:snapshot.target_scopes},current.proof,start,recheckTime());
  });
 }
 function recoveryPreviewInTransaction(authentication,runId,reference){
  checked('LifecycleId',runId);
  const run=proposalRun(runId);
  const proof=authorize(authentication,'getRun',run.definition_id,null);
  need(proof.actor.principal_kind==='human','local_authorization_human');
  const saved=recoveryPreviews.read(reference);
  need(saved.preview.snapshot.run_id===runId,'run_recovery_preview_missing');
  return {saved,proof};
 }
 function checkRecoveryPreviewWindow(window,now){
  need(window.service_boot_id===bootId&&same(window.domain,now.domain)&&now.tick>=BigInt(window.issued_tick_ns)&&
   now.tick<BigInt(window.expires_tick_ns),'run_recovery_protection_expired');
 }
 function previewRecovery(authentication,runId,executionId,revision){
  checked('LifecycleRevision',revision);
  return observeRecoveryProtection(authentication,runId,executionId,{includeResources:true,previewRevision:revision,
   finish(snapshot,plan,proof,start,now){
    const expires=start.tick+preflightWindowNs;
    const window={service_boot_id:bootId,domain:start.domain,issued_tick_ns:String(now.tick),
     expires_tick_ns:String(expires<start.validBeforeTick?expires:start.validBeforeTick)};
    checkRecoveryPreviewWindow(window,now);
    return recoveryPreviews.record(snapshot,plan,window,proof);
   }});
 }
 function verifyRecoveryPreview(authentication,{saved,proof},finish){
  const {snapshot,window,reader,basis_digest,plan_digest}=saved.preview;
  need(same(reader,{actor:proof.actor,authorization_digest:proof.authorization_digest}),'run_recovery_preview_changed');
  return observeRecoveryProtection(authentication,snapshot.run_id,snapshot.execution_ref.object_id,
   {includeResources:true,previewRevision:snapshot.run_revision,finish(current,plan,observed,_start,now){
    checkRecoveryPreviewWindow(window,now);
    need(same(proof,observed)&&digest(current)===basis_digest&&digest(plan)===plan_digest,'run_recovery_preview_changed');
    return finish(now);
   }});
 }
 function revalidateRecoveryPreview(authentication,runId,reference){
  const current=writer(()=>{
   const value=recoveryPreviewInTransaction(authentication,runId,reference);
   checkRecoveryPreviewWindow(value.saved.preview.window,authority.runApprovalTimeInTransaction());
   return value;
  });
  return verifyRecoveryPreview(authentication,current,()=>current.saved);
 }
 async function resumeRun(authentication,runId,request){
  checked('LifecycleId',runId);
  need(fields(request,'context,expected_revision,preview_ref')&&fields(request.context,'context_id,idempotency_key'),'run_recovery_preview_reference');
  const {context,expected_revision,preview_ref}=structuredClone(request);
  checked('LifecycleId',context.context_id);checked('LifecycleId',context.idempotency_key);checked('LifecycleRevision',expected_revision);
  const requestDigest=digest({operation:resumeOperation,run_id:runId,expected_revision,preview_ref});
  function lookup(){
   const run=proposalRun(runId),proof=authorize(authentication,'getRun',run.definition_id,null);
   need(proof.actor.principal_kind==='human','local_authorization_human');
   const key=requestIdentity(proof,context.idempotency_key,resumeOperation),prior=recoveries.prior(key);
   if(prior)need(prior.operation.run_id===runId&&prior.operation.request_digest===requestDigest,'run_recovery_operation_conflict');
   return {run,proof,key,prior};
  }
  const initial=writer(()=>{
   const entry=lookup();if(entry.prior)return entry;
   const approval=authorize(authentication,resumeOperation,entry.run.definition_id,context.context_id);
   need(same(approval.actor,entry.proof.actor)&&approval.authentication_evidence,'local_authorization_human');
   const {saved}=recoveryPreviewInTransaction(authentication,runId,preview_ref);
   need(saved.preview.snapshot.run_revision===expected_revision&&!recoveries.active(runId),'run_recovery_operation_conflict');
   checkRecoveryPreviewWindow(saved.preview.window,authority.runApprovalTimeInTransaction());
   need(saved.preview.plan.mode==='execute'||saved.preview.plan.mode==='reconcile_commit','run_recovery_ineligible');
   return {...entry,saved,approval};
  });
  if(initial.prior)return recoveryReceipt(initial.prior);
  try{
   return await verifyRecoveryPreview(authentication,initial,now=>{
    const current=lookup();if(current.prior)return recoveryReceipt(current.prior);
    const approval=authorize(authentication,resumeOperation,current.run.definition_id,context.context_id);
    need(current.key===initial.key&&same(initial.approval,approval),'run_authorization_changed');
    authority.assertRunCapacityInTransaction(approval);
    const result=recoveries.reserve({saved:initial.saved,proof:approval,key:current.key,requestDigest,contextId:context.context_id,now});
    need(same(approval,authorize(authentication,resumeOperation,current.run.definition_id,context.context_id)),'run_authorization_changed');
    checkRecoveryPreviewWindow(initial.saved.preview.window,authority.runApprovalTimeInTransaction());
    return recoveryReceipt(result);
   });
  }catch(error){
   const prior=writer(lookup).prior;
   if(prior)return recoveryReceipt(prior);
   throw error;
  }
 }
 function visibleRecovery(authentication,runId){
  const run=proposalRun(runId),proof=authorize(authentication,'getRun',run.definition_id,null);
  need(proof.actor.principal_kind==='human','local_authorization_human');return proof;
 }
 function recoveryApprovalBasis(run){
  const reserved=recoveries?.active(run.run_id);
  if(!reserved)return null;
  const current=currentRun(run),{operation,operation_ref}=reserved;
  need((current.lifecycle==='paused'||recoveryDispatch&&current.lifecycle==='running')&&current.active_operation_id===operation.operation_id&&
   current.active_execution_id===operation.new_execution_id&&!current.dispatch_eligible,'run_recovery_approval_changed');
  const {preview}=recoveryPreviews.read(operation.preview_ref);
  return {operation_ref,prior_approval_ref:preview.snapshot.approval_ref};
 }
 function reservedRecovery(runId,operationId){
  const original=recoveries.read(operationId);
  need(original?.operation.run_id===runId,'run_recovery_operation_missing');
  return original;
 }
 function recoveryPreparationState(original){
  const {operation,operation_ref}=original,run=currentRun(proposalRun(operation.run_id));
  const basis=recoveryApprovalBasis(proposalRun(run.run_id));
  need(same(basis?.operation_ref,operation_ref)&&original.execution.mode==='execute'&&
   slotFor(run.definition_id).active_run_id===run.run_id,'run_recovery_preparation_changed');
  need(publicationBasisCurrent(run),'run_publication_binding_unapproved');
  const approval=approvals.currentAuthorityInTransaction(run);
  need(!same(approval.approval_ref,basis.prior_approval_ref),'run_recovery_preparation_changed');
  return {run,approval};
 }
 function recoveryPreparationDeadline(original,registration,state,now){
  const {operation}=original,{run,approval}=state,{authentication,window}=registration;
  need(run.revision===registration.run_revision&&same(approval.approval_ref,registration.approval_ref)&&
   same(run.artifact_ref,registration.artifact_ref)&&digest(authentication)===registration.initiator_authorization_ref.digest,
   'run_recovery_preparation_changed');
  const grantEnd=authority.assertArmInitiatorInTransaction(operation.initiator,run.definition_id,authentication.grant_ref,'run.resume');
  const deadline=[authentication.credential.recent_before_ns,proposalWindowEnd(approval.saved.proposal),grantEnd,window.expires_ns].map(BigInt).reduce((a,b)=>a<b?a:b);
  need(window.service_boot_id===bootId&&same(window.domain,now.domain)&&now.tick>=BigInt(window.started_tick_ns)&&
   now.tick<BigInt(window.expires_tick_ns)&&now.upper<deadline,'run_recovery_preparation_expired');
  return {now,deadline};
 }
 function fixedRecoveryPlan(original,state,evidence,target){
  const {operation,operation_ref}=original,{run,approval}=state,{controller_scope,device_ids}=target,proposal=approval.saved.proposal;
  const endorsed=evidence.basis.endorsements.find(item=>same(item.endorsement_ref.scope,controller_scope));
  const controlled=evidence.controls.find(item=>same(item.controller_scope,controller_scope));
  const policy=endorsed?.target_policy_ref;
  need(policy?.kind==='run_endorsement_policy'&&same(policy.scope,controller_scope)&&same(endorsed.endorsement_ref,controlled?.endorsement_ref)&&
   same(device_ids,controlled.devices?.map(item=>item.device_id)),'run_recovery_approval_control');
  const {device_actions,parameter_constraints}=proposalTarget(proposal,controller_scope);
  const controls=controlled.devices.map(({device_id,control})=>{
   need(control?.mode==='automatic'&&control.gate_state==='protected'&&control.pending_operation_id===null,'run_recovery_approval_control');
   return {device_id,expected_control:{epoch:control.control_epoch,version:control.effective_version}};
  });
  const prior=arm.forRun(run),oldPlan=historicalPlan(prior,preparationTarget(prior,controller_scope));
  const baseline=preparationReceipts.read({...oldPlan,lookup:preparationLookup(oldPlan)});
  need(baseline,'run_recovery_preparation_changed');
  const preparation_id=crypto.randomUUID();
  const body={format:'leafseamer.run-recovery-plan/1',definition_id:run.definition_id,preparation:{
   operation_id:operation.operation_id,preparation_id,controller_scope,origin_scope:scope,run_id:run.run_id,definition_version:run.definition_version,
   arm_generation:operation.recovery_generation,mode:'recovery',execution_id:operation.new_execution_id,operation_ref,
   artifact_ref:run.artifact_ref,approval_ref:approval.approval_ref,authorization_digest:digest(evidence.basis),
   source_digest:operation.baseline_snapshot_digest,controls,baseline_grant_ref:baseline.target_grant_ref,
   grant_intent:{principal:{...proposal.service_principal,principal_kind:'service'},device_actions,parameter_constraints,
    validity:{...proposal.validity,authorization_policy_revision:policy.revision}}},
   controls:controlled.devices.map(({device_id,control})=>({device_id,control_ref:control.control_ref}))};
  const plan_ref={kind:'recovery_plan',scope,object_id:preparation_id,revision:'1',digest:digest(body)};
  const request=armPlanOriginal(body,plan_ref,controller_scope,{recovery:true});
  return {original:body,plan_ref,request,pairing_ref:endorsed.pairing_ref,endorsement_ref:endorsed.endorsement_ref};
 }
 function recoveryPreparationView(original){
  const registration=recoveryPreparations.read(original);
  const targets=registration?.plans.map(plan=>{
   const {preparation_id,controller_scope}=plan.request,received=recoveryPreparations.received(plan);
   return {controller_scope,preparation_id,state:received?'receipt_observed':recoveryPreparations.attempted(preparation_id)?'awaiting_receipt':'registered',
    receipt_ref:received?.receipt_ref??null,grant_ref:received?.grant_ref??null};
  })??[];
  return {operation_ref:original.operation_ref,execution_ref:original.execution_ref,approval_ref:registration?.approval_ref??null,
   state:!registration?'unregistered':targets.every(target=>target.receipt_ref)?'receipts_observed':'pending',targets,dispatch_eligible:false};
 }
 async function prepareRecovery(authentication,runId,operationId,contextId){
  for(const id of [runId,operationId,contextId])checked('LifecycleId',id);
  function capture(){
   const reader=visibleRecovery(authentication,runId),original=reservedRecovery(runId,operationId);
   need(same(reader.actor,original.operation.initiator),'local_authorization_human');
   const registration=recoveryPreparations.read(original);
   if(registration)return {original,registration};
   const state=recoveryPreparationState(original);
   const proof=authorize(authentication,resumeOperation,state.run.definition_id,contextId);
   need(same(proof.actor,reader.actor)&&proof.authentication_evidence?.credential,'local_authorization_human');
   return {original,state,proof};
  }
  const initial=writer(capture);
  function assertRegistrationActor(registration,state){
   need(contextId===registration.context_id,'run_recovery_preparation_changed');
   const proof=authorize(authentication,resumeOperation,state.run.definition_id,contextId);
   const recorded=registration.authentication,live=proof.authentication_evidence;
   need(same(proof.actor,initial.original.operation.initiator)&&same(recorded.grant_ref,live?.grant_ref)&&
    same(recorded.credential,live?.credential)&&registration.authorization_digest===proof.authorization_digest,'run_authorization_changed');
  }
  let registration=initial.registration;
  if(!registration){
   const start=writer(()=>authority.runApprovalTimeInTransaction());
   const evidence=await approvals.observeAuthority(initial.state.approval,{includeControls:true});
   registration=writer(()=>{
    const current=capture();
    if(current.registration)return current.registration;
    need(same(current,initial),'run_recovery_preparation_changed');
    const {original,state,proof}=current,{run,approval}=state,authentication=proof.authentication_evidence;
    evidence.assertCurrentInTransaction();
    authority.assertRunCapacityInTransaction(proof);
    const plans=original.operation.targets.map(target=>fixedRecoveryPlan(original,state,evidence,target));
    // 此版本的人员原件由目标 Controller 本机核验，全集须在提交前符合该约束。
    need(plans.length>0&&plans.every(plan=>plan.request.controller_scope.authority_service_id===authentication.credential.issuer_service_id),
     'run_recovery_preparation_issuer');
    const body={format:'leafseamer.recovery-preparation/1',operation_ref:original.operation_ref,context_id:contextId,
     artifact_ref:run.artifact_ref,approval_ref:approval.approval_ref,run_revision:run.revision,authorization_digest:proof.authorization_digest,authentication,
     initiator_authorization_ref:{id:operationId,revision:'1',digest:digest(authentication)},
     window:{service_boot_id:bootId,domain:start.domain,started_tick_ns:String(start.tick),expires_tick_ns:String(start.tick+recoveryPreparationWindowNs),
      expires_ns:String(start.lower+recoveryPreparationWindowNs)},plans};
    const deadline=recoveryPreparationDeadline(original,body,state,authority.runApprovalTimeInTransaction());
    for(const plan of plans)pairedDeadline(deadline,armPlanPair(plan));
    const registered=recoveryPreparations.register(original,body);
    evidence.assertCurrentInTransaction();
    recoveryPreparationDeadline(original,registered,state,authority.runApprovalTimeInTransaction());
    return registered;
   });
  }
  await inBatches(registration.plans,plan=>deliverRecoveryPreparation(initial.original,registration,plan,assertRegistrationActor));
  return writer(()=>{
   visibleRecovery(authentication,runId);
   return recoveryPreparationView(reservedRecovery(runId,operationId));
  });
 }
 async function deliverRecoveryPreparation(original,registration,plan,assertRegistrationActor){
  const lookup=recoveryPreparations.lookup(plan),id=lookup.preparation_id;
  const send=writer(()=>{
   if(recoveryPreparations.received(plan))return null;
   const pair=armPlanPair(plan);
   if(recoveryPreparations.attempted(id))return {pair,via:'lookup'};
   const state=recoveryPreparationState(original);
   assertRegistrationActor(registration,state);
   pairedDeadline(recoveryPreparationDeadline(original,registration,state,authority.runApprovalTimeInTransaction()),pair);
   need(recoveryPreparations.claim(id),'run_recovery_preparation_changed');
   return {pair,via:'submit'};
  });
  if(!send)return;
  const {pair,via}=send;
  let reply;
  try{
   reply=via==='submit'?await sendRecoveryPreparation(pair.binding_id,plan.request):await lookupRecoveryPreparation(pair.binding_id,lookup);
  }catch{return;}
  if(via==='lookup'&&reply.state!=='found')return;
  writer(()=>{
   need(same(armPlanPair(plan),pair),'run_recovery_preparation_changed');
   recoveryPreparations.adopt(plan,reply.record,via,authority.runApprovalTimeInTransaction(),bootId);
  });
 }
 function recoveryPreparationForPeer(bindingId,socket,request,isPlan){
  function read(){
   const {record,peer}=authorizeControllerPeerInTransaction(bindingId,socket),target_scope=peerScope(peer);
   const operationId=isPlan?recoveryPreparations.operationForPreparation(request.preparation_id):request.operation_id;
   const original=operationId?recoveries.read(operationId):null;
   const registration=original&&recoveryPreparations.read(original);
   const plan=registration?.plans.find(item=>same(item.request.controller_scope,target_scope));
   need(plan&&recoveryPreparations.attempted(plan.request.preparation_id)&&same(pairingReference(record),plan.pairing_ref)&&
    (isPlan?request.plan_digest===plan.plan_ref.digest&&request.preparation_id===plan.plan_ref.object_id:request.operation_digest===original.operation_ref.digest),
    'run_recovery_preparation_changed');
   const state=recoveryPreparationState(original),{run}=state;
   const deadline=recoveryPreparationDeadline(original,registration,state,authority.runApprovalTimeInTransaction());
   const eligible_before_ns=pairedDeadline(deadline,record);
   if(isPlan)return {plan_ref:plan.plan_ref,original:plan.original,eligible_before_ns};
   return {operation_ref:original.operation_ref,run_id:run.run_id,definition_id:run.definition_id,definition_version:run.definition_version,
    arm_generation:run.arm_generation,approval_ref:registration.approval_ref,artifact_ref:run.artifact_ref,target_scope,
    preparation_id:plan.request.preparation_id,execution_id:original.operation.new_execution_id,initiator:original.operation.initiator,
    initiator_authorization_ref:registration.initiator_authorization_ref,credential:registration.authentication.credential,eligible_before_ns};
  }
  const body=writer(read);
  if(!isPlan)return body;
  return {body,beforeSend(){const current=writer(read);need(same(body,current),'run_recovery_preparation_changed');return current;}};
 }
 let pendingRecoveryBindings=0;
 async function qualifyRecoveryExecution(authentication,runId,operationId,contextId,selection){
  for(const id of [runId,operationId,contextId])checked('LifecycleId',id);
  function readSelection(original,registration){
   const bound=recoveryPreparations.binding(original,registration);
   if(selection===null)return {bound,existing:bound};
   need(bound,'run_preparation_receipts_incomplete');
   need(same(bound.binding_ref,selection),'run_recovery_preparation_changed');
   return {bound,existing:recoveryPreparations.start(original,bound)};
  }
  function capture(){
   const reader=visibleRecovery(authentication,runId),original=reservedRecovery(runId,operationId);
   need(same(reader.actor,original.operation.initiator),'local_authorization_human');
   const registration=recoveryPreparations.read(original),{bound,existing}=readSelection(original,registration);
   if(existing)return {existing};
   need(registration,'run_preparation_receipts_incomplete');
   if(bound)need(bound.binding.service_boot_id===bootId,'run_recovery_preparation_changed');
   const {run,approval}=recoveryPreparationState(original);
   need(run.revision===registration.run_revision&&same(approval.approval_ref,registration.approval_ref),'run_recovery_preparation_changed');
   const proof=authorize(authentication,resumeOperation,run.definition_id,contextId);
   need(same(proof.actor,reader.actor)&&proof.authentication_evidence?.credential,'local_authorization_human');
   const grants=recoveryPreparations.grants(registration);
   const pairs=registration.plans.map(armPlanPair);
   return {original,registration,run,approval,proof,grants,pairs,bound};
  }
  need(pendingRecoveryBindings<4,'run_preparation_plan_capacity');
  const {captured,start}=writer(()=>({captured:capture(),start:authority.runApprovalTimeInTransaction()}));
  if(captured.existing)return captured.existing;
  pendingRecoveryBindings++;
  try{
   const requests=captured.grants.map(({lookup,receipt_ref,grant_ref})=>({lookup,receipt_ref,grant_ref}));
   const targets=requests.map((request,index)=>({request,pair:captured.pairs[index]}));
   const observations=await inBatches(targets,async({request,pair})=>{
    try{return await observeRecoveryPreparation(pair.binding_id,structuredClone(request));}
    catch{need(false,'run_recovery_preparation_ineligible');}
   });
   return await observeRecoveryProtection(authentication,runId,captured.original.operation.source_execution_ref.object_id,{
    includeResources:true,previewRevision:captured.run.revision,reservation:captured.original,
    finish(snapshot,plan){
     const current=capture();
     if(current.existing)return current.existing;
     need(same(current,captured)&&digest(snapshot)===current.original.operation.baseline_snapshot_digest&&
      digest(plan)===current.original.operation.plan_digest,'run_recovery_preparation_changed');
     function verify(){
      const proposal=captured.approval.saved.proposal,authentication=captured.proof.authentication_evidence;
      let deadline=[proposalWindowEnd(proposal),BigInt(authentication.credential.recent_before_ns),start.lower+recoveryPreparationWindowNs,
       authority.assertArmInitiatorInTransaction(captured.original.operation.initiator,captured.run.definition_id,authentication.grant_ref,'run.resume')]
       .map(BigInt).reduce((a,b)=>a<b?a:b);
      const qualifications=observations.map(({original,assertCurrentInTransaction},index)=>{
       need(original.state==='eligible'&&same(original.request,requests[index])&&assertCurrentInTransaction()===true,
        'run_recovery_preparation_ineligible');
       const expires=BigInt(captured.pairs[index].expires_ns),limit=BigInt(original.eligible_before_ns);
       need(limit<=expires,'run_recovery_preparation_ineligible');
       if(limit<deadline)deadline=limit;
       return original;
      });
      const now=authority.runApprovalTimeInTransaction();
      need(same(now.domain,start.domain)&&now.tick>=start.tick&&now.tick<start.tick+recoveryPreparationWindowNs&&
       now.tick<start.validBeforeTick&&now.upper<deadline,'run_recovery_preparation_expired');
      return {qualifications,deadline,now};
     }
     const {original,registration,run,approval,proof,grants,bound}=current;
     const {qualifications,deadline,now}=verify();
     authority.assertRunCapacityInTransaction(proof);
     const common={context_id:contextId,operation_ref:original.operation_ref,execution_ref:original.execution_ref,approval_ref:approval.approval_ref,
      arm_generation:run.arm_generation,authorization_digest:proof.authorization_digest,dispatch_eligible:false,service_boot_id:bootId,qualifications,
      observed_at:{domain:now.domain,tick_ns:String(now.tick),acquisition_ref:now.acquisition_ref},eligible_before_ns:String(deadline)};
     const committed=bound?recoveryPreparations.recordStart(original,bound,{
      ...common,format:'leafseamer.recovery-start/1',binding_ref:bound.binding_ref,previous_run_revision:run.revision,
      run_revision:nextExecutionRevision(run.revision),slot_revision:original.operation.committed_slot_revision,
      state:'start_recorded',steps:recoveryStartSteps(original,registration,grants)
     }):recoveryPreparations.bind(original,registration,{
      ...common,format:'leafseamer.recovery-execution-binding/1',run_revision:run.revision,
      registration_digest:digest(registration),plan_digest:original.operation.plan_digest,grants
     });
     const final=verify();
     const currentAuthority=recoveryPreparationState(original);
     need(same(final.qualifications,qualifications)&&final.deadline===deadline&&same(currentAuthority,{run,approval})&&
      same(registration.plans.map(armPlanPair),captured.pairs)&&
      same(proof,authorize(authentication,resumeOperation,run.definition_id,contextId))&&same(run,currentRun(proposalRun(runId))),
      'run_recovery_preparation_changed');
     if(bound){
      recoveryPreparations.advanceStart(committed.start,runId);
      const after=verify();
      need(after.deadline===deadline&&same(after.qualifications,qualifications),'run_recovery_preparation_changed');
     }
     return committed;
    }
   });
  }catch(error){
   const prior=writer(()=>{
    const proof=visibleRecovery(authentication,runId),original=reservedRecovery(runId,operationId);
    need(same(proof.actor,original.operation.initiator),'local_authorization_human');
    return readSelection(original,recoveryPreparations.read(original)).existing;
   });
   if(prior)return prior;
   throw error;
  }finally{pendingRecoveryBindings--;}
 }
 function recoveryStartSteps(original,registration,grants){
  const previous=executions.read(original.operation.source_execution_ref.object_id);
  need(same(previous?.execution_ref,original.operation.source_execution_ref)&&original.plan.mode==='execute'&&
   previous.execution.origin_kind==='trigger'&&original.plan.items.length===previous.steps.length,'run_recovery_preparation_changed');
  return original.plan.items.map((item,index)=>{
   const {plan}=previous.steps[index];
   need(item.step_id===plan.step_id&&item.plan_digest===digest(plan)&&['retain_success','execute_new'].includes(item.decision),
    'run_recovery_preparation_changed');
   let grant_ref=null;
   if(item.decision==='execute_new'&&plan.payload.kind==='action'){
    const {controller_scope,device_action}=plan.payload.binding.target;
    const target=registration.plans.findIndex(({request})=>same(request.controller_scope,controller_scope)&&
     request.grant_intent.device_actions.some(action=>same(action,device_action)));
    need(target>=0,'run_recovery_preparation_changed');
    grant_ref=grants[target].grant_ref;
   }
   return {...item,grant_ref};
  });
 }
 async function startRecoveryExecution(authentication,runId,operationId,request){
  need(fields(request,'binding_ref,context_id'),'run_recovery_preview_reference');
  const {context_id,binding_ref}=structuredClone(request);
  need(fields(binding_ref,'digest,kind,object_id,revision,scope')&&binding_ref.kind==='recovery_execution_binding'&&
   binding_ref.revision==='1'&&same(binding_ref.scope,scope),'run_recovery_preview_reference');
  checked('LifecycleId',binding_ref.object_id);checked('LifecycleDigest',binding_ref.digest);
  const result=await qualifyRecoveryExecution(authentication,runId,operationId,context_id,binding_ref);
  if(recoveryDispatch&&result.start.service_boot_id===bootId)queueRecoveryDispatch(result.start.execution_ref.object_id);
  return result;
 }
 function readRecoveryDispatch(executionId){
  const recovery=recoveries.execution(executionId);
  if(!recovery)return null;
  const registration=recoveryPreparations.read(recovery),bound=recoveryPreparations.binding(recovery,registration);
  const started=recoveryPreparations.start(recovery,bound);
  if(!started)return null;
  const {operation}=recovery,{start,start_ref}=started,previous=executions.read(operation.source_execution_ref.object_id);
  need(same(previous?.execution_ref,operation.source_execution_ref)&&
   same(start.steps,recoveryStartSteps(recovery,registration,bound.binding.grants)),'run_recovery_dispatch_integrity');
  const steps=start.steps.map((item,index)=>{
   const {plan:source_step_plan,prepared_parameters:source_parameters}=previous.steps[index];
   const plan={...source_step_plan,execution_id:executionId};
   if(item.grant_ref)plan.payload={...plan.payload,binding:{...plan.payload.binding,grant_ref:item.grant_ref}};
   return {item,plan,source_step_plan,source_parameters:source_parameters??null};
  });
  const execution={format:'leafseamer.recovery-action-execution/1',origin_kind:'recovery',execution_id:executionId,scope,
   run_id:operation.run_id,definition_version:registration.plans[0].request.definition_version,
   arm_generation:operation.recovery_generation,artifact_ref:registration.artifact_ref,service_boot_id:start.service_boot_id,
   reservation_ref:recovery.execution_ref,binding_ref:bound.binding_ref,start_ref,origin_digest:start_ref.digest,
   steps:steps.map(({plan})=>({step_id:plan.step_id,plan_digest:digest(plan)})),
   grant_basis:bound.binding.grants.map(member=>({...member,eligible_before_ns:start.eligible_before_ns})),approved_before_ns:start.eligible_before_ns};
  const execution_ref={kind:'execution',scope,object_id:executionId,revision:'2',digest:digest(execution)};
  return recoveryDispatch.read({execution,execution_ref,steps,recovery,registration,bound,started,source:previous.execution});
 }
 function recoveryCanRun(recovery,revision){
  const original=recoveryPreparations.start(recovery);
  if(!original||original.start.service_boot_id!==bootId||original.start.run_revision!==revision||
   recoveryDispatch.protection(recovery.operation.new_execution_id)||approvals.armRevocationLatch(original.start.approval_ref))return false;
  try{recoveryStartClock(original.start);return true;}
  catch{return false;}
 }
 function recoveryStartClock({observed_at,eligible_before_ns}){
  const now=authority.runApprovalTimeInTransaction();
  need(same(now.domain,observed_at.domain)&&now.tick>=BigInt(observed_at.tick_ns)&&now.tick<now.validBeforeTick&&
   now.upper<BigInt(eligible_before_ns),'run_recovery_dispatch_expired');
  return {tick_ns:String(now.tick),upper_ns:String(now.upper),valid_before_tick_ns:String(now.validBeforeTick),
   runtime_domain_ref:now.domain,acquisition_ref:now.acquisition_ref};
 }
 const recoveryDispatchClock=value=>recoveryStartClock(value.started.start);
 function recoveryDependencies(value,step){
  const evidence=[];
  for(const {from_step_id} of step.plan.dependencies){
   const predecessor=value.steps.find(item=>item.plan.step_id===from_step_id);
   if(predecessor?.item.decision==='retain_success'){
    evidence.push({step_id:from_step_id,completion_evidence_refs:predecessor.item.completion_evidence_refs});
    continue;
   }
   const terminal=predecessor?.submission?.terminal;
   if(terminal?.state.outcome!=='succeeded')return null;
   evidence.push({step_id:from_step_id,terminal_digest:digest(terminal)});
  }
  return evidence;
 }
 const runningRecoveryDispatch=new Set();
 function queueRecoveryDispatch(id){
  if(!executionScheduling||!isOpen()||!db.open||runningRecoveryDispatch.has(id))return;
  runningRecoveryDispatch.add(id);
  queueMicrotask(async()=>{
   try{if(executionScheduling&&isOpen()&&db.open)await dispatchRecovery(id);}
   catch{
    if(executionScheduling&&isOpen()&&db.open)try{writer(()=>recoveryDispatch.protect(id,'recovery_dispatch_unavailable'));}catch{}
   }finally{actionReaders.delete(id);runningRecoveryDispatch.delete(id);}
  });
 }
 function scheduleRecoveryTerminal(id,stepId){
  const running=()=>executionScheduling&&isOpen()&&db.open;
  if(!running())return;
  const timer=setTimeout(async()=>{
   delayTimers.delete(id);
   if(!running()||await readAndAdoptTerminal(id,stepId,running))return;
   try{
    const pending=writer(()=>{
     const {value,submission}=recordedSubmission(id,stepId);
     if(value.protection||submission.terminal)return false;
     recoveryDispatchClock(value);return true;
    });
    if(pending)scheduleRecoveryTerminal(id,stepId);
   }catch{}
  },100).unref();
  delayTimers.set(id,timer);
 }
 async function dispatchRecovery(id){
  const running=()=>executionScheduling&&isOpen()&&db.open;
  const initial=writer(()=>{
   const value=readRecoveryDispatch(id);
   if(!value||value.protection||value.execution.service_boot_id!==bootId||
    value.steps.some(step=>step.submission?.attempt&&!step.submission.terminal))return null;
   const step=value.steps.find(candidate=>candidate.item.grant_ref&&!candidate.submission&&!candidate.ready&&recoveryDependencies(value,candidate));
   if(!step)return null;
   return {value,step,clock:recoveryDispatchClock(value)};
  });
  if(!initial)return;
  const {recovery,registration,execution,execution_ref,started}=initial.value,stepId=initial.step.plan.step_id;
  const {snapshot}=writer(()=>recoveryPreviews.read(recovery.operation.preview_ref).preview);
  const grant=execution.grant_basis.find(member=>same(member.grant_ref,initial.step.item.grant_ref));
  const plan=registration.plans.find(member=>member.request.preparation_id===grant.lookup.preparation_id);
  const request={lookup:grant.lookup,receipt_ref:grant.receipt_ref,grant_ref:grant.grant_ref};
  const bundle=publications.prepareExecutionProgram(snapshot.definition_id,snapshot.definition_version),publication=bundle.publication;
  const {compiled}=publication;
  const [proof,transports]=await Promise.all([observeRecoveryPreparation(grant.pairing_ref.id,structuredClone(request)),readArmSourceTransports(snapshot.sources)]);
  if(!running())return;
  const qualified=proof.original;
  need(qualified.state==='eligible'&&same(qualified.request,request),'run_recovery_preparation_ineligible');
  const deadline=String([execution.approved_before_ns,qualified.eligible_before_ns].map(BigInt).reduce((a,b)=>a<b?a:b));
  function current(){
   const value=readRecoveryDispatch(id),step=value?.steps.find(candidate=>candidate.plan.step_id===stepId);
   need(running()&&value&&!value.protection&&same(value.execution_ref,execution_ref)&&step,'run_recovery_dispatch_changed');
   const {run,approval}=recoveryPreparationState(recovery);
   const dependencies=readDependencies(run);
   need(run.lifecycle==='running'&&run.revision===started.start.run_revision&&same(approval.approval_ref,started.start.approval_ref)&&
    same(run.business_state,snapshot.business_state)&&digest(run.pause_latches)===snapshot.latch_set_digest&&dependencies.digest===snapshot.dependency_digest&&
    same(resolveAutomationSourcesInTransaction(dependencies.body.dependencies.inputs),snapshot.sources.map(({transport,...item})=>item)),
    'run_recovery_dispatch_changed');
   assertArmSourceTransports(snapshot.sources,transports);
   bundle.assertOriginalInTransaction();
   need(same(publication.result.artifact_ref,execution.artifact_ref)&&proof.assertCurrentInTransaction()===true,'run_recovery_dispatch_changed');
   const pair=armPlanPair(plan),clock=recoveryDispatchClock(value);
   need(same(pairingReference(pair),grant.pairing_ref)&&BigInt(clock.upper_ns)<BigInt(deadline)&&
    BigInt(clock.upper_ns)<BigInt(pair.expires_ns)&&BigInt(clock.tick_ns)>=BigInt(initial.clock.tick_ns)&&
    BigInt(clock.tick_ns)<BigInt(initial.clock.valid_before_tick_ns),'run_recovery_dispatch_expired');
   const command_terminal_refs=recoveryDependencies(value,step);
   need(command_terminal_refs!==null,'run_recovery_dispatch_changed');
   return {value,step,clock,command_terminal_refs,pair};
  }
  const actualGrant=writer(()=>{
   current();return recoveryPreparations.received(plan).original.record.applied_receipt.grant;
  });
  const source=initial.step.source_parameters;
  let result;
  if(source){
   need(source.ref.digest===initial.step.item.parameters_digest,'run_recovery_dispatch_parameters');
   result=source.body;
  }else{
   const trigger=writer(()=>{
    current();
    const event=readRecoveryDispatchOrigin(initial.value.source.receive_cursor);
    need(event.origin_digest===initial.value.source.origin_digest,'execution_action_origin');
    return event.origin;
   });
   const snapshots=eventInputSnapshots(compiled.compiled.input_bindings,trigger),stepPlan=compiled.stepPlan(stepId);
   need(stepPlan&&evaluateExpression(stepPlan.guard,snapshots).value===true,'execution_action_guard');
   result=await compiled.parameterExecution({snapshots,schemas:parameterSchemas(publication)}).resolve(stepId);
   need(result.status==='validated'&&result.action_binding_id===initial.step.plan.payload.binding.action_binding_ref.id,'execution_action_parameters');
  }
  const action=initial.step.plan.payload.binding;
  need(same(result.parameter_schema_ref,action.parameter_schema_ref)&&digest(result.parameters)===result.parameters_digest,'run_recovery_dispatch_parameters');
  assertRunGrantParameters(actualGrant,action.target.device_action,result.parameters);
  if(!running())return;
  const ledger=recoveryDispatch.submissions;
  const recorded=writer(()=>{
   const {value,step,clock,command_terminal_refs}=current();
   recoveryDispatch.seal(value,step,result,deadline,clock,command_terminal_refs);
   const after=current();
   const budget=arm.forRun(proposalRun(execution.run_id)).operation.budget.required;
   ledger.plan(after.value,after.step,{request,pairing_ref:grant.pairing_ref,grant:actualGrant},after.clock,
    (bytes,rows)=>recoveryDispatch.reserve(id,budget,bytes,rows));
   return current().step.submission;
  });
  const original=writer(()=>{
   const {value,step,clock}=current();
   need(same(step.submission,recorded),'execution_submission_changed');
   return {...recorded,attempt:ledger.attempt(value,step,clock)};
  });
  const slot=submissionSlot(id,stepId);submitting.add(slot);
  actionReaders.set(id,(bindingId,socket,input)=>{
   const verify=()=>writer(()=>{
    const {step,clock,command_terminal_refs,pair}=current(),paired=authorizeControllerPeerInTransaction(bindingId,socket);
    need(same(input,original.body.request.action)&&same(step.submission?.attempt,original.attempt)&&
     same(pair,paired.record)&&same(peerScope(paired.peer),grant.lookup.controller_scope)&&
     same(step.ready.body.command_terminal_refs,command_terminal_refs),'execution_action_binding');
    recoveryDispatch.checkClock(initial.value,step,clock,original.attempt.clock);
    return {execution,step_plan:step.plan,step_ready:step.ready,parameters:step.prepared_parameters,eligible_before_ns:deadline,
     recovery:{binding:initial.value.bound.binding,start:started.start,source_step_plan:step.source_step_plan,source_parameters:step.source_parameters}};
   });
   const body=verify();
   return {body,beforeSend(){const latest=verify();need(same(body,latest),'execution_action_binding');return latest;}};
  });
  try{
   writer(()=>need(same(current().step.submission?.attempt,original.attempt),'execution_submission_changed'));
   const reply=await submitAutomationCommand(grant.pairing_ref.id,structuredClone(original.body.request));
   const adopted=isOpen()&&db.open?writer(()=>adoptLookup(original,reply)):null;
   if(adopted?.decision==='accepted')scheduleRecoveryTerminal(id,stepId);
  }catch{
   if(!isOpen()||!db.open)return;
   writer(()=>adoptSubmission(original,null));
   const found=await lookupSubmission(original).catch(()=>null);
   if(found&&isOpen()&&db.open)writer(()=>adoptLookup(original,found));
  }finally{submitting.delete(slot);}
 }
 return Object.freeze({
  ...(executions?.recordScheduled&&{inspectScheduleInTransaction,isScheduleSlotBusyInTransaction(definitionId){
   need(db.inTransaction,'execution_schedule_transaction');
   return Boolean(busySlot.get(definitionId));
  },...(withScheduledRuntime&&{resolveScheduledSource(candidate){
   return writer(()=>{
    const {schedule_id,occurrence_id,activation_ref,definition_id,definition_version,definition_artifact_ref}=candidate;
    const slot=slotFor(definition_id);
    if(slot.active_run_id===null)return null;
    const run=currentRun(proposalRun(slot.active_run_id));
    if(run.lifecycle!=='armed'||run.active_execution_id!==null||run.pause_latches.length||
     run.definition_version!==definition_version||!same(run.artifact_ref,definition_artifact_ref))return null;
    return {schedule_id,occurrence_id,activation_ref,run_id:run.run_id};
   });
  }}),async admitScheduledOccurrence(source,readClock,sample){
   need(typeof readClock==='function'&&(!withScheduledRuntime||typeof sample==='function'&&executionScheduling),'execution_schedule_dependencies');
   const fixed=structuredClone(source);
   checked('LifecycleId',fixed.run_id);
   const captured=writer(()=>proposalRun(fixed.run_id));
   const {publication,assertOriginalInTransaction}=publications.prepareExecutionProgram(captured.definition_id,captured.definition_version);
   function currentPlan(){
    need(!withScheduledRuntime||executionScheduling,'execution_schedule_dependencies');
    const run=currentRun(proposalRun(fixed.run_id));
    need(run.lifecycle==='armed'&&run.active_execution_id===null&&run.pause_latches.length===0,'execution_schedule_run');
    const approval=approvals.currentAuthorityInTransaction(run);
    assertOriginalInTransaction();
    need(Object.hasOwn(publication.snapshot.review,'binding_approval_refs')&&publicationBasisCurrent(run,publication),'execution_schedule_publication');
    const time=schedules.timeForAdmissionInTransaction(fixed,readClock());
    const selection=scheduleSelection(run,publication,time.first_step_id);
    need(selection.state==='ready_for_preparation','execution_schedule_'+selection.reason);
    return {run,approval,selectedBranch:selection.selected_branch,firstStepId:time.first_step_id};
   }
   const prepared=withScheduledRuntime?writer(()=>{
    const {run,selectedBranch,firstStepId}=currentPlan(),{preflight,grants}=executionGrantInputs(run);
    const steps=selectedExecutionSteps(publication.compiled,run,selectedBranch);
    const targets=executionActions(run,publication,steps,preflight,grants);
    return {firstStepId,observations:[...scheduledGrantDevices(targets)]
     .map(([grant,schedule_device_ids])=>({...grant,runId:run.run_id,schedule_device_ids}))};
   }):null;
   let firstParameters=null;
   if(prepared){
    const {firstStepId}=prepared,{compiled}=publication,plan=compiled.stepPlan(firstStepId);
    // 固定参数可以在目标开闸前校验。使用时仍核对发布原件、步骤、RunGrant 和当前窗口。
    if(plan?.parameters.every(parameter=>parameter.value.references.length===0)){
     const result=await compiled.parameterExecution({snapshots:new Map(),schemas:parameterSchemas(publication)}).resolve(firstStepId);
     firstParameters={step_id:firstStepId,artifact_ref:publication.result.artifact_ref,result};
    }
   }
   const grants=prepared?await observeExecutionGrants(prepared.observations):await prepareExecutionGrants(()=>[fixed.run_id]);
   const {item,recheck}=writer(()=>{
    const {run,approval,selectedBranch}=currentPlan();
    const plan=planExecution(run,publication,approval,selectedBranch,{grants,requireScheduled:withScheduledRuntime});
    need(plan,'execution_schedule_grant');
    return {item:executions.recordScheduled(run,fixed,selectedBranch,plan,readClock),recheck:plan.assertCurrentInTransaction};
   });
   if(withScheduledRuntime){
    const {execution_ref,admission}=item;
    try{
     const afterCommit=sample(),floor={runtime_domain_ref:admission.clock.domain,tick_ns:admission.clock.tick_ns};
     const hasWork=writer(()=>{
      const saved=executions.read(execution_ref.object_id),execution=saved?.execution;
      need(execution?.origin_kind==='schedule'&&same(saved.execution_ref,execution_ref),'execution_anchor_identity');
      const run=currentRun(proposalRun(execution.run_id));
      need(run.active_execution_id===execution.execution_id&&run.revision===execution.run_revision&&
       same(run.pause_latches,[execution.execution_id]),'execution_anchor_inactive');
      approvals.currentAuthorityInTransaction(run);
      const first=saved.steps.find(({plan})=>plan.step_id===admission.first_step_id);
      const check=()=>{
       recheck();
       need(first&&schedules.canStartActionInTransaction(execution,first.plan,readClock()),'schedule_execution_window');
      };
      check();
      const result=executions.anchorAdmission(saved,item,floor,afterCommit,sample);
      check();
      return result;
     });
     if(hasWork)scheduleExecution(item,{sample,scheduleClock:readClock,admissionGrants:grants,firstParameters});
    }catch{
     // Once admission commits, a failed anchor leaves the active slot paused.
    }
   }
   return item;
  }}),
  ...(executions&&{getExecution(authentication,executionId){return writer(()=>{
   const recovery=recoveries?.execution(executionId);
   if(recovery){
    visibleRecovery(authentication,recovery.operation.run_id);
    const {execution,execution_ref,plan}=recovery;
    const started=recoveryPreparations?.start?.(recovery);
    if(started){
     const dispatched=recoveryDispatch&&readRecoveryDispatch(executionId);
     const run=currentRun(proposalRun(recovery.operation.run_id));
     const protection=run.lifecycle==='running'?null:dispatched?.protection??(!recoveryDispatch?'recovery_dispatch_pending':
      started.start.service_boot_id!==bootId?'recovery_restart_protected':'recovery_dispatch_inactive');
     return {execution,execution_ref,plan,steps:started.start.steps,start:started,protection,completion:null,...(dispatched&&{dispatch:{
      execution_ref:dispatched.execution_ref,steps:dispatched.steps.map(({item,prepared_parameters,submission})=>({step_id:item.step_id,
       parameters_ref:prepared_parameters?.ref??null,submission}))
     }})};
    }
    return {execution,execution_ref,plan,steps:[],protection:'recovery_reserved',completion:null};
   }
   const result=executions.read(executionId);need(result,'execution_missing');
   const run=proposalRun(result.execution.run_id);
   authorize(authentication,'getRun',run.definition_id,null);
   const protectedResult=delayFailures.has(executionId)?{...result,protection:'execution_runtime_unavailable'}:result;
   if(!executionCommits)return protectedResult;
   const completion=executionCommits.read(executionId);
   return {...protectedResult,completion,reconciliation_required:!completion&&result.execution.activation_slot_revision!==undefined&&
    Boolean(protectedResult.protection||result.execution.service_boot_id!==bootId)&&completionEvidence(result.steps)!==null};
  });}}),
  ...(executionCommits&&{inspectRecoveryEvidence(authentication,runId,executionId){
   checked('LifecycleId',runId);checked('LifecycleId',executionId);
   return writer(()=>recoveryEvidenceInTransaction(authentication,runId,executionId).evidence);
  },observeRecoveryProtection:(authentication,runId,executionId)=>observeRecoveryProtection(authentication,runId,executionId),
  preflightRecovery:(authentication,runId,executionId)=>observeRecoveryProtection(authentication,runId,executionId,{includeResources:true})}),
  ...(recoveryPreviews&&{previewRecovery,getRecoveryPreview:(authentication,runId,reference)=>writer(()=>recoveryPreviewInTransaction(authentication,runId,reference).saved),
   revalidateRecoveryPreview}),
  ...(recoveryPreparations&&{prepareRecovery,getRecoveryPreparation(authentication,runId,operationId){
   for(const id of [runId,operationId])checked('LifecycleId',id);
   return writer(()=>{visibleRecovery(authentication,runId);return recoveryPreparationView(reservedRecovery(runId,operationId));});
  },prepareRecoveryPlanForPeer:(bindingId,socket,request)=>recoveryPreparationForPeer(bindingId,socket,request,true),
  readRecoveryInitiatorForPeer:(bindingId,socket,request)=>recoveryPreparationForPeer(bindingId,socket,request,false)}),
  ...(recoveryPreparations?.binding&&{bindRecoveryExecution:(authentication,runId,operationId,contextId)=>qualifyRecoveryExecution(authentication,runId,operationId,contextId,null),
  getRecoveryExecutionBinding(authentication,runId,operationId){
   for(const id of [runId,operationId])checked('LifecycleId',id);
   return writer(()=>{visibleRecovery(authentication,runId);return recoveryPreparations.binding(reservedRecovery(runId,operationId));});
  }}),
  ...(recoveryPreparations?.start&&{startRecoveryExecution}),
  ...(recoveries&&{resumeRun,getRecoveryOperation(authentication,runId,operationId){
   checked('LifecycleId',runId);checked('LifecycleId',operationId);
   return writer(()=>{
    visibleRecovery(authentication,runId);
    const original=recoveries.read(operationId);
    need(original?.operation.run_id===runId,'run_recovery_operation_missing');
    return recoveryReceipt(original);
   });
  },lookupRecoveryOperation(authentication,runId,{context_id,idempotency_key}){
   for(const value of [runId,context_id,idempotency_key])checked('LifecycleId',value);
   return writer(()=>{
    const proof=visibleRecovery(authentication,runId),read=authority.lookupInTransaction({authentication,context_id,operation_id:resumeOperation});
    need(same(proof.actor,read.actor)&&same(read.scope,scope)&&read.request_epoch===service.request_epoch,'run_authorization_changed');
    const original=recoveries.prior(requestIdentity(read,idempotency_key,resumeOperation));
    need(!original||original.operation.run_id===runId,'run_recovery_operation_conflict');
    return {result:original?recoveryReceipt(original):null};
   });
  }}),
  ...(submissions&&{startSubmissionRecovery,async reconcileAutomationSubmission(authentication,executionId,stepId){
   const slot=submissionSlot(executionId,stepId);
   need(!submitting.has(slot),'execution_submission_in_flight');
   submitting.add(slot);
   try{
    const captured=writer(()=>{
     const {value,submission}=recordedSubmission(executionId,stepId),run=proposalRun(value.execution.run_id);
     const proof=authorize(authentication,'getRun',run.definition_id,null);
     return {definitionId:run.definition_id,proof,submission};
    });
    if(captured.submission.adjudication)return captured.submission.adjudication;
    const found=await lookupSubmission(captured.submission);
    return writer(()=>{
     need(same(captured.proof,authorize(authentication,'getRun',captured.definitionId,null)),'run_authorization_changed');
     return adoptLookup(captured.submission,found);
    });
   }finally{submitting.delete(slot);}
  }}),
  ...(withDelays&&{stopExecutionTimers(){executionScheduling=false;submissionRecovery?.();for(const timer of delayTimers.values())clearTimeout(timer);delayTimers.clear();actionReaders.clear();}}),
  ...(withActionParameters&&{prepareActionBindingForPeer(bindingId,socket,request){
   const fixed=structuredClone(request),reader=actionReaders.get(fixed.execution_ref?.object_id);
   need(reader,'execution_action_binding');
   return reader(bindingId,socket,fixed);
  }}),
  ...(triggerDecisions&&{triggerDecisions:Object.freeze({read:triggerDecisions.read,prepare:prepareTriggerPrograms,record(match,observation,liveEvidence,origin,prepared){
   const admitted=triggerDecisions.record(match,observation,target=>currentRun(proposalRun(target.run_id)),liveEvidence,triggerEvaluator(prepared,origin,liveEvidence));
   if(!withExecutionAnchors||!admitted.length)return null;
   let consumed=false;
   return (floor,sample,recheckDelivery,checkConnection,readEvent)=>{
    need(!consumed&&!db.inTransaction,'execution_anchor_transaction');
    consumed=true;
    for(const item of admitted){
     try{
      const afterCommit=sample();
      const hasWork=writer(()=>{
       const saved=executions.read(item.execution_ref.object_id);
       need(saved&&same(saved.execution_ref,item.execution_ref),'execution_anchor_identity');
       const execution=saved.execution,run=currentRun(proposalRun(execution.run_id));
       need(run.active_execution_id===execution.execution_id&&run.revision===execution.run_revision&&
        same(run.pause_latches,[execution.execution_id]),'execution_anchor_inactive');
       approvals.currentAuthorityInTransaction(run);
       recheckDelivery();item.recheck();
       const delay=executions.anchorAdmission(saved,item,floor,afterCommit,sample);
       recheckDelivery();item.recheck();
       return delay;
      });
      if(withDelays&&hasWork){
       const {execution_ref,planUsage,budget}=item;
       scheduleExecution({execution_ref,planUsage,budget},{sample,checkConnection,predecessor:liveEvidence.predecessor,readEvent,recheckDelivery});
      }
     }catch{
      // 原事件已经持久提交。缺少锚的活动槽保持暂停，重投不补锚。
     }
    }
   };
  }})}),
  ...(approvals&&{approve:approvals.approve,getCurrentApproval:approvals.getCurrentApproval,getApprovalRevision:approvals.getApprovalRevision,
   getApprovalOperation:approvals.getApprovalOperation,lookupApproval:approvals.lookupApproval,readApprovalForPeer:approvals.readForPeer}),
  ...(approvals?.revokeApproval&&{revokeApproval:approvals.revokeApproval,lookupApprovalRevocation:approvals.lookupApprovalRevocation,
   prepareApprovalProtection:approvals.prepareApprovalProtection}),
  ...(approvals?.deliverApprovalProtection&&{scanApprovalProtection:approvals.scanApprovalProtection,deliverApprovalProtection:approvals.deliverApprovalProtection}),
  ...(approvals?.adoptBindingRevocations&&{adoptBindingRevocations:approvals.adoptBindingRevocations}),
  ...(approvals?.receiveEndorsementRevocation&&{receiveEndorsementRevocation:approvals.receiveEndorsementRevocation}),
  ...(Number(service.schema_revision)>=10&&{inspectArmPrerequisites(authentication,id,expectedRevision){return observeArmPrerequisites(authentication,id,expectedRevision);}}),
  ...(Number(service.schema_revision)>=13&&{recordArmPreflight,getArmPreflight,revalidateArmPreflight}),
  ...(armCompletion&&{completeArmRun}),
  ...(preparationReceipts&&{
   prepareArmCompletionEvidence,scanArmPreparation,deliverArmPreparation,
   submitArmPreparation:(reference,target)=>exchangeArmPreparation(reference,target,'submit'),
   lookupArmPreparation:(reference,target)=>exchangeArmPreparation(reference,target,'lookup'),
   armPreparationObservations(reference){
    return writer(()=>{
     const registered=registeredArm(reference);
     return registered.operation.targets.map(({controller_scope,preparation_id,...target})=>{
      const key={controller_scope,preparation_id};
      if(!preparationReceipts.has(preparation_id))return {...key,state:'pending',receipt_ref:null,target_grant_ref:null};
      const plan=historicalPlan(registered,{...key,...target}),read=preparationReceipts.read({...plan,lookup:preparationLookup(plan)});
      return {...key,state:'receipt_observed',receipt_ref:read.receipt_ref,target_grant_ref:read.target_grant_ref};
     });
    });
   }
  }),
  ...(arm&&{armRun,observeAndArmRun,prepareArmPlanForPeer,armPreparationIntents(reference){
   return writer(()=>{
    const registered=registeredArm(reference);
    if(registered.operation.targets.length===0)return [];
    const state=armReleaseState(registered);assertArmPlanSources(state);
    return registered.operation.targets.map(target=>{
     const plan=fixedArmPlan(state,target),pair=armPlanPair(plan);
     pairedDeadline(state,pair);
     return {binding_id:pair.binding_id,request:plan.request,lookup:preparationLookup(plan)};
    });
   });
  },armPreparationLookupIntent(reference,targetScope){
   return writer(()=>{
    const registered=registeredArm(reference),plan=historicalPlan(registered,preparationTarget(registered,targetScope));
    return {binding_id:armPlanPair(plan).binding_id,lookup:preparationLookup(plan)};
   });
  },readArmInitiatorForPeer(bindingId,socket,request){
   return writer(()=>{
    const paired=authorizeControllerPeerInTransaction(bindingId,socket),target_scope=peerScope(paired.peer);
    const registered=arm.operation(request.operation_id,request.operation_digest),{operation_ref,operation}=registered;
    const target=operation.targets.find(item=>same(item.controller_scope,target_scope));
    const {initiator,initiator_authentication:authentication,initiator_authorization_ref}=operation,credential=authentication.credential;
    need(target&&target.state==='pending'&&credential?.issuer_service_id===target_scope.authority_service_id,'run_initiator_scope');
    const state=armReleaseState(registered),{run,approval}=state,eligible_before_ns=pairedDeadline(state,paired.record);
    need(same(authorizeControllerPeerInTransaction(bindingId,socket),paired),'run_initiator_expired');
    return {operation_ref,run_id:run.run_id,definition_id:run.definition_id,definition_version:run.definition_version,arm_generation:run.arm_generation,
     approval_ref:approval.approval_ref,artifact_ref:run.artifact_ref,target_scope,preparation_id:target.preparation_id,initiator,initiator_authorization_ref,
     credential,eligible_before_ns};
   });
  },getActivationSlot(authentication,definitionId){
   checked('LifecycleId',definitionId);
   return writer(()=>{
    authorize(authentication,'getRun',definitionId,null);
    need(db.prepare('SELECT 1 FROM definition_identity WHERE definition_id=?').pluck().get(definitionId),'run_arm_definition_missing');
    const slot=slotFor(definitionId);
    if(!armCompletion||slot.active_run_id===null)return slot;
    const recovery=recoveries?.active(slot.active_run_id);
    if(recovery)return {...slot,revision:recovery.operation.committed_slot_revision,active_operation_id:recovery.operation.operation_id};
    const registered=arm.forRun(proposalRun(slot.active_run_id));
    return armCompletion.read(registered)?{...slot,active_operation_id:null}:slot;
   });
  }}),
  create(authentication,request){
   need(fields(request,'context,definition_id,definition_version,expected_artifact_ref')&&fields(request.context,'context_id,idempotency_key'),'run_request');
   const fixed=structuredClone(request),{context,definition_id,definition_version,expected_artifact_ref}=fixed;
   for(const value of [context.context_id,context.idempotency_key,definition_id])checked('LifecycleId',value);
   checked('LifecycleRevision',definition_version);
   return writer(()=>{
    const proof=authorize(authentication,'createRun',definition_id,context.context_id);
    const identity=requestIdentity(proof,context.idempotency_key);
    const requestDigest=digest({version:'automation-lifecycle/0.1.0-draft.1',operation_id:'createRun',definition_id,request:{definition_version,expected_artifact_ref}});
    const prior=db.prepare(select+' WHERE identity_digest=?').raw().get(identity);
    if(prior){
     const saved=original(prior);
     need(saved.requestDigest===requestDigest,'run_idempotency_conflict');
     need(same(proof,authorize(authentication,'createRun',definition_id,context.context_id)),'run_authorization_changed');
     return view(saved.run);
    }
    const published=publications.versionForRunInTransaction(definition_id,definition_version);
    need(same(published.result.artifact_ref,expected_artifact_ref),'run_artifact_conflict');
    authority.assertRunCapacityInTransaction(proof);
    need(db.prepare('SELECT count(*) FROM automation_run_creation').pluck().get()<16384,'run_capacity');
    const dependencyDigest=pinDependencies(published);
    const baseline=readReceiveCursor(db,scope),program=published.program.program;
    const run={scope,run_id:crypto.randomUUID(),definition_id,definition_version,artifact_ref:published.result.artifact_ref,
     revision:'1',lifecycle:'disarmed',business_state:program.kind==='rule'?null:{state_id:program.initial_state,revision:'1'},
     arm_generation:'0',ingest_revision:'1',last_receive_cursor:baseline,dependency_digest:dependencyDigest,active_execution_id:null,active_operation_id:null,
     last_execution_id:null,pause_latches:[],dispatch_eligible:false};
    const saved={format:'leafseamer-run-creation/1',run,request_digest:requestDigest,authorization:proof};
    const inserted=db.prepare('INSERT INTO automation_run_creation VALUES(?,?,?,?,?,?,?,?,?,?,?)')
     .run(run.run_id,definition_id,definition_version,context.context_id,identity,requestDigest,baseline.ingest_epoch,Number(baseline.receive_sequence),dependencyDigest,canonical(saved),digest(saved));
    need(inserted.changes===1,'run_storage');
    need(same(proof,authorize(authentication,'createRun',definition_id,context.context_id))&&
     same(baseline,readReceiveCursor(db,scope)),'run_authorization_changed');
    return view(run);
   });
  },
  get(authentication,id){
   checked('LifecycleId',id);
   return writer(()=>{
    const {run}=original(db.prepare(select+' WHERE run_id=?').raw().get(id));
    authorize(authentication,'getRun',run.definition_id,null);
    return view(currentRun(run));
   });
  },
  dependencies(authentication,id){
   return readWithDependencies(authentication,id,(_run,dependencies)=>dependencies);
  },
  async deviceTargets(authentication,id){
   const plan=readWithDependencies(authentication,id,deviceTargetPlan);
   if(plan.result.evidence.length===0)return plan.result;
   const verify=await prepareRunActionObservations(plan.result.evidence);
   return readWithDependencies(authentication,id,(run,dependencies,authorization)=>{
    need(same(plan.authorization,authorization),'run_authorization_changed');
    const current=deviceTargetPlan(run,dependencies,authorization);
    need(same(current.result,plan.result),'run_target_changed');
    return current.result;
   },verify);
  },
  async createApprovalProposal(authentication,runId,request){
   need(Number(service.schema_revision)>=6,'run_proposal_migration_required');
   checked('LifecycleId',runId);
   checked('Value',{kind:'literal',value:request});
   need(fields(request,'artifact_ref,context,controller_constraints,expected_run_revision,service_principal,source,validity')&&
    fields(request.context,'context_id,idempotency_key')&&fields(request.service_principal,'issuer_epoch,issuer_id,principal_id')&&
    fields(request.source,'approval_ref,binding_id')&&fields(request.validity,'not_after,not_before')&&Buffer.byteLength(canonical(request))<=1048576,'run_proposal_request');
   const {context,artifact_ref,expected_run_revision,service_principal,source,validity,controller_constraints}=structuredClone(request);
   for(const id of [...Object.values(context),...Object.values(service_principal),source.binding_id])checked('LifecycleId',id);
   checked('LifecycleRevision',expected_run_revision);checked('ContentRef',source.approval_ref);
   const before=utc(validity.not_before),after=utc(validity.not_after);need(before<after,'run_proposal_time');
   const semantic={artifact_ref,expected_run_revision,service_principal,source,validity,controller_constraints};
   const requestDigest=digest({operation_id:proposalOperation,run_id:runId,request:semantic});
   const prepared=writer(()=>{
    const run=currentRun(proposalRun(runId)),proof=authorize(authentication,proposalOperation,run.definition_id,context.context_id);
    const identity=requestIdentity(proof,context.idempotency_key,proposalOperation),prior=priorProposal(identity,run,requestDigest,authentication);
    if(prior)return {prior};
    need(run.revision===expected_run_revision&&same(run.artifact_ref,artifact_ref),'run_proposal_revision');
    const publication=publications.versionForRunInTransaction(run.definition_id,run.definition_version);
    need(publicationBasisCurrent(run,publication),'run_publication_binding_unapproved');
    const dependencies=readDependencies(run),plan=deviceTargetPlan(run,dependencies,proof).result;
    const recovery=recoveryApprovalBasis(proposalRun(runId));
    const priorApprovalProposal=recovery?approvals.originalAuthorityInTransaction(recovery.prior_approval_ref,run).saved.proposal:null;
    return {run,proof,identity,dependencies,plan,publication,recovery,priorApprovalProposal};
   });
   if(prepared.prior)return prepared.prior;
   const targets=await constrainRunTargets(controller_constraints,prepared.plan,prepared.publication);
   if(prepared.priorApprovalProposal){
    need(same(targets,prepared.priorApprovalProposal.controller_targets)&&same(service_principal,prepared.priorApprovalProposal.service_principal),
     'run_recovery_approval_scope');
   }
   const remote=await Promise.allSettled([prepareRunActionObservations(prepared.plan.evidence),prepareServiceApproval(source.binding_id,service_principal,source.approval_ref)]);
   const failed=remote.find(result=>result.status==='rejected');if(failed)throw failed.reason;
   const verifyTargets=remote[0].value,serviceApproval=remote[1].value,approved=serviceApproval.original;
   need(same(approved.approval.automation_origin.seamer_scope,scope)&&same(approved.approval_ref,source.approval_ref)&&
    targets.every(target=>same(target.controller_scope,approved.issuer_scope)&&target.device_actions.every(action=>approved.approval.device_ids.includes(action.device_id))),
    'run_proposal_service_scope');
   return writer(()=>{
    const run=currentRun(proposalRun(runId)),proof=authorize(authentication,proposalOperation,run.definition_id,context.context_id);
    need(same(proof,prepared.proof)&&same(run,prepared.run)&&same(recoveryApprovalBasis(proposalRun(runId)),prepared.recovery),'run_proposal_changed');
    const prior=priorProposal(prepared.identity,run,requestDigest,authentication);
    if(prior)return prior;
    need(publicationBasisCurrent(run,prepared.publication),'run_publication_binding_unapproved');
    const dependencies=readDependencies(run),plan=deviceTargetPlan(run,dependencies,proof).result;
    need(same(plan,prepared.plan),'run_proposal_changed');
    checkWindow(before,after,approved);authority.assertRunCapacityInTransaction(proof);
    need(db.prepare('SELECT count(*) FROM automation_run_approval_proposal').pluck().get()<16384,'run_proposal_capacity');
    const id=crypto.randomUUID(),audit={context_id:context.context_id,request_digest:requestDigest,evidence:proof.authentication_evidence,
     source_subject_ref:approved.subject_ref,source_approval_ref:approved.approval_ref,source_binding_id:source.binding_id,
     ...(prepared.recovery&&{recovery:prepared.recovery})};
    const auditDigest=digest(audit),body={scope,proposal_id:id,revision:'1',run_id:runId,definition_id:run.definition_id,definition_version:run.definition_version,
     artifact_ref,program_digest:dependencies.body.program_digest,bindings_digest:dependencies.body.bindings_digest,
     dependency_footprint_ref:{kind:'program_dependencies',scope,object_id:run.definition_id,revision:run.definition_version,digest:dependencies.digest},
     service_principal,source_service_approval_ref:{kind:'service_approval',scope:approved.issuer_scope,object_id:approved.approval_ref.id,revision:approved.approval_ref.revision,digest:approved.approval_ref.digest},
     controller_targets:targets,validity,proposed_by:{issuer_id:proof.actor.issuer_id,issuer_epoch:proof.actor.issuer_epoch,principal_id:proof.actor.principal_id},
     created_authentication_ref:{kind:'authentication',scope,object_id:id,revision:'1',digest:auditDigest}};
    const text=canonical(body),hash=digest(body);
    need(Buffer.byteLength(text)<=1048576,'run_proposal_capacity');
    need(db.prepare(`INSERT INTO automation_run_approval_proposal
     (proposal_id,run_id,context_id,identity_digest,request_digest,body_jcs,body_digest,audit_jcs,audit_digest) VALUES(?,?,?,?,?,?,?,?,?)`)
     .run(id,runId,context.context_id,prepared.identity,requestDigest,text,hash,canonical(audit),auditDigest).changes===1,'run_proposal_storage');
    need(same(proof,authorize(authentication,proposalOperation,run.definition_id,context.context_id)),'run_authorization_changed');
    checkWindow(before,after,approved);verifyTargets();serviceApproval.assertCurrentInTransaction();verifyTargets();
    return {proposal:body,proposal_ref:{kind:'run_approval_proposal',scope,object_id:id,revision:'1',digest:hash}};
   });
  },
  getApprovalProposal(authentication,id){
   need(Number(service.schema_revision)>=6,'run_proposal_migration_required');
   checked('LifecycleId',id);
   return writer(()=>{
    const row=db.prepare(selectProposal+' WHERE proposal_id=?').raw().get(id);
    need(row,'run_proposal_missing');const run=proposalRun(row[1]),access=authorize(authentication,'getRun',run.definition_id,null);
    const saved=readProposal(row,run);
    need(same(access,authorize(authentication,'getRun',run.definition_id,null)),'run_authorization_changed');
    return saved;
   });
  },
  readApprovalProposalForPeer(bindingId,socket,request){
   need(Number(service.schema_revision)>=6,'run_proposal_migration_required');
   return writer(()=>{
    const targetScope=peerScope(authorizeControllerPeerInTransaction(bindingId,socket).peer);
    const row=db.prepare(selectProposal+' WHERE proposal_id=?').raw().get(request.proposal_id);
    need(row&&row[5]===request.proposal_digest,'run_proposal_missing');
    const run=proposalRun(row[1]),saved=readProposal(row,run);proposalTarget(saved.proposal,targetScope);
    const recovery=JSON.parse(row[6]).recovery??null;
    need(!recovery||same(recovery,recoveryApprovalBasis(run)),'run_recovery_approval_changed');
    need(publicationBasisCurrent(run),'run_publication_binding_unapproved');
    need(authority.runApprovalTimeInTransaction().upper<proposalWindowEnd(saved.proposal),'run_proposal_expired');
    return saved;
   });
  },
  lookupApprovalProposal(authentication,{run_id,context_id,idempotency_key}){
   need(Number(service.schema_revision)>=6,'run_proposal_migration_required');
   for(const id of [run_id,context_id,idempotency_key])checked('LifecycleId',id);
   return writer(()=>{
    const input={authentication,context_id,operation_id:proposalOperation},proof=authority.lookupInTransaction(input);
    need(proof&&same(proof.scope,scope)&&proof.request_epoch===service.request_epoch,'run_authorization');
    const run=proposalRun(run_id),access=authorize(authentication,'getRun',run.definition_id,null);
    const row=db.prepare(selectProposal+' WHERE identity_digest=?').raw().get(requestIdentity(proof,idempotency_key,proposalOperation));
    const result=row?readProposal(row,run):null;
    need(same(proof,authority.lookupInTransaction(input))&&same(access,authorize(authentication,'getRun',run.definition_id,null)),'run_authorization_changed');
    return {request_digest:row?.[3]??null,result};
   });
  },
  lookup(authentication,request){
   need(fields(request,'context_id,idempotency_key,operation_id')&&request.operation_id==='createRun','run_request');
   const {context_id,idempotency_key,operation_id}=request;
   for(const value of [context_id,idempotency_key])checked('LifecycleId',value);
   return writer(()=>{
    const input={authentication,context_id,operation_id},proof=authority.lookupInTransaction(input);
    need(proof&&same(proof.scope,scope)&&proof.request_epoch===service.request_epoch,'run_authorization');
    const row=db.prepare(select+' WHERE identity_digest=?').raw().get(requestIdentity(proof,idempotency_key));
    const {run,requestDigest}=original(row),access=authorize(authentication,'getRun',run.definition_id,null);
    need(same(proof,authority.lookupInTransaction(input))&&same(access,authorize(authentication,'getRun',run.definition_id,null)),'run_authorization_changed');
    return {operation_id,request_digest:requestDigest,result:{kind:'RunRecord',value:view(run)}};
   });
  }
 });
}
