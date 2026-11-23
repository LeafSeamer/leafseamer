import {randomUUID} from 'node:crypto';
import {canonical,digest,same} from './runtime/auth-clock-value.mjs';

const limit=1048576;
const need=(ok,code)=>{if(!ok)throw Object.assign(new Error(code),{code});};
const epochMatches=(a,b)=>same(a.scope,b.scope)&&a.ingest_epoch===b.ingest_epoch;
const successor=revision=>String(BigInt(revision)+1n);

function suppression(run,cursor,observation,liveEvidence){
 if(!epochMatches(run.last_receive_cursor,cursor))return ['protected','ingest_epoch_changed'];
 if(BigInt(cursor.receive_sequence)<=BigInt(run.last_receive_cursor.receive_sequence))return ['suppressed_obsolete','receive_cursor_already_consumed'];
 if(run.active_execution_id!==null)return ['suppressed_busy','execution_active'];
 if(run.lifecycle!=='armed')return ['suppressed_inactive','run_'+run.lifecycle];
 if(!run.run_arm_completion_ref)return ['protected','arm_baseline_missing'];
 if(observation.delivery_mode!=='live')return ['protected','reconciled_delivery'];
 if(observation.age_basis==='unproven')return ['protected','event_age_unproven'];
 if(liveEvidence?.state!=='verified')return ['protected',liveEvidence?.reason??'source_predecessor_unavailable'];
 need(run.dispatch_eligible===false,'trigger_decision_execution_unavailable');
 return null;
}
const evaluatedDispositions=new Set(['accepted','no_match','evaluation_unknown','ambiguous']);
const pausesRun=disposition=>disposition==='evaluation_unknown'||disposition==='ambiguous';
const summary=(count,evaluated)=>evaluated?'eligible_decision_recorded':count?'suppressed':'observed_only';

export function automationTriggerDecisionStore(db,executions=null){
 const writeDecision=db.prepare('INSERT INTO automation_trigger_decision VALUES(?,?,?,?,?,?)');
 const writeSet=db.prepare('INSERT INTO automation_trigger_decision_set VALUES(?,?,?,?,?)');
 const pauseRun=db.prepare('UPDATE automation_run_approval_head SET run_revision=? WHERE run_id=? AND run_revision=?');
 const readSet=db.prepare('SELECT match_digest,body_jcs,body_digest FROM automation_trigger_decision_set WHERE ingest_epoch=? AND receive_sequence=?').raw();
 const base=`SELECT d.run_id,d.decision_id,d.body_jcs,d.body_digest,d.ingest_epoch,d.receive_sequence,s.match_digest
  FROM automation_trigger_decision d JOIN automation_trigger_decision_set s USING(ingest_epoch,receive_sequence)`;
 const readDecisions=db.prepare(base+' WHERE d.ingest_epoch=? AND d.receive_sequence=? ORDER BY d.run_id').raw();
 const head=db.prepare(base+' WHERE d.run_id=? AND d.ingest_epoch=? AND d.receive_sequence<=? ORDER BY d.receive_sequence DESC LIMIT 1').raw();
 function decode(text,hash){
  const value=JSON.parse(text);
  need(canonical(value)===text&&digest(value)===hash,'trigger_decision_integrity');return value;
 }
 function readDecision([runId,id,text,hash,epoch,sequence,matchDigest]){
  const value=decode(text,hash);
  const pauses=pausesRun(value.disposition),accepted=value.disposition==='accepted';
  need(value.format==='leafseamer.trigger-decision/1'&&value.run_id===runId&&value.decision_id===id&&value.match_digest===matchDigest&&
   value.receive_cursor.ingest_epoch===epoch&&value.receive_cursor.receive_sequence===String(sequence)&&
   value.ingest_revision_after===successor(value.ingest_revision_before)&&(accepted?executions&&typeof value.execution_id==='string':value.execution_id===null)&&
   value.run_revision_after===(pauses||accepted?successor(value.run_revision_before):value.run_revision_before)&&
   (!pauses||value.trigger_pause_revision===value.run_revision_after&&value.pause_latches?.includes(id)),'trigger_decision_integrity');
  return value;
 }
 function record({match,digest:matchDigest},observation,resolveRun,liveEvidence,evaluate){
  need(db.inTransaction,'trigger_decision_transaction');
  const receive_cursor=match.receive_cursor,epoch=receive_cursor.ingest_epoch,sequence=receive_cursor.receive_sequence,index=[];
  let bytes=0,evaluated=false;const admitted=[];
  for(const target of match.targets){
   const run=resolveRun(target);
   need(run.run_id===target.run_id&&same(run.scope,receive_cursor.scope)&&run.definition_id===target.definition_id&&
    run.definition_version===target.definition_version&&run.dependency_digest===target.dependency_digest&&same(run.artifact_ref,target.artifact_ref),
    'trigger_decision_target');
   const suppressed=suppression(run,receive_cursor,observation,liveEvidence);
   const {disposition,reason,evaluation,plan}=suppressed?{disposition:suppressed[0],reason:suppressed[1]}:evaluate(run,target);
   const decision_id=randomUUID();
   const pauses=pausesRun(disposition),accepted=disposition==='accepted';
   need(accepted?executions&&plan:plan===undefined,'execution_plan_acceptance');
   const pause_latches=pauses?[...run.pause_latches,decision_id]:run.pause_latches;
   const revisionAfter=pauses||accepted?successor(run.revision):run.revision;
   const trigger_pause_revision=pauses?revisionAfter:run.trigger_pause_revision;
   evaluated ||= evaluatedDispositions.has(disposition);
   const before=run.last_receive_cursor,after=epochMatches(before,receive_cursor)&&BigInt(sequence)>BigInt(before.receive_sequence)?receive_cursor:before;
   const decision={format:'leafseamer.trigger-decision/1',decision_id,match_digest:matchDigest,run_id:run.run_id,
    artifact_ref:run.artifact_ref,receive_cursor,receive_cursor_before:before,receive_cursor_after:after,arm_generation:run.arm_generation,
    arm_completion_ref:run.run_arm_completion_ref??null,run_revision_before:run.revision,run_revision_after:revisionAfter,
    ingest_revision_before:run.ingest_revision,ingest_revision_after:successor(run.ingest_revision),
    age_evidence_ref:liveEvidence?.age_evidence_ref??observation.disposition?.age_evidence_ref??null,disposition,reason,execution_id:accepted?randomUUID():null,
    ...(pause_latches.length&&{pause_latches}),...(trigger_pause_revision&&{trigger_pause_revision}),...(evaluation&&{evaluation})};
   const text=canonical(decision),hash=digest(decision);
   bytes+=Buffer.byteLength(text);
   need(bytes<=limit&&Buffer.byteLength(text)<=8192,'trigger_decision_capacity');
   need(writeDecision.run(epoch,sequence,run.run_id,decision.decision_id,text,hash).changes===1,'trigger_decision_storage');
   if(accepted)admitted.push({...executions.record(decision,hash,plan),recheck:plan.assertCurrentInTransaction});
   if(pauses||accepted)need(pauseRun.run(revisionAfter,run.run_id,run.revision).changes===1,accepted?'execution_run_revision':'trigger_pause_revision');
   index.push({run_id:run.run_id,decision_id:decision.decision_id,digest:hash});
  }
  const set={format:'leafseamer.trigger-decision-set/1',receive_cursor,match_digest:matchDigest,first_observation_digest:digest(observation),
   ...(liveEvidence&&{live_evidence:liveEvidence}),decisions:index,decisions_digest:digest(index),
   trigger_disposition:summary(index.length,evaluated)};
  const text=canonical(set);
  need(bytes+Buffer.byteLength(text)<=limit,'trigger_decision_capacity');
  need(writeSet.run(epoch,sequence,matchDigest,text,digest(set)).changes===1,'trigger_decision_storage');
  for(const execution of admitted)execution.recheck();
  return admitted;
 }
 function read({match,digest:matchDigest},observationDigest){
  const cursor=match.receive_cursor;
  const result=readSet.get(cursor.ingest_epoch,cursor.receive_sequence);
  if(!result)return null;
  const set=decode(result[1],result[2]);
  const rows=readDecisions.all(cursor.ingest_epoch,cursor.receive_sequence),decisions=rows.map(readDecision);
  const index=rows.map(([id,decisionId,,hash])=>({run_id:id,decision_id:decisionId,digest:hash}));
  need(set.format==='leafseamer.trigger-decision-set/1'&&same(set.receive_cursor,cursor)&&set.match_digest===result[0]&&result[0]===matchDigest&&
   set.first_observation_digest===observationDigest&&
   same(index,set.decisions)&&digest(index)===set.decisions_digest&&same(index.map(value=>value.run_id),match.targets.map(value=>value.run_id))&&
   set.trigger_disposition===summary(index.length,decisions.some(item=>evaluatedDispositions.has(item.disposition)))&&decisions.every((item,index)=>
    same(item.receive_cursor,cursor)&&same(item.artifact_ref,match.targets[index].artifact_ref)),'trigger_decision_integrity');
  return {set,digest:result[2],decisions};
 }
 function latest(runId,cursor){
  need(db.inTransaction,'trigger_decision_transaction');
  const row=head.get(runId,cursor.ingest_epoch,cursor.receive_sequence);
  if(!row)return null;
  const decision=readDecision(row);
  need(epochMatches(cursor,decision.receive_cursor_after),'trigger_decision_progress');
  return {receive_cursor:decision.receive_cursor_after,revision:decision.ingest_revision_after,pause_latches:decision.pause_latches??[],
   trigger_pause_revision:decision.trigger_pause_revision??null};
 }
 return Object.freeze({record,read,latest});
}
