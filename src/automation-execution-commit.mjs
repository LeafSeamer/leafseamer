import {canonical,digest,same} from './runtime/auth-clock-value.mjs';
import {need} from './automation-publication.mjs';

export function nextExecutionRevision(revision){
 const next=BigInt(revision)+1n;
 need(next<(1n<<64n),'execution_revision_exhausted');
 return String(next);
}

export function mayContinueAfterFailure(plan,state){
 return !plan.required&&plan.failure_policy==='continue_if_independent'&&state.outcome==='failed'&&
  state.resource_status==='clear'&&state.automation_continuation==='eligible';
}

export function completionEvidence(steps){
 const evidence=[];
 for(const {plan,elapsed,submission} of steps){
  if(plan.payload.kind==='delay'){
   if(!elapsed)return null;
   evidence.push({step_id:plan.step_id,outcome:'succeeded',evidence_digest:elapsed.ref.digest});
  }else{
   const terminal=submission?.terminal;
   if(!terminal||!(terminal.state.outcome==='succeeded'||mayContinueAfterFailure(plan,terminal.state)))return null;
   evidence.push({step_id:plan.step_id,outcome:terminal.state.outcome,evidence_digest:digest(terminal)});
  }
 }
 return evidence;
}

export function automationExecutionCommit(db){
 const select=`SELECT c.execution_id,c.run_id,c.run_revision,c.slot_revision,c.body_jcs,c.body_digest,
  c.audit_jcs,c.notification_jcs,e.body_digest FROM automation_execution_commit c JOIN automation_execution e USING(execution_id)`;
 const byId=db.prepare(select+' WHERE c.execution_id=?').raw();
 const byRun=db.prepare(select+' WHERE c.run_id=? ORDER BY length(c.run_revision) DESC,c.run_revision DESC LIMIT 1').raw();
 const insert=db.prepare('INSERT INTO automation_execution_commit VALUES(?,?,?,?,?,?,?,?)');
 const audit=(body,hash)=>({kind:'execution_completed',execution_ref:body.execution_ref,commit_digest:hash});
 const notification=(body,hash)=>({kind:'transition_committed',event_id:`${body.execution_ref.object_id}.completed`,
  run_id:body.run_id,run_revision:body.run_revision_after,commit_digest:hash});
 function decode(row){
  if(!row)return null;
  const [id,runId,revision,slot,text,hash,auditText,noticeText,executionHash]=row;
  const body=JSON.parse(text);
  need(canonical(body)===text&&digest(body)===hash&&body.format==='leafseamer.execution-commit/1'&&
   body.execution_ref.object_id===id&&body.execution_ref.digest===executionHash&&body.run_id===runId&&
   body.run_revision_after===revision&&revision===nextExecutionRevision(body.run_revision_before)&&body.slot_revision===slot&&
   canonical(audit(body,hash))===auditText&&canonical(notification(body,hash))===noticeText,'execution_commit_integrity');
  return {body,digest:hash,notification:notification(body,hash)};
 }
 function write(body,charge){
  need(db.inTransaction,'execution_commit_transaction');
  const {execution_ref,run_id,run_revision_after,slot_revision}=body,hash=digest(body);
  const bytes=[canonical(body),canonical(audit(body,hash)),canonical(notification(body,hash))];
  charge(bytes.reduce((size,text)=>size+Buffer.byteLength(text),0));
  need(insert.run(execution_ref.object_id,run_id,run_revision_after,slot_revision,bytes[0],hash,bytes[1],bytes[2]).changes===1,'execution_commit_storage');
 }
 return Object.freeze({read:id=>decode(byId.get(id)),latest:runId=>decode(byRun.get(runId)),write});
}
