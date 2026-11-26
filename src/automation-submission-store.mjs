import {randomUUID} from 'node:crypto';
import {canonical,digest,same} from './runtime/auth-clock-value.mjs';
import * as commandProtocol from './runtime/peer-automation-command.mjs';
import {need} from './automation-publication.mjs';
const {automationCommandAdjudication,automationCommandIdentity,automationCommandRequest}=commandProtocol;

// Reserve terminal storage before any network call, including one uncertain observation.
const submissionFollowupBytes=2048+8192+256;
const submissionOriginals=4;
const terminalBudgetBytes=8192;

export function automationSubmissionStore(db,{withTerminals=false,recovery=false}={}){
 const prepare=sql=>db.prepare(recovery?sql.replaceAll('automation_execution_','automation_recovery_dispatch_'):sql);
 const select=prepare(`SELECT s.step_id,s.body_jcs,s.body_digest,a.body_jcs,a.body_digest,
  u.step_id IS NOT NULL,d.body_jcs,d.body_digest${withTerminals?',t.body_jcs,t.body_digest':''} FROM automation_execution_submission s
  LEFT JOIN automation_execution_submission_attempt a USING(execution_id,step_id)
  LEFT JOIN automation_execution_submission_uncertain u USING(execution_id,step_id)
  LEFT JOIN automation_execution_submission_adjudication d USING(execution_id,step_id)
  ${withTerminals?'LEFT JOIN automation_execution_command_terminal t USING(execution_id,step_id)':''} WHERE s.execution_id=?`).raw();
 const usage=prepare(`SELECT count(*) AS count,coalesce(sum(length(CAST(body_jcs AS BLOB))),0) AS bytes,
  coalesce(sum(json_extract(body_jcs,'$.format')='leafseamer.action-submission/2'),0)
  FROM automation_execution_submission WHERE execution_id=?`).raw();
 const insert=prepare('INSERT INTO automation_execution_submission VALUES(?,?,?,?,?)');
 const insertAttempt=prepare('INSERT INTO automation_execution_submission_attempt VALUES(?,?,?,?,?,?,?,?,?)');
 const insertUncertain=prepare('INSERT INTO automation_execution_submission_uncertain VALUES(?,?) ON CONFLICT DO NOTHING');
 const readUncertain=prepare('SELECT 1 FROM automation_execution_submission_uncertain WHERE execution_id=? AND step_id=?').pluck();
 const insertAdjudication=prepare('INSERT INTO automation_execution_submission_adjudication VALUES(?,?,?,?)');
 const pending=prepare(`SELECT a.execution_id,a.step_id FROM automation_execution_submission_attempt a
  LEFT JOIN automation_execution_submission_adjudication d USING(execution_id,step_id)
  WHERE d.execution_id IS NULL AND (a.execution_id,a.step_id)>(?,?) ORDER BY a.execution_id,a.step_id LIMIT 16`).raw();
 const putTerminal=withTerminals&&prepare('INSERT INTO automation_execution_command_terminal VALUES(?,?,?,?)');
 const pendingTerminal=withTerminals&&prepare(`SELECT d.execution_id,d.step_id FROM automation_execution_submission_adjudication d
  LEFT JOIN automation_execution_command_terminal t USING(execution_id,step_id)
  WHERE t.execution_id IS NULL AND json_extract(d.body_jcs,'$.decision')='accepted' AND (d.execution_id,d.step_id)>(?,?)
  ORDER BY d.execution_id,d.step_id LIMIT 16`).raw();
 const checkWrite=value=>need(db.inTransaction&&value.changes===1,'execution_submission_storage');

 function decode(text,hash){
  const body=JSON.parse(text);
  need(canonical(body)===text&&digest(body)===hash,'execution_submission_integrity');
  return body;
 }
 function actionOriginal(value,step){
  const {binding}=step.plan.payload;
  const grant=value.execution.grant_basis.find(original=>same(original.grant_ref,binding.grant_ref));
  need(grant&&step.prepared_parameters&&same(grant.lookup.controller_scope,binding.target.controller_scope),'execution_submission_binding');
  return {action:{preparation:{lookup:grant.lookup,receipt_ref:grant.receipt_ref,grant_ref:grant.grant_ref},
   execution_ref:value.execution_ref,step_id:step.plan.step_id,parameters_ref:step.prepared_parameters.ref},
   target:binding.target.device_action,pairingRef:grant.pairing_ref};
 }
 function checkResult(body,reply){
  automationCommandAdjudication(reply,body.request);
  need(same(reply.identity,body.identity),'execution_submission_identity');
  return reply;
 }
 function read(value,checkClock){
  const records=new Map(select.all(value.execution.execution_id).map(row=>[row[0],row]));
  return value.steps.map(step=>{
   const row=records.get(step.plan.step_id);
   if(!row)return {...step,submission:null};
   const [,text,hash,attemptText,attemptHash,uncertain,resultText,resultHash,terminalText,terminalHash]=row;
   const body=decode(text,hash),{request}=body,{action,target,pairingRef}=actionOriginal(value,step);
   automationCommandRequest(request,action.preparation.lookup.controller_scope);
   need((body.format==='leafseamer.action-submission/1'||withTerminals&&body.format==='leafseamer.action-submission/2')&&same(request.action,action)&&same(request.target,target)&&
    same(body.pairing_ref,pairingRef)&&same(body.identity,automationCommandIdentity(request,body.identity.key)),
    'execution_submission_integrity');
   checkClock(step,body.clock);
   const attempt=attemptText===null?null:decode(attemptText,attemptHash);
   if(attempt){
    need(attempt.format==='leafseamer.action-submission-attempt/1'&&attempt.submission_digest===hash&&
     attempt.service_boot_id===value.execution.service_boot_id,'execution_submission_integrity');
    checkClock(step,attempt.clock,body.clock);
   }
   const adjudication=resultText===null?null:checkResult(body,decode(resultText,resultHash));
   const state=adjudication?.decision??(uncertain?'indeterminate':attempt?'attempt_recorded':'planned');
   const terminal=terminalText==null?null:commandProtocol.automationCommandTerminal(decode(terminalText,terminalHash),adjudication);
   return {...step,submission:{body,digest:hash,attempt,adjudication,state,...(withTerminals&&{terminal})}};
  });
 }
 function plan(value,step,grant,clock,reserve){
  need(db.inTransaction&&!step.submission,'execution_submission_transition');
  const {action,target,pairingRef}=actionOriginal(value,step);
  need(same(grant.request,action.preparation)&&same(grant.pairing_ref,pairingRef),'execution_submission_binding');
  const request=automationCommandRequest({action,target,idempotency_key:randomUUID()},grant.request.lookup.controller_scope);
  const body={format:withTerminals?'leafseamer.action-submission/2':'leafseamer.action-submission/1',
   request,identity:automationCommandIdentity(request,grant.grant.principal),pairing_ref:pairingRef,clock};
  const text=canonical(body),hash=digest(body);
  reserve(Buffer.byteLength(text)+submissionFollowupBytes+(withTerminals?terminalBudgetBytes:0),submissionOriginals+(withTerminals?1:0));
  checkWrite(insert.run(value.execution.execution_id,step.plan.step_id,request.idempotency_key,text,hash));
  return hash;
 }
 function attempt(value,step,clock){
  const {submission}=step,{execution}=value;
  need(db.inTransaction&&submission?.state==='planned'&&BigInt(clock.tick_ns)>=BigInt(submission.body.clock.tick_ns),
   'execution_submission_transition');
  const body={format:'leafseamer.action-submission-attempt/1',attempt_id:randomUUID(),submission_digest:submission.digest,
   service_boot_id:execution.service_boot_id,clock};
  checkWrite(insertAttempt.run(execution.execution_id,step.plan.step_id,submission.digest,body.attempt_id,body.service_boot_id,
   clock.runtime_domain_ref.clock_domain_id,clock.acquisition_ref.id,canonical(body),digest(body)));
  return body;
 }
 function adopt(submission,reply){
  need(db.inTransaction&&submission.attempt,'execution_submission_transition');
  const {execution_ref,step_id}=submission.body.request.action;
  if(reply===null){
   if(!submission.adjudication){
    insertUncertain.run(execution_ref.object_id,step_id);
    need(readUncertain.get(execution_ref.object_id,step_id)===1,'execution_submission_storage');
   }
   return submission.adjudication;
  }
  const result=checkResult(submission.body,reply);
  if(submission.adjudication)need(same(submission.adjudication,result),'execution_submission_conflict');
  else checkWrite(insertAdjudication.run(execution_ref.object_id,step_id,canonical(result),digest(result)));
  return result;
 }
 function reservedUsage(id){
  const [count,bytes,withTerminal]=usage.get(id),rows=count*submissionOriginals+withTerminal;
  return {bytes:bytes+count*submissionFollowupBytes+withTerminal*terminalBudgetBytes,rows,work:rows};
 }
 function adoptTerminal(submission,reply){
  need(db.inTransaction&&submission.adjudication?.decision==='accepted'&&same(submission.adjudication,reply.adjudication),
   'execution_command_terminal_adjudication');
  if(reply.terminal===null)return null;
  const original=commandProtocol.automationCommandTerminal(reply.terminal,submission.adjudication);
  if(submission.terminal)need(same(submission.terminal,original),'execution_command_terminal_conflict');
  else {
   const {execution_ref,step_id}=submission.body.request.action;
   checkWrite(putTerminal.run(execution_ref.object_id,step_id,canonical(original),digest(original)));
  }
  return original;
 }
 return Object.freeze({read,plan,attempt,adopt,reservedUsage,pendingAfter(cursor){return pending.all(...cursor);},
  ...(withTerminals&&{adoptTerminal,pendingTerminalAfter(cursor){return pendingTerminal.all(...cursor);}})});
}
