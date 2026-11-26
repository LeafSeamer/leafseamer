import crypto from 'node:crypto';
import {canonical,digest,same} from './runtime/auth-clock-value.mjs';
import {endorsementReference} from './runtime/peer-run-approval-endorsement.mjs';
import {runApprovalReference} from './runtime/peer-run-approval.mjs';
import {approvalProtectionNotice,approvalProtectionReceipt,runApprovalProtectionReply,endorsementRevocationNotice,endorsementRevocationReceipt} from './runtime/peer-run-approval-protection.mjs';
import {peerScope,proposalReference,proposalTarget,proposalWindowEnd} from './runtime/peer-run-approval-proposal.mjs';
import {checked,need} from './automation-publication.mjs';

const operation='approveRun';
const revokeOperation='revokeRunApproval';
const fields=(value,names)=>value&&Object.keys(value).sort().join(',')===names;
const write=result=>need(result.changes===1,'run_approval_storage');

export function runApprovalStore({db,scope,requestEpoch,writer,authority,authorize,runOriginal,publicationBasisCurrent,bindingRevocationForRun,getProposal,runForProposal,requestIdentity,prepareServiceApproval,prepareEndorsement,currentPairingInTransaction,authorizeControllerPeerInTransaction,canRevoke,canAdoptProtection,canCompleteProtection,sendApprovalProtection,readRevokedEndorsement,authorizeEndorsementSender,protectionTargets}){
 let pendingEndorsementRevocations=0;
 let bindingRevocationCursor='';
 const rejectFromPeer=!!authorizeEndorsementSender;
 const records=`SELECT a.approval_id,a.run_id,a.proposal_id,a.context_id,a.request_digest,a.body_jcs,a.body_digest,a.audit_jcs,a.audit_digest,
  o.operation_id,o.body_jcs,o.body_digest,s.status_revision,s.state,a.identity_digest FROM automation_run_approval a
  JOIN automation_run_approval_operation o USING(approval_id) JOIN automation_run_approval_state s USING(approval_id)`;
 const revocations=`SELECT operation_id,approval_id,context_id,identity_digest,request_digest,body_jcs,body_digest,audit_jcs,audit_digest FROM automation_run_approval_revocation`;
 const head=id=>db.prepare('SELECT approval_id,run_revision FROM automation_run_approval_head WHERE run_id=?').raw().get(id);
 const authRef=(id,hash)=>({kind:'authentication',scope,object_id:id,revision:'1',digest:hash});
 function decode(text,hash){const value=JSON.parse(text);need(canonical(value)===text&&digest(value)===hash,'run_approval_integrity');return value;}
 function read(row){
  need(row,'run_approval_missing');
  const [id,runId,proposalId,contextId,requestDigest,text,hash,auditText,auditHash,operationId,opText,opHash,statusRevision,state,identity]=row;
  const approval=decode(text,hash),audit=decode(auditText,auditHash),result=decode(opText,opHash);
  const object_ref={kind:'run_approval',scope,object_id:id,revision:'1',digest:hash};
  need(approval.approval_id===id&&approval.revision==='1'&&approval.run_id===runId&&approval.proposal_ref.object_id===proposalId&&same(approval.scope,scope)&&
   approval.approval_operation_id===operationId&&same(approval.created_authentication_ref,authRef(id,auditHash))&&
   audit.context_id===contextId&&audit.request_digest===requestDigest&&result.operation_id===operationId&&result.run_id===runId&&
   result.request_identity===identity&&result.request_digest===requestDigest&&same(result.approval_ref,object_ref)&&same(result.result_ref,object_ref)&&
   same(result.proposal_ref,approval.proposal_ref)&&result.kind==='approve'&&result.state==='applied','run_approval_integrity');
  return {approval,object_ref,result,status_revision:String(statusRevision),state};
 }
 function visible(authentication,run,read){
  const access=authorize(authentication,'getRun',run.definition_id,null),result=read();
  need(same(access,authorize(authentication,'getRun',run.definition_id,null)),'run_authorization_changed');return result;
 }
 function readRevocation(row,saved){
  need(row,'run_approval_missing');
  const [id,approvalId,contextId,identity,requestDigest,text,hash,auditText,auditHash]=row;
  const result=decode(text,hash),audit=decode(auditText,auditHash);
  need(result.operation_id===id&&saved.approval.approval_id===approvalId&&result.run_id===saved.approval.run_id&&same(result.scope,scope)&&
   same(result.approval_ref,saved.object_ref)&&same(result.proposal_ref,saved.approval.proposal_ref)&&result.kind==='revoke_approval'&&
   result.request_identity===identity&&result.request_digest===requestDigest&&audit.context_id===contextId&&audit.request_digest===requestDigest&&
   same(result.targets.map(target=>target.target_scope),saved.approval.controller_endorsement_refs.map(ref=>ref.scope)),'run_approval_integrity');
  return {result,reference:{kind:'run_approval_operation',scope,object_id:id,revision:'1',digest:hash}};
 }
 function prior(authentication,run,key,requestDigest){
  const row=db.prepare(records+' WHERE a.identity_digest=?').raw().get(key);
  if(!row)return null;
  return visible(authentication,run,()=>{need(row[1]===run.run_id&&row[4]===requestDigest,'run_approval_conflict');return read(row).result;});
 }
 function currentRun(run){
  const pointer=head(run.run_id);
  if(!pointer)return run;
  checked('LifecycleRevision',pointer[1]);
  need(BigInt(pointer[1])>BigInt(run.revision),'run_approval_integrity');
  return {...run,revision:pointer[1]};
 }
 function pairFor(target,purpose='automation.prepare.read'){
  const rows=db.prepare(`SELECT r.binding_id,r.revision,r.body_digest FROM peer_pairing_revision r JOIN peer_pairing_head h USING(binding_id,revision)
   WHERE r.peer_service_id=? AND r.state='activated' AND EXISTS(
    SELECT 1 FROM json_each(r.body_jcs,'$.transcript.parties') party JOIN json_each(party.value,'$.permissions') permission
    WHERE json_extract(party.value,'$.service_id')=? AND permission.value=?)`).raw().all(target.authority_service_id,scope.authority_service_id,purpose);
  const matched=rows.map(([id,revision,digest])=>currentPairingInTransaction({id,revision:String(revision),digest})).filter(record=>{
   const peer=record.transcript.parties.find(part=>part.service_id===record.peer_service_id);
   const local=record.transcript.parties.find(part=>part.service_id===scope.authority_service_id);
   return peer?.service_kind==='controller'&&same(peerScope(peer),target)&&local?.permissions.includes(purpose)&&
    (purpose!=='automation.protect'||peer.permissions.includes('automation.prepare.read'));
  });
  need(matched.length===1,'run_approval_pairing');return matched[0];
 }
 function protectionKey(operationId,target){return [operationId,target.authority_service_id,target.database_instance_id];}
 function protectionNotice(operationId,target,saved,operationRecord){
  const row=db.prepare(`SELECT body_jcs,body_digest FROM automation_run_approval_protection_outbox
   WHERE operation_id=? AND target_service_id=? AND target_database_instance_id=?`).raw().get(...protectionKey(operationId,target));
  need(row&&operationRecord.result.targets.some(entry=>same(entry.target_scope,target)&&entry.state==='pending'),'run_approval_missing');
  const notice=approvalProtectionNotice(decode(...row));
  need(same(notice.approval_ref,saved.object_ref)&&same(notice.revocation_operation_ref,operationRecord.reference)&&same(notice.target_scope,target)&&
   saved.approval.controller_endorsement_refs.some(ref=>same(ref,notice.endorsement_ref)),'run_approval_integrity');return notice;
 }
 function queuedProtection(operationId,target){
  const row=db.prepare(revocations+' WHERE operation_id=?').raw().get(operationId);need(row,'run_approval_missing');
  const saved=read(db.prepare(records+' WHERE a.approval_id=?').raw().get(row[1]));
  return protectionNotice(operationId,target,saved,readRevocation(row,saved));
 }
 function protectionReceipt(operationId,target,notice){
  const row=db.prepare(`SELECT receipt_id,body_jcs,body_digest FROM automation_run_approval_protection_receipt
   WHERE operation_id=? AND target_service_id=? AND target_database_instance_id=?`).raw().get(...protectionKey(operationId,target));
  if(!row)return null;
  const receipt=approvalProtectionReceipt(decode(row[1],row[2]),notice);
  need(receipt.receipt_id===row[0],'run_approval_integrity');return receipt;
 }
 function verifyCompletion(value,notice,rejection){
  const reply=runApprovalProtectionReply(value,notice,{completion:true}),saved=read(db.prepare(records+' WHERE a.approval_id=?').raw().get(notice.approval_ref.object_id));
  const run=runOriginal(saved.approval.run_id),proposal=approvalProposal(saved.approval,run).saved.proposal;
  const expected=[...new Set(proposalTarget(proposal,notice.target_scope).device_actions.map(action=>action.device_id))].sort();
  need(reply.protection_state==='applied'&&same(reply.rejection,rejection)&&same(notice.approval_ref,saved.object_ref)&&
   reply.run_id===run.run_id&&same(reply.device_ids,expected),'run_approval_protection_completion');return reply;
 }
 function protectionCompletion(operationId,target,notice,rejection){
  if(!canCompleteProtection)return null;
  const row=db.prepare(`SELECT receipt_id,body_jcs,body_digest FROM automation_run_approval_protection_completion
   WHERE operation_id=? AND target_service_id=? AND target_database_instance_id=?`).raw().get(...protectionKey(operationId,target));
  if(!row)return null;
  const reply=verifyCompletion(decode(row[1],row[2]),notice,rejection);
  need(reply.receipt_id===row[0],'run_approval_integrity');return reply;
 }
 function protectionObservation(operation,saved,target){
  const id=operation.result.operation_id,notice=protectionNotice(id,target,saved,operation);
  const rejection=protectionReceipt(id,target,notice),complete=protectionCompletion(id,target,notice,rejection),receipt=complete??rejection;
  const receipt_ref=receipt?{kind:complete?'run_approval_protection_receipt':'run_approval_rejection_receipt',
   scope:target,object_id:receipt.receipt_id,revision:'1',digest:digest(receipt)}:null;
  return {notice,complete,receipt_ref};
 }
 function revocationView(record,saved){
  const result=record.result;
  if(!canAdoptProtection||result.state==='applied')return result;
  const targets=result.targets.map(entry=>{
   const {complete,receipt_ref}=protectionObservation(record,saved,entry.target_scope);
   return {...entry,state:complete?'applied':entry.state,receipt_ref};
  });
  const applied=targets.every(entry=>entry.state==='applied');
  if(applied)need(saved.state==='revoked','run_approval_integrity');
  return {...result,state:applied?'applied':targets.some(entry=>entry.receipt_ref!==null)?'applying':result.state,targets};
 }
 function recoveryProtectionInTransaction(run,approvalRef,generation){
  need(db.inTransaction,'run_approval_transaction');
  const row=db.prepare(revocations+' WHERE approval_id=?').raw().get(approvalRef.object_id);
  if(!row)return null;
  const saved=read(db.prepare(records+' WHERE a.approval_id=?').raw().get(approvalRef.object_id));
  need(saved.approval.run_id===run.run_id&&same(saved.object_ref,approvalRef),'run_approval_integrity');
  const operation=readRevocation(row,saved);
  const targets=operation.result.targets.map(({target_scope})=>{
   const {notice,complete,receipt_ref}=protectionObservation(operation,saved,target_scope);
   need(notice.arm_generations.some(item=>item.run_id===run.run_id&&item.arm_generation===generation),'run_approval_protection_completion');
   return {target_scope,receipt_ref,state:complete?'applied':receipt_ref?'denied':'pending',
    deny_through_generation:complete?.deny_through_generation??null,device_ids:complete?.device_ids??null};
  });
  return {operation_ref:operation.reference,targets};
 }
 function adoptBindingRevocations(){
  const candidates=writer(()=>db.prepare(`SELECT h.approval_id,h.run_id FROM automation_run_approval_head h
   JOIN automation_run_approval_state s USING(approval_id) JOIN automation_run_creation c USING(run_id)
   WHERE h.approval_id>? AND s.state IN ('active','expired')
    AND NOT EXISTS(SELECT 1 FROM automation_run_approval_revocation r WHERE r.approval_id=h.approval_id)
    AND EXISTS(SELECT 1 FROM definition_publication_binding_pin pin JOIN revoked_publication_binding b
     USING(definition_id,kind,approval_id,approval_digest)
     WHERE pin.definition_id=c.definition_id AND pin.definition_version=c.definition_version AND pin.approval_revision='1')
   ORDER BY h.approval_id LIMIT 4`).raw().all(bindingRevocationCursor));
  bindingRevocationCursor=candidates.length===4?candidates.at(-1)[0]:'';
  let failure;
  for(const [approvalId,runId] of candidates){
   try{writer(()=>{
    const saved=read(db.prepare(records+' WHERE a.approval_id=?').raw().get(approvalId));
    if(!['active','expired'].includes(saved.state)||head(runId)?.[0]!==approvalId||
     db.prepare('SELECT 1 FROM automation_run_approval_revocation WHERE approval_id=?').pluck().get(approvalId))return;
    const run=currentRun(runOriginal(runId)),binding_revocation=bindingRevocationForRun(run);
    need(binding_revocation,'run_binding_revocation_integrity');
    approvalProposal(saved.approval,run);
    const requestDigest=digest({scope,operation_id:'applyBindingRevocation',approval_ref:saved.object_ref,binding_revocation});
    commitRevocation(saved,run,{key:requestDigest,requestDigest,contextId:null,audit:{binding_revocation}});
   });}catch(error){failure??=error;}
  }
  if(failure)throw failure;
 }
 function scanApprovalProtection({after,limit}){
  need(Number.isInteger(limit)&&limit>=1&&limit<=64&&(after===null||Array.isArray(after)&&after.length===3),'run_approval_scan');
  if(after!==null)for(const id of after)checked('LifecycleId',id);
  return writer(()=>{
   const table=canCompleteProtection?'automation_run_approval_protection_completion':'automation_run_approval_protection_receipt';
   const rows=db.prepare(`SELECT o.operation_id,o.target_service_id,o.target_database_instance_id FROM automation_run_approval_protection_outbox o
    LEFT JOIN ${table} r USING(operation_id,target_service_id,target_database_instance_id)
    WHERE r.receipt_id IS NULL AND (o.operation_id,o.target_service_id,o.target_database_instance_id)>(?,?,?)
    ORDER BY o.operation_id,o.target_service_id,o.target_database_instance_id LIMIT ?`).raw().all(...(after??['','','']),limit+1);
   const items=rows.slice(0,limit).map(([operation_id,authority_service_id,database_instance_id])=>({operation_id,target_scope:{authority_service_id,database_instance_id}}));
   return {items,next:rows.length>limit?rows[limit-1]:null};
  });
 }
 async function deliverApprovalProtection(operationId,target){
  const prepared=writer(()=>{
   const notice=queuedProtection(operationId,target),prior=protectionReceipt(operationId,target,notice);
   const complete=protectionCompletion(operationId,target,notice,prior);
   if(complete||prior&&!canCompleteProtection)return {final:complete??prior};
   return {notice,record:pairFor(target,'automation.protect')};
  });
  if(prepared.final)return {state:'acknowledged',receipt:prepared.final};
  const {notice,record}=prepared;
  const completion=!!canCompleteProtection;
  const reply=runApprovalProtectionReply(await sendApprovalProtection(record.binding_id,notice,{completion}),notice,{completion});
  const complete=reply.protection_state==='applied',receipt=complete?reply.rejection:reply;
  return writer(()=>{
   const current=queuedProtection(operationId,target);
   need(same(notice,current),'run_approval_changed');
   const prior=protectionReceipt(operationId,target,current);
   if(prior)need(same(prior,receipt),'run_approval_protection_conflict');
   const previous=protectionCompletion(operationId,target,current,prior);
   if(previous){
    need(!complete||same(previous,reply),'run_approval_protection_conflict');
    return {state:'acknowledged',receipt:previous};
   }
   const pair=currentPairingInTransaction({id:record.binding_id,revision:record.revision,digest:record.digest});
   need(same(pair,record),'run_approval_pairing');
   if(!prior)write(db.prepare('INSERT INTO automation_run_approval_protection_receipt VALUES(?,?,?,?,?,?)')
     .run(...protectionKey(operationId,target),receipt.receipt_id,canonical(receipt),digest(receipt)));
   if(!complete)return {state:prior?'acknowledged':'delivered',receipt};
   verifyCompletion(reply,current,receipt);
   write(db.prepare('INSERT INTO automation_run_approval_protection_completion VALUES(?,?,?,?,?,?)')
    .run(...protectionKey(operationId,target),reply.receipt_id,canonical(reply),digest(reply)));
   const remaining=db.prepare(`SELECT 1 FROM automation_run_approval_protection_outbox o LEFT JOIN automation_run_approval_protection_completion c
    USING(operation_id,target_service_id,target_database_instance_id) WHERE o.operation_id=? AND c.receipt_id IS NULL LIMIT 1`).pluck().get(operationId);
   if(!remaining)write(db.prepare("UPDATE automation_run_approval_state SET status_revision=3,state='revoked' WHERE approval_id=? AND state IN ('revoking','expired')")
    .run(notice.approval_ref.object_id));
   return {state:'delivered',receipt:protectionCompletion(operationId,target,current,receipt)};
  });
 }
 function eligibleRun(run,expected,priorRef){
  need(run.revision===expected,'run_approval_revision');
  need(!head(run.run_id)&&priorRef===null&&['disarmed','paused'].includes(run.lifecycle)&&run.arm_generation==='0'&&
   run.active_execution_id===null&&run.active_operation_id===null&&run.dispatch_eligible===false,'run_approval_transition');
 }
 function checkWindow(proposal,source){
  const {lower,upper}=authority.runApprovalTimeInTransaction(),before=BigInt(Date.parse(proposal.validity.not_before))*1000000n;
  const end=proposalWindowEnd(proposal);
  need(lower>=before&&upper<end&&before>=BigInt(source.approval.not_before_ns)&&end<=BigInt(source.approval.expires_ns),'run_approval_time');
 }
 async function observeAuthority(prepared,{includeControls=false}={}){
  const proposal=prepared.saved.proposal,sourceRef=proposal.source_service_approval_ref;
  const sourceRequest={id:sourceRef.object_id,revision:sourceRef.revision,digest:sourceRef.digest};
  async function readTargets(){
   const results=[];
   for(let start=0;start<prepared.targets.length;start+=4){
    const group=await Promise.all(prepared.targets.slice(start,start+4).map(({bindingId,ref})=>prepareEndorsement(bindingId,ref,prepared.saved,{includeControls})));
    results.push(...group);
   }
   return results;
  }
  const [source,endorsements]=await Promise.all([prepareServiceApproval(prepared.sourceBindingId,proposal.service_principal,sourceRequest),readTargets()]);
  need(same(source.original.issuer_scope,sourceRef.scope)&&same(source.original.approval_ref,sourceRequest)&&
   same(source.original.approval.automation_origin.seamer_scope,scope),'run_approval_source');
  return {
   controls:includeControls?endorsements.map(({original,devices})=>({controller_scope:original.object_ref.scope,endorsement_ref:original.object_ref,devices})):null,
   basis:includeControls?{service_approval_ref:sourceRef,source_pairing_ref:source.pairing_ref,
    endorsements:endorsements.map(({original,pairing_ref})=>({endorsement_ref:original.object_ref,pairing_ref,
     target_policy_ref:original.endorsement?.authorization_basis_refs[0]??null}))}:null,
   assertCurrentInTransaction(){
    checkWindow(proposal,source.original);
    source.assertCurrentInTransaction();
    for(const evidence of endorsements)evidence.assertCurrentInTransaction();
   }
  };
 }
 function approvalProposal(approval,run){
  const proposed=getProposal(approval.proposal_ref,run),proposal=proposed.saved.proposal;
  need(same(proposal.controller_targets.map(target=>target.controller_scope),approval.controller_endorsement_refs.map(ref=>ref.scope))&&
   same(proposal.source_service_approval_ref,approval.source_service_approval_ref)&&same(proposal.service_principal,approval.service_principal)&&
   same(proposal.validity,approval.validity)&&same(proposal.artifact_ref,approval.artifact_ref)&&
   same(proposal.dependency_footprint_ref,approval.dependency_footprint_ref)&&approval.program_digest===proposal.program_digest&&
   approval.bindings_digest===proposal.bindings_digest,'run_approval_integrity');
  return proposed;
 }
 function originalAuthorityInTransaction(reference,run){
  need(db.inTransaction,'run_approval_transaction');
  const saved=read(db.prepare(records+' WHERE a.approval_id=?').raw().get(reference.object_id));
  need(saved.approval.run_id===run.run_id&&same(saved.object_ref,reference),'run_approval_missing');
  return {saved:approvalProposal(saved.approval,run).saved,approval_ref:saved.object_ref};
 }
 function currentAuthorityInTransaction(run){
  const pointer=head(run.run_id);
  need(pointer&&pointer[1]===run.revision,'run_preflight_approval');
  const saved=read(db.prepare(records+' WHERE a.approval_id=?').raw().get(pointer[0])),approval=saved.approval;
  need(approval.run_id===run.run_id&&saved.state==='active'&&authority.runApprovalTimeInTransaction().upper<proposalWindowEnd(approval),'run_preflight_approval');
  requireUnrejectedProposal(approval.proposal_ref);
  const proposed=approvalProposal(approval,run);
  const targets=approval.controller_endorsement_refs.map(ref=>({bindingId:pairFor(ref.scope).binding_id,ref}));
  return {saved:proposed.saved,sourceBindingId:proposed.audit.source_binding_id,targets,approval_ref:saved.object_ref,status_revision:saved.status_revision};
 }
 function view(saved){
  const {approval,object_ref}=saved;
  let {state,status_revision}=saved;
  if(state==='active'&&authority.runApprovalTimeInTransaction().upper>=proposalWindowEnd(approval)){
   write(db.prepare("UPDATE automation_run_approval_state SET status_revision=2,state='expired' WHERE approval_id=? AND state='active'").run(approval.approval_id));
   state='expired';status_revision='2';
  }
  const revoked=canRevoke?db.prepare(revocations+' WHERE approval_id=?').raw().get(approval.approval_id):null;
  need((state==='active'&&!revoked)||state==='expired'||(['revoking','revoked'].includes(state)&&revoked),'run_approval_integrity');
  return {approval,object_ref,state,status_revision,replacement_ref:null,revocation_operation_ref:revoked?readRevocation(revoked,saved).reference:null,
   current_eligibility:state==='active'?'unavailable':'blocked',reason_code:state==='active'?null:state};
 }
 function commitRevocation(saved,run,{key,requestDigest,contextId,audit}){
  const approval=saved.approval,approval_ref=saved.object_ref,id=approval.approval_id,refs=approval.controller_endorsement_refs;
  const operationId=crypto.randomUUID(),revision=String(BigInt(run.revision)+1n);
  checked('LifecycleRevision',revision);
  const result={scope,operation_id:operationId,kind:'revoke_approval',request_identity:key,request_digest:requestDigest,run_id:run.run_id,
   proposal_ref:approval.proposal_ref,approval_ref,endorsement_ref:null,state:refs.length?'accepted':'applied',
   targets:refs.map(ref=>({target_scope:ref.scope,purpose:'close_controller',state:'pending',receipt_ref:null})),result_ref:null,reason_code:null};
  const originalAudit={...audit,context_id:contextId,request_digest:requestDigest,previous_run_revision:run.revision,run_revision:revision,previous_status_revision:saved.status_revision};
  write(db.prepare('INSERT INTO automation_run_approval_revocation VALUES(?,?,?,?,?,?,?,?,?)').run(operationId,id,contextId,key,requestDigest,
   canonical(result),digest(result),canonical(originalAudit),digest(originalAudit)));
  const stored=readRevocation(db.prepare(revocations+' WHERE operation_id=?').raw().get(operationId),saved);
  for(const ref of refs){
   const targets=protectionTargets?.(run,approval_ref,ref.scope)??{preparations:[],grants:[],arm_generations:[]};
   const message=approvalProtectionNotice({kind:'run_approval_revocation',origin_scope:scope,target_scope:ref.scope,revocation_operation_ref:stored.reference,
    approval_ref,endorsement_ref:ref,...targets});
   write(db.prepare('INSERT INTO automation_run_approval_protection_outbox VALUES(?,?,?,?,?)')
    .run(operationId,ref.scope.authority_service_id,ref.scope.database_instance_id,canonical(message),digest(message)));
  }
  if(saved.state==='active')write(db.prepare("UPDATE automation_run_approval_state SET status_revision=2,state=? WHERE approval_id=? AND state='active'")
   .run(refs.length?'revoking':'revoked',id));
  write(db.prepare('UPDATE automation_run_approval_head SET run_revision=? WHERE run_id=? AND approval_id=? AND run_revision=?').run(revision,run.run_id,id,run.revision));
  return stored;
 }
 function revokeApproval(authentication,id,request){
  checked('LifecycleId',id);
  need(fields(request,'approval_ref,context,expected_status_revision,reason')&&fields(request.context,'context_id,idempotency_key')&&
   typeof request.reason==='string'&&request.reason.trim().length>0&&Buffer.byteLength(request.reason)<=4096,'run_approval_revocation_request');
  const {context,approval_ref,expected_status_revision,reason}=structuredClone(request);
  for(const value of Object.values(context))checked('LifecycleId',value);
  checked('LifecycleRevision',expected_status_revision);runApprovalReference(approval_ref);
  const requestDigest=digest({operation_id:revokeOperation,path:'/api/v1/run-approvals/'+id+'/revoke',request:{approval_ref,expected_status_revision,reason}});
  return writer(()=>{
   const saved=read(db.prepare(records+' WHERE a.approval_id=?').raw().get(id)),run=currentRun(runOriginal(saved.approval.run_id));
   const actor=authority.identifyRunApprovalActorInTransaction(authentication),key=requestIdentity({actor,request_epoch:requestEpoch},context.idempotency_key,revokeOperation);
   const existing=db.prepare(revocations+' WHERE identity_digest=?').raw().get(key);
   if(existing)return visible(authentication,run,()=>{
    need(existing[1]===id&&existing[4]===requestDigest,'run_approval_revocation_conflict');return readRevocation(existing,saved).result;
   });
   const proof=authorize(authentication,revokeOperation,run.definition_id,context.context_id);
   need(actor.principal_kind==='human'&&same(actor,proof.actor),'run_approval_authorization');
   need(same(approval_ref,saved.object_ref)&&saved.status_revision===expected_status_revision&&head(run.run_id)?.[0]===id&&
    ['active','expired'].includes(saved.state)&&!db.prepare('SELECT 1 FROM automation_run_approval_revocation WHERE approval_id=?').pluck().get(id),'run_approval_revocation_conflict');
   authority.assertRunCapacityInTransaction(proof);
   const stored=commitRevocation(saved,run,{key,requestDigest,contextId:context.context_id,audit:{authentication:proof.authentication_evidence,reason}});
   need(same(proof,authorize(authentication,revokeOperation,run.definition_id,context.context_id)),'run_authorization_changed');
   return stored.result;
  });
 }
 function requireUnrejectedProposal(reference){
  if(rejectFromPeer)need(!db.prepare('SELECT 1 FROM automation_run_endorsement_rejection WHERE proposal_id=? LIMIT 1').pluck().get(reference.object_id),'run_approval_proposal_rejected');
 }
 async function receiveEndorsementRevocation(bindingId,socket,input){
  need(pendingEndorsementRevocations<4,'run_endorsement_capacity');
  const notice=endorsementRevocationNotice(structuredClone(input)),hash=digest(notice),origin=notice.origin_scope;
  function authorizePeer(){
   const paired=authorizeEndorsementSender(bindingId,socket);
   need(same(peerScope(paired.peer),origin)&&same(notice.target_scope,scope),'run_endorsement_sender');return paired;
  }
  function prior(){
   const row=db.prepare(`SELECT operation_id,endorsement_id,proposal_id,request_digest,protection_operation_id,receipt_id,body_jcs,body_digest
    FROM automation_run_endorsement_rejection WHERE origin_service_id=? AND origin_database_instance_id=? AND (operation_id=? OR endorsement_id=?)`).raw()
    .get(origin.authority_service_id,origin.database_instance_id,notice.revocation_operation_ref.object_id,notice.endorsement_ref.object_id);
   if(!row)return null;
   const [operationId,endorsementId,proposalId,requestDigest,protectionId,receiptId,text,digest]=row;
   need(operationId===notice.revocation_operation_ref.object_id&&endorsementId===notice.endorsement_ref.object_id&&
    proposalId===notice.proposal_ref.object_id&&requestDigest===hash,'run_endorsement_revocation_conflict');
   const receipt=endorsementRevocationReceipt(decode(text,digest),notice);
   need(receipt.receipt_id===receiptId&&(receipt.protection_operation_ref?.object_id??null)===protectionId,'run_approval_integrity');return receipt;
  }
  const prepared=writer(()=>{
   const paired=authorizePeer(),existing=prior();if(existing)return {existing};
   const run=runForProposal(notice.proposal_ref.object_id),{proposal}=getProposal(notice.proposal_ref,run).saved;
   need(proposal.controller_targets.some(target=>same(target.controller_scope,origin)),'run_endorsement_sender');
   return {record:paired.record,started:authority.runApprovalTimeInTransaction(),runId:run.run_id};
  });
  if(prepared.existing)return prepared.existing;
  pendingEndorsementRevocations++;
  try{
   const remote=await readRevokedEndorsement(bindingId,notice.endorsement_ref);
   need(same(remote.object_ref,notice.endorsement_ref)&&same(remote.endorsement.proposal_ref,notice.proposal_ref)&&remote.endorsement.run_id===prepared.runId&&
    remote.current_eligibility==='blocked'&&['revoking','expired'].includes(remote.state)&&remote.status_revision===notice.deny_revision&&
    same(remote.revocation_operation_ref,notice.revocation_operation_ref),'run_endorsement_revocation_original');
   return writer(()=>{
    function recheck(){
     const paired=authorizePeer(),current=authority.runApprovalTimeInTransaction();
     need(same(paired.record,prepared.record)&&same(current.domain,prepared.started.domain)&&current.tick>=prepared.started.tick&&
      current.tick<prepared.started.tick+2000000000n,'run_endorsement_revocation_expired');
    }
    recheck();const existing=prior();if(existing)return existing;
    const run=currentRun(runOriginal(prepared.runId)),{proposal}=getProposal(notice.proposal_ref,run).saved;
    const member=proposal.controller_targets.find(target=>same(target.controller_scope,origin));
    need(member&&digest(member)===remote.endorsement.controller_target_digest&&same(proposal.validity,remote.endorsement.validity)&&
     same(proposal.service_principal,remote.endorsement.service_principal),'run_endorsement_revocation_original');
    const row=db.prepare(records+' WHERE a.proposal_id=?').raw().get(proposal.proposal_id);
    let protection=null;
    if(row){
     const saved=read(row),id=saved.object_ref.object_id;
     need(saved.approval.controller_endorsement_refs.some(ref=>same(ref,notice.endorsement_ref)),'run_endorsement_revocation_original');
     const earlier=db.prepare(revocations+' WHERE approval_id=?').raw().get(id);
     if(earlier)protection=readRevocation(earlier,saved).reference;
     else{
      need(head(run.run_id)?.[0]===id&&['active','expired'].includes(saved.state),'run_approval_transition');
      const key=digest({scope,operation_id:'applyEndorsementRevocation',ref:notice.revocation_operation_ref});
      protection=commitRevocation(saved,run,{key,requestDigest:hash,contextId:null,
       audit:{endorsement_revocation_ref:notice.revocation_operation_ref,endorsement_ref:notice.endorsement_ref,pairing_ref:{id:prepared.record.binding_id,revision:prepared.record.revision,digest:prepared.record.digest}}}).reference;
     }
    }
    const receipt={format:'leafseamer.run-endorsement-rejection/1',receipt_id:crypto.randomUUID(),target_scope:scope,
     proposal_ref:notice.proposal_ref,endorsement_ref:notice.endorsement_ref,revocation_operation_ref:notice.revocation_operation_ref,
     request_digest:hash,identity_state:'denied',protection_operation_ref:protection};
    write(db.prepare('INSERT INTO automation_run_endorsement_rejection VALUES(?,?,?,?,?,?,?,?,?,?)').run(origin.authority_service_id,origin.database_instance_id,
     notice.revocation_operation_ref.object_id,notice.endorsement_ref.object_id,proposal.proposal_id,hash,protection?.object_id??null,receipt.receipt_id,canonical(receipt),digest(receipt)));
    recheck();return prior();
   });
  }finally{pendingEndorsementRevocations--;}
 }
 return Object.freeze({currentRun,currentAuthorityInTransaction,originalAuthorityInTransaction,observeAuthority,armRevocationLatch(reference){
  need(db.inTransaction,'run_approval_transaction');
  const row=canRevoke?db.prepare(revocations+' WHERE approval_id=?').raw().get(reference.object_id):null;
  if(!row)return null;
  const saved=read(db.prepare(records+' WHERE a.approval_id=?').raw().get(reference.object_id));
  need(same(saved.object_ref,reference),'run_approval_integrity');
  return readRevocation(row,saved).reference.object_id;
 },async approve(authentication,runId,request){
  checked('LifecycleId',runId);
  need(fields(request,'context,controller_endorsement_refs,expected_prior_approval_ref,expected_run_revision,proposal_ref')&&
   fields(request.context,'context_id,idempotency_key')&&Array.isArray(request.controller_endorsement_refs)&&request.controller_endorsement_refs.length<=256,'run_approval_request');
  const {context,proposal_ref,controller_endorsement_refs:refs,expected_run_revision,expected_prior_approval_ref}=structuredClone(request);
  for(const id of Object.values(context))checked('LifecycleId',id);
  checked('LifecycleRevision',expected_run_revision);proposalReference(proposal_ref);
  if(expected_prior_approval_ref!==null){checked('ContentRef',expected_prior_approval_ref);need(expected_prior_approval_ref.kind==='run_approval','run_approval_request');}
  refs.forEach(endorsementReference);
  const byScope=new Map(refs.map(ref=>[canonical(ref.scope),ref]));
  need(byScope.size===refs.length,'run_approval_targets');
  const ordered=[...byScope.keys()].sort().map(key=>byScope.get(key));
  const requestDigest=digest({operation_id:operation,path:'/api/v1/runs/'+runId+'/approvals',request:{proposal_ref,
   controller_endorsement_refs:ordered,expected_run_revision,expected_prior_approval_ref}});
  const prepared=writer(()=>{
   const run=currentRun(runOriginal(runId)),actor=authority.identifyRunApprovalActorInTransaction(authentication);
   const key=requestIdentity({actor,request_epoch:requestEpoch},context.idempotency_key,operation);
   const existing=prior(authentication,run,key,requestDigest);if(existing)return {existing};
   const proof=authorize(authentication,operation,run.definition_id,context.context_id);
   need(same(actor,proof.actor)&&actor.principal_kind==='human','run_approval_authorization');
   eligibleRun(run,expected_run_revision,expected_prior_approval_ref);
   requireUnrejectedProposal(proposal_ref);
   need(publicationBasisCurrent(run),'run_publication_binding_unapproved');
   const {saved,audit}=getProposal(proposal_ref,run),proposal=saved.proposal;
   need(proposal.controller_targets.length===byScope.size&&proposal.controller_targets.every(target=>byScope.has(canonical(target.controller_scope))),'run_approval_targets');
   const targets=proposal.controller_targets.map(({controller_scope})=>({bindingId:pairFor(controller_scope).binding_id,ref:byScope.get(canonical(controller_scope))}));
   return {key,proof,run,saved,targets,sourceBindingId:audit.source_binding_id};
  });
  if(prepared.existing)return prepared.existing;
  const proposal=prepared.saved.proposal,sourceRef=proposal.source_service_approval_ref;
  const {assertCurrentInTransaction:verify}=await observeAuthority(prepared);
  return writer(()=>{
   const run=currentRun(runOriginal(runId)),existing=prior(authentication,run,prepared.key,requestDigest);if(existing)return existing;
   const proof=authorize(authentication,operation,run.definition_id,context.context_id);
   need(same(proof,prepared.proof)&&same(run,prepared.run),'run_approval_changed');
   eligibleRun(run,expected_run_revision,expected_prior_approval_ref);
   requireUnrejectedProposal(proposal_ref);
   need(publicationBasisCurrent(run),'run_publication_binding_unapproved');
   need(same(getProposal(proposal_ref,run).saved,prepared.saved),'run_approval_proposal_changed');
   verify();authority.assertRunCapacityInTransaction(proof);
   need(db.prepare('SELECT count(*) FROM automation_run_approval').pluck().get()<16384,'run_approval_capacity');
   const id=crypto.randomUUID(),operationId=crypto.randomUUID(),revision=String(BigInt(run.revision)+1n);
   const {principal_kind:_kind,...approved_by}=proof.actor;
   const audit={context_id:context.context_id,request_digest:requestDigest,authentication:proof.authentication_evidence,
    source_binding_id:prepared.sourceBindingId,targets:prepared.targets,previous_run_revision:run.revision,run_revision:revision};
   const auditText=canonical(audit),auditHash=digest(audit);
   const approval={scope,approval_id:id,revision:'1',run_id:runId,proposal_ref,artifact_ref:proposal.artifact_ref,program_digest:proposal.program_digest,
    bindings_digest:proposal.bindings_digest,dependency_footprint_ref:proposal.dependency_footprint_ref,service_principal:proposal.service_principal,
    source_service_approval_ref:sourceRef,controller_endorsement_refs:prepared.targets.map(item=>item.ref),validity:proposal.validity,
    approved_by,approval_operation_id:operationId,created_authentication_ref:authRef(id,auditHash)};
   const text=canonical(approval),hash=digest(approval),reference={kind:'run_approval',scope,object_id:id,revision:'1',digest:hash};
   checked('LifecycleRevision',revision);
   need(Buffer.byteLength(text)<=1048576&&Buffer.byteLength(auditText)<=1048576,'run_approval_capacity');
   const result={scope,operation_id:operationId,kind:'approve',request_identity:prepared.key,request_digest:requestDigest,run_id:runId,
    proposal_ref,approval_ref:reference,endorsement_ref:null,state:'applied',targets:[],result_ref:reference,reason_code:null};
   const notice={kind:'run_approval_changed',operation_id:operationId,approval_ref:reference,run_id:runId,run_revision:revision};
   write(db.prepare('INSERT INTO automation_run_approval VALUES(?,?,?,?,?,?,?,?,?,?)').run(id,runId,proposal_ref.object_id,context.context_id,prepared.key,requestDigest,text,hash,auditText,auditHash));
   write(db.prepare('INSERT INTO automation_run_approval_head VALUES(?,?,?)').run(runId,id,revision));
   write(db.prepare("INSERT INTO automation_run_approval_state VALUES(?,1,'active')").run(id));
   write(db.prepare('INSERT INTO automation_run_approval_operation VALUES(?,?,?,?)').run(operationId,id,canonical(result),digest(result)));
   write(db.prepare('INSERT INTO automation_run_approval_outbox VALUES(?,?,?)').run(operationId,canonical(notice),digest(notice)));
   need(same(proof,authorize(authentication,operation,run.definition_id,context.context_id)),'run_authorization_changed');verify();
   return read(db.prepare(records+' WHERE a.approval_id=?').raw().get(id)).result;
  });
 },readForPeer(bindingId,socket,request){
  return writer(()=>{
   const paired=authorizeControllerPeerInTransaction(bindingId,socket),target=peerScope(paired.peer);
   const row=db.prepare(records+' WHERE a.approval_id=? AND a.body_digest=?').raw().get(request.approval_id,request.approval_digest);
   const saved=read(row),approval=saved.approval;
   need(saved.object_ref.revision===request.approval_revision&&approval.controller_endorsement_refs.some(ref=>same(ref.scope,target)),'run_approval_missing');
   const run=runOriginal(approval.run_id);
   approvalProposal(approval,run);
   const {object_ref,state,status_revision,revocation_operation_ref}=view(saved),{lower,upper}=authority.runApprovalTimeInTransaction();
   const end=proposalWindowEnd(approval),pairEnd=BigInt(paired.record.expires_ns),deadline=end<pairEnd?end:pairEnd;
   const before=BigInt(Date.parse(approval.validity.not_before))*1000000n;
   const eligible=state==='active'&&head(run.run_id)?.[0]===approval.approval_id&&lower>=before&&upper<deadline&&publicationBasisCurrent(run);
   need(same(paired,authorizeControllerPeerInTransaction(bindingId,socket)),'run_approval_pairing');
   return {approval,object_ref,state,status_revision,revocation_operation_ref,local_eligibility:eligible?'eligible':'blocked',eligible_before_ns:eligible?String(deadline):null};
  });
 },getCurrentApproval(authentication,id){
  checked('LifecycleId',id);
  return writer(()=>visible(authentication,runOriginal(id),()=>{
   const pointer=head(id);return pointer?view(read(db.prepare(records+' WHERE a.approval_id=?').raw().get(pointer[0]))):null;
  }));
 },getApprovalRevision(authentication,id,revision){
  checked('LifecycleId',id);need(revision==='1','run_approval_missing');
  return writer(()=>{
   const row=db.prepare(records+' WHERE a.approval_id=?').raw().get(id);need(row,'run_approval_missing');
   return visible(authentication,runOriginal(row[1]),()=>view(read(row)));
  });
 },getApprovalOperation(authentication,id){
  checked('LifecycleId',id);
  return writer(()=>{
   const row=db.prepare(records+' WHERE o.operation_id=?').raw().get(id);
   if(row)return visible(authentication,runOriginal(row[1]),()=>read(row).result);
   const reversal=canRevoke?db.prepare(revocations+' WHERE operation_id=?').raw().get(id):null;need(reversal,'run_approval_missing');
   const saved=read(db.prepare(records+' WHERE a.approval_id=?').raw().get(reversal[1]));
   return visible(authentication,runOriginal(saved.approval.run_id),()=>revocationView(readRevocation(reversal,saved),saved));
  });
 },lookupApproval(authentication,{run_id,context_id,idempotency_key}){
  for(const id of [run_id,context_id,idempotency_key])checked('LifecycleId',id);
  return writer(()=>{
   const run=runOriginal(run_id),input={authentication,context_id,operation_id:operation},proof=authority.lookupInTransaction(input);
   need(same(proof.scope,scope)&&proof.request_epoch===requestEpoch,'run_approval_authorization');
   return visible(authentication,run,()=>{
    const row=db.prepare(records+' WHERE a.identity_digest=?').raw().get(requestIdentity(proof,idempotency_key,operation));
    need(!row||row[1]===run_id,'run_approval_conflict');
    need(same(proof,authority.lookupInTransaction(input)),'run_authorization_changed');
    return {request_digest:row?.[4]??null,result:row?read(row).result:null};
   });
  });
 },...(canRevoke&&{revokeApproval,prepareApprovalProtection(operationId,target){
  checked('LifecycleId',operationId);need(fields(target,'authority_service_id,database_instance_id'),'run_approval_pairing');
  for(const id of Object.values(target))checked('LifecycleId',id);
  return writer(()=>({notice:queuedProtection(operationId,target),binding_id:pairFor(target,'automation.protect').binding_id}));
 },lookupApprovalRevocation(authentication,{approval_id,context_id,idempotency_key}){
  for(const id of [approval_id,context_id,idempotency_key])checked('LifecycleId',id);
  return writer(()=>{
   const saved=read(db.prepare(records+' WHERE a.approval_id=?').raw().get(approval_id)),run=runOriginal(saved.approval.run_id);
   const input={authentication,context_id,operation_id:revokeOperation},proof=authority.lookupInTransaction(input);
   need(same(proof.scope,scope)&&proof.request_epoch===requestEpoch,'run_approval_authorization');
   return visible(authentication,run,()=>{
    const row=db.prepare(revocations+' WHERE identity_digest=?').raw().get(requestIdentity(proof,idempotency_key,revokeOperation));
    need(!row||row[1]===approval_id,'run_approval_revocation_conflict');
    need(same(proof,authority.lookupInTransaction(input)),'run_authorization_changed');
    return {request_digest:row?.[4]??null,result:row?readRevocation(row,saved).result:null};
   });
  });
 }}),...(canAdoptProtection&&{scanApprovalProtection,deliverApprovalProtection}),...(canCompleteProtection&&{recoveryProtectionInTransaction}),
 ...(bindingRevocationForRun&&{adoptBindingRevocations}),
 ...(rejectFromPeer&&{receiveEndorsementRevocation})});
}
