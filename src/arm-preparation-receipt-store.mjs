import {canonical,digest,same} from './runtime/auth-clock-value.mjs';
import {armPreparationRecord,preparationResponseLimit} from './runtime/peer-run-preparation.mjs';
import {need} from './automation-publication.mjs';

export function armPreparationReceiptStore({db,bootId}){
 const select=`SELECT r.operation_id,r.controller_service_id,r.controller_database_instance_id,r.grant_id,r.receipt_id,r.request_digest,
  r.body_jcs,r.body_digest,r.audit_jcs,r.audit_digest,n.body_jcs,n.body_digest FROM automation_arm_preparation_receipt r
  LEFT JOIN automation_arm_preparation_notification n USING(preparation_id) WHERE r.preparation_id=?`;
 const inside=()=>need(db.inTransaction,'run_preparation_receipt_transaction');
 const put=result=>need(result.changes===1,'run_preparation_receipt_storage');
 function decode(text,hash){
  need(typeof text==='string','run_preparation_receipt_integrity');
  const body=JSON.parse(text);need(canonical(body)===text&&digest(body)===hash,'run_preparation_receipt_integrity');return body;
 }
 function validate(record,plan){
  armPreparationRecord(record,plan.lookup);
  need(same(record.request,plan.request)&&same(record.applied_receipt.grant.endorsement_ref,plan.endorsement_ref),'run_preparation_receipt_binding');
  return record.applied_receipt;
 }
 function notification(plan,receipt){
  const {controller_scope,preparation_id}=plan.lookup;
  const receipt_ref={kind:'preparation_receipt',scope:controller_scope,object_id:receipt.receipt_id,revision:receipt.revision,digest:digest(receipt)};
  return {kind:'run_arm_preparation_receipt_observed',operation_ref:plan.request.operation_ref,preparation_id,receipt_ref,target_grant_ref:receipt.grant_ref};
 }
 function read(plan){
  inside();const row=db.prepare(select).raw().get(plan.lookup.preparation_id);if(!row)return null;
  const [op,controllerId,databaseId,grantId,receiptId,requestDigest,text,hash,auditText,auditHash,noticeText,noticeHash]=row;
  const record=decode(text,hash),receipt=validate(record,plan),expected=notification(plan,receipt);
  const audit=decode(auditText,auditHash),notice=decode(noticeText,noticeHash);
  need(op===plan.request.operation_id&&controllerId===plan.lookup.controller_scope.authority_service_id&&databaseId===plan.lookup.controller_scope.database_instance_id&&
   grantId===receipt.grant_ref.object_id&&receiptId===receipt.receipt_id&&requestDigest===plan.lookup.request_digest&&
   audit.format==='leafseamer.arm-preparation-observation/1'&&['submit','lookup'].includes(audit.via)&&same(audit.pairing_ref,plan.pairing_ref)&&
   same(audit.receipt_ref,expected.receipt_ref)&&same(notice,expected),'run_preparation_receipt_integrity');
  return {record,audit,receipt_ref:expected.receipt_ref,target_grant_ref:expected.target_grant_ref};
 }
 function adopt(plan,input,via,now){
  inside();const record=structuredClone(input),receipt=validate(record,plan),old=read(plan);
  if(old){need(same(old.record,record),'run_preparation_receipt_conflict');return old;}
  const {preparation_id,controller_scope,request_digest}=plan.lookup,notice=notification(plan,receipt);
  const audit={format:'leafseamer.arm-preparation-observation/1',via,service_boot_id:bootId,pairing_ref:plan.pairing_ref,
   clock_domain_ref:now.domain,observed_tick_ns:String(now.tick),receipt_ref:notice.receipt_ref};
  const [text,auditText,noticeText]=[record,audit,notice].map(canonical),bytes=Buffer.byteLength(text);
  const [count,size]=db.prepare('SELECT count(*),coalesce(sum(length(CAST(body_jcs AS BLOB))),0) FROM automation_arm_preparation_receipt').raw().get();
  need(bytes<=preparationResponseLimit-4096&&count<16384&&size+bytes<=67108864,'run_preparation_receipt_capacity');
  put(db.prepare('INSERT INTO automation_arm_preparation_receipt VALUES(?,?,?,?,?,?,?,?,?,?,?)')
   .run(preparation_id,plan.request.operation_id,controller_scope.authority_service_id,controller_scope.database_instance_id,
    receipt.grant_ref.object_id,receipt.receipt_id,request_digest,text,digest(record),auditText,digest(audit)));
  put(db.prepare('INSERT INTO automation_arm_preparation_notification VALUES(?,?,?)').run(preparation_id,noticeText,digest(notice)));
  return read(plan);
 }
 return Object.freeze({read,adopt,has(preparationId){
  inside();return db.prepare('SELECT 1 FROM automation_arm_preparation_receipt WHERE preparation_id=?').pluck().get(preparationId)===1;
 }});
}
