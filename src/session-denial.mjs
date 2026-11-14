import crypto from 'node:crypto';
import {canonical,digest,same} from './runtime/auth-clock-value.mjs';
import {sessionDenialNotice,sessionDenialReceipt} from './runtime/peer-session-denial.mjs';
const need=(ok,code)=>{if(!ok)throw new Error('session_denial_'+code);};
export function receivedSessionDenials({db,service,bootId,isOpen,pairings,issuer}){
 const fixed=structuredClone(issuer),scope={authority_service_id:service.service_id,database_instance_id:service.database_instance_id};
 const live=()=>need(isOpen()&&db.open,'closed');
 function authorized(bindingId,socket,notice){
  const result=pairings.authorizeRevocationInTransaction(bindingId,socket),peer=result.peer;
  need(peer.service_kind==='controller'&&peer.service_id===fixed.service_id&&peer.issuer_id===fixed.issuer_id&&peer.issuer_epoch===fixed.issuer_epoch&&
   same(notice.target_scope,scope)&&notice.issuer_scope.authority_service_id===peer.service_id&&notice.issuer_scope.database_instance_id===peer.database_instance_id&&
   notice.issuer_id===fixed.issuer_id&&notice.issuer_epoch===fixed.issuer_epoch,'issuer');return result.record;
 }
 function decode(row){
  const notice=sessionDenialNotice(JSON.parse(row[0])),receipt=sessionDenialReceipt(JSON.parse(row[2]),notice);
  need(canonical(notice)===row[0]&&digest(notice)===row[1]&&canonical(receipt)===row[2]&&digest(receipt)===row[3]&&same(notice.target_scope,scope),'integrity');return {notice,receipt};
 }
 return Object.freeze({
  receive(bindingId,socket,input){
   live();need(!db.inTransaction,'transaction');const notice=sessionDenialNotice(JSON.parse(canonical(input)));
   return db.transaction(()=>{
    const record=authorized(bindingId,socket,notice),key=[fixed.service_id,fixed.issuer_id,fixed.issuer_epoch,notice.operation_ref.id];
    const old=db.prepare('SELECT notice_jcs,notice_digest,receipt_jcs,receipt_digest FROM received_session_denial WHERE issuer_service_id=? AND issuer_id=? AND issuer_epoch=? AND operation_id=?').raw().get(...key);
    if(old){const original=decode(old);need(same(original.notice,notice),'conflict');need(same(authorized(bindingId,socket,notice),record),'changed');return original.receipt;}
    need(db.prepare('SELECT count(*) FROM received_session_denial').pluck().get()<16384,'capacity');
    const receipt=sessionDenialReceipt({format:'leafseamer.session-denial-receipt/1',receipt_id:crypto.randomUUID(),notice_digest:digest(notice),target_scope:scope,
     service_boot_id:bootId,identity_state:'denied',protection_state:'pending'},notice);
    need(db.prepare('INSERT INTO received_session_denial VALUES(?,?,?,?,?,?,?,?,?,?)').run(...key,notice.session_id,bootId,canonical(notice),digest(notice),canonical(receipt),digest(receipt)).changes===1,'write');
    need(same(authorized(bindingId,socket,notice),record),'changed');return receipt;
   }).immediate();
  },
  assertAllowed(snapshot){
   live();need(snapshot.issuer_id===fixed.issuer_id&&snapshot.issuer_epoch===fixed.issuer_epoch,'issuer');
   if(snapshot.principal_kind!=='human')return;
   const row=db.prepare('SELECT notice_jcs,notice_digest,receipt_jcs,receipt_digest FROM received_session_denial WHERE issuer_service_id=? AND issuer_id=? AND issuer_epoch=? AND session_id=? LIMIT 1').raw().get(fixed.service_id,fixed.issuer_id,fixed.issuer_epoch,snapshot.session_id);
   if(row){const {notice}=decode(row);need(notice.session_id===snapshot.session_id,'integrity');throw new Error('session_denial_revoked');}
  }
 });
}
