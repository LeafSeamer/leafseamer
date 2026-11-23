import crypto from 'node:crypto';
import {canonical,digest,ref,seal} from './runtime/auth-clock-value.mjs';
import {pairingNeed as need,pairingUuid as uuid} from './runtime/peer-pairing-value.mjs';
import {verifyFirstPairingImport} from './first-pairing-import.mjs';
import {writeLocalPermission} from './local-authorization.mjs';

export function firstPairingEnrollment({db,service,issuer,remember}){
 if(Number(service.schema_revision)<19)return null;
 const scope={authority_service_id:service.service_id,database_instance_id:service.database_instance_id};
 const put=result=>need(result.changes===1,'first_enrollment_storage');
 return Object.freeze({
  select(id,now){
   uuid(id);
   need(!db.prepare("SELECT 1 FROM peer_pairing_revision WHERE state='activated' LIMIT 1").pluck().get()&&
    !db.prepare('SELECT 1 FROM local_permission_head LIMIT 1').pluck().get(),'first_import_already_configured');
   const row=db.prepare('SELECT body_jcs,body_digest,controller_certificate,seamer_certificate,target_binding_id FROM first_pairing_import WHERE authorization_id=?').raw().get(id);
   need(row,'first_enrollment_missing');
   const [text,envelopeDigest,controllerCertificate,seamerCertificate,localBinding]=row;
   const {envelope,target}=verifyFirstPairingImport(service,{bytes:Buffer.from(text),controllerCertificate,seamerCertificate});
   const {authorization}=envelope,controller=authorization.transcript.parties.find(p=>p.service_kind==='controller');
   need(envelopeDigest===digest(envelope)&&authorization.authorization_id===id&&target.binding_id===localBinding&&
    controller.service_id===issuer.service_id&&controller.issuer_id===issuer.issuer_id&&controller.issuer_epoch===issuer.issuer_epoch,'first_enrollment_binding');
   need(now.lower>=BigInt(authorization.not_before_ns)&&now.upper<BigInt(authorization.expires_ns),'first_authorization_expired');
   return {authorization,envelopeDigest,localBinding};
  },
  assertNoPending(){
   need(!db.prepare(`SELECT 1 FROM first_pairing_import i JOIN peer_pairing_operation o ON o.request_id=i.authorization_id
    JOIN peer_pairing_head h USING(binding_id) JOIN peer_pairing_revision r ON r.binding_id=h.binding_id AND r.revision=h.revision
    WHERE r.state IN ('staged','proving','proved')`).pluck().get(),'first_enrollment_pending');
  },
  remember,
  reserved(bindingId){
   return db.prepare('SELECT 1 FROM first_pairing_import i JOIN peer_pairing_operation o ON o.request_id=i.authorization_id WHERE o.binding_id=? AND o.revision=1')
    .pluck().get(bindingId)===1;
  },
  completed(id){
   uuid(id);
   const row=db.prepare(`SELECT c.body_jcs,c.body_digest,i.body_digest FROM first_pairing_completion c
    JOIN first_pairing_import i USING(authorization_id) WHERE c.authorization_id=?`).raw().get(id);
   if(!row)return null;
   const value=JSON.parse(row[0]);need(canonical(value)===row[0]&&digest(value)===row[1]&&value.authorization_id===id&&value.authorization_digest===row[2],
    'first_enrollment_integrity');return value;
  },
  complete({authorization,envelopeDigest},pairing,now){
   const {authorization_id:id,administrator:actor}=authorization,subject=crypto.randomUUID(),operation=crypto.randomUUID();
   const {issuer_id,issuer_epoch,principal_id}=actor;
   put(db.prepare("INSERT INTO local_subject VALUES(?,?,?,?,'human')").run(subject,issuer_id,issuer_epoch,principal_id));
   const grant=seal({format:'seamer-local-permission/1',id:crypto.randomUUID(),revision:'1',scope,subject_id:subject,issuer_id,issuer_epoch,
    permissions:['auth.manage','auth.read'],definition_scope:{kind:'all',definition_ids:[]},not_before_ns:String(now.lower),expires_ns:pairing.expires_ns,
    state:'active',operation_id:operation});
   writeLocalPermission(db,{grant,previous:null,actorSubjectId:subject,requestId:id,requestDigest:envelopeDigest});
   const receipt={format:'leafseamer.first-pairing-completion/1',authorization_id:id,authorization_digest:envelopeDigest,
    pairing_ref:{id:pairing.binding_id,revision:pairing.revision,digest:pairing.digest},controller_pairing_ref:pairing.approval.controller_binding_ref,
    administrator:actor,local_permission_ref:ref(grant),state:'activated'};
   put(db.prepare('INSERT INTO first_pairing_completion VALUES(1,?,?,?,?,?,?,?)').run(id,pairing.binding_id,Number(pairing.revision),
    operation,now.acquisition.id,canonical(receipt),digest(receipt)));
   return receipt;
  }
 });
}
