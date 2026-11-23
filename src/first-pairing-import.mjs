import {X509Certificate} from 'node:crypto';
import {canonical,digest} from './runtime/auth-clock-value.mjs';
import {verifyFirstPairingAuthorization} from './runtime/first-pairing-authorization.mjs';
import {pairingCertificate,pairingNeed as need} from './runtime/peer-pairing-value.mjs';

const certificate=value=>{
 need(Buffer.isBuffer(value)&&value.length>0&&value.length<=65536,'first_import_certificate');
 return new X509Certificate(value);
};

export function verifyFirstPairingImport(service,{bytes,controllerCertificate,seamerCertificate}){
 const controller=certificate(controllerCertificate),seamer=certificate(seamerCertificate);
 const envelope=verifyFirstPairingAuthorization(bytes,controller);
 need(bytes.equals(Buffer.from(canonical(envelope))),'first_authorization_encoding');
 const {transcript}=envelope.authorization;
 const target=transcript.parties.find(party=>party.service_kind==='seamer');
 need(target.service_id===service.service_id&&target.database_instance_id===service.database_instance_id,'first_import_scope');
 need(Object.entries(pairingCertificate(seamer.raw)).every(([key,value])=>target[key]===value),'first_import_target_certificate');
 return {envelope,target,issuerPin:controller.raw,localPin:seamer.raw};
}

export function saveFirstPairingImport(db,service,input){
 need(db.inTransaction&&service.authority==='seamer'&&Number(service.schema_revision)>=18,'first_import_storage');
 const {envelope,target,issuerPin,localPin}=verifyFirstPairingImport(service,input);
 const {authorization_id,transcript,transcript_digest}=envelope.authorization,bytes=input.bytes;
 const body=bytes.toString('utf8'),checksum=digest(envelope),binding=target.binding_id,attempt=transcript.pair_attempt_id;
 const rows=db.prepare(`SELECT body_jcs,body_digest,controller_certificate,seamer_certificate FROM first_pairing_import
  WHERE authorization_id=? OR pair_attempt_id=? OR target_binding_id=?`).raw().all(authorization_id,attempt,binding);
 if(rows.length){
  const [saved,hash,storedIssuerPin,storedLocalPin]=rows[0];
  need(rows.length===1&&saved===body&&hash===checksum&&storedIssuerPin.equals(issuerPin)&&storedLocalPin.equals(localPin),'first_import_conflict');
 }else{
  need(!db.prepare("SELECT 1 FROM peer_pairing_revision WHERE state='activated' LIMIT 1").pluck().get()&&
   !db.prepare('SELECT 1 FROM local_permission_head LIMIT 1').pluck().get(),'first_import_already_configured');
  need(db.prepare('INSERT INTO first_pairing_import VALUES(?,?,?,?,?,?,?)')
   .run(authorization_id,attempt,binding,body,checksum,issuerPin,localPin).changes===1,'first_import_storage');
 }
 return {format:'leafseamer.first-pairing-import/1',authorization_id,binding_id:binding,transcript_digest,authorization_digest:checksum,state:'imported'};
}
