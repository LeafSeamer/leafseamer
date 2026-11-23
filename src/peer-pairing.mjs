import {peerPairingStore} from './runtime/peer-pairing-store.mjs';
import {pairingNeed as need,pairingUuid as uuid} from './runtime/peer-pairing-value.mjs';
import {canonical,digest,checked} from './runtime/auth-clock-value.mjs';
import {clockInteger} from './runtime/native-clock.mjs';
import {firstPairingEnrollment} from './first-pairing-enrollment.mjs';

// 常规管理始终经远端人员核验。首次配对另从本机导入原件取得具名授权。
export function seamerPeerPairings({db,service,bootId,isOpen,clock,issuer,management=null}){
 need(issuer&&Object.keys(issuer).sort().join(',')==='issuer_epoch,issuer_id,service_id','issuer_configuration');
 for(const value of Object.values(issuer))uuid(value);need(issuer.service_id!==service.service_id,'issuer_configuration');
 const fixedIssuer=Object.freeze({...issuer});
 function live(){need(isOpen()&&db.open,'closed');}
 function source(){live();need(management&&['assertCurrent','authenticate','current'].every(key=>typeof management[key]==='function'),'management_unavailable');need(management.assertCurrent(fixedIssuer)===undefined,'management_async');}
 function time(){
  live();need(clock&&typeof clock.pairingSample==='function','clock_unavailable');
  const sample=clock.pairingSample(),lower=clockInteger(sample.lower_ns),upper=clockInteger(sample.upper_ns),tick=clockInteger(sample.tick_ns);
  const stored=db.prepare('SELECT digest,body FROM authentication_clock_acquisition WHERE id=? AND revision=?').raw().get(sample.acquisition_ref.id,sample.acquisition_ref.revision);
  need(stored&&stored[0]===sample.acquisition_ref.digest&&lower<=upper,'clock_binding');const original=JSON.parse(stored[1].toString());
  need(original.service_id===service.service_id&&original.service_boot_id===bootId&&original.clock_domain_id===sample.runtime_domain_ref.clock_domain_id&&original.descriptor_digest===sample.runtime_domain_ref.descriptor_digest,'clock_binding');
  return {lower,upper,tick,acquisition:sample.acquisition_ref,runtime_domain_ref:sample.runtime_domain_ref};
 }
 function authenticated(input,now,revocationScope=null){
  source();const found=management.authenticate(input,now,fixedIssuer,revocationScope),session=found?.session;
  need(session&&session.principal_kind==='human'&&session.audience===service.service_id&&session.issuer_id===fixedIssuer.issuer_id&&session.issuer_epoch===fixedIssuer.issuer_epoch,'management_denied');
  uuid(session.principal_id);uuid(session.session_id);
  need(now.lower>=clockInteger(session.not_before_ns)&&now.upper<clockInteger(session.expires_ns)&&now.upper<clockInteger(session.recent_before_ns),'management_denied');
  const actor={principal_id:session.principal_id,principal_kind:'human',issuer_id:session.issuer_id,issuer_epoch:session.issuer_epoch};
  remember(actor);
  return {session:JSON.parse(canonical(session))};
 }
 function remember(actor){
  const prior=db.prepare('SELECT body_jcs,body_digest FROM peer_pairing_manager WHERE principal_id=?').raw().get(actor.principal_id);
  if(prior)need(prior[0]===canonical(actor)&&prior[1]===digest(actor),'management_identity_conflict');
  else need(db.prepare('INSERT INTO peer_pairing_manager VALUES(?,?,?,?,?)').run(actor.principal_id,actor.issuer_id,actor.issuer_epoch,canonical(actor),digest(actor)).changes===1,'storage');
 }
 const authority={current(principal){
  source();const grant=checked(management.current(principal,fixedIssuer));
  need(grant.issuer_id===fixedIssuer.issuer_id&&grant.issuer_epoch===fixedIssuer.issuer_epoch&&Array.isArray(grant.permissions),'management_denied');return grant;
 }};
 return peerPairingStore({db,service,bootId,time,authenticated,authority,firstPairing:firstPairingEnrollment({db,service,issuer:fixedIssuer,remember}),
  writer(work){live();need(!db.inTransaction,'outer_transaction');return db.transaction(work).immediate();},
  issuerIdentity:()=>[fixedIssuer.issuer_id,fixedIssuer.issuer_epoch],
  humanPrincipal(id){const row=db.prepare('SELECT body_jcs,body_digest FROM peer_pairing_manager WHERE principal_id=?').raw().get(id);if(!row)return false;
   const actor=JSON.parse(row[0]);return row[1]===digest(actor)&&actor.principal_kind==='human'&&actor.issuer_id===fixedIssuer.issuer_id&&actor.issuer_epoch===fixedIssuer.issuer_epoch;}
 });
}
