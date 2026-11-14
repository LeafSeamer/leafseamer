import {canonical,digest,same} from './runtime/auth-clock-value.mjs';
import {clockInteger} from './runtime/native-clock.mjs';
const need=(value,reason)=>{if(!value)throw new Error('peer_identity_watermark_'+reason);};
const id=value=>typeof value==='string'&&/^[A-Za-z0-9_.:-]{1,128}$(?![\s\S])/.test(value);
const hash=value=>typeof value==='string'&&/^sha256:[0-9a-f]{64}$(?![\s\S])/.test(value);
const keys=(value,names)=>value&&Object.keys(value).sort().join(',')===names.split(',').sort().join(',');
// 仅供已核验接收器装配；这里保存已见水位，不签发身份，也不是撤销传播确认。
export function peerIdentityWatermarks({db,service,bootId,isOpen}){
 const live=()=>need(isOpen()&&db.open,'closed');
 const issuer=value=>{need(keys(value,'service_id,issuer_id,issuer_epoch')&&Object.values(value).every(id)&&value.service_id!==service.service_id,'issuer');return value;};
 function latest(value){
  live();issuer(value);
  const row=db.prepare('SELECT revision,service_boot_id,body_jcs,body_digest FROM peer_identity_watermark WHERE issuer_service_id=? AND issuer_id=? AND issuer_epoch=? ORDER BY revision DESC LIMIT 1').raw().get(value.service_id,value.issuer_id,value.issuer_epoch);
  if(!row)return null;
  const body=JSON.parse(row[2]);need(canonical(body)===row[2]&&digest(body)===row[3]&&same(body.issuer,value)&&body.local_boot_id===row[1]&&
   body.scope.authority_service_id===service.service_id&&body.scope.database_instance_id===service.database_instance_id&&body.revision.padStart(20,'0')===row[0],'integrity');
  need(clockInteger(body.revision)>0n,'integrity');return body;
 }
 return Object.freeze({
  current(value){return latest(value)?.revision??'1';},
  observe(value){
   live();need(!db.inTransaction,'transaction');
   need(keys(value,'issuer,revision,snapshot_id,snapshot_digest,issuer_boot_id,pairing_ref'),'input');issuer(value.issuer);
   need(clockInteger(value.revision)>0n&&id(value.snapshot_id)&&hash(value.snapshot_digest)&&id(value.issuer_boot_id)&&
    keys(value.pairing_ref,'id,revision,digest')&&id(value.pairing_ref.id)&&clockInteger(value.pairing_ref.revision)>0n&&hash(value.pairing_ref.digest),'input');
   const fixed=JSON.parse(canonical(value));
   return db.transaction(()=>{
    const old=latest(fixed.issuer);need(!old||clockInteger(fixed.revision)>=clockInteger(old.revision),'regression');
    if(old?.revision===fixed.revision)return old.revision;
    need(db.prepare('SELECT count(*) FROM peer_identity_watermark').pluck().get()<16384,'capacity');
    const body={format:'received-issuer-watermark/1',scope:{authority_service_id:service.service_id,database_instance_id:service.database_instance_id},local_boot_id:bootId,...fixed};
    need(db.prepare('INSERT INTO peer_identity_watermark VALUES(?,?,?,?,?,?,?)').run(fixed.issuer.service_id,fixed.issuer.issuer_id,fixed.issuer.issuer_epoch,
     fixed.revision.padStart(20,'0'),bootId,canonical(body),digest(body)).changes===1,'write');
    need(same(latest(fixed.issuer),body),'integrity');return fixed.revision;
   }).immediate();
  }
 });
}
