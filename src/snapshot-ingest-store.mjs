import crypto from 'node:crypto';
import {canonical,digest,same} from './runtime/auth-clock-value.mjs';
const need=(ok,why)=>{if(!ok)throw Object.assign(new Error('snapshot_ingest_'+why),{code:'snapshot_ingest_'+why});};
const put=value=>need(value.changes===1,'storage'),max=(a,b)=>BigInt(a)>BigInt(b)?a:b;
const unpack=(body,hash)=>{const value=JSON.parse(body);need(canonical(value)===body&&digest(value)===hash,'integrity');return value;};
const lineage=key=>digest([key.authority_service_id,key.database_instance_id,key.registration_id]);
const ref=value=>({source_key:value.source_key,revision:value.revision,digest:digest(value)});
// 唯一写入输入为本机已核全页的句柄；快照建立拒绝基线，不触发规则或自动恢复来源。
export function snapshotIngestStore({db,service,bootId,isOpen,receiver}){
 const live=()=>need(isOpen()&&db.open&&service.schema_revision==='4','unavailable');
 const writer=work=>{live();need(!db.inTransaction,'outer_transaction');return db.transaction(work).immediate();};
 function original(owner,subscription,snapshot){const row=db.prepare('SELECT body_jcs,body_digest,receipt_jcs,receipt_digest FROM snapshot_ingest_apply WHERE owner_key=? AND subscription_id=? AND snapshot_id=?').raw().get(owner,subscription,snapshot);return row?{body:unpack(row[0],row[1]),receipt:unpack(row[2],row[3])}:null;}
 function currentSource(key){const row=db.prepare('SELECT r.body_jcs,r.body_digest FROM ingest_source_revision r JOIN ingest_source_head h USING(source_key_digest,revision) WHERE source_key_digest=?').raw().get(digest(key));return row?unpack(...row):null;}
 function sourceRevision(value,apply){
  const key=digest(value.source_key),revision=Number(value.revision);need(Buffer.byteLength(canonical(value))<=262144,'capacity');put(db.prepare('INSERT INTO ingest_source_revision VALUES(?,?,?,?,?,?)').run(key,revision,lineage(value.source_key),apply,canonical(value),digest(value)));
  put(revision===1?db.prepare('INSERT INTO ingest_source_head VALUES(?,1)').run(key):db.prepare('UPDATE ingest_source_head SET revision=? WHERE source_key_digest=? AND revision=?').run(revision,key,revision-1));
 }
 function bound(value){const {subscription:s}=value;need(s.owner.kind==='service'&&s.owner.consumer_service_id===service.service_id&&s.owner.consumer_database_instance_id===service.database_instance_id&&s.purpose==='canonical_trigger','owner');return value;}
 return Object.freeze({
  apply(handle){return writer(()=>{
   const value=bound(receiver.currentInTransaction(handle)),{subscription,context,projection}=value,owner=digest(subscription.owner),snapshot=subscription.snapshot_ref;
   const originalBody={subscription,context,projection},old=original(owner,subscription.binding.subscription_id,snapshot.snapshot_id);
   if(old){need(same(old.receipt.snapshot_ref,snapshot)&&same(old.body.projection,projection),'conflict');return old.receipt;}
   const slot=digest([subscription.binding.publisher_service_id,subscription.binding.publisher_database_instance_id,subscription.owner.consumer_id]),head=db.prepare('SELECT generation FROM snapshot_projection_head WHERE slot_key=?').pluck().get(slot),generation=(head??0)+1;
   const apply_id=crypto.randomUUID(),sourceStates=[];
   need(projection.records.length>=1&&projection.records.length<=32&&projection.source_watermarks.length===projection.records.length,'coverage');
   for(const record of projection.records){const incoming=record.view,key=incoming.source_key,prior=currentSource(key),baseline=incoming.contiguous_sequence;
    if(prior){need(same(prior.binding_ref,incoming.binding_ref)&&prior.state!=='closed','source_closed_or_changed');need(BigInt(baseline)>=BigInt(prior.highest_seen_sequence)&&BigInt(baseline)>=BigInt(prior.reject_floor),'baseline_regression');}
    const gaps=new Map((prior?.gaps??[]).map(g=>[g.gap_id,g])),history=new Map((prior?.gap_history??[]).map(g=>[digest(g),g]));
    for(const gap of incoming.gaps){if(!gaps.has(gap.gap_id))gaps.set(gap.gap_id,structuredClone(gap));history.set(digest(gap),structuredClone(gap));}
    need(gaps.size<=256&&history.size<=1024,'gap_capacity');
    const superseded=db.prepare("SELECT r.body_jcs,r.body_digest FROM ingest_source_revision r JOIN ingest_source_head h USING(source_key_digest,revision) WHERE lineage_key=? AND source_key_digest<>? AND json_extract(r.body_jcs,'$.state')<>'closed'").raw().all(lineage(key),digest(key));need(superseded.length<=1,'lineage');
    for(const row of superseded){const previous=unpack(...row);sourceStates.push({...previous,revision:String(Number(previous.revision)+1),state:'closed',closed_by_snapshot:snapshot});}
    sourceStates.push({format:'leafseamer-ingest-source/1',source_key:key,binding_ref:incoming.binding_ref,revision:String(Number(prior?.revision??'0')+1),state:incoming.state==='closed'?'closed':prior?.state==='quarantined'?'quarantined':incoming.state==='synchronized'?'baseline_required':incoming.state,
     contiguous_sequence:baseline,highest_seen_sequence:prior?max(incoming.highest_seen_sequence,prior.highest_seen_sequence):incoming.highest_seen_sequence,reject_floor:prior?max(baseline,prior.reject_floor):baseline,
     baseline_ref:{apply_id,snapshot_ref:snapshot},publisher_baseline_ref:incoming.baseline_ref,gaps:[...gaps.values()],gap_history:[...history.values()],closed_by_snapshot:incoming.state==='closed'?snapshot:null});
   }
   const receipt={format:'leafseamer-snapshot-apply/1',apply_id,scope:{authority_service_id:service.service_id,database_instance_id:service.database_instance_id},service_boot_id:bootId,slot_key:slot,generation:String(generation),owner:subscription.owner,
    delivery_binding:subscription.binding,snapshot_ref:snapshot,source_refs:sourceStates.map(ref),state:'committed'};
   const body=canonical(originalBody),stored=Buffer.byteLength(body);need(stored<=9437184&&generation<=65536&&sourceStates.every(s=>Number(s.revision)<=65536),'capacity');
   need(db.prepare('SELECT count(*) FROM snapshot_ingest_apply').pluck().get()<1024&&db.prepare('SELECT coalesce(sum(stored_bytes),0) FROM snapshot_ingest_apply').pluck().get()+stored<=67108864&&db.prepare('SELECT count(*) FROM ingest_source_revision').pluck().get()+sourceStates.length<=16384,'capacity');
   put(db.prepare('INSERT INTO snapshot_ingest_apply VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)').run(apply_id,slot,generation,owner,subscription.binding.subscription_id,snapshot.snapshot_id,snapshot.manifest_digest,bootId,body,digest(originalBody),stored,sourceStates.length,canonical(receipt),digest(receipt)));
   for(const source of sourceStates)sourceRevision(source,apply_id);
   put(head?db.prepare('UPDATE snapshot_projection_head SET generation=?,apply_id=? WHERE slot_key=? AND generation=?').run(generation,apply_id,slot,head):db.prepare('INSERT INTO snapshot_projection_head VALUES(?,1,?)').run(slot,apply_id));
   const notice={format:'leafseamer-snapshot-adopted/1',apply_id,receipt_digest:digest(receipt),snapshot_ref:snapshot,generation:String(generation),acknowledged:false};
   for(const table of ['snapshot_ingest_audit','snapshot_ingest_outbox'])put(db.prepare('INSERT INTO '+table+' VALUES(?,?,?)').run(apply_id,canonical(notice),digest(notice)));
   put(db.prepare('INSERT INTO snapshot_ingest_seal VALUES(?,?)').run(apply_id,digest(receipt)));need(same(bound(receiver.currentInTransaction(handle)),value),'changed');return receipt;
  });},
  lookup(handle){return writer(()=>{const value=bound(receiver.currentInTransaction(handle)),s=value.subscription,found=original(digest(s.owner),s.binding.subscription_id,s.snapshot_ref.snapshot_id);need(found&&same(found.receipt.snapshot_ref,s.snapshot_ref),'missing');return found.receipt;});},
  // 本地历史查原只返回已提交事实；不证明远端当前授权或ACK资格。
  find({owner,subscription_id,snapshot_ref}){return writer(()=>{need(owner?.kind==='service'&&owner.consumer_service_id===service.service_id&&owner.consumer_database_instance_id===service.database_instance_id&&typeof subscription_id==='string'&&snapshot_ref,'owner');const found=original(digest(owner),subscription_id,snapshot_ref.snapshot_id);need(found&&same(found.receipt.snapshot_ref,snapshot_ref),'missing');return found.receipt;});},
  receipt(applyId){return writer(()=>{need(typeof applyId==='string','input');const row=db.prepare('SELECT receipt_jcs,receipt_digest FROM snapshot_ingest_apply WHERE apply_id=?').raw().get(applyId);need(row,'missing');return unpack(...row);});},
  confirmationReceipt(applyId){return writer(()=>{need(typeof applyId==='string','input');const row=db.prepare('SELECT a.receipt_jcs,a.receipt_digest FROM snapshot_ingest_apply a JOIN snapshot_projection_head h ON h.apply_id=a.apply_id AND h.slot_key=a.slot_key AND h.generation=a.generation JOIN snapshot_ingest_seal s ON s.apply_id=a.apply_id AND s.receipt_digest=a.receipt_digest WHERE a.apply_id=?').raw().get(applyId);need(row,'not_current');const receipt=unpack(...row);for(const source of receipt.source_refs){const current=currentSource(source.source_key);need(current&&same(ref(current),source),'not_current');}return receipt;});},
  sourceState(key){return writer(()=>{const value=currentSource(key);need(value,'missing');return value;});}
 });
}
