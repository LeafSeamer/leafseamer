import fs from 'node:fs';import crypto from 'node:crypto';
import {same} from './runtime/auth-clock-value.mjs';
import {snapshotLeaseDeadline,checkSnapshotLease} from './snapshot-lease.mjs';
import {createSourceSnapshotContent} from './snapshot-content.mjs';
const graphBytes=fs.readFileSync(new URL('./sdk/contract/models.schema.json',import.meta.url)),graphIdentity={id:JSON.parse(graphBytes).$id,digest:crypto.createHash('sha256').update(graphBytes).digest('hex')};
const need=(ok,why)=>{if(!ok)throw Object.assign(new Error('peer_snapshot_'+why),{code:'peer_snapshot_'+why});};
// 下载结果是有界暂存句柄；持久采用、来源资格与ACK由后续同writer流程处理。
export function createPeerSnapshotReceiver({service,client,clock,pairings}){
 need(service&&typeof service.service_id==='string'&&typeof service.database_instance_id==='string','local_identity');
 const localService=structuredClone({service_id:service.service_id,database_instance_id:service.database_instance_id});
 const entries=new Map();let pending=0,closed=false;
 function currentEntry(e,inside=false){
  need(!closed,'closed');checkSnapshotLease(e.lease,clock.durationSample());
  const pair=inside?pairings.requireActivatedInTransaction(e.pairing_ref):pairings.currentPending(e.bindingId),attached=inside?client.checkAttachedInTransaction(e.connection,e.bindingId):client.checkAttached(e.connection,e.bindingId);
  need(same(pair,e.pair)&&same(attached,e.attached),'changed');return e;
 }
 function sweep(){for(const [key,e]of entries)try{currentEntry(e);}catch{entries.delete(key);}}
 return Object.freeze({
  async prepare(bindingId,connection){
   need(!closed&&pending<2,'capacity');sweep();need(entries.size+pending<8,'capacity');pending++;
   try{
    const attached=client.checkAttached(connection,bindingId),pair=pairings.currentPending(bindingId),key=attached.attached.binding.subscription_id;
    const subscription=(await client.get(bindingId,key)).view;need(subscription.state==='snapshot_ready'&&same(subscription.binding,attached.attached.binding)&&same(subscription.owner,attached.ready.channel.owner),'subscription');
    const publisher=pair.transcript.parties.find(p=>p.service_id===pair.peer_service_id),consumer=pair.transcript.parties.find(p=>p.service_id===localService.service_id);
    need(pair.state==='activated'&&subscription.binding.publisher_service_id===pair.peer_service_id&&publisher?.service_kind==='controller'&&publisher.database_instance_id===subscription.binding.publisher_database_instance_id,'publisher');
    need(subscription.owner.kind==='service'&&subscription.owner.consumer_service_id===localService.service_id&&subscription.owner.consumer_database_instance_id===localService.database_instance_id&&consumer?.service_kind==='seamer'&&consumer.database_instance_id===localService.database_instance_id&&subscription.owner.pairing_id===publisher.binding_id,'consumer');
    const challenge_id=crypto.randomUUID(),start=clock.durationSample(),context=(await client.context(bindingId,key,{challenge_id,expected_connection_generation:subscription.binding.connection_generation,snapshot_ref:subscription.snapshot_ref})).view,received=clock.durationSample();
    need(context.challenge_id===challenge_id&&context.publisher_service_boot_id===attached.ready.channel.service_boot_id,'challenge');
    const lease=snapshotLeaseDeadline(start,received,context.lease),e={bindingId,connection,pair,attached,lease,pairing_ref:{id:pair.binding_id,revision:pair.revision,digest:pair.digest}};currentEntry(e);
    const manifest=(await client.snapshot(bindingId,key,subscription.binding.connection_generation)).view;currentEntry(e);
    const content=createSourceSnapshotContent({subscription,context,manifest,graphIdentity});
    for(let index=0;index<manifest.page_count;index++){const response=(await client.page(bindingId,key,manifest.binding.snapshot_id,index,subscription.binding.connection_generation)).view;currentEntry(e);content.add(response);}
    const projection=content.finish();currentEntry(e);const handle=Object.freeze({});entries.set(handle,{...e,value:{subscription,context,projection,lease}});return handle;
   }finally{pending--;}
  },
  current(handle){const e=entries.get(handle);need(e,'handle');try{return structuredClone(currentEntry(e).value);}catch(error){entries.delete(handle);throw error;}},
  currentInTransaction(handle){const e=entries.get(handle);need(e,'handle');try{return structuredClone(currentEntry(e,true).value);}catch(error){entries.delete(handle);throw error;}},
  release(handle){entries.delete(handle);},close(){closed=true;entries.clear();}
 });
}
