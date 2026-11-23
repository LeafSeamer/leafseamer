import crypto from 'node:crypto';
import {createClient,contractSelection} from './sdk/index.mjs';
import {clockInteger} from './runtime/native-clock.mjs';
import {bounded,same,digest} from './runtime/auth-clock-value.mjs';
const need=(ok,reason)=>{if(!ok)throw new Error('peer_identity_'+reason);};
const freeze=value=>{if(value&&typeof value==='object'){Object.values(value).forEach(freeze);Object.freeze(value);}return value;};
const utc=value=>{const time=Date.parse(value);need(Number.isSafeInteger(time)&&time>=0,'time');return BigInt(time)*1000000n;};

// 只证明当前远端身份。业务操作仍须与Seamer自身对象授权、撤销及请求上下文取交集。
export function createPeerIdentityReceiver({service,clock,pairings,transport,issuer,watermarks,denials}){
 need(watermarks&&typeof watermarks.current==='function'&&typeof watermarks.observe==='function','watermark_required');
 need(denials&&typeof denials.assertAllowed==='function','denial_required');
 const fixed=freeze(structuredClone(issuer)),handles=new WeakMap(),entries=new Set();let closed=false,pending=0;
 const forget=entry=>{handles.delete(entry.handle);entries.delete(entry);};
 const binding=(id,reference=null,revocationScope=null)=>{need(!closed,'closed');const record=reference?(revocationScope?pairings.identityForRevocationInTransaction(reference,revocationScope):pairings.requireActivatedInTransaction(reference)):pairings.currentPending(id),local=record.transcript.parties.find(p=>p.service_id===service.service_id),peer=record.transcript.parties.find(p=>p.service_id===fixed.service_id);
  need(record.state==='activated'&&record.peer_service_id===fixed.service_id&&local?.database_instance_id===service.database_instance_id&&local.service_kind==='seamer'&&
   peer?.issuer_id===fixed.issuer_id&&peer.issuer_epoch===fixed.issuer_epoch,'binding');return record;};
 const current=(handle,inTransaction=false,revocationScope=null)=>{
  const entry=handles.get(handle);need(entry,'handle');
  try{
   const now=clock.pairingSample(),reference=inTransaction?{id:entry.record.binding_id,revision:entry.record.revision,digest:entry.record.digest}:null;
   need(!closed&&same(binding(entry.binding_id,reference,revocationScope),entry.record)&&same(now.runtime_domain_ref,entry.domain),'changed');
   need(clockInteger(entry.value.snapshot.issuer_revision)>=clockInteger(watermarks.current(fixed)),'watermark');
   denials.assertAllowed(entry.value.snapshot);
   const tick=clockInteger(now.tick_ns);need(tick>=entry.started&&tick<entry.deadline,'expired');
   need(clockInteger(now.lower_ns)>=entry.notBefore&&clockInteger(now.upper_ns)<entry.expires,'expired');return entry.value;
  }catch(error){forget(entry);throw error;}
 };
 return Object.freeze({
  async prepare({binding_id,token,required_revision='1'}){
   need(!closed&&pending<16,'capacity');const record=binding(binding_id),start=clock.pairingSample(),started=clockInteger(start.tick_ns);
   for(const entry of entries)if(!same(entry.domain,start.runtime_domain_ref)||started>=entry.deadline)forget(entry);
   need(entries.size+pending<256,'capacity');pending++;
   try{
    const floor=clockInteger(watermarks.current(fixed)),requested=clockInteger(required_revision);required_revision=String(floor>requested?floor:requested);
    const challenge=crypto.randomUUID(),peer=record.transcript.parties.find(p=>p.service_id===fixed.service_id),deadline=bounded(started+2000000000n);
    const client=createClient('controller',contractSelection,packet=>transport.exchangeIntrospection(binding_id,packet));
    const response=await client.call('introspect',{headers:{'x-leafseamer-pairing':peer.binding_id},body:{token,audience:service.service_id,challenge_id:challenge,required_revision}});
    need(response.status==='200'&&response.body.service_id===fixed.service_id,'response');
    const data=response.body.data,snapshot=data.snapshot;need(data.active===true&&snapshot?.state==='active','inactive');
    need(snapshot.issuer_id===fixed.issuer_id&&snapshot.issuer_epoch===fixed.issuer_epoch&&snapshot.audience===service.service_id&&snapshot.challenge_id===challenge&&
     clockInteger(snapshot.issuer_revision)>=clockInteger(required_revision)&&snapshot.fresh_for_ms<=2000,'identity');
    if(snapshot.principal_kind==='human'){
     const recent=data.recent_authentication;need(recent&&recent.session_id===snapshot.session_id&&recent.session_revision===snapshot.session_revision&&utc(recent.not_before)<utc(recent.expires_at),'recent_binding');
    }else need(data.recent_authentication===null,'recent_binding');
    const handle=Object.freeze({}),fresh=bounded(started+BigInt(snapshot.fresh_for_ms)*1000000n),entry={handle,binding_id,record,started,deadline:fresh<deadline?fresh:deadline,
     domain:structuredClone(start.runtime_domain_ref),notBefore:utc(snapshot.validity.not_before),expires:utc(snapshot.validity.expires_at),
     value:freeze({issuer_service_id:fixed.service_id,issuer_boot_id:response.body.service_boot_id,
      token_id:/^v1\.([0-9a-f-]{36})\.[A-Za-z0-9_-]{43}$(?![\s\S])/.exec(token)?.[1]??null,
      token_binding_digest:'sha256:'+crypto.createHash('sha256').update('LeafSeamer/received-token/1\0'+token).digest('hex'),snapshot:structuredClone(snapshot),recent_authentication:structuredClone(data.recent_authentication)})};
    need(entry.notBefore<entry.expires,'time');handles.set(handle,entry);entries.add(entry);current(handle);
    try{need(watermarks.observe({issuer:fixed,revision:snapshot.issuer_revision,snapshot_id:snapshot.snapshot_id,snapshot_digest:digest(snapshot),issuer_boot_id:response.body.service_boot_id,
     pairing_ref:{id:record.binding_id,revision:record.revision,digest:record.digest}})===snapshot.issuer_revision,'watermark_commit');}
    catch(error){closed=true;for(const known of entries)forget(known);throw error;}
    current(handle);return handle;
   }finally{pending--;}
  },
  current:handle=>current(handle),
  currentInTransaction:(handle,revocationScope=null)=>current(handle,true,revocationScope),
  close(){closed=true;for(const entry of entries)forget(entry);}
 });
}
