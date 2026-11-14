import crypto from 'node:crypto';
import {same,digest} from './runtime/auth-clock-value.mjs';
import {clockInteger} from './runtime/native-clock.mjs';
import {deviceEventsReply,deviceEventSourceRef} from './runtime/peer-device-events.mjs';
import {validateEventTimeModel} from './sdk/event-time.mjs';
const need=(ok,code)=>{if(!ok)throw Object.assign(new Error('peer_event_'+code),{code:'peer_event_'+code});};
// 仅保存有界的原件读取句柄；不分配摄取身份，不推进来源前沿，也不发送durable_ingest。
export function createPeerEventReceiver({service,pairings,clock,transport}){
 const entries=new Map();let closed=false,pending=0;
 function current(handle,inside=false){
  const e=entries.get(handle);need(!closed&&e,'handle');
  try{const now=clock.pairingSample(),record=inside?pairings.requireActivatedInTransaction(e.value.pairing_ref):pairings.currentPending(e.value.pairing_ref.id);
   need(same(record,e.record)&&same(now.runtime_domain_ref,e.domain)&&clockInteger(now.tick_ns)>=e.started&&clockInteger(now.tick_ns)<e.deadline,'expired');return structuredClone(e.value);
  }catch(error){entries.delete(handle);throw error;}
 }
 return Object.freeze({
  async prepare({binding_id,scope,device_id,expected_source_ref=null,after_sequence='0',limit=32}){
   need(!closed&&pending<4,'capacity');const start=clock.pairingSample(),started=clockInteger(start.tick_ns);
   for(const [handle,e]of entries)if(started>=e.deadline||!same(start.runtime_domain_ref,e.domain))entries.delete(handle);
   need(entries.size+pending<64,'capacity');pending++;
   try{const record=pairings.currentPending(binding_id),request={format:'leafseamer.device-events-request/1',challenge_id:crypto.randomUUID(),scope:structuredClone(scope),device_id,expected_source_ref:structuredClone(expected_source_ref),after_sequence,limit};
    const reply=await transport.readDeviceEvents(binding_id,request);deviceEventsReply(reply,request,record,record.peer_service_id,service.service_id);
    validateEventTimeModel('SourceBinding',reply.bundle.registration.binding_ref);for(const item of reply.bundle.events)validateEventTimeModel('Origin',item.origin);
    const received=clock.pairingSample();need(same(received.runtime_domain_ref,start.runtime_domain_ref),'domain');
    const handle=Object.freeze({}),value={pairing_ref:{id:record.binding_id,revision:record.revision,digest:record.digest},request,reply,
     source_ref:deviceEventSourceRef(reply.bundle.registration),bundle_digest:digest(reply.bundle),
     read_observation:{runtime_domain_ref:structuredClone(start.runtime_domain_ref),started_tick_ns:start.tick_ns,received_tick_ns:received.tick_ns}};
    need(!closed,'closed');entries.set(handle,{record,started,deadline:started+2000000000n,domain:structuredClone(start.runtime_domain_ref),value});current(handle);return handle;
   }finally{pending--;}
  },
  current:handle=>current(handle),currentInTransaction:handle=>current(handle,true),
  release(handle){entries.delete(handle);},
  close(){closed=true;entries.clear();}
 });
}
