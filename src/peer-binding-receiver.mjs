import crypto from 'node:crypto';
import {same,digest} from './runtime/auth-clock-value.mjs';
import {clockInteger} from './runtime/native-clock.mjs';
import {deviceBindingsReply} from './runtime/peer-device-bindings.mjs';
import {validateAutomationModel} from './sdk/automation-program.mjs';
const need=(ok,code)=>{if(!ok)throw Object.assign(new Error('peer_binding_'+code),{code:'peer_binding_'+code});};
// 这是一次读取结果的短期本地句柄，不是远端来源的持续当前性租约。
export function createPeerBindingReceiver({service,pairings,clock,transport}){
 const entries=new Map();let closed=false,pending=0;
 function current(handle,inside=false){
  const e=entries.get(handle);need(!closed&&e,'handle');
  try{const now=clock.pairingSample(),record=inside?pairings.requireActivatedInTransaction(e.value.pairing_ref):pairings.currentPending(e.value.pairing_ref.id);
   need(same(record,e.record)&&same(now.runtime_domain_ref,e.domain)&&clockInteger(now.tick_ns)>=e.started&&clockInteger(now.tick_ns)<e.deadline,'expired');return structuredClone(e.value);
  }catch(error){entries.delete(handle);throw error;}
 }
 return Object.freeze({
  async prepare({binding_id,scope,device_id,expected_registration_ref=null}){
   need(!closed&&pending<4,'capacity');const start=clock.pairingSample(),started=clockInteger(start.tick_ns);
   for(const [handle,e]of entries)if(started>=e.deadline||!same(start.runtime_domain_ref,e.domain))entries.delete(handle);
   need(entries.size+pending<64,'capacity');pending++;
   try{const record=pairings.currentPending(binding_id),request={format:'leafseamer.device-bindings-request/1',challenge_id:crypto.randomUUID(),scope:structuredClone(scope),device_id,expected_registration_ref:structuredClone(expected_registration_ref)};
    const reply=await transport.readDeviceBindings(binding_id,request);deviceBindingsReply(reply,request,record,record.peer_service_id,service.service_id);
    for(const binding of reply.bundle.bindings)validateAutomationModel(binding.kind==='input'?'InputBinding':'ActionBinding',binding.value);
    validateAutomationModel('AliasTable',reply.bundle.alias_table);
    const handle=Object.freeze({}),value={pairing_ref:{id:record.binding_id,revision:record.revision,digest:record.digest},request,reply,bundle_digest:digest(reply.bundle)};
    need(!closed,'closed');entries.set(handle,{record,started,deadline:started+2000000000n,domain:structuredClone(start.runtime_domain_ref),value});current(handle);return handle;
   }finally{pending--;}
  },
  current:handle=>current(handle),currentInTransaction:handle=>current(handle,true),
  release(handle){entries.delete(handle);},
  close(){closed=true;entries.clear();}
 });
}
