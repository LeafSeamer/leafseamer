import crypto from 'node:crypto';
import {same} from './runtime/auth-clock-value.mjs';
import {clockInteger} from './runtime/native-clock.mjs';
import {deviceBindingsReply} from './runtime/peer-device-bindings.mjs';
import {validateAutomationModel} from './sdk/automation-program.mjs';
const need=(ok,code)=>{if(!ok)throw Object.assign(new Error('peer_binding_'+code),{code:'peer_binding_'+code});};
// 这是一次读取结果的短期本地句柄，不是远端来源的持续当前性租约。
export function createPeerBindingReceiver({service,pairings,clock,transport}){
 const entries=new Map();let closed=false,pending=0;
 const reference=record=>({id:record.binding_id,revision:record.revision,digest:record.digest});
 function assertCurrent(observation,inside){
  need(!closed,'closed');
  const {pairingRef,domain,started,deadline}=observation;
  const record=inside?pairings.requireActivatedInTransaction(pairingRef):pairings.currentPending(pairingRef.id);
  const now=clock.pairingSample(),tick=clockInteger(now.tick_ns);
  need(record.state==='activated'&&same(reference(record),pairingRef)&&same(now.runtime_domain_ref,domain)&&tick>=started&&tick<deadline,'expired');
 }
 function current(handle,inside=false){
  const e=entries.get(handle);need(!closed&&e,'handle');
  try{assertCurrent(e.observation,inside);return structuredClone(e.value);
  }catch(error){entries.delete(handle);throw error;}
 }
 async function receive({binding_id,scope,device_id,expected_registration_ref=null},onRead,retain){
   need(!closed&&pending<4,'capacity');const start=clock.pairingSample(),started=clockInteger(start.tick_ns);
   for(const [handle,{observation:e}]of entries)if(started>=e.deadline||!same(start.runtime_domain_ref,e.domain))entries.delete(handle);
   need(!retain||entries.size+pending<64,'capacity');pending++;
   try{const record=pairings.currentPending(binding_id),request={format:'leafseamer.device-bindings-request/1',challenge_id:crypto.randomUUID(),scope:structuredClone(scope),device_id,expected_registration_ref:structuredClone(expected_registration_ref)};
    const reply=await transport.readDeviceBindings(binding_id,request);deviceBindingsReply(reply,request,record,record.peer_service_id,service.service_id);
    for(const binding of reply.bundle.bindings)validateAutomationModel(binding.kind==='input'?'InputBinding':'ActionBinding',binding.value);
    validateAutomationModel('AliasTable',reply.bundle.alias_table);
    const pairingRef=reference(record),observation={pairingRef,started,deadline:started+2000000000n,domain:structuredClone(start.runtime_domain_ref)};
    assertCurrent(observation,false);
    return onRead({pairing_ref:pairingRef,request,reply,bundle_digest:reply.bundle_digest},observation);
   }finally{pending--;}
 }
 return Object.freeze({
  prepare(query){
   return receive(query,(value,observation)=>{
    const handle=Object.freeze({});entries.set(handle,{value,observation});return handle;
   },true);
  },
  prepareMatch(query,expected){
   const fixed=structuredClone(expected);
   return receive(query,(value,observation)=>{
    need(same(value.pairing_ref,fixed.pairing_ref)&&value.bundle_digest===fixed.bundle_digest,'changed');
    // 匹配后只保留本机时钟和配对引用，复核沿用原读取期限。
    let valid=true;
    return inside=>{
     need(valid,'expired');
     try{assertCurrent(observation,inside);}catch(error){valid=false;throw error;}
    };
   },false);
  },
  current:handle=>current(handle),currentInTransaction:handle=>current(handle,true),
  release(handle){entries.delete(handle);},
  close(){closed=true;entries.clear();}
 });
}
