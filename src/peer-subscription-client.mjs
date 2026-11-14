import {createHttpAdapter} from './sdk/http.mjs';
import {contractSelection as selection} from './sdk/index.mjs';
import crypto from 'node:crypto';
import {createDecoder,profile} from './sdk/runtime/panel-wire/decoder.mjs';
import {same} from './runtime/auth-clock-value.mjs';
export function createPeerSubscriptionClient({transport}){
 const adapter=createHttpAdapter(selection),connections=new WeakMap();
 const snapshotProfile=profile({max_message_bytes:8388608,max_depth:48,max_value_nodes:1048576,max_object_members:65536,max_string_utf8_bytes:8388608,parse_deadline_ms:5000});
 async function send(bindingId,method,path,input=null){
  const body=Buffer.from(input===null?'':JSON.stringify(input)),headers=[['x-leafseamer-contract',selection.contract_id],['x-leafseamer-contract-digest',selection.contract_digest],
   ...(input===null?[]:[['content-type','application/json'],['content-length',String(body.length)]])];
  const request=adapter.prepareRequest('controller',method,path,headers);if(input!==null)await adapter.decodeRequest(body,request);
  const reply=await transport.requestSubscription(bindingId,{method,path,headers,body}),response=adapter.prepareResponse(request,String(reply.status),reply.headers);
  const result=(await adapter.decodeResponse(reply.body,response,['internalGetSnapshot','internalGetSnapshotPage'].includes(request.operation_id)?{profile:snapshotProfile}:{})).value;
  if(reply.status>=400)throw Object.assign(new Error(result.code),{code:result.code,status:reply.status});
  return {status:reply.status,view:result};
 }
 const route=key=>{if(typeof key!=='string'||!/^[A-Za-z0-9_-]{1,128}$(?![\s\S])/.test(key))throw new Error('subscription_id');return '/internal/subscriptions/'+key;};
 const generation=value=>{if(typeof value!=='string'||!/^(0|[1-9][0-9]{0,19})$/.test(value))throw Error('subscription_generation');return value;};
 async function attach(bindingId,view,{replace_existing=false,connection_attempt_id=crypto.randomUUID()}={}){
  const decoder=createDecoder(selection),incoming=decoder.websocket('controller','internalOpenEvents','server'),outgoing=decoder.websocket('controller','internalOpenEvents','client');
  const request={schema_version:'event-channel/0.1.0-draft.1',type:'client.attach',subscription_id:view.binding.subscription_id,owner:view.owner,stream_id:view.binding.stream_id,stream_epoch:view.binding.stream_epoch,scope_digest:view.binding.scope_digest,authorization_revision:view.binding.authorization_revision,expected_connection_generation:view.binding.connection_generation,replace_existing,resume:null,connection_attempt_id};
  const body=Buffer.from(JSON.stringify(request));await decoder.decode(body,outgoing);let ready=null,settled=false,resolve,reject,channel;
  const result=new Promise((yes,no)=>{resolve=yes;reject=no;});result.catch(()=>{});const timer=setTimeout(()=>{reject(Error('subscription_attach_timeout'));channel?.close();},5000);
  try{channel=await transport.openSubscriptionChannel(bindingId,{protocol:'leafseamer.contract.'+selection.contract_id+'.'+selection.contract_digest.slice(7),
   async onFrame(bytes){const value=(await decoder.decode(bytes,incoming)).value;
    if(!ready){if(value.type!=='contract.ready'||!same(value.channel.selection,selection)||!same(value.channel.owner,view.owner)||value.channel.publisher_service_id!==view.binding.publisher_service_id||value.channel.publisher_database_instance_id!==view.binding.publisher_database_instance_id||value.channel.transport!=='service_mtls'||value.channel.subscription_id!==null)throw Error('subscription_ready');ready=value;return;}
    if(value.type!=='server.attached'||settled)throw Error('subscription_frame');const expected={...view.binding,connection_generation:String(BigInt(view.binding.connection_generation)+1n)};if(!same(value.binding,expected))throw Error('subscription_binding');settled=true;clearTimeout(timer);resolve({ready,attached:value});
   },onClose:error=>{clearTimeout(timer);if(!settled)reject(error??Error('subscription_closed'));}
  });channel.send(body);const value=await result;channel.check();const handle=Object.freeze({...value,connection_attempt_id,close:()=>channel.close()});connections.set(handle,{bindingId,channel,value:structuredClone(value)});return handle;
  }catch(error){clearTimeout(timer);channel?.close();throw error;}
 }
 function checkAttached(handle,bindingId,inside=false){const entry=connections.get(handle);if(!entry||entry.bindingId!==bindingId)throw Error('subscription_connection');if(inside)entry.channel.checkInTransaction();else entry.channel.check();return structuredClone(entry.value);}
 return Object.freeze({checkAttached,checkAttachedInTransaction:(handle,bindingId)=>checkAttached(handle,bindingId,true),create:(bindingId,input)=>send(bindingId,'POST','/internal/subscriptions',input),get:(bindingId,key)=>send(bindingId,'GET',route(key)),close:(bindingId,key,revision)=>send(bindingId,'DELETE',route(key)+'?expected_revision='+encodeURIComponent(revision)),attach,
  async snapshotApplied(bindingId,handle,snapshot,beforeSend){const attached=checkAttached(handle,bindingId),decoder=createDecoder(selection),outgoing=decoder.websocket('controller','internalOpenEvents','client'),body=Buffer.from(JSON.stringify({schema_version:'event-channel/0.1.0-draft.1',type:'client.snapshot.applied',binding:attached.attached.binding,snapshot}));await decoder.decode(body,outgoing);if(typeof beforeSend!=='function')throw Error('subscription_confirmation_gate');checkAttached(handle,bindingId);beforeSend();connections.get(handle).channel.send(body);},
  attempt:(bindingId,key,attemptId)=>send(bindingId,'GET',route(key)+'/connections/'+encodeURIComponent(attemptId)),
  context:(bindingId,key,input)=>send(bindingId,'POST',route(key)+'/snapshot-context',input),
  snapshot:(bindingId,key,expected)=>send(bindingId,'GET',route(key)+'/snapshot?expected_connection_generation='+generation(expected)),
  page:(bindingId,key,snapshotId,index,expected)=>send(bindingId,'GET',route(key)+'/snapshots/'+encodeURIComponent(snapshotId)+'/pages/'+encodeURIComponent(index)+'?expected_connection_generation='+generation(expected))
 });
}
