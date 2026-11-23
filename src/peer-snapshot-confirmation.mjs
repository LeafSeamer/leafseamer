import {digest,same} from './runtime/auth-clock-value.mjs';
import {clockInteger} from './runtime/native-clock.mjs';
const need=(ok,why)=>{if(!ok)throw Object.assign(new Error('snapshot_confirmation_'+why),{code:'snapshot_confirmation_'+why});};
// 发送只使用本机已提交回执。查询结果是发布端当前ACK，不授予来源年龄或运行资格。
export function createPeerSnapshotConfirmation({ingest,receiver,client,reconcile=null,clock=null}){
 const active=new Map();
 function matching(view,receipt){const {connection_generation:current,...binding}=view.binding,{connection_generation:original,...expected}=receipt.delivery_binding;need(same(binding,expected)&&same(view.owner,receipt.owner)&&same(view.snapshot_ref,receipt.snapshot_ref)&&view.purpose==='canonical_trigger','binding');}
 function confirmed(view,receipt){
  matching(view,receipt);
  const marker=digest({entry_kind:'snapshot.baseline',payload:receipt.snapshot_ref}).slice(7);
  need(['catching_up','live'].includes(view.state)&&(view.ack_cursor===receipt.snapshot_ref.snapshot_cursor
   ? view.ack_payload_digest===marker
   : reconcile?.confirmed(receipt,view.ack_cursor,view.ack_payload_digest)),'ack');
  if(view.state==='live')need(reconcile?.ready(receipt,view.ack_cursor,view.ack_payload_digest,view.binding.connection_generation),'ack');
  return {cursor:view.ack_cursor,payload_digest:view.ack_payload_digest,ack_kind:'durable_ingest'};
 }
 const onReconcile=(bindingId,receipt)=>reconcile?async(frame,check)=>{
  await reconcile.acceptFromPeer(receipt,frame,check,reference=>client.sourceTime(bindingId,reference),
   reference=>client.referenceBundle(bindingId,reference,receipt.delivery_binding.publisher_service_id),
   {readSourceInTransaction:ingest.sourceStateInTransaction,prepareSourceTransport});
  return ()=>need(reconcile.confirmed(receipt,frame.cursor,frame.payload_digest),'ack');
 }:null;
 function bound(bindingId,connection,receipt,inside=false){const attached=inside?client.checkAttachedInTransaction(connection,bindingId):client.checkAttached(connection,bindingId),{connection_generation:current,...currentBinding}=attached.attached.binding,{connection_generation:original,...originalBinding}=receipt.delivery_binding;need(receipt.state==='committed'&&same(currentBinding,originalBinding)&&same(attached.ready.channel.owner,receipt.owner),'connection');return attached;}
 function retain(bindingId,connection,receipt,attached){
  const generation=BigInt(attached.attached.binding.connection_generation),previous=active.get(receipt.apply_id);
  if(!previous||generation>previous.generation)active.set(receipt.apply_id,{bindingId,connection,receipt,attached,generation});
 }
 async function query(bindingId,connection,applyId){const receipt=ingest.confirmationReceipt(applyId),attached=bound(bindingId,connection,receipt);
  const view=(await client.get(bindingId,receipt.delivery_binding.subscription_id)).view;need(same(bound(bindingId,connection,receipt),attached)&&same(ingest.confirmationReceipt(applyId),receipt),'changed');
  need(same(view.binding,attached.attached.binding)&&same(view.owner,receipt.owner)&&same(view.snapshot_ref,receipt.snapshot_ref)&&view.purpose==='canonical_trigger','binding');
  if(view.state==='snapshot_ready'){need(view.ack_cursor==='0'&&view.ack_payload_digest==='0'.repeat(64),'ack');return {state:'pending',apply_id:applyId};}
  confirmed(view,receipt);
  const result={state:'confirmed',apply_id:applyId,view};
  if(reconcile)result.transport_ready=view.state==='live';
  return result;
 }
 async function prepareSourceTransport(applyId){
  const entry=active.get(applyId);
  const requireActive=()=>need(active.get(applyId)===entry,'changed');
  function unavailable(){
   requireActive();active.delete(applyId);
   return {observation:{state:'unavailable'},assertCurrentInTransaction:()=>need(!active.has(applyId),'changed')};
  }
  if(!entry||client.isClosed(entry.connection))return unavailable();
  const {bindingId,connection,receipt,attached}=entry;
  const started=clock.pairingSample(),start=clockInteger(started.tick_ns);
  let result;
  try{result=await query(bindingId,connection,applyId);}
  catch(error){if(client.isClosed(connection))return unavailable();throw error;}
  function checkTime(){
   const current=clock.pairingSample(),tick=clockInteger(current.tick_ns);
   need(same(started.runtime_domain_ref,current.runtime_domain_ref)&&tick>=start&&tick<start+2000000000n,'observation_expired');
  }
  requireActive();checkTime();
  const view=result.view,state=result.transport_ready?'ready':(view?.state??'pending');
  const observation={state,connection_generation:attached.attached.binding.connection_generation,
   ...(view&&{ack_cursor:view.ack_cursor,ack_payload_digest:view.ack_payload_digest})};
  return {observation,assertCurrentInTransaction(){
   requireActive();
   need(same(bound(bindingId,connection,receipt,true),attached),'changed');
   if(view)confirmed(view,receipt);
   checkTime();
  }};
 }
 return Object.freeze({query,prepareSourceTransport,...(reconcile?.prepareAge&&{prepareEventAge(input){
  return reconcile.prepareAge(input,{readSourceInTransaction:ingest.sourceStateInTransaction,prepareSourceTransport});
 }}),async resume(bindingId,applyId,options={}){
  const receipt=ingest.confirmationReceipt(applyId),current=()=>need(same(ingest.confirmationReceipt(applyId),receipt),'changed');
  const {view}=await client.get(bindingId,receipt.delivery_binding.subscription_id);current();const resume=confirmed(view,receipt);
  const connection=await client.attach(bindingId,view,{...options,resume,beforeSend:current,onReconcile:onReconcile(bindingId,receipt)});
  try{current();const attached=bound(bindingId,connection,receipt);if(reconcile&&clock)retain(bindingId,connection,receipt,attached);return connection;}catch(error){connection.close();throw error;}
 },async send(bindingId,connection,handle){const receipt=ingest.lookup(handle),attached=bound(bindingId,connection,receipt);
  await client.snapshotApplied(bindingId,connection,receipt.snapshot_ref,()=>{const current=receiver.current(handle);need(same(current.subscription.binding,attached.attached.binding)&&same(current.subscription.owner,receipt.owner)&&same(current.subscription.snapshot_ref,receipt.snapshot_ref)&&same(ingest.confirmationReceipt(receipt.apply_id),receipt),'changed');},onReconcile(bindingId,receipt));
  if(reconcile&&clock)retain(bindingId,connection,receipt,attached);
  return {state:'sent',apply_id:receipt.apply_id};
 }});
}
