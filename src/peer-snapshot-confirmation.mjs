import {digest,same} from './runtime/auth-clock-value.mjs';
const need=(ok,why)=>{if(!ok)throw Object.assign(new Error('snapshot_confirmation_'+why),{code:'snapshot_confirmation_'+why});};
// 发送只使用本机已提交回执。查询结果是发布端当前ACK，不授予来源年龄或运行资格。
export function createPeerSnapshotConfirmation({ingest,receiver,client}){
 function bound(bindingId,connection,receipt){const attached=client.checkAttached(connection,bindingId),{connection_generation:current,...currentBinding}=attached.attached.binding,{connection_generation:original,...originalBinding}=receipt.delivery_binding;need(receipt.state==='committed'&&same(currentBinding,originalBinding)&&same(attached.ready.channel.owner,receipt.owner),'connection');return attached;}
 async function query(bindingId,connection,applyId){const receipt=ingest.confirmationReceipt(applyId),attached=bound(bindingId,connection,receipt);
  const view=(await client.get(bindingId,receipt.delivery_binding.subscription_id)).view;need(same(bound(bindingId,connection,receipt),attached)&&same(ingest.confirmationReceipt(applyId),receipt),'changed');
  need(same(view.binding,attached.attached.binding)&&same(view.owner,receipt.owner)&&same(view.snapshot_ref,receipt.snapshot_ref)&&view.purpose==='canonical_trigger','binding');
  const marker=digest({entry_kind:'snapshot.baseline',payload:receipt.snapshot_ref}).slice(7);
  if(view.state==='snapshot_ready'){need(view.ack_cursor==='0'&&view.ack_payload_digest==='0'.repeat(64),'ack');return {state:'pending',apply_id:applyId};}
  need(view.state==='catching_up'&&view.ack_cursor===receipt.snapshot_ref.snapshot_cursor&&view.ack_payload_digest===marker,'ack');
  return {state:'confirmed',apply_id:applyId,view};
 }
 return Object.freeze({query,async send(bindingId,connection,handle){const receipt=ingest.lookup(handle),attached=bound(bindingId,connection,receipt);
  await client.snapshotApplied(bindingId,connection,receipt.snapshot_ref,()=>{const current=receiver.current(handle);need(same(current.subscription.binding,attached.attached.binding)&&same(current.subscription.owner,receipt.owner)&&same(current.subscription.snapshot_ref,receipt.snapshot_ref)&&same(ingest.confirmationReceipt(receipt.apply_id),receipt),'changed');});
  return {state:'sent',apply_id:receipt.apply_id};
 }});
}
