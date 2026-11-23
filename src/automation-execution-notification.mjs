import {automationExecutionCommit} from './automation-execution-commit.mjs';
import {canonical,digest,same} from './runtime/auth-clock-value.mjs';
import {pairingUuid} from './runtime/peer-pairing-value.mjs';
import {startOutboxDelivery} from './runtime/session-denial-delivery.mjs';
import {need} from './automation-publication.mjs';

export function executionNotificationOutbox({db,scope,consumerId,isOpen}){
 pairingUuid(consumerId);
 const commits=automationExecutionCommit(db);
 const pending=db.prepare(`SELECT c.rowid,c.execution_id FROM automation_execution_commit c
  WHERE c.rowid>? AND NOT EXISTS(SELECT 1 FROM automation_execution_notification_receipt r
   WHERE r.consumer_id=? AND r.execution_id=c.execution_id) ORDER BY c.rowid LIMIT ?`).raw().safeIntegers();
 const saved=db.prepare(`SELECT body_jcs FROM automation_execution_notification_receipt
  WHERE consumer_id=? AND execution_id=?`).pluck();
 const insert=db.prepare('INSERT INTO automation_execution_notification_receipt VALUES(?,?,?,?,?,?)');
 const writer=work=>{
  need(isOpen()&&db.open&&!db.inTransaction,'execution_notification_unavailable');
  return db.transaction(work).immediate();
 };
 function load(executionId){
  const committed=commits.read(executionId);
  need(committed&&same(committed.body.execution_ref.scope,scope),'execution_notification_missing');
  const {notification:event}=committed;
  const ack={format:'leafseamer.execution-notification-ack/1',scope,consumer_id:consumerId,
   event_id:event.event_id,payload_digest:digest(event)};
  const previous=saved.get(consumerId,executionId);
  need(previous===undefined||previous===canonical(ack),'execution_notification_integrity');
  return {event,ack,acknowledged:previous!==undefined};
 }
 return Object.freeze({scan({after,limit}){return writer(()=>{
  const rows=pending.all(after??0n,consumerId,limit+1);
  return {items:rows.slice(0,limit).map(([,id])=>id),next:rows.length>limit?rows[limit-1][0]:null};
 });},read(executionId){return writer(()=>load(executionId));},acknowledge(executionId,receipt){return writer(()=>{
  const {event,ack,acknowledged}=load(executionId);
  need(same(receipt,ack),'execution_notification_ack');
  if(acknowledged)return 'acknowledged';
  need(insert.run(consumerId,executionId,event.event_id,ack.payload_digest,event.commit_digest,canonical(ack)).changes===1,
   'execution_notification_storage');
  return 'delivered';
 });}});
}

export function startExecutionNotificationDelivery(outbox,deliver){
 const lifetime=new AbortController();
 let pending=null;
 const relay=startOutboxDelivery({scan:outbox.scan,async deliver(id){
  if(pending)throw new Error('execution_notification_in_flight');
  const {event,ack,acknowledged}=outbox.read(id);
  if(acknowledged)return 'acknowledged';
  const signal=AbortSignal.any([lifetime.signal,AbortSignal.timeout(5000)]);
  signal.throwIfAborted();
  const task=Promise.resolve().then(()=>{signal.throwIfAborted();return deliver(structuredClone({event,ack}),signal);});
  pending=task;
  const clear=()=>{pending=null;};
  void task.then(clear,clear);
  let abort;
  try{
   const cancelled=new Promise((_,reject)=>{
    abort=()=>reject(signal.reason);
    signal.addEventListener('abort',abort,{once:true});
   });
   const receipt=await Promise.race([task,cancelled]);
   signal.throwIfAborted();
   return outbox.acknowledge(id,receipt);
  }finally{signal.removeEventListener('abort',abort);}
 }});
 return Object.freeze({status:relay.status,close(){lifetime.abort();return relay.close();}});
}
