import {same} from './runtime/auth-clock-value.mjs';

const fail=code=>{throw Object.assign(new Error(code),{code});};
const activeStates=new Set(['building','snapshot_ready','catching_up','live']);

export function manageSourceSubscriptions({bindingId,client,receiver,ingest,confirmation}){
 const connections=new Map(),pending=new Set();
 let closed=false;

 function assertOpen(){if(closed)fail('managed_subscription_unavailable');}
 function checkView(view,request){
  if(view?.owner?.consumer_id!==request.consumer_id||view.purpose!=='canonical_trigger'||!same(view.scope,request.scope))
   fail('managed_subscription_binding');
 }
 function status(state,subscription,applyId=null,transportReady=false){
  return {state,subscription_id:subscription,apply_id:applyId,transport_ready:transportReady};
 }

 return Object.freeze({
  async connect(request,authorize){
   assertOpen();
   const {consumer_id:slot}=request;
   for(const [id,entry] of connections)if(client.isClosed(entry.connection))connections.delete(id);
   if(pending.has(slot)||pending.size>=2)fail('managed_subscription_busy');
   pending.add(slot);
   let connection;
   try{
    const assertInitial=await authorize();
    assertOpen();assertInitial();
    const existing=connections.get(slot);
    if(existing){
     if(!same(existing.request,request))fail('managed_subscription_conflict');
     const state=await confirmation.query(bindingId,existing.connection,existing.applyId);
     assertOpen();assertInitial();
     return status(state.state,existing.subscription,existing.applyId,state.transport_ready===true);
    }
    const {view}=await client.create(bindingId,{...request,purpose:'canonical_trigger'});
    assertOpen();assertInitial();checkView(view,request);
    const subscription=view.binding.subscription_id;
    if(!activeStates.has(view.state))fail('managed_subscription_ended');
    if(view.state==='building')return status('building',subscription);
    let receipt;
    if(view.state==='snapshot_ready'){
     connection=await client.attach(bindingId,view);
     assertOpen();assertInitial();
     const handle=await receiver.prepare(bindingId,connection);
     try{
      const assertAdoption=await authorize();
      const current=()=>{assertOpen();assertAdoption();};
      current();
      receipt=ingest.apply(handle);
      await confirmation.send(bindingId,connection,handle,current);
     }finally{receiver.release(handle);}
    }else{
     receipt=ingest.find({owner:view.owner,subscription_id:subscription,snapshot_ref:view.snapshot_ref});
     connection=await confirmation.resume(bindingId,receipt.apply_id,{assertCurrent(){assertOpen();assertInitial();}});
    }
    assertOpen();
    connections.set(slot,{request:structuredClone(request),connection,subscription,applyId:receipt.apply_id});
    connection=null;
    return status('pending',subscription,receipt.apply_id);
   }finally{
    connection?.close();
    pending.delete(slot);
   }
  },
  close(){closed=true;for(const entry of connections.values())entry.connection.close();connections.clear();}
 });
}
