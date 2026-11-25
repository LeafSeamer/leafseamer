// Reconcile clock boundaries independently of the asynchronous peer observations.
export function startScheduleMonitor({schedules,runs,readClock,sample,dispatchAvailable=true}){
 const canAdmit=dispatchAvailable&&typeof runs?.resolveScheduledSource==='function';
 const pending=new Map();
 let closed=false,lastOccurrence='';
 const liveClock=()=>closed?null:readClock();
 function scan(){
  if(closed)return;
  let due;
  try{due=schedules.reconcileClock(liveClock,runs?.isScheduleSlotBusyInTransaction,{collectDue:canAdmit});}
  catch{return;}
  if(!canAdmit||pending.size>=4)return;
  const split=due.findIndex(({occurrence_id})=>occurrence_id===lastOccurrence)+1;
  const ordered=split>0?[...due.slice(split),...due.slice(0,split)]:due;
  for(const candidate of ordered){
   if(pending.size>=4)break;
   const {definition_id,occurrence_id}=candidate;
   if(pending.has(definition_id))continue;
   lastOccurrence=occurrence_id;
   let source;
   try{source=runs.resolveScheduledSource(candidate);}
   catch{continue;}
   if(!source)continue;
   const work=runs.admitScheduledOccurrence(source,liveClock,sample)
    .catch(()=>{}).finally(()=>pending.delete(definition_id));
   pending.set(definition_id,work);
  }
 }
 const timer=setInterval(scan,250);
 timer.unref();
 scan();
 return Object.freeze({close(){
  closed=true;
  clearInterval(timer);
  return Promise.allSettled(pending.values());
 }});
}
