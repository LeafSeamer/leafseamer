import {createHash} from 'node:crypto';
import {timeProviderPollStore} from './time-provider-polls.mjs';
import {timeProviderCaptureStore} from './time-provider-capture.mjs';
import {referenceTimeAnchorStore} from './reference-time-anchor.mjs';
import {referenceTimeScheduler} from './reference-time-scheduler.mjs';
import {canonical} from './runtime/auth-clock-value.mjs';

const require=(condition,code)=>{if(!condition)throw new Error(code);};

export function openReferenceSampling({db,identity,bootId,binding,supervision,collector,authentication,authority,archiveOnly=false}){
 const {limits,readSource,readEndpoint,captureProtection,referenceAuthority}=authority;
 const sourceSlot=archiveOnly?authority.schedule.sourceSlots[0]:authority.sourceSlot;
 require(typeof readEndpoint==='function'&&(archiveOnly?referenceAuthority===undefined:typeof referenceAuthority?.readBundleOriginals==='function'),
  'reference_sampling_authority');
 let closing;
 const assertCurrent=()=>{
  require(!closing&&db.open,'reference_sampling_closed');
  supervision.assertCurrent();collector.assertCurrent();
 };
 assertCurrent();
 const polls=timeProviderPollStore(db,bootId,binding,{limits,readSource:slot=>readSource(slot,authentication),readAuthentication:authentication.referenceTimeSample,assertCurrent,
  observeRetirement:supervision.observeTimeProviderRetirement,assertRetirement:supervision.assertTimeProviderRetirement},true,true,true);
 const anchors=archiveOnly?null:referenceTimeAnchorStore(db,bootId,binding,{...referenceAuthority,sourceSlot,readContext:polls.referenceContext});
 const archive=timeProviderCaptureStore(db,identity,captureProtection,anchors);
 const scheduler=referenceTimeScheduler({sourceSlots:[sourceSlot]},{plan:polls.schedulePlan,recover:polls.recover,
  collect(slot,pollId,facts){
   return collector.collect(()=>{
    assertCurrent();
    const endpoint=canonical(readEndpoint(Object.freeze({...facts.endpoint_ref})));
    require(typeof endpoint==='string'&&Buffer.byteLength(endpoint)<=12000&&
     'sha256:'+createHash('sha256').update(endpoint).digest('hex')===facts.endpoint_ref.digest,'reference_sampling_endpoint');
    archive.assertWritable();
    const {reservation,digest}=polls.reserve(slot,pollId,facts),attempt=polls.markAttempt({id:pollId,digest});
    const network=JSON.parse(endpoint),authorize=()=>polls.authorizeAttempt(attempt);
    return {reservation,attempt,network,authorize,
     storeControl:control=>polls.storeControl(attempt,control),releaseControl:polls.releaseControl,
     archive:(candidate,ntsBootstrap)=>archive.store({reservation,attempt,candidate,authorize,network,ntsBootstrap}),
     release:(receipt,acknowledgement)=>archive.release({receipt,acknowledgement,reservation,assertCurrent})};
   });
  }});
 return Object.freeze({receiverReference:anchors?.receiverReference??null,current:anchors?.current??null,status:scheduler.status,start:scheduler.start,
  close(){return closing??=(async()=>{const drained=scheduler.close();collector.close();await drained;})();}});
}
