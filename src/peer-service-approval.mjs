import {same} from './runtime/auth-clock-value.mjs';
import {clockInteger} from './runtime/native-clock.mjs';
import {need} from './automation-publication.mjs';

export function createPeerServiceApprovalReader({clock,pairings,transport,issuer}){
 const fixed=structuredClone(issuer);
 let pending=0;
 return Object.freeze({
  async prepare(bindingId,principal,approvalRef){
   need(pending<4,'run_service_approval_capacity');
   const start=clock.pairingSample(),started=clockInteger(start.tick_ns),current=pairings.currentPending(bindingId);
   const peer=current.transcript.parties.find(party=>party.service_id===fixed.service_id);
   need(current.state==='activated'&&current.peer_service_id===fixed.service_id&&peer?.service_kind==='controller'&&
    principal.issuer_id===fixed.issuer_id&&principal.issuer_epoch===fixed.issuer_epoch&&peer.issuer_id===fixed.issuer_id&&peer.issuer_epoch===fixed.issuer_epoch,'run_service_issuer');
   const pairingRef={id:current.binding_id,revision:current.revision,digest:current.digest},domain=structuredClone(start.runtime_domain_ref);
   pending++;
   try{
    const original=await transport.readAutomationServiceApproval(bindingId,{principal,approval_ref:approvalRef});
    let valid=true;
    function check(inside){
     need(valid,'run_service_approval_expired');
     try{
      const record=inside?pairings.requireActivatedInTransaction(pairingRef):pairings.currentPending(bindingId);
      const now=clock.pairingSample(),tick=clockInteger(now.tick_ns);
      need(same(record,current)&&same(now.runtime_domain_ref,domain)&&tick>=started&&tick<started+2000000000n&&
       clockInteger(now.lower_ns)>=clockInteger(original.approval.not_before_ns)&&clockInteger(now.upper_ns)<clockInteger(original.approval.expires_ns),
       'run_service_approval_expired');
     }catch(error){valid=false;throw error;}
    }
    check(false);
    return {original,pairing_ref:pairingRef,assertCurrentInTransaction:()=>check(true)};
   }finally{pending--;}
  }
 });
}
