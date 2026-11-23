import {digest,same} from './runtime/auth-clock-value.mjs';
import {clockInteger} from './runtime/native-clock.mjs';
import {endorsementReference} from './runtime/peer-run-approval-endorsement.mjs';
import {peerScope,proposalReference,proposalTarget} from './runtime/peer-run-approval-proposal.mjs';
import {need} from './automation-publication.mjs';

export function createPeerRunApprovalEndorsementReader({service,clock,pairings,transport}){
 const scope=peerScope(service);
 let pending=0;
 return Object.freeze({async prepare(bindingId,reference,savedProposal,{includeControls=false}={}){
  need(pending<4,'run_endorsement_capacity');
  const fixed=endorsementReference(structuredClone(reference)),saved=structuredClone(savedProposal);
  const proposal=proposalReference(saved.proposal_ref),target=proposalTarget(saved.proposal,fixed.scope);
  need(same(proposal.scope,scope)&&same(saved.proposal.scope,scope)&&saved.proposal.proposal_id===proposal.object_id&&
   saved.proposal.revision===proposal.revision&&digest(saved.proposal)===proposal.digest,'run_endorsement_proposal');
  const targetDigest=digest(target),start=clock.pairingSample(),started=clockInteger(start.tick_ns);
  const current=pairings.currentPending(bindingId);
  const peer=current.transcript.parties.find(party=>party.service_id===current.peer_service_id);
  need(current.state==='activated'&&peer?.service_kind==='controller'&&same(peerScope(peer),fixed.scope),'run_endorsement_pairing');
  const pairingRef={id:current.binding_id,revision:current.revision,digest:current.digest};
  pending++;
  try{
   let controlEvidence=null;
   if(includeControls){
    try{controlEvidence=await transport.readRunApprovalControls(bindingId,fixed);}
    catch(error){if(error.code!=='peer_pairing_response')throw error;}
   }
   const received=controlEvidence?.endorsement_read??await transport.readRunApprovalEndorsement(bindingId,fixed),value=received.endorsement;
   need(same(received.object_ref,fixed)&&same(value.proposal_ref,proposal)&&same(value.origin_scope,scope)&&
    value.run_id===saved.proposal.run_id&&same(value.service_principal,saved.proposal.service_principal)&&
    value.controller_target_digest===targetDigest&&same(value.validity,saved.proposal.validity)&&
    same(value.authorization_basis_refs[1],saved.proposal.source_service_approval_ref),'run_endorsement_proposal');
   need(received.state==='active'&&received.current_eligibility==='eligible','run_endorsement_ineligible');
   const deadline=clockInteger(received.eligible_before_ns),notBefore=BigInt(Date.parse(value.validity.not_before))*1000000n;
   let valid=true;
   function check(inside){
    need(valid,'run_endorsement_expired');
    try{
     const record=inside?pairings.requireActivatedInTransaction(pairingRef):pairings.currentPending(bindingId);
     const now=clock.pairingSample(),tick=clockInteger(now.tick_ns);
     need(same(record,current)&&same(now.runtime_domain_ref,start.runtime_domain_ref)&&tick>=started&&tick<started+2000000000n&&
      clockInteger(now.lower_ns)>=notBefore&&clockInteger(now.upper_ns)<deadline,'run_endorsement_expired');
    }catch(error){valid=false;throw error;}
   }
   check(false);
   return Object.freeze({original:structuredClone(received),pairing_ref:pairingRef,devices:structuredClone(controlEvidence?.devices??null),assertCurrentInTransaction:()=>check(true)});
  }finally{pending--;}
 }});
}
