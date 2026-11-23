import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import test from 'node:test';
import {createPeerRunApprovalEndorsementReader} from '../dist/peer-run-approval-endorsement.mjs';
import {digest} from '../dist/runtime/auth-clock-value.mjs';
import {endorsementControlReadReply,endorsementReadRequest} from '../dist/runtime/peer-run-approval-endorsement.mjs';

const id=()=>crypto.randomUUID();
const scope=()=>({authority_service_id:id(),database_instance_id:id()});

test('提交时复核认可证据的配对、单调域、两秒窗口与原有效期，失败后该证据失效',async()=>{
 const origin=scope(),controller=scope(),service={service_id:origin.authority_service_id,database_instance_id:origin.database_instance_id};
 const start=1700000000000000000n,end=start+60000000000n;
 const iso=ns=>new Date(Number(ns/1000000n)).toISOString();
 const target={controller_scope:controller,device_actions:[{device_id:id(),action_id:'simulator.value.set',capability_version:'1.0.0'}],parameter_constraints:[]};
 const source={kind:'service_approval',scope:controller,object_id:id(),revision:'1',digest:digest('source')};
 const proposal={scope:origin,proposal_id:id(),revision:'1',run_id:id(),service_principal:{issuer_id:id(),issuer_epoch:id(),principal_id:id()},
  source_service_approval_ref:source,validity:{not_before:iso(start),not_after:iso(end)},controller_targets:[target]};
 const proposalRef={kind:'run_approval_proposal',scope:origin,object_id:proposal.proposal_id,revision:'1',digest:digest(proposal)};
 const endorsement={proposal_ref:proposalRef,origin_scope:origin,run_id:proposal.run_id,service_principal:proposal.service_principal,
  controller_target_digest:digest(target),validity:proposal.validity,authorization_basis_refs:[null,source]};
 const object_ref={kind:'run_approval_endorsement',scope:controller,object_id:id(),revision:'1',digest:digest(endorsement)};
 const received={endorsement,object_ref,state:'active',current_eligibility:'eligible',eligible_before_ns:String(end)};
 const original={binding_id:id(),revision:'1',digest:digest('pair'),state:'activated',peer_service_id:controller.authority_service_id,
  transcript:{parties:[{service_id:controller.authority_service_id,database_instance_id:controller.database_instance_id,service_kind:'controller'}]}};
 let pair=original,tick='100',checked=0,upper=start+100000000n,domain={id:id()};
 const reader=createPeerRunApprovalEndorsementReader({service,
  clock:{pairingSample:()=>({tick_ns:tick,lower_ns:String(start),upper_ns:String(upper),runtime_domain_ref:domain})},
  pairings:{currentPending:()=>pair,requireActivatedInTransaction(reference){
   assert.deepEqual(reference,{id:original.binding_id,revision:original.revision,digest:original.digest});checked++;return pair;
  }},transport:{async readRunApprovalEndorsement(){return received;}}});
 const prepare=()=>reader.prepare(original.binding_id,object_ref,{proposal,proposal_ref:proposalRef});
 const valid=await prepare();
 valid.assertCurrentInTransaction();assert.equal(checked,1);assert.deepEqual(valid.original,received);
 const changes=[()=>{tick='2000000100';},()=>{upper=end;},()=>{domain={id:id()};},()=>{pair={...original,revision:'2'};}];
 for(const change of changes){
  tick='100';upper=start+100000000n;pair=original;
  const prepared=await prepare();change();
  assert.throws(prepared.assertCurrentInTransaction,{message:'run_endorsement_expired'});
  tick='100';upper=start+100000000n;pair=original;
  assert.throws(prepared.assertCurrentInTransaction,{message:'run_endorsement_expired'});
 }
});

test('控制读回与背书绑定，只接受完整的目标设备集合，失效背书不释放控制记录',()=>{
 const controller=scope(),seamer=scope(),endorsementId=id(),devices=[id(),id()].sort(),principal={issuer_id:id(),issuer_epoch:id(),principal_id:id()};
 const ref=(kind,scope,object_id)=>({kind,scope,object_id,revision:'1',digest:digest({kind,object_id})});
 const target={controller_scope:controller,device_actions:devices.map(device_id=>({device_id,action_id:'simulator.value.set',capability_version:'1.0.0'})),parameter_constraints:[]};
 const endorsement={scope:controller,endorsement_id:endorsementId,revision:'1',origin_scope:seamer,run_id:id(),service_principal:principal,
  controller_target_digest:digest(target),device_actions:target.device_actions,parameter_constraints:target.parameter_constraints,
  proposal_ref:ref('run_approval_proposal',seamer,id()),endorsed_by:principal,validity:{not_before:'2026-11-01T00:00:00.000Z',not_after:'2026-11-01T00:01:00.000Z'},
  authorization_basis_refs:[ref('run_endorsement_policy',controller,id()),ref('service_approval',controller,id())],created_authentication_ref:ref('authentication',controller,endorsementId)};
 const object_ref={...ref('run_approval_endorsement',controller,endorsementId),digest:digest(endorsement)};
 const request=endorsementReadRequest(endorsementId,object_ref.digest,id()),expires_ns=String(BigInt(Date.parse(endorsement.validity.not_after))*1000000n);
 const record={transcript:{pair_attempt_id:id()},transcript_digest:digest('paired'),expires_ns};
 const endorsement_read={endorsement,object_ref,state:'active',status_revision:'1',current_eligibility:'eligible',eligible_before_ns:expires_ns};
 const control={control_ref:{id:id(),revision:'2',digest:digest('control record')},control_epoch:id(),effective_version:'1',mode:'manual',gate_state:'protected',pending_operation_id:null};
 const body={endorsement_read,devices:[{device_id:devices[0],control},{device_id:devices[1],control:null}]};
 const reply={format:'leafseamer.run-approval-control-observation-response/1',challenge_id:request.challenge_id,pair_attempt_id:record.transcript.pair_attempt_id,transcript_digest:record.transcript_digest,body};
 const read=body=>endorsementControlReadReply({...reply,body},request,record,controller,seamer);
 assert.deepEqual(read(body),body);
 for(const changed of [body.devices.slice(1),[...body.devices].reverse(),[body.devices[0],{...body.devices[1],device_id:id()}]]){
  assert.throws(()=>read({...body,devices:changed}),{code:'peer_pairing_control_observation_scope'});
 }
 const blocked={...endorsement_read,state:'expired',status_revision:'2',current_eligibility:'blocked',eligible_before_ns:null};
 assert.throws(()=>read({...body,endorsement_read:blocked}),{code:'peer_pairing_control_observation_scope'});
 assert.deepEqual(read({endorsement_read:blocked,devices:null}),{endorsement_read:blocked,devices:null});
});
