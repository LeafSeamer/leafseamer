import {randomUUID as id} from 'node:crypto';
import {referenceBundleAuthority} from '../../../leafseamer-controller/test/fixtures/reference-bundle-authority.mjs';
import {digest} from '../automation-program-fixture.mjs';

const fixtureRef=key=>({id:key,revision:'1',digest:digest(key)});
const time=seconds=>({seconds:String(2208988800n+seconds),fraction:'0',era:'0'});
function original(authority,runtime,seconds,m1,m4){
 const utc=seconds*1000000000n,stamp=time(seconds),span=m4-m1;
 const packet={request_size:48,response_size:48,ntp_version:4,ntp_mode:4,leap:0,stratum:1,precision:0,root_delay_word:'0',root_dispersion_word:'0',
  request_digest:digest('request'),response_digest:digest('response'),response_deadline_tick_ns:String(m4+1000n),t2:stamp,t3:stamp};
 const anchor={anchor_id:id(),poll_ref:fixtureRef('poll'),capture_ref:fixtureRef('capture'),authorization:{endpoint_ref:fixtureRef('endpoint')},
  runtime_digest:digest('runtime'),m1_tick_ns:String(m1),m4_tick_ns:String(m4),lower_utc_ns:String(utc-1000n),upper_utc_ns:String(utc+2000n+span)};
 return authority.readBundleOriginals({anchor,runtime,packet,assessment:{tick_ns:String(m4),lower_utc_ns:anchor.lower_utc_ns,
  upper_utc_ns:String(BigInt(anchor.upper_utc_ns)+1000n),anchor_age_ns:'1000',exchange_age_ns:String(1000n+span),upstream_age_ns:String(2000n+span)}});
}

export function triggerReference({service,bootId,registration}){
 const {publisher,source_key,original:binding}=registration,descriptor_digest=digest('trigger source domain');
 const capture_domain={...publisher,provider_id:binding.provider_id,provider_boot_id:binding.provider_boot_id,runtime_binding_ref:fixtureRef('source.runtime'),
  clock_domain_id:'clock_'+descriptor_digest.slice(7),descriptor_digest,clock_generation:'1'};
 const issuer=referenceBundleAuthority({serviceId:publisher.authority_service_id,configuration:fixtureRef('source.config'),
  timeBounds:{lower_ns:'0',upper_ns:'3000000000'}});
 const receiver=referenceBundleAuthority({serviceId:service.service_id,configuration:fixtureRef('receiver.config'),
  timeBounds:{lower_ns:'0',upper_ns:'3000000000'}});
 const runtime={service_id:publisher.authority_service_id,database_instance_id:publisher.database_instance_id,
  launch_intent:{member_id:binding.provider_id},admission_receipt:{member_boot_id:binding.provider_boot_id},runtime_binding_id:'runtime',clock_domain_descriptor:capture_domain};
 const {bundle}=original(issuer,runtime,1n,1n,50n);
 const receiverMemberBoot=id();
 return {
  bundle,
  receiver:{readFirst({tick_ns,runtime_domain_ref}){
   const tick=BigInt(tick_ns),runtime={service_id:service.service_id,database_instance_id:service.database_instance_id,
    launch_intent:{member_id:'reference.receiver'},admission_receipt:{member_boot_id:receiverMemberBoot},runtime_binding_id:'runtime.receiver',clock_domain_descriptor:runtime_domain_ref};
   return {service_boot_id:bootId,valid_before_tick_ns:String(tick+100000000n),...original(receiver,runtime,2n,tick-200n,tick-100n)};
  },isCurrent:()=>true},
  qualify(origin){
   const body={id:id(),revision:'1',source_key,binding_ref:binding,event_id:origin.event_id,source_sequence:origin.source_sequence,
    payload_digest:digest(origin.payload),capture_semantics:'event_occurrence',capture_evidence_ref:fixtureRef('source.capture'),capture_domain,
    capture_start_tick_ns:'100',capture_end_tick_ns:'200',elapsed_profile_ref:issuer.references.profile_ref,
    reference_context_ref:{id:bundle.bundle_id,revision:bundle.revision,digest:bundle.bundle_digest},utc_interval:{lower_ns:'999999000',upper_ns:'1000003199'}};
   const evidence={...body,digest:digest(body)};
   origin.source_time_ref={kind:'source_time_evidence',scope:publisher,object_id:evidence.id,revision:evidence.revision,digest:evidence.digest};
   return evidence;
  }
 };
}
