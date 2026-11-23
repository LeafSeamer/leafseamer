import crypto from 'node:crypto';

// 仅为回环验收签发来源资格、时代证据及密钥。采集和封存走实际服务路径。
export function referenceSamplingFixture({endpoint,digest,bundleAuthority,utcOffsetNs=0n,eraBounds=null,approved=()=>true}){
 const fixed=(id,hash=digest(id))=>({id,revision:'1',digest:hash});
 const boundary=((1n<<32n)-2208988800n)*1000000000n+utcOffsetNs;
 const timeBounds=eraBounds??{lower_ns:String(boundary-1000000000n),upper_ns:String(boundary+1000000000n)};
 return ({identity,source})=>{
  const sourceSlot='source.seamer-reference',valid_before_tick_ns=String(source.tick()+90000000000n);
  const network={authentication:'trusted_network',endpoint},configuration_ref=fixed('configuration.seamer-time');
  const originals=bundleAuthority({serviceId:identity.service_id,configuration:configuration_ref,timeBounds,
   maxAnchorAge:'90000000000',maxExchangeAge:'90000000000',maxUpstreamAge:'91000000000'});
  const {approval_ref,profile_ref,policy_ref}=originals.references;
  const facts={source_slot:sourceSlot,source_digest:originals.sourceDoc.source_digest,approval_ref,configuration_ref,
   endpoint_ref:fixed('endpoint.seamer-loopback',digest(network)),minimum_poll_interval_ns:'16000000000',
   poll_lease_ns:'2000000000',reserved_bytes:16384,valid_before_tick_ns};
  const key=crypto.createSecretKey(crypto.randomBytes(32));
  return {sourceSlot,limits:{max_outstanding:1,max_reserved_bytes:131072},readSource:slot=>approved()&&slot===sourceSlot?facts:null,
   readEndpoint:reference=>reference.digest===facts.endpoint_ref.digest?network:null,
   captureProtection:{currentKey:scope=>({...scope,key,id:'capture.fixture',writable:true}),readKey:scope=>({...scope,key,id:'capture.fixture'})},
   referenceAuthority:{readApproval:()=>approved()?({format:'leafseamer-reference-approval/1',authentication:network.authentication,
    source_digest:facts.source_digest,approval_ref,configuration_ref,profile_ref,policy_ref,time_scale:'UTC',leap_mode:'normal_only',smear:false,
    elapsed_error_ppm:0,span_quantization_ns:'1000',max_span_ns:'2000000000',max_anchor_age_ns:'90000000000',max_exchange_age_ns:'90000000000',
    max_upstream_age_ns:'91000000000',max_half_width_ns:'1000000000',valid_before_tick_ns}):null,
   readEvidence:request=>({format:'leafseamer-reference-evidence/1',exchange_digest:request.exchange_digest,...originals.proofs(request),
    independent_utc_lower_ns:timeBounds.lower_ns,independent_utc_upper_ns:timeBounds.upper_ns,t2_error_ns:'1000',t3_error_ns:'1000',
    upstream_age_at_m1_ns:'1000',valid_before_tick_ns}),readBundleOriginals:originals.readBundleOriginals}};
 };
}
