import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {isIP} from 'node:net';
import {setTimeout as delay} from 'node:timers/promises';
import {canonical,context,digest,fields,same} from './runtime/auth-clock-value.mjs';
import {clockInteger} from './runtime/native-clock.mjs';
import {pairingUuid} from './runtime/peer-pairing-value.mjs';
import {authenticationClockChallenge,verifyClockEvidence} from './runtime/clock-evidence-envelope.mjs';
import {readLinuxProtectedFile} from './runtime/linux-clock-evidence.mjs';
import {validateAutomationModel} from './sdk/automation-program.mjs';

const publicationFormat='leafseamer.publication-policy-approval/1';
const eventPublicationFormat='leafseamer.publication-policy-approval/2';
const hash=value=>typeof value==='string'&&/^sha256:[a-f0-9]{64}$/.test(value);
const loopback=value=>['127.0.0.1','::1'].includes(value);
const need=(ok,code)=>{if(!ok)throw Object.assign(new Error(code),{code});};

export function readLocalFile(file,limit,{noFollow=false}={}){
 need(typeof file==='string'&&path.isAbsolute(file),'arguments');
 if(!noFollow)need(fs.statSync(file).isFile(),'local_file_type');
 const handle=fs.openSync(file,noFollow?fs.constants.O_RDONLY|fs.constants.O_NOFOLLOW|fs.constants.O_NONBLOCK:'r');
 try{
  const stat=fs.fstatSync(handle);
  need(stat.isFile()&&stat.size>0&&stat.size<=limit,'local_file_size');
  const data=Buffer.alloc(stat.size+1);let offset=0,count;
  while(offset<data.length&&(count=fs.readSync(handle,data,offset,data.length-offset,null))>0)offset+=count;
  need(offset===stat.size,'local_file_size');return data.subarray(0,offset);
 }finally{fs.closeSync(handle);}
}

function json(buffer){return JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(buffer));}
function httpsOrigin(value){const url=new URL(value);return typeof value==='string'&&url.protocol==='https:'&&url.origin===value;}

export function readTrustedPairingProfile(file,{linux=false}={}){
 try{
  const readFile=linux?readLinuxProtectedFile:readLocalFile;
  const value=json(readFile(file,65536));
  fields(value,['format','scope','local','controller',...(linux?[]:['clock']),'first_pairing_authorization_id',
   ...(Object.hasOwn(value,'public_api')?['public_api']:[]),...(Object.hasOwn(value,'publication_policy')?['publication_policy']:[])]);
  need(value.format==='leafseamer.seamer-pairing-profile/'+(linux?'2':'1'),'pairing_profile_format');
  const {scope,local,controller,clock,first_pairing_authorization_id:authorizationId}=value;
  fields(scope,['service_id','database_instance_id']);Object.values(scope).forEach(pairingUuid);
  fields(local,['credential_id','host','port','certificate_file','private_key_file','ca_files',
   ...(Object.hasOwn(local,'network')?['network']:[]),...(Object.hasOwn(local,'server_name')?['server_name']:[])]);
  fields(controller,['identity','origin','server_name','certificate_file','trust_bundle_digest']);
  fields(controller.identity,['service_id','database_instance_id','service_kind','issuer_id','issuer_epoch','credential_id']);
  if(!linux)fields(clock,['policy_digest','evidence_file','public_key_file']);
  pairingUuid(local.credential_id);if(authorizationId!==null)pairingUuid(authorizationId);
  const {service_kind,...ids}=controller.identity;Object.values(ids).forEach(pairingUuid);
  const network=local.network===true;
  need(service_kind==='controller'&&ids.service_id!==scope.service_id&&(loopback(local.host)||network&&isIP(local.host)!==0)&&
   (!Object.hasOwn(local,'network')||typeof local.network==='boolean')&&
   (!Object.hasOwn(local,'server_name')||typeof local.server_name==='string'&&local.server_name.length>0&&local.server_name.length<=253)&&
   Number.isInteger(local.port)&&local.port>0&&local.port<=65535&&
   Array.isArray(local.ca_files)&&local.ca_files.length>0&&local.ca_files.length<=8&&
   hash(controller.trust_bundle_digest)&&(linux||hash(clock.policy_digest)&&typeof clock.evidence_file==='string'&&path.isAbsolute(clock.evidence_file)),'pairing_profile_scope');
  need(httpsOrigin(controller.origin)&&typeof controller.server_name==='string'&&controller.server_name.length>0&&controller.server_name.length<=253,'pairing_profile_endpoint');
  let publicApi=null;
  if(Object.hasOwn(value,'public_api')){
   const api=value.public_api;
   fields(api,['host','port','certificate_file','private_key_file','binding_id','allowed_origins','local_permissions',
    ...(Object.hasOwn(api,'local_event_bindings')?['local_event_bindings']:[]),
    ...(Object.hasOwn(api,'local_source_subscriptions')?['local_source_subscriptions']:[])]);
   pairingUuid(api.binding_id);
   need(loopback(api.host)&&Number.isInteger(api.port)&&api.port>0&&api.port<=65535&&
    (api.host!==local.host||api.port!==local.port)&&Array.isArray(api.allowed_origins)&&api.allowed_origins.length<=8&&
    new Set(api.allowed_origins).size===api.allowed_origins.length&&api.allowed_origins.every(httpsOrigin)&&
    typeof api.local_permissions==='boolean'&&
    ['local_event_bindings','local_source_subscriptions'].every(key=>!Object.hasOwn(api,key)||typeof api[key]==='boolean'),'pairing_profile_public_api');
   publicApi={host:api.host,port:api.port,binding_id:api.binding_id,allowedOrigins:api.allowed_origins,localPermissions:api.local_permissions,
    localEventBindings:api.local_event_bindings===true,localSourceSubscriptions:api.local_source_subscriptions===true,
    cert:readFile(api.certificate_file,65536),key:readFile(api.private_key_file,65536)};
  }
  const publicationPolicy=value.publication_policy??null;
  if(Object.hasOwn(value,'publication_policy')){
   fields(publicationPolicy,['approval_file','approval_sha256']);
   need(typeof publicationPolicy.approval_file==='string'&&path.isAbsolute(publicationPolicy.approval_file)&&
    hash(publicationPolicy.approval_sha256),'pairing_profile_publication_policy');
  }
  const {service_id,issuer_id,issuer_epoch}=ids;
  const key=linux?null:crypto.createPublicKey(readLocalFile(clock.public_key_file,16384));
  need(linux||key.asymmetricKeyType==='ed25519','pairing_profile_signer');
  return {scope,authorizationId,publicApi,publicationPolicy,clock:linux?null:{file:clock.evidence_file,policyDigest:clock.policy_digest,key},
   pairing:{host:local.host,port:local.port,network,serverName:local.server_name??local.host,credential_id:local.credential_id,issuer:{service_id,issuer_id,issuer_epoch},
    cert:readFile(local.certificate_file,65536),key:readFile(local.private_key_file,65536),ca:local.ca_files.map(file=>readFile(file,65536))},
   peer:{identity:controller.identity,origin:controller.origin,server_name:controller.server_name,trust_bundle_digest:controller.trust_bundle_digest,
    cert:readFile(controller.certificate_file,65536).toString('utf8')}};
 }catch{throw Object.assign(new Error('pairing_profile_invalid'),{code:'pairing_profile_invalid'});}
}

export function openInstalledPublicationPolicy(settings,{identity,clock},{linux=false}={}){
 const {approval_file,approval_sha256}=settings;
 const readFile=linux?readLinuxProtectedFile:readLocalFile;
 function readApproval(){
  const bytes=readFile(approval_file,16384);
  need('sha256:'+crypto.createHash('sha256').update(bytes).digest('hex')===approval_sha256,'publication_policy_digest');
  return bytes;
 }
 let policy,starts,expires,eventLimits=null;
 try{
  const bytes=readApproval();
  policy=json(bytes);
  const withEventPolicy=policy.format===eventPublicationFormat;
  fields(policy,['format','policy_ref','not_before_ns','expires_ns',...(withEventPolicy?['event_age_limits','max_condition_nodes']:[])]);
  validateAutomationModel('LifecyclePolicyRef',policy.policy_ref);
  starts=clockInteger(policy.not_before_ns);expires=clockInteger(policy.expires_ns);
  const scope={authority_service_id:identity.service_id,database_instance_id:identity.database_instance_id};
  need((policy.format===publicationFormat||withEventPolicy)&&canonical(policy)===bytes.toString('utf8')&&
   same(policy.policy_ref.scope,scope)&&starts<expires,'publication_policy_approval');
  if(withEventPolicy){
   const {event_age_limits,max_condition_nodes}=policy;
   need(Array.isArray(event_age_limits)&&event_age_limits.length>0&&Number.isInteger(max_condition_nodes)&&max_condition_nodes>0&&max_condition_nodes<=1048576&&
    policy.policy_ref.digest===digest({event_age_limits,max_condition_nodes}),'publication_policy_event_limits');
   eventLimits=new Map();
   for(const value of event_age_limits){
    fields(value,['authority_id','registration_ref','capture_semantics','max_age_ms']);
    pairingUuid(value.authority_id);validateAutomationModel('ContentRef',value.registration_ref);
    need(['event_occurrence','sampled_observation'].includes(value.capture_semantics)&&Number.isSafeInteger(value.max_age_ms)&&value.max_age_ms>=0,
     'publication_policy_event_limits');
    const key=canonical([value.authority_id,value.registration_ref]);
    need(!eventLimits.has(key),'publication_policy_event_limits');eventLimits.set(key,Object.freeze(value));
   }
  }
 }catch(error){
  throw Object.assign(new Error('pairing_profile_publication_policy_invalid',{cause:error}),{code:'pairing_profile_publication_policy_invalid'});
 }
 let withdrawn=false;
 function assertCurrentInTransaction(reference){
  if(withdrawn||!same(reference,policy.policy_ref))return false;
  try{
   readApproval();
   const sample=clock.current(),lower=clockInteger(sample.lower_ns),upper=clockInteger(sample.upper_ns);
   if(lower>upper||upper>=expires){withdrawn=true;return false;}
   return lower>=starts;
  }catch{withdrawn=true;return false;}
 }
 const eventPolicy=eventLimits&&Object.freeze({max_condition_nodes:policy.max_condition_nodes,limitInTransaction(reference,authority,registration){
  if(!assertCurrentInTransaction(reference))return null;
  const value=eventLimits.get(canonical([authority,registration]));
  return value?{capture_semantics:value.capture_semantics,max_age_ms:value.max_age_ms,approval_digest:approval_sha256}:null;
 }});
 return Object.freeze({assertCurrentInTransaction,eventAgePolicy:()=>eventPolicy});
}

export async function openPairingClock(profile,{scope,identity,source,supervisedDomain=null},emit,signal){
 need(scope.service_id===profile.scope.service_id&&identity.database_instance_id===profile.scope.database_instance_id,'pairing_profile_identity');
 const capture=source.capture();need(capture.continuity_available,'pairing_clock_continuity');
 const settings=profile.clock;
 function read(allowPreviousBoot=false){
  let bytes;
  try{bytes=readLocalFile(settings.file,65536);}catch(error){if(allowPreviousBoot&&error.code==='ENOENT')return null;throw error;}
  const evidence=verifyClockEvidence(bytes,settings.key);
  need(evidence.policy?.digest===settings.policyDigest&&evidence.runtime?.service_id===scope.service_id,'pairing_clock_policy');
  if(allowPreviousBoot&&evidence.runtime.service_boot_id!==scope.service_boot_id)return null;
  if(supervisedDomain)need(evidence.runtime.clock_domain_id===supervisedDomain.clock_domain_id&&
   evidence.runtime.descriptor_digest===supervisedDomain.descriptor_digest,'pairing_clock_supervised_domain');
  return context(evidence,scope,source.artifactDigest,source.tick());
 }
 emit(authenticationClockChallenge(scope,identity.database_instance_id,source,capture,supervisedDomain));
 const deadline=performance.now()+10000;
 for(;;){
  signal.throwIfAborted();if(read(true))return {read:()=>read()};
  need(performance.now()<deadline,'pairing_clock_evidence_unavailable');await delay(50,undefined,{signal});
 }
}

export async function completeLocalFirstPairing(pairing,authorizationId,emit,signal,publicBindingId=null){
 let notified=false;
 while(!signal.aborted){
  const old=pairing.firstPairingCompletion(authorizationId);
  if(old){
   need(publicBindingId===null||old.pairing_ref.id===publicBindingId,'pairing_profile_public_binding');
   emit({event:'first_pairing_completed',receipt:old});return;
  }
  const stage=pairing.stageFirstPairing(authorizationId);
  if(!notified){emit({event:'first_pairing_staged',authorization_id:authorizationId,binding_id:stage.binding_id});notified=true;}
  let receipt;
  try{
   await pairing.transport.prove(stage.binding_id);
   if(signal.aborted)return;
   receipt=await pairing.completeFirstPairing(authorizationId);
  }catch(error){
   if(signal.aborted)return;
   if(!['peer_pairing_activation','peer_pairing_first_authorization_remote','peer_pairing_response','peer_pairing_timeout',
    'ECONNREFUSED','ECONNRESET'].includes(error.code??error.message))throw error;
  }
  if(receipt){emit({event:'first_pairing_completed',receipt});return;}
  await delay(150,undefined,{signal});
 }
}
