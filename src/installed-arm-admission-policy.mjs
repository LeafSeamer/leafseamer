import crypto from 'node:crypto';
import {canonical,fields,same} from './runtime/auth-clock-value.mjs';
import {readLinuxProtectedFile} from './runtime/linux-clock-evidence.mjs';
import {clockInteger} from './runtime/native-clock.mjs';
import {validateAutomationModel} from './sdk/automation-program.mjs';

const dimensions=['logical_bytes','queue_slots','rows','work_units'];
const limits=['artifact_bytes','controller_targets','device_actions','preflight_bytes','sources'];
const fail=()=>{throw Object.assign(new Error('pairing_profile_arm_admission_invalid'),{code:'pairing_profile_arm_admission_invalid'});};

export function openInstalledArmAdmissionPolicy({approval_file,approval_sha256},{identity,clock}){
 const scope={authority_service_id:identity.service_id,database_instance_id:identity.database_instance_id};
 function readApproval(){
  const bytes=readLinuxProtectedFile(approval_file,16384);
  if('sha256:'+crypto.createHash('sha256').update(bytes).digest('hex')!==approval_sha256)fail();
  return bytes;
 }
 let config,starts,expires,profileRef;
 try{
  const bytes=readApproval(),text=new TextDecoder('utf-8',{fatal:true}).decode(bytes);
  config=JSON.parse(text);
  fields(config,['format','scope','profile_id','revision','qualification_ref','controller_scopes','limits','capacity','required','not_before_ns','expires_ns']);
  starts=clockInteger(config.not_before_ns);expires=clockInteger(config.expires_ns);
  if(config.format!=='leafseamer.arm-admission-approval/1'||canonical(config)!==text||!same(scope,config.scope)||starts>=expires)fail();
  profileRef={id:config.profile_id,revision:config.revision,digest:approval_sha256};
  validateAutomationModel('ContentRef',profileRef);
  validateAutomationModel('ContentRef',config.qualification_ref);
  fields(config.limits,limits);
  if(!limits.every(key=>Number.isSafeInteger(config.limits[key])&&config.limits[key]>0&&config.limits[key]<=1048576))fail();
  if(!Array.isArray(config.controller_scopes)||config.controller_scopes.length>256||
     new Set(config.controller_scopes.map(item=>canonical(item))).size!==config.controller_scopes.length)fail();
  for(const item of config.controller_scopes)validateAutomationModel('LifecycleScope',item);
  fields(config.capacity,dimensions);fields(config.required,dimensions);
  for(const key of dimensions){
   const capacity=clockInteger(config.capacity[key]),required=clockInteger(config.required[key]);
   if(required<=0n||required>capacity)fail();
  }
 }catch{fail();}
 const allowed=new Set(config.controller_scopes.map(item=>canonical(item)));
 let withdrawn=false;
 return Object.freeze({inspectInTransaction({scope:requested,observation,basis,artifactBytes}){
  if(withdrawn||!same(scope,requested)||!Number.isSafeInteger(artifactBytes)||artifactBytes<1||artifactBytes>config.limits.artifact_bytes)return null;
  try{
   readApproval();
   const sample=clock.current(),lower=clockInteger(sample.lower_ns),upper=clockInteger(sample.upper_ns);
   if(lower>upper||upper>=expires){withdrawn=true;return null;}
   if(lower<starts)return null;
  }catch{withdrawn=true;return null;}
  if(observation.targets.length>config.limits.controller_targets||
     observation.targets.some(item=>!allowed.has(canonical(item.controller_scope)))||
     observation.targets.reduce((count,item)=>count+item.device_actions.length,0)>config.limits.device_actions||
     observation.sources.length>config.limits.sources||
     Buffer.byteLength(canonical({observation,basis}))>config.limits.preflight_bytes)return null;
  return structuredClone({profile_ref:profileRef,qualification_ref:config.qualification_ref,capacity:config.capacity,required:config.required});
 }});
}
