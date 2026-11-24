#!/usr/bin/env node
import path from 'node:path';
import {canonical} from './runtime/auth-clock-value.mjs';
import {importFirstPairingAuthorization,initializeStorage,migrateStorage,openStorage,StorageError} from './storage.mjs';
import {startSeamer} from './server.mjs';
import {completeLocalFirstPairing,openInstalledPublicationPolicy,openPairingClock,readLocalFile,readTrustedPairingProfile} from './trusted-pairing-profile.mjs';
import {readProtectedIngressProfile} from './protected-ingress-profile.mjs';
import {readLinuxClockEvidenceProfile} from './runtime/linux-clock-evidence.mjs';
const emit=value=>process.stdout.write(JSON.stringify(value)+'\n');
function readFirstPairingFile(file,limit){
 try{return readLocalFile(file,limit);}catch{throw new StorageError('first_pairing_input_file');}
}
const supervisorOptions=['--bootstrap-policy','--supervisor-pipe','--supervisor-channel','--time-provider-intent'];
async function serve(options,contained,referenceSamplingAuthority,onStartupFailure,protectedIngress,containedLinux){
 const port=options['--port']??'0',file=options['--pairing-profile'];
 if(!/^(0|[1-9][0-9]{0,4})$/.test(port)||file!==undefined&&!path.isAbsolute(file))throw new Error('arguments');
 const profile=file===undefined?null:readTrustedPairingProfile(file,{linux:containedLinux}),stopRequest=new AbortController(),{signal}=stopRequest;
 if(contained&&(!profile||!supervisorOptions.every(name=>options[name])))throw new Error('reference_supervisor_options');
 const publish=contained?value=>process.stderr.write(JSON.stringify(value)+'\n'):emit;
 let app,stopping,supervisedReference=null,linuxBootstrap=null,linuxClockEvidenceProfile=null,control='';
 const stopInput=()=>{
  if(!contained)return;
  process.stdin.removeListener('data',onControl);process.stdin.removeListener('end',stop);process.stdin.removeListener('error',stop);
  process.stdin.destroy();
 };
 const stop=()=>{
  stopRequest.abort();
  stopInput();
  if(app)stopping??=app.close().then(()=>publish({event:'stopped'})).catch(()=>{
   process.stderr.write('{"error":"shutdown_failed"}\n');process.exitCode=1;
  });
  return stopping;
 };
 const onControl=chunk=>{
  control+=chunk;
  if(control==='stop\n'||!('stop\n'.startsWith(control)))void stop();
 };
 process.once('SIGINT',stop);process.once('SIGTERM',stop);
 try{
  if(containedLinux){
   const raw=options['--bootstrap-policy'];
   if(process.platform!=='linux'||typeof raw!=='string'||Buffer.byteLength(raw)>8192)throw new Error('bootstrap_policy');
   const {validateBootstrapPolicy}=await import('./core-bootstrap.mjs');
   const {openLinuxCoreBootstrap}=await import('./linux-core-bootstrap.mjs');
   const policy=validateBootstrapPolicy(JSON.parse(raw));
   if(canonical(policy)!==raw)throw new Error('bootstrap_policy');
   if(options['--clock-evidence-profile']!==undefined){
    linuxClockEvidenceProfile=readLinuxClockEvidenceProfile(options['--clock-evidence-profile'],policy,'seamer');
   }
   if(profile&&(!linuxClockEvidenceProfile?.authentication||profile.scope.service_id!==policy.service_id||
    profile.scope.database_instance_id!==policy.database_instance_id))throw new Error('pairing_profile_identity');
   linuxBootstrap=openLinuxCoreBootstrap(policy,'seamer',{compose:protectedIngress!==null});
  }
  if(contained){
   const {createSupervisedReference}=await import('./supervised-reference.mjs').catch(error=>{
    if(error.code==='ERR_MODULE_NOT_FOUND')throw new Error('reference_supervisor_unavailable');
    throw error;
   });
   supervisedReference=createSupervisedReference(options,{input:process.stdin,output:process.stdout,onLost:()=>{
    if(signal.aborted)return;
    publish({event:'reference_supervisor_lost'});void stop();
   },onAdmitted:()=>{
    process.stdin.on('data',onControl);process.stdin.once('end',stop);process.stdin.once('error',stop);process.stdin.resume();
   }});
  }
  app=await startSeamer({dataDir:options['--data-dir'],host:options['--host']??'127.0.0.1',port:Number(port),
   supervisedReference,linuxBootstrap,linuxClockEvidenceProfile,referenceSamplingAuthority,onStartupFailure,protectedIngress,...(profile&&{pairing:profile.pairing,publicApi:profile.publicApi,
    ...(!containedLinux&&{authenticationProvider:input=>openPairingClock(profile,input,publish,signal)}),
    publicationPolicyFactory:profile.publicationPolicy?input=>openInstalledPublicationPolicy(profile.publicationPolicy,input):null})});
  linuxBootstrap?.monitor(stop);
  if(signal.aborted){await stop();return;}
  if(profile){
   if(!app.authenticationStatus().qualified)throw new Error('pairing_clock_unqualified');
   app.pairing.transport.bindPeers([profile.peer]);
   if(profile.authorizationId!==null&&typeof app.pairing.stageFirstPairing!=='function')throw new Error('pairing_profile_migration_required');
  }
  publish({event:'listening',...app.identity,service_boot_id:app.serviceBootId,host:app.address.address,port:app.address.port,
   ...(app.publicOrigin&&{public_origin:app.publicOrigin}),...(app.collectorBinding&&{reference_collector:app.collectorBinding}),ready:false});
  if(profile&&profile.authorizationId!==null)await completeLocalFirstPairing(app.pairing,profile.authorizationId,publish,signal,profile.publicApi?.binding_id??null);
  return app;
 }catch(error){
  if(signal.aborted){await stop();return;}
  await app?.close().catch(()=>{});app=null;throw error;
 }finally{
  if(!app){await supervisedReference?.close();linuxBootstrap?.close();stopInput();}
  if(!app||signal.aborted){process.removeListener('SIGINT',stop);process.removeListener('SIGTERM',stop);}
 }
}
export async function runCli(argv,{referenceSamplingAuthority=null}={}){
let reported=false;
async function report(error){
 if(reported)return;
 reported=true;
 const code=error.code??error.message,safe=error instanceof StorageError||/^(arguments|protected_ingress_(?:profile|platform|address)|linux_clock_evidence_(?:profile|binding|directory)|clock_kernel_sleep_(?:available|unverified)|pairing_(profile|clock)_[a-z_]+|peer_pairing_[a-z_]+|auth_clock_[a-z_]+|bootstrap_[a-z_]+|reference_sampling_[a-z_]+|(?:reference_)?supervisor_[a-z_]+|time_provider_[a-z_]+)$/.test(code);
 await new Promise(resolve=>process.stderr.write(JSON.stringify({error:safe?code:'startup_failed'})+'\n',resolve));
}
try{
 const [command,...args]=argv,options={};
 const contained=command==='contained-core-auth';
 const composeCore=command==='contained-compose-linux';
 const containedLinux=command==='contained-core-linux'||composeCore;
 const ingress=command==='start-protected-ingress'||composeCore;
 const importOptions=['--authorization-file','--controller-certificate','--seamer-certificate'];
 if(!['init-storage','migrate-storage','identity-status','import-first-pairing','start','contained-core-auth','contained-core-linux','contained-compose-linux','start-protected-ingress'].includes(command)||args.length%2)throw new Error('arguments');
 const allowed=['--data-dir',...(composeCore?['--bootstrap-policy','--ingress-profile','--clock-evidence-profile']:containedLinux?['--host','--port','--bootstrap-policy','--clock-evidence-profile','--pairing-profile']:command==='start'||contained?['--host','--port','--pairing-profile',...(contained?supervisorOptions:[])]:ingress?['--ingress-profile']:command==='migrate-storage'?['--to-revision']:command==='import-first-pairing'?importOptions:[])];
 for(let i=0;i<args.length;i+=2){if(!allowed.includes(args[i])||Object.hasOwn(options,args[i]))throw new Error('arguments');options[args[i]]=args[i+1];}
 if(!options['--data-dir']||!path.isAbsolute(options['--data-dir']))throw new Error('arguments');
 if(command==='init-storage')emit({event:'storage_initialized',...initializeStorage(options['--data-dir']),ready:false});
 else if(command==='migrate-storage')emit({event:'storage_migrated',...migrateStorage(options['--data-dir'],options['--to-revision']),ready:false});
 else if(command==='identity-status'){const storage=openStorage(options['--data-dir']);try{emit({event:'identity_status',...storage.identity,ready:false});}finally{storage.close();}}
 else if(command==='import-first-pairing'){
  if(!importOptions.every(name=>options[name]&&path.isAbsolute(options[name])))throw new Error('arguments');
  const result=importFirstPairingAuthorization(options['--data-dir'],{bytes:readFirstPairingFile(options['--authorization-file'],32768),
   controllerCertificate:readFirstPairingFile(options['--controller-certificate'],65536),seamerCertificate:readFirstPairingFile(options['--seamer-certificate'],65536)});
  emit({event:'first_pairing_imported',...result,ready:false});
 }
 else return await serve(options,contained,referenceSamplingAuthority,contained?report:null,
  ingress?readProtectedIngressProfile(options['--ingress-profile']):null,containedLinux);
}catch(error){
 process.exitCode=1;await report(error);
}
}
if(import.meta.main)await runCli(process.argv.slice(2));
