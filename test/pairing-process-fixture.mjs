// 仅测试子进程：合成环境资格与Seamer人员核验，不作为部署入口打包。
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import https from 'node:https';
import net from 'node:net';
import assert from 'node:assert/strict';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {spawnSync} from 'node:child_process';
import Database from 'better-sqlite3';
import {setup} from '../../leafseamer-controller/test/fixtures/extension-installation.mjs';
import {authenticated} from '../../leafseamer-controller/test/fixtures/extension-auth.mjs';
import {openNativeClock} from '../../leafseamer-controller/dist/native-clock.mjs';
import {createIntrospectionEndpoint} from '../../leafseamer-controller/dist/introspection-http.mjs';
import {createAuthenticationHttp} from '../../leafseamer-controller/dist/auth-http.mjs';
import {loadGuiAssets} from '../../leafseamer-controller/dist/gui-assets.mjs';
import {canonical,seal,digest} from '../dist/runtime/auth-clock-value.mjs';
import {fixture} from './auth-clock-fixture.mjs';
import {seedLocalPermission} from './local-authorization-fixture.mjs';
import {source} from './automation-program-fixture.mjs';
import {registered as registeredBindings} from '../../leafseamer-controller/test/fixtures/device-bindings.mjs';
import {registerDeviceAutomation} from '../../leafseamer-controller/dist/device-automation-binding.mjs';
import {publicationMaterial} from './automation-publication-fixture.mjs';
import {contractSelection as selection} from '../dist/sdk/index.mjs';
import {referenceBundleAuthority} from '../../leafseamer-controller/test/fixtures/reference-bundle-authority.mjs';
import {referenceSamplingFixture} from './fixtures/reference-sampling-authority.mjs';
import {startNativeReferenceEventPeer,writeNativePeerMessage} from './fixtures/native-reference-event-peer.mjs';
import {setTimeout as delay} from 'node:timers/promises';
const packageRoot=process.env.LEAFSEAMER_PACKAGE_UNDER_TEST??fileURLToPath(new URL('../',import.meta.url));assert.ok(path.isAbsolute(packageRoot));
const {initializeStorage,migrateStorage}=await import(pathToFileURL(path.join(packageRoot,'dist/storage.mjs')));
const {startSeamer}=await import(pathToFileURL(path.join(packageRoot,'dist/server.mjs')));
const {openInstalledPublicationPolicy}=await import(pathToFileURL(path.join(packageRoot,'dist/trusted-pairing-profile.mjs')));
const controllerPackage=process.env.LEAFSEAMER_CONTROLLER_PACKAGE_UNDER_TEST;
if(controllerPackage)assert.ok(path.isAbsolute(controllerPackage));
const loadController=name=>import(controllerPackage?pathToFileURL(path.join(controllerPackage,'dist',name)):new URL('../../leafseamer-controller/dist/'+name,import.meta.url));
const [{startController},{startPeerPairingTransport}]=await Promise.all([loadController('server.mjs'),loadController('peer-pairing-transport.mjs')]);
const id=()=>crypto.randomUUID(),cleanup=[],harness={after:fn=>cleanup.push(fn)};
let api,transport,credential,app,controllerAuth,initialized=false;
let gui=null,controllerOrigin=null,controllerIdentity=null;
let seamerDirectory=null,seamerIssuer=null,seamerOptions=null,seamerPeers=null,appClosed=false;
let seamerInitial=null,nativePeer=null;
let firstProtectionReceipt=null,protectionBlocked=false,dropApprovalId=null;
let controllerFixture=null,controllerTransportOptions=null,controllerPeers=null;
let controllerApp=null,publication=null,bindingFixture=null,bindingAvailable=true,includeRunScopes=false,wireEndorsement=null;const bindingHandles=new Map();
const identityHandles=new Map();
const eventHandles=new Map();
const subscriptionConnections=new Map();
function receiverReferenceFixture({identity,serviceBootId,source}){
 const ref=id=>({id,revision:'1',digest:digest(id)}),timestamp={seconds:'60',fraction:'0',era:'1'};
 const utc=(4294967296n+60n-2208988800n)*1000000000n,ttl=180000000000n;
 const authority=referenceBundleAuthority({serviceId:identity.service_id,configuration:ref('configuration.receiver'),
  timeBounds:{lower_ns:String(utc-1000000000n),upper_ns:String(utc+1000000000n)},maxAnchorAge:String(ttl),
  maxExchangeAge:String(ttl),maxUpstreamAge:String(ttl+1000000000n)});
 const m1=source.tick(),m4=source.tick(),span=m4-m1+1000n,valid_before_tick_ns=String(m1+ttl-1000000n);
 const packet={request_size:48,response_size:48,ntp_version:4,ntp_mode:4,leap:0,stratum:1,precision:0,root_delay_word:'0',root_dispersion_word:'0',
  request_digest:digest('receiver request'),response_digest:digest('receiver response'),response_deadline_tick_ns:String(m1+2000000000n),t2:timestamp,t3:timestamp};
 const anchor={anchor_id:id(),poll_ref:ref('poll.receiver'),capture_ref:ref('capture.receiver'),authorization:{endpoint_ref:ref('endpoint.receiver')},
  runtime_digest:digest('runtime.receiver'),m1_tick_ns:String(m1),m4_tick_ns:String(m4),lower_utc_ns:String(utc-1000n),upper_utc_ns:String(utc+1000n+span)};
 let original=null;
 return Object.freeze({readFirst({runtime_domain_ref}){
  if(original===null){
   const runtime={service_id:identity.service_id,database_instance_id:identity.database_instance_id,launch_intent:{member_id:'fixture.receiver.clock'},
    admission_receipt:{member_boot_id:id()},runtime_binding_id:'fixture.receiver.runtime',clock_domain_descriptor:runtime_domain_ref};
   original={service_boot_id:serviceBootId,valid_before_tick_ns,...authority.readBundleOriginals({anchor,runtime,packet,assessment:{tick_ns:String(m4),
    lower_utc_ns:anchor.lower_utc_ns,upper_utc_ns:String(BigInt(anchor.upper_utc_ns)+1000n),anchor_age_ns:'1000',
    exchange_age_ns:String(span),upstream_age_ns:String(span+1000n)}})};
  }
  return original;
 },isCurrent({reference}){return reference.bundle.bundle_digest===original?.bundle.bundle_digest&&source.tick()<BigInt(valid_before_tick_ns);}});
}
async function initialize(input,native=null){
 const {role,material,ca,issuer,principal_id,allowedOrigins=[],automaticDenials=false,productController=false,bindingRead=false,runTargets=false,reconcileIngest=false,eventApprovals=false,receiverReference=false,firstEnrollment=false,armTarget=null,snapshotIngest=process.env.LEAFSEAMER_SEAMER_SCHEMA4_TEST==='1'}=input;
 assert.equal(initialized,false);initialized=true;
 let identity,publicOrigin=null,apiOrigin=null,bindingId=null;
 if(role==='controller'){
  const f=await setup(harness,productController?76:runTargets?74:62),capability=f.store.enrollment.begin();controllerFixture=f;
  await f.store.enrollment.complete({token_id:capability.token_id,secret:capability.secret,login:'role.test',password:'Synthetic-role-password-123',mode:'standalone'});
  const auth=await authenticated(harness,f,{grant:false,native:{source:openNativeClock(),descriptor:{clock_domain_id:id(),descriptor_digest:digest('synthetic-controller-process-domain')}},sampleWindowPolicy:'authentication-clock-sample/2'});
  controllerAuth=auth;credential=auth.auth;api=auth.sessions.peerPairings;identity={service_id:f.store.identity.service_id,database_instance_id:f.store.identity.database_instance_id,
   service_kind:'controller',issuer_id:auth.logged.session.issuer_id,issuer_epoch:auth.logged.session.issuer_epoch,credential_id:id()};
  controllerTransportOptions={identity,...material,ca};
  includeRunScopes=runTargets;
  if(bindingRead){
   const device=f.store.registerDevice({request_id:id(),device_kind:'simulator',display_name:'配对测试设备'}).device_id;
   bindingFixture=registeredBindings(harness,{service:{service_id:identity.service_id,database_instance_id:identity.database_instance_id},device});
  }
  if(productController){
   const interval=auth.real.current();auth.real.stop();f.close();
   const reservation=net.createServer();await new Promise(resolve=>reservation.listen(0,'127.0.0.1',resolve));const port=reservation.address().port;await new Promise(resolve=>reservation.close(resolve));
   apiOrigin='https://127.0.0.1:'+port;
   controllerApp=await startController({dataDir:f.dir,port,tls:{...material,origin:apiOrigin},pairing:{...material,ca,credential_id:identity.credential_id},
    authenticationProvider({scope,source}){return fixture(scope,{source,descriptor:{clock_domain_id:id(),descriptor_digest:digest('synthetic-controller-app-domain')}},{sampleWindowPolicy:'authentication-clock-sample/2'}).provider;}});
   harness.after(()=>controllerApp.close());transport=controllerApp.pairing.transport;api=controllerApp.pairing.api;
   const send=(method,target,body,extraHeaders={})=>new Promise((resolve,reject)=>{
    const bytes=body===undefined?null:Buffer.from(JSON.stringify(body)),request=https.request(apiOrigin+target,{method,ca,rejectUnauthorized:true,agent:false,
     headers:{Origin:apiOrigin,Cookie:'leafseamer_session=v1.'+credential.credential_id+'.'+credential.secret,'X-CSRF-Token':credential.csrf,
      'X-LeafSeamer-Contract':selection.contract_id,'X-LeafSeamer-Contract-Digest':selection.contract_digest,...(bytes?{'Content-Type':'application/json','Content-Length':bytes.length}:{}),...extraHeaders}},response=>{
      const chunks=[];response.on('data',chunk=>chunks.push(chunk));response.on('error',reject);response.on('end',()=>{try{assert.ok([200,201,202].includes(response.statusCode),'controller_http_'+response.statusCode);resolve(JSON.parse(Buffer.concat(chunks)));}catch(error){reject(error);}});
     });request.on('error',reject);request.setTimeout(5000,()=>request.destroy(new Error('controller_test_timeout')));request.end(bytes);
   });
   // 此区间仅构造测试请求期限；实际身份、权限及发送时间仍由启动器自己的原生时钟核验。
   controllerAuth={...auth,real:{current:()=>interval},sessions:{remotePermissions:controllerApp.pairing.remotePermissions,sessionDenials:controllerApp.pairing.sessionDenials,
    async issueToken(_input,request){const reply=await send('POST','/auth/service-tokens',{audience:request.audience,requested_scopes:request.requested_scopes,extension_instance_id:null});
     const [,token_id,secret]=reply.data.access_token.split('.');return {token:{token_id},secret};},
    async logout(){const reply=await send('DELETE','/auth/sessions/current');return {operation_ref:{id:reply.data.operation_ref.id}};}}};
   const own=await send('POST','/auth/service-tokens',{audience:identity.service_id,requested_scopes:['session.read'],extension_instance_id:null});
   assert.equal((await send('GET','/api/v1/session',undefined,{Authorization:'Bearer '+own.data.access_token})).data.session_id,auth.logged.session.session_id);
   const [,token_id,secret]=own.data.access_token.split('.'),token={token_id,secret};
   const endorsements=controllerApp.pairing.runApprovalEndorsements;
   assert.equal(endorsements.revocationDeliveryStatus().state,'running');
   assert.throws(()=>endorsements.createContext(token,{ttl_ms:1000,max_unique_requests:1}),{code:'request_context_permission'});
   assert.throws(()=>endorsements.createRevocationContext(token,{ttl_ms:1000,max_unique_requests:1}),{code:'request_context_permission'});
   assert.throws(()=>endorsements.get(token,id()),{code:'run_endorsement_permission'});
  }else{
  const introspect=createIntrospectionEndpoint({sessions:auth.sessions,serviceId:identity.service_id,serviceBootId:f.store.serviceBootId});
  const reportIntrospection=(phase,error)=>{const value=error?.reason??error?.code;process.send({diagnostic:{phase,name:error?.name??'Error',code:typeof value==='string'&&/^[A-Za-z0-9_:-]{1,128}$/.test(value)?value:'unclassified'}});};
  transport=await startPeerPairingTransport({pairings:api,clock:auth.real,...controllerTransportOptions,sessionDenials:automaticDenials?auth.sessions.sessionDenials:null,
   deviceBindings:bindingFixture?(device,expected)=>{assert.equal(device,bindingFixture.input.device);assert.equal(bindingAvailable,true,'synthetic_binding_unavailable');return bindingFixture.bindings(expected);}:null,
   automationApproval:bindingRead?auth.sessions.currentAutomationServiceApproval:null,
   runEndorsement:runTargets?(_bindingId,_socket,query)=>{
    assert.equal(query.endorsement_id,wireEndorsement?.object_ref.object_id);
    assert.equal(query.endorsement_digest,wireEndorsement?.object_ref.digest);return wireEndorsement;
   }:null,
   runApprovalProtection:runTargets?async(bindingId,socket,notice)=>{
    if(protectionBlocked)throw Error('test_protection_unavailable');
    const receipt=await auth.sessions.runApprovalEndorsements.receiveProtection(bindingId,socket,notice);
    if(firstProtectionReceipt===null&&notice.approval_ref.object_id===dropApprovalId){firstProtectionReceipt=receipt;protectionBlocked=true;socket.destroy();}
    return receipt;
   }:null,
   async introspection(input){try{const reply=await introspect(input);return {...reply,beforeSend(){try{reply.beforeSend();}catch(error){reportIntrospection('introspection_before_send',error);throw error;}}};}catch(error){reportIntrospection('introspection_response',error);throw error;}}});harness.after(()=>transport.close());
  if(runTargets)auth.sessions.runApprovalEndorsements.bindApprovalReader((...args)=>transport.readRunApproval(...args));
  let handler;const server=https.createServer({...material,minVersion:'TLSv1.3'},(req,res)=>{if(!handler.handle(req,res)&&!gui?.handle(req,res)){res.writeHead(404);res.end();}});
  await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(0,'127.0.0.1',resolve);});
  harness.after(()=>new Promise(resolve=>{server.close(resolve);server.closeAllConnections();}));
  apiOrigin='https://127.0.0.1:'+server.address().port;
  controllerOrigin=apiOrigin;controllerIdentity=identity;
  handler=createAuthenticationHttp({serviceId:identity.service_id,databaseInstanceId:identity.database_instance_id,serviceBootId:f.store.serviceBootId,sessions:auth.sessions,origin:apiOrigin});
  }
 }else{
  assert.equal(role,'seamer');const directory=native?.resume.directory??fs.mkdtempSync(path.join(os.tmpdir(),'leafseamer-pair-process-'));
  seamerInitial=input;
  seamerDirectory=directory;seamerIssuer=issuer;
  if(!native){harness.after(()=>{assert.equal(path.dirname(path.resolve(directory)),path.resolve(os.tmpdir()));fs.rmSync(directory,{recursive:true,force:true,maxRetries:5,retryDelay:50});});initializeStorage(directory);}
  const service=migrateStorage(directory,input.scheduledAdmission?'41':input.executionSource?'32':native||armTarget?'27':firstEnrollment?'19':eventApprovals?'17':runTargets?'12':reconcileIngest?'5':snapshotIngest?'4':bindingRead?'3':'2');
  // 双端测试授时装配有固定时代偏移，年龄上限只用于这份临时安装批准。
  const agePolicy=input.executionSource?{event_age_limits:[{...input.executionSource,capture_semantics:'event_occurrence',max_age_ms:90000}],max_condition_nodes:64}:null;
  publication=publicationMaterial({authority_service_id:service.service_id,database_instance_id:service.database_instance_id},
   {optional:true,...(agePolicy&&{triggerReview:{condition:{kind:'literal',value:true},policyDigest:digest(agePolicy)}})});credential='synthetic-seamer-person';let session;
  let publicationPolicyFactory=null;
  if(eventApprovals){
   const approval_file=path.join(directory,'publication-policy.json'),approval={
    format:agePolicy?'leafseamer.publication-policy-approval/2':'leafseamer.publication-policy-approval/1',policy_ref:publication.content.policy_ref,
    not_before_ns:'0',expires_ns:'18446744073709551615',...agePolicy};
   fs.writeFileSync(approval_file,canonical(approval));
   const approval_sha256=digest(approval);
   publicationPolicyFactory=input=>openInstalledPublicationPolicy({approval_file,approval_sha256},input);
  }
  if(runTargets){
   const db=new Database(path.join(directory,'seamer.sqlite'));
   try{db.exec(`CREATE TRIGGER test_abort_run_approval_notice BEFORE INSERT ON automation_run_approval_outbox
    WHEN EXISTS (SELECT 1 FROM automation_run_approval_operation o JOIN automation_run_approval a USING(approval_id)
     JOIN local_request_context c USING(context_id) WHERE o.operation_id=NEW.operation_id AND json_extract(c.body_jcs,'$.context.max_unique_requests')=2)
    BEGIN SELECT RAISE(IGNORE); END;
    CREATE TRIGGER test_abort_run_approval_protection BEFORE INSERT ON automation_run_approval_protection_outbox
    WHEN EXISTS (SELECT 1 FROM automation_run_approval_revocation r JOIN local_request_context c USING(context_id)
     WHERE r.operation_id=NEW.operation_id AND json_extract(c.body_jcs,'$.context.max_unique_requests')=2)
    BEGIN SELECT RAISE(IGNORE); END;
    CREATE TRIGGER test_abort_endorsement_rejection BEFORE INSERT ON automation_run_endorsement_rejection
    WHEN json_extract(NEW.body_jcs,'$.revocation_operation_ref.digest')='${digest('reverse rejection rollback')}'
    BEGIN SELECT RAISE(IGNORE); END`);}finally{db.close();}
  }
  if(!firstEnrollment)seedLocalPermission(directory,service,issuer,principal_id,['auth.manage','auth.read','request.create','request.read','definition.read']);
  const grant=seal({id:id(),revision:'1',issuer_id:issuer.issuer_id,issuer_epoch:issuer.issuer_epoch,state:'active',permissions:['auth.manage']});
  const management={assertCurrent(expected){assert.deepEqual(expected,issuer);},authenticate(input,now){
   assert.equal(input,credential);session??={session_id:id(),principal_id:id(),principal_kind:'human',audience:service.service_id,
    issuer_id:issuer.issuer_id,issuer_epoch:issuer.issuer_epoch,not_before_ns:String(now.lower-1000000000n),expires_ns:String(now.upper+120000000000n),recent_before_ns:String(now.upper+120000000000n)};
   return {session};},current(principal){assert.equal(principal,session.principal_id);return grant;}};
  bindingId=native?.resume.bindingId??id();seamerOptions={dataDir:directory,publicationSource:eventApprovals||bindingRead&&!runTargets?null:{async resolve(){return structuredClone(publication.snapshot);},assertCurrentInTransaction(){return digest(publication.snapshot);}},
   armAdmissionPolicy:armTarget?{inspectInTransaction({observation}){
    const targets=observation.targets.flatMap(target=>target.device_actions.map(action=>action.device_id));
    if(targets.length!==1||targets[0]!==armTarget)return null;
    const profile_ref={id:'fixture.arm.profile',revision:'1',digest:digest('fixture arm budget')};
    const qualification_ref={id:'fixture.arm.qualification',revision:'1',digest:digest(armTarget)};
    const required=agePolicy?{logical_bytes:'131072',queue_slots:'4',rows:'128',work_units:'512'}:{logical_bytes:'4096',queue_slots:'4',rows:'32',work_units:'64'};
    return {profile_ref,qualification_ref,capacity:required,required};
   }}:null,
   publicationPolicyFactory,...(native?{supervisedReference:native.supervision,referenceSamplingAuthority:native.samplingAuthority}:
    receiverReference&&{referenceTimeProvider:receiverReferenceFixture}),
   publicApi:{...material,port:native?.resume.publicPort,binding_id:bindingId,allowedOrigins,localPermissions:firstEnrollment,localEventBindings:eventApprovals,
    localRunApprovals:runTargets||input.scheduledAdmission===true,
    localSourceSubscriptions:input.managedSubscriptions===true,localSchedules:input.scheduledAdmission===true,
    onFailure:value=>process.send?.({diagnostic:value})},authenticationProvider({scope,source,supervisedDomain}){
   const descriptor_digest=digest({scope,provider:'test Seamer process domain'});
   const clock_domain_id=eventApprovals?'clock_'+descriptor_digest.slice(7):id();
   return fixture(scope,{source,descriptor:supervisedDomain??{clock_domain_id,descriptor_digest}},{sampleWindowPolicy:'authentication-clock-sample/2'}).provider;
  },pairing:{...material,ca,issuer,port:native?.resume.priorPort,credential_id:native?.resume.credentialId??id(),...(firstEnrollment?{}:{management})}};
  app=await startSeamer(seamerOptions);
  harness.after(()=>app.close());api=app.pairing.api;transport=app.pairing.transport;identity=Object.fromEntries(['service_id','database_instance_id','service_kind','issuer_id','issuer_epoch','credential_id'].map(key=>[key,transport.configuration[key]]));
  if(native){seamerPeers=native.resume.peers;transport.bindPeers(seamerPeers);}
  publicOrigin='http://127.0.0.1:'+app.address.port;
  apiOrigin=app.publicOrigin;
 }
 return {device_id:bindingFixture?.input.device??null,pid:process.pid,identity,origin:transport.origin,configuration:transport.configuration,publicOrigin,apiOrigin,bindingId,principal_id:controllerAuth?.logged.session.principal_id??principal_id};
}
async function stop(){let failure;for(const fn of cleanup.reverse()){try{await fn();}catch(error){failure??=error;}}if(failure)throw failure;}
async function execute(command,value){
 if(command==='initialize')return initialize(value);
 assert.equal(initialized,true);
 if(nativePeer&&command==='close'){
  const [code,signal]=await nativePeer.close();assert.equal(code,0);assert.equal(signal,null);
  nativePeer=null;appClosed=true;return true;
 }
 if(nativePeer&&command!=='shutdown'){
  const result=await nativePeer.call(command,value);
  return command==='nativeReferenceStatus'?{...result,packet_count:nativePeer.packetCount()}:result;
 }
 if(command==='bind'){transport.bindPeers(value);if(controllerAuth)controllerPeers=value;else seamerPeers=value;return true;}
 if(command==='importFirstPairing'){
  const {envelope,controllerCertificate}=value,authorizationId=envelope.authorization.authorization_id;
  const priorPort=Number(new URL(transport.origin).port),publicPort=Number(new URL(app.publicOrigin).port);
  await app.close();
  const file=(name,bytes)=>{const target=path.join(seamerDirectory,name);fs.writeFileSync(target,bytes);return target;};
  const result=spawnSync(process.execPath,['--disallow-code-generation-from-strings',path.join(packageRoot,'dist/cli.mjs'),
   'import-first-pairing','--data-dir',seamerDirectory,'--authorization-file',file('authorization.json',canonical(envelope)),
   '--controller-certificate',file('controller-pairing.pem',controllerCertificate),
   '--seamer-certificate',file('seamer-pairing.pem',seamerOptions.pairing.cert)],{encoding:'utf8',windowsHide:true,timeout:10000});
  assert.equal(result.status,0,result.stderr||result.error?.message);
  assert.equal(JSON.parse(result.stdout).authorization_id,authorizationId);
  if(seamerInitial.nativeReferenceAssets){
   const {service_id,database_instance_id}=app.identity;
   nativePeer=await startNativeReferenceEventPeer({packageRoot,assets:seamerInitial.nativeReferenceAssets,scope:{service_id,database_instance_id},canonical,digest,
    clock:seamerInitial.referenceTimeline?openNativeClock():null,
    resume:{input:seamerInitial,directory:seamerDirectory,priorPort,publicPort,bindingId:seamerOptions.publicApi.binding_id,
     credentialId:seamerOptions.pairing.credential_id,peers:seamerPeers}});
   harness.after(()=>nativePeer?.close());
   return nativePeer.call('stageImportedPairing',authorizationId);
  }
  app=await startSeamer({...seamerOptions,pairing:{...seamerOptions.pairing,port:priorPort},
   publicApi:{...seamerOptions.publicApi,port:publicPort}});
  api=app.pairing.api;transport=app.pairing.transport;transport.bindPeers(seamerPeers);
  return app.pairing.stageFirstPairing(authorizationId);
 }
 if(command==='stageImportedPairing'&&app.collectorBinding)return app.pairing.stageFirstPairing(value);
 if(command==='nativeReferenceStatus')return {current:app.referenceSampling?.current(),status:app.referenceSampling?.status(),binding:app.collectorBinding,boot:app.serviceBootId};
 if(command==='evaluateNativeAge')return (await app.pairing.snapshotConfirmation.prepareEventAge(value)).commit();
 if(command==='nativeReferenceArchive'){
  assert.ok(appClosed);
  const db=new Database(path.join(seamerDirectory,'seamer.sqlite'),{readonly:true,fileMustExist:true});
  try{
   const row=db.prepare(`SELECT a.body_jcs,a.body_digest,r.body_jcs,r.body_digest,i.observation_jcs,i.observation_digest,z.confirmation_jcs
    FROM time_provider_reference_anchor a JOIN time_provider_runtime_binding r USING(service_boot_id)
    JOIN time_provider_poll_release z USING(poll_id) JOIN subscription_reconcile_inbox i ON i.receiver_boot_id=a.service_boot_id
    WHERE a.anchor_id=? AND i.event_id=?`).raw().get(value.anchor_id,value.event_id);
   const [anchor,anchorDigest,runtime,runtimeDigest,observation,observationDigest,ack]=row;
   for(const [body,hash] of [[anchor,anchorDigest],[runtime,runtimeDigest],[observation,observationDigest]])assert.equal(digest(JSON.parse(body)),hash);
   const ages=db.prepare(`SELECT e.body_jcs,e.body_digest FROM event_age_revision e JOIN subscription_reconcile_inbox i USING(apply_id,cursor)
    WHERE i.event_id=? ORDER BY e.revision`).raw().all(value.event_id).map(([body,hash])=>{
     const age=JSON.parse(body);assert.equal(age.digest,hash);return age;
    });
   return {anchor:JSON.parse(anchor),runtime:JSON.parse(runtime),observation:JSON.parse(observation),ack:JSON.parse(ack),ages};
  }finally{db.close();}
 }
 if(command==='completeFirstPairing')return app.pairing.completeFirstPairing(value);
 if(command==='stageWithIdentity')return api.stage(identityHandles.get(value.identity),value.request);
 if(command==='getWithIdentity')return api.get(identityHandles.get(value.identity),value.binding_id);
 if(command==='activateWithIdentity')return api.activate(identityHandles.get(value.identity),value.request);
 if(command==='prepareEvents'){const handle=await app.pairing.eventReceiver.prepare(value),key=id();eventHandles.set(key,handle);return {handle_id:key,value:app.pairing.eventReceiver.current(handle)};}
 if(command==='currentEvents')return app.pairing.eventReceiver.current(eventHandles.get(value));
 if(command==='releaseEvents'){app.pairing.eventReceiver.release(eventHandles.get(value));eventHandles.delete(value);return true;}
  if(command==='createSubscription')return app.pairing.subscriptions.create(value.binding_id,value.request);
  if(command==='getSubscription')return app.pairing.subscriptions.get(value.binding_id,value.subscription_id);
  if(command==='closeSubscription')return app.pairing.subscriptions.close(value.binding_id,value.subscription_id,value.expected_revision);
  if(command==='detachSubscription'){subscriptionConnections.get(value).close();return true;}
  if(command==='attachSubscription'){const handle=await app.pairing.subscriptions.attach(value.binding_id,value.view,value.options);subscriptionConnections.set(handle.connection_attempt_id,handle);return handle;}
   if(command==='resumeSnapshot'){
    assert.equal(app.pairing.subscriptions.snapshotApplied,undefined);
    await assert.rejects(app.pairing.subscriptions.attach(value.binding_id,{}, {resume:{},beforeSend(){}}),/subscription_resume_requires_receipt/);
    if(value.previous_attempt_id)subscriptionConnections.get(value.previous_attempt_id).close();
    const handle=await app.pairing.snapshotConfirmation.resume(value.binding_id,value.apply_id,value.options);
    subscriptionConnections.set(handle.connection_attempt_id,handle);
    let confirmation;
    for(let i=0;i<50;i++){
     confirmation=await app.pairing.snapshotConfirmation.query(value.binding_id,handle,value.apply_id);
     if(!value.transportReady||confirmation.transport_ready===true)break;
     await new Promise(resolve=>setTimeout(resolve,20));
    }
    return {connection:handle,confirmation,receipt:app.pairing.snapshotIngest.confirmationReceipt(value.apply_id)};
   }
  if(command==='prepareSnapshot'){const handle=await app.pairing.snapshotReceiver.prepare(value.binding_id,subscriptionConnections.get(value.connection_attempt_id));try{return app.pairing.snapshotReceiver.current(handle);}finally{app.pairing.snapshotReceiver.release(handle);}}
  if(command==='snapshotSourceTransport')return (await app.pairing.snapshotConfirmation.prepareSourceTransport(value.apply_id)).observation;
  if(command==='localSourceEvent')return app.pairing.sourceEvent(value.source_key,value.event_id);
  if(command==='localSourceState')return app.pairing.snapshotIngest.sourceState(value);
  if(command==='adoptSnapshot'){const handle=await app.pairing.snapshotReceiver.prepare(value.binding_id,subscriptionConnections.get(value.connection_attempt_id));try{const selected=app.pairing.snapshotReceiver.current(handle),receipt=app.pairing.snapshotIngest.apply(handle);assert.deepEqual(app.pairing.snapshotIngest.apply(handle),receipt);assert.deepEqual(app.pairing.snapshotIngest.lookup(handle),receipt);assert.deepEqual(app.pairing.snapshotIngest.receipt(receipt.apply_id),receipt);assert.deepEqual(app.pairing.snapshotIngest.find({owner:selected.subscription.owner,subscription_id:selected.subscription.binding.subscription_id,snapshot_ref:selected.subscription.snapshot_ref}),receipt);let confirmation=null;if(value.confirm){await app.pairing.snapshotConfirmation.send(value.binding_id,subscriptionConnections.get(value.connection_attempt_id),handle);for(let i=0;i<50;i++){confirmation=await app.pairing.snapshotConfirmation.query(value.binding_id,subscriptionConnections.get(value.connection_attempt_id),receipt.apply_id);if(confirmation.state==='confirmed'&&(!value.transportReady||confirmation.transport_ready===true))break;await new Promise(resolve=>setTimeout(resolve,20));}assert.equal(confirmation.state,'confirmed');assert.deepEqual(await app.pairing.snapshotConfirmation.query(value.binding_id,subscriptionConnections.get(value.connection_attempt_id),receipt.apply_id),confirmation);}return {receipt,confirmation,sources:receipt.source_refs.map(r=>app.pairing.snapshotIngest.sourceState(r.source_key))};}finally{app.pairing.snapshotReceiver.release(handle);}}
  if(command==='subscriptionAttempt')return app.pairing.subscriptions.attempt(value.binding_id,value.subscription_id,value.attempt_id);
  if(command==='subscriptionSnapshot')return app.pairing.subscriptions.snapshot(value.binding_id,value.subscription_id,value.generation);
  if(command==='subscriptionPage')return app.pairing.subscriptions.page(value.binding_id,value.subscription_id,value.snapshot_id,value.page_index,value.generation);
 if(command==='prepareBindings'){const handle=await app.bindingApprovals.prepare(value),key=id();bindingHandles.set(key,handle);return {handle_id:key,value:app.bindingApprovals.inspect(handle)};}
 if(command==='createEventBindingContext')return app.authorization.createRequestContext(identityHandles.get(value.identity),{audience:app.identity.service_id,
  selection,operations:['approveDefinitionBindings','revokeBindingApproval'],ttl_ms:30000,max_unique_requests:value.max_unique_requests});
 if(command==='prepareEventPublication'){
  const actor=identityHandles.get(value.identity),draft=app.drafts.get(actor,value.definition_id);
  const material=publicationMaterial(draft.scope),program=material.content.program,step=program.groups[0].steps[0];
  program.trigger_binding_id=value.event.binding_id;step.action_binding_id=value.action.binding_id;step.parameters={value:12};
  const {context_id}=value;
  const result=app.drafts.update(actor,value.definition_id,{context:{context_id,idempotency_key:id()},expected_draft_revision:draft.revision,
   content:{...material.content,policy_ref:publication.content.policy_ref,selections:[value.event,value.action].map(({kind,binding_id,content_ref})=>({kind,binding_id,content_ref}))}});
  return {context:{context_id,idempotency_key:id()},expected_draft_revision:result.revision,
   expected_content_digest:result.content_digest,expected_selection_digest:result.selection_digest};
 }
 if(command==='readPublishedEventProgram')return app.publications.getProgram(identityHandles.get(value.identity),value.definition_id,value.version);
 if(command==='nativeTriggerExecution'&&seamerInitial.executionSource)
  return app.runs.getExecution(identityHandles.get(value.identity),value.execution_id);
 if(command==='nativeAutomationCommandResource'&&seamerInitial.executionSource){
  const execution=app.runs.getExecution(identityHandles.get(value.identity),value.execution_id);
  const {pairing_ref,request}=execution.steps.find(({plan})=>plan.step_id===value.step_id).submission.body;
  return app.pairing.transport.readAutomationCommandResource(pairing_ref.id,request);
 }
 if(command==='approveBindings')return app.bindingApprovals.approve(identityHandles.get(value.identity),bindingHandles.get(value.handle),value.request);
 if(command==='bindingCandidate')return app.bindingApprovals.candidate(identityHandles.get(value.identity),value.definition_id,value.query);
 if(command==='approveCurrentBindings')return app.bindingApprovals.approveCurrent(identityHandles.get(value.identity),value.query,value.request,value.context??null);
 if(command==='lookupBindingApproval')return app.bindingApprovals.lookup(identityHandles.get(value.identity),value.request);
 if(command==='getBindingApproval')return app.bindingApprovals.get(identityHandles.get(value.identity),value.approval_id);
 if(command==='checkBindingApproval')return app.bindingApprovals.check(identityHandles.get(value.identity),value.approval_ref,value.definition_id,bindingHandles.get(value.handle));
 if(command==='revokeBindingApproval')return app.bindingApprovals.revoke(identityHandles.get(value.identity),value.request);
 if(command==='createAutomationPrincipal')return controllerAuth.sessions.createServicePrincipal(credential,{request_id:id(),display_name:'配对自动化测试',purpose:'automation'});
 if(command==='approveAutomationPrincipal')return controllerAuth.sessions.approveAutomationServicePrincipal(credential,value);
 if(command==='revokeAutomationPrincipal')return controllerAuth.sessions.revokeServicePrincipal(credential,value);
 if(command==='readAutomationApproval')return transport.readAutomationServiceApproval(value.binding_id,value.request);
 if(command==='readPeerRunApprovalProposal')return transport.readRunApprovalProposal(value.binding_id,value.proposal_ref);
 if(command==='readPeerRunApproval')return transport.readRunApproval(value.binding_id,value.approval_ref);
 if(command==='deliverApprovalProtection')return app.pairing.deliverRunApprovalProtection(value.operation_id,value.target_scope);
 if(command==='deliverPeerEndorsementRevocation')return transport.deliverRunEndorsementRevocation(value.binding_id,value.notice);
 if(command==='loseProtectionResponseFor'){dropApprovalId=value;return true;}
 if(command==='approvalProtectionStatus')return app.pairing.runApprovalProtectionStatus();
 if(command==='restoreApprovalProtection'){protectionBlocked=false;return firstProtectionReceipt;}
 if(command==='deliverConflictingApprovalProtection'){
  const {binding_id,notice}=app.runs.prepareApprovalProtection(value.operation_id,value.target_scope);
  return transport.deliverRunApprovalProtection(binding_id,{...notice,endorsement_ref:{...notice.endorsement_ref,digest:digest('wrong endorsement')}});
 }
 if(command==='setWireEndorsement'){wireEndorsement=value;return true;}
 if(command==='readPeerEndorsement')return transport.readRunApprovalEndorsement(value.binding_id,value.endorsement_ref);
 if(command==='readPeerControlObservations')return transport.readRunApprovalControls(value.binding_id,value.endorsement_ref);
 if(command==='preparePeerEndorsement')return (await app.pairing.runEndorsementReader.prepare(value.binding_id,value.endorsement_ref,value.proposal)).original;
 if(command==='checkRunEndorsementPolicy')return controllerApp.pairing.runEndorsementPolicies.check(credential,value);
 if(command==='bindingAvailability'){bindingAvailable=value;return true;}
 if(command==='rotateBindings'){bindingFixture.approved.approval_id=id();bindingFixture.approved.automation_registration=registerDeviceAutomation(bindingFixture.approved,bindingFixture.input.runtime);bindingFixture.save();return true;}
 if(command==='sessionDeliveryStatus')return transport.sessionDenialStatus();
 if(command==='closeController'){
  assert.ok(controllerApp);await controllerApp.close();await controllerApp.close();
  assert.equal(controllerApp.pairing.runApprovalEndorsements.revocationDeliveryStatus().state,'stopped');return controllerApp.authenticationStatus();
 }
 if(command==='restartSessionDelivery'){
  assert.ok(controllerFixture);const port=Number(new URL(transport.origin).port),boot=controllerFixture.store.serviceBootId;
  await transport.close();controllerAuth.real.stop();controllerFixture.close();
  const result=spawnSync(process.execPath,['--disallow-code-generation-from-strings',fileURLToPath(new URL('./session-delivery-restart.mjs',import.meta.url))],
   {input:JSON.stringify({directory:controllerFixture.dir,transport:{...controllerTransportOptions,port},peers:controllerPeers,operation_id:value}),encoding:'utf8',windowsHide:true,timeout:20000});
  assert.equal(result.status,0,result.stderr);const output=JSON.parse(result.stdout);assert.notEqual(output.pid,process.pid);assert.notEqual(output.service_boot_id,boot);return output;
 }
 if(command==='enableGui'){assert.ok(controllerOrigin);assert.equal(gui,null);gui=loadGuiAssets(value,controllerIdentity,controllerOrigin);return true;}
 if(command==='stage')return api.stage(credential,value);
 if(command==='get')return api.get(credential,value);
 if(command==='status')return transport.status(value);
 if(command==='prove')return transport.prove(value);
 if(command==='activate')return api.activate(credential,value);
 if(command==='revoke')return api.revoke(credential,value);
 if(command==='issueIdentity'){
  assert.ok(controllerAuth);const record=api.get(credential,value),now=controllerAuth.real.current(),principal=controllerAuth.logged.session.principal_id;
  const permissions=['auth.manage','auth.read','request.create','request.read','definition.read','definition.edit','definition.publish'];
  if(includeRunScopes)permissions.push('run.approval.revoke','run.approve','run.manage','run.read');
  controllerAuth.sessions.remotePermissions.replace(credential,{request_id:id(),principal_id:principal,audience:record.peer_service_id,
   pairing_ref:{id:record.binding_id,revision:record.revision,digest:record.digest},expected_revision:null,permissions,
   not_before_ns:String(BigInt(now.lower_ns)-1000000000n>BigInt(record.not_before_ns)?BigInt(now.lower_ns)-1000000000n:BigInt(record.not_before_ns)),
   expires_ns:String(BigInt(now.upper_ns)+60000000000n<BigInt(record.expires_ns)?BigInt(now.upper_ns)+60000000000n:BigInt(record.expires_ns))});
  const issued=await controllerAuth.sessions.issueToken(credential,{request_id:id(),audience:record.peer_service_id,requested_scopes:permissions});
  return {token:'v1.'+issued.token.token_id+'.'+issued.secret,principal_id:principal,session_id:controllerAuth.logged.session.session_id,
   recent_before_ns:controllerAuth.logged.session.recent_before_ns};
 }
 if(command==='issueReadIdentity'){
  assert.ok(controllerAuth);const record=api.get(credential,value),issued=await controllerAuth.sessions.issueToken(credential,{request_id:id(),audience:record.peer_service_id,requested_scopes:['auth.read']});
  return 'v1.'+issued.token.token_id+'.'+issued.secret;
 }
 if(command==='prepareIdentity'){
  assert.ok(app);const handle=await app.pairing.identityReceiver.prepare(value),key=id();identityHandles.set(key,handle);
  return {handle_id:key,identity:app.pairing.identityReceiver.current(handle)};
 }
 if(command==='logoutSession'){assert.ok(controllerAuth);return controllerAuth.sessions.logout(credential);}
 if(command==='deliverSessionDenial'){
  assert.ok(controllerAuth);const record=api.currentPending(value.binding_id),bindingRef={id:record.binding_id,revision:record.revision,digest:record.digest};
  if(value.transport_only){const notice=controllerAuth.sessions.sessionDenials.prepare(value.operation_id,bindingRef);return transport.deliverSessionDenial(value.binding_id,notice);}
  return controllerAuth.sessions.sessionDenials.deliver(value.operation_id,bindingRef,transport);
 }
 if(command==='prepareSessionDenial'){
  const record=api.currentPending(value.binding_id);return controllerAuth.sessions.sessionDenials.prepare(value.operation_id,{id:record.binding_id,revision:record.revision,digest:record.digest});
 }
 if(command==='sendSessionDenialNotice')return transport.deliverSessionDenial(value.binding_id,value.notice);
 if(command==='currentIdentity'){assert.ok(app);return app.pairing.identityReceiver.current(identityHandles.get(value));}
 if(command==='useIdentityManagement'){app.pairing.useIdentityManagement();return true;}
 if(command==='identityPairingGet')return api.get(identityHandles.get(value.handle_id),value.binding_id);
 if(command==='identityPairingStage')return api.stage(identityHandles.get(value.handle_id),value.request);
 if(command==='identityPairingActivate')return api.activate(identityHandles.get(value.handle_id),value.request);
 if(command==='identityPairingReject')return api.reject(identityHandles.get(value.handle_id),value.request);
 if(command==='identityPairingRevoke')return api.revoke(identityHandles.get(value.handle_id),value.request);
 if(command==='localGrant')return app.authorization.get(identityHandles.get(value.handle_id),value.principal_id);
 if(command==='replaceLocalGrant')return app.authorization.replace(identityHandles.get(value.handle_id),value.request);
 if(command==='publicationContent')return publication.content;
 if(command==='createDraftContext')return app.authorization.createRequestContext(identityHandles.get(value),{audience:app.identity.service_id,selection,operations:['createDefinition','getDraft'],ttl_ms:60000,max_unique_requests:2});
 if(command==='createDraft'){
  const body={context:{context_id:value.context_id,idempotency_key:value.key},content:{program:source().program,selections:[],policy_ref:{kind:'policy',scope:{authority_service_id:app.identity.service_id,database_instance_id:app.identity.database_instance_id},object_id:'policy',revision:'1',digest:digest({})}},metadata:{name:'实际身份草稿',description:'',layout:{}}};
  return app.drafts.create(identityHandles.get(value.handle_id),body);
 }
 if(command==='readDraft')return app.drafts.get(identityHandles.get(value.handle_id),value.definition_id,value.context_id);
 if(command==='createBoundRun'){
  const actor=identityHandles.get(value.identity),approval=app.bindingApprovals.get(actor,value.approval_id);
  const action=approval.bundle.bindings.find(binding=>binding.kind==='action'),material=publicationMaterial(approval.scope);
  material.snapshot.bindings[1]=structuredClone(action);
  material.snapshot.schemas=approval.bundle.schemas;
  material.snapshot.alias_table=approval.bundle.alias_table;
  material.content.selections=material.snapshot.bindings.map(({kind,binding_id,content_ref})=>({kind,binding_id,content_ref}));
  const step=material.content.program.groups[0].steps[0];step.action_binding_id=action.binding_id;step.parameters={value:12};
  const {review,review_ref}=material.snapshot;review.selection_digest=digest(material.content.selections);review.alias_table_digest=digest(material.snapshot.alias_table);
  review.parameter_schema_refs=material.snapshot.schemas.map(({binding_id,schema_ref})=>({binding_id,schema_ref}));review_ref.digest=digest(review);
  const issued=app.authorization.createRequestContext(actor,{audience:app.identity.service_id,selection,operations:['updateDraft','publishDefinition','createRun'],ttl_ms:30000,max_unique_requests:3});
  const context=()=>({context_id:issued.context_id,idempotency_key:id()});
  const draft=app.drafts.update(actor,approval.definition_id,{context:context(),expected_draft_revision:value.draft_revision,content:material.content});
  const previous=publication;
  try{
   publication=material;
   const published=await app.publications.publish(actor,draft.definition_id,{context:context(),expected_draft_revision:draft.revision,expected_content_digest:draft.content_digest,expected_selection_digest:draft.selection_digest});
   return app.runs.create(actor,{context:context(),definition_id:draft.definition_id,definition_version:published.definition_version,expected_artifact_ref:published.artifact_ref});
  }finally{publication=previous;}
 }
 if(command==='createAnotherPublishedRun'){
  const {run}=value,actor=identityHandles.get(value.identity);
  const issued=app.authorization.createRequestContext(actor,{audience:app.identity.service_id,selection,operations:['createRun'],ttl_ms:30000,max_unique_requests:1});
  return app.runs.create(actor,{context:{context_id:issued.context_id,idempotency_key:id()},definition_id:run.definition_id,definition_version:run.definition_version,expected_artifact_ref:run.artifact_ref});
 }
 if(command==='runDeviceTargets')return app.runs.deviceTargets(identityHandles.get(value.identity),value.run_id);
 if(command==='getRun')return app.runs.get(identityHandles.get(value.identity),value.run_id);
 if(command==='inspectArmPrerequisites')return app.runs.inspectArmPrerequisites(identityHandles.get(value.identity),value.run_id,value.revision);
 if(command==='recordArmPreflight')return app.runs.recordArmPreflight(identityHandles.get(value.identity),value.run_id,value.revision);
 if(command==='getArmPreflight')return app.runs.getArmPreflight(identityHandles.get(value.identity),value.reference);
 if(command==='revalidateArmPreflight')return app.runs.revalidateArmPreflight(identityHandles.get(value.identity),value.reference);
 if(command==='createArmRunContext')return app.authorization.createArmRunContext(identityHandles.get(value),{ttl_ms:30000,max_unique_requests:1});
 if(command==='armRun')return app.runs.armRun(identityHandles.get(value.identity),value.run_id,value.request);
 if(command==='recordAndArmRun'&&seamerInitial.executionSource){
  const identity=()=>app.pairing.identityReceiver.prepare(value.identity_request);
  const context=app.authorization.createArmRunContext(await identity(),{ttl_ms:30000,max_unique_requests:1});
  const requestContext={context_id:context.context_id,idempotency_key:id()};
  if(seamerInitial.scheduledAdmission){
   const actor=await identity();
   const issued=await app.runs.observeAndArmRun(actor,value.run_id,{context:requestContext,expected_run_revision:value.revision});
   const recorded=app.runs.getArmPreflight(await identity(),issued.operation.preflight_ref);
   return {recorded,issued,expiredPreflight:null};
  }
  let expiredPreflight=null;
  for(;;){
   const recorded=await app.runs.recordArmPreflight(await identity(),value.run_id,value.revision);
   const actor=await identity();
   try{
    const issued=await app.runs.armRun(actor,value.run_id,{context:requestContext,preflight_ref:recorded.preflight_ref});
    return {recorded,issued,expiredPreflight};
   }catch(error){
    if(error.code!=='run_preflight_expired'||expiredPreflight)throw error;
    const run=app.runs.get(await identity(),value.run_id);
    assert.equal(run.revision,value.revision);assert.equal(run.lifecycle,'disarmed');assert.equal(run.active_operation_id,null);
    expiredPreflight=recorded;
   }
  }
 }
 if(command==='armPreparationObservations')return app.runs.armPreparationObservations(value);
 if(command==='completeArmRun')return app.runs.completeArmRun(identityHandles.get(value.identity),value.reference);
 if(command==='getActivationSlot')return app.runs.getActivationSlot(identityHandles.get(value.identity),value.definition_id);
 if(command==='createRunApprovalContext')return app.authorization.createRunApprovalContext(identityHandles.get(value),{ttl_ms:30000,max_unique_requests:1});
 if(command==='createApproveRunContext')return app.authorization.createApproveRunContext(identityHandles.get(value.handle_id),{ttl_ms:30000,max_unique_requests:value.max_unique_requests});
 if(command==='createRevokeRunApprovalContext')return app.authorization.createRevokeRunApprovalContext(identityHandles.get(value.handle_id),{ttl_ms:30000,max_unique_requests:value.max_unique_requests});
 if(command==='approveRun')return app.runs.approve(identityHandles.get(value.identity),value.run_id,value.request);
 if(command==='getCurrentRunApproval')return app.runs.getCurrentApproval(identityHandles.get(value.identity),value.run_id);
 if(command==='getRunApprovalRevision')return app.runs.getApprovalRevision(identityHandles.get(value.identity),value.approval_id,value.revision);
 if(command==='getRunApprovalOperation')return app.runs.getApprovalOperation(identityHandles.get(value.identity),value.operation_id);
 if(command==='lookupRunApproval')return app.runs.lookupApproval(identityHandles.get(value.identity),value.request);
 if(command==='revokeRunApproval')return app.runs.revokeApproval(identityHandles.get(value.identity),value.approval_id,value.request);
 if(command==='lookupRunApprovalRevocation')return app.runs.lookupApprovalRevocation(identityHandles.get(value.identity),value.request);
 if(command==='createRunApprovalProposal')return app.runs.createApprovalProposal(identityHandles.get(value.identity),value.run_id,value.request);
 if(command==='getRunApprovalProposal')return app.runs.getApprovalProposal(identityHandles.get(value.identity),value.proposal_id);
 if(command==='lookupRunApprovalProposal')return app.runs.lookupApprovalProposal(identityHandles.get(value.identity),value.request);
 if(command==='close'){assert.ok(app);await app.close();appClosed=true;return true;}
 if(command==='restartRunProtectionService'){
  assert.ok(appClosed&&seamerPeers);const boot=app.serviceBootId,port=Number(new URL(transport.origin).port);
  app=await startSeamer({...seamerOptions,pairing:{...seamerOptions.pairing,port}});appClosed=false;
  api=app.pairing.api;transport=app.pairing.transport;transport.bindPeers(seamerPeers);
  assert.notEqual(app.serviceBootId,boot);
  return {schema_revision:app.identity.schema_revision,public_origin:app.publicOrigin};
 }
 if(command==='checkDurableApprovalProtection'){
  assert.ok(appClosed);const db=new Database(path.join(seamerDirectory,'seamer.sqlite'),{readonly:true});
  try{
   const [text,hash]=db.prepare('SELECT body_jcs,body_digest FROM automation_run_approval_protection_outbox WHERE operation_id=?').raw().get(value);
   const notice=JSON.parse(text);assert.equal(digest(notice),hash);
   const stored=db.prepare('SELECT body_jcs,body_digest FROM automation_run_approval_protection_receipt WHERE operation_id=?').raw().get(value);
   const receipt=stored?JSON.parse(stored[0]):null;
   if(stored)assert.equal(digest(receipt),stored[1]);assert.deepEqual(db.pragma('foreign_key_check'),[]);return {notice,receipt};
  }finally{db.close();}
 }
 if(command==='readDurableEventBindingApproval'){
  assert.ok(appClosed);const db=new Database(path.join(seamerDirectory,'seamer.sqlite'),{readonly:true});
  try{
   assert.deepEqual(db.pragma('foreign_key_check'),[]);
   const rows=db.prepare('SELECT approval_id,revision,body_jcs,body_digest FROM event_binding_approval_revision WHERE approval_id=? ORDER BY revision').raw().all(value);
   return rows.map(([id,revision,text,hash])=>{assert.equal(digest(JSON.parse(text)),hash);return {id,revision:String(revision),digest:hash};});
  }finally{db.close();}
 }
 if(command==='readDurableArmPreflights'){
  assert.ok(appClosed);const db=new Database(path.join(seamerDirectory,'seamer.sqlite'),{readonly:true});
  try{
   assert.deepEqual(db.pragma('foreign_key_check'),[]);
   const rows=db.prepare('SELECT body_jcs,body_digest FROM automation_run_preflight WHERE run_id=? ORDER BY preflight_id').raw().all(value);
   return rows.map(([text,hash])=>{const preflight=JSON.parse(text);assert.equal(digest(preflight),hash);return preflight;});
  }finally{db.close();}
 }
 if(command==='readDurableRunApprovalProtection'){
  assert.ok(appClosed);const db=new Database(path.join(seamerDirectory,'seamer.sqlite'),{readonly:true});
  try{
   const [text,hash]=db.prepare('SELECT body_jcs,body_digest FROM automation_run_approval_protection_completion WHERE operation_id=?').raw().get(value);
   const body=JSON.parse(text);assert.equal(digest(body),hash);return body;
  }finally{db.close();}
 }
 if(command==='readDurablePublicationApprovals'){
  assert.ok(appClosed);const db=new Database(path.join(seamerDirectory,'seamer.sqlite'),{readonly:true});
  try{
   const [text,hash]=db.prepare('SELECT bundle_jcs,bundle_digest FROM definition_publication WHERE definition_id=? AND definition_version=?').raw().get(value.definition_id,value.version);
   const bundle=JSON.parse(text);assert.equal(digest(bundle),hash);assert.equal(digest(bundle.snapshot.review),bundle.artifact.review_ref.digest);
   const approvals=bundle.snapshot.review.binding_approval_refs;
   const pins=db.prepare(`SELECT kind,approval_id,approval_revision,approval_digest FROM definition_publication_binding_pin
    WHERE definition_id=? AND definition_version=? ORDER BY kind,approval_id`).raw().all(value.definition_id,value.version);
   assert.deepEqual(pins,approvals.map(({kind,approval_ref})=>[kind,approval_ref.id,approval_ref.revision,approval_ref.digest]).sort());
   return approvals;
  }finally{db.close();}
 }
 if(command==='closedWatermark'){
  assert.ok(appClosed);const result=spawnSync(process.execPath,['--disallow-code-generation-from-strings',fileURLToPath(new URL('./identity-watermark-read.mjs',import.meta.url)),packageRoot,seamerDirectory,JSON.stringify(seamerIssuer),...(value?.session_id?[value.session_id]:[])],{encoding:'utf8',windowsHide:true,timeout:10000});
  assert.equal(result.status,0,result.stderr);const read=JSON.parse(result.stdout);assert.notEqual(read.service_boot_id,app.serviceBootId);return value?.session_id?read:read.floor;
 }
 if(command==='shutdown'){try{await nativePeer?.call('shutdown');}finally{await stop();}return true;}
 throw new Error('fixture_command');
}
let tail=Promise.resolve();
if(process.send)process.on('message',message=>{tail=tail.then(async()=>{
 try{const value=await execute(message.command,message.value);process.send({id:message.id,ok:true,value},()=>{if(message.command==='shutdown')process.disconnect();});}
 catch(error){process.send({id:message.id,ok:false,error:{code:error.code??null,message:error.message,stack:message.command+': '+(error.remoteStack??error.stack)}});}
});});

export async function serveSupervisedPairing({resume,request,response,endpoint}){
 const {createSupervisedReference}=await import(pathToFileURL(path.join(packageRoot,'dist/supervised-reference.mjs')));
 const [, , command,...args]=process.argv;
 assert.equal(command,'contained-core-auth');assert.equal(args.length%2,0);
 const options=Object.fromEntries(args.filter((_,index)=>index%2===0).map((name,index)=>[name,args[index*2+1]]));
 let stopping=false;
 const stopRequested=()=>{stopping=true;};
 const supervision=createSupervisedReference(options,{input:process.stdin,output:process.stdout,onLost:stopRequested,onAdmitted(){
  process.stdin.once('data',stopRequested);process.stdin.once('end',stopRequested);process.stdin.resume();
 }});
 const timeline=resume.input.referenceTimeline;
 const eraBounds=timeline?{lower_ns:String(BigInt(timeline.utc_ns)-1000000000n),upper_ns:String(BigInt(timeline.utc_ns)+180000000000n)}:null;
 const samplingAuthority=referenceSamplingFixture({endpoint,digest,bundleAuthority:referenceBundleAuthority,utcOffsetNs:60000000000n,eraBounds});
 let active={id:'initial',command:'initialize'};
 function reply(ok,value){writeNativePeerMessage(response,{id:active.id,ok,...(ok?{value}:{error:{code:value.code??null,message:value.message,stack:value.stack}})});}
 try{
  reply(true,await initialize(resume.input,{resume,supervision,samplingAuthority}));
  while(!stopping){
   if(!fs.existsSync(request)){await delay(10);continue;}
   active=JSON.parse(fs.readFileSync(request));fs.unlinkSync(request);
   try{reply(true,await execute(active.command,active.value));}
   catch(error){reply(false,error);}
   if(active.command==='shutdown')return;
  }
  await stop();
 }catch(error){reply(false,error);throw error;}
 finally{try{await app?.close();}finally{await supervision.close();process.stdin.destroy();}}
}
