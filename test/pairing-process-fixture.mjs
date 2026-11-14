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
import {setup} from '../../leafseamer-controller/test/fixtures/extension-installation.mjs';
import {authenticated} from '../../leafseamer-controller/test/fixtures/extension-auth.mjs';
import {openNativeClock} from '../../leafseamer-controller/dist/native-clock.mjs';
import {startPeerPairingTransport} from '../../leafseamer-controller/dist/peer-pairing-transport.mjs';
import {createIntrospectionEndpoint} from '../../leafseamer-controller/dist/introspection-http.mjs';
import {createAuthenticationHttp} from '../../leafseamer-controller/dist/auth-http.mjs';
import {loadGuiAssets} from '../../leafseamer-controller/dist/gui-assets.mjs';
import {seal,digest} from '../dist/runtime/auth-clock-value.mjs';
import {fixture} from './auth-clock-fixture.mjs';
import {seedLocalPermission} from './local-authorization-fixture.mjs';
import {source} from './automation-program-fixture.mjs';
import {registered as registeredBindings} from '../../leafseamer-controller/test/fixtures/device-bindings.mjs';
import {registerDeviceAutomation} from '../../leafseamer-controller/dist/device-automation-binding.mjs';
import {publicationMaterial} from './automation-publication-fixture.mjs';
import {contractSelection as selection} from '../dist/sdk/index.mjs';
const packageRoot=process.env.LEAFSEAMER_PACKAGE_UNDER_TEST??fileURLToPath(new URL('../',import.meta.url));assert.ok(path.isAbsolute(packageRoot));
const {initializeStorage,migrateStorage}=await import(pathToFileURL(path.join(packageRoot,'dist/storage.mjs')));
const {startSeamer}=await import(pathToFileURL(path.join(packageRoot,'dist/server.mjs')));
const controllerPackage=process.env.LEAFSEAMER_CONTROLLER_PACKAGE_UNDER_TEST;
if(controllerPackage)assert.ok(path.isAbsolute(controllerPackage));
const {startController}=await import(controllerPackage?pathToFileURL(path.join(controllerPackage,'dist/server.mjs')):new URL('../../leafseamer-controller/dist/server.mjs',import.meta.url));
const id=()=>crypto.randomUUID(),cleanup=[],harness={after:fn=>cleanup.push(fn)};
let api,transport,credential,app,controllerAuth,initialized=false;
let gui=null,controllerOrigin=null,controllerIdentity=null;
let seamerDirectory=null,seamerIssuer=null,appClosed=false;
let controllerFixture=null,controllerTransportOptions=null,controllerPeers=null;
let controllerApp=null,publication=null,bindingFixture=null,bindingAvailable=true;const bindingHandles=new Map();
const identityHandles=new Map();
const eventHandles=new Map();
const subscriptionConnections=new Map();
async function initialize({role,material,ca,issuer,principal_id,allowedOrigins=[],automaticDenials=false,productController=false,bindingRead=false,snapshotIngest=process.env.LEAFSEAMER_SEAMER_SCHEMA4_TEST==='1'}){
 assert.equal(initialized,false);initialized=true;
 let identity,publicOrigin=null,apiOrigin=null,bindingId=null;
 if(role==='controller'){
  const f=await setup(harness,62),capability=f.store.enrollment.begin();controllerFixture=f;
  await f.store.enrollment.complete({token_id:capability.token_id,secret:capability.secret,login:'role.test',password:'Synthetic-role-password-123',mode:'standalone'});
  const auth=await authenticated(harness,f,{grant:false,native:{source:openNativeClock(),descriptor:{clock_domain_id:id(),descriptor_digest:digest('synthetic-controller-process-domain')}},sampleWindowPolicy:'authentication-clock-sample/2'});
  controllerAuth=auth;credential=auth.auth;api=auth.sessions.peerPairings;identity={service_id:f.store.identity.service_id,database_instance_id:f.store.identity.database_instance_id,
   service_kind:'controller',issuer_id:auth.logged.session.issuer_id,issuer_epoch:auth.logged.session.issuer_epoch,credential_id:id()};
  controllerTransportOptions={identity,...material,ca};
  if(bindingRead)bindingFixture=registeredBindings(harness,{service:{service_id:identity.service_id,database_instance_id:identity.database_instance_id},device:id()});
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
  }else{
  const introspect=createIntrospectionEndpoint({sessions:auth.sessions,serviceId:identity.service_id,serviceBootId:f.store.serviceBootId});
  const reportIntrospection=(phase,error)=>{const value=error?.reason??error?.code;process.send({diagnostic:{phase,name:error?.name??'Error',code:typeof value==='string'&&/^[A-Za-z0-9_:-]{1,128}$/.test(value)?value:'unclassified'}});};
  transport=await startPeerPairingTransport({pairings:api,clock:auth.real,...controllerTransportOptions,sessionDenials:automaticDenials?auth.sessions.sessionDenials:null,
   deviceBindings:bindingFixture?(device,expected)=>{assert.equal(device,bindingFixture.input.device);assert.equal(bindingAvailable,true,'synthetic_binding_unavailable');return bindingFixture.bindings(expected);}:null,
   async introspection(input){try{const reply=await introspect(input);return {...reply,beforeSend(){try{reply.beforeSend();}catch(error){reportIntrospection('introspection_before_send',error);throw error;}}};}catch(error){reportIntrospection('introspection_response',error);throw error;}}});harness.after(()=>transport.close());
  let handler;const server=https.createServer({...material,minVersion:'TLSv1.3'},(req,res)=>{if(!handler.handle(req,res)&&!gui?.handle(req,res)){res.writeHead(404);res.end();}});
  await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(0,'127.0.0.1',resolve);});
  harness.after(()=>new Promise(resolve=>{server.close(resolve);server.closeAllConnections();}));
  apiOrigin='https://127.0.0.1:'+server.address().port;
  controllerOrigin=apiOrigin;controllerIdentity=identity;
  handler=createAuthenticationHttp({serviceId:identity.service_id,databaseInstanceId:identity.database_instance_id,serviceBootId:f.store.serviceBootId,sessions:auth.sessions,origin:apiOrigin});
  }
 }else{
  assert.equal(role,'seamer');const directory=fs.mkdtempSync(path.join(os.tmpdir(),'leafseamer-pair-process-'));
  seamerDirectory=directory;seamerIssuer=issuer;
  harness.after(()=>{assert.equal(path.dirname(path.resolve(directory)),path.resolve(os.tmpdir()));fs.rmSync(directory,{recursive:true,force:true,maxRetries:5,retryDelay:50});});
  const service=initializeStorage(directory);migrateStorage(directory,snapshotIngest?'4':bindingRead?'3':'2');publication=publicationMaterial({authority_service_id:service.service_id,database_instance_id:service.database_instance_id},{optional:true});credential='synthetic-seamer-person';let session;
  seedLocalPermission(directory,service,issuer,principal_id,['auth.manage','auth.read','request.create','request.read','definition.read']);
  const grant=seal({id:id(),revision:'1',issuer_id:issuer.issuer_id,issuer_epoch:issuer.issuer_epoch,state:'active',permissions:['auth.manage']});
  const management={assertCurrent(expected){assert.deepEqual(expected,issuer);},authenticate(input,now){
   assert.equal(input,credential);session??={session_id:id(),principal_id:id(),principal_kind:'human',audience:service.service_id,
    issuer_id:issuer.issuer_id,issuer_epoch:issuer.issuer_epoch,not_before_ns:String(now.lower-1000000000n),expires_ns:String(now.upper+120000000000n),recent_before_ns:String(now.upper+120000000000n)};
   return {session};},current(principal){assert.equal(principal,session.principal_id);return grant;}};
  bindingId=id();app=await startSeamer({dataDir:directory,publicationSource:bindingRead?null:{async resolve(){return structuredClone(publication.snapshot);},assertCurrentInTransaction(){return digest(publication.snapshot);}},publicApi:{...material,binding_id:bindingId,allowedOrigins,onFailure:value=>process.send({diagnostic:value})},authenticationProvider({scope,source}){
   return fixture(scope,{source,descriptor:{clock_domain_id:id(),descriptor_digest:digest('synthetic-seamer-process-domain')}},{sampleWindowPolicy:'authentication-clock-sample/2'}).provider;
  },pairing:{...material,ca,issuer,credential_id:id(),management}});
  harness.after(()=>app.close());api=app.pairing.api;transport=app.pairing.transport;identity=Object.fromEntries(['service_id','database_instance_id','service_kind','issuer_id','issuer_epoch','credential_id'].map(key=>[key,transport.configuration[key]]));
  publicOrigin='http://127.0.0.1:'+app.address.port;
  apiOrigin=app.publicOrigin;
 }
 return {device_id:bindingFixture?.input.device??null,pid:process.pid,identity,origin:transport.origin,configuration:transport.configuration,publicOrigin,apiOrigin,bindingId,principal_id:controllerAuth?.logged.session.principal_id??principal_id};
}
async function stop(){let failure;for(const fn of cleanup.reverse()){try{await fn();}catch(error){failure??=error;}}if(failure)throw failure;}
async function execute(command,value){
 if(command==='initialize')return initialize(value);
 assert.equal(initialized,true);
 if(command==='bind'){transport.bindPeers(value);if(controllerAuth)controllerPeers=value;return true;}
 if(command==='prepareEvents'){const handle=await app.pairing.eventReceiver.prepare(value),key=id();eventHandles.set(key,handle);return {handle_id:key,value:app.pairing.eventReceiver.current(handle)};}
 if(command==='currentEvents')return app.pairing.eventReceiver.current(eventHandles.get(value));
 if(command==='releaseEvents'){app.pairing.eventReceiver.release(eventHandles.get(value));eventHandles.delete(value);return true;}
  if(command==='createSubscription')return app.pairing.subscriptions.create(value.binding_id,value.request);
  if(command==='getSubscription')return app.pairing.subscriptions.get(value.binding_id,value.subscription_id);
  if(command==='closeSubscription')return app.pairing.subscriptions.close(value.binding_id,value.subscription_id,value.expected_revision);
  if(command==='attachSubscription'){const handle=await app.pairing.subscriptions.attach(value.binding_id,value.view,value.options);subscriptionConnections.set(handle.connection_attempt_id,handle);return handle;}
  if(command==='prepareSnapshot'){const handle=await app.pairing.snapshotReceiver.prepare(value.binding_id,subscriptionConnections.get(value.connection_attempt_id));try{return app.pairing.snapshotReceiver.current(handle);}finally{app.pairing.snapshotReceiver.release(handle);}}
  if(command==='adoptSnapshot'){const handle=await app.pairing.snapshotReceiver.prepare(value.binding_id,subscriptionConnections.get(value.connection_attempt_id));try{const selected=app.pairing.snapshotReceiver.current(handle),receipt=app.pairing.snapshotIngest.apply(handle);assert.deepEqual(app.pairing.snapshotIngest.apply(handle),receipt);assert.deepEqual(app.pairing.snapshotIngest.lookup(handle),receipt);assert.deepEqual(app.pairing.snapshotIngest.receipt(receipt.apply_id),receipt);assert.deepEqual(app.pairing.snapshotIngest.find({owner:selected.subscription.owner,subscription_id:selected.subscription.binding.subscription_id,snapshot_ref:selected.subscription.snapshot_ref}),receipt);let confirmation=null;if(value.confirm){await app.pairing.snapshotConfirmation.send(value.binding_id,subscriptionConnections.get(value.connection_attempt_id),handle);for(let i=0;i<50;i++){confirmation=await app.pairing.snapshotConfirmation.query(value.binding_id,subscriptionConnections.get(value.connection_attempt_id),receipt.apply_id);if(confirmation.state==='confirmed')break;await new Promise(resolve=>setTimeout(resolve,20));}assert.equal(confirmation.state,'confirmed');assert.deepEqual(await app.pairing.snapshotConfirmation.query(value.binding_id,subscriptionConnections.get(value.connection_attempt_id),receipt.apply_id),confirmation);}return {receipt,confirmation,sources:receipt.source_refs.map(r=>app.pairing.snapshotIngest.sourceState(r.source_key))};}finally{app.pairing.snapshotReceiver.release(handle);}}
  if(command==='subscriptionAttempt')return app.pairing.subscriptions.attempt(value.binding_id,value.subscription_id,value.attempt_id);
  if(command==='subscriptionSnapshot')return app.pairing.subscriptions.snapshot(value.binding_id,value.subscription_id,value.generation);
  if(command==='subscriptionPage')return app.pairing.subscriptions.page(value.binding_id,value.subscription_id,value.snapshot_id,value.page_index,value.generation);
 if(command==='prepareBindings'){const handle=await app.bindingApprovals.prepare(value),key=id();bindingHandles.set(key,handle);return {handle_id:key,value:app.bindingApprovals.inspect(handle)};}
 if(command==='approveBindings')return app.bindingApprovals.approve(identityHandles.get(value.identity),bindingHandles.get(value.handle),value.request);
 if(command==='bindingCandidate')return app.bindingApprovals.candidate(identityHandles.get(value.identity),value.definition_id,value.query);
 if(command==='approveCurrentBindings')return app.bindingApprovals.approveCurrent(identityHandles.get(value.identity),value.query,value.request);
 if(command==='lookupBindingApproval')return app.bindingApprovals.lookup(identityHandles.get(value.identity),value.request);
 if(command==='getBindingApproval')return app.bindingApprovals.get(identityHandles.get(value.identity),value.approval_id);
 if(command==='checkBindingApproval')return app.bindingApprovals.check(identityHandles.get(value.identity),value.approval_ref,value.definition_id,bindingHandles.get(value.handle));
 if(command==='revokeBindingApproval')return app.bindingApprovals.revoke(identityHandles.get(value.identity),value.request);
 if(command==='bindingAvailability'){bindingAvailable=value;return true;}
 if(command==='rotateBindings'){bindingFixture.approved.approval_id=id();bindingFixture.approved.automation_registration=registerDeviceAutomation(bindingFixture.approved,bindingFixture.input.runtime);bindingFixture.save();return true;}
 if(command==='sessionDeliveryStatus')return transport.sessionDenialStatus();
 if(command==='closeController'){assert.ok(controllerApp);await controllerApp.close();await controllerApp.close();return controllerApp.authenticationStatus();}
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
  controllerAuth.sessions.remotePermissions.replace(credential,{request_id:id(),principal_id:principal,audience:record.peer_service_id,
   pairing_ref:{id:record.binding_id,revision:record.revision,digest:record.digest},expected_revision:null,permissions:['auth.manage','auth.read','request.create','request.read','definition.read','definition.edit','definition.publish'],
   not_before_ns:String(BigInt(now.lower_ns)-1000000000n>BigInt(record.not_before_ns)?BigInt(now.lower_ns)-1000000000n:BigInt(record.not_before_ns)),
   expires_ns:String(BigInt(now.upper_ns)+60000000000n<BigInt(record.expires_ns)?BigInt(now.upper_ns)+60000000000n:BigInt(record.expires_ns))});
  const issued=await controllerAuth.sessions.issueToken(credential,{request_id:id(),audience:record.peer_service_id,requested_scopes:['auth.manage','auth.read','request.create','request.read','definition.read','definition.edit','definition.publish']});
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
 if(command==='close'){assert.ok(app);await app.close();appClosed=true;return true;}
 if(command==='closedWatermark'){
  assert.ok(appClosed);const result=spawnSync(process.execPath,['--disallow-code-generation-from-strings',fileURLToPath(new URL('./identity-watermark-read.mjs',import.meta.url)),packageRoot,seamerDirectory,JSON.stringify(seamerIssuer),...(value?.session_id?[value.session_id]:[])],{encoding:'utf8',windowsHide:true,timeout:10000});
  assert.equal(result.status,0,result.stderr);const read=JSON.parse(result.stdout);assert.notEqual(read.service_boot_id,app.serviceBootId);return value?.session_id?read:read.floor;
 }
 if(command==='shutdown'){await stop();return true;}
 throw new Error('fixture_command');
}
let tail=Promise.resolve();
process.on('message',message=>{tail=tail.then(async()=>{
 try{const value=await execute(message.command,message.value);process.send({id:message.id,ok:true,value},()=>{if(message.command==='shutdown')process.disconnect();});}
 catch(error){process.send({id:message.id,ok:false,error:{code:error.code??null,message:error.message,stack:message.command+': '+error.stack}});}
});});
