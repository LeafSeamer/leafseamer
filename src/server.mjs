import http from 'node:http';
import {openStorage} from './storage.mjs';
import {openNativeClock} from './runtime/native-clock.mjs';
import {startAuthenticationClock} from './runtime/auth-clock.mjs';
import {startPeerPairingTransport} from './runtime/peer-pairing-transport.mjs';
import {startOutboxDelivery} from './runtime/session-denial-delivery.mjs';
import {startExecutionNotificationDelivery} from './automation-execution-notification.mjs';
import {createPeerSubscriptionClient} from './peer-subscription-client.mjs';
import {createPeerSnapshotReceiver} from './peer-snapshot-receiver.mjs';
import {createPeerSnapshotConfirmation} from './peer-snapshot-confirmation.mjs';
import {pairingUuid} from './runtime/peer-pairing-value.mjs';
import {createPeerIdentityReceiver} from './peer-identity.mjs';
import {createPeerBindingReceiver} from './peer-binding-receiver.mjs';
import {createPeerEventReceiver} from './peer-event-receiver.mjs';
import {createPeerServiceApprovalReader} from './peer-service-approval.mjs';
import {createPeerRunApprovalEndorsementReader} from './peer-run-approval-endorsement.mjs';
import {assertProtectedDraftAddress,startDraftHttp} from './draft-http.mjs';
import {fields,same} from './runtime/auth-clock-value.mjs';
import {contractSelection} from './sdk/index.mjs';

export async function startSeamer({dataDir,host='127.0.0.1',port=0,authenticationProvider=null,referenceTimeProvider=null,
 supervisedReference=null,referenceSamplingAuthority=null,onStartupFailure=null,pairing=null,publicApi=null,publicationSource=null,publicationPolicy=null,publicationPolicyFactory=null,
 armAdmissionPolicy=null,executionNotificationSink=null,protectedIngress=null}={}){
 if(!['127.0.0.1','::1'].includes(host)||!Number.isInteger(port)||port<0||port>65535)throw new Error('listen_options');
 if(authenticationProvider!==null&&typeof authenticationProvider!=='function')throw new Error('authentication_provider');
 if(referenceTimeProvider!==null&&(typeof referenceTimeProvider!=='function'||pairing===null))throw new Error('receiver_reference_provider');
 if(supervisedReference!==null&&(authenticationProvider===null||pairing===null))throw new Error('reference_supervisor_options');
 if(referenceSamplingAuthority!==null&&(typeof referenceSamplingAuthority!=='function'||supervisedReference===null||referenceTimeProvider!==null))
  throw new Error('reference_sampling_authority');
 if(publicationPolicyFactory!==null&&(publicationPolicy!==null||publicationSource!==null||typeof publicationPolicyFactory!=='function'))
  throw new Error('publication_source_configuration');
 if(publicApi!==null){if(pairing===null)throw new Error('public_api_pairing_required');pairingUuid(publicApi.binding_id);}
 if(protectedIngress!==null){
  if([authenticationProvider,referenceTimeProvider,supervisedReference,referenceSamplingAuthority,pairing,publicApi,publicationSource,
   publicationPolicy,publicationPolicyFactory,armAdmissionPolicy,executionNotificationSink].some(value=>value!==null))throw new Error('protected_ingress_options');
  fields(protectedIngress.identity,['service_id','database_instance_id']);
  pairingUuid(protectedIngress.identity.service_id);pairingUuid(protectedIngress.identity.database_instance_id);
  assertProtectedDraftAddress(protectedIngress.host,protectedIngress.port);
 }
 const notificationSink=executionNotificationSink===null?null:{consumerId:executionNotificationSink?.consumerId,deliver:executionNotificationSink?.deliver};
 if(notificationSink!==null){
  pairingUuid(notificationSink.consumerId);
  if(typeof notificationSink.deliver!=='function')throw new Error('execution_notification_sink');
 }
 let authentication,installedPolicy,referenceProvider=null,collectorBinding=null,sampling=null;
 const policy=publicationPolicyFactory===null?publicationPolicy:{assertCurrentInTransaction(reference){return installedPolicy.assertCurrentInTransaction(reference);},
  eventAgePolicy(){return installedPolicy?.eventAgePolicy?.()??null;}};
 const storage=openStorage(dataDir,{publicationSource,publicationPolicy:policy,armAdmissionPolicy,expectedIdentity:protectedIngress?.identity??null});
 let executionNotifications=null,executionNotificationDelivery=null;
 const closeFailedStartup=async error=>{
  // 监督管道关闭后宿主可立即回收核心，先交付已分类的启动诊断。
  try{await onStartupFailure?.(error);}catch{/* 不用诊断写入错误替换原启动失败。 */}
  try{await supervisedReference?.close();}finally{try{authentication?.stop();}finally{storage.close();}}
 };
 try{
  const source=openNativeClock(),identity=storage.identity;
  if(notificationSink)executionNotifications=storage.bindExecutionNotifications(notificationSink.consumerId);
  if(supervisedReference&&Number(identity.schema_revision)<20)throw new Error('reference_supervisor_schema');
  if(referenceSamplingAuthority&&Number(identity.schema_revision)<21)throw new Error('reference_sampling_schema');
  const supervisedDomain=await supervisedReference?.admit({identity,source});
  const provider=await authenticationProvider?.({scope:storage.authenticationClock.scope,identity,source,...(supervisedDomain&&{supervisedDomain})});
  authentication=startAuthenticationClock(source,storage.authenticationClock,provider??null);
  const openSampling=referenceSamplingAuthority===null?null:async input=>{
   const authority=await referenceSamplingAuthority({identity,serviceBootId:storage.serviceBootId,source,supervisedDomain});
   return storage.openReferenceSampling({...input,authority});
  };
  const collectorRuntime=await supervisedReference?.connect({identity,serviceBootId:storage.serviceBootId,authentication,adopt:storage.adoptReferenceRuntime,openSampling});
  collectorBinding=collectorRuntime?.binding??null;
  sampling=collectorRuntime?.sampling??null;
  referenceProvider=sampling?.receiverReference??null;
  if(referenceTimeProvider!==null){
   // 只从受信本机装配取得参考原件。认证时钟提供同域单调时刻，不提供事件 UTC 界。
   if(Number(storage.identity.schema_revision)<5)throw new Error('receiver_reference_provider');
   referenceProvider=await referenceTimeProvider({identity:storage.identity,serviceBootId:storage.serviceBootId,source});
   if(typeof referenceProvider?.readFirst!=='function'||typeof referenceProvider?.isCurrent!=='function')throw new Error('receiver_reference_provider');
  }
  if(publicationPolicyFactory!==null){
   if(Number(storage.identity.schema_revision)<11||pairing===null)throw new Error('pairing_profile_publication_policy_unavailable');
   installedPolicy=publicationPolicyFactory({identity:storage.identity,clock:authentication});
   if(typeof installedPolicy?.assertCurrentInTransaction!=='function')throw new Error('publication_source_configuration');
  }
 }
 catch(error){await closeFailedStartup(error);throw error;}
 const server=http.createServer({maxHeaderSize:8192,requestTimeout:5000,headersTimeout:5000,keepAliveTimeout:1000},(req,res)=>{
  res.setHeader('Cache-Control','no-store');res.setHeader('X-Content-Type-Options','nosniff');
  if(req.headers['transfer-encoding']!==undefined||(req.headers['content-length']!==undefined&&req.headers['content-length']!=='0')){res.writeHead(413,{Connection:'close'});res.end();return;}
  if(!['GET','HEAD'].includes(req.method)){res.writeHead(405,{Allow:'GET, HEAD'});res.end();return;}
  const health=req.url==='/health/live'?{status:200,body:{status:'alive'}}:req.url==='/health/ready'?{status:503,body:{ready:false,reason:authentication.status().qualified?'business_incomplete':'authentication_clock_unqualified'}}:null;
  if(!health){res.writeHead(req.url==='/.well-known/leafseamer-contracts'?503:404);res.end();return;}
  const body=Buffer.from(JSON.stringify(health.body));res.writeHead(health.status,{'Content-Type':'application/json','Content-Length':body.length});res.end(req.method==='HEAD'?undefined:body);
 });
 server.maxConnections=32;server.maxRequestsPerSocket=100;server.on('clientError',(_error,socket)=>socket.destroy());
 try{await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(port,host,resolve);});}
 catch(error){server.close();await closeFailedStartup(error);throw error;}
 const closeHttp=()=>new Promise((resolve,reject)=>{server.close(error=>error?reject(error):resolve());server.closeAllConnections();});
 let peerTransport=null,peerApi=null,identityReceiver=null,authorization=null,publicTransport=null,management=null,bindingReceiver=null,bindingApprovals=null,eventReceiver=null,subscriptions=null,snapshotReceiver=null,snapshotIngest=null,reconcile=null,snapshotConfirmation=null,runEndorsementReader=null,runProtectionDelivery=null;
 let armPreparationDelivery=null;
 let bindingRevocationScanFailures=0;
 const delegate=(method,args)=>{if(!management||typeof management[method]!=='function')throw new Error('peer_pairing_management_unavailable');return management[method](...args);};
 try{if(pairing!==null){
  pairingUuid(pairing.credential_id);
  management=pairing.management??null;
  peerApi=storage.bindPeerPairings({clock:authentication,issuer:pairing.issuer,management:{assertCurrent(...args){return delegate('assertCurrent',args);},authenticate(...args){return delegate('authenticate',args);},current(...args){return delegate('current',args);}}});
  const identity={service_id:storage.identity.service_id,database_instance_id:storage.identity.database_instance_id,service_kind:'seamer',
   issuer_id:pairing.issuer.issuer_id,issuer_epoch:pairing.issuer.issuer_epoch,credential_id:pairing.credential_id};
  const denials=storage.bindSessionDenials({issuer:pairing.issuer});
  peerTransport=await startPeerPairingTransport({pairings:peerApi,clock:authentication,identity,cert:pairing.cert,key:pairing.key,ca:pairing.ca,sessionDenial:(bindingId,socket,notice)=>denials.receive(bindingId,socket,notice),
   runProposal:Number(storage.identity.schema_revision)>=6?storage.runs.readApprovalProposalForPeer:null,
   runApproval:Number(storage.identity.schema_revision)>=7?storage.runs.readApprovalForPeer:null,
   runInitiator:storage.runs?.readArmInitiatorForPeer??null,
   runArmPlan:storage.runs?.prepareArmPlanForPeer??null,
   runActionBinding:storage.runs?.prepareActionBindingForPeer??null,
   runEndorsementRevocation:storage.runs?.receiveEndorsementRevocation??null,
   eventAge:Number(storage.identity.schema_revision)>=5?{selection:contractSelection,read:(reference,publisher)=>reconcile?.ageEvidenceForPublisher(reference,publisher)}:null,
   host:pairing.host??'127.0.0.1',port:pairing.port??0,serverName:pairing.serverName??pairing.host??'127.0.0.1',network:pairing.network??false});
  identityReceiver=createPeerIdentityReceiver({service:storage.identity,clock:authentication,pairings:peerApi,transport:peerTransport,issuer:pairing.issuer,watermarks:storage.identityWatermarks,denials});
  eventReceiver=createPeerEventReceiver({service:storage.identity,clock:authentication,pairings:peerApi,transport:peerTransport});
  const client=createPeerSubscriptionClient({transport:peerTransport});
  snapshotReceiver=createPeerSnapshotReceiver({service:storage.identity,client,clock:authentication,pairings:peerApi});
  if(Number(storage.identity.schema_revision)>=4){
   snapshotIngest=storage.bindSnapshotIngest({receiver:snapshotReceiver});
   reconcile=Number(storage.identity.schema_revision)>=5?storage.bindReconcileInbox({clock:authentication,referenceProvider}):null;
   snapshotConfirmation=createPeerSnapshotConfirmation({ingest:snapshotIngest,receiver:snapshotReceiver,client,reconcile,clock:authentication});
   if(reconcile)storage.bindSourceTransport(snapshotConfirmation.prepareSourceTransport);
  }
  const {snapshotApplied,attach,isClosed,...readOperations}=client;
  subscriptions=Object.freeze({...readOperations,async attach(bindingId,view,options={}){
   if(options.resume!=null||options.beforeSend!==undefined)throw Error('subscription_resume_requires_receipt');
   return attach(bindingId,view,{replace_existing:options.replace_existing,connection_attempt_id:options.connection_attempt_id});
  }});
  authorization=storage.bindLocalAuthorization({clock:authentication,receiver:identityReceiver,issuer:pairing.issuer});
  if(storage.runs?.submitArmPreparation)storage.bindArmPreparationTransport({submit:peerTransport.submitRunPreparation,lookup:peerTransport.lookupRunPreparation,
   eligibility:peerTransport.prepareRunPreparationEligibility});
  if(storage.runs?.reconcileAutomationSubmission&&peerTransport.submitAutomationCommand){
   storage.bindAutomationCommandTransport({submit:peerTransport.submitAutomationCommand,lookup:peerTransport.lookupAutomationCommand,
    result:peerTransport.readAutomationCommandResult});
  }
  management??=authorization.pairingManagement;
  if(Number(storage.identity.schema_revision)>=6){
   storage.bindRunServiceApprovals(createPeerServiceApprovalReader({clock:authentication,pairings:peerApi,transport:peerTransport,issuer:pairing.issuer}));
   runEndorsementReader=createPeerRunApprovalEndorsementReader({service:storage.identity,clock:authentication,pairings:peerApi,transport:peerTransport});
   if(Number(storage.identity.schema_revision)>=7)storage.bindRunEndorsements(runEndorsementReader);
   if(Number(storage.identity.schema_revision)>=10)storage.bindRevokedEndorsementReader(peerTransport.readRunApprovalEndorsement);
  }
  if(Number(storage.identity.schema_revision)>=3){bindingReceiver=createPeerBindingReceiver({service:storage.identity,pairings:peerApi,clock:authentication,transport:peerTransport});bindingApprovals=storage.bindBindingApprovals({receiver:bindingReceiver,eventReceiver});}
  if(publicApi!==null)publicTransport=await startDraftHttp({host:publicApi.host??'127.0.0.1',port:publicApi.port??0,cert:publicApi.cert,key:publicApi.key,bindingId:publicApi.binding_id,
   allowedOrigins:publicApi.allowedOrigins??[],localPermissions:publicApi.localPermissions===true,localEventBindings:publicApi.localEventBindings===true,onFailure:publicApi.onFailure??null,
   service:storage.identity,bootId:storage.serviceBootId,receiver:identityReceiver,authorization,bindingApprovals,drafts:storage.drafts,publications:storage.publications,runs:storage.runs});
  if(storage.runs?.scanArmPreparation)armPreparationDelivery=startOutboxDelivery({scan:storage.runs.scanArmPreparation,
   async deliver(item){return (await storage.runs.deliverArmPreparation(item)).state;},pageSize:4});
  if(storage.runs?.deliverApprovalProtection){
   storage.bindRunProtectionTransport(peerTransport.deliverRunApprovalProtection);
   runProtectionDelivery=startOutboxDelivery({scan(request){
    try{storage.runs.adoptBindingRevocations?.();}
    catch{bindingRevocationScanFailures=Math.min(Number.MAX_SAFE_INTEGER,bindingRevocationScanFailures+1);}
    return storage.runs.scanApprovalProtection(request);
   },async deliver({operation_id,target_scope}){
    return (await storage.runs.deliverApprovalProtection(operation_id,target_scope)).state;
   }});
  }
 }
 if(protectedIngress!==null){
  const {host,port,cert,key,origin}=protectedIngress;
  publicTransport=await startDraftHttp({host,port,cert,key,service:storage.identity,bootId:storage.serviceBootId,
   allowedOrigins:[origin],protectedIngress:true});
 }
 if(executionNotifications)executionNotificationDelivery=startExecutionNotificationDelivery(executionNotifications,notificationSink.deliver);
 }catch(error){identityReceiver?.close();bindingReceiver?.close();eventReceiver?.close();snapshotReceiver?.close();await Promise.allSettled([closeHttp(),armPreparationDelivery?.close(),runProtectionDelivery?.close(),executionNotificationDelivery?.close(),peerTransport?.close(),publicTransport?.close()]);await closeFailedStartup(error);throw error;}
 sampling?.start();
 const {stageFirstPairing,readFirstPairingCompletion,activateFirstPairing,...normalPairing}=peerApi??{};
 const assertFirstPairingManagement=()=>{if(management!==authorization.pairingManagement)throw new Error('peer_pairing_first_enrollment_management');};
 let stopping;
 return Object.freeze({address:server.address(),identity:storage.identity,serviceBootId:storage.serviceBootId,collectorBinding,
  ...(sampling&&{referenceSampling:Object.freeze({current:sampling.current,status:sampling.status,receiverReference:sampling.receiverReference})}),
  authenticationStatus:()=>authentication.status(),authorization,bindingApprovals,drafts:storage.drafts,publications:storage.publications,runs:storage.runs,publicOrigin:publicTransport?.origin??null,
  ...(executionNotificationDelivery&&{executionNotificationStatus:executionNotificationDelivery.status}),
  pairing:peerTransport?Object.freeze({api:Object.freeze(normalPairing),transport:peerTransport,identityReceiver,eventReceiver,subscriptions,snapshotReceiver,snapshotIngest,snapshotConfirmation,sourceEvent:reconcile?.sourceEvent??null,runEndorsementReader,
   ...(stageFirstPairing&&{stageFirstPairing(id){assertFirstPairingManagement();return stageFirstPairing(id,publicApi?.binding_id??null);},
    firstPairingCompletion(id){assertFirstPairingManagement();return readFirstPairingCompletion(id);},async completeFirstPairing(id){
    assertFirstPairingManagement();
    const completed=readFirstPairingCompletion(id);if(completed)return completed;
    const stage=stageFirstPairing(id,publicApi?.binding_id??null),remote=await peerTransport.status(stage.binding_id);
    return activateFirstPairing(id,remote);
   }}),
   submitArmPreparation:storage.runs?.submitArmPreparation??(storage.runs?.armPreparationIntents?async(operationRef,target)=>{
    const intent=storage.runs.armPreparationIntents(operationRef).find(item=>same(item.request.controller_scope,target));
    if(!intent)throw new Error('run_preparation_plan_target');
    return peerTransport.submitRunPreparation(intent.binding_id,intent.request);
   }:null),
   lookupArmPreparation:storage.runs?.lookupArmPreparation??(storage.runs?.armPreparationLookupIntent?async(operationRef,target)=>{
    const {binding_id,lookup}=storage.runs.armPreparationLookupIntent(operationRef,target);
    return peerTransport.lookupRunPreparation(binding_id,lookup);
   }:null),
   deliverRunApprovalProtection:storage.runs?.prepareApprovalProtection?async(operationId,target)=>{
    if(storage.runs.deliverApprovalProtection)return (await storage.runs.deliverApprovalProtection(operationId,target)).receipt;
    const {binding_id,notice}=storage.runs.prepareApprovalProtection(operationId,target);
    return await peerTransport.deliverRunApprovalProtection(binding_id,notice);
   }:null,runApprovalProtectionStatus:()=>runProtectionDelivery?{...runProtectionDelivery.status(),binding_revocation_scan_failed:bindingRevocationScanFailures}:null,
   useIdentityManagement(){management=authorization.pairingManagement;}}):null,
  close(){return stopping??=(async()=>{identityReceiver?.close();bindingReceiver?.close();eventReceiver?.close();snapshotReceiver?.close();
   const results=await Promise.allSettled([closeHttp(),armPreparationDelivery?.close(),runProtectionDelivery?.close(),executionNotificationDelivery?.close(),peerTransport?.close(),publicTransport?.close(),supervisedReference?.close()]);
   try{authentication.stop();}finally{storage.close();}
   const failure=results.find(result=>result.status==='rejected');if(failure)throw failure.reason;})();}
 });
}
