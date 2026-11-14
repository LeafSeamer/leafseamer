import http from 'node:http';
import {openStorage} from './storage.mjs';
import {openNativeClock} from './runtime/native-clock.mjs';
import {startAuthenticationClock} from './runtime/auth-clock.mjs';
import {startPeerPairingTransport} from './runtime/peer-pairing-transport.mjs';
import {createPeerSubscriptionClient} from './peer-subscription-client.mjs';
import {createPeerSnapshotReceiver} from './peer-snapshot-receiver.mjs';
import {createPeerSnapshotConfirmation} from './peer-snapshot-confirmation.mjs';
import {pairingUuid} from './runtime/peer-pairing-value.mjs';
import {createPeerIdentityReceiver} from './peer-identity.mjs';
import {createPeerBindingReceiver} from './peer-binding-receiver.mjs';
import {createPeerEventReceiver} from './peer-event-receiver.mjs';
import {startDraftHttp} from './draft-http.mjs';

export async function startSeamer({dataDir,host='127.0.0.1',port=0,authenticationProvider=null,pairing=null,publicApi=null,publicationSource=null}={}){
 if(!['127.0.0.1','::1'].includes(host)||!Number.isInteger(port)||port<0||port>65535)throw new Error('listen_options');
 if(authenticationProvider!==null&&typeof authenticationProvider!=='function')throw new Error('authentication_provider');
 if(publicApi!==null){if(pairing===null)throw new Error('public_api_pairing_required');pairingUuid(publicApi.binding_id);}
 const storage=openStorage(dataDir,{publicationSource});let authentication;
 try{const source=openNativeClock();authentication=startAuthenticationClock(source,storage.authenticationClock,authenticationProvider?.({scope:storage.authenticationClock.scope,source})??null);}
 catch(error){storage.close();throw error;}
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
 catch(error){server.close();authentication.stop();storage.close();throw error;}
 const closeHttp=()=>new Promise((resolve,reject)=>{server.close(error=>error?reject(error):resolve());server.closeAllConnections();});
 let peerTransport=null,peerApi=null,identityReceiver=null,authorization=null,publicTransport=null,management=null,bindingReceiver=null,bindingApprovals=null,eventReceiver=null,subscriptions=null,snapshotReceiver=null,snapshotIngest=null,snapshotConfirmation=null;
 const delegate=(method,args)=>{if(!management||typeof management[method]!=='function')throw new Error('peer_pairing_management_unavailable');return management[method](...args);};
 try{if(pairing!==null){
  pairingUuid(pairing.credential_id);
  management=pairing.management??null;
  peerApi=storage.bindPeerPairings({clock:authentication,issuer:pairing.issuer,management:{assertCurrent(...args){return delegate('assertCurrent',args);},authenticate(...args){return delegate('authenticate',args);},current(...args){return delegate('current',args);}}});
  const identity={service_id:storage.identity.service_id,database_instance_id:storage.identity.database_instance_id,service_kind:'seamer',
   issuer_id:pairing.issuer.issuer_id,issuer_epoch:pairing.issuer.issuer_epoch,credential_id:pairing.credential_id};
  const denials=storage.bindSessionDenials({issuer:pairing.issuer});
  peerTransport=await startPeerPairingTransport({pairings:peerApi,clock:authentication,identity,cert:pairing.cert,key:pairing.key,ca:pairing.ca,sessionDenial:(bindingId,socket,notice)=>denials.receive(bindingId,socket,notice),
   host:pairing.host??'127.0.0.1',port:pairing.port??0,serverName:pairing.serverName??pairing.host??'127.0.0.1'});
  identityReceiver=createPeerIdentityReceiver({service:storage.identity,clock:authentication,pairings:peerApi,transport:peerTransport,issuer:pairing.issuer,watermarks:storage.identityWatermarks,denials});
  eventReceiver=createPeerEventReceiver({service:storage.identity,clock:authentication,pairings:peerApi,transport:peerTransport});
  subscriptions=createPeerSubscriptionClient({transport:peerTransport});snapshotReceiver=createPeerSnapshotReceiver({service:storage.identity,client:subscriptions,clock:authentication,pairings:peerApi});
  if(storage.identity.schema_revision==='4'){snapshotIngest=storage.bindSnapshotIngest({receiver:snapshotReceiver});snapshotConfirmation=createPeerSnapshotConfirmation({ingest:snapshotIngest,receiver:snapshotReceiver,client:subscriptions});}
  authorization=storage.bindLocalAuthorization({clock:authentication,receiver:identityReceiver,issuer:pairing.issuer});
  management??=authorization.pairingManagement;
  if(['3','4'].includes(storage.identity.schema_revision)){bindingReceiver=createPeerBindingReceiver({service:storage.identity,pairings:peerApi,clock:authentication,transport:peerTransport});bindingApprovals=storage.bindBindingApprovals({receiver:bindingReceiver});}
  if(publicApi!==null)publicTransport=await startDraftHttp({host:publicApi.host??'127.0.0.1',port:publicApi.port??0,cert:publicApi.cert,key:publicApi.key,bindingId:publicApi.binding_id,
   allowedOrigins:publicApi.allowedOrigins??[],onFailure:publicApi.onFailure??null,service:storage.identity,bootId:storage.serviceBootId,receiver:identityReceiver,authorization,bindingApprovals,drafts:storage.drafts,publications:storage.publications});
 }}catch(error){identityReceiver?.close();bindingReceiver?.close();eventReceiver?.close();snapshotReceiver?.close();await Promise.allSettled([closeHttp(),peerTransport?.close(),publicTransport?.close()]);try{authentication.stop();}finally{storage.close();}throw error;}
 let stopping;
 return Object.freeze({address:server.address(),identity:storage.identity,serviceBootId:storage.serviceBootId,authenticationStatus:()=>authentication.status(),authorization,bindingApprovals,drafts:storage.drafts,publications:storage.publications,publicOrigin:publicTransport?.origin??null,
  pairing:peerTransport?Object.freeze({api:peerApi,transport:peerTransport,identityReceiver,eventReceiver,subscriptions,snapshotReceiver,snapshotIngest,snapshotConfirmation,useIdentityManagement(){management=authorization.pairingManagement;}}):null,
  close(){return stopping??=(async()=>{identityReceiver?.close();bindingReceiver?.close();eventReceiver?.close();snapshotReceiver?.close();const results=await Promise.allSettled([closeHttp(),peerTransport?.close(),publicTransport?.close()]);try{authentication.stop();}finally{storage.close();}
   const failure=results.find(result=>result.status==='rejected');if(failure)throw failure.reason;})();}
 });
}
