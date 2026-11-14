import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import https from 'node:https';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {bindingMaterial} from './device-bindings-fixture.mjs';
import {setup} from '../../leafseamer-controller/test/fixtures/extension-installation.mjs';
import {authenticated} from '../../leafseamer-controller/test/fixtures/extension-auth.mjs';
import {peerCertificates} from '../../leafseamer-controller/test/fixtures/peer-certificates.mjs';
const controllerRoot=process.env.LEAFSEAMER_CONTROLLER_PACKAGE_UNDER_TEST??fileURLToPath(new URL('../../leafseamer-controller/',import.meta.url));assert.ok(path.isAbsolute(controllerRoot));
const seamerRoot=process.env.LEAFSEAMER_PACKAGE_UNDER_TEST??fileURLToPath(new URL('../',import.meta.url));assert.ok(path.isAbsolute(seamerRoot));
const load=(root,name)=>import(pathToFileURL(path.join(root,'dist',name)));
const {openNativeClock:controllerClock}=await load(controllerRoot,'native-clock.mjs');
const {startPeerPairingTransport:controllerTransport}=await load(controllerRoot,'peer-pairing-transport.mjs');
const {initializeStorage,openStorage}=await load(seamerRoot,'storage.mjs');
const {openNativeClock}=await load(seamerRoot,'runtime/native-clock.mjs');
const {startAuthenticationClock}=await load(seamerRoot,'runtime/auth-clock.mjs');
const {startPeerPairingTransport}=await load(seamerRoot,'runtime/peer-pairing-transport.mjs');
import {digest,seal,canonical} from '../dist/runtime/auth-clock-value.mjs';
import {fixture} from './auth-clock-fixture.mjs';
const id=()=>crypto.randomUUID();

test('Controller与Seamer实际TLS证明、独立激活与撤销；Seamer人员核验为合成装配', {timeout:60000},async t=>{
 const certificates=peerCertificates(),[ac,bc]=certificates.leaves,controller=await setup(t,59);
 const enrollment=controller.store.enrollment.begin();await controller.store.enrollment.complete({token_id:enrollment.token_id,secret:enrollment.secret,login:'role.test',password:'Synthetic-role-password-123',mode:'standalone'});
 const directory=fs.mkdtempSync(path.join(os.tmpdir(),'leafseamer-cross-pairing-'));
 initializeStorage(directory);const seamer=openStorage(directory);
 let transportA,transportB,clockB;
 t.after(async()=>{try{await transportA?.close();await transportB?.close();}finally{clockB?.stop();seamer.close();assert.equal(path.dirname(path.resolve(directory)),path.resolve(os.tmpdir()));fs.rmSync(directory,{recursive:true,force:true,maxRetries:5,retryDelay:50});}});
 const auth=await authenticated(t,controller,{grant:false,native:{source:controllerClock(),descriptor:{clock_domain_id:id(),descriptor_digest:digest('synthetic-controller-pairing-domain')}},sampleWindowPolicy:'authentication-clock-sample/2'});
 const issuer={service_id:controller.store.identity.service_id,issuer_id:auth.logged.session.issuer_id,issuer_epoch:auth.logged.session.issuer_epoch};
 const native={source:openNativeClock(),descriptor:{clock_domain_id:id(),descriptor_digest:digest('synthetic-seamer-pairing-domain')}};
 const clockFixture=fixture(seamer.authenticationClock.scope,native,{sampleWindowPolicy:'authentication-clock-sample/2'});
 clockB=startAuthenticationClock(native.source,seamer.authenticationClock,clockFixture.provider);
 const now=clockB.current(),session={session_id:id(),principal_id:id(),principal_kind:'human',audience:seamer.identity.service_id,issuer_id:issuer.issuer_id,issuer_epoch:issuer.issuer_epoch,
  not_before_ns:String(BigInt(now.lower_ns)-1000000000n),expires_ns:String(BigInt(now.upper_ns)+120000000000n),recent_before_ns:String(BigInt(now.upper_ns)+120000000000n)};
 const grant=seal({id:id(),revision:'1',issuer_id:issuer.issuer_id,issuer_epoch:issuer.issuer_epoch,state:'active',permissions:['auth.manage']}),state={enabled:true,kind:'human',authenticationCalls:0,switchAt:0};
 const management={assertCurrent(expected){assert.deepEqual(expected,issuer);if(!state.enabled)throw new Error('synthetic_source_lost');},
  authenticate(input,_now,_issuer,revocationScope){
   assert.equal(input,'synthetic-seamer-human');state.authenticationCalls++;
   if(state.revocationReference){
    if(revocationScope){
     state.capturedScope=revocationScope;
     assert.equal(apiB.identityForRevocationInTransaction(state.revocationReference,revocationScope).state,'activated');
     assert.throws(()=>apiB.identityForRevocationInTransaction(state.revocationReference,{...revocationScope}),{code:'peer_pairing_revocation_scope'});
     assert.throws(()=>apiB.identityForRevocationInTransaction({...state.revocationReference,digest:'sha256:'+'0'.repeat(64)},revocationScope),{code:'peer_pairing_revocation_scope'});
     if(state.failRevocation)throw new Error('synthetic_revocation_finish_failure');
    }else if(state.capturedScope){assert.throws(()=>apiB.identityForRevocationInTransaction(state.revocationReference,state.capturedScope),{code:'peer_pairing_revocation_scope'});state.expiredScopeChecks=(state.expiredScopeChecks??0)+1;}
   }
   return {session:{...session,principal_kind:state.kind,principal_id:state.switchAt&&state.authenticationCalls>=state.switchAt?id():session.principal_id}};
  },current(principal){assert.equal(principal,session.principal_id);return grant;}};
 const apiB=seamer.bindPeerPairings({clock:clockB,issuer,management}),apiA=auth.sessions.peerPairings;
 const identityA={...issuer,database_instance_id:controller.store.identity.database_instance_id,service_kind:'controller',credential_id:id()},
  identityB={service_id:seamer.identity.service_id,database_instance_id:seamer.identity.database_instance_id,service_kind:'seamer',issuer_id:issuer.issuer_id,issuer_epoch:issuer.issuer_epoch,credential_id:id()};
 const device=id(),scope={format:'leafseamer.device-binding-scope/1',authority_service_id:identityA.service_id,database_instance_id:identityA.database_instance_id,device_ids:[device]};
 const bundle=bindingMaterial({authority_service_id:identityA.service_id,database_instance_id:identityA.database_instance_id},device);
 let available=true,reads=0,rejectFinal=false;
 transportA=await controllerTransport({pairings:apiA,clock:auth.real,identity:identityA,...ac,ca:certificates.ca,deviceBindings(selected,expected){
  reads++;assert.equal(selected,device);if(!available||(rejectFinal&&reads%2===0))throw new Error('synthetic_binding_unavailable');
  if(expected!==null)assert.deepEqual(expected,bundle.registration_ref);return structuredClone(bundle);
 }});
 transportB=await startPeerPairingTransport({pairings:apiB,clock:clockB,identity:identityB,...bc,ca:certificates.ca});
 transportA.bindPeers([{identity:identityB,cert:bc.cert,origin:transportB.origin,server_name:'127.0.0.1',trust_bundle_digest:transportB.configuration.trust_bundle_digest}]);
 transportB.bindPeers([{identity:identityA,cert:ac.cert,origin:transportA.origin,server_name:'127.0.0.1',trust_bundle_digest:transportA.configuration.trust_bundle_digest}]);
 const samples=[auth.real.current(),clockB.current()],begin=samples.reduce((a,b)=>a<BigInt(b.lower_ns)?a:BigInt(b.lower_ns),BigInt(samples[0].lower_ns))-1000000000n;
 const parties=[transportA,transportB].map(transport=>({...transport.configuration,binding_id:id(),permissions:transport===transportB?['automation.bindings.read','clock.internal.read']:['clock.internal.read'],object_scope_digest:transport===transportB?digest(scope):digest({devices:[]}),not_before_ns:String(begin),expires_ns:String(begin+120000000000n)}));
 const transcript={format:'leafseamer.peer-pairing/1',pair_attempt_id:id(),parties},requestB={request_id:id(),transcript};
 state.kind='service';assert.throws(()=>apiB.stage('synthetic-seamer-human',requestB),{code:'peer_pairing_management_denied'});state.kind='human';
 state.enabled=false;assert.throws(()=>apiB.stage('synthetic-seamer-human',requestB),/synthetic_source_lost/);state.enabled=true;
 state.authenticationCalls=0;state.switchAt=2;assert.throws(()=>apiB.stage('synthetic-seamer-human',requestB),{code:'peer_pairing_management_changed'});state.switchAt=0;
 const a=apiA.stage(auth.auth,{request_id:id(),transcript}),b=apiB.stage('synthetic-seamer-human',requestB);assert.equal(a.transcript_digest,b.transcript_digest);
 assert.equal((await transportA.status(a.binding_id)).state,'staged');assert.equal((await transportB.status(b.binding_id)).state,'staged');
 await transportA.prove(a.binding_id);await transportB.prove(b.binding_id);
 const readyA=apiA.get(auth.auth,a.binding_id),readyB=apiB.get('synthetic-seamer-human',b.binding_id);assert.equal(readyA.state,'proved');assert.equal(readyB.state,'proved');
 apiA.activate(auth.auth,{request_id:id(),binding_id:a.binding_id,expected_revision:readyA.revision,transcript_digest:a.transcript_digest});
 assert.equal((await transportA.status(a.binding_id)).state,'proved');
 const activation={request_id:id(),binding_id:b.binding_id,expected_revision:readyB.revision,transcript_digest:b.transcript_digest};
 state.enabled=false;assert.throws(()=>apiB.activate('synthetic-seamer-human',activation),/synthetic_source_lost/);state.enabled=true;
 const active=apiB.activate('synthetic-seamer-human',activation);assert.equal(active.state,'activated');assert.equal((await transportA.status(a.binding_id)).state,'activated');
 const bindingRequest={format:'leafseamer.device-bindings-request/1',challenge_id:id(),scope,device_id:device,expected_registration_ref:null};
 const bindings=await transportB.readDeviceBindings(b.binding_id,bindingRequest);assert.deepEqual(bindings.bundle,bundle);assert.equal(reads,2);
 assert.equal(bindings.challenge_id,bindingRequest.challenge_id);assert.equal(bindings.controller_id,identityA.service_id);assert.equal(bindings.seamer_id,identityB.service_id);
 assert.deepEqual((await transportB.readDeviceBindings(b.binding_id,{...bindingRequest,challenge_id:id(),expected_registration_ref:bundle.registration_ref})).bundle,bundle);
 await assert.rejects(transportB.readDeviceBindings(b.binding_id,{...bindingRequest,expected_registration_ref:{...bundle.registration_ref,digest:digest('stale')}}),{code:'peer_pairing_response'});
 available=false;await assert.rejects(transportB.readDeviceBindings(b.binding_id,bindingRequest),{code:'peer_pairing_response'});available=true;
 reads=0;rejectFinal=true;await assert.rejects(transportB.readDeviceBindings(b.binding_id,bindingRequest),{code:'peer_pairing_response'});rejectFinal=false;
 assert.throws(()=>transportB.readDeviceBindings(b.binding_id,{...bindingRequest,scope:{...scope,device_ids:[device,id()].sort()}}),{code:'peer_pairing_bindings_scope'});
 const raw=(body,bindingId=a.binding_id)=>new Promise((resolve,reject)=>{const bytes=Buffer.from(canonical(body)),request=https.request(transportA.origin+'/internal/pairings/'+bindingId+'/device-bindings',
  {method:'POST',agent:false,...bc,ca:certificates.ca,minVersion:'TLSv1.3',maxVersion:'TLSv1.3',headers:{'Content-Type':'application/json','Content-Length':bytes.length}},response=>{
   response.resume();response.on('end',()=>resolve(response.statusCode));});request.on('error',reject);request.end(bytes);});
 const before=reads;assert.equal(await raw({...bindingRequest,scope:{...scope,device_ids:[device,id()].sort()}}),403);assert.equal(reads,before);
 // 旧配对只有时钟用途，即使证书和设备范围相同也不能读取绑定。
 const limitedTranscript={...transcript,pair_attempt_id:id(),parties:parties.map(p=>({...p,binding_id:id(),permissions:['clock.internal.read']}))};
 const limitedA=apiA.stage(auth.auth,{request_id:id(),transcript:limitedTranscript}),limitedB=apiB.stage('synthetic-seamer-human',{request_id:id(),transcript:limitedTranscript});
 await transportA.prove(limitedA.binding_id);await transportB.prove(limitedB.binding_id);
 for(const [api,input,record]of [[apiA,auth.auth,limitedA],[apiB,'synthetic-seamer-human',limitedB]]){const ready=api.get(input,record.binding_id);api.activate(input,{request_id:id(),binding_id:ready.binding_id,expected_revision:ready.revision,transcript_digest:ready.transcript_digest});}
 assert.throws(()=>transportB.readDeviceBindings(limitedB.binding_id,bindingRequest),{code:'peer_pairing_purpose'});
 const beforeDenied=reads;assert.equal(await raw(bindingRequest,limitedA.binding_id),403);assert.equal(reads,beforeDenied);
 state.revocationReference={id:active.binding_id,revision:active.revision,digest:active.digest};state.failRevocation=true;
 const revoke={request_id:id(),binding_id:b.binding_id,expected_revision:active.revision};
 assert.throws(()=>apiB.revoke('synthetic-seamer-human',revoke),/synthetic_revocation_finish_failure/);
 assert.equal(apiB.get('synthetic-seamer-human',b.binding_id).state,'activated');state.failRevocation=false;
 apiB.revoke('synthetic-seamer-human',revoke);assert.equal(apiB.get('synthetic-seamer-human',b.binding_id).state,'revoked');assert.ok(state.expiredScopeChecks>=4);
 await assert.rejects(transportA.status(a.binding_id),{code:'peer_pairing_response'});
 assert.throws(()=>transportB.readDeviceBindings(b.binding_id,bindingRequest),{code:'peer_pairing_state'});
 await transportA.close();transportA=null;await transportB.close();transportB=null;clockB.stop();seamer.close();
 const db=new Database(path.join(directory,'seamer.sqlite'));try{
  assert.equal(db.prepare('SELECT state FROM peer_pairing_revision r JOIN peer_pairing_head h USING(binding_id,revision)').pluck().get(),'revoked');
  assert.equal(db.prepare('SELECT count(*) FROM peer_pairing_proof WHERE binding_id=?').pluck().get(b.binding_id),2);
  assert.equal(db.prepare("SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='identity_issuer'").pluck().get(),0);
  assert.deepEqual(db.prepare('PRAGMA foreign_key_check').raw().all(),[]);
 }finally{db.close();}
});
