import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import net from 'node:net';
import https from 'node:https';
import {spawn,spawnSync} from 'node:child_process';
import {once} from 'node:events';
import {setTimeout as delay} from 'node:timers/promises';
import {fileURLToPath,pathToFileURL} from 'node:url';
import Database from 'better-sqlite3';
import {canonical,digest,ref,seal} from '../dist/runtime/auth-clock-value.mjs';
import {peerCertificates} from '../../leafseamer-controller/test/fixtures/peer-certificates.mjs';
import {seedLocalPermission} from './local-authorization-fixture.mjs';
import {fixture as clockFixture} from './auth-clock-fixture.mjs';
import {source as draftSource} from './automation-program-fixture.mjs';

const controllerRoot=process.env.LEAFSEAMER_CONTROLLER_TEST_ROOT??fileURLToPath(new URL('../../leafseamer-controller/',import.meta.url));
const seamerRoot=process.env.LEAFSEAMER_PACKAGE_UNDER_TEST??fileURLToPath(new URL('../',import.meta.url));
assert.ok(path.isAbsolute(controllerRoot)&&path.isAbsolute(seamerRoot));
const load=(root,file)=>import(pathToFileURL(path.join(root,file)));
const {setup}=await load(controllerRoot,'test/fixtures/extension-installation.mjs');
const {authenticated}=await load(controllerRoot,'test/fixtures/extension-auth.mjs');
const {openNativeClock}=await load(controllerRoot,'dist/native-clock.mjs');
const {startPeerPairingTransport}=await load(controllerRoot,'dist/peer-pairing-transport.mjs');
const {signFirstPairingAuthorization}=await load(controllerRoot,'dist/first-pairing-authorization.mjs');
const {createIntrospectionEndpoint}=await load(controllerRoot,'dist/introspection-http.mjs');
const controllerStorage=await load(controllerRoot,'dist/storage.mjs');
const {initializeProcessHost,installedCoreDigest}=await load(controllerRoot,'dist/process-host.mjs');
const {initializeStorage,migrateStorage,openStorage}=await load(seamerRoot,'dist/storage.mjs');
const {startSeamer}=await load(seamerRoot,'dist/server.mjs');
const {openNativeClock:openSeamerClock}=await load(seamerRoot,'dist/runtime/native-clock.mjs');
const {createClient,contractSelection}=await load(seamerRoot,'dist/sdk/index.mjs');
const {openInstalledPublicationPolicy}=await load(seamerRoot,'dist/trusted-pairing-profile.mjs');
const id=()=>crypto.randomUUID(),cli=path.join(seamerRoot,'dist/cli.mjs');

function inspect(directory,fn){
 const db=new Database(path.join(directory,'seamer.sqlite'),{fileMustExist:true});
 try{return fn(db);}finally{db.close();}
}

test('安装批准固定发布策略，错库、篡改、到期和撤下审批均拒绝',t=>{
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'leafseamer-policy-approval-'));
 t.after(()=>{assert.equal(path.dirname(path.resolve(dir)),path.resolve(os.tmpdir()));fs.rmSync(dir,{recursive:true,force:true});});
 const identity={service_id:id(),database_instance_id:id()},scope={authority_service_id:identity.service_id,database_instance_id:identity.database_instance_id};
 const reference={kind:'policy',scope,object_id:'policy',revision:'1',digest:digest({})},approval_file=path.join(dir,'publication.json');
 const bytes=canonical({format:'leafseamer.publication-policy-approval/1',policy_ref:reference,not_before_ns:'100',expires_ns:'200'});
 fs.writeFileSync(approval_file,bytes);
 const approval_sha256='sha256:'+crypto.createHash('sha256').update(bytes).digest('hex');
 const options={approval_file,approval_sha256};
 let lower_ns='99',upper_ns='101';
 const clock={current:()=>({lower_ns,upper_ns})};
 const open=()=>openInstalledPublicationPolicy(options,{identity,clock});
 assert.throws(()=>openInstalledPublicationPolicy(options,{identity:{...identity,database_instance_id:id()},clock}),{code:'pairing_profile_publication_policy_invalid'});
 const policy=open();
 assert.equal(policy.assertCurrentInTransaction(reference),false);
 lower_ns='100';assert.equal(policy.assertCurrentInTransaction(reference),true);
 assert.equal(policy.assertCurrentInTransaction({...reference,revision:'2'}),false);
 upper_ns='200';assert.equal(policy.assertCurrentInTransaction(reference),false);
 upper_ns='101';assert.equal(policy.assertCurrentInTransaction(reference),false);
 const active=open(),removed=path.join(dir,'withdrawn.json');
 fs.renameSync(approval_file,removed);
 assert.equal(active.assertCurrentInTransaction(reference),false);
 fs.renameSync(removed,approval_file);
 assert.equal(active.assertCurrentInTransaction(reference),false);
 fs.appendFileSync(approval_file,'\n');
 assert.throws(open,{code:'pairing_profile_publication_policy_invalid'});
});

test('Seamer CLI 导入实际 Controller 签发的授权，重投查原并拒绝跨库、错证书、篡改和冲突',{timeout:120000},async t=>{
 const root=fs.mkdtempSync(path.join(os.tmpdir(),'leafseamer-first-pairing-'));
 t.after(()=>{assert.equal(path.dirname(path.resolve(root)),path.resolve(os.tmpdir()));fs.rmSync(root,{recursive:true,force:true,maxRetries:3,retryDelay:50});});
 const dir=path.join(root,'seamer'),other=path.join(root,'other');
 const original=initializeStorage(dir);initializeStorage(other);migrateStorage(dir,'17');migrateStorage(other,'18');
 const {leaves:[ac,bc,oc],ca}=peerCertificates(),controller=await setup(t,93);
 const enrollment=controller.store.enrollment.begin();
 await controller.store.enrollment.complete({token_id:enrollment.token_id,secret:enrollment.secret,login:'role.test',password:'Synthetic-role-password-123',mode:'standalone'});
 const auth=await authenticated(t,controller,{grant:false,native:{source:openNativeClock(),descriptor:{clock_domain_id:id(),descriptor_digest:digest('test issuer clock')}},sampleWindowPolicy:'authentication-clock-sample/2'});
 const {issuer_id,issuer_epoch}=auth.logged.session;
 const identityA={service_id:controller.store.identity.service_id,database_instance_id:controller.store.identity.database_instance_id,
  service_kind:'controller',issuer_id,issuer_epoch,credential_id:id()};
 const identityB={service_id:original.service_id,database_instance_id:original.database_instance_id,service_kind:'seamer',issuer_id,issuer_epoch,credential_id:id()};
 const api=auth.sessions.peerPairings,a=await startPeerPairingTransport({pairings:api,identity:identityA,clock:auth.real,...ac,ca});t.after(()=>a.close());
 const b=await startPeerPairingTransport({pairings:{},identity:identityB,clock:auth.real,...bc,ca});t.after(()=>b.close());
 a.bindPeers([{identity:identityB,cert:bc.cert,origin:b.origin,server_name:'127.0.0.1',trust_bundle_digest:b.configuration.trust_bundle_digest}]);
 function issue(){
  const begin=BigInt(auth.real.current().lower_ns)-1000000000n,end=begin+120000000000n;
  const party=(transport,permissions)=>({...transport.configuration,binding_id:id(),permissions,object_scope_digest:digest({devices:[]}),not_before_ns:String(begin),expires_ns:String(end)});
  const parties=[party(a,[]),party(b,['auth.introspect'])];
  const stage=api.stage(auth.auth,{request_id:id(),transcript:{format:'leafseamer.peer-pairing/1',pair_attempt_id:id(),parties}});
  return api.exportFirstPairingAuthorization(auth.auth,{binding_id:stage.binding_id,transcript_digest:stage.transcript_digest});
 }
 const signed=issue(),failed=issue(),{authorization_id,administrator}=signed.authorization;
 assert.equal(administrator.principal_id,auth.logged.session.principal_id);
 const file=(name,bytes)=>{const target=path.join(root,name);fs.writeFileSync(target,bytes);return target;};
 const envelope=file('authorization.json',canonical(signed)),controllerCert=file('controller.pem',ac.cert),seamerCert=file('seamer.pem',bc.cert),wrongCert=file('other.pem',oc.cert);
 const withBom=file('authorization-bom.json','\uFEFF'+canonical(signed));
 const altered=structuredClone(signed);altered.authorization.administrator.principal_id=id();
 const alteredFile=file('altered.json',canonical(altered));
 const conflicting=signFirstPairingAuthorization({...signed.authorization,authorization_id:id()},new crypto.X509Certificate(ac.cert),crypto.createPrivateKey(ac.key));
 const conflictFile=file('conflict.json',canonical(conflicting)),missingWrite=file('missing-write.json',canonical(failed));
 function run(target=dir,authorization=envelope,from=controllerCert,to=seamerCert){
  const result=spawnSync(process.execPath,['--disallow-code-generation-from-strings',cli,'import-first-pairing','--data-dir',target,
   '--authorization-file',authorization,'--controller-certificate',from,'--seamer-certificate',to],{encoding:'utf8',windowsHide:true,timeout:8000});
  assert.ifError(result.error);
  return {status:result.status,response:JSON.parse((result.status===0?result.stdout:result.stderr).trim())};
 }
 const denied=error=>({status:1,response:{error}});
 const old=inspect(dir,db=>db.prepare('SELECT * FROM schema_migration ORDER BY CAST(to_revision AS INTEGER)').raw().all());
 assert.deepEqual(run(),denied('first_pairing_import_migration_required'));
 assert.deepEqual(migrateStorage(dir,'18'),{...original,schema_revision:'18'});
 assert.deepEqual(run(other),denied('peer_pairing_first_import_scope'));
 assert.deepEqual(run(dir,envelope,wrongCert),denied('peer_pairing_first_authorization_certificate'));
 assert.deepEqual(run(dir,envelope,controllerCert,wrongCert),denied('peer_pairing_first_import_target_certificate'));
 assert.deepEqual(run(dir,alteredFile),denied('peer_pairing_first_authorization_signature'));
 assert.deepEqual(run(dir,withBom),denied('peer_pairing_first_authorization_encoding'));
 inspect(dir,db=>db.exec("CREATE TRIGGER test_import_ignore BEFORE INSERT ON first_pairing_import WHEN NEW.authorization_id='"+failed.authorization.authorization_id+"' BEGIN SELECT RAISE(IGNORE); END;"));
 assert.deepEqual(run(dir,missingWrite),denied('peer_pairing_first_import_storage'));
 const accepted=run();assert.equal(accepted.status,0,JSON.stringify(accepted.response));
 assert.deepEqual(run(),accepted);
 assert.deepEqual(run(dir,conflictFile),denied('peer_pairing_first_import_conflict'));
 assert.equal(accepted.response.authorization_id,authorization_id);assert.equal(accepted.response.authorization_digest,digest(signed));
 assert.equal(accepted.response.state,'imported');assert.equal(accepted.response.ready,false);
 inspect(dir,db=>{
  assert.deepEqual(db.prepare('SELECT authorization_id,body_jcs,body_digest FROM first_pairing_import').raw().all(),[[authorization_id,canonical(signed),digest(signed)]]);
  assert.deepEqual(db.prepare('SELECT * FROM schema_migration ORDER BY CAST(to_revision AS INTEGER)').raw().all().slice(0,-1),old);
  for(const table of ['peer_pairing_revision','local_subject','local_permission_head','service_boot'])
   assert.equal(db.prepare('SELECT count(*) FROM '+table).pluck().get(),0,table);
  assert.throws(()=>db.exec('DELETE FROM first_pairing_import'),/immutable_first_pairing_import/);
  assert.deepEqual(db.pragma('foreign_key_check'),[]);
 });
 const running=openStorage(dir);try{assert.deepEqual(run(),denied('database_in_use'));}finally{running.close();}
 seedLocalPermission(dir,original,identityA,administrator.principal_id,['auth.manage']);
 assert.deepEqual(run(dir,missingWrite),denied('peer_pairing_first_import_already_configured'));
 assert.deepEqual(run(),accepted);
});

test('受信 CLI 在独立进程完成首次配对、两端授权与 HTTPS 草稿',{skip:process.platform!=='win32',timeout:180000},async t=>{
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'leafseamer-trusted-cli-'));
 const live=new Set();t.after(async()=>{
  for(const child of live){child.kill();await child.done;}
  assert.equal(path.dirname(path.resolve(dir)),path.resolve(os.tmpdir()));fs.rmSync(dir,{recursive:true,force:true,maxRetries:4,retryDelay:50});
 });
 function launch(profile,{executable=cli,dataDir=dir,port=null,supervised=false}={}){
  const child=spawn(process.execPath,['--disallow-code-generation-from-strings',executable,supervised?'supervise':'start',
   '--data-dir',dataDir,'--pairing-profile',profile,...(port===null?[]:['--port',String(port)])],
   {stdio:['ignore','pipe','pipe'],windowsHide:true});
  const frames=[];let pending='',errors='';child.stdout.setEncoding('utf8');child.stderr.setEncoding('utf8');
  (supervised?child.stderr:child.stdout).on('data',chunk=>{pending+=chunk;let split;while((split=pending.indexOf('\n'))>=0){
   frames.push(JSON.parse(pending.slice(0,split)));pending=pending.slice(split+1);
  }});child.stderr.on('data',chunk=>{errors+=chunk;});
  if(supervised)child.stdout.resume();
  child.done=once(child,'close');live.add(child);child.done.finally(()=>live.delete(child));
  return {child,frames,failure:()=>JSON.parse(errors),async event(name){
   const before=performance.now()+8000;
   while(!frames.some(f=>f.event===name)){
    assert.ok(child.exitCode===null&&performance.now()<before,JSON.stringify({name,frames,errors}));await delay(20);
   }
   return frames.find(f=>f.event===name);
  },async close(){child.kill();await child.done;}};
 }
 const service=initializeStorage(dir);migrateStorage(dir,'28');
 const {leaves:[ac,bc,site],ca}=peerCertificates(),controllerDir=path.join(dir,'controller');
 fs.mkdirSync(controllerDir);
 const controller=controllerStorage.initializeStorage(controllerDir);
 await controllerStorage.migrateStorage(controllerDir,93);await initializeProcessHost(controllerDir);
 const store=controllerStorage.openStorage(controllerDir),credentials={login:'role.test',password:'Synthetic-role-password-123'};
 let issuer,principal_id;
 try{
  const enrollment=store.enrollment.begin();
  ({principal_id}=await store.enrollment.complete({token_id:enrollment.token_id,secret:enrollment.secret,...credentials,mode:'standalone'}));
  issuer=store.enrollment.summary();
 }finally{store.close();}
 const {issuer_id,issuer_epoch}=issuer,
  identityA={service_id:controller.service_id,database_instance_id:controller.database_instance_id,service_kind:'controller',issuer_id,issuer_epoch,credential_id:id()},
  identityB={service_id:service.service_id,database_instance_id:service.database_instance_id,service_kind:'seamer',issuer_id,issuer_epoch,credential_id:id()};
 const file=(name,bytes)=>{const target=path.join(dir,name);fs.writeFileSync(target,bytes);return target;};
 const controllerCert=file('controller.pem',ac.cert),seamerCert=file('seamer.pem',bc.cert),caFile=file('ca.pem',ca);
 const reservations=[];
 try{
  for(let i=0;i<4;i++){
   const listener=net.createServer();
   await new Promise((resolve,reject)=>listener.once('error',reject).listen(0,'127.0.0.1',resolve));
   reservations.push(listener);
  }
 }catch(error){for(const listener of reservations)listener.close();throw error;}
 const [sitePort,controllerPort,port,publicPort]=reservations.map(listener=>listener.address().port),origin=port=>'https://127.0.0.1:'+port;
 await Promise.all(reservations.map(listener=>new Promise(resolve=>listener.close(resolve))));
 const controllerCli=path.join(controllerRoot,'dist/cli.mjs'),controllerSource=openNativeClock(),controllerClockKey=crypto.generateKeyPairSync('ed25519');
 const clockEvidenceCli=process.env.LEAFSEAMER_CLOCK_EVIDENCE_CLI_UNDER_TEST??path.join(controllerRoot,'dist/clock-evidence.mjs');
 assert.ok(path.isAbsolute(clockEvidenceCli));
 const controllerEvidenceFile=path.join(dir,'controller-clock.json');
 const controllerTemplate=clockFixture({service_id:controller.service_id,service_boot_id:id()},{source:controllerSource},
  {sampleWindowPolicy:'authentication-clock-sample/2'}).evidence;
 const sha=value=>'sha256:'+crypto.createHash('sha256').update(value).digest('hex');
 const trust=digest([sha(new crypto.X509Certificate(ca).raw)]);
 const browserOptions={playwright:process.env.LEAFSEAMER_GUI_PLAYWRIGHT,browser:process.env.LEAFSEAMER_GUI_BROWSER,output:process.env.LEAFSEAMER_GUI_OUTPUT};
 const verifyBrowser=browserOptions.playwright!==undefined,guiRoot=fileURLToPath(new URL('../../leafseamer-gui/',import.meta.url));
 let gui;
 if(verifyBrowser){
  assert.ok(Object.values(browserOptions).every(value=>typeof value==='string'&&path.isAbsolute(value)));
  const directory=path.join(dir,'configured-gui'),config=file('gui-site.json',canonical({site_label:'首次配对工作台',
   service_id:identityA.service_id,database_instance_id:identityA.database_instance_id,seamer:{service_id:identityB.service_id,
    database_instance_id:identityB.database_instance_id,base_url:origin(publicPort),service_label:'本机 Seamer'}}));
  const built=spawnSync(process.execPath,['scripts/build.mjs','--config',config,'--out-dir',directory],
   {cwd:guiRoot,encoding:'utf8',windowsHide:true,timeout:40000});
  assert.equal(built.status,0,[built.stderr,built.stdout,built.error?.code].join('\n'));
  gui={directory,manifest_sha256:sha(fs.readFileSync(path.join(directory,'gui-manifest.json')))};
 }
 const controllerProfile=file('controller-pairing-profile.json',canonical({format:'leafseamer.controller-pairing-profile/1',
  bootstrap:{format:'leafseamer-core-bootstrap-policy/1',service_id:controller.service_id,database_instance_id:controller.database_instance_id,
   artifact_manifest_ref:{manifest_id:'local-pairing-test',revision:'1',digest:installedCoreDigest()},
   platform_profile_ref:{profile_id:'synthetic-windows-profile',revision:'1'},policy_revision:'1',bootstrap_timeout_ms:10000,clock_backend_revision:'1'},
  https:{origin:origin(sitePort),certificate_file:file('https.pem',site.cert),private_key_file:file('https-key.pem',site.key)},
  local:{credential_id:identityA.credential_id,host:'127.0.0.1',port:controllerPort,certificate_file:controllerCert,
   private_key_file:file('controller-key.pem',ac.key),ca_files:[caFile]},
  seamer:{identity:identityB,origin:origin(port),server_name:'127.0.0.1',certificate_file:seamerCert,trust_bundle_digest:trust},
  ...(gui&&{gui}),
  clock:{policy_digest:controllerTemplate.policy.digest,evidence_file:controllerEvidenceFile,
   public_key_file:file('controller-clock.pem',controllerClockKey.publicKey.export({type:'spki',format:'pem'}))}}));
 function publishEvidence(probe,template,key,evidenceFile,service,source,{checkRejection=false}={}){
  assert.equal(probe.database_instance_id,service.database_instance_id);assert.equal(probe.native_binary_digest,source.artifactDigest);
  const {digest:_r,...oldRuntime}=template.runtime,{digest:_h,...oldHealth}=template.health;
  const expiry=String(BigInt(probe.tick_ns)+120000000000n),runtime=seal({...oldRuntime,id:id(),service_boot_id:probe.service_boot_id,
   clock_domain_id:probe.clock_domain_id??oldRuntime.clock_domain_id,descriptor_digest:probe.descriptor_digest??oldRuntime.descriptor_digest,
   continuity:probe.continuity,valid_before_tick_ns:expiry});
  const health=seal({...oldHealth,id:id(),service_boot_id:probe.service_boot_id,clock_domain_id:runtime.clock_domain_id,
   runtime_ref:ref(runtime),valid_before_tick_ns:expiry});
  const evidence={policy:template.policy,runtime,health},name=path.basename(evidenceFile),
   privateKey=file(name+'.key.pem',key.privateKey.export({type:'pkcs8',format:'pem'}));
  const profile=file(name+'.signer.json',canonical({format:'leafseamer.authentication-clock-signing-profile/1',service_id:service.service_id,
   database_instance_id:service.database_instance_id,policy_digest:template.policy.digest,native_binary_digest:source.artifactDigest,
   private_key_file:privateKey,evidence_file:evidenceFile}));
  const invoke=statement=>spawnSync(process.execPath,['--disallow-code-generation-from-strings',clockEvidenceCli,
   '--profile',profile,'--statement',file(name+'.statement.json',canonical(statement))],{encoding:'utf8',windowsHide:true,timeout:5000});
  const result=invoke({challenge:probe,evidence});
  assert.equal(result.status,0,result.stderr||result.error?.message);
  assert.deepEqual(JSON.parse(result.stdout),{state:'published',service_id:service.service_id,service_boot_id:probe.service_boot_id});
  const bytes=fs.readFileSync(evidenceFile,'utf8');
  if(checkRejection){
   const expiredHealth=seal({...oldHealth,id:id(),service_boot_id:probe.service_boot_id,clock_domain_id:runtime.clock_domain_id,
    runtime_ref:ref(runtime),valid_before_tick_ns:probe.tick_ns});
   for(const [statement,error] of [
    [{challenge:{...probe,database_instance_id:id()},evidence},'clock_evidence_challenge'],
    [{challenge:{...probe,descriptor_digest:digest('another domain')},evidence},'clock_evidence_runtime'],
    [{challenge:probe,evidence:{...evidence,health:expiredHealth}},'auth_clock_evidence_expired']
   ]){
    const rejected=invoke(statement);assert.equal(rejected.status,1,rejected.error?.message);
    assert.deepEqual(JSON.parse(rejected.stderr),{error});assert.equal(fs.readFileSync(evidenceFile,'utf8'),bytes);
   }
  }
  return bytes;
 }
 const native=launch(controllerProfile,{executable:controllerCli,dataDir:controllerDir,port:sitePort,supervised:true});
 publishEvidence(await native.event('authentication_clock_challenge'),controllerTemplate,controllerClockKey,controllerEvidenceFile,controller,controllerSource,{checkRejection:true});
 assert.equal((await native.event('listening')).pairing_origin,origin(controllerPort));
 const admin=(operation,request,expected=null,{raw=false}={})=>{
  const result=spawnSync(process.execPath,['--disallow-code-generation-from-strings',controllerCli,'pairing-admin','--origin',origin(sitePort),'--ca-file',caFile],
   {input:canonical({...credentials,operation,request}),windowsHide:true,timeout:20000});
  if(expected){assert.equal(result.status,1,result.error?.message);assert.deepEqual(JSON.parse(result.stderr.toString('utf8')),{error:expected});return;}
  assert.equal(result.status,0,result.stderr.toString('utf8')||result.error?.message);
  const body=JSON.parse(result.stdout.toString('utf8'));assert.deepEqual(result.stdout,Buffer.from(canonical(body)));
  return raw?result.stdout:body;
 };
 const unauthenticated=await new Promise((resolve,reject)=>{
  const body=canonical({operation:'inspect',request:{binding_id:id()}});
  const req=https.request(origin(sitePort)+'/auth/local-pairing/v1',{ca,method:'POST',
   headers:{Origin:origin(sitePort),'Content-Type':'application/json','Content-Length':Buffer.byteLength(body)}},res=>{
   res.resume();res.on('end',()=>resolve(res.statusCode));
  });
  req.on('error',reject);req.end(body);
 });
 assert.equal(unauthenticated,401);
 const begin=BigInt(Date.now()-2000)*1000000n,end=begin+120000000000n;
 const party=(identity,port,cert,permissions)=>{
  const x509=new crypto.X509Certificate(cert);
  return {...identity,binding_id:id(),permissions,object_scope_digest:digest({devices:[]}),not_before_ns:String(begin),expires_ns:String(end),
   certificate_der_sha256:sha(x509.raw),spki_sha256:sha(x509.publicKey.export({type:'spki',format:'der'})),trust_bundle_digest:trust,
   endpoint_config_digest:digest({origin:origin(port),server_name:'127.0.0.1'})};
 };
 const local=party(identityB,port,bc.cert,['auth.introspect']);
 const stage=admin('stage',{request_id:id(),transcript:{format:'leafseamer.peer-pairing/1',pair_attempt_id:id(),parties:[party(identityA,controllerPort,ac.cert,[]),local]}});
 const exportRequest={binding_id:stage.binding_id,transcript_digest:stage.transcript_digest};
 const signedBytes=admin('export',exportRequest,null,{raw:true});
 const signed=JSON.parse(signedBytes.toString('utf8')),authorizationId=signed.authorization.authorization_id;
 assert.equal(signed.authorization.administrator.principal_id,principal_id);
 assert.deepEqual(admin('export',exportRequest),signed);
 admin('activate',{request_id:id(),binding_id:stage.binding_id,expected_revision:stage.revision,transcript_digest:stage.transcript_digest},'peer_pairing_activation');
 const imported=spawnSync(process.execPath,['--disallow-code-generation-from-strings',cli,'import-first-pairing','--data-dir',dir,
  '--authorization-file',file('authorization.json',signedBytes),'--controller-certificate',controllerCert,'--seamer-certificate',seamerCert],
  {encoding:'utf8',windowsHide:true,timeout:8000});assert.equal(imported.status,0,imported.stderr);
 const source=openSeamerClock(),clockDomain=id(),descriptorDigest=digest('controlled CLI qualification'),
  template=clockFixture({service_id:service.service_id,service_boot_id:id()},{source,descriptor:{clock_domain_id:clockDomain,descriptor_digest:descriptorDigest}},
   {sampleWindowPolicy:'authentication-clock-sample/2'}).evidence;
 const key=crypto.generateKeyPairSync('ed25519'),evidenceFile=path.join(dir,'clock-evidence.json');
 const policyRef={kind:'policy',scope:{authority_service_id:service.service_id,database_instance_id:service.database_instance_id},
  object_id:'policy',revision:'1',digest:digest({})};
 const policyApproval=canonical({format:'leafseamer.publication-policy-approval/1',policy_ref:policyRef,not_before_ns:String(begin),expires_ns:String(end)});
 const profilePath=file('pairing-profile.json',JSON.stringify({format:'leafseamer.seamer-pairing-profile/1',
  scope:{service_id:service.service_id,database_instance_id:service.database_instance_id},first_pairing_authorization_id:authorizationId,
  local:{credential_id:local.credential_id,host:'127.0.0.1',port,certificate_file:seamerCert,private_key_file:file('seamer-key.pem',bc.key),ca_files:[caFile]},
  public_api:{binding_id:local.binding_id,host:'127.0.0.1',port:publicPort,certificate_file:seamerCert,private_key_file:path.join(dir,'seamer-key.pem'),
   allowed_origins:[origin(sitePort)],local_permissions:true,local_event_bindings:true},
  publication_policy:{approval_file:file('publication-policy.json',policyApproval),approval_sha256:sha(policyApproval)},
  controller:{identity:identityA,origin:origin(controllerPort),server_name:'127.0.0.1',certificate_file:controllerCert,trust_bundle_digest:trust},
  clock:{policy_digest:template.policy.digest,evidence_file:evidenceFile,public_key_file:file('clock-key.pem',key.publicKey.export({type:'spki',format:'pem'}))}}));
 const publish=probe=>publishEvidence(probe,template,key,evidenceFile,service,source);
 const first=launch(profilePath),probe=await first.event('authentication_clock_challenge');
 const initialBytes=publish(probe),listening=await first.event('listening');const staged=await first.event('first_pairing_staged');
 assert.equal(listening.public_origin,origin(publicPort));
 assert.equal(staged.authorization_id,authorizationId);
 let proved=admin('prove',{binding_id:stage.binding_id}),deadline=performance.now()+8000;
 while(proved.state!=='proved'){
  assert.ok(performance.now()<deadline);
  await delay(30);proved=admin('inspect',{binding_id:stage.binding_id});
 }
 const activation={request_id:id(),binding_id:stage.binding_id,expected_revision:proved.revision,transcript_digest:proved.transcript_digest};
 const activated=admin('activate',activation);
 assert.equal(activated.state,'activated');assert.deepEqual(admin('activate',activation),activated);
 const {receipt}=await first.event('first_pairing_completed');assert.equal(receipt.administrator.principal_id,principal_id);
 assert.equal(admin('inspect',{binding_id:stage.binding_id}).state,'activated');
 const permissions=['auth.manage','auth.read','definition.edit','definition.read','request.create','request.read'];
 const grantInput={request_id:id(),principal_id,audience:service.service_id,expected_revision:null,permissions,
  not_before_ns:activated.not_before_ns,expires_ns:activated.expires_ns,pairing_ref:{id:activated.binding_id,revision:activated.revision,digest:activated.digest}};
 const audienceGrant=admin('grant-audience',grantInput);
 assert.deepEqual(admin('grant-audience',grantInput),audienceGrant);
 assert.deepEqual(admin('read-audience',{principal_id,audience:service.service_id}).permissions,permissions);
 const wireHeaders={'x-leafseamer-contract':contractSelection.contract_id,'x-leafseamer-contract-digest':contractSelection.contract_digest};
 const send=(base,target,method,headers,body)=>new Promise((resolve,reject)=>{
  const req=https.request(new URL(target,base),{ca,method,headers,agent:false,rejectUnauthorized:true},res=>{
   const chunks=[];
   res.on('data',chunk=>chunks.push(chunk));res.on('error',reject);
   res.on('end',()=>resolve({status:String(res.statusCode),headers:Array.from({length:res.rawHeaders.length/2},(_,i)=>[res.rawHeaders[2*i],res.rawHeaders[2*i+1]]),body:Buffer.concat(chunks)}));
  });req.on('error',reject);req.setTimeout(10000,()=>req.destroy());req.end(body);
 });
 const post=(base,target,body,headers={})=>{
  const bytes=Buffer.from(canonical(body));
  return send(base,target,'POST',{'content-type':'application/json','content-length':String(bytes.length),...headers},bytes);
 };
 const login=await post(origin(sitePort),'/auth/sessions',credentials,{...wireHeaders,origin:origin(sitePort)});
 assert.equal(login.status,'201');
 const loginHeaders=new Headers(login.headers);
 const tokenResponse=await post(origin(sitePort),'/auth/service-tokens',
  {audience:service.service_id,requested_scopes:permissions,extension_instance_id:null},
  {...wireHeaders,origin:origin(sitePort),cookie:loginHeaders.get('set-cookie').split(';')[0],'x-csrf-token':loginHeaders.get('x-csrf-token')});
 assert.equal(tokenResponse.status,'201',tokenResponse.body.toString());
 const token=JSON.parse(tokenResponse.body).data.access_token,authorization='Bearer '+token;
 assert.equal((await post(origin(publicPort),'/auth/local-event-bindings/v1',{operation:'inspect',request:{approval_id:id()}})).status,'401');
 const permission=(operation,request,headers={})=>post(origin(publicPort),'/auth/local-permissions/v1',{operation,request},{authorization,...headers});
 const client=createClient('seamer',contractSelection,packet=>send(origin(publicPort),packet.target,packet.method,
  {...Object.fromEntries(packet.headers),authorization,origin:origin(sitePort)},packet.body));
 const contextBody={audience:service.service_id,selection:contractSelection,operations:['createDefinition','getDraft'],ttl_ms:30000,max_unique_requests:2};
 assert.equal((await client.call('createRequestContext',{body:contextBody})).status,'403');
 assert.equal((await permission('inspect',{principal_id},{origin:origin(sitePort)})).status,'403');
 const initialGrant=await permission('inspect',{principal_id});
 assert.equal(initialGrant.status,'200',initialGrant.body.toString());
 const current=JSON.parse(initialGrant.body).result;
 assert.deepEqual(current.permissions,['auth.manage','auth.read']);
 const localChange={request_id:id(),principal_id,principal_kind:'human',expected_revision:current.revision,
  permissions,definition_scope:current.definition_scope,not_before_ns:current.not_before_ns,expires_ns:current.expires_ns};
 const replaced=await permission('replace',localChange);
 assert.equal(replaced.status,'200',replaced.body.toString());
 assert.deepEqual((await permission('replace',localChange)).body,replaced.body);
 const context=await client.call('createRequestContext',{body:contextBody});
 assert.equal(context.status,'201');
 const create={context:{context_id:context.body.data.context_id,idempotency_key:id()},
  content:{program:draftSource().program,selections:[],policy_ref:policyRef},
  metadata:{name:'首次配对草稿',description:'',layout:{}}};
 const draft=await client.call('createDefinition',{body:create});
 assert.equal(draft.status,'201');
 assert.deepEqual((await client.call('createDefinition',{body:create})).body,draft.body);
 await first.close();
 const second=launch(profilePath),fresh=await second.event('authentication_clock_challenge');assert.notEqual(fresh.service_boot_id,probe.service_boot_id);
 await delay(150);assert.ok(!second.frames.some(event=>event.event==='listening'));
 publish(fresh);assert.deepEqual((await second.event('first_pairing_completed')).receipt,receipt);
 assert.deepEqual((await client.call('getDraft',{path:{definition_id:draft.body.definition_id}})).body,draft.body);
 if(verifyBrowser){
  const spki=cert=>crypto.createHash('sha256').update(new crypto.X509Certificate(cert).publicKey.export({type:'spki',format:'der'})).digest('base64');
  const config=file('browser-config.json',canonical({...browserOptions,flow:'draft-only',origin:origin(sitePort),seamerOrigin:origin(publicPort),
   spkis:[spki(site.cert),spki(bc.cert)],content:create.content,existingDraft:{id:draft.body.definition_id,name:draft.body.metadata.name}}));
  const checked=spawnSync(process.execPath,[path.join(guiRoot,'test/seamer-draft-browser-check.mjs'),config],
   {encoding:'utf8',windowsHide:true,timeout:60000,env:{...process.env,NODE_EXTRA_CA_CERTS:caFile}});
  assert.equal(checked.status,0,[checked.stderr,checked.stdout,checked.error?.code].join('\n'));
  t.diagnostic(checked.stdout.trim());
 }
 const recordedEvidence=fs.readFileSync(evidenceFile);
 fs.unlinkSync(evidenceFile);
 const readDraft=()=>client.call('getDraft',{path:{definition_id:draft.body.definition_id}});
 assert.equal((await readDraft()).status,'503');
 fs.writeFileSync(evidenceFile,recordedEvidence);
 assert.equal((await readDraft()).status,'503');
 await second.close();
 const altered=JSON.parse(initialBytes);altered.evidence.health.state='holdover';file('clock-evidence.json',canonical(altered));
 const denied=launch(profilePath);await denied.event('authentication_clock_challenge');
 assert.equal((await denied.child.done)[0],1);assert.deepEqual(denied.failure(),{error:'pairing_clock_signature'});
 inspect(dir,db=>{
  assert.equal(db.prepare('SELECT count(*) FROM first_pairing_completion').pluck().get(),1);
  assert.equal(db.prepare('SELECT count(*) FROM local_permission_head').pluck().get(),1);
 });
});

for(const rollback of [false,true])test(rollback?'首次配对完成记录漏写时激活及首位权限一起回滚':'首次配对经双向 TLS 证明后消费授权，并切换到真实人员核验',{timeout:120000},async t=>{
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'leafseamer-first-enrollment-'));
 t.after(()=>{assert.equal(path.dirname(path.resolve(dir)),path.resolve(os.tmpdir()));fs.rmSync(dir,{recursive:true,force:true,maxRetries:3,retryDelay:50});});
 const service=initializeStorage(dir);migrateStorage(dir,'18');
 const {leaves:[ac,bc],ca}=peerCertificates(),controller=await setup(t,93);
 const enrollment=controller.store.enrollment.begin();
 await controller.store.enrollment.complete({token_id:enrollment.token_id,secret:enrollment.secret,login:'role.test',password:'Synthetic-role-password-123',mode:'standalone'});
 const auth=await authenticated(t,controller,{grant:false,native:{source:openNativeClock(),descriptor:{clock_domain_id:id(),descriptor_digest:digest('administration clock')}},sampleWindowPolicy:'authentication-clock-sample/2'});
 const api=auth.sessions.peerPairings,{issuer_id,issuer_epoch,principal_id}=auth.logged.session,
  issuer={service_id:controller.store.identity.service_id,issuer_id,issuer_epoch};
 const identityA={...issuer,database_instance_id:controller.store.identity.database_instance_id,service_kind:'controller',credential_id:id()};
 const identityB={service_id:service.service_id,database_instance_id:service.database_instance_id,service_kind:'seamer',issuer_id,issuer_epoch,credential_id:id()};
 const introspection=createIntrospectionEndpoint({sessions:auth.sessions,serviceId:issuer.service_id,serviceBootId:controller.store.serviceBootId});
 const a=await startPeerPairingTransport({pairings:api,clock:auth.real,identity:identityA,...ac,ca,introspection});t.after(()=>a.close());
 const preflight=await startPeerPairingTransport({pairings:{},clock:auth.real,identity:identityB,...bc,ca});
 const port=Number(new URL(preflight.origin).port),configuration=preflight.configuration;
 a.bindPeers([{identity:identityB,cert:bc.cert,origin:preflight.origin,server_name:'127.0.0.1',trust_bundle_digest:configuration.trust_bundle_digest}]);
 await preflight.close();
 const begin=BigInt(auth.real.current().lower_ns)-1000000000n,end=begin+120000000000n;
 const party=(config,permissions)=>({...config,binding_id:id(),permissions,object_scope_digest:digest({devices:[]}),not_before_ns:String(begin),expires_ns:String(end)});
 const parties=[party(a.configuration,[]),party(configuration,['auth.introspect'])];
 const stageA=api.stage(auth.auth,{request_id:id(),transcript:{format:'leafseamer.peer-pairing/1',pair_attempt_id:id(),parties}});
 const issued=api.exportFirstPairingAuthorization(auth.auth,{binding_id:stageA.binding_id,transcript_digest:stageA.transcript_digest}),authorizationId=issued.authorization.authorization_id;
 const file=(name,bytes)=>{const target=path.join(dir,name);fs.writeFileSync(target,bytes);return target;};
 const imported=spawnSync(process.execPath,['--disallow-code-generation-from-strings',cli,'import-first-pairing','--data-dir',dir,
  '--authorization-file',file('authorization.json',canonical(issued)),'--controller-certificate',file('controller.pem',ac.cert),
  '--seamer-certificate',file('seamer.pem',bc.cert)],{encoding:'utf8',windowsHide:true,timeout:8000});
 assert.equal(imported.status,0,imported.stderr);
 assert.deepEqual(migrateStorage(dir,'19'),{...service,schema_revision:'19'});
 if(rollback)inspect(dir,db=>db.exec('CREATE TRIGGER test_completion_omit BEFORE INSERT ON first_pairing_completion BEGIN SELECT RAISE(IGNORE); END;'));
 const options={dataDir:dir,pairing:{...bc,ca,port,issuer,credential_id:identityB.credential_id},
  authenticationProvider({scope,source}){return clockFixture(scope,{source,descriptor:{clock_domain_id:id(),descriptor_digest:digest('enrollment clock')}},
   {sampleWindowPolicy:'authentication-clock-sample/2'}).provider;}};
 const configured=async(overrides={})=>{
  const app=await startSeamer({...options,pairing:{...options.pairing,...overrides}});t.after(()=>app.close());
  app.pairing.transport.bindPeers([{identity:identityA,cert:ac.cert,origin:a.origin,server_name:'127.0.0.1',trust_bundle_digest:a.configuration.trust_bundle_digest}]);
  return app;
 };
 if(!rollback){
  const wrong=await configured({credential_id:id()});
  assert.throws(()=>wrong.pairing.stageFirstPairing(authorizationId),{code:'peer_pairing_configuration_changed'});
  await wrong.close();
 }
 const app=await configured();
 assert.equal(app.pairing.api.activateFirstPairing,undefined);
 assert.throws(()=>app.pairing.stageFirstPairing(id()),{code:'peer_pairing_first_enrollment_missing'});
 const staged=app.pairing.stageFirstPairing(authorizationId),binding=staged.binding_id;
 assert.deepEqual(app.pairing.stageFirstPairing(authorizationId),staged);
 assert.equal(staged.attempt_expires_ns,issued.authorization.expires_ns);
 await assert.rejects(app.pairing.completeFirstPairing(authorizationId),{code:'peer_pairing_activation'});
 await app.pairing.transport.prove(binding);await a.prove(stageA.binding_id);
 await assert.rejects(app.pairing.completeFirstPairing(authorizationId),{code:'peer_pairing_first_authorization_remote'});
 const provedA=api.get(auth.auth,stageA.binding_id);
 const activeA=api.activate(auth.auth,{request_id:id(),binding_id:stageA.binding_id,expected_revision:provedA.revision,transcript_digest:provedA.transcript_digest});
 if(rollback){
  await assert.rejects(app.pairing.completeFirstPairing(authorizationId),{code:'peer_pairing_first_enrollment_storage'});
  assert.equal(app.pairing.transport.configuration.service_id,service.service_id);
  await app.close();
  inspect(dir,db=>{
   assert.equal(db.prepare('SELECT state FROM peer_pairing_revision r JOIN peer_pairing_head h USING(binding_id,revision)').pluck().get(),'proved');
   for(const table of ['local_subject','local_permission_head','local_permission_operation','local_permission_outbox','first_pairing_completion'])
    assert.equal(db.prepare('SELECT count(*) FROM '+table).pluck().get(),0,table);
   assert.equal(db.prepare('SELECT count(*) FROM first_pairing_import').pluck().get(),1);
   assert.deepEqual(db.pragma('foreign_key_check'),[]);
  });
  return;
 }
 const completed=await app.pairing.completeFirstPairing(authorizationId);
 assert.equal(completed.administrator.principal_id,principal_id);assert.equal(completed.state,'activated');
 assert.deepEqual(await app.pairing.completeFirstPairing(authorizationId),completed);
 assert.throws(()=>app.pairing.stageFirstPairing(authorizationId),{code:'peer_pairing_first_import_already_configured'});
 const permissions=['auth.manage','auth.read'];
 auth.sessions.remotePermissions.replace(auth.auth,{request_id:id(),principal_id,audience:service.service_id,expected_revision:null,permissions,
  not_before_ns:activeA.not_before_ns,expires_ns:activeA.expires_ns,
  pairing_ref:{id:activeA.binding_id,revision:activeA.revision,digest:activeA.digest}});
 const token=auth.sessions.issueToken(auth.auth,{request_id:id(),audience:service.service_id,requested_scopes:permissions});
 const bearer='v1.'+token.token.token_id+'.'+token.secret,
  handle=await app.pairing.identityReceiver.prepare({binding_id:binding,token:bearer});
 assert.equal(app.pairing.api.get(handle,binding).digest,completed.pairing_ref.digest);
 assert.deepEqual(app.authorization.get(handle,principal_id).permissions,permissions);
 await app.close();
 const restarted=await configured();assert.deepEqual(await restarted.pairing.completeFirstPairing(authorizationId),completed);await restarted.close();
 inspect(dir,db=>{
  assert.equal(db.prepare('SELECT count(*) FROM first_pairing_completion').pluck().get(),1);
  assert.equal(db.prepare('SELECT count(*) FROM local_permission_head').pluck().get(),1);
  assert.deepEqual(db.pragma('foreign_key_check'),[]);
 });
});
