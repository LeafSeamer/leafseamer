import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import net from 'node:net';
import dgram from 'node:dgram';
import {spawn,spawnSync} from 'node:child_process';
import {once} from 'node:events';
import {fileURLToPath} from 'node:url';
import {setTimeout as delay} from 'node:timers/promises';
import Database from 'better-sqlite3';
import {initializeStorage,migrateStorage,openStorage} from '../dist/storage.mjs';
import {canonical,digest,ref,seal} from '../dist/runtime/auth-clock-value.mjs';
import {openNativeClock} from '../dist/runtime/native-clock.mjs';
import {peerCertificates} from '../../leafseamer-controller/test/fixtures/peer-certificates.mjs';
import {fixture} from './auth-clock-fixture.mjs';

const assets=process.env.LEAFSEAMER_SUPERVISOR_DIST;
const id=()=>crypto.randomUUID();
const sha=bytes=>crypto.createHash('sha256').update(bytes).digest('hex');
function write(root,name,contents){const target=path.join(root,name);fs.writeFileSync(target,contents);return target;}

async function freePort(){
 const socket=net.createServer();socket.listen(0,'127.0.0.1');await once(socket,'listening');
 const {port}=socket.address();await new Promise((resolve,reject)=>socket.close(error=>error?reject(error):resolve()));return port;
}

test('Seamer 在原生监督下核验本次启动并独立采样和封存参考锚点',
 {skip:process.platform!=='win32'||!assets,timeout:50000},async t=>{
  assert.ok(path.isAbsolute(assets));
  const logs=fileURLToPath(new URL('../logs/',import.meta.url));fs.mkdirSync(logs,{recursive:true});
  const root=fs.mkdtempSync(path.join(logs,'seamer-reference-host-'));
  const processes=[];
  t.after(async()=>{
   for(const {child,exited} of processes){if(child.exitCode===null)child.stdin.end('stop\n');await exited;}
   const target=fs.realpathSync(root);assert.equal(path.dirname(target),fs.realpathSync(logs));
   fs.rmSync(target,{recursive:true,force:true,maxRetries:5,retryDelay:50});
  });
  function mkdir(name){const target=path.join(root,name);fs.mkdirSync(target);return target;}
  const runtime=mkdir('runtime'),state=mkdir('state'),materials=mkdir('inputs');
  const udp=dgram.createSocket('udp4'),packets=[];
  udp.bind(0,'127.0.0.1');await once(udp,'listening');t.after(()=>new Promise(resolve=>udp.close(resolve)));
  udp.on('message',(request,peer)=>{
   packets.push(request);
   const response=Buffer.alloc(48);response[0]=36;response[1]=1;response[19]=1;
   request.copy(response,24,40,48);response.writeUInt32BE(0xffffffff,32);response.writeUInt32BE(0xffffffff,36);response[47]=1;
   udp.send(response,peer.port,peer.address);
  });
  const dist=fileURLToPath(new URL('../dist/',import.meta.url));
  const {artifacts}=JSON.parse(fs.readFileSync(path.join(dist,'build-manifest.json')));
  assert.ok(artifacts['supervised-reference.mjs']);
  const installed=JSON.parse(fs.readFileSync(path.join(assets,'build-manifest.json')));
  for(const name of ['native/leafseamer_process_host.exe','runtime/node.exe','runtime/reference-time-provider.exe']){
   const bytes=fs.readFileSync(path.join(assets,name));assert.equal(sha(bytes),installed.artifacts[name]);artifacts[name]=sha(bytes);
   fs.mkdirSync(path.dirname(path.join(runtime,name)),{recursive:true});write(runtime,name,bytes);
  }
  const executable=path.join(runtime,'native/leafseamer_process_host.exe');
  const seamerBuild=JSON.parse(fs.readFileSync(path.join(dist,'build-manifest.json')));
  for(const [name,hash] of Object.entries(seamerBuild.artifacts)){
   const bytes=fs.readFileSync(path.join(dist,name));assert.equal(sha(bytes),hash);
   fs.mkdirSync(path.dirname(path.join(runtime,name)),{recursive:true});write(runtime,name,bytes);
  }
  const fixtureInputs={
   'fixture-sampling-core.mjs':fs.readFileSync(new URL('./fixtures/supervised-reference-sampling-core.mjs',import.meta.url)),
   'fixture-sampling-authority.mjs':fs.readFileSync(new URL('./fixtures/reference-sampling-authority.mjs',import.meta.url)),
   'fixture-reference-authority.mjs':fs.readFileSync(new URL('../../leafseamer-controller/test/fixtures/reference-bundle-authority.mjs',import.meta.url)),
   'fixture-reference-endpoint.mjs':Buffer.from(`export const endpoint='127.0.0.1:${udp.address().port}';\n`)
  };
  for(const [name,bytes] of Object.entries(fixtureInputs)){write(runtime,name,bytes);artifacts[name]=sha(bytes);}
  // 已固定的宿主识别现有监督清单标记。混合运行目录仅用于本用例。
  const manifest=Buffer.from(JSON.stringify({format:'leafseamer-controller-build/1',artifacts},null,2)+'\n');
  write(runtime,'build-manifest.json',manifest);
  const node=path.join(runtime,'runtime/node.exe'),cli=path.join(runtime,'cli.mjs');
  function inspect(run){const db=new Database(path.join(state,'seamer.sqlite'),{fileMustExist:true});try{return run(db);}finally{db.close();}}
  initializeStorage(state);const priorIdentity=migrateStorage(state,'19');
  const priorBoot=openStorage(state);priorBoot.close();
  const history=()=>inspect(db=>({boots:db.prepare('SELECT * FROM service_boot ORDER BY service_boot_id').raw().all(),
   migrations:db.prepare('SELECT * FROM schema_migration WHERE CAST(to_revision AS INTEGER)<20 ORDER BY CAST(to_revision AS INTEGER)').raw().all()}));
  const priorHistory=history(),identity=migrateStorage(state,'20');
  assert.deepEqual(identity,{...priorIdentity,schema_revision:'20'});
  assert.deepEqual(history(),priorHistory);
  const scope={service_id:identity.service_id,database_instance_id:identity.database_instance_id};
  for(const command of ['--initialize','--upgrade-time-provider']){
   const result=spawnSync(executable,[command,state],{encoding:'utf8',windowsHide:true,timeout:10000});
   assert.equal(result.status,0,result.stdout+result.stderr);
  }
  const certificates=peerCertificates(),[local,remote]=certificates.leaves,issuer=id();
  const key=crypto.generateKeyPairSync('ed25519'),clock=openNativeClock();
  const clockPolicy=fixture({service_id:identity.service_id,service_boot_id:id()},{source:clock},{sampleWindowPolicy:'authentication-clock-sample/2'}).evidence.policy;
  const evidenceFile=path.join(materials,'clock.json'),port=await freePort();
  const profile=write(materials,'profile.json',canonical({format:'leafseamer.seamer-pairing-profile/1',scope,first_pairing_authorization_id:null,
   local:{credential_id:id(),host:'127.0.0.1',port,certificate_file:write(materials,'local.pem',local.cert),private_key_file:write(materials,'local-key.pem',local.key),
    ca_files:[write(materials,'ca.pem',certificates.ca)]},
   controller:{identity:{service_id:issuer,database_instance_id:id(),service_kind:'controller',issuer_id:id(),issuer_epoch:id(),credential_id:id()},
    origin:'https://127.0.0.1:49000',server_name:'127.0.0.1',certificate_file:write(materials,'remote.pem',remote.cert),trust_bundle_digest:digest('fixture trust')},
   clock:{policy_digest:clockPolicy.digest,evidence_file:evidenceFile,public_key_file:write(materials,'clock-key.pem',key.publicKey.export({type:'spki',format:'pem'}))}}));
  const corePolicy={format:'leafseamer-core-bootstrap-policy/1',...scope,artifact_manifest_ref:{manifest_id:'seamer.runtime.fixture',revision:'1',
   digest:digest({controller_build_sha256:sha(manifest),node_sha256:artifacts['runtime/node.exe']})},platform_profile_ref:{profile_id:'fixture.windows',revision:'1'},
   policy_revision:'1',bootstrap_timeout_ms:8000,clock_backend_revision:'interrupt.unbiased.1'};
  const timePolicy={...corePolicy,format:'leafseamer-time-provider-bootstrap-policy/1',artifact_manifest_ref:{manifest_id:'time.runtime.fixture',revision:'1',
   digest:'sha256:'+artifacts['runtime/reference-time-provider.exe']}};
  function start(entry=cli){
   const child=spawn(executable,[state,node,entry,'contained-core-auth','--bootstrap-policy',canonical(corePolicy),'--time-provider-policy',canonical(timePolicy),
    '--pairing-profile',profile],{stdio:['pipe','pipe','pipe'],windowsHide:true});
   const exited=once(child,'close'),frames=[];child.stdin.on('error',()=>{});
   for(const stream of [child.stdout,child.stderr]){
    let rest='';stream.setEncoding('utf8').on('data',chunk=>{
     rest+=chunk;const lines=rest.split('\n');rest=lines.pop();
     for(const line of lines)try{frames.push(JSON.parse(line));}catch{frames.push(line);}
    });
   }
   processes.push({child,exited});
   return {child,exited,frames,async find(event){
    const until=performance.now()+12000;
    while(!frames.some(frame=>frame.event===event)&&child.exitCode===null&&performance.now()<until)await delay(20);
    const frame=frames.find(frame=>frame.event===event);assert.ok(frame,JSON.stringify(frames));return frame;
   }};
  }
  function publishClock(challenge,wrongDomain=false){
   const {service_boot_id,native_binary_digest,clock_domain_id,descriptor_digest,continuity}=challenge;
   const clockScope={service_id:scope.service_id,service_boot_id},valid_before_tick_ns=String(clock.tick()+60000000000n);
   const runtime=seal({id:id(),revision:'1',...clockScope,clock_domain_id,descriptor_digest:wrongDomain?digest('other domain'):descriptor_digest,
    native_binary_digest,continuity,valid_before_tick_ns,policy_ref:ref(clockPolicy),runtime_profile_ref:clockPolicy.runtime_profile_ref,
    native_artifact_ref:clockPolicy.native_artifact_ref,environment_approval_ref:clockPolicy.environment_approval_ref});
   const health=seal({id:id(),revision:'1',...clockScope,clock_domain_id,runtime_ref:ref(runtime),policy_ref:ref(clockPolicy),health_profile_ref:clockPolicy.health_profile_ref,
    system_time_source_ref:clockPolicy.system_time_source_ref,environment_approval_ref:clockPolicy.environment_approval_ref,
    state:'synchronized',holdover_ref:null,valid_before_tick_ns});
   const format='leafseamer.authentication-clock-evidence/1',evidence={policy:clockPolicy,runtime,health};
   const signature=crypto.sign(null,Buffer.from(format+'\0'+canonical(evidence)),key.privateKey).toString('base64url');
   fs.renameSync(write(materials,'clock.new',canonical({format,evidence,signature})),evidenceFile);
  }
  const invalid=start(),bad=await invalid.find('authentication_clock_challenge');publishClock(bad,true);
  await invalid.exited;
  assert.ok(invalid.frames.some(frame=>frame.error==='pairing_clock_supervised_domain'),JSON.stringify(invalid.frames));
  assert.equal(invalid.frames.some(frame=>frame.event==='listening'),false);
  const adopted=()=>inspect(db=>({core:db.prepare('SELECT service_boot_id,receipt_id FROM core_runtime_binding').raw().all(),
   collectors:db.prepare('SELECT service_boot_id,runtime_id,body_jcs,body_digest FROM time_provider_runtime_binding').raw().all()}));
  assert.deepEqual(adopted(),{core:[],collectors:[]});
  inspect(db=>db.exec(`CREATE TRIGGER test_reject_collector BEFORE INSERT ON time_provider_runtime_binding BEGIN SELECT RAISE(IGNORE); END;`));
  const failed=start();publishClock(await failed.find('authentication_clock_challenge'));await failed.exited;
  assert.ok(failed.frames.some(frame=>frame.error==='reference_supervisor_storage'),JSON.stringify(failed.frames));
  assert.deepEqual(adopted(),{core:[],collectors:[]});
  inspect(db=>db.exec('DROP TRIGGER test_reject_collector'));
  const active=start(),challenge=await active.find('authentication_clock_challenge');
  assert.notEqual(challenge.service_boot_id,bad.service_boot_id);
  assert.notEqual(challenge.clock_domain_id,bad.clock_domain_id);publishClock(challenge);
  const started=await active.find('listening'),host=await active.find('process_contained');
  assert.deepEqual({service_id:started.service_id,database_instance_id:started.database_instance_id},scope);
  assert.equal(started.service_boot_id,challenge.service_boot_id);
  assert.deepEqual(started.reference_collector.clock_domain_ref,{clock_domain_id:challenge.clock_domain_id,descriptor_digest:challenge.descriptor_digest});
  assert.equal(started.reference_collector.provider_pid,host.time_provider_pid);
  assert.notEqual(host.time_provider_pid,host.core_pid);
  assert.equal((await fetch(`http://127.0.0.1:${started.port}/health/ready`)).status,503);
  active.child.stdin.end('stop\n');const [code]=await active.exited;
  const retired=active.frames.find(frame=>frame.event==='process_group_empty');
  assert.equal(code,0,JSON.stringify(active.frames));assert.equal(retired?.active_processes,0);
  const {core:[[coreBoot,coreReceiptId]],collectors:[[collectorBoot,runtimeId,body,bodyDigest]]}=adopted(),original=JSON.parse(body);
  assert.equal(coreBoot,challenge.service_boot_id);
  assert.equal(collectorBoot,challenge.service_boot_id);
  assert.equal(runtimeId,started.reference_collector.runtime_id);
  assert.equal(bodyDigest,started.reference_collector.runtime_digest);
  assert.equal(bodyDigest,digest(original));
  assert.equal(original.core_receipt_id,coreReceiptId);
  assert.equal(original.process_observation.provider_pid,host.time_provider_pid);
  assert.deepEqual(inspect(db=>db.prepare('PRAGMA foreign_key_check').raw().all()),[]);
  assert.equal(packets.length,0);
  const priorRuntime=adopted(),samplingIdentity=migrateStorage(state,'21');
  assert.deepEqual(samplingIdentity,{...identity,schema_revision:'21'});
  assert.deepEqual(adopted(),priorRuntime);
  const sampling=start(path.join(runtime,'fixture-sampling-core.mjs'));
  const samplerBoot=await sampling.find('authentication_clock_challenge');publishClock(samplerBoot);
  const sampler=await sampling.find('listening'),published=await sampling.find('seamer_reference_observed');
  assert.equal(published.current?.status,'conditional',JSON.stringify(published));
  assert.deepEqual(published.current.runtime_domain_ref,sampler.reference_collector.clock_domain_ref);
  assert.ok(published.current.bundle_ref?.digest);
  assert.equal(published.receiver?.current,true);
  assert.equal(published.receiver.withdrawn,true);
  assert.equal(published.receiver.context_digest,published.current.bundle_ref.digest);
  assert.equal(published.receiver.scope.authority_service_id,scope.service_id);
  assert.equal(published.receiver.scope.database_instance_id,scope.database_instance_id);
  sampling.child.stdin.end('stop\n');assert.equal((await sampling.exited)[0],0,JSON.stringify(sampling.frames));
  assert.equal(packets.length,1);assert.equal(packets[0].length,48);
  const readPersisted=()=>inspect(db=>db.prepare(`SELECT p.service_boot_id,p.body_jcs,t.body_jcs,c.body_jcs,c.ciphertext,r.confirmation_jcs,a.body_jcs,a.body_digest
   FROM time_provider_poll_reservation p JOIN time_provider_poll_attempt t USING(poll_id)
   JOIN time_provider_capture c USING(poll_id) JOIN time_provider_poll_release r USING(poll_id)
   JOIN time_provider_reference_anchor a USING(poll_id)`).raw().all());
  const persisted=readPersisted();
  assert.equal(persisted.length,1);
  const [[sampleBoot,pollJson,attemptJson,captureJson,ciphertext,ackJson,anchorJson,anchorDigest]]=persisted;
  const [poll,attempt,capture,ack,anchor]=[pollJson,attemptJson,captureJson,ackJson,anchorJson].map(JSON.parse);
  assert.equal(sampleBoot,samplerBoot.service_boot_id);assert.equal(poll.runtime_digest,sampler.reference_collector.runtime_digest);
  assert.equal(attempt.poll_ref.id,poll.poll_id);assert.equal(capture.attempt_id,attempt.attempt_id);
  assert.ok(Buffer.isBuffer(ciphertext)&&ciphertext.length>48&&!ciphertext.includes(packets[0]));
  assert.equal(ack.type,'POLL_STORED_ACK');assert.equal(ack.receipt.capture_ref.id,capture.capture_id);
  assert.equal(anchor.anchor_id,published.current.anchor_ref.id);assert.equal(anchorDigest,published.current.anchor_ref.digest);
  assert.equal(anchor.capture_ref.id,capture.capture_id);assert.equal(anchor.reference_bundle.digest,published.current.bundle_ref.digest);
  assert.deepEqual(inspect(db=>db.prepare('PRAGMA foreign_key_check').raw().all()),[]);
  assert.deepEqual(migrateStorage(state,'22'),{...samplingIdentity,schema_revision:'22'});
  assert.deepEqual(readPersisted(),persisted);
  assert.equal(inspect(db=>db.prepare('SELECT count(*) FROM event_age_revision').pluck().get()),0);
  const reopened=openStorage(state);reopened.close();
 });
