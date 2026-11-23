import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import dgram from 'node:dgram';
import {spawn,spawnSync} from 'node:child_process';
import {once} from 'node:events';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {setTimeout as delay} from 'node:timers/promises';

const sha=bytes=>crypto.createHash('sha256').update(bytes).digest('hex');
export function writeNativePeerMessage(file,value){
 fs.writeFileSync(file+'.next',JSON.stringify(value),{flag:'wx'});
 fs.renameSync(file+'.next',file);
}

export async function startNativeReferenceEventPeer({packageRoot,assets,scope,resume,canonical,digest,clock=null}){
 assert.equal(process.platform,'win32');assert.ok(path.isAbsolute(assets));
 const logs=fileURLToPath(new URL('../../logs/',import.meta.url));fs.mkdirSync(logs,{recursive:true});
 const root=fs.mkdtempSync(path.join(logs,'seamer-event-reference-'));
 const udp=dgram.createSocket('udp4');let child,exited,closing,packets=0,stderr='';
 function close(){return closing??=(async()=>{
  if(child&&child.exitCode===null&&child.signalCode===null)child.stdin.end('stop\n');
  const result=await exited?.catch(()=>{});
  await new Promise(resolve=>udp.close(resolve));
  const target=fs.realpathSync(root);assert.equal(path.dirname(target),fs.realpathSync(logs));
  fs.rmSync(target,{recursive:true,force:true,maxRetries:5,retryDelay:50});
  return result;
 })();}
 try{
  udp.bind(0,'127.0.0.1');await once(udp,'listening');
  const timeline=resume.input.referenceTimeline;
  const ntpBase=2208988800n,second=1000000000n,fraction=1n<<32n;
  udp.on('message',(request,peer)=>{
   packets++;
   const reply=Buffer.alloc(48);reply[0]=36;reply[1]=1;reply[19]=1;
   request.copy(reply,24,40,48);
   if(timeline){
    // 仅限本机联调。两个回环端以同一 Windows 单调时基推进测试 UTC。
    const utc=BigInt(timeline.utc_ns)+clock.tick()-BigInt(timeline.tick_ns);
    const seconds=(utc/second+ntpBase)%fraction,subseconds=utc%second*fraction/second;
    reply.writeUInt32BE(Number(seconds),32);reply.writeUInt32BE(Number(subseconds),36);
    reply.copy(reply,40,32,40);
   }else{reply.writeUInt32BE(60,32);reply.writeUInt32BE(60,40);}
   udp.send(reply,peer.port,peer.address);
  });
  const dist=path.join(packageRoot,'dist'),runtime=path.join(root,'package','dist');
  const manifestAt=directory=>JSON.parse(fs.readFileSync(path.join(directory,'build-manifest.json'))).artifacts;
  const artifacts=manifestAt(dist),native=manifestAt(assets);
  function install(name,bytes){
   const target=path.join(runtime,name);fs.mkdirSync(path.dirname(target),{recursive:true});fs.writeFileSync(target,bytes);
  }
  for(const [name,hash] of Object.entries(artifacts)){
   const bytes=fs.readFileSync(path.join(dist,name));assert.equal(sha(bytes),hash);install(name,bytes);
  }
  for(const name of ['native/leafseamer_process_host.exe','runtime/node.exe','runtime/reference-time-provider.exe']){
   const bytes=fs.readFileSync(path.join(assets,name));assert.equal(sha(bytes),native[name]);install(name,bytes);artifacts[name]=native[name];
  }
  assert.ok(artifacts['reference-sampling.mjs']);
  const resumeFile=path.join(root,'resume.json'),request=path.join(root,'request.json'),response=path.join(root,'response.json');
  const entry=Buffer.from(`import fs from 'node:fs';
import {fileURLToPath} from 'node:url';
process.env.LEAFSEAMER_PACKAGE_UNDER_TEST=fileURLToPath(new URL('../',import.meta.url));
const {serveSupervisedPairing}=await import(${JSON.stringify(new URL('../pairing-process-fixture.mjs',import.meta.url).href)});
await serveSupervisedPairing(JSON.parse(fs.readFileSync(${JSON.stringify(resumeFile)})));
`);
  install('fixture-pairing-core.mjs',entry);artifacts['fixture-pairing-core.mjs']=sha(entry);
  const manifest=Buffer.from(JSON.stringify({format:'leafseamer-controller-build/1',artifacts},null,2)+'\n');
  install('build-manifest.json',manifest);
  fs.writeFileSync(resumeFile,JSON.stringify({resume,request,response,endpoint:'127.0.0.1:'+udp.address().port}),{flag:'wx'});
  const executable=path.join(runtime,'native/leafseamer_process_host.exe'),node=path.join(runtime,'runtime/node.exe');
  for(const operation of ['--initialize','--upgrade-time-provider']){
   const result=spawnSync(executable,[operation,resume.directory],{encoding:'utf8',windowsHide:true,timeout:10000});
   assert.equal(result.status,0,result.stdout+result.stderr);
  }
  const policy={format:'leafseamer-core-bootstrap-policy/1',...scope,artifact_manifest_ref:{manifest_id:'seamer.event.fixture',revision:'1',
   digest:digest({controller_build_sha256:sha(manifest),node_sha256:artifacts['runtime/node.exe']})},platform_profile_ref:{profile_id:'fixture.windows',revision:'1'},
   policy_revision:'1',bootstrap_timeout_ms:8000,clock_backend_revision:'interrupt.unbiased.1'};
  const timePolicy={...policy,format:'leafseamer-time-provider-bootstrap-policy/1',artifact_manifest_ref:{manifest_id:'time.event.fixture',revision:'1',
   digest:'sha256:'+artifacts['runtime/reference-time-provider.exe']}};
  child=spawn(executable,[resume.directory,node,path.join(runtime,'fixture-pairing-core.mjs'),'contained-core-auth',
   '--bootstrap-policy',canonical(policy),'--time-provider-policy',canonical(timePolicy)],{stdio:['pipe','pipe','pipe'],windowsHide:true});
  exited=once(child,'close');exited.catch(()=>{});child.stdin.on('error',()=>{});child.stdout.resume();
  child.stderr.on('data',part=>{stderr=(stderr+part).slice(-4096);});
  async function read(id){
   const until=performance.now()+18000;
   while(!fs.existsSync(response)){
    assert.equal(child.exitCode,null,'native_reference_peer_exit:'+stderr);assert.equal(child.signalCode,null,'native_reference_peer_signal:'+stderr);
    assert.ok(performance.now()<until,'native_reference_peer_timeout:'+stderr);await delay(10);
   }
   const result=JSON.parse(fs.readFileSync(response));fs.unlinkSync(response);assert.equal(result.id,id);
   if(!result.ok)throw Object.assign(new Error(result.error.message),{code:result.error.code,remoteStack:result.error.stack});
   return result.value;
  }
  await read('initial');
  return Object.freeze({async call(command,value){const id=crypto.randomUUID();
   assert.equal(fs.existsSync(request)||fs.existsSync(response),false);writeNativePeerMessage(request,{id,command,value});return read(id);
  },close,packetCount:()=>packets});
 }catch(error){await close();throw error;}
}
