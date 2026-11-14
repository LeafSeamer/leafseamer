import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import net from 'node:net';
import crypto from 'node:crypto';
import {spawn,spawnSync} from 'node:child_process';
import {once} from 'node:events';
import {fileURLToPath} from 'node:url';
import {startSeamer} from '../dist/server.mjs';
import {initializeStorage,openStorage} from '../dist/storage.mjs';
import {peerCertificates} from '../../leafseamer-controller/test/fixtures/peer-certificates.mjs';
import {fixture} from './auth-clock-fixture.mjs';
const cli=process.env.LEAFSEAMER_CLI_UNDER_TEST??fileURLToPath(new URL('../dist/cli.mjs',import.meta.url));assert.ok(path.isAbsolute(cli));
function directory(t){const dir=fs.mkdtempSync(path.join(os.tmpdir(),'leafseamer-seamer-process-'));t.after(()=>{assert.equal(path.dirname(path.resolve(dir)),path.resolve(os.tmpdir()));fs.rmSync(dir,{recursive:true,force:true});});return dir;}
function launch(dir){
 const child=spawn(process.execPath,['--disallow-code-generation-from-strings',cli,'start','--data-dir',dir],{stdio:['ignore','pipe','pipe'],windowsHide:true});
 let stdout='',stderr='';child.stderr.on('data',part=>{stderr+=part;});
 const exited=once(child,'close');const listening=new Promise((resolve,reject)=>{
  child.on('error',reject);child.stdout.on('data',part=>{stdout+=part;if(stdout.includes('\n')){try{resolve(JSON.parse(stdout.split('\n')[0]));}catch(error){reject(error);}}});
  exited.then(([code])=>reject(new Error('start_failed:'+code+':'+stderr)));
 });return {child,listening,exited};
}
test('真实CLI进程独立启动、HTTP保护态、排他拒绝和进程终止后重启', {timeout:15000},async t=>{
 const dir=directory(t),init=spawnSync(process.execPath,['--disallow-code-generation-from-strings',cli,'init-storage','--data-dir',dir],{encoding:'utf8',windowsHide:true});
 assert.equal(init.status,0,init.stderr);const identity=JSON.parse(init.stdout);const first=launch(dir);t.after(()=>first.child.kill());
 const ready=await first.listening;assert.equal(ready.authority,'seamer');assert.equal(ready.service_id,identity.service_id);assert.equal(ready.ready,false);
 const origin='http://127.0.0.1:'+ready.port;
 for(const [url,status]of [['/health/live',200],['/health/ready',503],['/.well-known/leafseamer-contracts',503],['/api/v1/runs',404]]){const response=await fetch(origin+url);assert.equal(response.status,status);assert.equal(response.headers.get('cache-control'),'no-store');}
 assert.equal((await fetch(origin+'/api/v1/runs',{method:'POST'})).status,405);
 const blocked=spawnSync(process.execPath,['--disallow-code-generation-from-strings',cli,'start','--data-dir',dir],{encoding:'utf8',windowsHide:true});assert.equal(blocked.status,1);assert.deepEqual(JSON.parse(blocked.stderr),{error:'database_in_use'});
 first.child.kill();await first.exited;const second=launch(dir);t.after(()=>second.child.kill());const resumed=await second.listening;
 assert.equal(resumed.service_id,ready.service_id);assert.equal(resumed.database_instance_id,ready.database_instance_id);assert.notEqual(resumed.service_boot_id,ready.service_boot_id);assert.equal(resumed.ready,false);
 second.child.kill();await second.exited;
});
test('监听失败释放数据库，服务正常关闭后可以重新打开',async t=>{
 const dir=directory(t);initializeStorage(dir);const app=await startSeamer({dataDir:dir});
 const other=directory(t);initializeStorage(other);await assert.rejects(startSeamer({dataDir:other,port:app.address.port}));
 const storage=openStorage(other);storage.close();await app.close();const reopened=openStorage(dir);reopened.close();
});

test('配对监听启动失败关闭已启动HTTP与认证时钟并释放文件库',async t=>{
 const blocker=net.createServer();await new Promise(resolve=>blocker.listen(0,'127.0.0.1',resolve));
 t.after(()=>new Promise(resolve=>blocker.close(resolve)));
 const reservation=net.createServer();await new Promise(resolve=>reservation.listen(0,'127.0.0.1',resolve));
 const port=reservation.address().port;await new Promise(resolve=>reservation.close(resolve));
 const certificates=peerCertificates(),issuer={service_id:crypto.randomUUID(),issuer_id:crypto.randomUUID(),issuer_epoch:crypto.randomUUID()};
 const dir=directory(t);initializeStorage(dir);
 let evidenceReads=0;
 const authenticationProvider=({scope,source})=>{const f=fixture(scope,{source},{sampleWindowPolicy:'authentication-clock-sample/2'});return {read(){evidenceReads++;return f.provider.read();}};};
 await assert.rejects(startSeamer({dataDir:dir,port,authenticationProvider,pairing:{...certificates.leaves[0],ca:certificates.ca,issuer,credential_id:crypto.randomUUID(),port:blocker.address().port}}),{code:'EADDRINUSE'});
 // 公共HTTPS在内部TLS之后启动；失败也必须释放先前全部监听和文件库。
 const peerReservation=net.createServer();await new Promise(resolve=>peerReservation.listen(0,'127.0.0.1',resolve));const peerPort=peerReservation.address().port;await new Promise(resolve=>peerReservation.close(resolve));
 await assert.rejects(startSeamer({dataDir:dir,port,authenticationProvider,pairing:{...certificates.leaves[0],ca:certificates.ca,issuer,credential_id:crypto.randomUUID(),port:peerPort},
  publicApi:{...certificates.leaves[0],binding_id:crypto.randomUUID(),port:blocker.address().port}}),{code:'EADDRINUSE'});
 const peerReleased=net.createServer();await new Promise((resolve,reject)=>{peerReleased.once('error',reject);peerReleased.listen(peerPort,'127.0.0.1',resolve);});await new Promise(resolve=>peerReleased.close(resolve));
 assert.ok(evidenceReads>0);const before=evidenceReads;
 const app=await startSeamer({dataDir:dir,port});t.after(()=>app.close());
 assert.equal((await fetch('http://127.0.0.1:'+port+'/health/live')).status,200);
 // 跨过采样周期，确认失败实例的认证定时器已经停止。
 await new Promise(resolve=>setTimeout(resolve,550));assert.equal(evidenceReads,before);
 assert.strictEqual(app.close(),app.close());await app.close();
 const reopened=openStorage(dir);reopened.close();
});
