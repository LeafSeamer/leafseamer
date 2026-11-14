import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import {initializeStorage} from '../dist/storage.mjs';
import {startSeamer} from '../dist/server.mjs';
import {fixture} from './auth-clock-fixture.mjs';
import {seal} from '../dist/runtime/auth-clock-value.mjs';
test('Seamer使用自身真实原生采样和文件库；健康失效锁存，重启不恢复旧资格',async t=>{
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'leafseamer-seamer-clock-'));t.after(()=>{assert.equal(path.dirname(path.resolve(dir)),path.resolve(os.tmpdir()));fs.rmSync(dir,{recursive:true,force:true});});
 const identity=initializeStorage(dir);let evidence,source;
 const app=await startSeamer({dataDir:dir,authenticationProvider({scope,source:actual}){
  source=actual;const f=fixture(scope,{source:actual},{sampleWindowPolicy:'authentication-clock-sample/2'});evidence=f.evidence;return f.provider;
 }});t.after(()=>app.close());assert.equal(app.identity.service_id,identity.service_id);assert.equal(app.authenticationStatus().qualified,true);assert.ok(source.tick()>0n);
 const response=await fetch('http://127.0.0.1:'+app.address.port+'/health/ready');assert.equal(response.status,503);assert.equal((await response.json()).reason,'business_incomplete');
 const {digest:_old,...health}=evidence.health;evidence.health=seal({...health,state:'unavailable'});
 assert.equal(app.authenticationStatus().state,'faulted');assert.equal(app.authenticationStatus().durable,true);evidence.health=seal(health);assert.equal(app.authenticationStatus().qualified,false);
 await app.close();const db=new Database(path.join(dir,'seamer.sqlite'));try{
  assert.ok(db.prepare('SELECT count(*) FROM authentication_clock_acquisition').pluck().get()>0);assert.equal(db.prepare('SELECT count(*) FROM authentication_clock_fault').pluck().get(),1);
  assert.deepEqual(db.prepare('PRAGMA foreign_key_check').raw().all(),[]);
 }finally{db.close();}
 const restarted=await startSeamer({dataDir:dir});try{assert.equal(restarted.identity.service_id,identity.service_id);assert.notEqual(restarted.serviceBootId,app.serviceBootId);assert.equal(restarted.authenticationStatus().qualified,false);}finally{await restarted.close();}
});
