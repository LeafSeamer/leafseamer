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
test('Windows Seamer 使用真实原生采样和文件库，健康失效锁存', {skip:process.platform!=='win32'},async t=>{
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

test('Linux Seamer 在原生连续性证据不可用时保持保护态', {skip:process.platform!=='linux'},async t=>{
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'leafseamer-linux-clock-'));
 t.after(()=>{assert.equal(path.dirname(path.resolve(dir)),path.resolve(os.tmpdir()));fs.rmSync(dir,{recursive:true,force:true});});
 initializeStorage(dir);
 const app=await startSeamer({dataDir:dir,authenticationProvider({scope,source}){
  const capture=source.capture();
  assert.equal(capture.raw.platform,'linux');
  assert.equal(capture.continuity_available,false);
  return fixture(scope,{source}).provider;
 }});
 try{
  assert.deepEqual(app.authenticationStatus(),{state:'faulted',reason:'auth_clock_fault_latched',qualified:false,durable:true});
  const ready=await fetch('http://127.0.0.1:'+app.address.port+'/health/ready');
  assert.equal(ready.status,503);
  assert.deepEqual(await ready.json(),{ready:false,reason:'authentication_clock_unqualified'});
 }finally{await app.close();}
 const db=new Database(path.join(dir,'seamer.sqlite'),{readonly:true,fileMustExist:true});
 try{
  assert.equal(db.prepare('SELECT count(*) FROM authentication_clock_acquisition').pluck().get(),0);
  assert.equal(db.prepare('SELECT count(*) FROM authentication_clock_fault').pluck().get(),1);
 }finally{db.close();}
});
