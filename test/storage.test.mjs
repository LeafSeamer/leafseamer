import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import {initializeStorage,openStorage} from '../dist/storage.mjs';
function directory(t){const dir=fs.mkdtempSync(path.join(os.tmpdir(),'leafseamer-seamer-test-'));t.after(()=>{assert.equal(path.dirname(path.resolve(dir)),path.resolve(os.tmpdir()));fs.rmSync(dir,{recursive:true,force:true,maxRetries:5,retryDelay:50});});return dir;}
test('Seamer身份只创建一次，排他writer拒绝第二实例，重启更换boot并保留服务身份',t=>{
 const dir=directory(t),identity=initializeStorage(dir);assert.equal(identity.authority,'seamer');assert.equal(identity.maintenance_state,'protected');
 assert.throws(()=>initializeStorage(dir),e=>e.code==='database_already_exists');
 const first=openStorage(dir);assert.deepEqual(first.identity,identity);assert.throws(()=>openStorage(dir),e=>e.code==='database_in_use');
 first.close();first.close();const second=openStorage(dir);assert.deepEqual(second.identity,identity);assert.notEqual(second.serviceBootId,first.serviceBootId);second.close();
 const db=new Database(path.join(dir,'seamer.sqlite'));try{assert.deepEqual(db.prepare('SELECT state FROM service_boot').pluck().all(),['stopped','stopped']);assert.deepEqual(db.prepare('PRAGMA foreign_key_check').raw().all(),[]);
  assert.throws(()=>db.prepare('UPDATE service_state SET service_id=?').run('replacement'));assert.throws(()=>db.exec('DELETE FROM schema_migration'));
 }finally{db.close();}
});
test('未知版本、迁移摘要变化和非Seamer文件均拒绝，不重新初始化或替换身份',t=>{
 const dir=directory(t),identity=initializeStorage(dir),file=path.join(dir,'seamer.sqlite');
 let db=new Database(file);db.exec('PRAGMA application_id=1280525132');db.close();assert.throws(()=>openStorage(dir),e=>e.code==='database_version');
 db=new Database(file);db.exec('PRAGMA application_id=1280529229');db.exec('DROP TRIGGER schema_migration_no_update');db.prepare('UPDATE schema_migration SET script_digest=?').run('changed');db.close();
 assert.throws(()=>openStorage(dir),e=>e.code==='migration_mismatch');db=new Database(file);try{assert.equal(db.prepare('SELECT service_id FROM service_state').pluck().get(),identity.service_id);}finally{db.close();}
});
