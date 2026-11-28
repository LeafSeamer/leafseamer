import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import Database from 'better-sqlite3';
import {initializeStorage,migrateStorage,openStorage,verifyBackupSnapshot} from '../dist/storage.mjs';

test('validates the exact standalone Schema 48 backup and its original scope',async t=>{
 const directory=fs.mkdtempSync(path.join(os.tmpdir(),'leafseamer-seamer-backup-'));
 t.after(()=>{
  assert.equal(path.dirname(path.resolve(directory)),path.resolve(os.tmpdir()));
  fs.rmSync(directory,{recursive:true,force:true,maxRetries:5,retryDelay:50});
 });
 initializeStorage(directory);
 migrateStorage(directory,'48');
 const runtime=openStorage(directory),identity=runtime.identity;
 runtime.close();
 const file=path.join(directory,'snapshot.sqlite');
 const source=new Database(path.join(directory,'seamer.sqlite'),{readonly:true,fileMustExist:true});
 try{await source.backup(file);}finally{source.close();}
 const copied=new Database(file,{fileMustExist:true});
 try{assert.equal(copied.pragma('journal_mode=DELETE',{simple:true}),'delete');}finally{copied.close();}
 const payload=fs.readFileSync(file);
 const scope={authority:'seamer',service_id:identity.service_id,database_instance_id:identity.database_instance_id,
  schema_revision:identity.schema_revision,request_epoch:identity.request_epoch,control_epoch:null};
 const receipt={state:'staged',engine:'3.53.4',backup_operation_id:crypto.randomUUID(),bytes:payload.length,
  sha256:crypto.createHash('sha256').update(payload).digest('hex'),scope};
 const report=verifyBackupSnapshot(file,receipt);
 assert.equal(report.backup_operation_id,receipt.backup_operation_id);
 assert.deepEqual(report.scope,scope);
 assert.deepEqual(report.execution_inventory,{triggered:0,scheduled:0,steps:0,completed:0,active:0,protected:0});
 assert.throws(()=>verifyBackupSnapshot(file,{...receipt,sha256:'0'.repeat(64)}),{code:'backup_snapshot_digest'});
 assert.throws(()=>verifyBackupSnapshot(file,{...receipt,scope:{...scope,request_epoch:crypto.randomUUID()}}),{code:'backup_snapshot_scope'});
 const wal=path.join(directory,'uncheckpointed.sqlite'),walBytes=Buffer.from(payload);
 walBytes[18]=2;walBytes[19]=2;
 fs.writeFileSync(wal,walBytes);
 const walReceipt={...receipt,sha256:crypto.createHash('sha256').update(walBytes).digest('hex')};
 assert.throws(()=>verifyBackupSnapshot(wal,walReceipt),{code:'backup_snapshot_file'});
 assert.ok(['-wal','-shm','-journal'].every(suffix=>!fs.existsSync(wal+suffix)));
 assert.deepEqual(fs.readFileSync(file),payload);
 assert.ok(['-wal','-shm','-journal'].every(suffix=>!fs.existsSync(file+suffix)));
 const verified=new Database(file,{readonly:true,fileMustExist:true});
 try{
  assert.deepEqual(verified.prepare('SELECT service_boot_id,state FROM service_boot').raw().all(),[[runtime.serviceBootId,'stopped']]);
 }finally{verified.close();}
});
