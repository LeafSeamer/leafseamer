import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';
import {authenticationClockStore} from './runtime/auth-clock-store.mjs';
import {automationPublicationStore} from './automation-publication-store.mjs';
import {automationDraftStore} from './automation-draft-store.mjs';
import {seamerPeerPairings} from './peer-pairing.mjs';
import {localAuthorization} from './local-authorization.mjs';
import {peerIdentityWatermarks} from './peer-identity-watermark.mjs';
import {bindingApprovalStore} from './binding-approval-store.mjs';
import {receivedSessionDenials} from './session-denial.mjs';
import {snapshotIngestStore} from './snapshot-ingest-store.mjs';

export class StorageError extends Error {constructor(code){super(code);this.name='StorageError';this.code=code;}}
const need=(value,code)=>{if(!value)throw new StorageError(code);};
const sql=['storage.sql','auth-clock.sql','automation-draft.sql','peer-pairing.sql','local-authorization.sql','peer-identity-watermark.sql','session-denial.sql'].map(name=>fs.readFileSync(new URL('./'+name,import.meta.url),'utf8')).join('\n');
const digest='sha256:'+crypto.createHash('sha256').update(sql).digest('hex');
const migrationSql=fs.readFileSync(new URL('./storage-v2.sql',import.meta.url),'utf8');
const migrationDigest='sha256:'+crypto.createHash('sha256').update(migrationSql).digest('hex');
const migration3Sql=fs.readFileSync(new URL('./storage-v3.sql',import.meta.url),'utf8');
const migration3Digest='sha256:'+crypto.createHash('sha256').update(migration3Sql).digest('hex');
const migration4Sql=fs.readFileSync(new URL('./storage-v4.sql',import.meta.url),'utf8');
const migration4Digest='sha256:'+crypto.createHash('sha256').update(migration4Sql).digest('hex');
const applicationId=0x4c53534d;
const engine={version:'3.53.4',source:'2026-07-24 19:02:57 bf7c7f30031888f4e796e429ab3978879485813aaca6f641c7b33e4e09459bcc'};
const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
function pragma(db,text){const statement=db.prepare('PRAGMA '+text);if(statement.reader)return statement.raw().get()?.[0];statement.run();}
function rows(db,text){const statement=db.prepare(text),names=statement.columns().map(c=>c.name);return statement.raw().all().map(row=>Object.fromEntries(names.map((name,i)=>[name,row[i]])));}
function filename(directory){need(typeof directory==='string'&&path.isAbsolute(directory),'absolute_data_directory_required');return path.join(path.resolve(directory),'seamer.sqlite');}
function open(file){
 const db=new Database(file,{fileMustExist:true,timeout:0});
 try{
  const [version,source]=db.prepare('SELECT sqlite_version(),sqlite_source_id()').raw().get();need(version===engine.version&&source===engine.source,'sqlite_engine_mismatch');
  pragma(db,'foreign_keys=ON');pragma(db,'trusted_schema=OFF');pragma(db,'recursive_triggers=ON');pragma(db,'locking_mode=EXCLUSIVE');pragma(db,'wal_autocheckpoint=0');pragma(db,'synchronous=FULL');
  for(const [key,value]of Object.entries({foreign_keys:1,trusted_schema:0,recursive_triggers:1,locking_mode:'exclusive',wal_autocheckpoint:0,synchronous:2}))need(pragma(db,key)===value,'sqlite_configuration');
  return db;
 }catch(error){db.close();throw error;}
}
function identity(db){
 const records=rows(db,'SELECT * FROM service_state');need(records.length===1,'service_identity');const value=records[0];
 need(value.singleton===1&&value.authority==='seamer'&&['1','2','3','4'].includes(value.schema_revision)&&value.maintenance_state==='protected'&&['service_id','database_instance_id','request_epoch'].every(key=>uuid.test(value[key])),'service_identity');
 return Object.freeze({...value});
}
function validate(db){
 const version=pragma(db,'user_version');need(pragma(db,'application_id')===applicationId&&[1,2,3,4].includes(version),'database_version');
 need(pragma(db,'journal_mode')==='wal'&&pragma(db,'quick_check')==='ok'&&db.prepare('PRAGMA foreign_key_check').raw().all().length===0,'database_integrity');
 const migrations=rows(db,'SELECT * FROM schema_migration ORDER BY to_revision');
 need(migrations.length===version&&migrations.every((row,index)=>row.from_revision===String(index)&&row.to_revision===String(index+1)&&row.script_digest===[digest,migrationDigest,migration3Digest,migration4Digest][index]&&row.state==='committed'),'migration_mismatch');
 const value=identity(db);need(value.schema_revision===String(version),'database_version');return value;
}
export function migrateStorage(directory,targetRevision){
 need(['2','3','4'].includes(targetRevision),'migration_target');let db;
 try{
  db=open(filename(directory));const original=validate(db);
  if(original.schema_revision===targetRevision)return original;
  need(Number(original.schema_revision)<Number(targetRevision),'migration_source');
  pragma(db,'foreign_keys=OFF');need(pragma(db,'foreign_keys')===0,'sqlite_configuration');
  const result=db.transaction(()=>{
   need(validate(db).schema_revision===original.schema_revision,'migration_source');
   for(let version=Number(original.schema_revision)+1;version<=Number(targetRevision);version++){
    db.exec([migrationSql,migration3Sql,migration4Sql][version-2]);
    db.prepare("INSERT INTO schema_migration VALUES(?,?,?,?,'committed')").run(crypto.randomUUID(),String(version-1),String(version),[migrationDigest,migration3Digest,migration4Digest][version-2]);
    pragma(db,'user_version='+version);validate(db);
   }
   const value=validate(db);
   need(['service_id','database_instance_id','request_epoch','authority','maintenance_state'].every(key=>value[key]===original[key]),'migration_identity_changed');
   return value;
  }).immediate();
  pragma(db,'foreign_keys=ON');need(pragma(db,'foreign_keys')===1,'sqlite_configuration');return result;
 }catch(error){if(error instanceof StorageError)throw error;throw new StorageError(error.code==='SQLITE_BUSY'?'database_in_use':'migration_failed');}
 finally{db?.close();}
}
export function initializeStorage(directory){
 const file=filename(directory);fs.mkdirSync(path.dirname(file),{recursive:true,mode:0o700});let handle;
 try{handle=fs.openSync(file,'wx',0o600);}catch(error){throw new StorageError(error.code==='EEXIST'?'database_already_exists':'database_create_failed');}fs.closeSync(handle);
 let db;try{
  db=open(file);need(pragma(db,'journal_mode=WAL')==='wal','wal_unavailable');
  db.transaction(()=>{
   db.exec(sql);pragma(db,'application_id='+applicationId);pragma(db,'user_version=1');
   db.prepare("INSERT INTO service_state VALUES(1,'seamer',?,?,?,'1','protected')").run(crypto.randomUUID(),crypto.randomUUID(),crypto.randomUUID());
   db.prepare("INSERT INTO schema_migration VALUES(?,'0','1',?,'committed')").run(crypto.randomUUID(),digest);
  }).immediate();return validate(db);
 }finally{db?.close();}
}
export function openStorage(directory,{draftAuthority=null,publicationSource=null}={}){
 let db;try{
  db=open(filename(directory));const value=validate(db),bootId=crypto.randomUUID();let closed=false,pairings=null,boundAuthority=draftAuthority;
  const authority={authorizeInTransaction(input){need(boundAuthority,'draft_authority_unavailable');return boundAuthority.authorizeInTransaction(input);},
   commitRequestInTransaction(input){boundAuthority?.commitRequestInTransaction?.(input);},
   lookupInTransaction(input){need(boundAuthority?.lookupInTransaction,'draft_authority_unavailable');return boundAuthority.lookupInTransaction(input);}};
  db.transaction(()=>db.prepare("INSERT INTO service_boot VALUES(?,?,?,'started')").run(value.service_id,value.database_instance_id,bootId)).immediate();
  return Object.freeze({identity:value,serviceBootId:bootId,authenticationClock:authenticationClockStore(db,{service_id:value.service_id,service_boot_id:bootId}),
   identityWatermarks:peerIdentityWatermarks({db,service:value,bootId,isOpen:()=>!closed}),
   bindSessionDenials({issuer}){need(!closed,'closed');return receivedSessionDenials({db,service:value,bootId,isOpen:()=>!closed,pairings,issuer});},
   drafts:automationDraftStore({db,service:value,isOpen:()=>!closed,authority}),
   publications:automationPublicationStore({db,service:value,isOpen:()=>!closed,authority,source:publicationSource}),
   bindBindingApprovals({receiver}){need(!closed&&boundAuthority&&pairings,'binding_approval_dependencies');return bindingApprovalStore({db,service:value,isOpen:()=>!closed,authority:boundAuthority,receiver,pairings});},
   bindSnapshotIngest({receiver}){need(!closed&&pairings&&value.schema_revision==='4','snapshot_ingest_dependencies');return snapshotIngestStore({db,service:value,bootId,isOpen:()=>!closed,receiver});},
   bindLocalAuthorization({clock,receiver,issuer}){need(!closed&&!boundAuthority,'local_authorization_bound');boundAuthority=localAuthorization({db,service:value,bootId,isOpen:()=>!closed,clock,receiver,issuer});return boundAuthority;},
   bindPeerPairings({clock,issuer,management=null}={}){need(!closed&&!pairings,'pairing_service_bound');pairings=seamerPeerPairings({db,service:value,bootId,isOpen:()=>!closed,clock,issuer,management});return pairings;},
   close(){if(closed)return;closed=true;try{db.transaction(()=>need(db.prepare("UPDATE service_boot SET state='stopped' WHERE service_boot_id=? AND state='started'").run(bootId).changes===1,'boot_stop')).immediate();}finally{db.close();}}
  });
 }catch(error){if(db?.open)db.close();if(error instanceof StorageError)throw error;throw new StorageError(error.code==='SQLITE_BUSY'?'database_in_use':'database_unavailable');}
}
