import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';
import {authenticationClockStore} from './runtime/auth-clock-store.mjs';
import {canonical} from './runtime/auth-clock-value.mjs';
import {automationPublicationStore} from './automation-publication-store.mjs';
import {automationRunStore} from './automation-run-store.mjs';
import {automationDraftStore} from './automation-draft-store.mjs';
import {seamerPeerPairings} from './peer-pairing.mjs';
import {localAuthorization} from './local-authorization.mjs';
import {peerIdentityWatermarks} from './peer-identity-watermark.mjs';
import {bindingApprovalStore} from './binding-approval-store.mjs';
import {receivedSessionDenials} from './session-denial.mjs';
import {snapshotIngestStore} from './snapshot-ingest-store.mjs';
import {reconcileInbox} from './reconcile-inbox.mjs';
import {saveFirstPairingImport} from './first-pairing-import.mjs';
import {executionNotificationOutbox} from './automation-execution-notification.mjs';

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
const migration5Sql=fs.readFileSync(new URL('./storage-v5.sql',import.meta.url),'utf8');
const migration5Digest='sha256:'+crypto.createHash('sha256').update(migration5Sql).digest('hex');
const migration6Sql=fs.readFileSync(new URL('./storage-v6.sql',import.meta.url),'utf8');
const migration6Digest='sha256:'+crypto.createHash('sha256').update(migration6Sql).digest('hex');
const migration7Sql=fs.readFileSync(new URL('./storage-v7.sql',import.meta.url),'utf8');
const migration7Digest='sha256:'+crypto.createHash('sha256').update(migration7Sql).digest('hex');
const migration8Sql=fs.readFileSync(new URL('./storage-v8.sql',import.meta.url),'utf8');
const migration8Digest='sha256:'+crypto.createHash('sha256').update(migration8Sql).digest('hex');
const migration9Sql=fs.readFileSync(new URL('./storage-v9.sql',import.meta.url),'utf8');
const migration9Digest='sha256:'+crypto.createHash('sha256').update(migration9Sql).digest('hex');
const migration10Sql=fs.readFileSync(new URL('./storage-v10.sql',import.meta.url),'utf8');
const migration10Digest='sha256:'+crypto.createHash('sha256').update(migration10Sql).digest('hex');
const migration11Sql=fs.readFileSync(new URL('./storage-v11.sql',import.meta.url),'utf8');
const migration11Digest='sha256:'+crypto.createHash('sha256').update(migration11Sql).digest('hex');
const migration12Sql=fs.readFileSync(new URL('./storage-v12.sql',import.meta.url),'utf8');
const migration12Digest='sha256:'+crypto.createHash('sha256').update(migration12Sql).digest('hex');
const migration13Sql=fs.readFileSync(new URL('./storage-v13.sql',import.meta.url),'utf8');
const migration13Digest='sha256:'+crypto.createHash('sha256').update(migration13Sql).digest('hex');
const migration14Sql=fs.readFileSync(new URL('./storage-v14.sql',import.meta.url),'utf8');
const migration14Digest='sha256:'+crypto.createHash('sha256').update(migration14Sql).digest('hex');
const migration15Sql=fs.readFileSync(new URL('./storage-v15.sql',import.meta.url),'utf8');
const migration15Digest='sha256:'+crypto.createHash('sha256').update(migration15Sql).digest('hex');
const migration16Sql=fs.readFileSync(new URL('./storage-v16.sql',import.meta.url),'utf8');
const migration16Digest='sha256:'+crypto.createHash('sha256').update(migration16Sql).digest('hex');
const migration17Sql=fs.readFileSync(new URL('./storage-v17.sql',import.meta.url),'utf8');
const migration17Digest='sha256:'+crypto.createHash('sha256').update(migration17Sql).digest('hex');
const migration18Sql=fs.readFileSync(new URL('./storage-v18.sql',import.meta.url),'utf8');
const migration18Digest='sha256:'+crypto.createHash('sha256').update(migration18Sql).digest('hex');
const migration19Sql=fs.readFileSync(new URL('./storage-v19.sql',import.meta.url),'utf8');
const migration19Digest='sha256:'+crypto.createHash('sha256').update(migration19Sql).digest('hex');
const migration20Sql=fs.readFileSync(new URL('./storage-v20.sql',import.meta.url),'utf8');
const migration20Digest='sha256:'+crypto.createHash('sha256').update(migration20Sql).digest('hex');
const migration21Sql=fs.readFileSync(new URL('./storage-v21.sql',import.meta.url),'utf8');
const migration21Digest='sha256:'+crypto.createHash('sha256').update(migration21Sql).digest('hex');
const migration22Sql=fs.readFileSync(new URL('./storage-v22.sql',import.meta.url),'utf8');
const migration22Digest='sha256:'+crypto.createHash('sha256').update(migration22Sql).digest('hex');
const migration23Sql=fs.readFileSync(new URL('./storage-v23.sql',import.meta.url),'utf8');
const migration23Digest='sha256:'+crypto.createHash('sha256').update(migration23Sql).digest('hex');
const migration24Sql=fs.readFileSync(new URL('./storage-v24.sql',import.meta.url),'utf8');
const migration24Digest='sha256:'+crypto.createHash('sha256').update(migration24Sql).digest('hex');
const migration25Sql=fs.readFileSync(new URL('./storage-v25.sql',import.meta.url),'utf8');
const migration25Digest='sha256:'+crypto.createHash('sha256').update(migration25Sql).digest('hex');
const migration26Sql=fs.readFileSync(new URL('./storage-v26.sql',import.meta.url),'utf8');
const migration26Digest='sha256:'+crypto.createHash('sha256').update(migration26Sql).digest('hex');
const migration27Sql=fs.readFileSync(new URL('./storage-v27.sql',import.meta.url),'utf8');
const migration27Digest='sha256:'+crypto.createHash('sha256').update(migration27Sql).digest('hex');
const migration28Sql=fs.readFileSync(new URL('./storage-v28.sql',import.meta.url),'utf8');
const migration28Digest='sha256:'+crypto.createHash('sha256').update(migration28Sql).digest('hex');
const migration29Sql=fs.readFileSync(new URL('./storage-v29.sql',import.meta.url),'utf8');
const migration29Digest='sha256:'+crypto.createHash('sha256').update(migration29Sql).digest('hex');
const migration30Sql=fs.readFileSync(new URL('./storage-v30.sql',import.meta.url),'utf8');
const migration30Digest='sha256:'+crypto.createHash('sha256').update(migration30Sql).digest('hex');
const migration31Sql=fs.readFileSync(new URL('./storage-v31.sql',import.meta.url),'utf8');
const migration31Digest='sha256:'+crypto.createHash('sha256').update(migration31Sql).digest('hex');
const migration32Sql=fs.readFileSync(new URL('./storage-v32.sql',import.meta.url),'utf8');
const migration32Digest='sha256:'+crypto.createHash('sha256').update(migration32Sql).digest('hex');
const migration33Sql=fs.readFileSync(new URL('./storage-v33.sql',import.meta.url),'utf8');
const migration33Digest='sha256:'+crypto.createHash('sha256').update(migration33Sql).digest('hex');
const migration34Sql=fs.readFileSync(new URL('./storage-v34.sql',import.meta.url),'utf8');
const migration34Digest='sha256:'+crypto.createHash('sha256').update(migration34Sql).digest('hex');
const migration35Sql=fs.readFileSync(new URL('./storage-v35.sql',import.meta.url),'utf8');
const migration35Digest='sha256:'+crypto.createHash('sha256').update(migration35Sql).digest('hex');
const migrationScripts=[migrationSql,migration3Sql,migration4Sql,migration5Sql,migration6Sql,migration7Sql,migration8Sql,migration9Sql,migration10Sql,migration11Sql,migration12Sql,migration13Sql,migration14Sql,migration15Sql,migration16Sql,migration17Sql,migration18Sql,migration19Sql,migration20Sql,migration21Sql,migration22Sql,migration23Sql,migration24Sql,migration25Sql,migration26Sql,migration27Sql,migration28Sql,migration29Sql,migration30Sql,migration31Sql,migration32Sql,migration33Sql,migration34Sql,migration35Sql];
const migrationDigests=[digest,migrationDigest,migration3Digest,migration4Digest,migration5Digest,migration6Digest,migration7Digest,migration8Digest,migration9Digest,migration10Digest,migration11Digest,migration12Digest,migration13Digest,migration14Digest,migration15Digest,migration16Digest,migration17Digest,migration18Digest,migration19Digest,migration20Digest,migration21Digest,migration22Digest,migration23Digest,migration24Digest,migration25Digest,migration26Digest,migration27Digest,migration28Digest,migration29Digest,migration30Digest,migration31Digest,migration32Digest,migration33Digest,migration34Digest,migration35Digest];
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
 need(value.singleton===1&&value.authority==='seamer'&&value.schema_revision===String(Number(value.schema_revision))&&
  Number.isInteger(Number(value.schema_revision))&&Number(value.schema_revision)>=1&&Number(value.schema_revision)<=migrationDigests.length&&
  value.maintenance_state==='protected'&&['service_id','database_instance_id','request_epoch'].every(key=>uuid.test(value[key])),'service_identity');
 return Object.freeze({...value});
}
function validate(db){
 const version=pragma(db,'user_version');need(pragma(db,'application_id')===applicationId&&Number.isInteger(version)&&version>=1&&version<=migrationDigests.length,'database_version');
 need(pragma(db,'journal_mode')==='wal'&&pragma(db,'quick_check')==='ok'&&db.prepare('PRAGMA foreign_key_check').raw().all().length===0,'database_integrity');
 const migrations=rows(db,'SELECT * FROM schema_migration ORDER BY CAST(to_revision AS INTEGER)');
 need(migrations.length===version&&migrations.every((row,index)=>row.from_revision===String(index)&&row.to_revision===String(index+1)&&row.script_digest===migrationDigests[index]&&row.state==='committed'),'migration_mismatch');
 const value=identity(db);need(value.schema_revision===String(version),'database_version');
 if(version>=5){
  const epoch=db.prepare('SELECT ingest_epoch FROM event_ingest_epoch WHERE singleton=1 AND service_id=? AND database_instance_id=?')
   .pluck().get(value.service_id,value.database_instance_id);
  need(uuid.test(epoch),'ingest_epoch');
 }
 return value;
}
export function migrateStorage(directory,targetRevision){
 const version=Number(targetRevision);
 need(targetRevision===String(version)&&Number.isInteger(version)&&version>=2&&version<=migrationDigests.length,'migration_target');let db;
 try{
  db=open(filename(directory));const original=validate(db);
  if(original.schema_revision===targetRevision)return original;
  need(Number(original.schema_revision)<Number(targetRevision),'migration_source');
  pragma(db,'foreign_keys=OFF');need(pragma(db,'foreign_keys')===0,'sqlite_configuration');
  const result=db.transaction(()=>{
   need(validate(db).schema_revision===original.schema_revision,'migration_source');
   for(let version=Number(original.schema_revision)+1;version<=Number(targetRevision);version++){
    db.exec(migrationScripts[version-2]);
    if(version===5){
     const seeded=db.prepare('INSERT INTO event_ingest_epoch VALUES(1,?,?,?)')
      .run(original.service_id,original.database_instance_id,crypto.randomUUID());
     need(seeded.changes===1,'ingest_epoch');
    }
    db.prepare("INSERT INTO schema_migration VALUES(?,?,?,?,'committed')").run(crypto.randomUUID(),String(version-1),String(version),migrationDigests[version-1]);
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
export function importFirstPairingAuthorization(directory,input){
 let db;
 try{
  db=open(filename(directory));
  return db.transaction(()=>{
   const service=validate(db);
   need(Number(service.schema_revision)>=18,'first_pairing_import_migration_required');
   return saveFirstPairingImport(db,service,input);
  }).immediate();
 }catch(error){
  if(error instanceof StorageError)throw error;
  const code=typeof error.code==='string'&&/^peer_pairing_[a-z_]+$/.test(error.code)?error.code:
   error.code==='SQLITE_BUSY'?'database_in_use':'first_pairing_import_failed';
  throw new StorageError(code);
 }finally{db?.close();}
}
export function openStorage(directory,{draftAuthority=null,publicationSource=null,publicationPolicy=null,armAdmissionPolicy=null,expectedIdentity=null}={}){
 need(publicationPolicy===null||publicationSource===null&&typeof publicationPolicy.assertCurrentInTransaction==='function','publication_source_configuration');
 need(armAdmissionPolicy===null||typeof armAdmissionPolicy.inspectInTransaction==='function','run_arm_policy_configuration');
 let db;try{
  db=open(filename(directory));const value=validate(db),bootId=crypto.randomUUID();let closed=false,pairings=null,bindingApprovals=null,snapshotIngest=null,sourceTransportReader=null,serviceApprovalReader=null,runEndorsementReader=null,readRevokedEndorsement=null,sendProtection=null,preparationTransport=null,commandTransport=null,boundAuthority=draftAuthority;
  if(expectedIdentity!==null)need(value.service_id===expectedIdentity.service_id&&value.database_instance_id===expectedIdentity.database_instance_id,'protected_ingress_identity');
  const authority={authorizeInTransaction(input){need(boundAuthority,'draft_authority_unavailable');return boundAuthority.authorizeInTransaction(input);},
   commitRequestInTransaction(input){boundAuthority?.commitRequestInTransaction?.(input);},
   assertRunCapacityInTransaction(proof){need(boundAuthority?.assertRunCapacityInTransaction,'run_authority_unavailable');boundAuthority.assertRunCapacityInTransaction(proof);},
   runApprovalTimeInTransaction(){need(boundAuthority?.runApprovalTimeInTransaction,'run_authority_unavailable');return boundAuthority.runApprovalTimeInTransaction();},
   assertArmInitiatorInTransaction(...args){need(boundAuthority?.assertArmInitiatorInTransaction,'run_authority_unavailable');return boundAuthority.assertArmInitiatorInTransaction(...args);},
   identifyRunApprovalActorInTransaction(input){need(boundAuthority?.identifyRunApprovalActorInTransaction,'run_authority_unavailable');return boundAuthority.identifyRunApprovalActorInTransaction(input);},
   lookupInTransaction(input){need(boundAuthority?.lookupInTransaction,'draft_authority_unavailable');return boundAuthority.lookupInTransaction(input);}};
  db.transaction(()=>db.prepare("INSERT INTO service_boot VALUES(?,?,?,'started')").run(value.service_id,value.database_instance_id,bootId)).immediate();
  const approvedBindings=publicationPolicy===null?null:draft=>{
   need(bindingApprovals?.preparePublication,'publication_source_unavailable');return bindingApprovals.preparePublication(draft,publicationPolicy);
  };
  const publications=automationPublicationStore({db,service:value,isOpen:()=>!closed,authority,source:publicationSource,approvedBindings});
  function saveCoreRuntime({intent,descriptor,receipt},policy,code){
   need(db.prepare('INSERT INTO core_runtime_binding VALUES(?,?,?,?,?,?,?,?)').run(bootId,receipt.receipt_id,receipt.bootstrap_id,receipt.member_boot_id,
    ...[intent,descriptor,receipt,policy].map(canonical)).changes===1,code);
  }
  const storage=Object.freeze({identity:value,serviceBootId:bootId,authenticationClock:authenticationClockStore(db,{service_id:value.service_id,service_boot_id:bootId}),
   adoptFirstPairing(input,bindingId,assertCurrent){
    need(!closed&&!db.inTransaction&&Number(value.schema_revision)>=19,'first_pairing_import_migration_required');
    return db.transaction(()=>{
     assertCurrent();
     const result=saveFirstPairingImport(db,value,input);
     need(result.binding_id===bindingId,'first_pairing_import_binding');
     assertCurrent();
     return result;
    }).immediate();
   },
   adoptLinuxCoreRuntime({binding,policy,assertCurrent}){
    need(!closed&&!db.inTransaction&&Number(value.schema_revision)>=20,'bootstrap_linux_schema');
    need(binding.receipt.containment_ref.backend==='linux_cgroup_v2'&&binding.receipt.gate_state==='closed'&&
     binding.intent.service_id===value.service_id&&policy.service_id===value.service_id&&policy.database_instance_id===value.database_instance_id,
    'bootstrap_linux_scope');
    db.transaction(()=>{
     assertCurrent();
     saveCoreRuntime(binding,policy,'bootstrap_linux_storage');
     assertCurrent();
    }).immediate();
   },
   adoptReferenceRuntime({core,policy,binding,assertCurrent}){
    need(!closed&&!db.inTransaction&&Number(value.schema_revision)>=20,'reference_supervisor_schema');
    const {text,digest:runtimeDigest,runtimeId,memberBoot}=binding;
    const runtime=JSON.parse(text),{launch_intent:launch,admission_receipt:provider}=runtime;
    if(runtime.format==='leafseamer-time-provider-runtime-binding/2')need(Number(value.schema_revision)>=34,'reference_supervisor_schema');
    need(canonical(runtime)===text&&runtimeDigest==='sha256:'+crypto.createHash('sha256').update(text).digest('hex')&&provider.member_boot_id===memberBoot,
     'reference_supervisor_integrity');
    db.transaction(()=>{
     assertCurrent();
     saveCoreRuntime(core,policy,'reference_supervisor_storage');
     need(db.prepare('INSERT INTO time_provider_runtime_binding VALUES(?,?,?,?,?,?,?,?)').run(bootId,runtimeId,launch.launch_id,provider.receipt_id,
      provider.bootstrap_id,memberBoot,text,runtimeDigest).changes===1,'reference_supervisor_storage');
     assertCurrent();
    }).immediate();
   },
   async openReferenceSampling({binding,supervision,collector,authentication,authority,archiveOnly=false}){
    need(!closed&&Number(value.schema_revision)>=21,'reference_sampling_schema');
    const {openReferenceSampling}=await import('./reference-sampling.mjs');
    need(!closed&&db.prepare('SELECT body_jcs FROM time_provider_runtime_binding WHERE service_boot_id=? AND runtime_id=? AND body_digest=?')
     .pluck().get(bootId,binding.runtimeId,binding.digest)===binding.text,'reference_supervisor_integrity');
    return openReferenceSampling({db,identity:value,bootId,binding,supervision,collector,authentication,authority,archiveOnly});
   },
   identityWatermarks:peerIdentityWatermarks({db,service:value,bootId,isOpen:()=>!closed}),
   bindSessionDenials({issuer}){need(!closed,'closed');return receivedSessionDenials({db,service:value,bootId,isOpen:()=>!closed,pairings,issuer});},
   drafts:automationDraftStore({db,service:value,isOpen:()=>!closed,authority}),
   publications,
   runs:Number(value.schema_revision)>=5?automationRunStore({db,service:value,bootId,isOpen:()=>!closed,authority,publications,armAdmissionPolicy,
    eventAgePolicy:()=>publicationPolicy?.eventAgePolicy?.()??null,
    publicationBindingsCurrentInTransaction(publication){return bindingApprovals?.publicationBindingsCurrentInTransaction(publication,publicationPolicy)===true;},
    publicationBindingRevocationInTransaction:Number(value.schema_revision)>=12?publication=>{
     need(bindingApprovals,'binding_approval_dependencies');return bindingApprovals.publicationBindingRevocationInTransaction(publication);
    }:null,
    resolveAutomationSourcesInTransaction(inputs){need(snapshotIngest,'run_source_dependencies');return snapshotIngest.resolveAutomationSourcesInTransaction(inputs);},
    prepareSourceTransport(applyId){need(!closed&&sourceTransportReader,'run_source_dependencies');return sourceTransportReader(applyId);},
    resolveDeviceActionsInTransaction(definitionId,actions){need(bindingApprovals,'binding_approval_dependencies');return bindingApprovals.resolveRunActionsInTransaction(definitionId,actions);},
    prepareRunActionObservations(evidence){need(bindingApprovals,'binding_approval_dependencies');return bindingApprovals.prepareRunActionObservations(evidence);},
    prepareServiceApproval(...args){need(!closed&&serviceApprovalReader,'run_service_approval_unavailable');return serviceApprovalReader.prepare(...args);},
    prepareEndorsement(...args){need(!closed&&runEndorsementReader,'run_endorsement_unavailable');return runEndorsementReader.prepare(...args);},
    readRevokedEndorsement(...args){need(!closed&&readRevokedEndorsement,'run_endorsement_unavailable');return readRevokedEndorsement(...args);},
    authorizeEndorsementSender:Number(value.schema_revision)>=10?(bindingId,socket)=>{
     need(!closed&&pairings,'run_endorsement_unavailable');return pairings.authorizeRunProtectionInTransaction(bindingId,socket,'controller');
    }:null,
    sendApprovalProtection(...args){need(!closed&&sendProtection,'run_approval_protection_unavailable');return sendProtection(...args);},
    sendArmPreparation(...args){need(!closed&&preparationTransport,'run_preparation_transport_unavailable');return preparationTransport.submit(...args);},
    lookupArmPreparation(...args){need(!closed&&preparationTransport,'run_preparation_transport_unavailable');return preparationTransport.lookup(...args);},
    observeArmPreparation(...args){need(!closed&&preparationTransport,'run_preparation_transport_unavailable');return preparationTransport.eligibility(...args);},
    submitAutomationCommand(...args){need(!closed&&commandTransport,'execution_submission_transport_unavailable');return commandTransport.submit(...args);},
    lookupAutomationCommand(...args){need(!closed&&commandTransport,'execution_submission_transport_unavailable');return commandTransport.lookup(...args);},
    readAutomationCommandResult(...args){need(!closed&&commandTransport?.result,'execution_command_terminal_transport_unavailable');return commandTransport.result(...args);},
    currentPairingInTransaction(reference){need(!closed&&pairings,'run_approval_pairing');return pairings.requireActivatedInTransaction(reference);},
    authorizeControllerPeerInTransaction(bindingId,socket){need(!closed&&pairings,'run_peer_unavailable');return pairings.authorizeRunRecordInTransaction(bindingId,socket);}
   }):null,
   bindRunServiceApprovals(reader){need(!closed&&Number(value.schema_revision)>=6&&pairings&&boundAuthority&&!serviceApprovalReader,'run_service_approval_dependencies');serviceApprovalReader=reader;},
   bindRunEndorsements(reader){need(!closed&&Number(value.schema_revision)>=7&&pairings&&boundAuthority&&!runEndorsementReader,'run_endorsement_dependencies');runEndorsementReader=reader;},
   bindRevokedEndorsementReader(reader){need(!closed&&Number(value.schema_revision)>=10&&pairings&&boundAuthority&&!readRevokedEndorsement&&typeof reader==='function','run_endorsement_dependencies');readRevokedEndorsement=reader;},
   bindRunProtectionTransport(send){need(!closed&&Number(value.schema_revision)>=9&&pairings&&boundAuthority&&!sendProtection&&typeof send==='function','run_approval_protection_dependencies');sendProtection=send;},
   bindArmPreparationTransport({submit,lookup,eligibility}){
    need(!closed&&Number(value.schema_revision)>=15&&pairings&&boundAuthority&&!preparationTransport&&
     [submit,lookup,eligibility].every(method=>typeof method==='function'),'run_preparation_transport_unavailable');
    preparationTransport=Object.freeze({submit,lookup,eligibility});
   },
   bindAutomationCommandTransport({submit,lookup,result}){
    need(!closed&&Number(value.schema_revision)>=29&&pairings&&boundAuthority&&!commandTransport&&
     [submit,lookup].every(method=>typeof method==='function')&&
     (Number(value.schema_revision)<30||typeof result==='function'),'execution_submission_transport_unavailable');
    commandTransport=Object.freeze({submit,lookup,result});
    storage.runs.startSubmissionRecovery();
   },
   bindBindingApprovals({receiver,eventReceiver=null}){need(!closed&&boundAuthority&&pairings,'binding_approval_dependencies');bindingApprovals=bindingApprovalStore({db,service:value,isOpen:()=>!closed,authority:boundAuthority,receiver,eventReceiver,pairings});return bindingApprovals;},
   bindSnapshotIngest({receiver}){need(!closed&&pairings&&Number(value.schema_revision)>=4,'snapshot_ingest_dependencies');snapshotIngest=snapshotIngestStore({db,service:value,bootId,isOpen:()=>!closed,receiver});return snapshotIngest;},
   bindReconcileInbox({clock,referenceProvider=null}){need(!closed&&pairings&&Number(value.schema_revision)>=5,'reconcile_inbox_dependencies');return reconcileInbox({db,service:value,bootId,isOpen:()=>!closed,clock,referenceProvider,triggerDecisions:storage.runs.triggerDecisions});},
   bindSourceTransport(reader){need(!closed&&snapshotIngest&&Number(value.schema_revision)>=5&&!sourceTransportReader&&typeof reader==='function','run_source_dependencies');sourceTransportReader=reader;},
   bindExecutionNotifications(consumerId){
    need(!closed&&Number(value.schema_revision)>=33,'execution_notification_schema');
    return executionNotificationOutbox({db,consumerId,isOpen:()=>!closed,
     scope:{authority_service_id:value.service_id,database_instance_id:value.database_instance_id}});
   },
   bindLocalAuthorization({clock,receiver,issuer}){need(!closed&&!boundAuthority,'local_authorization_bound');boundAuthority=localAuthorization({db,service:value,bootId,isOpen:()=>!closed,clock,receiver,issuer});return boundAuthority;},
   bindPeerPairings({clock,issuer,management=null}={}){need(!closed&&!pairings,'pairing_service_bound');pairings=seamerPeerPairings({db,service:value,bootId,isOpen:()=>!closed,clock,issuer,management});return pairings;},
   close(){if(closed)return;closed=true;storage.runs?.stopExecutionTimers?.();try{db.transaction(()=>need(db.prepare("UPDATE service_boot SET state='stopped' WHERE service_boot_id=? AND state='started'").run(bootId).changes===1,'boot_stop')).immediate();}finally{db.close();}}
  });
  return storage;
 }catch(error){if(db?.open)db.close();if(error instanceof StorageError)throw error;throw new StorageError(error.code==='SQLITE_BUSY'?'database_in_use':'database_unavailable');}
}
