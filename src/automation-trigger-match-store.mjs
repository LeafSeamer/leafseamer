import {canonical,digest,same} from './runtime/auth-clock-value.mjs';

const limit=1048576;
const need=(valid,code)=>{if(!valid)throw Object.assign(new Error(code),{code});};
const original=(text,hash)=>{
 const value=JSON.parse(text);
 need(canonical(value)===text&&digest(value)===hash,'trigger_match_integrity');
 return value;
};

export function automationTriggerMatchStore(db,scope){
 const candidates=db.prepare(`SELECT r.run_id,r.definition_id,r.definition_version,r.body_jcs,r.body_digest,r.dependency_digest,b.binding_id
  FROM automation_trigger_binding b JOIN automation_run_creation r USING(definition_id,definition_version)
  WHERE b.authority_id=? AND b.registration_id=? AND b.registration_revision=? AND b.registration_digest=?
  ORDER BY r.definition_id,r.definition_version,r.run_id,b.binding_id`).raw();
 const dependencies=db.prepare(`SELECT body_jcs,body_digest FROM definition_program_dependencies
  WHERE definition_id=? AND definition_version=?`).raw();
 const count=db.prepare('SELECT count(*) FROM automation_run_creation').pluck();
 const insert=db.prepare('INSERT INTO automation_trigger_match_set VALUES(?,?,?,?,?)');
 const lookup=db.prepare(`SELECT origin_digest,body_jcs,body_digest FROM automation_trigger_match_set
  WHERE ingest_epoch=? AND receive_sequence=?`).raw();
 const versions=db.prepare(`SELECT DISTINCT r.definition_id,r.definition_version,length(CAST(p.bundle_jcs AS BLOB))
  FROM automation_trigger_binding b JOIN automation_run_creation r USING(definition_id,definition_version)
  JOIN automation_arm_completion a USING(run_id) JOIN definition_publication p USING(definition_id,definition_version)
  WHERE b.authority_id=? AND b.registration_id=? AND b.registration_revision=? AND b.registration_digest=?`).raw();
 const runs=db.prepare(`SELECT DISTINCT r.run_id FROM automation_trigger_binding b JOIN automation_run_creation r USING(definition_id,definition_version)
  JOIN automation_arm_completion a USING(run_id) WHERE b.authority_id=? AND b.registration_id=? AND b.registration_revision=? AND b.registration_digest=?`).pluck();
 const bindingKey=origin=>[origin.source_key.authority_service_id,origin.binding_ref.registration_id,origin.binding_ref.revision,digest(origin.binding_ref)];

 function record({origin,origin_digest},receive_cursor){
  need(db.inTransaction&&same(receive_cursor.scope,scope),'trigger_match_transaction');
  const {source_key,event_id,binding_ref}=origin,authority=source_key.authority_service_id;
  const registration={id:binding_ref.registration_id,revision:binding_ref.revision,digest:digest(binding_ref)};
  const targets=[];
  let pinned=null,pinnedHash=null,inputs=null,current=null,bytes=0;
  for(const [runId,definition,version,body,hash,dependencyHash,bindingId] of candidates.iterate(authority,registration.id,registration.revision,registration.digest)){
   if(pinned?.definition_id!==definition||pinned?.definition_version!==version){
    const row=dependencies.get(definition,version);
    need(row,'trigger_match_integrity');
    pinned=original(...row);pinnedHash=row[1];
    need(same(pinned.scope,scope)&&pinned.definition_id===definition&&pinned.definition_version===version,'trigger_match_integrity');
    inputs=new Map(pinned.dependencies.inputs.map(value=>[value.binding_id,value]));
   }
   need(pinnedHash===dependencyHash,'trigger_match_integrity');
   if(current?.run_id!==runId){
    const {run}=original(body,hash);
    need(same(run.scope,scope)&&run.run_id===runId&&run.definition_id===definition&&run.definition_version===version&&
     run.dependency_digest===dependencyHash&&same(run.artifact_ref,pinned.artifact_ref),'trigger_match_integrity');
    current={run_id:runId,definition_id:definition,definition_version:version,artifact_ref:run.artifact_ref,
     dependency_digest:dependencyHash,bindings:[]};
    targets.push(current);
    bytes+=Buffer.byteLength(canonical(current));
   }
   const input=inputs.get(bindingId);
   need(input?.source==='event'&&input.authority_id===authority&&input.uses.includes('trigger')&&same(input.registration_ref,registration),'trigger_match_integrity');
   const binding={binding_id:bindingId,registration_ref:input.registration_ref,schema_ref:input.schema_ref};
   bytes+=Buffer.byteLength(canonical(binding))+1;
   need(bytes<=limit,'trigger_match_capacity');
   current.bindings.push(binding);
  }
  // 运行身份为 UUID，SQLite 的二进制排序和规范 JSON 字节排序一致。
  targets.sort((a,b)=>a.run_id<b.run_id?-1:a.run_id>b.run_id?1:0);
  const match={format:'leafseamer.trigger-match-set/1',receive_cursor,source_key,event_id,origin_digest,
   registry_revision:String(count.get()),targets,targets_digest:digest(targets)};
  const text=canonical(match);
  need(Buffer.byteLength(text)<=limit,'trigger_match_capacity');
  const hash=digest(match);
  need(insert.run(receive_cursor.ingest_epoch,receive_cursor.receive_sequence,origin_digest,text,hash).changes===1,'trigger_match_storage');
  return {match,digest:hash};
 }

 function read({origin,origin_digest},receive_cursor){
  const row=lookup.get(receive_cursor.ingest_epoch,receive_cursor.receive_sequence);
  if(!row)return null;
  const match=original(row[1],row[2]);
  need(match.format==='leafseamer.trigger-match-set/1'&&match.origin_digest===origin_digest&&row[0]===origin_digest&&
   same(match.receive_cursor,receive_cursor)&&same(match.source_key,origin.source_key)&&match.event_id===origin.event_id&&
   digest(match.targets)===match.targets_digest,'trigger_match_integrity');
  return {match,digest:row[2]};
 }
 return Object.freeze({record,read,programVersions:origin=>versions.iterate(...bindingKey(origin)),runs:origin=>runs.iterate(...bindingKey(origin))});
}
