import crypto from 'node:crypto';
import canonicalize from 'canonicalize';
import {validateAutomationModel} from './sdk/automation-program.mjs';
import {freeze} from './automation-expression.mjs';

export class DraftError extends Error {constructor(code){super(code);this.name='DraftError';this.code=code;}}
const need=(value,code)=>{if(!value)throw new DraftError(code);};
const hash=value=>'sha256:'+crypto.createHash('sha256').update(canonicalize(value)).digest('hex');
const equal=(a,b)=>canonicalize(a)===canonicalize(b);
const rows=(db,sql,...args)=>{const statement=db.prepare(sql),names=statement.columns().map(column=>column.name);return statement.raw().all(...args).map(row=>Object.fromEntries(names.map((name,i)=>[name,row[i]])));};
const decode=(body,digest)=>{const value=JSON.parse(body);need(canonicalize(value)===body&&hash(value)===digest,'draft_integrity');return value;};
const next=revision=>{const value=BigInt(revision)+1n;need(value<10n**20n,'draft_revision_exhausted');return String(value);};
const checked=(name,value)=>{validateAutomationModel(name,value);return freeze(structuredClone(value));};
const keys=(value,expected)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).sort().join(',')===[...expected].sort().join(',');

const draftHeads=`SELECT i.definition_id,i.service_id,i.database_instance_id,c.revision,c.content_jcs,c.content_digest,c.selection_digest,
 m.revision AS metadata_revision,m.metadata_jcs,m.metadata_digest
 FROM definition_identity i LEFT JOIN definition_head h USING(definition_id)
 LEFT JOIN definition_content c ON c.definition_id=h.definition_id AND c.revision=h.content_revision
 LEFT JOIN definition_metadata m ON m.definition_id=h.definition_id AND m.revision=h.metadata_revision`;
function draftRecord(row,scope){
 need(row?.revision&&row.metadata_revision,'definition_missing');
 const content=decode(row.content_jcs,row.content_digest),metadata=decode(row.metadata_jcs,row.metadata_digest);
 need(hash(content.selections)===row.selection_digest,'draft_integrity');
 return checked('LifecycleDraftRecord',{scope,definition_id:row.definition_id,revision:row.revision,content,content_digest:row.content_digest,
  selection_digest:row.selection_digest,metadata_revision:row.metadata_revision,metadata});
}

// Maintenance creates new draft heads in an unopened generation. It carries provenance, never a previous request identity.
export function restoreDraftHeads(source,target,sourceScope,next,origin){
 need(source.readonly&&target.inTransaction,'draft_recovery_writer');
 const old={authority_service_id:sourceScope.service_id,database_instance_id:sourceScope.database_instance_id};
 const scope={authority_service_id:next.service_id,database_instance_id:next.database_instance_id};
 const actor={issuer_id:next.service_id,issuer_epoch:next.request_epoch,principal_id:'local-recovery',principal_kind:'service'};
 const writeIdentity=target.prepare('INSERT INTO definition_identity VALUES(?,?,?,?)');
 const writeContent=target.prepare("INSERT INTO definition_content VALUES(?,'1',NULL,?,?,?,?)");
 const writeMetadata=target.prepare("INSERT INTO definition_metadata VALUES(?,'1',NULL,?,?,?)");
 const writeHead=target.prepare("INSERT INTO definition_head VALUES(?,'1','1')");
 const writeRequest=target.prepare("INSERT INTO definition_request VALUES(?,?,?,?,'createDefinition',?,?,?)");
 const writeNotification=target.prepare('INSERT INTO definition_notification VALUES(?,?,?)');
 const originals=source.prepare(draftHeads+' ORDER BY i.definition_id');
 const columns=originals.columns().map(column=>column.name);
 const origins=crypto.createHash('sha256');let count=0;
 for(const row of originals.raw().iterate()){
  const entry=Object.fromEntries(columns.map((column,index)=>[column,row[index]]));
  need(++count<=4096&&entry.service_id===old.authority_service_id&&entry.database_instance_id===old.database_instance_id,'draft_recovery_source');
  const id=entry.definition_id,draft=draftRecord(entry,old),transaction=crypto.randomUUID();
  const recovered=checked('LifecycleDraftRecord',{...draft,scope,revision:'1',metadata_revision:'1'});
  const original={definition_id:id,draft_revision:draft.revision,metadata_revision:draft.metadata_revision,
   content_digest:draft.content_digest,metadata_digest:hash(draft.metadata)};
  const provenance={...origin,source_scope:old,...original};
  const body=canonicalize(recovered);need(Buffer.byteLength(body)<=16777216,'draft_capacity');
  writeIdentity.run(id,next.service_id,next.database_instance_id,transaction);
  writeContent.run(id,canonicalize(recovered.content),recovered.content_digest,recovered.selection_digest,transaction);
  writeMetadata.run(id,canonicalize(recovered.metadata),original.metadata_digest,transaction);
  writeHead.run(id);
  writeRequest.run(transaction,hash({scope,recovery:provenance}),hash(provenance),canonicalize(actor),id,body,hash(recovered));
  const notification={kind:'definitionRestored',transaction_id:transaction,scope,definition_id:id,revision:'1',
   content_digest:recovered.content_digest,metadata_revision:'1',actor,provenance};
  writeNotification.run(transaction,canonicalize(notification),hash(notification));
  origins.update(canonicalize(original)+'\n');
 }
 return {draft_count:count,draft_origin_digest:'sha256:'+origins.digest('hex')};
}

// authority仅由可信进程装配提供，负责当前人员/服务身份、context、对象权限和认证时钟检查。
// 缺提供者时读写均拒绝；不能用请求正文中的声明代替该权威。
export function automationDraftStore({db,service,isOpen,authority=null}){
 const scope=freeze({authority_service_id:service.service_id,database_instance_id:service.database_instance_id});
 const available=()=>need(isOpen()&&db.open&&!db.inTransaction,'draft_store_closed');
 function authorize(authentication,operation_id,definition_id,context_id){
  need(db.inTransaction&&authority&&typeof authority.authorizeInTransaction==='function','draft_authority_unavailable');
  const value=authority.authorizeInTransaction({authentication,operation_id,definition_id,context_id,scope});
  need(keys(value,['scope','actor','context_id','request_epoch','operation_id','definition_id','authorization_digest']),'draft_authorization');
  checked('LifecycleScope',value.scope);if(!(operation_id==='getDraft'&&context_id===null))checked('LifecycleId',value.context_id);checked('LifecycleId',value.request_epoch);checked('LifecycleDigest',value.authorization_digest);
  need(keys(value.actor,['issuer_id','issuer_epoch','principal_id','principal_kind'])&&['human','service'].includes(value.actor.principal_kind),'draft_actor');
  for(const key of ['issuer_id','issuer_epoch','principal_id'])checked('LifecycleId',value.actor[key]);
  need(equal(value.scope,scope)&&value.request_epoch===service.request_epoch&&value.context_id===context_id&&value.operation_id===operation_id&&value.definition_id===definition_id,'draft_authorization_binding');
  return freeze(structuredClone(value));
 }
 const record=id=>draftRecord(rows(db,draftHeads+' WHERE i.definition_id=?',id)[0],scope);
 function content(value){
  const result=checked('LifecycleDraftContent',value),seen=new Set();
  for(const selection of result.selections){const key=selection.kind+':'+selection.binding_id;need(!seen.has(key),'draft_duplicate_selection');seen.add(key);}
  return result;
 }
 function writeContent(id,revision,previous,value,transaction){
  db.prepare('INSERT INTO definition_content VALUES(?,?,?,?,?,?,?)').run(id,revision,previous,canonicalize(value),hash(value),hash(value.selections),transaction);
 }
 function writeMetadata(id,revision,previous,value,transaction){db.prepare('INSERT INTO definition_metadata VALUES(?,?,?,?,?,?)').run(id,revision,previous,canonicalize(value),hash(value),transaction);}
 function mutate(authentication,operation,id,request,model){
  available();const body=checked(model,request);if(id!==null)checked('LifecycleId',id);
  const semantic=Object.fromEntries(Object.entries(body).filter(([key])=>key!=='context'));
  if(body.content)content(body.content);
  const requestDigest=hash({version:'automation-lifecycle/0.1.0-draft.1',operation_id:operation,definition_id:id,request:semantic});
  return db.transaction(()=>{
   const proof=authorize(authentication,operation,id,body.context.context_id);
   const identityDigest=hash({scope,request_epoch:proof.request_epoch,actor:proof.actor,operation_id:operation,idempotency_key:body.context.idempotency_key});
   const prior=rows(db,'SELECT * FROM definition_request WHERE identity_digest=?',identityDigest)[0];
   if(prior){
    need(prior.request_digest===requestDigest,'draft_idempotency_conflict');
    const result=checked('LifecycleDraftRecord',decode(prior.result_jcs,prior.result_digest));
    need(result.definition_id===prior.definition_id&&equal(result.scope,scope),'draft_integrity');
    // 创建重试也须检查原定义对象当前可读，不能只保留通用创建权限。
    const readProof=authorize(authentication,'getDraft',result.definition_id,body.context.context_id);
    need(equal(proof,authorize(authentication,operation,id,body.context.context_id))&&equal(readProof,authorize(authentication,'getDraft',result.definition_id,body.context.context_id)),'draft_authorization_changed');return result;
   }
   need(db.prepare('SELECT count(*) FROM definition_request').pluck().get()<16384,'draft_capacity');
   const transaction=crypto.randomUUID();authority.commitRequestInTransaction?.({proof,transaction_id:transaction});let definition=id;
   if(operation==='createDefinition'){
    need(db.prepare('SELECT count(*) FROM definition_identity').pluck().get()<4096,'draft_capacity');definition=crypto.randomUUID();
    db.prepare('INSERT INTO definition_identity VALUES(?,?,?,?)').run(definition,service.service_id,service.database_instance_id,transaction);
    writeContent(definition,'1',null,body.content,transaction);writeMetadata(definition,'1',null,body.metadata,transaction);
    db.prepare("INSERT INTO definition_head VALUES(?,'1','1')").run(definition);
   }else{
    const current=record(definition);
    if(operation==='updateDraft'){
     need(current.revision===body.expected_draft_revision,'draft_revision_conflict');const revision=next(current.revision);
     writeContent(definition,revision,current.revision,body.content,transaction);
     need(db.prepare('UPDATE definition_head SET content_revision=? WHERE definition_id=? AND content_revision=?').run(revision,definition,current.revision).changes===1,'draft_revision_conflict');
    }else{
     need(current.metadata_revision===body.expected_metadata_revision,'metadata_revision_conflict');const revision=next(current.metadata_revision);
     writeMetadata(definition,revision,current.metadata_revision,body.metadata,transaction);
     need(db.prepare('UPDATE definition_head SET metadata_revision=? WHERE definition_id=? AND metadata_revision=?').run(revision,definition,current.metadata_revision).changes===1,'metadata_revision_conflict');
    }
   }
   const result=record(definition),resultText=canonicalize(result);need(Buffer.byteLength(resultText)<=16777216,'draft_capacity');
   db.prepare('INSERT INTO definition_request VALUES(?,?,?,?,?,?,?,?)').run(transaction,identityDigest,requestDigest,canonicalize(proof.actor),operation,definition,resultText,hash(result));
   const notification={kind:operation,transaction_id:transaction,scope,definition_id:definition,revision:result.revision,content_digest:result.content_digest,metadata_revision:result.metadata_revision,actor:proof.actor};
   db.prepare('INSERT INTO definition_notification VALUES(?,?,?)').run(transaction,canonicalize(notification),hash(notification));
   need(equal(proof,authorize(authentication,operation,id,body.context.context_id)),'draft_authorization_changed');return result;
  }).immediate();
 }
 return Object.freeze({
  lookup(authentication,request){
   available();need(keys(request,['context_id','idempotency_key','operation_id']),'draft_lookup');
   for(const value of Object.values(request))checked('LifecycleId',value);
   need(['createDefinition','updateDraft','updateDefinitionMetadata','publishDefinition'].includes(request.operation_id),'draft_lookup');
   return db.transaction(()=>{
    need(typeof authority?.lookupInTransaction==='function','draft_authority_unavailable');
    const input={authentication,context_id:request.context_id,operation_id:request.operation_id},proof=authority.lookupInTransaction(input);
    need(equal(proof.scope,scope)&&proof.request_epoch===service.request_epoch,'draft_authorization_binding');
    const identityDigest=hash({scope,request_epoch:proof.request_epoch,actor:proof.actor,operation_id:request.operation_id,idempotency_key:request.idempotency_key});
    const prior=rows(db,'SELECT * FROM definition_request WHERE identity_digest=?',identityDigest)[0];need(prior,'draft_request_missing');
    need(prior.operation_id===request.operation_id&&equal(JSON.parse(prior.actor_jcs),proof.actor),'draft_integrity');
    const published=request.operation_id==='publishDefinition',value=checked(published?'LifecyclePublishedVersion':'LifecycleDraftRecord',decode(prior.result_jcs,prior.result_digest));
    need(value.definition_id===prior.definition_id&&equal(value.scope,scope),'draft_integrity');
    const readProof=authorize(authentication,'getDraft',value.definition_id,null);
    need(equal(proof,authority.lookupInTransaction(input))&&equal(readProof,authorize(authentication,'getDraft',value.definition_id,null)),'draft_authorization_changed');
    return freeze({operation_id:request.operation_id,request_digest:prior.request_digest,result:{kind:published?'PublishedVersion':'DraftRecord',value}});
   }).immediate();
  },
  create(authentication,request){return mutate(authentication,'createDefinition',null,request,'LifecycleCreateDefinition');},
  update(authentication,id,request){return mutate(authentication,'updateDraft',id,request,'LifecycleUpdateDraft');},
  updateMetadata(authentication,id,request){return mutate(authentication,'updateDefinitionMetadata',id,request,'LifecycleUpdateMetadata');},
  get(authentication,id,contextId=null){available();checked('LifecycleId',id);if(contextId!==null)checked('LifecycleId',contextId);return db.transaction(()=>{const proof=authorize(authentication,'getDraft',id,contextId),result=record(id);need(equal(proof,authorize(authentication,'getDraft',id,contextId)),'draft_authorization_changed');return result;}).immediate();}
 });
}
