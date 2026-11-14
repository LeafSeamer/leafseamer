import crypto from 'node:crypto';
import canonicalize from 'canonicalize';
import {freeze} from './automation-expression.mjs';
import {need,hash,same,checked,derivePublication,validateStaticParameters} from './automation-publication.mjs';

const decode=(body,digest)=>{const value=JSON.parse(body);need(canonicalize(value)===body&&hash(value)===digest,'publication_integrity');return freeze(value);};
const contentRef=value=>({id:crypto.randomUUID(),revision:'1',digest:hash(value)});
export function automationPublicationStore({db,service,isOpen,authority,source=null}){
 const scope=freeze({authority_service_id:service.service_id,database_instance_id:service.database_instance_id});let pending=0;
 const available=()=>need(isOpen()&&db.open&&!db.inTransaction,'publication_store_closed');
 const writer=work=>{available();need(['2','3','4'].includes(service.schema_revision),'publication_migration_required');return db.transaction(work).immediate();};
 function authorize(authentication,operation_id,definition_id,context_id){
  need(db.inTransaction&&authority?.authorizeInTransaction,'publication_authority_unavailable');
  const proof=authority.authorizeInTransaction({authentication,operation_id,definition_id,context_id,scope});
  need(proof&&same(proof.scope,scope)&&proof.operation_id===operation_id&&proof.definition_id===definition_id&&proof.context_id===context_id&&proof.request_epoch===service.request_epoch,'publication_authorization');
  checked('LifecycleId',proof.request_epoch);checked('LifecycleDigest',proof.authorization_digest);
  need(proof.actor&&['human','service'].includes(proof.actor.principal_kind),'publication_authorization');
  for(const key of ['issuer_id','issuer_epoch','principal_id'])checked('LifecycleId',proof.actor[key]);
  if(operation_id==='publishDefinition')need(proof.actor.principal_kind==='human','publication_human_required');
  return freeze(structuredClone(proof));
 }
 function draft(id){
  const row=db.prepare(`SELECT c.revision,c.content_jcs,c.content_digest,c.selection_digest,m.revision,m.metadata_jcs,m.metadata_digest FROM definition_head h
   JOIN definition_content c ON c.definition_id=h.definition_id AND c.revision=h.content_revision JOIN definition_metadata m ON m.definition_id=h.definition_id AND m.revision=h.metadata_revision WHERE h.definition_id=?`).raw().get(id);
  need(row,'definition_missing');const content=decode(row[1],row[2]);need(hash(content.selections)===row[3],'publication_integrity');
  return checked('LifecycleDraftRecord',{scope,definition_id:id,revision:row[0],content,content_digest:row[2],selection_digest:row[3],metadata_revision:row[4],metadata:decode(row[5],row[6])});
 }
 function cas(current,request){need(current.revision===request.expected_draft_revision&&current.content_digest===request.expected_content_digest&&current.selection_digest===request.expected_selection_digest,'publication_draft_conflict');}
 function prior(identity,requestDigest,authentication,id){
  const row=db.prepare('SELECT operation_id,definition_id,request_digest,result_jcs,result_digest FROM definition_request WHERE identity_digest=?').raw().get(identity);
  if(!row)return null;
  need(row[0]==='publishDefinition'&&row[1]===id&&row[2]===requestDigest,'publication_idempotency_conflict');
  const proof=authorize(authentication,'getDraft',id,null),value=checked('LifecyclePublishedVersion',decode(row[3],row[4]));
  need(value.definition_id===id&&same(value.scope,scope),'publication_integrity');
  need(same(proof,authorize(authentication,'getDraft',id,null)),'publication_authorization_changed');return value;
 }
 function historical(id,version){
  const row=db.prepare(`SELECT p.bundle_jcs,p.bundle_digest,r.source_draft_revision,q.result_jcs,q.result_digest,q.actor_jcs,q.operation_id FROM definition_publication p
   JOIN definition_version_reservation r USING(definition_id,definition_version) JOIN definition_request q ON q.transaction_id=p.transaction_id WHERE p.definition_id=? AND p.definition_version=?`).raw().get(id,version);
  need(row,'publication_missing');const bundle=decode(row[0],row[1]),result=checked('LifecyclePublishedVersion',decode(row[3],row[4]));
  need(row[6]==='publishDefinition'&&bundle.draft.definition_id===id&&bundle.draft.revision===row[2]&&same(bundle.draft.scope,scope)&&same(bundle.result,result),'publication_integrity');
  const original=db.prepare('SELECT content_jcs,content_digest,selection_digest FROM definition_content WHERE definition_id=? AND revision=?').raw().get(id,row[2]);
  need(original&&same(decode(original[0],original[1]),bundle.draft.content)&&original[1]===bundle.draft.content_digest&&original[2]===bundle.draft.selection_digest,'publication_integrity');
  checked('LifecycleDraftRecord',bundle.draft);need(hash(bundle.draft.content)===bundle.draft.content_digest&&hash(bundle.draft.content.selections)===bundle.draft.selection_digest,'publication_integrity');
  const derived=derivePublication(bundle.draft,version,bundle.snapshot);
  need(same(bundle.program,derived.compiled.compiled)&&same(bundle.graph,derived.compiled.graph)&&same(bundle.proofs.map(item=>item.value),derived.proofs),'publication_integrity');
  const artifact=checked('PublishedArtifact',bundle.artifact),manifest={...artifact};delete manifest.manifest_digest;
  need(artifact.manifest_digest===hash(manifest)&&artifact.program_digest===derived.compiled.program_digest&&artifact.bindings_digest===derived.compiled.bindings_digest&&
   same(artifact.proof_refs,bundle.proofs.map(item=>item.ref))&&artifact.publisher_ref.digest===hash(bundle.publisher)&&same(artifact.review_ref,bundle.snapshot.review_ref),'publication_integrity');
  for(const item of bundle.proofs){checked('ContentRef',item.ref);need(item.ref.digest===hash(item.value),'publication_integrity');}
  need(new Set(bundle.proofs.map(item=>item.ref.id)).size===bundle.proofs.length,'publication_integrity');
  const index=db.prepare('SELECT proof_id,revision,proof_digest FROM definition_publication_proof WHERE definition_id=? AND definition_version=? ORDER BY proof_id').raw().all(id,version);
  need(same(index,bundle.proofs.map(item=>[item.ref.id,item.ref.revision,item.ref.digest]).sort((a,b)=>a[0].localeCompare(b[0]))),'publication_integrity');
  const publisher=bundle.publisher;
  need(same(publisher.scope,scope)&&publisher.definition_id===id&&publisher.definition_version===version&&publisher.source_draft_revision===row[2]&&same(publisher.authorization.actor,JSON.parse(row[5]))&&publisher.authorization.actor.principal_kind==='human','publication_integrity');
  need(result.definition_id===id&&result.definition_version===version&&result.source_draft_revision===row[2]&&same(result.scope,scope)&&result.program_digest===artifact.program_digest&&result.bindings_digest===artifact.bindings_digest&&result.published_by===publisher.authorization.actor.principal_id&&same(result.artifact_ref,{kind:'artifact',scope,object_id:artifact.artifact_id,revision:'1',digest:artifact.manifest_digest}),'publication_integrity');
  return bundle;
 }
 function read(authentication,id,version,select){
  checked('LifecycleId',id);checked('LifecycleRevision',version);
  return writer(()=>{const proof=authorize(authentication,'getDraft',id,null),bundle=historical(id,version),value=select(bundle);need(same(proof,authorize(authentication,'getDraft',id,null)),'publication_authorization_changed');return freeze(structuredClone(value));});
 }
 return Object.freeze({
  async publish(authentication,id,request){
   available();checked('LifecycleId',id);const body=checked('LifecyclePublish',request);need(pending<4,'publication_capacity');pending++;
   try{
    const semantic={expected_draft_revision:body.expected_draft_revision,expected_content_digest:body.expected_content_digest,expected_selection_digest:body.expected_selection_digest};
    const requestDigest=hash({version:'automation-lifecycle/0.1.0-draft.1',operation_id:'publishDefinition',definition_id:id,request:semantic});
    const prepared=writer(()=>{
     const proof=authorize(authentication,'publishDefinition',id,body.context.context_id);
     const identity=hash({scope,request_epoch:proof.request_epoch,actor:proof.actor,operation_id:'publishDefinition',idempotency_key:body.context.idempotency_key}),existing=prior(identity,requestDigest,authentication,id);
     if(existing){need(same(proof,authorize(authentication,'publishDefinition',id,body.context.context_id)),'publication_authorization_changed');return {existing};}
     need(source&&typeof source.resolve==='function'&&typeof source.assertCurrentInTransaction==='function','publication_source_unavailable');
     const current=draft(id);cas(current,body);
     need(db.prepare('SELECT count(*) FROM definition_version_reservation').pluck().get()<16384&&db.prepare('SELECT count(*) FROM definition_request').pluck().get()<16384,'publication_capacity');
     const last=db.prepare('SELECT definition_version FROM definition_version_reservation WHERE definition_id=? ORDER BY length(definition_version) DESC,definition_version DESC LIMIT 1').pluck().get(id);
     const version=String(BigInt(last??'0')+1n);checked('LifecycleRevision',version);
     need(db.prepare('INSERT INTO definition_version_reservation VALUES(?,?,?,?)').run(id,version,current.revision,crypto.randomUUID()).changes===1,'publication_integrity');
     need(same(proof,authorize(authentication,'publishDefinition',id,body.context.context_id)),'publication_authorization_changed');return {proof,identity,draft:current,version};
    });
    if(prepared.existing)return prepared.existing;
    const snapshot=await source.resolve(freeze({scope,definition_id:id,draft:prepared.draft}));available();
    const derived=derivePublication(prepared.draft,prepared.version,snapshot);await validateStaticParameters(derived);available();
    const fixed=derived.input.snapshot,binding=freeze({scope,definition_id:id,source_draft_revision:prepared.draft.revision,content_digest:prepared.draft.content_digest,selection_digest:prepared.draft.selection_digest,snapshot:fixed});
    return writer(()=>{
     const proof=authorize(authentication,'publishDefinition',id,body.context.context_id);need(same(proof,prepared.proof),'publication_authorization_changed');
     const existing=prior(prepared.identity,requestDigest,authentication,id);if(existing){need(same(proof,authorize(authentication,'publishDefinition',id,body.context.context_id)),'publication_authorization_changed');return existing;}
     cas(draft(id),body);need(source.assertCurrentInTransaction(binding)===hash(fixed),'publication_source_changed');
     need(db.prepare('SELECT count(*) FROM definition_request').pluck().get()<16384,'publication_capacity');
     const transaction=crypto.randomUUID();authority.commitRequestInTransaction?.({proof,transaction_id:transaction});
     const publisher={scope,definition_id:id,definition_version:prepared.version,source_draft_revision:prepared.draft.revision,authorization:proof};
     const proofs=derived.proofs.map(value=>({ref:contentRef(value),value}));
     const artifact={artifact_version:'automation-program/0.1.0-draft.1',artifact_id:crypto.randomUUID(),program_digest:derived.compiled.program_digest,bindings_digest:derived.compiled.bindings_digest,proof_refs:proofs.map(item=>item.ref),publisher_ref:contentRef(publisher),review_ref:fixed.review_ref};
     artifact.manifest_digest=hash(artifact);checked('PublishedArtifact',artifact);
     const result=checked('LifecyclePublishedVersion',{scope,definition_id:id,definition_version:prepared.version,source_draft_revision:prepared.draft.revision,artifact_ref:{kind:'artifact',scope,object_id:artifact.artifact_id,revision:'1',digest:artifact.manifest_digest},program_digest:artifact.program_digest,bindings_digest:artifact.bindings_digest,published_by:proof.actor.principal_id});
     const bundle={draft:prepared.draft,snapshot:fixed,program:derived.compiled.compiled,graph:derived.compiled.graph,proofs,publisher,artifact,result},encoded=canonicalize(bundle);need(Buffer.byteLength(encoded)<=67108864,'publication_capacity');
     need(db.prepare('INSERT INTO definition_publication VALUES(?,?,?,?,?)').run(id,prepared.version,transaction,encoded,hash(bundle)).changes===1,'publication_integrity');
     for(const item of proofs)need(db.prepare('INSERT INTO definition_publication_proof VALUES(?,?,?,?,?)').run(id,item.ref.id,item.ref.revision,prepared.version,item.ref.digest).changes===1,'publication_integrity');
     need(db.prepare('INSERT INTO definition_request VALUES(?,?,?,?,?,?,?,?)').run(transaction,prepared.identity,requestDigest,canonicalize(proof.actor),'publishDefinition',id,canonicalize(result),hash(result)).changes===1,'publication_integrity');
     const notification={kind:'publishDefinition',transaction_id:transaction,scope,definition_id:id,definition_version:prepared.version,source_draft_revision:prepared.draft.revision,artifact_ref:result.artifact_ref,actor:proof.actor};
     need(db.prepare('INSERT INTO definition_notification VALUES(?,?,?)').run(transaction,canonicalize(notification),hash(notification)).changes===1,'publication_integrity');
     need(same(proof,authorize(authentication,'publishDefinition',id,body.context.context_id)),'publication_authorization_changed');
     need(source.assertCurrentInTransaction(binding)===hash(fixed),'publication_source_changed');return result;
    });
   }finally{pending--;}
  },
  getArtifact(authentication,id,version){return read(authentication,id,version,bundle=>bundle.artifact);},
  getProgram(authentication,id,version){return read(authentication,id,version,bundle=>bundle.program);},
  getProof(authentication,id,proofId,revision){
   checked('LifecycleId',id);checked('ContentRef',{id:proofId,revision,digest:'sha256:'+'0'.repeat(64)});
   return writer(()=>{
    const proof=authorize(authentication,'getDraft',id,null),row=db.prepare('SELECT definition_version,proof_digest FROM definition_publication_proof WHERE definition_id=? AND proof_id=? AND revision=?').raw().get(id,proofId,revision);need(row,'publication_missing');
    const bundle=historical(id,row[0]),found=bundle.proofs.find(item=>item.ref.id===proofId&&item.ref.revision===revision&&item.ref.digest===row[1]);need(found,'publication_integrity');
    need(same(proof,authorize(authentication,'getDraft',id,null)),'publication_authorization_changed');return found.value;
   });
  }
 });
}
