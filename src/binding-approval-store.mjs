import crypto from 'node:crypto';
import {canonical,digest,same} from './runtime/auth-clock-value.mjs';
const need=(ok,code)=>{if(!ok)throw Object.assign(new Error('binding_approval_'+code),{code:'binding_approval_'+code});};
const fields=(v,keys)=>need(v&&Object.keys(v).sort().join(',')===keys.split(',').sort().join(','),'request');
const uuid=v=>need(typeof v==='string'&&/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(v),'request');
const hash=v=>need(typeof v==='string'&&/^sha256:[0-9a-f]{64}$/.test(v),'request');
const put=result=>need(result.changes===1,'storage');
export function bindingApprovalStore({db,service,isOpen,authority,receiver,pairings}){
 const scope={authority_service_id:service.service_id,database_instance_id:service.database_instance_id};
 const inside=()=>need(isOpen()&&db.open&&db.inTransaction&&['3','4'].includes(service.schema_revision),'transaction');
 const writer=work=>{need(isOpen()&&db.open&&!db.inTransaction,'closed');need(['3','4'].includes(service.schema_revision),'migration_required');return db.transaction(work).immediate();};
 const reference=value=>({id:value.approval_id,revision:value.revision,digest:digest(value)});
 function read(id,revision=null){inside();uuid(id);const row=revision===null?db.prepare('SELECT r.body_jcs,r.body_digest,r.revision,r.definition_id,r.state FROM binding_approval_revision r JOIN binding_approval_head h USING(approval_id,revision) WHERE approval_id=?').raw().get(id):db.prepare('SELECT body_jcs,body_digest,revision,definition_id,state FROM binding_approval_revision WHERE approval_id=? AND revision=?').raw().get(id,Number(revision));
  need(row,'missing');const value=JSON.parse(row[0]);need(canonical(value)===row[0]&&digest(value)===row[1]&&value.approval_id===id&&value.revision===String(row[2])&&value.definition_id===row[3]&&value.state===row[4]&&same(value.scope,scope),'integrity');return value;
 }
 const authorize=(authentication,definition,write=true,context=null)=>{inside();const proof=structuredClone(authority.bindingApprovalInTransaction(authentication,definition,write,context));need(same(proof.scope,scope)&&proof.definition_id===definition&&(!write||proof.actor.principal_kind==='human'),'authority');return proof;};
 function prior(actor,request){const row=db.prepare('SELECT request_digest,approval_id,revision FROM binding_approval_operation WHERE actor_digest=? AND request_id=?').raw().get(digest(actor),request.request_id);if(!row)return null;need(row[0]===digest(request),'conflict');return read(row[1],String(row[2]));}
 function save(value,proof,request){
  authority.bindingApprovalCapacityInTransaction?.(proof);const transaction=crypto.randomUUID(),body=canonical(value);need(Buffer.byteLength(body)<=524288,'capacity');
  put(db.prepare('INSERT INTO binding_approval_revision VALUES(?,?,?,?,?,?,?)').run(value.approval_id,Number(value.revision),value.definition_id,value.state,body,digest(value),transaction));
  if(value.revision==='1')put(db.prepare('INSERT INTO binding_approval_head VALUES(?,1)').run(value.approval_id));
  else put(db.prepare('UPDATE binding_approval_head SET revision=2 WHERE approval_id=? AND revision=1').run(value.approval_id));
  put(db.prepare('INSERT INTO binding_approval_operation VALUES(?,?,?,?,?,?)').run(transaction,digest(proof.actor),request.request_id,digest(request),value.approval_id,Number(value.revision)));
  const notice={format:'leafseamer.binding-approval-notice/1',scope,transaction_id:transaction,approval_ref:reference(value),definition_id:value.definition_id,state:value.state,protection:'pending'};
  put(db.prepare('INSERT INTO binding_approval_outbox VALUES(?,?,?)').run(transaction,canonical(notice),digest(notice)));need(same(read(value.approval_id),value),'integrity');return value;
 }
 function current(approvalRef,definitionId,handle){
   inside();fields(approvalRef,'id,revision,digest');const value=read(approvalRef.id);need(value.state==='approved'&&same(reference(value),approvalRef)&&value.definition_id===definitionId,'not_current');
   pairings.requireActivatedInTransaction(value.pairing_ref);const source=structuredClone(receiver.currentInTransaction(handle));
   need(same(source.pairing_ref,value.pairing_ref)&&same(source.request.scope,value.source_scope)&&same(source.reply.bundle.registration_ref,value.registration_ref)&&source.bundle_digest===value.bundle_digest&&same(source.reply.bundle,value.bundle),'source_changed');
   return value;
  }
 function approvalRequest(request){
  fields(request,'request_id,approval_id,definition_id,expected_bundle_digest');uuid(request.request_id);uuid(request.approval_id);uuid(request.definition_id);hash(request.expected_bundle_digest);return structuredClone(request);
 }
 function queryRequest(request){
  fields(request,'binding_id,scope,device_id,expected_registration_ref');uuid(request.binding_id);uuid(request.device_id);return structuredClone(request);
 }
 function approve(authentication,handle,request,expectedProof=null,context=null){
   request=approvalRequest(request);
   return writer(()=>{
    const proof=authorize(authentication,request.definition_id,true,context);need(expectedProof===null||same(proof,expectedProof),'authority_changed');
    const old=prior(proof.actor,request);if(old){need(same(proof,authorize(authentication,request.definition_id,true,context)),'authority_changed');return old;}
    need(db.prepare('SELECT 1 FROM definition_identity WHERE definition_id=?').pluck().get(request.definition_id),'definition_missing');
    need(!db.prepare('SELECT 1 FROM binding_approval_head WHERE approval_id=?').pluck().get(request.approval_id),'exists');need(db.prepare('SELECT count(*) FROM binding_approval_revision').pluck().get()<2048,'capacity');
    const source=structuredClone(receiver.currentInTransaction(handle));need(source.bundle_digest===request.expected_bundle_digest,'source_changed');
    const value={format:'leafseamer.binding-approval/1',scope,approval_id:request.approval_id,revision:'1',definition_id:request.definition_id,state:'approved',purpose:'definition_compile',
     pairing_ref:source.pairing_ref,source_scope:source.request.scope,registration_ref:source.reply.bundle.registration_ref,bundle_digest:source.bundle_digest,bundle:source.reply.bundle,authorization:proof};
    save(value,proof,request);need(same(source,receiver.currentInTransaction(handle)),'source_changed');need(same(proof,authorize(authentication,request.definition_id,true,context)),'authority_changed');return value;
   });
  }
 return Object.freeze({
  prepare:request=>receiver.prepare(request),inspect:handle=>receiver.current(handle),approve,
  // 候选正文供人员查看；不把本地短期读取句柄交给交互界面持有。
  async candidate(authentication,definitionId,request){
   uuid(definitionId);request=queryRequest(request);
   const proof=writer(()=>{const p=authorize(authentication,definitionId,false);need(db.prepare('SELECT 1 FROM definition_identity WHERE definition_id=?').pluck().get(definitionId),'definition_missing');return p;});
   const handle=await receiver.prepare(request);
   try{return writer(()=>{need(same(proof,authorize(authentication,definitionId,false)),'authority_changed');const source=structuredClone(receiver.currentInTransaction(handle));
    need(same(proof,authorize(authentication,definitionId,false)),'authority_changed');return {definition_id:definitionId,bundle_digest:source.bundle_digest,bundle:source.reply.bundle};});}
   finally{receiver.release(handle);}
  },
  async approveCurrent(authentication,sourceRequest,request,context=null){
   context=structuredClone(context);
   request=approvalRequest(request);sourceRequest=queryRequest(sourceRequest);
   const prepared=writer(()=>{const proof=authorize(authentication,request.definition_id,true,context),old=prior(proof.actor,request);
    need(db.prepare('SELECT 1 FROM definition_identity WHERE definition_id=?').pluck().get(request.definition_id),'definition_missing');
    need(same(proof,authorize(authentication,request.definition_id,true,context)),'authority_changed');return {proof,old};});
   if(prepared.old)return prepared.old;
   const handle=await receiver.prepare(sourceRequest);
   try{return approve(authentication,handle,request,prepared.proof,context);}finally{receiver.release(handle);}
  },
  lookup(authentication,request){
   fields(request,'request_id,request_digest,definition_id');uuid(request.request_id);uuid(request.definition_id);hash(request.request_digest);request=structuredClone(request);
   return writer(()=>{const proof=structuredClone(authority.bindingApprovalLookupInTransaction(authentication,request.definition_id));
    need(same(proof.scope,scope)&&proof.definition_id===request.definition_id,'authority');
    const row=db.prepare('SELECT request_digest,approval_id,revision FROM binding_approval_operation WHERE actor_digest=? AND request_id=?').raw().get(digest(proof.actor),request.request_id);
    let result=null;if(row){need(row[0]===request.request_digest,'conflict');result=read(row[1],String(row[2]));need(result.definition_id===request.definition_id,'missing');}
    need(same(proof,authority.bindingApprovalLookupInTransaction(authentication,request.definition_id)),'authority_changed');return {...request,result};});
  },
  revoke(authentication,request,context=null,definitionId=null){context=structuredClone(context);fields(request,'request_id,approval_id,expected_revision');uuid(request.request_id);uuid(request.approval_id);need(request.expected_revision==='1','revision');request=structuredClone(request);
   return writer(()=>{const current=read(request.approval_id);need(definitionId===null||current.definition_id===definitionId,'missing');const proof=authorize(authentication,current.definition_id,true,context),old=prior(proof.actor,request);if(old){need(same(proof,authorize(authentication,current.definition_id,true,context)),'authority_changed');return old;}
    need(current.state==='approved'&&current.revision===request.expected_revision,'revision');const result=save({...current,revision:'2',state:'revoked',revocation:proof},proof,request);
    need(same(proof,authorize(authentication,current.definition_id,true,context)),'authority_changed');return result;});
  },
  get(authentication,id){return writer(()=>{const value=read(id),proof=authorize(authentication,value.definition_id,false);need(same(proof,authorize(authentication,value.definition_id,false)),'authority_changed');return value;});},
  check(authentication,approvalRef,definitionId,handle){return writer(()=>{const proof=authorize(authentication,definitionId,false),value=current(approvalRef,definitionId,handle);need(same(proof,authorize(authentication,definitionId,false)),'authority_changed');return value;});},
  currentInTransaction:current
 });
}
