import crypto from 'node:crypto';
import {canonical,checked,digest,ref,same,seal} from './runtime/auth-clock-value.mjs';
import {clockInteger} from './runtime/native-clock.mjs';
import {contractSelection as selection} from './sdk/index.mjs';
import {createHttpAdapter} from './sdk/http.mjs';
const need=(ok,code)=>{if(!ok)throw Object.assign(new Error('local_authorization_'+code),{code:'local_authorization_'+code});};
const uuid=v=>typeof v==='string'&&v.length===36&&/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(v);
const fields=(v,keys)=>v&&Object.keys(v).sort().join(',')===[...keys].sort().join(',');
const pad=v=>String(v).padStart(20,'0'),put=r=>need(r.changes===1,'storage');
const operations=['createDefinition','getDraft','updateDraft','updateDefinitionMetadata','publishDefinition','approveDefinitionBindings','revokeBindingApproval','createRun','getRun'];
const proposalOperation='createRunApprovalProposal';
const approveOperation='approveRun';
const revokeOperation='revokeRunApproval';
const armOperation='armRun';
const supported=new Set(['auth.manage','auth.read','request.create','request.read','definition.read','definition.edit','definition.publish','run.manage','run.read','run.approve','run.approval.revoke','run.arm']);
const utc=v=>BigInt(Date.parse(v))*1000000n;
const binding=v=>{const s=v.snapshot;return {issuer_id:s.issuer_id,issuer_epoch:s.issuer_epoch,principal_id:s.principal_id,principal_kind:s.principal_kind,
 session_id:s.session_id,session_revision:s.session_revision,credential_id:s.credential_id,subject_revision:s.subject_revision,policy_revision:s.policy_revision,
 scope_digest:s.scope_digest,token_binding_digest:v.token_binding_digest};};

export function writeLocalPermission(db,{grant,previous,actorSubjectId,requestId,requestDigest}){
 need(db.inTransaction,'transaction');
 const {operation_id,subject_id,revision}=grant,reduced=grant.state==='pending';
 const result={operation_id,subject_id,previous_ref:previous?ref(previous):null,current_ref:ref(grant),reduced,propagation:'pending'};
 put(db.prepare('INSERT INTO local_permission_revision VALUES(?,?,?,?,?,?,?,?)').run(subject_id,pad(revision),grant.id,previous?pad(previous.revision):null,
  grant.state,canonical(grant),grant.digest,operation_id));
 if(previous)put(db.prepare('UPDATE local_permission_head SET revision=? WHERE subject_id=? AND revision=?').run(pad(revision),subject_id,pad(previous.revision)));
 else put(db.prepare('INSERT INTO local_permission_head VALUES(?,?)').run(subject_id,pad(revision)));
 put(db.prepare('INSERT INTO local_permission_operation VALUES(?,?,?,?,?,?,?,?)').run(operation_id,subject_id,pad(revision),actorSubjectId,requestId,requestDigest,canonical(result),digest(result)));
 const notification={kind:'local_permission_changed',operation_id,subject_id,grant_ref:ref(grant),reduced};
 put(db.prepare('INSERT INTO local_permission_outbox VALUES(?,?,?)').run(operation_id,canonical(notification),digest(notification)));
 return result;
}

// 远端身份只提供上限；本服务的主体映射、批准和context由本库独立裁决。
// 普通登录和查询不建立主体或批准。
export function localAuthorization({db,service,bootId,isOpen,clock,receiver,issuer}){
 const fixed=structuredClone(issuer),scope={authority_service_id:service.service_id,database_instance_id:service.database_instance_id},adapter=createHttpAdapter(selection);
 const privateApprovalOperation=operation=>operation===proposalOperation&&Number(service.schema_revision)>=6||operation===approveOperation&&Number(service.schema_revision)>=7||
  operation===revokeOperation&&Number(service.schema_revision)>=8;
 const privateHumanOperation=operation=>privateApprovalOperation(operation)||operation===armOperation&&Number(service.schema_revision)>=14;
 const permissionFor=operation=>operation===armOperation?'run.arm':privateApprovalOperation(operation)?(operation===revokeOperation?'run.approval.revoke':'run.approve'):adapter.describe('seamer',operation).extensions['x-permission'];
 const inside=()=>need(isOpen()&&db.open&&db.inTransaction,'transaction');
 const writer=work=>{need(isOpen()&&db.open&&!db.inTransaction,'closed');return db.transaction(work).immediate();};
 const decode=(text,hash)=>{const v=JSON.parse(text);need(canonical(v)===text&&digest(v)===hash,'integrity');return v;};
 function time(){
  inside();const sample=clock.pairingSample(),row=db.prepare('SELECT digest,body FROM authentication_clock_acquisition WHERE id=? AND revision=?').raw().get(sample.acquisition_ref.id,sample.acquisition_ref.revision);
  need(row&&row[0]===sample.acquisition_ref.digest,'clock');const v=JSON.parse(row[1].toString());
  need(v.service_id===service.service_id&&v.service_boot_id===bootId&&v.clock_domain_id===sample.runtime_domain_ref.clock_domain_id&&v.descriptor_digest===sample.runtime_domain_ref.descriptor_digest,'clock');
  const lower=clockInteger(sample.lower_ns),upper=clockInteger(sample.upper_ns);need(lower<=upper,'clock');
  return {lower,upper,tick:clockInteger(sample.tick_ns),domain:sample.runtime_domain_ref,acquisition_ref:sample.acquisition_ref};
 }
 function identity(handle,revocationScope=null){inside();const value=receiver.currentInTransaction(handle,revocationScope),s=value.snapshot;
  need(value.issuer_service_id===fixed.service_id&&s.issuer_id===fixed.issuer_id&&s.issuer_epoch===fixed.issuer_epoch&&s.audience===service.service_id&&s.state==='active','identity');
  need(uuid(s.principal_id)&&['human','service'].includes(s.principal_kind)&&/^sha256:[0-9a-f]{64}$/.test(value.token_binding_digest),'identity');return value;
 }
 function subject(s){const row=db.prepare('SELECT subject_id,principal_kind FROM local_subject WHERE issuer_id=? AND issuer_epoch=? AND principal_id=?').raw().get(s.issuer_id,s.issuer_epoch,s.principal_id);
  if(!row)return null;need(row[1]===s.principal_kind,'subject');return row[0];}
 function current(id){
  inside();const row=db.prepare('SELECT r.body_jcs,r.digest,r.id,r.revision,r.state FROM local_permission_revision r JOIN local_permission_head h USING(subject_id,revision) WHERE subject_id=?').raw().get(id);
  need(row,'missing');const v=checked(JSON.parse(row[0]));need(canonical(v)===row[0]&&v.digest===row[1]&&v.id===row[2]&&pad(v.revision)===row[3]&&v.state===row[4]&&v.subject_id===id&&same(v.scope,scope)&&v.issuer_id===fixed.issuer_id&&v.issuer_epoch===fixed.issuer_epoch,'integrity');return v;
 }
 function authorized(handle,permission,definition=undefined,revocationScope=null){
  const value=identity(handle,revocationScope),id=subject(value.snapshot);need(id,'missing');const grant=current(id),now=time();
  need(grant.state==='active'&&now.lower>=clockInteger(grant.not_before_ns)&&now.upper<clockInteger(grant.expires_ns)&&grant.permissions.includes(permission)&&value.snapshot.permissions.includes(permission),'denied');
  if(definition!==undefined)need(grant.definition_scope.kind==='all'||definition!==null&&grant.definition_scope.definition_ids.includes(definition),'object');
  return {value,subject_id:id,grant};
 }
 function manager(handle){const found=authorized(handle,'auth.manage'),recent=found.value.recent_authentication,now=time();
  need(found.value.snapshot.principal_kind==='human'&&recent&&now.lower>=utc(recent.not_before)&&now.upper<utc(recent.expires_at),'recent');return found;
 }
 function recheck(handle,original,recent=false){const again=identity(handle),now=time();need(same(again,original.value)&&now.lower>=clockInteger(original.grant.not_before_ns)&&now.upper<clockInteger(original.grant.expires_ns),'changed');
  if(recent)need(again.recent_authentication&&now.lower>=utc(again.recent_authentication.not_before)&&now.upper<utc(again.recent_authentication.expires_at),'recent');}
 function allowed(names,handle){need(Array.isArray(names)&&names.length>0&&names.length<=operations.length&&new Set(names).size===names.length,'operations');
  for(const name of names){need(operations.includes(name)&&(Number(service.schema_revision)>=5||!['createRun','getRun'].includes(name)),'operations');const found=authorized(handle,adapter.describe('seamer',name).extensions['x-permission']);if(['publishDefinition','approveDefinitionBindings','revokeBindingApproval'].includes(name))need(found.value.snapshot.principal_kind==='human','human');}return [...names].sort();
 }
 function context(handle,id,operation){
  const found=authorized(handle,permissionFor(operation)),now=time();
  const row=db.prepare('SELECT subject_id,service_boot_id,body_jcs,digest FROM local_request_context WHERE context_id=?').raw().get(id);need(row,'context');
  const original=decode(row[2],row[3]);need(row[0]===found.subject_id&&row[1]===bootId&&original.context.context_epoch===service.request_epoch&&same(original.context.selection,selection)&&
   same(original.binding,binding(found.value))&&same(original.grant_ref,ref(found.grant))&&same(original.domain,now.domain)&&now.tick>=clockInteger(original.started_tick_ns)&&now.tick<clockInteger(original.deadline_tick_ns)&&
   now.lower>=utc(original.context.issued_at)&&now.upper<utc(original.context.expires_at)&&original.context.operations.includes(operation),'context');
  return {found,original};
 }
 function draftProof({authentication,operation_id,definition_id,context_id,scope:expected}){
  inside();const human=privateHumanOperation(operation_id);
  need(same(scope,expected)&&(operations.includes(operation_id)||human),'scope');
  const read=operation_id==='getDraft'?'definition.read':operation_id==='getRun'?'run.read':null;
  const {found,original}=read&&context_id===null?{found:authorized(authentication,read),original:null}:context(authentication,context_id,operation_id);
  if(operation_id==='publishDefinition')need(found.value.snapshot.principal_kind==='human','human');
  need(found.grant.definition_scope.kind==='all'||definition_id!==null&&found.grant.definition_scope.definition_ids.includes(definition_id),'object');
  const s=found.value.snapshot;
  const proof={scope,actor:{issuer_id:fixed.issuer_id,issuer_epoch:fixed.issuer_epoch,principal_id:s.principal_id,principal_kind:s.principal_kind},context_id,
   request_epoch:service.request_epoch,operation_id,definition_id,authorization_digest:digest({binding:binding(found.value),grant_ref:ref(found.grant),context:original?.context??null})};
  if(!human)return proof;
  need(s.principal_kind==='human','human');recheck(authentication,found,true);
  const authentication_evidence={snapshot_id:s.snapshot_id,issuer_revision:s.issuer_revision,issuer_service_id:found.value.issuer_service_id,
   snapshot_digest:digest(s),recent_authentication:found.value.recent_authentication,grant_ref:ref(found.grant),authorization_digest:proof.authorization_digest};
  if(operation_id===armOperation){
   need(uuid(found.value.token_id),'arm_token');
   authentication_evidence.credential={issuer_service_id:found.value.issuer_service_id,token_id:found.value.token_id,session_id:s.session_id,
    session_revision:s.session_revision,credential_id:s.credential_id,policy_revision:s.policy_revision,scope_digest:s.scope_digest,
    recent_before_ns:String(utc(found.value.recent_authentication.expires_at))};
  }
  return {...proof,authentication_evidence};
 }
 function contextCharges(contextId){
  const draft=db.prepare('SELECT count(*) FROM local_context_request WHERE context_id=?').pluck().get(contextId);
  if(Number(service.schema_revision)<3)return draft;
  const approvals=db.prepare("SELECT count(*) FROM binding_approval_revision WHERE CASE state WHEN 'approved' THEN json_extract(body_jcs,'$.authorization.context_id') ELSE json_extract(body_jcs,'$.revocation.context_id') END=?").pluck().get(contextId);
  const runs=Number(service.schema_revision)>=5?db.prepare('SELECT count(*) FROM automation_run_creation WHERE context_id=?').pluck().get(contextId):0;
  const proposals=Number(service.schema_revision)>=6?db.prepare('SELECT count(*) FROM automation_run_approval_proposal WHERE context_id=?').pluck().get(contextId):0;
  const runApprovals=Number(service.schema_revision)>=7?db.prepare('SELECT count(*) FROM automation_run_approval WHERE context_id=?').pluck().get(contextId):0;
  const revocations=Number(service.schema_revision)>=8?db.prepare('SELECT count(*) FROM automation_run_approval_revocation WHERE context_id=?').pluck().get(contextId):0;
  const eventApprovals=Number(service.schema_revision)>=11?db.prepare('SELECT count(*) FROM event_binding_approval_revision WHERE context_id=?').pluck().get(contextId):0;
  const arms=Number(service.schema_revision)>=14?db.prepare('SELECT count(*) FROM automation_run_arm WHERE context_id=?').pluck().get(contextId):0;
  return draft+approvals+runs+proposals+runApprovals+revocations+eventApprovals+arms;
 }
 function assertContextCapacity(proof){
  inside();const row=db.prepare('SELECT body_jcs,digest FROM local_request_context WHERE context_id=?').raw().get(proof.context_id);need(row,'context');
  need(contextCharges(proof.context_id)<decode(row[0],row[1]).context.max_unique_requests,'context_capacity');
 }
 function bindingProof(handle,definitionId,write=true,requestContext=null){
  inside();need(uuid(definitionId),'object');const found=write?manager(handle):authorized(handle,'auth.read');
  need(found.grant.definition_scope.kind==='all'||found.grant.definition_scope.definition_ids.includes(definitionId),'object');
  let contextId=null;
  if(requestContext!==null){need(write&&fields(requestContext,['context_id','operation_id'])&&['approveDefinitionBindings','revokeBindingApproval'].includes(requestContext.operation_id),'context');context(handle,requestContext.context_id,requestContext.operation_id);contextId=requestContext.context_id;}
  const s=found.value.snapshot;return {scope,definition_id:definitionId,actor:{issuer_id:s.issuer_id,issuer_epoch:s.issuer_epoch,principal_id:s.principal_id,principal_kind:s.principal_kind},
   grant_ref:ref(found.grant),...(contextId?{context_id:contextId}:{}),authorization_digest:digest({binding:binding(found.value),grant_ref:ref(found.grant),...(contextId?{context_id:contextId}: {})})};
 }
 function issueContext(handle,request,approvalOperation=null){
  need(fields(request,['audience','selection','operations','ttl_ms','max_unique_requests'])&&request.audience===service.service_id&&same(request.selection,selection)&&Number.isSafeInteger(request.ttl_ms)&&request.ttl_ms>0&&request.ttl_ms<=86400000&&Number.isSafeInteger(request.max_unique_requests)&&request.max_unique_requests>0&&request.max_unique_requests<=4096,'request');
  return writer(()=>{
   const found=authorized(handle,'request.create');
   let names;
   if(approvalOperation){
    need(privateHumanOperation(approvalOperation),'operations');const approval=authorized(handle,permissionFor(approvalOperation));
    need(approval.value.snapshot.principal_kind==='human','human');recheck(handle,approval,true);names=[approvalOperation];
   }else names=allowed(request.operations,handle);
   const now=time();need(db.prepare('SELECT count(*) FROM local_request_context').pluck().get()<4096,'capacity');
   const start=(now.lower+999999n)/1000000n*1000000n,expiry=(now.lower+BigInt(Math.min(request.ttl_ms,60000))*1000000n)/1000000n*1000000n;need(now.upper<expiry&&start<expiry,'time');
   const value={context_id:crypto.randomUUID(),service_id:service.service_id,issuer_id:fixed.issuer_id,issuer_epoch:fixed.issuer_epoch,principal_id:found.value.snapshot.principal_id,audience:service.service_id,
    operations:names,context_epoch:service.request_epoch,issued_at:new Date(Number(start/1000000n)).toISOString(),expires_at:new Date(Number(expiry/1000000n)).toISOString(),ttl_ms:Number((expiry-start)/1000000n),max_unique_requests:Math.min(request.max_unique_requests,256),selection};
   const original={context:value,binding:binding(found.value),grant_ref:ref(found.grant),domain:now.domain,started_tick_ns:String(now.tick),deadline_tick_ns:String(now.tick+BigInt(value.ttl_ms)*1000000n)};
   put(db.prepare('INSERT INTO local_request_context VALUES(?,?,?,?,?)').run(value.context_id,found.subject_id,bootId,canonical(original),digest(original)));
   recheck(handle,found,approvalOperation!==null);need(same(current(found.subject_id),found.grant),'changed');return value;
  });
 }
 return Object.freeze({
  pairingManagement:Object.freeze({
   assertCurrent(expected){inside();need(same(fixed,expected),'issuer');},
   authenticate(handle,observed,expected,revocationScope=null){
    inside();need(same(fixed,expected),'issuer');const found=authorized(handle,'auth.manage',undefined,revocationScope),s=found.value.snapshot,recent=found.value.recent_authentication;
    need(s.principal_kind==='human'&&recent&&observed.lower>=utc(recent.not_before)&&observed.upper<utc(recent.expires_at),'recent');
    const notBefore=[utc(s.validity.not_before),clockInteger(found.grant.not_before_ns)].reduce((a,b)=>a>b?a:b),expiry=[utc(s.validity.expires_at),clockInteger(found.grant.expires_ns)].reduce((a,b)=>a<b?a:b);
    return {session:{session_id:s.session_id,principal_id:s.principal_id,principal_kind:'human',audience:service.service_id,issuer_id:s.issuer_id,issuer_epoch:s.issuer_epoch,
     not_before_ns:String(notBefore),expires_ns:String(expiry),recent_before_ns:String(utc(recent.expires_at))}};
   },
   current(principal,expected){inside();need(same(fixed,expected)&&uuid(principal),'issuer');const id=subject({issuer_id:fixed.issuer_id,issuer_epoch:fixed.issuer_epoch,principal_id:principal,principal_kind:'human'});need(id,'missing');return current(id);}
  }),
  get(handle,principal){return writer(()=>{const actor=authorized(handle,'auth.read');need(uuid(principal),'request');const id=subject({issuer_id:fixed.issuer_id,issuer_epoch:fixed.issuer_epoch,principal_id:principal,principal_kind:'human'}),value=id?current(id):null;recheck(handle,actor);return value;});},
  replace(handle,request){
   need(fields(request,['request_id','principal_id','principal_kind','expected_revision','permissions','definition_scope','not_before_ns','expires_ns'])&&uuid(request.request_id)&&uuid(request.principal_id)&&['human','service'].includes(request.principal_kind),'request');
   need(request.expected_revision===null||typeof request.expected_revision==='string'&&/^[1-9][0-9]{0,19}$/.test(request.expected_revision)&&request.expected_revision.length<=20,'request');
   need(Array.isArray(request.permissions)&&request.permissions.length<=supported.size&&new Set(request.permissions).size===request.permissions.length&&request.permissions.every(p=>supported.has(p)),'request');
   need(request.principal_kind==='human'||!request.permissions.some(p=>['definition.publish','run.approve','run.approval.revoke','run.arm'].includes(p)),'human');
   const ds=request.definition_scope;need(fields(ds,['kind','definition_ids'])&&['all','selected'].includes(ds.kind)&&Array.isArray(ds.definition_ids)&&ds.definition_ids.length<=256&&new Set(ds.definition_ids).size===ds.definition_ids.length&&ds.definition_ids.every(uuid)&& (ds.kind!=='all'||ds.definition_ids.length===0),'request');
   request=structuredClone(request);request.permissions.sort();request.definition_scope.definition_ids.sort();const requestDigest=digest(request);
   return writer(()=>{
    const actor=manager(handle),now=time(),before=clockInteger(request.not_before_ns),expiry=clockInteger(request.expires_ns);need(before<expiry&&now.upper<expiry,'time');
    const previous=db.prepare('SELECT request_digest,body_jcs,digest FROM local_permission_operation WHERE actor_subject_id=? AND request_id=?').raw().get(actor.subject_id,request.request_id);
    if(previous){need(previous[0]===requestDigest,'conflict');const result=decode(previous[1],previous[2]);recheck(handle,actor,true);return result;}
    need(db.prepare('SELECT count(*) FROM local_permission_revision').pluck().get()<16384,'capacity');
    let id=subject({issuer_id:fixed.issuer_id,issuer_epoch:fixed.issuer_epoch,principal_id:request.principal_id,principal_kind:request.principal_kind}),old=null;
    if(id)old=current(id);else{need(request.expected_revision===null,'revision');id=crypto.randomUUID();put(db.prepare('INSERT INTO local_subject VALUES(?,?,?,?,?)').run(id,fixed.issuer_id,fixed.issuer_epoch,request.principal_id,request.principal_kind));}
    need((old?.revision??null)===request.expected_revision&&(!old||old.state==='active'),'revision');const revision=BigInt(old?.revision??'0')+1n;need(revision<10n**20n,'capacity');
    const reduced=!!old&&(old.permissions.some(p=>!request.permissions.includes(p))||before>BigInt(old.not_before_ns)||expiry<BigInt(old.expires_ns)||old.definition_scope.kind==='all'&&ds.kind!=='all'||old.definition_scope.kind==='selected'&&ds.kind==='selected'&&old.definition_scope.definition_ids.some(id=>!ds.definition_ids.includes(id)));
    const operationId=crypto.randomUUID(),grant=seal({format:'seamer-local-permission/1',id:crypto.randomUUID(),revision:String(revision),scope,subject_id:id,issuer_id:fixed.issuer_id,issuer_epoch:fixed.issuer_epoch,
     permissions:request.permissions,definition_scope:request.definition_scope,not_before_ns:request.not_before_ns,expires_ns:request.expires_ns,state:reduced?'pending':'active',operation_id:operationId});
    const result=writeLocalPermission(db,{grant,previous:old,actorSubjectId:actor.subject_id,requestId:request.request_id,requestDigest});
    recheck(handle,actor,true);need(same(current(actor.subject_id),actor.subject_id===id?grant:actor.grant),'changed');return result;
   });
  },
  createRequestContext(handle,request){return issueContext(handle,request);},
  createRunApprovalContext(handle,options){
   need(fields(options,['ttl_ms','max_unique_requests']),'request');
   return issueContext(handle,{...options,audience:service.service_id,selection,operations:[proposalOperation]},proposalOperation);
  },
  createApproveRunContext(handle,options){
   need(fields(options,['ttl_ms','max_unique_requests']),'request');
   return issueContext(handle,{...options,audience:service.service_id,selection,operations:[approveOperation]},approveOperation);
  },
  createRevokeRunApprovalContext(handle,options){
   need(fields(options,['ttl_ms','max_unique_requests']),'request');
   return issueContext(handle,{...options,audience:service.service_id,selection,operations:[revokeOperation]},revokeOperation);
  },
  createArmRunContext(handle,options){
   need(fields(options,['ttl_ms','max_unique_requests']),'request');
   return issueContext(handle,{...options,audience:service.service_id,selection,operations:[armOperation]},armOperation);
  },
  identifyRunApprovalActorInTransaction(handle){
   const {snapshot:s}=identity(handle);
   return {issuer_id:s.issuer_id,issuer_epoch:s.issuer_epoch,principal_id:s.principal_id,principal_kind:s.principal_kind};
  },
  runApprovalTimeInTransaction(){return time();},
  assertArmInitiatorInTransaction(actor,definition,reference){
   inside();need(actor.principal_kind==='human'&&actor.issuer_id===fixed.issuer_id&&actor.issuer_epoch===fixed.issuer_epoch,'human');
   const id=subject(actor);need(id,'missing');const grant=current(id),now=time();
   need(same(ref(grant),reference)&&grant.state==='active'&&grant.permissions.includes('run.arm')&&
    (grant.definition_scope.kind==='all'||grant.definition_scope.definition_ids.includes(definition))&&
    now.lower>=clockInteger(grant.not_before_ns)&&now.upper<clockInteger(grant.expires_ns),'denied');
   return clockInteger(grant.expires_ns);
  },
  bindingApprovalInTransaction:bindingProof,
  confirmBindingResult(handle,definitionId,requestContext=null){return writer(()=>bindingProof(handle,definitionId,requestContext!==null,requestContext));},
  bindingApprovalCapacityInTransaction(proof){
   inside();if(proof.context_id)assertContextCapacity(proof);
  },
  assertRunCapacityInTransaction:assertContextCapacity,
  bindingApprovalLookupInTransaction(handle,definitionId){
   inside();need(uuid(definitionId),'object');const found=authorized(handle,'auth.read',definitionId);authorized(handle,'request.read',definitionId);
   const s=found.value.snapshot;return {scope,definition_id:definitionId,actor:{issuer_id:s.issuer_id,issuer_epoch:s.issuer_epoch,principal_id:s.principal_id,principal_kind:s.principal_kind},
    grant_ref:ref(found.grant),authorization_digest:digest({binding:binding(found.value),grant_ref:ref(found.grant)})};
  },
  authorizeInTransaction:draftProof,
  lookupInTransaction({authentication,context_id,operation_id}){
   inside();need((operations.includes(operation_id)||privateHumanOperation(operation_id))&&operation_id!=='getDraft','operations');
   const found=authorized(authentication,'request.read'),s=found.value.snapshot;
   const row=db.prepare('SELECT subject_id,body_jcs,digest FROM local_request_context WHERE context_id=?').raw().get(context_id);need(row&&row[0]===found.subject_id,'context');
   const original=decode(row[1],row[2]),c=original.context,b=original.binding;
   // 原context仅定位不可变请求epoch；查原使用当前读取权，不重获旧写入期限或令牌资格。
   need(c.context_id===context_id&&c.service_id===service.service_id&&c.audience===service.service_id&&same(c.selection,selection)&&
    c.context_epoch===service.request_epoch&&c.operations.includes(operation_id)&&b.issuer_id===s.issuer_id&&b.issuer_epoch===s.issuer_epoch&&b.principal_id===s.principal_id&&b.principal_kind===s.principal_kind,'context');
   return {scope,request_epoch:c.context_epoch,actor:{issuer_id:s.issuer_id,issuer_epoch:s.issuer_epoch,principal_id:s.principal_id,principal_kind:s.principal_kind},
    authorization_digest:digest({binding:binding(found.value),grant_ref:ref(found.grant),context_id,operation_id})};
  },
  confirmDraftResult(input){return writer(()=>draftProof({...input,scope}));},
  getRequestContext(handle,id){return writer(()=>{
   const row=db.prepare('SELECT body_jcs,digest FROM local_request_context WHERE context_id=?').raw().get(id);need(row,'context');const stored=decode(row[0],row[1]);
   authorized(handle,'request.create');const result=context(handle,id,stored.context.operations[0]).original.context;allowed(result.operations,handle);return result;
  });},
  commitRequestInTransaction({proof,transaction_id}){
   assertContextCapacity(proof);
   put(db.prepare('INSERT INTO local_context_request VALUES(?,?)').run(proof.context_id,transaction_id));
  }
 });
}
