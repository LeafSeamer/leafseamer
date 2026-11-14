import crypto from 'node:crypto';
import https from 'node:https';
import {TLSSocket} from 'node:tls';
import {createHttpAdapter} from './sdk/http.mjs';
import {contractSelection as selection} from './sdk/index.mjs';
import {same,digest} from './runtime/auth-clock-value.mjs';
const supported=new Set(['createRequestContext','createDefinition','getDraft','updateDraft','updateDefinitionMetadata','lookupAutomationRequest','publishDefinition','getPublishedArtifact','getCompiledProgram','getIndependenceProof','queryBindingCandidate','approveDefinitionBindings','getBindingApproval','revokeBindingApproval','lookupBindingApproval']);
const protectedWrites=new Set(['publishDefinition','approveDefinitionBindings','revokeBindingApproval']);
const bindingView=v=>({scope:v.scope,approval_ref:{id:v.approval_id,revision:v.revision,digest:digest(v)},definition_id:v.definition_id,state:v.state,purpose:v.purpose,registration_ref:v.registration_ref,bundle_digest:v.bundle_digest,bundle:v.bundle});
const maximumBody=16777216,maximumReserved=33554432;
const pairs=flat=>Array.from({length:flat.length/2},(_,i)=>[flat[i*2],flat[i*2+1]]);
const fail=code=>{throw Object.assign(new Error(code),{code});};
function failure(error){
 const code=error?.code??error?.message;
 if(code==='http_token'||code==='peer_identity_inactive'||code==='peer_identity_handle')return [401,'AUTH_REQUIRED'];
 if(['local_authorization_object','definition_missing','draft_request_missing','publication_missing','binding_approval_missing','binding_approval_definition_missing'].includes(code))return [404,'NOT_FOUND'];
 if(['local_authorization_denied','local_authorization_missing','local_authorization_context','local_authorization_recent','http_origin','http_csrf','local_authorization_human','publication_human_required'].includes(code))return [403,'PERMISSION_DENIED'];
 if(['draft_revision_conflict','metadata_revision_conflict','draft_idempotency_conflict','publication_draft_conflict','publication_idempotency_conflict','binding_approval_conflict','binding_approval_revision','binding_approval_exists','binding_approval_source_changed'].includes(code))return [409,'REVISION_CONFLICT'];
 if(['local_authorization_capacity','local_authorization_context_capacity','draft_capacity','publication_capacity','binding_approval_capacity','peer_binding_capacity'].includes(code))return [429,'CAPACITY_EXCEEDED'];
 if(['local_authorization_request','local_authorization_operations','draft_duplicate_selection','automation_schema','binding_approval_request'].includes(code))return [422,'INVALID_REQUEST'];
 if(error?.name==='ClockError'||code==='peer_identity_expired')return [503,'CLOCK_UNTRUSTED'];
 return [503,'WORKER_NOT_READY'];
}
// 草稿及发布入口共用固定配对身份，发布采用本监听独立的 context/令牌/origin 绑定 CSRF。
export async function startDraftHttp({host='127.0.0.1',port=0,cert,key,bindingId,service,bootId,receiver,authorization,drafts,publications=null,bindingApprovals=null,allowedOrigins=[],onFailure=null}){
 if(!['127.0.0.1','::1'].includes(host)||!Number.isInteger(port)||port<0||port>65535||typeof bindingId!=='string'||!Array.isArray(allowedOrigins)||allowedOrigins.length>8)throw new Error('draft_http_configuration');
 for(const origin of allowedOrigins){const url=new URL(origin);if(url.protocol!=='https:'||url.origin!==origin)throw new Error('draft_http_origin');}
 const csrfKey=crypto.randomBytes(32);
 const csrfFor=(context,token,origin)=>crypto.createHmac('sha256',csrfKey).update(JSON.stringify([service.service_id,bootId,context,token,origin])).digest('base64url');
 const adapter=createHttpAdapter(selection),origins=new Set(allowedOrigins);let origin,site,closed=false,pending=0,reserved=0;
 const observe=error=>{try{onFailure?.({name:error?.name??'Error',code:error?.reason??error?.code??'http_internal'});}catch{}};
 const empty=(res,status)=>{if(!res.headersSent&&!res.destroyed){res.writeHead(status,{'Cache-Control':'no-store','Content-Length':'0',Connection:'close'});res.end();}};
 async function reply(res,request,status,data=null,code=null,beforeSend=()=>{},extraHeaders=[]){
  const id=crypto.randomUUID(),context=request.operation_id==='createRequestContext';
  const body=Buffer.from(JSON.stringify(context?{request_id:id,service_id:service.service_id,service_boot_id:bootId,protocol_version:selection.contract_id,selection,
   ...(code?{error:{code,message_key:code.toLowerCase(),request_id:id,details:{},existing_command_id:null,existing_operation_id:null,retry_advice:'do_not_retry'}}:{data})}:code?{code,correlation_id:id,diagnostics:[]}:data));
  const headers=[['content-type','application/json'],['content-length',String(body.length)],['cache-control','no-store'],['connection','close'],['x-content-type-options','nosniff'],['x-leafseamer-contract',selection.contract_id],['x-leafseamer-contract-digest',selection.contract_digest]];
  headers.push(...extraHeaders);await adapter.decodeResponse(body,adapter.prepareResponse(request,String(status),headers));
  if(closed||res.destroyed)return;beforeSend();if(!res.headersSent){res.writeHead(status,Object.fromEntries(headers));res.end(body);}
 }
 async function serve(req,res){
  let request;try{request=adapter.prepareRequest('seamer',req.method,req.url,pairs(req.rawHeaders));}catch{empty(res,400);return;}
  if(!supported.has(request.operation_id)){empty(res,404);return;}
  if(!(req.socket instanceof TLSSocket)||req.socket.getProtocol()!=='TLSv1.3'||req.headers.host!==site.host||req.headers.cookie!==undefined){empty(res,403);return;}
  if(req.headers.origin!==undefined&&!origins.has(req.headers.origin)){await reply(res,request,403,null,'PERMISSION_DENIED');return;}
  if(req.headers.origin){res.setHeader('Access-Control-Allow-Origin',req.headers.origin);res.setHeader('Vary','Origin');
   res.setHeader('Access-Control-Expose-Headers','X-LeafSeamer-Contract, X-LeafSeamer-Contract-Digest, X-CSRF-Token');}
  const match=typeof req.headers.authorization==='string'?/^Bearer (v1\.[0-9a-f-]{36}\.[A-Za-z0-9_-]{43})$/.exec(req.headers.authorization):null;
  if(!match||match[0]!==req.headers.authorization){await reply(res,request,401,null,'AUTH_REQUIRED');return;}
  const length=Number(req.headers['content-length']??0);
  if(req.headers['transfer-encoding']!==undefined||!Number.isSafeInteger(length)||length<0){empty(res,400);return;}
  if(length>maximumBody){await reply(res,request,413,null,'PAYLOAD_TOO_LARGE');return;}
  if(pending>=8||reserved+length>maximumReserved){await reply(res,request,429,null,'CAPACITY_EXCEEDED');return;}
  pending++;reserved+=length;
  try{
   const bytes=await new Promise((resolve,reject)=>{
    const chunks=[];let size=0,done=false;
    const finish=(error,value)=>{if(done)return;done=true;clearTimeout(timer);req.off('data',data);req.off('end',end);req.off('aborted',abort);req.off('error',abort);res.off('close',abort);req.pause();error?reject(error):resolve(value);};
    const data=chunk=>{size+=chunk.length;if(size>length||size>maximumBody)finish(413);else chunks.push(chunk);};
    const end=()=>finish(size===length?null:400,Buffer.concat(chunks,size)),abort=()=>finish(400),timer=setTimeout(()=>finish(408),5000);timer.unref();
    req.on('data',data);req.once('end',end);req.once('aborted',abort);req.once('error',abort);res.once('close',abort);
   });
   let body;try{body=(await adapter.decodeRequest(bytes,request))?.value??null;}catch{await reply(res,request,400,null,'INVALID_REQUEST');return;}
   const publishing=request.operation_id==='publishDefinition',protectedWrite=protectedWrites.has(request.operation_id),publishingContext=request.operation_id==='createRequestContext'&&body.operations.some(op=>protectedWrites.has(op));
   if((protectedWrite||publishingContext)&&!req.headers.origin)fail('http_origin');
   if(protectedWrite){const actual=req.headers['x-csrf-token'],expected=csrfFor(body.context.context_id,match[1],req.headers.origin);
    if(typeof actual!=='string'||!/^[A-Za-z0-9_-]{43}$/.test(actual)||!crypto.timingSafeEqual(Buffer.from(actual),Buffer.from(expected)))fail('http_csrf');}
   const handle=await receiver.prepare({binding_id:bindingId,token:match[1]});if(closed||res.destroyed)return;
   const op=request.operation_id,definition=request.parameters.path.definition_id??null;let result,status=200,confirm,extraHeaders=[];
   if(op==='createRequestContext'){
    result=authorization.createRequestContext(handle,body);status=201;
    if(publishingContext)extraHeaders=[['x-csrf-token',csrfFor(result.context_id,match[1],req.headers.origin)]];
    confirm=()=>{if(!same(authorization.getRequestContext(handle,result.context_id),result))fail('local_authorization_changed');};
   }else if(['queryBindingCandidate','approveDefinitionBindings','getBindingApproval','revokeBindingApproval','lookupBindingApproval'].includes(op)){
    if(!bindingApprovals)fail('binding_approval_unavailable');
    const context=protectedWrite?{context_id:body.context.context_id,operation_id:op}:null;
    if(op==='queryBindingCandidate')result=await bindingApprovals.candidate(handle,definition,body);
    else if(op==='approveDefinitionBindings'){
     result=bindingView(await bindingApprovals.approveCurrent(handle,body.source,{request_id:body.request_id,approval_id:body.approval_id,definition_id:definition,expected_bundle_digest:body.expected_bundle_digest},context));status=201;
    }else if(op==='revokeBindingApproval')result=bindingView(bindingApprovals.revoke(handle,{request_id:body.request_id,approval_id:request.parameters.path.approval_id,expected_revision:body.expected_revision},context,definition));
    else if(op==='getBindingApproval'){const value=bindingApprovals.get(handle,request.parameters.path.approval_id);if(value.definition_id!==definition)fail('binding_approval_missing');result=bindingView(value);}
    else {const value=bindingApprovals.lookup(handle,{...body,definition_id:definition});result={...value,result:value.result?bindingView(value.result):null};}
    confirm=()=>{if(op==='lookupBindingApproval')bindingApprovals.lookup(handle,{...body,definition_id:definition});else authorization.confirmBindingResult(handle,definition,context);};
   }else if(op==='lookupAutomationRequest'){
    result=drafts.lookup(handle,body);confirm=()=>drafts.lookup(handle,body);
   }else if(publishing){
    if(!publications)fail('publication_source_unavailable');result=await publications.publish(handle,definition,body);status=201;
    confirm=()=>authorization.confirmDraftResult({authentication:handle,operation_id:op,definition_id:definition,context_id:body.context.context_id});
   }else if(['getPublishedArtifact','getCompiledProgram','getIndependenceProof'].includes(op)){
    if(!publications)fail('publication_source_unavailable');
    const read=()=>op==='getPublishedArtifact'?publications.getArtifact(handle,definition,request.parameters.path.definition_version):op==='getCompiledProgram'?publications.getProgram(handle,definition,request.parameters.path.definition_version):publications.getProof(handle,definition,request.parameters.path.proof_id,request.parameters.query.revision);
    result=read();confirm=()=>{if(!same(result,read()))fail('publication_integrity');};
   }else{
    if(op==='createDefinition'){result=drafts.create(handle,body);status=201;}
    else if(op==='updateDraft')result=drafts.update(handle,definition,body);
    else if(op==='updateDefinitionMetadata')result=drafts.updateMetadata(handle,definition,body);
    else result=drafts.get(handle,definition);
    confirm=()=>authorization.confirmDraftResult({authentication:handle,operation_id:op,definition_id:definition,context_id:body?.context.context_id??null});
   }
   await reply(res,request,status,result,null,confirm,extraHeaders);
  }catch(error){observe(error);if(typeof error==='number'){empty(res,error);return;}const [status,code]=failure(error);await reply(res,request,status,null,code);}
  finally{pending--;reserved-=length;}
 }
 const server=https.createServer({cert,key,minVersion:'TLSv1.3',maxVersion:'TLSv1.3',maxHeaderSize:8192,requestTimeout:5000,headersTimeout:5000,keepAliveTimeout:1000},(req,res)=>{
  if(req.method==='OPTIONS'){
   const method=req.headers['access-control-request-method'],headers=(req.headers['access-control-request-headers']??'').toLowerCase().split(',').map(x=>x.trim()).filter(Boolean);
   let allowed=false;try{allowed=supported.has(adapter.prepareRequest('seamer',method,req.url,[...(['GET','HEAD'].includes(method)?[]:[['content-type','application/json']]),['x-leafseamer-contract',selection.contract_id],['x-leafseamer-contract-digest',selection.contract_digest]]).operation_id);}catch{}
   if(!allowed||req.headers.host!==site.host||!origins.has(req.headers.origin)||headers.some(h=>!['authorization','content-type','x-csrf-token','x-leafseamer-contract','x-leafseamer-contract-digest'].includes(h))){empty(res,403);return;}
   res.writeHead(204,{'Access-Control-Allow-Origin':req.headers.origin,'Access-Control-Allow-Methods':method,'Access-Control-Allow-Headers':headers.join(', '),Vary:'Origin','Cache-Control':'no-store'});res.end();return;
  }
  void serve(req,res).catch(error=>{observe(error);empty(res,503);});
 });
 server.maxConnections=32;server.maxRequestsPerSocket=1;server.on('clientError',(_error,socket)=>socket.destroy());
 try{await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(port,host,resolve);});}
 catch(error){server.close();throw error;}
 origin='https://'+(host==='::1'?'[::1]':host)+':'+server.address().port;site=new URL(origin);origins.add(origin);let stopping;
 return Object.freeze({origin,close(){return stopping??=(async()=>{closed=true;await new Promise((resolve,reject)=>{server.close(error=>error?reject(error):resolve());server.closeAllConnections();});})();}});
}
