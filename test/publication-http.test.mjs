import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import https from 'node:https';
import path from 'node:path';
import {pathToFileURL} from 'node:url';
import {peerCertificates} from '../../leafseamer-controller/test/fixtures/peer-certificates.mjs';
import {setup,counts,packageRoot} from './automation-publication-fixture.mjs';
const {startDraftHttp}=await import(pathToFileURL(path.join(packageRoot,'dist/draft-http.mjs')));
const {createClient,contractSelection:selection}=await import(pathToFileURL(path.join(packageRoot,'dist/sdk/index.mjs')));

test('实际 HTTPS 发布、历史与查原使用文件库，CSRF 绑定本监听、context、令牌和 Origin',async t=>{
 const f=setup(t,{optional:true}),certificates=peerCertificates(),page='https://controller.invalid',otherPage='https://other.invalid',bootId=crypto.randomUUID();
 let contextSeq=0,confirmDenied=false;const contexts=new Map(),authorization={
  createRequestContext(_handle,body){const value={context_id:'context-'+ ++contextSeq,service_id:f.identity.service_id,issuer_id:'issuer',issuer_epoch:'epoch',principal_id:'person',audience:f.identity.service_id,operations:body.operations,context_epoch:f.identity.request_epoch,issued_at:'2026-11-12T00:00:00.000Z',expires_at:'2026-11-12T00:00:30.000Z',ttl_ms:30000,max_unique_requests:8,selection};contexts.set(value.context_id,value);return value;},
  getRequestContext(_handle,id){return contexts.get(id);},confirmDraftResult(){if(confirmDenied)throw Object.assign(new Error('denied'),{code:'local_authorization_denied'});}
 };
 const config={cert:certificates.leaves[0].cert,key:certificates.leaves[0].key,bindingId:crypto.randomUUID(),service:f.identity,bootId,receiver:{async prepare(){return null;}},authorization,drafts:f.store.drafts,publications:f.store.publications,allowedOrigins:[page,otherPage]};
 const app=await startDraftHttp(config);t.after(()=>app.close());
 let token='v1.'+crypto.randomUUID()+'.'+'A'.repeat(43),origin=page,csrf=null,lastHeaders=[];
 const client=createClient('seamer',selection,packet=>new Promise((resolve,reject)=>{
  const headers=Object.fromEntries([...packet.headers,['Authorization','Bearer '+token],...(origin?[['Origin',origin]]:[]),...(csrf?[['X-CSRF-Token',csrf]]:[])]);
  const req=https.request(new URL(packet.target,app.origin),{method:packet.method,headers,ca:certificates.ca,agent:false},res=>{const chunks=[];res.on('data',chunk=>chunks.push(chunk));res.on('error',reject);res.on('end',()=>{lastHeaders=Array.from({length:res.rawHeaders.length/2},(_,i)=>[res.rawHeaders[i*2],res.rawHeaders[i*2+1]]);resolve({status:String(res.statusCode),headers:lastHeaders,body:Buffer.concat(chunks)});});});req.on('error',reject);req.end(packet.body);
 }));
 const issue=()=>client.call('createRequestContext',{body:{audience:f.identity.service_id,selection,operations:['publishDefinition'],ttl_ms:30000,max_unique_requests:8}});
 origin=null;assert.equal((await issue()).status,'403');origin=page;
 const ctx=(await issue()).body.data,ownCsrf=lastHeaders.find(([name])=>name.toLowerCase()==='x-csrf-token')[1];assert.match(ownCsrf,/^[A-Za-z0-9_-]{43}$/);
 assert.match(lastHeaders.find(([name])=>name.toLowerCase()==='access-control-expose-headers')[1],/X-CSRF-Token/);
 const body={...f.request,context:{context_id:ctx.context_id,idempotency_key:'http-publish'}},publish=()=>client.call('publishDefinition',{path:{definition_id:f.draft.definition_id},body});
 assert.equal((await publish()).status,'403');csrf='A'.repeat(43);assert.equal((await publish()).status,'403');
 csrf=ownCsrf;origin=otherPage;assert.equal((await publish()).status,'403');origin=null;assert.equal((await publish()).status,'403');origin=page;
 const originalToken=token;token='v1.'+crypto.randomUUID()+'.'+'B'.repeat(43);assert.equal((await publish()).status,'403');token=originalToken;
 const originalContext=body.context.context_id;body.context.context_id='other-context';assert.equal((await publish()).status,'403');body.context.context_id=originalContext;
 confirmDenied=true;assert.equal((await publish()).status,'403');confirmDenied=false;
 // 已提交后的响应拒绝不能伪称回滚，随后仅查原。
 csrf=null;const recovered=(await client.call('lookupAutomationRequest',{body:{context_id:ctx.context_id,idempotency_key:'http-publish',operation_id:'publishDefinition'}})).body.result.value;
 assert.equal(recovered.definition_version,'1');const path={definition_id:f.draft.definition_id,definition_version:'1'};
 const artifact=await client.call('getPublishedArtifact',{path});assert.equal(artifact.status,'200');assert.equal(artifact.body.manifest_digest,recovered.artifact_ref.digest);
 assert.equal((await client.call('getCompiledProgram',{path})).body.definition_version,'1');const proof=artifact.body.proof_refs[0];
 assert.equal((await client.call('getIndependenceProof',{path:{definition_id:f.draft.definition_id,proof_id:proof.id},query:{revision:proof.revision}})).body.verdict,'independent');
 const preflight=await new Promise((resolve,reject)=>{const req=https.request(new URL('/api/v1/definitions/'+f.draft.definition_id+'/publish',app.origin),{method:'OPTIONS',ca:certificates.ca,agent:false,headers:{Origin:page,'Access-Control-Request-Method':'POST','Access-Control-Request-Headers':'authorization,content-type,x-csrf-token,x-leafseamer-contract,x-leafseamer-contract-digest'}},res=>{res.resume();res.on('end',()=>resolve(res.statusCode));});req.on('error',reject);req.end();});assert.equal(preflight,204);
 await app.close();const restarted=await startDraftHttp(config);t.after(()=>restarted.close());
 const request=await new Promise((resolve,reject)=>{const req=https.request(new URL('/api/v1/definitions/'+f.draft.definition_id+'/publish',restarted.origin),{method:'POST',ca:certificates.ca,agent:false,headers:{Origin:page,Authorization:'Bearer '+token,'X-CSRF-Token':ownCsrf,'Content-Type':'application/json','Content-Length':Buffer.byteLength(JSON.stringify(body)),'X-LeafSeamer-Contract':selection.contract_id,'X-LeafSeamer-Contract-Digest':selection.contract_digest}},res=>{res.resume();res.on('end',()=>resolve(res.statusCode));});req.on('error',reject);req.end(JSON.stringify(body));});assert.equal(request,403);
 await restarted.close();f.store.close();assert.equal(counts(f.dir).definition_version_reservation,1);assert.equal(counts(f.dir).definition_publication,1);
});
