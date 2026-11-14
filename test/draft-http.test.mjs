import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import https from 'node:https';
import {startDraftHttp} from '../dist/draft-http.mjs';
import {createClient,contractSelection} from '../dist/sdk/index.mjs';
import {peerCertificates} from '../../leafseamer-controller/test/fixtures/peer-certificates.mjs';
import {source,digest} from './automation-program-fixture.mjs';
const id=()=>crypto.randomUUID();

test('HTTPS响应释放前撤权不交付成功正文；已提交写不能被错误响应伪称为回滚',async t=>{
 const certificates=peerCertificates(),service={service_id:id(),database_instance_id:id()},context={context_id:id(),idempotency_key:id()},scope={authority_service_id:service.service_id,database_instance_id:service.database_instance_id};
 const content={program:source().program,selections:[],policy_ref:{kind:'policy',scope,object_id:'policy',revision:'1',digest:digest({})}},metadata={name:'受保护正文',description:'',layout:{}};
 const record={scope,definition_id:id(),revision:'1',content,content_digest:digest(content),selection_digest:digest([]),metadata_revision:'1',metadata};
 // 此项只验证HTTP释放边界；真实身份、SQLite与撤权由双进程测试验证。
 let denied=false,writes=0,confirmations=0;
 const app=await startDraftHttp({cert:certificates.leaves[0].cert,key:certificates.leaves[0].key,bindingId:id(),service,bootId:id(),receiver:{async prepare(){return {}; }},
  authorization:{confirmDraftResult(){confirmations++;if(denied)throw Object.assign(new Error('denied'),{code:'local_authorization_denied'});}},
  drafts:{get(){return record;},create(){writes++;denied=true;return record;}}});t.after(()=>app.close());
 const token='v1.'+id()+'.'+'A'.repeat(43),client=createClient('seamer',contractSelection,packet=>new Promise((resolve,reject)=>{
  const request=https.request(new URL(packet.target,app.origin),{method:packet.method,headers:Object.fromEntries([...packet.headers,['authorization','Bearer '+token]]),ca:certificates.ca,agent:false},response=>{
   const chunks=[];response.on('data',chunk=>chunks.push(chunk));response.on('error',reject);response.on('end',()=>resolve({status:String(response.statusCode),headers:Array.from({length:response.rawHeaders.length/2},(_,i)=>[response.rawHeaders[2*i],response.rawHeaders[2*i+1]]),body:Buffer.concat(chunks)}));
  });request.on('error',reject);request.end(packet.body);
 }));
 assert.deepEqual((await client.call('getDraft',{path:{definition_id:record.definition_id}})).body,record);
 const preflight=await new Promise((resolve,reject)=>{
  const request=https.request(new URL('/api/v1/definitions/'+record.definition_id+'/draft',app.origin),{method:'OPTIONS',ca:certificates.ca,agent:false,
   headers:{origin:app.origin,'access-control-request-method':'GET','access-control-request-headers':'authorization, x-leafseamer-contract, x-leafseamer-contract-digest'}},response=>{
   response.resume();response.on('end',()=>resolve({status:response.statusCode,origin:response.headers['access-control-allow-origin'],method:response.headers['access-control-allow-methods']}));
  });request.on('error',reject);request.end();
 });assert.deepEqual(preflight,{status:204,origin:app.origin,method:'GET'});
 const result=await client.call('createDefinition',{body:{context,content,metadata}});assert.equal(result.status,'403');assert.equal(result.body.code,'PERMISSION_DENIED');
 assert.equal(JSON.stringify(result.body).includes(metadata.name),false);assert.equal(writes,1);assert.equal(confirmations,2);
 assert.strictEqual(app.close(),app.close());await app.close();
});
