import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import {digest} from '../dist/runtime/auth-clock-value.mjs';
const id=()=>crypto.randomUUID();
export async function bindingHttpAcceptance({createClient,selection,send,token,origin,serviceId,definitionId,source}){
 let csrf=null,withCsrf=true,withOrigin=true;
 const client=createClient('seamer',selection,async packet=>{
  const response=await send({...packet,headers:[...packet.headers,['authorization','Bearer '+token],...(withOrigin?[['origin',origin]]:[]),...(withCsrf&&csrf?[['x-csrf-token',csrf]]:[])]});
  const header=response.headers.find(([name])=>name.toLowerCase()==='x-csrf-token');if(header)csrf=header[1];return response;
 });
 const context=async(max=1)=>{const r=await client.call('createRequestContext',{body:{audience:serviceId,selection,operations:['approveDefinitionBindings','revokeBindingApproval','updateDefinitionMetadata'],ttl_ms:60000,max_unique_requests:max}});assert.equal(r.status,'201');assert.ok(csrf);return {context_id:r.body.data.context_id};};
 const path={definition_id:definitionId},candidate=await client.call('queryBindingCandidate',{path,body:source});assert.equal(candidate.status,'200');
 const body={context:await context(),source:{...source,expected_registration_ref:candidate.body.bundle.registration_ref},request_id:id(),approval_id:id(),expected_bundle_digest:candidate.body.bundle_digest};
 withCsrf=false;assert.equal((await client.call('approveDefinitionBindings',{path,body})).status,'403');withCsrf=true;
 withOrigin=false;assert.equal((await client.call('approveDefinitionBindings',{path,body})).status,'403');withOrigin=true;
 const first=await client.call('approveDefinitionBindings',{path,body});assert.equal(first.status,'201');assert.equal(first.body.state,'approved');assert.deepEqual(first.body.bundle,candidate.body.bundle);
 assert.deepEqual((await client.call('approveDefinitionBindings',{path,body})).body,first.body);
 assert.equal((await client.call('approveDefinitionBindings',{path,body:{...body,expected_bundle_digest:digest('changed')}})).status,'409');
 assert.equal((await client.call('approveDefinitionBindings',{path,body:{...body,request_id:id(),approval_id:id()}})).status,'429');
 assert.equal((await client.call('updateDefinitionMetadata',{path,body:{context:{...body.context,idempotency_key:id()},expected_metadata_revision:'1',metadata:{name:'quota-check',description:'',layout:{}}}})).status,'429');
 const selected={...path,approval_id:body.approval_id},revocation={context:body.context,request_id:id(),expected_revision:'1'};
 assert.equal((await client.call('revokeBindingApproval',{path:selected,body:revocation})).status,'429');
 assert.equal((await client.call('getBindingApproval',{path:selected})).body.state,'approved');
 revocation.context=await context();assert.equal((await client.call('revokeBindingApproval',{path:{...selected,definition_id:id()},body:revocation})).status,'404');
 const revoked=await client.call('revokeBindingApproval',{path:selected,body:revocation});assert.equal(revoked.status,'200');assert.equal(revoked.body.state,'revoked');
 assert.deepEqual((await client.call('revokeBindingApproval',{path:selected,body:revocation})).body,revoked.body);
 const request={request_id:body.request_id,approval_id:body.approval_id,definition_id:definitionId,expected_bundle_digest:body.expected_bundle_digest};
 const lookup={request_id:request.request_id,request_digest:digest(request)};
 assert.deepEqual((await client.call('lookupBindingApproval',{path,body:lookup})).body.result,first.body);
 assert.equal((await client.call('getBindingApproval',{path:selected})).body.state,'revoked');
 assert.equal((await client.call('lookupBindingApproval',{path,body:{...lookup,request_id:id()}})).body.result,null);
 assert.equal((await client.call('lookupBindingApproval',{path,body:{...lookup,request_digest:digest('wrong')}})).status,'409');
}
