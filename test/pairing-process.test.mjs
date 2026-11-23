import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import net from 'node:net';
import https from 'node:https';
import path from 'node:path';
import fs from 'node:fs';
import os from 'node:os';
import {spawn,spawnSync} from 'node:child_process';
import {once} from 'node:events';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {peerCertificates} from '../../leafseamer-controller/test/fixtures/peer-certificates.mjs';
import {digest} from '../dist/runtime/auth-clock-value.mjs';
import {source} from './automation-program-fixture.mjs';
import {bindingHttpAcceptance} from './binding-http-acceptance.mjs';
const packageRoot=process.env.LEAFSEAMER_PACKAGE_UNDER_TEST??fileURLToPath(new URL('../',import.meta.url));assert.ok(path.isAbsolute(packageRoot));
const {createBrowserSessionClient,createClient,contractSelection}=await import(pathToFileURL(path.join(packageRoot,'dist/sdk/index.mjs')));
const id=()=>crypto.randomUUID();
function requestHttps(origin,ca,{method='GET',target='/',headers=[],body=Buffer.alloc(0)}){return new Promise((resolve,reject)=>{
 const req=https.request(new URL(target,origin),{method,headers:Object.fromEntries(headers),ca,rejectUnauthorized:true,minVersion:'TLSv1.3',agent:false},res=>{
  const chunks=[];res.on('data',chunk=>chunks.push(chunk));res.on('error',reject);res.on('end',()=>resolve({status:String(res.statusCode),headers:Array.from({length:res.rawHeaders.length/2},(_,i)=>[res.rawHeaders[i*2],res.rawHeaders[i*2+1]]),body:Buffer.concat(chunks)}));
 });req.on('error',reject);req.setTimeout(10000,()=>req.destroy(new Error('https_timeout')));req.end(body);
});}
async function refused(origin){
 const url=new URL(origin);
 await assert.rejects(new Promise((resolve,reject)=>{
  const socket=net.connect({host:url.hostname,port:Number(url.port)});
  socket.once('connect',()=>{socket.destroy();resolve();});socket.once('error',reject);
  socket.setTimeout(3000,()=>{socket.destroy();reject(new Error('port_check_timeout'));});
 }),{code:'ECONNREFUSED'});
}
function peer(t){
 const child=spawn(process.execPath,['--disallow-code-generation-from-strings',fileURLToPath(new URL('./pairing-process-fixture.mjs',import.meta.url))],{stdio:['ignore','ignore','pipe','ipc'],windowsHide:true}),pending=new Map();let stopped=false,stderr='';
 const exited=once(child,'exit');child.stderr.on('data',value=>{stderr=(stderr+value).slice(-8192);});
 child.on('message',message=>{if(message.diagnostic){t.diagnostic(JSON.stringify(message.diagnostic));return;}const item=pending.get(message.id);if(!item)return;pending.delete(message.id);clearTimeout(item.timer);message.ok?item.resolve(message.value):item.reject(Object.assign(new Error(message.error.message),{code:message.error.code}));});
 child.on('exit',code=>{stopped=true;for(const item of pending.values()){clearTimeout(item.timer);item.reject(new Error('fixture_exit:'+code+':'+stderr));}pending.clear();});
 child.on('error',error=>{for(const item of pending.values()){clearTimeout(item.timer);item.reject(error);}pending.clear();});
 function call(command,value){return new Promise((resolve,reject)=>{if(stopped){reject(new Error('fixture_stopped'));return;}const key=id(),timer=setTimeout(()=>{pending.delete(key);reject(new Error('fixture_timeout:'+command));},30000);pending.set(key,{resolve,reject,timer});child.send({id:key,command,value},error=>{if(error){clearTimeout(timer);pending.delete(key);reject(error);}});});}
 t.after(async()=>{if(!stopped){try{await call('shutdown');}catch{child.kill();}}await exited;});
 return {call,exited,child};
}

for(const mode of ['drafts','management','revocation','automatic','delivery-restart','controller-app','bindings'])test(mode==='bindings'?'真实跨进程人员身份与TLS注册读取形成Seamer本地绑定批准，变化及撤销拒绝':mode==='drafts'?'两个独立进程完成真实身份、公共草稿与撤权查原；首次配对管理为合成来源':mode==='management'?'两个独立进程将配对管理切换到真实人员，本地交集允许撤销当前身份核验配对':mode==='automatic'?'注销后自动交付会话拒绝并保存回执':mode==='delivery-restart'?'Controller新进程从持久outbox恢复并查回Seamer原回执':mode==='controller-app'?'Controller正式启动器装配HTTPS人员令牌、内部核验与自动拒绝并统一关闭':'真实会话撤销经双向TLS持久拒绝旧身份并查回原回执；新进程仍拒绝', {timeout:90000},async t=>{
 const certificates=peerCertificates(),a=peer(t),b=peer(t),outsider=mode==='bindings'?peer(t):null,[ac,bc,otherCert]=certificates.leaves;
 const first=await a.call('initialize',{role:'controller',material:ac,ca:certificates.ca,automaticDenials:mode==='automatic',productController:mode==='controller-app',bindingRead:mode==='bindings',runTargets:mode==='bindings'});
 const issuer={service_id:first.identity.service_id,issuer_id:first.identity.issuer_id,issuer_epoch:first.identity.issuer_epoch};
 const second=await b.call('initialize',{role:'seamer',material:bc,ca:certificates.ca,issuer,principal_id:first.principal_id,allowedOrigins:[first.apiOrigin],bindingRead:mode==='bindings',runTargets:mode==='bindings'});
 const third=outsider?await outsider.call('initialize',{role:'controller',material:otherCert,ca:certificates.ca}):null;
 assert.notEqual(first.pid,second.pid);assert.notEqual(first.pid,process.pid);assert.notEqual(second.pid,process.pid);
 assert.equal((await fetch(second.publicOrigin+'/health/live')).status,200);assert.equal((await fetch(second.publicOrigin+'/health/ready')).status,503);
 assert.equal((await fetch(second.publicOrigin+'/internal/auth/introspect',{method:'POST'})).status,405);
 await a.call('bind',[{identity:second.identity,cert:bc.cert,origin:second.origin,server_name:'127.0.0.1',trust_bundle_digest:second.configuration.trust_bundle_digest}]);
 const describe=(peer,certificate)=>({identity:peer.identity,cert:certificate.cert,origin:peer.origin,server_name:'127.0.0.1',trust_bundle_digest:peer.configuration.trust_bundle_digest});
 await b.call('bind',[describe(first,ac),...(third?[describe(third,otherCert)]:[])]);
 if(outsider)await outsider.call('bind',[describe(second,bc)]);
 const bindingScope={format:'leafseamer.device-binding-scope/1',authority_service_id:first.identity.service_id,database_instance_id:first.identity.database_instance_id,device_ids:first.device_id?[first.device_id]:[]};
 const begin=BigInt(Date.now())*1000000n-1000000000n,parties=[first,second].map(p=>({...p.configuration,binding_id:p.bindingId??id(),permissions:mode==='bindings'&&p===second?['auth.introspect','auth.revoke','automation.bindings.read','automation.prepare','automation.prepare.read','automation.protect','clock.internal.read']:
  ['auth.introspect','auth.revoke',...(mode==='bindings'?['automation.prepare.read','automation.protect']:[]),'clock.internal.read'],object_scope_digest:mode==='bindings'&&p===second?digest(bindingScope):digest({devices:[]}),not_before_ns:String(begin),expires_ns:String(begin+120000000000n)}));
 const transcript={format:'leafseamer.peer-pairing/1',pair_attempt_id:id(),parties},ar=await a.call('stage',{request_id:id(),transcript}),br=await b.call('stage',{request_id:id(),transcript});
 assert.notEqual(ar.service_boot_id,br.service_boot_id);assert.notEqual(ar.runtime_domain_ref.clock_domain_id,br.runtime_domain_ref.clock_domain_id);
 assert.equal((await a.call('status',ar.binding_id)).state,'staged');assert.equal((await b.call('status',br.binding_id)).state,'staged');
 await a.call('prove',ar.binding_id);await b.call('prove',br.binding_id);
 const readyA=await a.call('get',ar.binding_id),readyB=await b.call('get',br.binding_id);assert.equal(readyA.state,'proved');assert.equal(readyB.state,'proved');
 await a.call('activate',{request_id:id(),binding_id:ar.binding_id,expected_revision:readyA.revision,transcript_digest:ar.transcript_digest});
 assert.equal((await a.call('status',ar.binding_id)).state,'proved');
 const activeB=await b.call('activate',{request_id:id(),binding_id:br.binding_id,expected_revision:readyB.revision,transcript_digest:br.transcript_digest});
 assert.equal((await a.call('status',ar.binding_id)).state,'activated');
 let outsiderBinding;
 if(outsider){
  const paired={format:transcript.format,pair_attempt_id:id(),parties:[third,second].map(p=>({...p.configuration,binding_id:id(),
   permissions:p===third?['automation.prepare.read']:[],object_scope_digest:digest({devices:[]}),not_before_ns:String(begin),expires_ns:String(begin+120000000000n)}))};
  const left=await outsider.call('stage',{request_id:id(),transcript:paired}),right=await b.call('stage',{request_id:id(),transcript:paired});
  await outsider.call('prove',left.binding_id);await b.call('prove',right.binding_id);
  for(const [endpoint,original]of [[outsider,left],[b,right]]){
   const ready=await endpoint.call('get',original.binding_id);
   await endpoint.call('activate',{request_id:id(),binding_id:original.binding_id,expected_revision:ready.revision,transcript_digest:original.transcript_digest});
  }
  outsiderBinding=left.binding_id;
 }
 if(mode==='automatic'){
  const limited={...transcript,pair_attempt_id:id(),parties:transcript.parties.map(p=>({...p,binding_id:id(),permissions:p.service_id===first.identity.service_id?p.permissions.filter(x=>x!=='auth.revoke'):p.permissions}))};
  const left=await a.call('stage',{request_id:id(),transcript:limited}),right=await b.call('stage',{request_id:id(),transcript:limited});
  await a.call('prove',left.binding_id);await b.call('prove',right.binding_id);
  for(const [peer,record]of [[a,left],[b,right]]){const ready=await peer.call('get',record.binding_id);await peer.call('activate',{request_id:id(),binding_id:record.binding_id,expected_revision:ready.revision,transcript_digest:ready.transcript_digest});}
 }
 const issued=await a.call('issueIdentity',ar.binding_id);let received=await b.call('prepareIdentity',{binding_id:br.binding_id,token:issued.token});
 assert.equal(received.identity.issuer_service_id,first.identity.service_id);
 assert.equal(received.identity.snapshot.issuer_id,issuer.issuer_id);assert.equal(received.identity.snapshot.issuer_epoch,issuer.issuer_epoch);
 assert.equal(received.identity.snapshot.audience,second.identity.service_id);assert.equal(received.identity.snapshot.principal_id,issued.principal_id);
 assert.equal(received.identity.snapshot.session_id,issued.session_id);assert.deepEqual(received.identity.snapshot.permissions,['auth.manage','auth.read','definition.edit','definition.publish','definition.read','request.create','request.read',...(mode==='bindings'?['run.approval.revoke','run.approve','run.manage','run.read']:[])]);
 assert.equal(received.identity.recent_authentication.expires_at,new Date(Number(BigInt(issued.recent_before_ns)/1000000n)).toISOString());
 assert.deepEqual(await b.call('currentIdentity',received.handle_id),received.identity);
 if(mode==='bindings'){
  const controllerPair=await a.call('get',ar.binding_id),principal=await a.call('createAutomationPrincipal');
  const seamerScope={authority_service_id:second.identity.service_id,database_instance_id:second.identity.database_instance_id};
  const approvalInput={request_id:id(),principal_id:principal.id,expected_revision:principal.revision,audiences:[first.identity.service_id],
   permissions:['automation.prepare'],device_ids:[first.device_id],not_before_ns:String(begin),expires_ns:String(begin+120000000000n),
   automation_origin:{seamer_scope:seamerScope,pairing_ref:{id:controllerPair.binding_id,revision:controllerPair.revision,digest:controllerPair.digest},device_scope:bindingScope}};
  await assert.rejects(a.call('approveAutomationPrincipal',{...approvalInput,permissions:['automation.prepare','control.acquire']}),{code:'service_identity_automation_permissions'});
  await assert.rejects(a.call('approveAutomationPrincipal',{...approvalInput,automation_origin:{...approvalInput.automation_origin,seamer_scope:{...seamerScope,database_instance_id:id()}}}),{code:'service_identity_automation_pairing'});
  const approvedService=await a.call('approveAutomationPrincipal',approvalInput);
  assert.deepEqual(await a.call('approveAutomationPrincipal',approvalInput),approvedService);
  const sourceRequest={principal:{issuer_id:principal.issuer_id,issuer_epoch:principal.issuer_epoch,principal_id:principal.id},approval_ref:approvedService.principal.approval_ref};
  const lookupSource=request=>b.call('readAutomationApproval',{binding_id:br.binding_id,request});
  const source=await lookupSource(sourceRequest);
  assert.deepEqual(source.approval,approvedService.approval);
  assert.deepEqual(source.issuer_scope,{authority_service_id:first.identity.service_id,database_instance_id:first.identity.database_instance_id});
  await assert.rejects(lookupSource({...sourceRequest,approval_ref:{...sourceRequest.approval_ref,digest:digest('other')}}),{code:'peer_pairing_response'});
  let identity=received.handle_id;const refresh=async()=>{identity=(await b.call('prepareIdentity',{binding_id:br.binding_id,token:issued.token})).handle_id;return identity;};
  await refresh();
  const grant=await b.call('localGrant',{handle_id:identity,principal_id:issued.principal_id});
  await b.call('replaceLocalGrant',{handle_id:identity,request:{request_id:id(),principal_id:issued.principal_id,principal_kind:'human',expected_revision:grant.revision,
   permissions:[...grant.permissions,'definition.edit','definition.publish','run.approve','run.manage','run.read'],definition_scope:grant.definition_scope,not_before_ns:grant.not_before_ns,expires_ns:grant.expires_ns}});
  await refresh();const context=await b.call('createDraftContext',identity),draft=await b.call('createDraft',{handle_id:identity,context_id:context.context_id,key:id()});
  const query={binding_id:br.binding_id,scope:bindingScope,device_id:first.device_id,expected_registration_ref:null};
  let prepared=await b.call('prepareBindings',query);const request={request_id:id(),approval_id:id(),definition_id:draft.definition_id,expected_bundle_digest:prepared.value.bundle_digest};
  const approved=await b.call('approveBindings',{identity,handle:prepared.handle_id,request});assert.equal(approved.state,'approved');assert.equal(approved.purpose,'definition_compile');
  assert.deepEqual(approved.bundle,prepared.value.reply.bundle);const approvalRef={id:approved.approval_id,revision:approved.revision,digest:digest(approved)};
  assert.deepEqual(await b.call('approveBindings',{identity,handle:prepared.handle_id,request}),approved);
  assert.deepEqual(await b.call('checkBindingApproval',{identity,approval_ref:approvalRef,definition_id:draft.definition_id,handle:prepared.handle_id}),approved);
  await refresh();
  const run=await b.call('createBoundRun',{identity,approval_id:approved.approval_id,draft_revision:draft.revision});
  assert.equal(run.lifecycle,'disarmed');assert.equal(run.dispatch_eligible,false);
  const deviceTargets=()=>b.call('runDeviceTargets',{identity,run_id:run.run_id});
  const resolved=await deviceTargets(),registration=approved.bundle.registration;
  assert.equal(resolved.run_id,run.run_id);assert.match(resolved.dependency_digest,/^sha256:[0-9a-f]{64}$/);
  assert.deepEqual(resolved.targets,[{controller_scope:{authority_service_id:first.identity.service_id,database_instance_id:first.identity.database_instance_id},
   device_actions:[{device_id:first.device_id,action_id:registration.capability_id,capability_version:registration.capability_version}]}]);
  assert.equal(resolved.evidence.length,1);assert.deepEqual(resolved.evidence[0].approval_ref,approvalRef);assert.deepEqual(resolved.evidence[0].registration_ref,approved.registration_ref);
  await refresh();
  const proposalContext=await b.call('createRunApprovalContext',identity),controllerScope=source.issuer_scope,deviceAction=resolved.targets[0].device_actions[0];
  const condition={...deviceAction,parameter_pointer:'/value',rule:{kind:'one_of',values:[13,12]}};
  const proposalRequest={context:{context_id:proposalContext.context_id,idempotency_key:id()},artifact_ref:run.artifact_ref,expected_run_revision:run.revision,
   service_principal:sourceRequest.principal,source:{binding_id:br.binding_id,approval_ref:sourceRequest.approval_ref},
   validity:{not_before:new Date(Number(begin/1000000n)).toISOString(),not_after:new Date(Number(begin/1000000n)+60000).toISOString()},
   controller_constraints:[{controller_scope:controllerScope,constraints:[condition,{...condition,rule:{kind:'range',minimum:11,maximum:13}}]}]};
  const createProposal=(request=proposalRequest)=>b.call('createRunApprovalProposal',{identity,run_id:run.run_id,request});
  await assert.rejects(createProposal({...proposalRequest,expected_run_revision:'2'}),{code:'run_proposal_revision'});
  await assert.rejects(createProposal({...proposalRequest,controller_constraints:[{controller_scope:controllerScope,constraints:[{...condition,parameter_pointer:'/missing'}]}]}),{code:'run_constraint_pointer'});
  await assert.rejects(createProposal({...proposalRequest,controller_constraints:[{controller_scope:controllerScope,constraints:[
   {...condition,rule:{kind:'range',minimum:11,maximum:12}},{...condition,rule:{kind:'range',minimum:13,maximum:14}}]}]}),{code:'run_constraint_empty'});
  const proposal=await createProposal();
  assert.equal(proposal.proposal_ref.digest,digest(proposal.proposal));assert.equal(proposal.proposal.run_id,run.run_id);
  assert.equal(proposal.proposal.dependency_footprint_ref.digest,resolved.dependency_digest);
  assert.deepEqual(proposal.proposal.source_service_approval_ref,{kind:'service_approval',scope:controllerScope,object_id:sourceRequest.approval_ref.id,revision:'1',digest:sourceRequest.approval_ref.digest});
  assert.deepEqual(proposal.proposal.controller_targets[0].device_actions,[deviceAction]);
  assert.deepEqual(proposal.proposal.controller_targets[0].parameter_constraints.find(item=>item.rule.kind==='one_of').rule.values,[12,13]);
  const pairedRead={binding_id:ar.binding_id,proposal_ref:proposal.proposal_ref};
  assert.deepEqual(await a.call('readPeerRunApprovalProposal',pairedRead),proposal);
  await assert.rejects(a.call('readPeerRunApprovalProposal',{...pairedRead,proposal_ref:{...proposal.proposal_ref,digest:digest('different')}}),{code:'peer_pairing_response'});
  await assert.rejects(outsider.call('readPeerRunApprovalProposal',{...pairedRead,binding_id:outsiderBinding}),{code:'peer_pairing_response'});
  assert.equal((await fetch(second.publicOrigin+'/internal/automation/approval-proposals/'+proposal.proposal.proposal_id)).status,404);
  const endorsementId=id(),endorsement={scope:controllerScope,endorsement_id:endorsementId,revision:'1',proposal_ref:proposal.proposal_ref,origin_scope:proposal.proposal.scope,
   run_id:run.run_id,service_principal:sourceRequest.principal,controller_target_digest:digest(proposal.proposal.controller_targets[0]),
   device_actions:proposal.proposal.controller_targets[0].device_actions,parameter_constraints:proposal.proposal.controller_targets[0].parameter_constraints,
   validity:proposal.proposal.validity,endorsed_by:{issuer_id:issuer.issuer_id,issuer_epoch:issuer.issuer_epoch,principal_id:first.principal_id},
   authorization_basis_refs:[{kind:'run_endorsement_policy',scope:controllerScope,object_id:id(),revision:'1',digest:digest('test policy')},
    proposal.proposal.source_service_approval_ref],created_authentication_ref:{kind:'authentication',scope:controllerScope,object_id:endorsementId,revision:'1',digest:digest('test authentication')}};
  const endorsementRef={kind:'run_approval_endorsement',scope:controllerScope,object_id:endorsement.endorsement_id,revision:'1',digest:digest(endorsement)};
  const wire={endorsement,object_ref:endorsementRef,state:'active',status_revision:'1',current_eligibility:'eligible',
   eligible_before_ns:String(BigInt(Date.parse(endorsement.validity.not_after))*1000000n)};
  const endorsementRead={binding_id:br.binding_id,endorsement_ref:endorsementRef,proposal};
  await a.call('setWireEndorsement',wire);
  assert.deepEqual(await b.call('preparePeerEndorsement',endorsementRead),wire);
  await assert.rejects(b.call('readPeerEndorsement',{...endorsementRead,endorsement_ref:{...endorsementRef,digest:digest('different')}}),{code:'peer_pairing_response'});
  const otherProposal={...proposal.proposal,proposal_id:id()};
  await assert.rejects(b.call('preparePeerEndorsement',{...endorsementRead,proposal:{proposal:otherProposal,
   proposal_ref:{...proposal.proposal_ref,object_id:otherProposal.proposal_id,digest:digest(otherProposal)}}}),{code:'run_endorsement_proposal'});
  const blocked={...wire,current_eligibility:'blocked',eligible_before_ns:null};
  await a.call('setWireEndorsement',blocked);
  assert.deepEqual(await b.call('readPeerEndorsement',endorsementRead),blocked);
  await assert.rejects(b.call('preparePeerEndorsement',endorsementRead),{code:'run_endorsement_ineligible'});
  const revokedEndorsement={...blocked,state:'revoking',status_revision:'2',revocation_operation_ref:{kind:'run_approval_operation',scope:controllerScope,
   object_id:id(),revision:'1',digest:digest('test endorsement revocation')}};
  await a.call('setWireEndorsement',revokedEndorsement);
  assert.deepEqual(await b.call('readPeerEndorsement',endorsementRead),revokedEndorsement);
  await assert.rejects(b.call('preparePeerEndorsement',endorsementRead),{code:'run_endorsement_ineligible'});
  function wireFor(saved){
   const endorsement_id=id(),target=saved.proposal.controller_targets[0];
   const original={...endorsement,endorsement_id,proposal_ref:saved.proposal_ref,run_id:saved.proposal.run_id,
    controller_target_digest:digest(target),device_actions:target.device_actions,parameter_constraints:target.parameter_constraints,
    created_authentication_ref:{...endorsement.created_authentication_ref,object_id:endorsement_id}};
   return {...wire,endorsement:original,object_ref:{...endorsementRef,object_id:endorsement_id,digest:digest(original)}};
  }
  function noticeFor(target,hash=digest('test reversal')){
   return {kind:'run_approval_endorsement_revoked',origin_scope:controllerScope,target_scope:target.endorsement.origin_scope,
    proposal_ref:target.endorsement.proposal_ref,endorsement_ref:target.object_ref,deny_revision:'2',
    revocation_operation_ref:{kind:'run_approval_operation',scope:controllerScope,object_id:id(),revision:'1',digest:hash}};
  }
  const sendRevocation=notice=>a.call('deliverPeerEndorsementRevocation',{binding_id:ar.binding_id,notice});
  const setNotice=async(target,notice)=>a.call('setWireEndorsement',{...target,state:'revoking',status_revision:'2',
   revocation_operation_ref:notice.revocation_operation_ref,current_eligibility:'blocked',eligible_before_ns:null});
  await refresh();
  const deniedContext=await b.call('createRunApprovalContext',identity);
  const deniedProposal=await createProposal({...proposalRequest,context:{context_id:deniedContext.context_id,idempotency_key:id()}});
  const deniedWire=wireFor(deniedProposal),deniedNotice=noticeFor(deniedWire);
  await a.call('setWireEndorsement',deniedWire);
  await assert.rejects(sendRevocation(deniedNotice),{code:'peer_pairing_response'});
  await setNotice(deniedWire,deniedNotice);
  const deniedReceipt=await sendRevocation(deniedNotice);assert.equal(deniedReceipt.identity_state,'denied');assert.equal(deniedReceipt.protection_operation_ref,null);
  await a.call('setWireEndorsement',deniedWire);
  assert.deepEqual(await sendRevocation(deniedNotice),deniedReceipt);
  await assert.rejects(sendRevocation({...deniedNotice,revocation_operation_ref:{...deniedNotice.revocation_operation_ref,digest:digest('different')}}),{code:'peer_pairing_response'});
  const deniedApproval=await b.call('createApproveRunContext',{handle_id:identity,max_unique_requests:1});
  await assert.rejects(b.call('approveRun',{identity,run_id:run.run_id,request:{context:{context_id:deniedApproval.context_id,idempotency_key:id()},
   expected_run_revision:run.revision,expected_prior_approval_ref:null,proposal_ref:deniedProposal.proposal_ref,controller_endorsement_refs:[deniedWire.object_ref]}}),{code:'run_approval_proposal_rejected'});
  await a.call('setWireEndorsement',revokedEndorsement);
  assert.deepEqual(await b.call('getRun',{identity,run_id:run.run_id}),run);
  assert.deepEqual(await createProposal(),proposal);
  await assert.rejects(createProposal({...proposalRequest,controller_constraints:[]}),{code:'run_proposal_conflict'});
  const lookup=async()=>b.call('lookupRunApprovalProposal',{identity,request:{run_id:run.run_id,...proposalRequest.context}});
  assert.deepEqual((await lookup()).result,proposal);
  await refresh();
  const approvalContext=await b.call('createApproveRunContext',{handle_id:identity,max_unique_requests:1}),approvalRequest={context:{context_id:approvalContext.context_id,idempotency_key:id()},
   expected_run_revision:run.revision,expected_prior_approval_ref:null,proposal_ref:proposal.proposal_ref,controller_endorsement_refs:[endorsementRef]};
  const approve=(request=approvalRequest)=>b.call('approveRun',{identity,run_id:run.run_id,request});
  const currentApproval=()=>b.call('getCurrentRunApproval',{identity,run_id:run.run_id});
  const inspectArm=revision=>b.call('inspectArmPrerequisites',{identity,run_id:run.run_id,revision});
  const lookupApproval=()=>b.call('lookupRunApproval',{identity,request:{run_id:run.run_id,...approvalRequest.context}});
  assert.equal(await currentApproval(),null);
  await assert.rejects(inspectArm('1'),{code:'run_preflight_approval'});
  await assert.rejects(approve({...approvalRequest,expected_run_revision:'2'}),{code:'run_approval_revision'});
  await assert.rejects(approve({...approvalRequest,controller_endorsement_refs:[]}),{code:'run_approval_targets'});
  await assert.rejects(approve(),{code:'run_endorsement_ineligible'});
  assert.deepEqual(await lookupApproval(),{request_digest:null,result:null});
  await a.call('setWireEndorsement',wire);
  const failureContext=await b.call('createApproveRunContext',{handle_id:identity,max_unique_requests:2});
  await assert.rejects(approve({...approvalRequest,context:{...approvalRequest.context,context_id:failureContext.context_id}}),{code:'run_approval_storage'});
  assert.equal(await currentApproval(),null);
  assert.deepEqual(await lookupApproval(),{request_digest:null,result:null});
  assert.deepEqual(await b.call('getRun',{identity,run_id:run.run_id}),run);
  const result=await approve();
  assert.equal(result.state,'applied');assert.deepEqual(result.targets,[]);
  await a.call('loseProtectionResponseFor',result.approval_ref.object_id);
  const after=await b.call('getRun',{identity,run_id:run.run_id});
  assert.deepEqual(after,{...run,revision:'2'});
  const current=await currentApproval();
  assert.deepEqual(current.object_ref,result.approval_ref);
  assert.deepEqual(current.approval.controller_endorsement_refs,[endorsementRef]);
  assert.equal(digest(current.approval),current.object_ref.digest);
  assert.equal(current.current_eligibility,'unavailable');
  await refresh();
  await assert.rejects(inspectArm('1'),{code:'run_preflight_revision'});
  const prerequisites=await inspectArm('2');
  assert.equal(prerequisites.sources.length,1);
  const [sourceCheck]=prerequisites.sources;
  assert.equal(sourceCheck.state,'missing');assert.equal(sourceCheck.source,null);
  assert.deepEqual(prerequisites,{scope:run.scope,run_id:run.run_id,run_revision:'2',artifact_ref:run.artifact_ref,
   dependency_digest:proposal.proposal.dependency_footprint_ref.digest,approval_ref:result.approval_ref,approval_status_revision:'1',
   targets:proposal.proposal.controller_targets.map(({controller_scope,device_actions})=>({controller_scope,device_actions})),sources:[sourceCheck],
   control_observations:[{controller_scope:controllerScope,endorsement_ref:wire.object_ref,devices:null}],can_arm:false});
  await a.call('setWireEndorsement',blocked);
  await assert.rejects(inspectArm('2'),{code:'run_endorsement_ineligible'});
  await a.call('setWireEndorsement',wire);
  const approvalRead={binding_id:ar.binding_id,approval_ref:result.approval_ref};
  const receivedApproval=await a.call('readPeerRunApproval',approvalRead);
  assert.deepEqual(receivedApproval.approval,current.approval);
  assert.deepEqual(receivedApproval.object_ref,result.approval_ref);
  assert.equal(receivedApproval.local_eligibility,'eligible');
  assert.equal(receivedApproval.eligible_before_ns,wire.eligible_before_ns);
  await assert.rejects(a.call('readPeerRunApproval',{...approvalRead,approval_ref:{...result.approval_ref,digest:digest('incorrect approval')}}),{code:'peer_pairing_response'});
  await assert.rejects(outsider.call('readPeerRunApproval',{...approvalRead,binding_id:outsiderBinding}),{code:'peer_pairing_response'});
  assert.equal((await fetch(second.publicOrigin+'/internal/automation/run-approvals/'+current.approval.approval_id+'/revisions/1')).status,404);
  assert.deepEqual(await b.call('getRunApprovalRevision',{identity,approval_id:current.approval.approval_id,revision:'1'}),current);
  assert.deepEqual(await b.call('getRunApprovalOperation',{identity,operation_id:result.operation_id}),result);
  assert.deepEqual(await approve(),result);
  await assert.rejects(approve({...approvalRequest,expected_run_revision:'2'}),{code:'run_approval_conflict'});
  assert.deepEqual((await lookupApproval()).result,result);
  await assert.rejects(approve({...approvalRequest,expected_run_revision:'2',context:{...approvalRequest.context,idempotency_key:id()}}),{code:'run_approval_transition'});
  await refresh();
  const secondaryRun=await b.call('createAnotherPublishedRun',{identity,run});
  const secondaryProposalContext=await b.call('createRunApprovalContext',identity);
  const secondaryProposal=await b.call('createRunApprovalProposal',{identity,run_id:secondaryRun.run_id,request:{...proposalRequest,
   context:{context_id:secondaryProposalContext.context_id,idempotency_key:id()},expected_run_revision:secondaryRun.revision}});
  const secondaryWire=wireFor(secondaryProposal);await a.call('setWireEndorsement',secondaryWire);
  const secondaryApprovalContext=await b.call('createApproveRunContext',{handle_id:identity,max_unique_requests:1});
  const secondaryResult=await b.call('approveRun',{identity,run_id:secondaryRun.run_id,request:{...approvalRequest,
   context:{context_id:secondaryApprovalContext.context_id,idempotency_key:id()},expected_run_revision:secondaryRun.revision,
   proposal_ref:secondaryProposal.proposal_ref,controller_endorsement_refs:[secondaryWire.object_ref]}});
  const failedReverse=noticeFor(secondaryWire,digest('reverse rejection rollback'));
  await setNotice(secondaryWire,failedReverse);
  await assert.rejects(sendRevocation(failedReverse),{code:'peer_pairing_response'});
  const secondaryRead={binding_id:ar.binding_id,approval_ref:secondaryResult.approval_ref};
  assert.equal((await a.call('readPeerRunApproval',secondaryRead)).local_eligibility,'eligible');
  const activeReverse=noticeFor(secondaryWire);await setNotice(secondaryWire,activeReverse);
  const protectionReceipt2=await sendRevocation(activeReverse);assert.equal(protectionReceipt2.identity_state,'denied');
  const secondaryProtection=await a.call('readPeerRunApproval',secondaryRead);
  assert.equal(secondaryProtection.local_eligibility,'blocked');assert.equal(secondaryProtection.state,'revoking');
  assert.deepEqual(secondaryProtection.revocation_operation_ref,protectionReceipt2.protection_operation_ref);
  assert.deepEqual(await sendRevocation(activeReverse),protectionReceipt2);
  let secondaryOperation;
  for(let i=0;i<40;i++){
   await refresh();secondaryOperation=await b.call('getRunApprovalOperation',{identity,operation_id:secondaryProtection.revocation_operation_ref.object_id});
   if(secondaryOperation.state==='applying')break;
   await new Promise(resolve=>setTimeout(resolve,100));
  }
  assert.equal(secondaryOperation.state,'applying');
  assert.deepEqual(secondaryOperation.approval_ref,secondaryResult.approval_ref);
  assert.equal(secondaryOperation.targets.length,1);
  await a.call('setWireEndorsement',wire);
  const revokeContext=max=>b.call('createRevokeRunApprovalContext',{handle_id:identity,max_unique_requests:max});
  await refresh();await assert.rejects(revokeContext(1),{code:'local_authorization_denied'});
  const approvalGrant=await b.call('localGrant',{handle_id:identity,principal_id:issued.principal_id});
  await b.call('replaceLocalGrant',{handle_id:identity,request:{request_id:id(),principal_id:issued.principal_id,principal_kind:'human',expected_revision:approvalGrant.revision,
   permissions:[...approvalGrant.permissions,'run.approval.revoke'],definition_scope:approvalGrant.definition_scope,not_before_ns:approvalGrant.not_before_ns,expires_ns:approvalGrant.expires_ns}});
  await refresh();
  const revocationContext=await revokeContext(1),failedRevocationContext=await revokeContext(2);
  const revocationRequest={context:{context_id:revocationContext.context_id,idempotency_key:id()},approval_ref:result.approval_ref,expected_status_revision:'1',reason:'取消本次运行授权'};
  const revokeApproval=(request=revocationRequest)=>b.call('revokeRunApproval',{identity,approval_id:current.approval.approval_id,request});
  const lookupRevocation=()=>b.call('lookupRunApprovalRevocation',{identity,request:{approval_id:current.approval.approval_id,...revocationRequest.context}});
  await assert.rejects(revokeApproval({...revocationRequest,expected_status_revision:'2'}),{code:'run_approval_revocation_conflict'});
  await assert.rejects(revokeApproval({...revocationRequest,context:{...revocationRequest.context,context_id:failedRevocationContext.context_id}}),{code:'run_approval_storage'});
  assert.deepEqual(await currentApproval(),current);
  assert.deepEqual(await lookupRevocation(),{request_digest:null,result:null});
  const reversal=await revokeApproval();
  assert.equal(reversal.state,'accepted');assert.deepEqual(reversal.targets,[{target_scope:controllerScope,purpose:'close_controller',state:'pending',receipt_ref:null}]);
  assert.deepEqual(await b.call('getRun',{identity,run_id:run.run_id}),{...after,revision:'3'});
  await assert.rejects(inspectArm('2'),{code:'run_preflight_revision'});
  await assert.rejects(inspectArm('3'),{code:'run_preflight_approval'});
  assert.deepEqual(await revokeApproval(),reversal);
  assert.deepEqual((await lookupRevocation()).result,reversal);
  assert.deepEqual(await b.call('getRunApprovalOperation',{identity,operation_id:reversal.operation_id}),reversal);
  const revokedApproval=await a.call('readPeerRunApproval',approvalRead);
  assert.equal(revokedApproval.state,'revoking');assert.equal(revokedApproval.local_eligibility,'blocked');assert.equal(revokedApproval.eligible_before_ns,null);
  assert.equal(revokedApproval.revocation_operation_ref.object_id,reversal.operation_id);
  assert.deepEqual(revokedApproval.approval,current.approval);
  const delivery={operation_id:reversal.operation_id,target_scope:controllerScope};
  await assert.rejects(b.call('deliverConflictingApprovalProtection',delivery),{code:'peer_pairing_response'});
  const waitForDelivery=async field=>{
   let status;
   for(let count=0;count<100;count++){
    status=await b.call('approvalProtectionStatus');if(status[field]>0)return status;
    await new Promise(resolve=>setTimeout(resolve,100));
   }
   assert.fail(JSON.stringify(status));
  };
  await waitForDelivery('failed');
  assert.deepEqual(await b.call('getRunApprovalOperation',{identity,operation_id:reversal.operation_id}),reversal);
  await a.call('revokeAutomationPrincipal',{request_id:id(),principal_id:principal.id,expected_revision:approvedService.principal.revision});
  await assert.rejects(lookupSource(sourceRequest),{code:'peer_pairing_response'});
  assert.deepEqual(await a.call('approveAutomationPrincipal',approvalInput),approvedService);
  await refresh();
  const newProposalContext=await b.call('createRunApprovalContext',identity);
  await assert.rejects(createProposal({...proposalRequest,expected_run_revision:'3',context:{context_id:newProposalContext.context_id,idempotency_key:id()},controller_constraints:[]}),{code:'peer_pairing_response'});
  assert.deepEqual((await lookup()).result,proposal);
  assert.deepEqual(await b.call('getRunApprovalProposal',{identity,proposal_id:proposal.proposal.proposal_id}),proposal);
  assert.deepEqual(await a.call('readPeerRunApprovalProposal',pairedRead),proposal);
  const duplicate=await b.call('approveCurrentBindings',{identity,query,request:{...request,request_id:id(),approval_id:id()}});
  await assert.rejects(deviceTargets(),{code:'binding_approval_run_target_ambiguous'});
  await b.call('revokeBindingApproval',{identity,request:{request_id:id(),approval_id:duplicate.approval_id,expected_revision:'1'}});
  assert.deepEqual(await deviceTargets(),resolved);
  await a.call('bindingAvailability',false);
  await assert.rejects(b.call('prepareBindings',query),/peer_pairing_response/);
  await assert.rejects(deviceTargets(),/peer_pairing_response/);
  await a.call('bindingAvailability',true);
  assert.deepEqual(await deviceTargets(),resolved);
  await a.call('rotateBindings');await assert.rejects(b.call('prepareBindings',{...query,expected_registration_ref:approved.registration_ref}),/peer_pairing_response/);
  await assert.rejects(deviceTargets(),/peer_pairing_response/);
  prepared=await b.call('prepareBindings',query);await refresh();
  await assert.rejects(b.call('checkBindingApproval',{identity,approval_ref:approvalRef,definition_id:draft.definition_id,handle:prepared.handle_id}),/binding_approval_source_changed/);
  const revoke={request_id:id(),approval_id:approved.approval_id,expected_revision:'1'},revoked=await b.call('revokeBindingApproval',{identity,request:revoke});
  assert.equal(revoked.state,'revoked');assert.equal(revoked.revision,'2');assert.deepEqual(await b.call('revokeBindingApproval',{identity,request:revoke}),revoked);
  await assert.rejects(deviceTargets(),{code:'binding_approval_run_target_unapproved'});
  await assert.rejects(b.call('checkBindingApproval',{identity,approval_ref:approvalRef,definition_id:draft.definition_id,handle:prepared.handle_id}),/binding_approval_not_current/);
  assert.deepEqual(await b.call('getBindingApproval',{identity,approval_id:approved.approval_id}),revoked);
  const bindingLookup={request_id:request.request_id,request_digest:digest(request),definition_id:draft.definition_id};
  assert.deepEqual((await b.call('lookupBindingApproval',{identity,request:bindingLookup})).result,approved);
  await bindingHttpAcceptance({createClient,selection:contractSelection,send:packet=>requestHttps(second.apiOrigin,certificates.ca,packet),token:issued.token,origin:first.apiOrigin,serviceId:second.identity.service_id,definitionId:draft.definition_id,source:query});
  const readToken=await a.call('issueReadIdentity',ar.binding_id),reader=(await b.call('prepareIdentity',{binding_id:br.binding_id,token:readToken})).handle_id;
  assert.deepEqual(await b.call('getBindingApproval',{identity:reader,approval_id:approved.approval_id}),revoked);
  await assert.rejects(b.call('runDeviceTargets',{identity:reader,run_id:run.run_id}),{code:'local_authorization_denied'});
  await assert.rejects(b.call('lookupBindingApproval',{identity:reader,request:bindingLookup}),{code:'local_authorization_denied'});
  if(process.env.LEAFSEAMER_BINDING_GUI_PLAYWRIGHT){
   const directory=fs.mkdtempSync(path.join(os.tmpdir(),'leafseamer-gui-bindings-')),guiRoot=fileURLToPath(new URL('../../leafseamer-gui',import.meta.url));
   t.after(()=>{assert.equal(path.dirname(path.resolve(directory)),path.resolve(os.tmpdir()));fs.rmSync(directory,{recursive:true,force:true,maxRetries:5,retryDelay:50});});
   const config=path.join(directory,'site.json');fs.writeFileSync(config,JSON.stringify({site_label:'绑定测试工作台',service_id:first.identity.service_id,database_instance_id:first.identity.database_instance_id,
    seamer:{service_id:second.identity.service_id,database_instance_id:second.identity.database_instance_id,base_url:second.apiOrigin,service_label:'本机 Seamer',binding_source:{binding_id:br.binding_id,devices:[{device_id:first.device_id,label:'内存模拟器'}]}}}));
   const built=spawnSync(process.execPath,['scripts/build.mjs','--config',config],{cwd:guiRoot,encoding:'utf8',windowsHide:true,timeout:30000});assert.equal(built.status,0,built.stderr);
   await a.call('enableGui',path.join(guiRoot,'dist'));
   const browserConfig=path.join(directory,'browser.json'),caFile=path.join(directory,'test-ca.pem'),spki=cert=>crypto.createHash('sha256').update(new crypto.X509Certificate(cert).publicKey.export({type:'spki',format:'der'})).digest('base64');
   fs.writeFileSync(caFile,certificates.ca);fs.writeFileSync(browserConfig,JSON.stringify({origin:first.apiOrigin,seamerOrigin:second.apiOrigin,spkis:[spki(ac.cert),spki(bc.cert)],content:await b.call('publicationContent'),
    playwright:process.env.LEAFSEAMER_BINDING_GUI_PLAYWRIGHT,browser:process.env.LEAFSEAMER_GUI_BROWSER,output:process.env.LEAFSEAMER_GUI_OUTPUT}));
   const checked=spawnSync(process.execPath,[path.join(guiRoot,'test/seamer-binding-browser-check.mjs'),browserConfig],{encoding:'utf8',windowsHide:true,timeout:60000,env:{...process.env,NODE_EXTRA_CA_CERTS:caFile}});
   assert.equal(checked.status,0,checked.stderr+'\n'+checked.stdout);t.diagnostic(checked.stdout.trim());
  }
  await b.call('close');
  const unadopted=await b.call('checkDurableApprovalProtection',reversal.operation_id);assert.equal(unadopted.receipt,null);
  const originalReceipt=await a.call('restoreApprovalProtection');assert.ok(originalReceipt);
  assert.equal(await b.call('restartRunProtectionService'),'12');
  const resumedDelivery=await waitForDelivery('delivered');assert.equal(resumedDelivery.delivered,1);
  assert.equal(resumedDelivery.binding_revocation_scan_failed,0);
  const protectionReceipt=await b.call('deliverApprovalProtection',delivery);
  assert.deepEqual(protectionReceipt,originalReceipt);
  assert.equal(protectionReceipt.identity_state,'denied');assert.equal(protectionReceipt.protection_state,'pending');
  assert.deepEqual(protectionReceipt.approval_ref,result.approval_ref);
  assert.deepEqual(protectionReceipt.revocation_operation_ref,revokedApproval.revocation_operation_ref);
  await assert.rejects(b.call('deliverConflictingApprovalProtection',delivery),{code:'peer_pairing_response'});
  await refresh();
  const adopting=await b.call('getRunApprovalOperation',{identity,operation_id:reversal.operation_id});
  assert.deepEqual(adopting,{...reversal,state:'applying',targets:[{...reversal.targets[0],receipt_ref:{kind:'run_approval_rejection_receipt',
   scope:controllerScope,object_id:protectionReceipt.receipt_id,revision:'1',digest:digest(protectionReceipt)}}]});
  assert.deepEqual((await lookupRevocation()).result,reversal);
  assert.deepEqual(await revokeApproval(),reversal);
  await b.call('revoke',{request_id:id(),binding_id:br.binding_id,expected_revision:activeB.revision});
  await assert.rejects(a.call('readPeerRunApprovalProposal',pairedRead),{code:'peer_pairing_response'});
  await assert.rejects(a.call('readPeerRunApproval',approvalRead),{code:'peer_pairing_response'});
  await assert.rejects(b.call('readPeerEndorsement',endorsementRead),{code:'peer_pairing_state'});
  await b.call('close');
  const {notice:protection,receipt:storedReceipt}=await b.call('checkDurableApprovalProtection',reversal.operation_id);
  assert.deepEqual(protection.revocation_operation_ref,revokedApproval.revocation_operation_ref);
  assert.deepEqual(protection.approval_ref,result.approval_ref);
  assert.deepEqual(protection.endorsement_ref,endorsementRef);
  assert.equal(digest(protection),protectionReceipt.request_digest);
  assert.deepEqual(storedReceipt,protectionReceipt);
  return;
 }
 if(mode==='automatic'||mode==='delivery-restart'||mode==='controller-app'){
  if(mode==='controller-app')await assert.rejects(a.call('checkRunEndorsementPolicy',{binding_id:ar.binding_id,
   proposal_ref:{kind:'run_approval_proposal',scope:{authority_service_id:second.identity.service_id,database_instance_id:second.identity.database_instance_id},
    object_id:id(),revision:'1',digest:digest('no implicit grant.manage')}}),{code:'service_identity_management_denied'});
  const revoked=await a.call('logoutSession'),query={operation_id:revoked.operation_ref.id,binding_id:ar.binding_id};
  if(mode==='automatic'||mode==='controller-app'){
   let status;for(let i=0;i<100;i++){status=await a.call('sessionDeliveryStatus');if(status.delivered>0&&(mode==='controller-app'||status.failed>0))break;await new Promise(resolve=>setTimeout(resolve,20));}
   assert.equal(status.delivered,1,JSON.stringify(status));assert.ok(mode==='controller-app'?status.failed===0:status.failed>0,JSON.stringify(status));
   const receipt=await a.call('deliverSessionDenial',query);assert.equal(receipt.identity_state,'denied');assert.equal(receipt.protection_state,'pending');
  }else{
   const original=await a.call('deliverSessionDenial',{...query,transport_only:true});
   const resumed=await a.call('restartSessionDelivery',query.operation_id);assert.notEqual(resumed.pid,first.pid);assert.equal(resumed.status.delivered,1);assert.deepEqual(resumed.receipt,original);
   const again=await a.call('restartSessionDelivery',query.operation_id);assert.equal(again.status.delivered,0);assert.equal(again.status.acknowledged,1);assert.deepEqual(again.receipt,original);
  }
  await assert.rejects(b.call('currentIdentity',received.handle_id),/session_denial_revoked|peer_identity_expired/);
  await b.call('close');assert.equal((await b.call('closedWatermark',{session_id:issued.session_id})).session_denied,true);
  if(mode==='controller-app'){const stopped=await a.call('closeController');assert.equal(stopped.qualified,false);assert.equal(stopped.reason,'auth_clock_stopped');
   assert.equal((await a.call('sessionDeliveryStatus')).state,'stopped');assert.equal(a.child.exitCode,null);await refused(first.origin);await refused(first.apiOrigin);}
  return;
 }
 if(mode==='revocation'){
  assert.ok((await b.call('localGrant',{handle_id:received.handle_id,principal_id:issued.principal_id})).permissions.includes('auth.read'));
  const revoked=await a.call('logoutSession'),query={operation_id:revoked.operation_ref.id,binding_id:ar.binding_id};
  const notice=await a.call('prepareSessionDenial',query);
  for(const changed of [{issuer_id:id()},{target_scope:{...notice.target_scope,database_instance_id:id()}}])await assert.rejects(a.call('sendSessionDenialNotice',{binding_id:ar.binding_id,notice:{...notice,...changed}}),/peer_pairing_response/);
  const committed=await a.call('deliverSessionDenial',{...query,transport_only:true});
  assert.equal(committed.identity_state,'denied');assert.equal(committed.protection_state,'pending');
  await assert.rejects(b.call('localGrant',{handle_id:received.handle_id,principal_id:issued.principal_id}),/session_denial_revoked/);
  await assert.rejects(b.call('currentIdentity',received.handle_id),/peer_identity_handle/);
  const acknowledged=await a.call('deliverSessionDenial',query);assert.deepEqual(acknowledged,committed);
  assert.deepEqual(await a.call('deliverSessionDenial',query),committed);
  await assert.rejects(b.call('prepareIdentity',{binding_id:br.binding_id,token:issued.token}),/peer_identity_inactive/);
  await b.call('close');assert.equal((await b.call('closedWatermark',{session_id:issued.session_id})).session_denied,true);
  await assert.rejects(a.call('deliverSessionDenial',query));await b.call('shutdown');assert.equal((await b.exited)[0],0);return;
 }
 if(mode==='management'){
  await b.call('useIdentityManagement');await assert.rejects(b.call('get',br.binding_id));
  const readToken=await a.call('issueReadIdentity',ar.binding_id),readIdentity=await b.call('prepareIdentity',{binding_id:br.binding_id,token:readToken});
  await assert.rejects(b.call('currentIdentity',received.handle_id),/peer_identity_watermark/);
  received=await b.call('prepareIdentity',{binding_id:br.binding_id,token:issued.token});
  await assert.rejects(b.call('identityPairingGet',{handle_id:readIdentity.handle_id,binding_id:br.binding_id}),{code:'local_authorization_denied'});
  assert.equal((await b.call('identityPairingGet',{handle_id:received.handle_id,binding_id:br.binding_id})).state,'activated');
  const nextTranscript={...transcript,pair_attempt_id:id(),parties:transcript.parties.map(p=>({...p,binding_id:id()}))};
  const nextA=await a.call('stage',{request_id:id(),transcript:nextTranscript}),nextB=await b.call('identityPairingStage',{handle_id:received.handle_id,request:{request_id:id(),transcript:nextTranscript}});
  await a.call('prove',nextA.binding_id);await b.call('prove',nextB.binding_id);
  const nextReadyA=await a.call('get',nextA.binding_id),nextReadyB=await b.call('identityPairingGet',{handle_id:received.handle_id,binding_id:nextB.binding_id});
  await a.call('activate',{request_id:id(),binding_id:nextA.binding_id,expected_revision:nextReadyA.revision,transcript_digest:nextA.transcript_digest});
  assert.equal((await b.call('identityPairingActivate',{handle_id:received.handle_id,request:{request_id:id(),binding_id:nextB.binding_id,expected_revision:nextReadyB.revision,transcript_digest:nextB.transcript_digest}})).state,'activated');
  const rejectedTranscript={...transcript,pair_attempt_id:id(),parties:transcript.parties.map(p=>({...p,binding_id:id()}))};
  const rejected=await b.call('identityPairingStage',{handle_id:received.handle_id,request:{request_id:id(),transcript:rejectedTranscript}});
  assert.equal((await b.call('identityPairingReject',{handle_id:received.handle_id,request:{request_id:id(),binding_id:rejected.binding_id,expected_revision:rejected.revision}})).state,'rejected');
  const fresh=await b.call('prepareIdentity',{binding_id:br.binding_id,token:issued.token});
  const revoked=await b.call('identityPairingRevoke',{handle_id:fresh.handle_id,request:{request_id:id(),binding_id:br.binding_id,expected_revision:activeB.revision}});
  assert.equal(revoked.state,'revoked');assert.equal(BigInt(revoked.revision),BigInt(activeB.revision)+1n);
  await assert.rejects(b.call('currentIdentity',fresh.handle_id));await assert.rejects(b.call('identityPairingGet',{handle_id:fresh.handle_id,binding_id:br.binding_id}));
  await assert.rejects(b.call('prepareIdentity',{binding_id:br.binding_id,token:issued.token}));await assert.rejects(a.call('status',ar.binding_id));
  assert.equal((await a.call('status',nextA.binding_id)).state,'activated');
  await b.call('close');assert.equal(b.child.exitCode,null);await refused(second.publicOrigin);await refused(second.origin);await refused(second.apiOrigin);
  assert.ok(BigInt(await b.call('closedWatermark'))>=BigInt(fresh.identity.snapshot.issuer_revision));
  await b.call('shutdown');assert.equal((await b.exited)[0],0);return;
 }
 await assert.rejects(b.call('createDraftContext',received.handle_id),{code:'local_authorization_denied'});
 const local=await b.call('localGrant',{handle_id:received.handle_id,principal_id:issued.principal_id});
 const approval={request_id:id(),principal_id:issued.principal_id,principal_kind:'human',expected_revision:local.revision,
  permissions:[...local.permissions,'definition.edit','definition.publish'],definition_scope:local.definition_scope,not_before_ns:local.not_before_ns,expires_ns:local.expires_ns};
 await b.call('replaceLocalGrant',{handle_id:received.handle_id,request:approval});
 const context=await b.call('createDraftContext',received.handle_id),create={handle_id:received.handle_id,context_id:context.context_id,key:id()},draft=await b.call('createDraft',create);
 assert.equal(draft.metadata.name,'实际身份草稿');assert.deepEqual(await b.call('createDraft',create),draft);
 assert.deepEqual(await b.call('readDraft',{handle_id:received.handle_id,context_id:context.context_id,definition_id:draft.definition_id}),draft);
 if(process.env.LEAFSEAMER_GUI_PLAYWRIGHT){
  const directory=fs.mkdtempSync(path.join(os.tmpdir(),'leafseamer-gui-draft-')),guiRoot=fileURLToPath(new URL('../../leafseamer-gui',import.meta.url));
  t.after(()=>{assert.equal(path.dirname(path.resolve(directory)),path.resolve(os.tmpdir()));fs.rmSync(directory,{recursive:true,force:true,maxRetries:5,retryDelay:50});});
  const config=path.join(directory,'site.json');fs.writeFileSync(config,JSON.stringify({site_label:'草稿测试工作台',service_id:first.identity.service_id,database_instance_id:first.identity.database_instance_id,
   seamer:{service_id:second.identity.service_id,database_instance_id:second.identity.database_instance_id,base_url:second.apiOrigin,service_label:'本机 Seamer'}}));
  const built=spawnSync(process.execPath,['scripts/build.mjs','--config',config],{cwd:guiRoot,encoding:'utf8',windowsHide:true,timeout:30000});assert.equal(built.status,0,built.stderr);
  await a.call('enableGui',path.join(guiRoot,'dist'));
  const browserConfig=path.join(directory,'browser.json'),caFile=path.join(directory,'test-ca.pem'),spki=cert=>crypto.createHash('sha256').update(new crypto.X509Certificate(cert).publicKey.export({type:'spki',format:'der'})).digest('base64');
  fs.writeFileSync(caFile,certificates.ca);
  fs.writeFileSync(browserConfig,JSON.stringify({origin:first.apiOrigin,seamerOrigin:second.apiOrigin,spkis:[spki(ac.cert),spki(bc.cert)],content:await b.call('publicationContent'),
   playwright:process.env.LEAFSEAMER_GUI_PLAYWRIGHT,browser:process.env.LEAFSEAMER_GUI_BROWSER,output:process.env.LEAFSEAMER_GUI_OUTPUT}));
  const checked=spawnSync(process.execPath,[path.join(guiRoot,'test/seamer-draft-browser-check.mjs'),browserConfig],{encoding:'utf8',windowsHide:true,timeout:60000,env:{...process.env,NODE_EXTRA_CA_CERTS:caFile}});
  assert.equal(checked.status,0,checked.stderr+'\n'+checked.stdout);t.diagnostic(checked.stdout.trim());
 }
 // 实际HTTPS网络；这里只模拟浏览器Cookie容器及CORS可见头，不声称浏览器验收。
 let cookie=null;const browserCalls=[];
 const browser=createBrowserSessionClient({baseUrl:first.apiOrigin,fetchImpl:async(url,options)=>{
  const target=new URL(url),controller=target.origin===first.apiOrigin;assert.ok(controller||target.origin===second.apiOrigin);
  const headers=[...options.headers.entries()];browserCalls.push({controller,credentials:options.credentials,headers});
  if(controller&&cookie&&options.credentials==='same-origin')headers.push(['cookie',cookie]);
  if(!controller){assert.equal(options.credentials,'omit');assert.equal(options.headers.get('x-csrf-token'),null);assert.equal(options.headers.get('cookie'),null);}
  const response=await requestHttps(target.origin,certificates.ca,{method:options.method,target:target.pathname+target.search,headers,body:options.body});
  let visible=response.headers;
  if(controller){for(const [name,value]of visible)if(name.toLowerCase()==='set-cookie')cookie=value.split(';')[0];}
  else{
   const map=new Headers(visible);assert.equal(map.get('access-control-allow-origin'),first.apiOrigin);
   const exposed=(map.get('access-control-expose-headers')??'').toLowerCase().split(',').map(v=>v.trim());
   assert.ok(exposed.includes('x-leafseamer-contract'));assert.ok(exposed.includes('x-leafseamer-contract-digest'));
   visible=visible.filter(([name])=>['content-type','content-length','cache-control',...exposed].includes(name.toLowerCase()));
  }
  return new Response(response.body,{status:Number(response.status),headers:visible});
 }});
 assert.equal((await browser.login({login:'role.test',password:'Synthetic-role-password-123'})).status,'201');
 const remote=browser.createDraftClient({baseUrl:second.apiOrigin,audience:second.identity.service_id});
 const remoteToken=await remote.issueToken(['definition.edit','definition.publish','definition.read','request.create','request.read']);assert.equal(remoteToken.status,'201');assert.equal(remoteToken.body.data.audience,second.identity.service_id);
 const client={call:(operation,input)=>operation==='createRequestContext'?remote.createRequestContext(input.body):operation==='createDefinition'?remote.createDefinition(input.body):operation==='getDraft'?remote.getDraft(input.path.definition_id):remote[operation](input.path.definition_id,input.body)};
 const createdContext=await client.call('createRequestContext',{body:{audience:second.identity.service_id,selection:contractSelection,operations:['createDefinition','getDraft','updateDraft','updateDefinitionMetadata'],ttl_ms:60000,max_unique_requests:4}});
 assert.equal(createdContext.status,'201');const httpContext=createdContext.body.data;
 const httpCreate={context:{context_id:httpContext.context_id,idempotency_key:id()},content:{program:source().program,selections:[],policy_ref:{kind:'policy',scope:{authority_service_id:second.identity.service_id,database_instance_id:second.identity.database_instance_id},object_id:'policy',revision:'1',digest:digest({})}},metadata:{name:'HTTPS草稿',description:'',layout:{}}};
 const publicDraft=await client.call('createDefinition',{body:httpCreate});assert.equal(publicDraft.status,'201');
 assert.deepEqual((await client.call('createDefinition',{body:httpCreate})).body,publicDraft.body);
 const definition_id=publicDraft.body.definition_id;assert.deepEqual((await client.call('getDraft',{path:{definition_id}})).body,publicDraft.body);
 const metadata={context:{context_id:httpContext.context_id,idempotency_key:id()},expected_metadata_revision:'1',metadata:{...httpCreate.metadata,name:'HTTPS改名'}};
 assert.equal((await client.call('updateDefinitionMetadata',{path:{definition_id},body:metadata})).body.metadata_revision,'2');
 const updated=await client.call('updateDraft',{path:{definition_id},body:{context:{context_id:httpContext.context_id,idempotency_key:id()},expected_draft_revision:'1',content:httpCreate.content}});
 assert.equal(updated.body.revision,'2');assert.equal(updated.body.metadata.name,'HTTPS改名');
 assert.equal((await client.call('updateDraft',{path:{definition_id},body:{context:{context_id:httpContext.context_id,idempotency_key:id()},expected_draft_revision:'1',content:httpCreate.content}})).status,'409');
 const preflight=await requestHttps(second.apiOrigin,certificates.ca,{method:'OPTIONS',target:'/api/v1/definitions',headers:[['origin',second.apiOrigin],['access-control-request-method','POST'],['access-control-request-headers','authorization, content-type, x-leafseamer-contract, x-leafseamer-contract-digest']]});assert.equal(preflight.status,'204');
 const wireHeaders=[['x-leafseamer-contract',contractSelection.contract_id],['x-leafseamer-contract-digest',contractSelection.contract_digest]],bearer=['authorization','Bearer '+issued.token];
 assert.equal((await requestHttps(second.apiOrigin,certificates.ca,{target:'/api/v1/definitions/'+definition_id+'/draft',headers:wireHeaders})).status,'401');
 assert.equal((await requestHttps(second.apiOrigin,certificates.ca,{target:'/api/v1/definitions/'+definition_id+'/draft',headers:[...wireHeaders,bearer,['origin','https://unapproved.invalid']]})).status,'403');
 assert.equal((await requestHttps(second.apiOrigin,certificates.ca,{method:'POST',target:'/api/v1/definitions',headers:[...wireHeaders,bearer,['content-type','application/json'],['content-length','1']],body:Buffer.from('{')})).status,'400');
 assert.equal((await requestHttps(second.apiOrigin,certificates.ca,{method:'POST',target:'/api/v1/definitions',headers:[...wireHeaders,bearer,['content-type','application/json'],['content-length','16777217']]})).status,'413');
 assert.equal((await client.call('getDraft',{path:{definition_id:id()}})).status,'404');
 // 重签发的令牌不能重用旧写context，但可按当前request.read和对象读取权查原。
 assert.equal((await remote.issueToken(['request.read','definition.read'])).status,'201');
 const lookup={...httpCreate.context,operation_id:'createDefinition'},original=await remote.lookupAutomationRequest(lookup);
 assert.equal(original.status,'200');assert.deepEqual(original.body.result,{kind:'DraftRecord',value:publicDraft.body});
 assert.equal((await remote.lookupAutomationRequest({...lookup,idempotency_key:id()})).status,'404');
 const refreshed=await b.call('prepareIdentity',{binding_id:br.binding_id,token:issued.token});
 await b.call('replaceLocalGrant',{handle_id:refreshed.handle_id,request:{...approval,request_id:id(),expected_revision:'2',permissions:local.permissions}});
 await assert.rejects(b.call('createDraft',{...create,handle_id:refreshed.handle_id}),{code:'local_authorization_denied'});
 await assert.rejects(b.call('readDraft',{handle_id:refreshed.handle_id,context_id:context.context_id,definition_id:draft.definition_id}),{code:'local_authorization_denied'});
 assert.equal((await client.call('getDraft',{path:{definition_id}})).status,'403');
 assert.equal((await client.call('createDefinition',{body:httpCreate})).status,'403');
 assert.equal((await remote.lookupAutomationRequest(lookup)).status,'403');
 assert.ok(browserCalls.some(call=>call.controller));assert.ok(browserCalls.some(call=>!call.controller));
 browser.clearLocalState();const callsBefore=browserCalls.length;await assert.rejects(remote.getDraft(definition_id),{code:'session_credentials_missing'});assert.equal(browserCalls.length,callsBefore);
 assert.equal((await b.call('currentIdentity',refreshed.handle_id)).snapshot.principal_id,issued.principal_id);
 await b.call('revoke',{request_id:id(),binding_id:br.binding_id,expected_revision:activeB.revision});
 await assert.rejects(b.call('currentIdentity',refreshed.handle_id));
 await assert.rejects(b.call('prepareIdentity',{binding_id:br.binding_id,token:issued.token}));
 await assert.rejects(a.call('status',ar.binding_id),{code:'peer_pairing_response'});
 await b.call('close');assert.equal(b.child.exitCode,null);
 assert.ok(BigInt(await b.call('closedWatermark'))>=BigInt(refreshed.identity.snapshot.issuer_revision));
 await refused(second.publicOrigin);await refused(second.origin);await refused(second.apiOrigin);
 await b.call('close');
 await b.call('shutdown');const [code]=await b.exited;assert.equal(code,0);
});
