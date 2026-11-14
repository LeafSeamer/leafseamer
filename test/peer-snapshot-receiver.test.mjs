import test from 'node:test';import assert from 'node:assert/strict';import crypto from 'node:crypto';import fs from 'node:fs';import path from 'node:path';import {fileURLToPath,pathToFileURL} from 'node:url';
const root=process.env.LEAFSEAMER_SEAMER_PACKAGE_UNDER_TEST??fileURLToPath(new URL('../',import.meta.url)),load=name=>import(pathToFileURL(path.join(root,'dist',name)));
const {createPeerSnapshotReceiver}=await load('peer-snapshot-receiver.mjs'),{createSourceSnapshotContent}=await load('snapshot-content.mjs'),{digest}=await load('runtime/auth-clock-value.mjs'),{contractSelection:selection}=await load('sdk/selection.mjs');
const {sealSourceSnapshot}=await import(new URL('./dist/service-snapshot-content.mjs',import.meta.resolve('@leafseamer/controller/package.json')));
const uuid=()=>crypto.randomUUID(),hash='a'.repeat(64),ref=()=>({id:uuid(),revision:'1',digest:'sha256:'+hash}),clone=structuredClone;
function fixture({large=false}={}){
 const bytes=fs.readFileSync(path.join(root,'dist/sdk/contract/models.schema.json')),graphIdentity={id:JSON.parse(bytes).$id,digest:crypto.createHash('sha256').update(bytes).digest('hex')};
 const service=uuid(),database=uuid(),epoch=uuid(),common={format_version:'scope-registry/0.1.0-draft.1',authority_service_id:service,database_instance_id:database,entity_epoch:epoch,revision:'1'};
 const object=v=>({authority_service_id:service,database_instance_id:database,entity_kind:v.kind,entity_id:v.entity_id,entity_epoch:epoch,revision:'1',content_digest:digest(v).slice(7)});
 const profile={...common,kind:'projection_profile',entity_id:uuid(),authority_role:'controller',selection,owner_kinds:['service'],purposes:['canonical_trigger'],graph_digest:graphIdentity.digest,maximum_records:32,collections:[{entity_kind:'sources',permission:'status.read',view_schema_ref:{schema_id:graphIdentity.id,definition:'snapshot__SourceSnapshotView',schema_digest:graphIdentity.digest},relation_policies:['authorized_reference'],history_policies:['current']}]};
 const records=Array.from({length:large?32:2},(_,i)=>{const entity_id='source_'+String(i).padStart(2,'0'),source_key={authority_service_id:service,database_instance_id:database,registration_id:entity_id,source_stream_id:uuid(),source_epoch:epoch};
  return {entity_kind:'sources',entity_id,entity_epoch:epoch,entity_revision:'2',view:{source_key,binding_ref:{...object({kind:'source_binding',entity_id:uuid()}),content_digest:hash},state:'baseline_required',contiguous_sequence:'2',highest_seen_sequence:'2',baseline_ref:null,gaps:large?Array.from({length:64},(_,n)=>({gap_id:String(n).padStart(128,'g'),loss_extent:'unknown',first_missing:null,last_missing:null,resolution_state:'open'})):[]}};});
 const selector={...common,kind:'selector',entity_id:uuid(),profile_ref:object(profile),target_collection:'sources',mode:'entity_set',entity_ids:records.map(r=>r.entity_id),relation_policy:'authorized_reference',history_policy:'current',window:null},scope={scope_id:uuid(),scope_revision:'1',projection_profile_ref:object(profile),collections:[{entity_kind:'sources',selector_ref:object(selector),relation_policy:'authorized_reference',history_policy:'current'}]};
 const binding={subscription_id:uuid(),publisher_service_id:service,publisher_database_instance_id:database,stream_id:uuid(),stream_epoch:epoch,scope_digest:digest(scope).slice(7),authorization_revision:'1',connection_generation:'1'},owner={kind:'service',issuer_id:uuid(),issuer_epoch:uuid(),principal_id:uuid(),consumer_service_id:uuid(),consumer_database_instance_id:uuid(),consumer_id:'events',pairing_id:uuid()};
 const subscription={purpose:'canonical_trigger',state:'snapshot_ready',owner,scope,binding,selection},source_watermarks=records.map(r=>({source_key:r.view.source_key,binding_ref:r.view.binding_ref,baseline_sequence:r.view.contiguous_sequence,source_state_revision:r.entity_revision,baseline_ref:r.view.baseline_ref,gap_summary:r.view.gaps})),domain={clock_domain_id:'clock_'+hash,descriptor_digest:'sha256:'+hash};
 const sealed=sealSourceSnapshot({view:subscription,cut:{registration:{scope,profile,selector},records,source_watermarks},snapshotId:uuid(),domainRef:domain,createdAt:'2026-11-15T00:00:00Z',expiresAt:'2026-11-15T00:00:30Z'});subscription.snapshot_ref=sealed.snapshotRef;
 const lease={publisher_domain_ref:domain,observed_tick_ns:'100',valid_before_tick_ns:'5000000100',clock_policy_ref:ref(),clock_acquisition_ref:ref(),drift_ppb:1000000,monotonic_quantization_ns:'10'},boot=uuid();
 const context={format_version:'snapshot-context/1',challenge_id:uuid(),delivery_binding:binding,owner,publisher_service_boot_id:boot,snapshot_ref:sealed.snapshotRef,projection_profile:profile,selector,lease};
 const pages=sealed.pages.map(p=>({manifest_digest:sealed.manifest.manifest_digest,content_digest:p.content_digest,content:JSON.parse(p.body),delivery_binding:binding})),pair={binding_id:uuid(),revision:'1',digest:'sha256:'+hash,peer_service_id:service},connection=Object.freeze({}),state={tick:100n,connected:true,pair};
 const localService={service_id:owner.consumer_service_id,database_instance_id:owner.consumer_database_instance_id};pair.state='activated';pair.transcript={parties:[{service_id:service,database_instance_id:database,service_kind:'controller',binding_id:owner.pairing_id},{...localService,service_kind:'seamer',binding_id:pair.binding_id}]};
 const attached={ready:{channel:{owner,service_boot_id:boot}},attached:{binding}},client={checkAttached(h,id){assert.equal(h,connection);assert.equal(id,pair.binding_id);if(!state.connected)throw Error('closed');return clone(attached);},async get(){return {view:clone(subscription)};},async context(_id,_key,input){return {view:{...clone(context),challenge_id:input.challenge_id}};},async snapshot(){return {view:clone(sealed.manifest)};},async page(_id,_key,_snapshot,index){return {view:clone(pages[index])};}};
 client.checkAttachedInTransaction=client.checkAttached;
 const policy=ref(),clock={durationSample(){return {tick_ns:String(state.tick++),runtime_domain_ref:domain,valid_before_tick_ns:'4000000000',duration_bounds:{policy_ref:policy,drift_ppb:1000000,monotonic_quantization_ns:'10'}};}},pairings={currentPending:()=>clone(state.pair),requireActivatedInTransaction:()=>clone(state.pair)};
 return {service:localService,subscription,context,manifest:sealed.manifest,graphIdentity,pages,records,state,pair,connection,client,clock,pairings};
}
test('多页完整下载暂存保留baseline_required和全部gap，重复页不累加且未发送ACK',async()=>{
 const f=fixture({large:true});assert.ok(f.pages.length>1);const content=createSourceSnapshotContent(f);for(const p of f.pages){content.add(p);content.add(p);}const value=content.finish();assert.equal(value.records.length,32);assert.equal(value.records[0].view.gaps.length,64);assert.equal(value.records[0].view.state,'baseline_required');
 const receiver=createPeerSnapshotReceiver(f),handle=await receiver.prepare(f.pair.binding_id,f.connection);assert.equal(receiver.current(handle).projection.records.length,32);assert.deepEqual(receiver.currentInTransaction(handle).projection,value);receiver.release(handle);assert.throws(()=>receiver.current(handle),{code:'peer_snapshot_handle'});receiver.close();
});
test('缺页、错代、损坏摘要与不匹配水位均拒绝；外部对象变更不能改变已核输入',()=>{
 let f=fixture(),content=createSourceSnapshotContent(f);assert.throws(()=>content.finish(),{code:'snapshot_content_incomplete'});
 content=createSourceSnapshotContent(f);assert.throws(()=>content.add({...f.pages[0],delivery_binding:{...f.pages[0].delivery_binding,connection_generation:'2'}}),{code:'snapshot_content_page'});
 content=createSourceSnapshotContent(f);const broken=clone(f.pages[0]);broken.content.records[0].view.contiguous_sequence='3';assert.throws(()=>content.add(broken),{code:'snapshot_content_page'});
 f=fixture();f.manifest.source_watermarks[0].baseline_sequence='1';const {manifest_digest,...unsigned}=f.manifest;f.manifest.manifest_digest=digest(unsigned).slice(7);f.subscription.snapshot_ref.manifest_digest=f.manifest.manifest_digest;f.context.snapshot_ref=f.subscription.snapshot_ref;for(const p of f.pages)p.manifest_digest=f.manifest.manifest_digest;
 content=createSourceSnapshotContent(f);for(const p of f.pages)content.add(p);assert.throws(()=>content.finish(),{code:'snapshot_content_watermarks'});
 f=fixture();content=createSourceSnapshotContent(f);f.manifest.source_watermarks=[];for(const p of f.pages)content.add(p);assert.equal(content.finish().source_watermarks.length,2);
});
test('即使重算摘要，跨页重复、身份倒序和不完整来源集合仍拒绝',()=>{
 for(const mutate of [f=>{f.pages[1].content.records[0]=clone(f.pages[0].content.records[0]);},f=>{f.pages[0].content.records.reverse();},f=>{f.pages[0].content.records.pop();}]){
  const f=fixture({large:true});mutate(f);
  for(const [index,p]of f.pages.entries()){p.content_digest=digest(p.content).slice(7);f.manifest.pages[index].content_digest=p.content_digest;f.manifest.pages[index].content_bytes=Buffer.byteLength(JSON.stringify(p.content));}
  f.manifest.total_content_bytes=f.manifest.pages.reduce((n,p)=>n+p.content_bytes,0);const {manifest_digest,...unsigned}=f.manifest;f.manifest.manifest_digest=digest(unsigned).slice(7);f.subscription.snapshot_ref.manifest_digest=f.manifest.manifest_digest;for(const p of f.pages)p.manifest_digest=f.manifest.manifest_digest;
  const content=createSourceSnapshotContent(f);for(const p of f.pages)content.add(p);assert.throws(()=>content.finish(),error=>['snapshot_content_record','snapshot_content_order','snapshot_content_coverage'].includes(error.code));
 }
});
test('下载中断线、配对变化或本地截止到期不产生可用句柄，成功句柄不能续期',async()=>{
 for(const change of [f=>{f.state.connected=false;},f=>{f.state.pair={...f.pair,revision:'2'};},f=>{f.state.tick=4000000000n;}]){const f=fixture(),page=f.client.page;f.client.page=async(...args)=>{const value=await page(...args);change(f);return value;};const receiver=createPeerSnapshotReceiver(f);await assert.rejects(receiver.prepare(f.pair.binding_id,f.connection));receiver.close();}
 const f=fixture(),receiver=createPeerSnapshotReceiver(f),handle=await receiver.prepare(f.pair.binding_id,f.connection),value=receiver.current(handle);f.state.tick=BigInt(value.lease.valid_before_tick_ns);assert.throws(()=>receiver.current(handle),{code:'snapshot_lease_expired'});f.state.tick=100n;assert.throws(()=>receiver.current(handle),{code:'peer_snapshot_handle'});receiver.close();
});
test('本机消费者身份、数据库、对端角色及发布方配对记录必须与订阅相符',async()=>{
 for(const change of [f=>{f.service.service_id=uuid();},f=>{f.service.database_instance_id=uuid();},f=>{f.pair.transcript.parties[0].service_kind='seamer';},f=>{f.pair.transcript.parties[0].binding_id=uuid();}]){const f=fixture();change(f);const receiver=createPeerSnapshotReceiver(f);await assert.rejects(receiver.prepare(f.pair.binding_id,f.connection),error=>['peer_snapshot_consumer','peer_snapshot_publisher'].includes(error.code));receiver.close();}
});
test('摘要有效的来源gap也须区分未知范围、有效已知区间和唯一身份',()=>{
 const gap={gap_id:'gap',loss_extent:'unknown',first_missing:null,last_missing:null,resolution_state:'open'};
 for(const gaps of [[{...gap,first_missing:'1'}],[{...gap,loss_extent:'known'}],[{...gap,loss_extent:'known',first_missing:'3',last_missing:'2'}],[gap,gap]]){
  const f=fixture();f.pages[0].content.records[0].view.gaps=gaps;f.manifest.source_watermarks[0].gap_summary=gaps;
  const p=f.pages[0];p.content_digest=digest(p.content).slice(7);f.manifest.pages[0].content_digest=p.content_digest;f.manifest.pages[0].content_bytes=Buffer.byteLength(JSON.stringify(p.content));f.manifest.total_content_bytes=f.manifest.pages[0].content_bytes;
  const {manifest_digest,...unsigned}=f.manifest;f.manifest.manifest_digest=digest(unsigned).slice(7);f.subscription.snapshot_ref.manifest_digest=f.manifest.manifest_digest;p.manifest_digest=f.manifest.manifest_digest;
  const content=createSourceSnapshotContent(f);content.add(p);assert.throws(()=>content.finish(),{code:'snapshot_content_gap'});
 }
});
