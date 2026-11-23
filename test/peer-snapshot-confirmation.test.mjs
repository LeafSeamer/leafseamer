import test from 'node:test';import assert from 'node:assert/strict';import path from 'node:path';import {fileURLToPath,pathToFileURL} from 'node:url';
import {randomUUID} from 'node:crypto';
const root=process.env.LEAFSEAMER_PACKAGE_UNDER_TEST??fileURLToPath(new URL('../',import.meta.url)),load=name=>import(pathToFileURL(path.join(root,'dist',name)));
const {createPeerSnapshotConfirmation}=await load('peer-snapshot-confirmation.mjs'),{digest}=await load('runtime/auth-clock-value.mjs');
test('握手在附着期限之后打开时关闭迟到连接，发送帧数为零',async t=>{
 const {createPeerSubscriptionClient}=await load('peer-subscription-client.mjs');
 const opening=Promise.withResolvers(),started=Promise.withResolvers();
 const client=createPeerSubscriptionClient({transport:{openSubscriptionChannel(){started.resolve();return opening.promise;}}});
 const owner={kind:'service',issuer_id:randomUUID(),issuer_epoch:randomUUID(),principal_id:randomUUID(),consumer_service_id:randomUUID(),consumer_database_instance_id:randomUUID(),consumer_id:'events',pairing_id:randomUUID()};
 const binding={subscription_id:randomUUID(),stream_id:randomUUID(),stream_epoch:randomUUID(),scope_digest:'a'.repeat(64),authorization_revision:'1',connection_generation:'0'};
 t.mock.timers.enable({apis:['setTimeout']});
 const pending=client.attach('binding',{owner,binding});
 await Promise.race([started.promise,pending]);
 t.mock.timers.tick(5000);
 let sent=0,closed=0;
 opening.resolve({send(){sent++;},close(){closed++;}});
 await assert.rejects(pending,/subscription_attach_timeout/);
 assert.equal(sent,0);
 assert.equal(closed,1);
});
function fixture(options={}){const receipt={apply_id:'apply',state:'committed',owner:{kind:'service',consumer_id:'events'},delivery_binding:{subscription_id:'subscription',connection_generation:'1'},snapshot_ref:{snapshot_id:'snapshot',snapshot_cursor:'0',manifest_digest:'a'.repeat(64)}},view={state:'snapshot_ready',owner:receipt.owner,binding:receipt.delivery_binding,snapshot_ref:receipt.snapshot_ref,purpose:'canonical_trigger',ack_cursor:'0',ack_payload_digest:'0'.repeat(64)};
 const state={receipt,view,committed:true,valid:true,sent:0,beforeSend:null,beforeGet:null},ingest={lookup(){assert.ok(state.committed);return structuredClone(receipt);},confirmationReceipt(){assert.ok(state.valid);return structuredClone(receipt);}},receiver={current(){assert.ok(state.valid);return {subscription:{binding:state.view.binding,owner:receipt.owner,snapshot_ref:receipt.snapshot_ref}};}},client={checkAttached(handle){assert.ok(state.valid);return {attached:{binding:handle.binding??receipt.delivery_binding},ready:{channel:{owner:receipt.owner}}};},checkAttachedInTransaction(handle){return this.checkAttached(handle);},async snapshotApplied(_b,_c,snapshot,gate){assert.deepEqual(snapshot,receipt.snapshot_ref);state.beforeSend?.();gate();state.sent++;},async get(){state.beforeGet?.();return {view:state.view};}};
 client.isClosed=handle=>handle.closed===true;
 return {state,client,api:createPeerSnapshotConfirmation({ingest,receiver,client,...options}),confirm(){state.view={...view,state:'catching_up',ack_payload_digest:digest({entry_kind:'snapshot.baseline',payload:receipt.snapshot_ref}).slice(7)};}};
}
test('恢复从持久采用原件和发布端确认交集生成，不能用manifest摘要或未确认点',async()=>{
 const f=fixture();let sent=0;f.client.attach=async(_binding,view,options)=>{assert.deepEqual(options.resume,{cursor:'0',payload_digest:f.state.view.ack_payload_digest,ack_kind:'durable_ingest'});options.beforeSend();sent++;return {close(){}};};
 await assert.rejects(f.api.resume('binding','apply'));assert.equal(sent,0);f.confirm();await f.api.resume('binding','apply');assert.equal(sent,1);
 for(const patch of [{ack_payload_digest:f.state.receipt.snapshot_ref.manifest_digest},{ack_cursor:'1'},{state:'snapshot_ready'},{owner:{kind:'service',consumer_id:'wrong'}}]){const original=f.state.view;f.state.view={...original,...patch};await assert.rejects(f.api.resume('binding','apply'));f.state.view=original;}assert.equal(sent,1);
});

test('当前订阅在查询中关断时撤下原代际，预检报告不可用，新代际会使该结论失效',async()=>{
 const f=fixture({clock:{pairingSample:()=>({tick_ns:'1',runtime_domain_ref:{clock_domain_id:'local'}})},reconcile:{}}),handle={closed:false};
 f.confirm();await f.api.send('binding',handle,{});
 const prior=await f.api.prepareSourceTransport('apply');
 f.state.beforeGet=()=>{handle.closed=true;throw Error('subscription_closed');};
 const current=await f.api.prepareSourceTransport('apply');
 assert.deepEqual(current.observation,{state:'unavailable'});current.assertCurrentInTransaction();
 assert.throws(prior.assertCurrentInTransaction,{code:'snapshot_confirmation_changed'});
 f.state.beforeGet=null;
 const binding={...f.state.view.binding,connection_generation:'2'};f.state.view={...f.state.view,binding};
 await f.api.send('binding',{binding},{});
 assert.throws(current.assertCurrentInTransaction,{code:'snapshot_confirmation_changed'});
});
test('恢复等待期间本地投影失效不发送，已附着后的失效关闭新连接',async()=>{
 for(const phase of ['query','send','return']){const f=fixture();f.confirm();let sent=0,closed=0;if(phase==='query')f.state.beforeGet=()=>{f.state.valid=false;};f.client.attach=async(_binding,_view,options)=>{if(phase==='send')f.state.valid=false;options.beforeSend();sent++;if(phase==='return')f.state.valid=false;return {close(){closed++;}};};await assert.rejects(f.api.resume('binding','apply'));assert.equal(sent,phase==='return'?1:0);assert.equal(closed,phase==='return'?1:0);}
});
test('仅已提交且发送时仍有效的回执可确认；编码等待期间失效不发送',async()=>{
 const f=fixture();f.state.committed=false;await assert.rejects(f.api.send('binding',{},{}));assert.equal(f.state.sent,0);f.state.committed=true;f.state.beforeSend=()=>{f.state.valid=false;};await assert.rejects(f.api.send('binding',{},{}));assert.equal(f.state.sent,0);
 const g=fixture();assert.deepEqual(await g.api.send('binding',{},{}),{state:'sent',apply_id:'apply'});assert.equal(g.state.sent,1);
});
test('发送不冒充确认，查原须精确核对独立baseline摘要和当前代际',async()=>{
 const f=fixture();assert.equal((await f.api.query('binding',{},'apply')).state,'pending');f.confirm();const original=await f.api.query('binding',{},'apply');assert.equal(original.state,'confirmed');assert.equal(f.state.sent,0);
 for(const view of [{...f.state.view,ack_payload_digest:f.state.receipt.snapshot_ref.manifest_digest},{...f.state.view,state:'live'},{...f.state.view,binding:{...f.state.view.binding,connection_generation:'2'}},{...f.state.view,owner:{kind:'service',consumer_id:'other'}}]){const before=f.state.view;f.state.view=view;await assert.rejects(f.api.query('binding',{},'apply'));f.state.view=before;}
 assert.deepEqual(await f.api.query('binding',{},'apply'),original);
});
test('查原往返期间投影或连接失效，不能返回旧ACK为当前确认',async()=>{const f=fixture();f.confirm();f.state.beforeGet=()=>{f.state.valid=false;};await assert.rejects(f.api.query('binding',{},'apply'));});
test('布防前传输证据须来自发布端当前 ACK 和本机同代检查点，并受原查询时间及换代约束',async()=>{
 let tick=1n,currentCheckpoint=true;
 const hash='1'.repeat(64);
 const f=fixture({clock:{pairingSample:()=>({tick_ns:String(tick),runtime_domain_ref:{clock_domain_id:'local'}})},
  reconcile:{confirmed:(receipt,cursor,digest)=>receipt.apply_id==='apply'&&cursor==='1'&&digest===hash,
   ready:(_receipt,_cursor,_digest,generation)=>currentCheckpoint&&generation==='1'}});
 const missing=await f.api.prepareSourceTransport('apply');assert.deepEqual(missing.observation,{state:'unavailable'});
 await f.api.send('binding',{},{});
 assert.throws(()=>missing.assertCurrentInTransaction(),{code:'snapshot_confirmation_changed'});
 f.confirm();assert.equal((await f.api.prepareSourceTransport('apply')).observation.state,'catching_up');
 f.state.view={...f.state.view,state:'live',ack_cursor:'1',ack_payload_digest:hash};
 const prepared=await f.api.prepareSourceTransport('apply');
 assert.deepEqual(prepared.observation,{state:'ready',connection_generation:'1',ack_cursor:'1',ack_payload_digest:hash});
 prepared.assertCurrentInTransaction();
 currentCheckpoint=false;assert.throws(()=>prepared.assertCurrentInTransaction(),{code:'snapshot_confirmation_ack'});
 await assert.rejects(f.api.prepareSourceTransport('apply'),{code:'snapshot_confirmation_ack'});
 currentCheckpoint=true;tick+=2000000000n;
 assert.throws(()=>prepared.assertCurrentInTransaction(),{code:'snapshot_confirmation_observation_expired'});
 const beforeReplace=await f.api.prepareSourceTransport('apply');
 const binding={...f.state.view.binding,connection_generation:'2'};f.state.view={...f.state.view,binding};
 await f.api.send('binding',{binding},{});
 assert.throws(()=>beforeReplace.assertCurrentInTransaction(),{code:'snapshot_confirmation_changed'});
 await assert.rejects(f.api.prepareSourceTransport('apply'),{code:'snapshot_confirmation_ack'});
});
