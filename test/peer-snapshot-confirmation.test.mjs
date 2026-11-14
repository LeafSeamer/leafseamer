import test from 'node:test';import assert from 'node:assert/strict';import path from 'node:path';import {fileURLToPath,pathToFileURL} from 'node:url';
const root=process.env.LEAFSEAMER_PACKAGE_UNDER_TEST??fileURLToPath(new URL('../',import.meta.url)),load=name=>import(pathToFileURL(path.join(root,'dist',name)));
const {createPeerSnapshotConfirmation}=await load('peer-snapshot-confirmation.mjs'),{digest}=await load('runtime/auth-clock-value.mjs');
function fixture(){const receipt={apply_id:'apply',state:'committed',owner:{kind:'service',consumer_id:'events'},delivery_binding:{subscription_id:'subscription',connection_generation:'1'},snapshot_ref:{snapshot_id:'snapshot',snapshot_cursor:'0',manifest_digest:'a'.repeat(64)}},view={state:'snapshot_ready',owner:receipt.owner,binding:receipt.delivery_binding,snapshot_ref:receipt.snapshot_ref,purpose:'canonical_trigger',ack_cursor:'0',ack_payload_digest:'0'.repeat(64)};
 const state={receipt,view,committed:true,valid:true,sent:0,beforeSend:null,beforeGet:null},ingest={lookup(){assert.ok(state.committed);return structuredClone(receipt);},confirmationReceipt(){assert.ok(state.valid);return structuredClone(receipt);}},receiver={current(){assert.ok(state.valid);return {subscription:{binding:receipt.delivery_binding,owner:receipt.owner,snapshot_ref:receipt.snapshot_ref}};}},client={checkAttached(){assert.ok(state.valid);return {attached:{binding:receipt.delivery_binding},ready:{channel:{owner:receipt.owner}}};},async snapshotApplied(_b,_c,snapshot,gate){assert.deepEqual(snapshot,receipt.snapshot_ref);state.beforeSend?.();gate();state.sent++;},async get(){state.beforeGet?.();return {view:state.view};}};
 return {state,api:createPeerSnapshotConfirmation({ingest,receiver,client}),confirm(){state.view={...view,state:'catching_up',ack_payload_digest:digest({entry_kind:'snapshot.baseline',payload:receipt.snapshot_ref}).slice(7)};}};
}
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
