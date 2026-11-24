import test from 'node:test';
import assert from 'node:assert/strict';
import {manageSourceSubscriptions} from '../dist/managed-source-subscription.mjs';

function fixture(t){
 const request={request_id:'request',consumer_id:'slot',scope:{scope_id:'scope'}};
 const view={owner:{consumer_id:'slot'},scope:request.scope,purpose:'canonical_trigger',binding:{subscription_id:'subscription'},state:'building'};
 const stages=[],handle={},receipt={apply_id:'application'};
 const open=()=>({closed:false,close(){this.closed=true;stages.push('close');}});
 let connection,checks=0;
 const state={denied:false};
 const manager=manageSourceSubscriptions({bindingId:'binding',client:{
  async create(bindingId,body){assert.equal(bindingId,'binding');assert.deepEqual(body,{...request,purpose:'canonical_trigger'});return {view};},
  async attach(){return connection=open();},isClosed:entry=>entry.closed
 },receiver:{
  async prepare(){stages.push('prepare');return handle;},release(value){assert.equal(value,handle);stages.push('release');}
 },ingest:{apply(value){assert.equal(value,handle);stages.push('apply');return receipt;},find(){stages.push('find');return receipt;}},
 confirmation:{
  async send(_binding,channel,value,check){assert.equal(channel,connection);assert.equal(value,handle);check();stages.push('send');},
  async resume(_binding,id,{assertCurrent}){assert.equal(id,receipt.apply_id);assertCurrent();stages.push('resume');return connection=open();},
  async query(){return {state:'confirmed',transport_ready:true};}
 }});
 t.after(()=>manager.close());
 return {request,view,stages,state,connect:()=>manager.connect(request,async()=>{
  ++checks;
  return ()=>{if(state.denied||checks===2&&state.denyAdoption)throw Error('management_denied');};
 }),disconnect:()=>connection.close()};
}

test('管理入口等待快照、先持久采用再确认；重投复用连接，断连用同一持久回执恢复',async t=>{
 const f=fixture(t);
 assert.deepEqual(await f.connect(),{state:'building',subscription_id:'subscription',apply_id:null,transport_ready:false});
 f.view.state='snapshot_ready';
 assert.deepEqual(await f.connect(),{state:'pending',subscription_id:'subscription',apply_id:'application',transport_ready:false});
 assert.deepEqual(f.stages,['prepare','apply','send','release']);
 assert.deepEqual(await f.connect(),{state:'confirmed',subscription_id:'subscription',apply_id:'application',transport_ready:true});
 f.disconnect();f.view.state='live';
 assert.equal((await f.connect()).state,'pending');
 assert.deepEqual(f.stages,['prepare','apply','send','release','close','find','resume']);
});

test('下载完成后管理员失权时释放暂存和连接，避免落库和发送确认',async t=>{
 const f=fixture(t);f.view.state='snapshot_ready';f.state.denyAdoption=true;
 await assert.rejects(f.connect(),/management_denied/);
 assert.deepEqual(f.stages,['prepare','release','close']);
});
