import test from 'node:test';
import assert from 'node:assert/strict';
import {digest} from './automation-program-fixture.mjs';
import {setup,inspect,counts} from './automation-publication-fixture.mjs';
import {automationPublicationStore} from '../dist/automation-publication-store.mjs';
test('编译、证明、发布原件及请求结果原子保存，重启查原不重新解析来源',async t=>{
 const f=setup(t,{optional:true}),result=await f.publish();assert.equal(result.definition_version,'1');assert.equal(result.source_draft_revision,'1');
 const artifact=f.store.publications.getArtifact(null,f.draft.definition_id,'1'),program=f.store.publications.getProgram(null,f.draft.definition_id,'1');
 assert.equal(digest(program),result.program_digest);assert.equal(artifact.manifest_digest,result.artifact_ref.digest);assert.equal(artifact.proof_refs.length,1);
 const ref=artifact.proof_refs[0],proof=f.store.publications.getProof(null,f.draft.definition_id,ref.id,ref.revision);assert.equal(digest(proof),ref.digest);assert.equal(proof.verdict,'independent');assert.deepEqual(proof.successor_step_ids,['second']);
 f.state.resolve=()=>{throw new Error('must_not_resolve_again');};assert.deepEqual(await f.publish(),result);
 f.reopen();assert.deepEqual(f.store.publications.getArtifact(null,f.draft.definition_id,'1'),artifact);
 const lookup=f.store.drafts.lookup(null,{context_id:'context',operation_id:'publishDefinition',idempotency_key:'publish'});assert.deepEqual(lookup.result,{kind:'PublishedVersion',value:result});
 await assert.rejects(f.publish({...f.request,expected_draft_revision:'2'}),{code:'publication_idempotency_conflict'});
 f.state.denyRead=true;assert.throws(()=>f.store.drafts.lookup(null,{context_id:'context',operation_id:'publishDefinition',idempotency_key:'publish'}),/test_denied/);assert.throws(()=>f.store.publications.getProgram(null,f.draft.definition_id,'1'),/test_denied/);
 f.store.close();assert.deepEqual(counts(f.dir),{definition_version_reservation:1,definition_publication:1,definition_request:2,definition_notification:2});
 inspect(f.dir,db=>{assert.deepEqual(db.prepare('PRAGMA foreign_key_check').raw().all(),[]);for(const table of ['definition_publication','definition_version_reservation'])assert.throws(()=>db.exec('DELETE FROM '+table));});
});
test('必须显式迁移、实际人员授权及可信来源，拒绝时不创建发布结果',async t=>{
 for(const option of [{migrate:false},{provider:false},{}]){
  const f=setup(t,option);if(Object.keys(option).length===0)f.state.kind='service';
  await assert.rejects(f.publish(),{code:option.migrate===false?'publication_migration_required':option.provider===false?'publication_source_unavailable':'publication_human_required'});
  f.store.close();if(option.migrate!==false)assert.equal(counts(f.dir).definition_version_reservation,0);
 }
});
test('来源失效不产生结果，失败版本不重用；历史读取不要求旧来源继续有效',async t=>{
 const f=setup(t);f.state.current=()=>digest({changed:true});await assert.rejects(f.publish(),{code:'publication_source_changed'});
 f.state.current=null;const result=await f.publish();assert.equal(result.definition_version,'2');
 f.state.current=()=>{throw new Error('source_revoked');};assert.equal(f.store.publications.getProgram(null,f.draft.definition_id,'2').definition_version,'2');
 f.store.close();assert.deepEqual(counts(f.dir),{definition_version_reservation:2,definition_publication:1,definition_request:2,definition_notification:2});
});
test('异步解析期间撤权或授权修订变化均拒绝提交',async t=>{
 for(const denied of [true,false]){const f=setup(t);f.state.resolve=()=>{if(denied)f.state.deny=true;else f.state.revision++;return f.snapshot;};
  await assert.rejects(f.publish(),denied?/test_denied/:{code:'publication_authorization_changed'});f.store.close();assert.equal(counts(f.dir).definition_publication,0);
 }
});
test('编译期间草稿内容 CAS 变化拒绝发布；元数据修改不改变固定内容',async t=>{
 const f=setup(t);f.state.resolve=()=>{const content=structuredClone(f.content);content.program.groups[0].steps[0].parameters.value=true;f.store.drafts.update(null,f.draft.definition_id,{context:{context_id:'context',idempotency_key:'edit'},expected_draft_revision:'1',content});return f.snapshot;};
 await assert.rejects(f.publish(),{code:'publication_draft_conflict'});
 const g=setup(t);g.state.resolve=()=>{g.store.drafts.updateMetadata(null,g.draft.definition_id,{context:{context_id:'context',idempotency_key:'rename'},expected_metadata_revision:'1',metadata:{name:'新名称',description:'',layout:{}}});return g.snapshot;};
 assert.equal((await g.publish()).source_draft_revision,'1');assert.equal(g.store.drafts.get(null,g.draft.definition_id).metadata_revision,'2');
});
test('绑定摘要、选择集、参数 Schema、审阅绑定及完整足迹缺一即拒绝',async t=>{
 const changes=[s=>s.bindings.pop(),s=>s.bindings[0].value.projections[0].reads.push('new/read'),s=>s.schemas[0].document.required=[],s=>s.review.parameter_scope='current_values_only',s=>s.bindings[1].value.footprint.complete=false];
 for(const change of changes){const f=setup(t);f.state.resolve=()=>{const copy=structuredClone(f.snapshot);change(copy);return copy;};await assert.rejects(f.publish());f.store.close();assert.equal(counts(f.dir).definition_publication,0);}
});
test('完整参数 Schema 验证拒绝不匹配的静态参数，不仅检查模板摘要',async t=>{
 const f=setup(t);const content=structuredClone(f.content);content.program.groups[0].steps[0].parameters.value='invalid';const changed=f.store.drafts.update(null,f.draft.definition_id,{context:{context_id:'context',idempotency_key:'bad-parameter'},expected_draft_revision:'1',content});
 await assert.rejects(f.publish({...f.request,expected_draft_revision:changed.revision,expected_content_digest:changed.content_digest}));f.store.close();assert.equal(counts(f.dir).definition_publication,0);
});
for(const table of ['definition_publication','definition_request','definition_notification'])test('发布写入失败整笔回滚但保留预留空号：'+table,async t=>{
 const f=setup(t);f.store.close();inspect(f.dir,db=>db.exec(`CREATE TRIGGER fail_publication BEFORE INSERT ON ${table} BEGIN SELECT RAISE(ABORT,'injected_publication_failure'); END`));f.reopen();
 await assert.rejects(f.publish(),/injected_publication_failure/);f.store.close();assert.deepEqual(counts(f.dir),{definition_version_reservation:1,definition_publication:0,definition_request:1,definition_notification:1});
 inspect(f.dir,db=>db.exec('DROP TRIGGER fail_publication'));f.reopen();assert.equal((await f.publish()).definition_version,'2');
});
test('事务末来源变化回滚产物、请求及通知；同 key 可用新版本重试',async t=>{
 const f=setup(t);f.state.current=()=>f.state.currentCalls===2?digest({stale:true}):digest(f.snapshot);
 await assert.rejects(f.publish(),{code:'publication_source_changed'});f.state.current=null;assert.equal((await f.publish()).definition_version,'2');
});
test('并发同 key 只提交一份结果，保留各自预留且不重新消耗已提交请求',async t=>{
 const f=setup(t);let release;const gate=new Promise(resolve=>{release=resolve;});let arrived=0;
 f.state.resolve=async()=>{if(++arrived===2)release();await gate;return f.snapshot;};
 const [a,b]=await Promise.all([f.publish(),f.publish()]);assert.deepEqual(a,b);f.store.close();assert.deepEqual(counts(f.dir),{definition_version_reservation:2,definition_publication:1,definition_request:2,definition_notification:2});
});
test('不可变发布原件的摘要损坏在历史读取时拒绝',async t=>{
 const f=setup(t);await f.publish();f.store.close();inspect(f.dir,db=>db.exec("DROP TRIGGER definition_publication_no_update; UPDATE definition_publication SET bundle_digest='changed'"));f.reopen();
 assert.throws(()=>f.store.publications.getArtifact(null,f.draft.definition_id,'1'),{code:'publication_integrity'});
});
test('已校验的发布原件仍逐次检查关联索引，原文字节变化必须重新验证',async t=>{
 const f=setup(t,{optional:true});const result=await f.publish();f.store.close();
 inspect(f.dir,db=>{
  const publications=automationPublicationStore({db,service:{...f.identity,schema_revision:'2'},isOpen:()=>db.open});
  const read=()=>db.transaction(()=>publications.versionForRunInTransaction(result.definition_id,result.definition_version).result).immediate();
  assert.deepEqual(read(),result);
  db.exec('DROP TRIGGER definition_publication_proof_no_update');
  const original=db.prepare('SELECT proof_digest FROM definition_publication_proof').pluck().get();
  const update=db.prepare('UPDATE definition_publication_proof SET proof_digest=?');
  assert.equal(update.run(digest('altered proof index')).changes,1);
  assert.throws(read,{code:'publication_integrity'});
  assert.equal(update.run(original).changes,1);
  assert.deepEqual(read(),result);
  db.exec(`DROP TRIGGER definition_request_no_update;
   UPDATE definition_request SET result_jcs=result_jcs||' ' WHERE operation_id='publishDefinition'`);
  assert.throws(read,{code:'publication_integrity'});
  db.exec("UPDATE definition_request SET result_jcs=rtrim(result_jcs) WHERE operation_id='publishDefinition'");
  assert.deepEqual(read(),result);
  db.exec("DROP TRIGGER definition_publication_no_update; UPDATE definition_publication SET bundle_jcs=bundle_jcs||' '");
  assert.throws(read,{code:'publication_integrity'});
 });
});

for(const table of ['definition_version_reservation','definition_publication','definition_publication_proof','definition_request','definition_notification'])test('静默遗漏写入也拒绝发布：'+table,async t=>{
 const f=setup(t,{optional:table==='definition_publication_proof'});f.store.close();inspect(f.dir,db=>db.exec(`CREATE TRIGGER omit_publication BEFORE INSERT ON ${table} BEGIN SELECT RAISE(IGNORE); END`));f.reopen();
 await assert.rejects(f.publish(),{code:'publication_integrity'});f.store.close();const result=counts(f.dir);assert.equal(result.definition_publication,0);assert.equal(result.definition_request,1);assert.equal(result.definition_notification,1);
});

test('依赖失败步骤的后继不能被发布为独立继续',async t=>{
 const f=setup(t,{optional:true}),content=structuredClone(f.content);content.program.groups[0].steps[1].depends_on=['first'];
 const draft=f.store.drafts.update(null,f.draft.definition_id,{context:{context_id:'context',idempotency_key:'dependency'},expected_draft_revision:'1',content});
 await assert.rejects(f.publish({...f.request,expected_draft_revision:draft.revision,expected_content_digest:draft.content_digest}),{code:'publication_dependency'});assert.equal(f.state.currentCalls,0);
});
test('累计证明预算在启动参数子进程前拒绝超大继续集合',async t=>{
 const f=setup(t,{optional:true}),content=structuredClone(f.content),template=content.program.groups[0].steps[0];
 content.program.groups=Array.from({length:18},(_,group)=>({mode:'sequence',steps:Array.from({length:group===17?12:64},(_,index)=>({...template,step_id:'step-'+(group*64+index)}))}));
 const draft=f.store.drafts.update(null,f.draft.definition_id,{context:{context_id:'context',idempotency_key:'large-proofs'},expected_draft_revision:'1',content});
 await assert.rejects(f.publish({...f.request,expected_draft_revision:draft.revision,expected_content_digest:draft.content_digest}),{code:'publication_capacity'});assert.equal(f.state.currentCalls,0);
});

test('schema3沿用发布事务与原件读取，不把本地绑定批准自动当来源提供者',async t=>{
 const f=setup(t,{revision:'3'}),published=await f.publish();assert.equal(published.definition_version,'1');assert.deepEqual(await f.publish(),published);
 assert.equal(f.store.publications.getArtifact(null,published.definition_id,'1').manifest_digest,published.artifact_ref.digest);
 const missing=setup(t,{revision:'3',provider:false});await assert.rejects(missing.publish(),{code:'publication_source_unavailable'});
});
