import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import {fileURLToPath,pathToFileURL} from 'node:url';
export const packageRoot=process.env.LEAFSEAMER_PACKAGE_UNDER_TEST??fileURLToPath(new URL('../',import.meta.url));
assert.ok(path.isAbsolute(packageRoot));
const {initializeStorage,migrateStorage,openStorage}=await import(pathToFileURL(path.join(packageRoot,'dist/storage.mjs')));
import {source,digest} from './automation-program-fixture.mjs';

// 合成来源仅验证事务与编译集成，不授予任何型号或部署运行资格。
function directory(){return fs.mkdtempSync(path.join(os.tmpdir(),'leafseamer-publication-'));}
export function inspect(dir,run){const db=new Database(path.join(dir,'seamer.sqlite'));try{return run(db);}finally{db.close();}}
const tables=['definition_version_reservation','definition_publication','definition_request','definition_notification'];
export const counts=dir=>inspect(dir,db=>Object.fromEntries(tables.map(name=>[name,db.prepare('SELECT count(*) FROM '+name).pluck().get()])));
export function publicationMaterial(scope,{optional=false,withoutActions=false,triggerBinding=null,triggerReview=null}={}){
 const model=source(),schema={type:'object',properties:{value:{type:'boolean'}},required:['value'],additionalProperties:false};
 if(triggerBinding)Object.assign(model.input_bindings[0],triggerBinding);
 if(triggerReview){model.program.condition=triggerReview.condition;model.input_bindings.push(...(triggerReview.extraInputs??[]));}
 model.action_bindings[0].parameter_schema_ref={id:'parameter-schema',revision:'1',digest:digest(schema)};
 if(optional){model.program.groups[0].steps[0].required=false;model.program.groups[0].steps[0].failure_policy='continue_if_independent';}
 else model.program.groups[0].steps.pop();
 if(withoutActions){model.action_bindings.length=0;model.program.groups[0].steps=[{step_id:'wait',depends_on:[],required:true,
  failure_policy:'stop',kind:'delay',delay_ms:0,max_lateness_ms:100}];}
 if(triggerReview?.beforeSteps)model.program.groups[0].steps.unshift(...triggerReview.beforeSteps);
 if(triggerReview?.extraSteps)model.program.groups[0].steps.push(...triggerReview.extraSteps);
 if(triggerReview?.actionParameters)Object.assign(model.program.groups[0].steps.find(step=>step.kind==='action'),triggerReview.actionParameters);
 if(triggerReview?.stateMachine){
  const {condition,trigger_binding_id,groups}=model.program;
  model.program={kind:'state_machine',states:['idle','live'],initial_state:'idle',transitions:[{
   transition_id:'start',from_state:'idle',to_state:'live',priority:0,condition,trigger_binding_id,exit:[],transition:groups[0].steps,enter:[]}]};
 }
 const bindings=[...model.input_bindings.map(value=>({kind:'input',value})),...model.action_bindings.map(value=>({kind:'action',value}))].map(item=>({...item,binding_id:item.value.binding_id,content_ref:{id:item.kind+'-'+item.value.binding_id,revision:'1',digest:digest(item.value)}}));
 const content={program:model.program,selections:bindings.map(({kind,binding_id,content_ref})=>({kind,binding_id,content_ref})),policy_ref:{kind:'policy',scope,object_id:'policy',revision:'1',digest:triggerReview?.policyDigest??digest({})}};
 const schemas=model.action_bindings.map(value=>({binding_id:value.binding_id,schema_ref:value.parameter_schema_ref,document:schema}));
 const review={scope,selection_digest:digest(content.selections),alias_table_digest:digest(model.alias_table),policy_ref:content.policy_ref,parameter_schema_refs:schemas.map(({binding_id,schema_ref})=>({binding_id,schema_ref})),parameter_scope:'all_allowed_parameters'};
 if(triggerReview)review.binding_approval_refs=bindings.map(({binding_id,kind,content_ref})=>({binding_id,kind:kind==='input'?'event':'device',approval_ref:content_ref}));
 const snapshot={bindings,alias_table:model.alias_table,schemas,review_ref:{id:'synthetic-review',revision:'1',digest:digest(review)},review};
 return {content,snapshot};
}
export function setup(t,{migrate=true,provider=true,optional=false,withoutActions=false,revision='2',triggerBinding=null,triggerReview=null}={}){
 const dir=directory(t),identity=initializeStorage(dir);if(migrate)migrateStorage(dir,revision);
 const state={kind:'human',revision:1,deny:false,denyRead:false,resolve:null,current:null,currentCalls:0},scope={authority_service_id:identity.service_id,database_instance_id:identity.database_instance_id};
 const actor=()=>({issuer_id:'issuer',issuer_epoch:'epoch',principal_id:'person',principal_kind:state.kind});
 const authority={authorizeInTransaction({operation_id,definition_id,context_id,scope}){
  if(state.deny||state.denyRead&&operation_id==='getDraft')throw new Error('test_denied');
  return {scope,actor:actor(),context_id,request_epoch:identity.request_epoch,operation_id,definition_id,authorization_digest:digest({revision:state.revision})};
 },lookupInTransaction(){if(state.deny)throw new Error('test_denied');return {scope,request_epoch:identity.request_epoch,actor:actor(),authorization_digest:digest({revision:state.revision})};},assertRunCapacityInTransaction(){}};
 const {content,snapshot}=publicationMaterial(scope,{optional,withoutActions,triggerBinding,triggerReview});
 const publicationSource={async resolve(input){return state.resolve?state.resolve(input):structuredClone(snapshot);},assertCurrentInTransaction(input){state.currentCalls++;return state.current?state.current(input):digest(snapshot);}};
 let store;
 const reopen=()=>{store?.close();store=openStorage(dir,{draftAuthority:authority,publicationSource:provider?publicationSource:null});return store;};reopen();t.after(()=>{store.close();assert.equal(path.dirname(path.resolve(dir)),path.resolve(os.tmpdir()));fs.rmSync(dir,{recursive:true,force:true,maxRetries:5,retryDelay:50});});
 const draft=store.drafts.create(null,{context:{context_id:'context',idempotency_key:'create'},content,metadata:{name:'合成发布事务',description:'',layout:{}}});
 const request={context:{context_id:'context',idempotency_key:'publish'},expected_draft_revision:draft.revision,expected_content_digest:draft.content_digest,expected_selection_digest:draft.selection_digest};
 return {dir,identity,state,scope,snapshot,content,draft,request,reopen,get store(){return store;},publish(body=request){return store.publications.publish(null,draft.definition_id,body);}};
}
