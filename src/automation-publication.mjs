import crypto from 'node:crypto';
import canonicalize from 'canonicalize';
import {validateAutomationModel} from './sdk/automation-program.mjs';
import {freeze} from './automation-expression.mjs';
import {compileProgram} from './automation-compiler.mjs';

export class PublicationError extends Error {constructor(code){super(code);this.name='PublicationError';this.code=code;}}
export const need=(value,code)=>{if(!value)throw new PublicationError(code);};
export const hash=value=>'sha256:'+crypto.createHash('sha256').update(canonicalize(value)).digest('hex');
export const same=(a,b)=>canonicalize(a)===canonicalize(b);
export const checked=(name,value)=>{validateAutomationModel(name,value);return freeze(structuredClone(value));};
const fields=(value,keys)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).sort().join(',')===[...keys].sort().join(',');
const ordered=values=>[...new Set(values)].sort((a,b)=>Buffer.compare(Buffer.from(canonicalize(a)),Buffer.from(canonicalize(b))));
const allSteps=program=>program.kind==='rule'?program.groups.flatMap(group=>group.steps):program.transitions.flatMap(value=>[...value.exit,...value.transition,...value.enter]);

// 只验证可信来源返回的固定原件及其绑定，不把摘要、complete 或审阅文档本身当作来源批准。
// 调用方仍须在最终写事务内要求该来源复核当前注册、审阅及所有允许参数的作用范围资格。
export function publicationInputs(draft,snapshot){
 checked('Value',{kind:'literal',value:snapshot});
 need(fields(snapshot,['bindings','alias_table','schemas','review_ref','review']),'publication_source_shape');
 const fixed=freeze(structuredClone(snapshot)),selections=draft.content.selections;
 need(Array.isArray(fixed.bindings)&&fixed.bindings.length===selections.length,'publication_selection');
 const bindings=new Map();
 for(const item of fixed.bindings){
  need(fields(item,['kind','binding_id','content_ref','value'])&&['input','action'].includes(item.kind),'publication_selection');
  const key=item.kind+':'+item.binding_id;need(!bindings.has(key),'publication_selection');
  checked(item.kind==='input'?'InputBinding':'ActionBinding',item.value);checked('ContentRef',item.content_ref);
  need(item.binding_id===item.value.binding_id&&item.content_ref.digest===hash(item.value),'publication_binding_digest');bindings.set(key,item);
 }
 for(const selection of selections){const item=bindings.get(selection.kind+':'+selection.binding_id);need(item&&same(item.content_ref,selection.content_ref),'publication_selection');}
 checked('AliasTable',fixed.alias_table);need(fixed.alias_table.ref.digest===hash(fixed.alias_table.entries),'publication_alias_digest');
 const actions=fixed.bindings.filter(item=>item.kind==='action').map(item=>item.value),inputs=fixed.bindings.filter(item=>item.kind==='input').map(item=>item.value);
 need(actions.every(binding=>binding.footprint.complete),'publication_footprint_incomplete');
 need(Array.isArray(fixed.schemas)&&fixed.schemas.length===actions.length,'publication_schemas');const schemas=new Map();
 for(const item of fixed.schemas){
  need(fields(item,['binding_id','schema_ref','document'])&&!schemas.has(item.binding_id),'publication_schemas');
  const binding=actions.find(value=>value.binding_id===item.binding_id);need(binding&&same(binding.parameter_schema_ref,item.schema_ref)&&hash(item.document)===item.schema_ref.digest,'publication_schema_digest');
  schemas.set(item.binding_id,{schema_ref:item.schema_ref,document:item.document});
 }
 checked('ContentRef',fixed.review_ref);
 const expected={scope:draft.scope,selection_digest:draft.selection_digest,alias_table_digest:hash(fixed.alias_table),policy_ref:draft.content.policy_ref,
  parameter_schema_refs:fixed.schemas.map(item=>({binding_id:item.binding_id,schema_ref:item.schema_ref})),parameter_scope:'all_allowed_parameters'};
 if(Object.hasOwn(fixed.review??{},'binding_approval_refs')){
  const references=fixed.review.binding_approval_refs;
  need(Array.isArray(references)&&references.length===selections.length,'publication_review_binding');
  for(const [index,reference] of references.entries()){
   need(fields(reference,['binding_id','kind','approval_ref'])&&reference.binding_id===selections[index].binding_id&&
    ['device','event'].includes(reference.kind)&&(reference.kind!=='event'||selections[index].kind==='input'),'publication_review_binding');
   checked('ContentRef',reference.approval_ref);
  }
  expected.binding_approval_refs=references;
 }
 need(same(fixed.review,expected)&&fixed.review_ref.digest===hash(fixed.review),'publication_review_binding');
 return {snapshot:fixed,schemas,inputs,actions};
}
export function derivePublication(draft,version,snapshot){
 const input=publicationInputs(draft,snapshot),policy=draft.content.policy_ref;
 const compiled=compileProgram({definition_id:draft.definition_id,definition_version:version,policy_ref:{id:policy.object_id,revision:policy.revision,digest:policy.digest},program:draft.content.program,
  input_bindings:input.inputs,action_bindings:input.actions,alias_table:input.snapshot.alias_table});
 const steps=new Map(allSteps(compiled.compiled.program).map(step=>[step.step_id,step])),actions=new Map(input.actions.map(binding=>[binding.binding_id,binding])),inputs=new Map(input.inputs.map(binding=>[binding.binding_id,binding]));
 const proofs=[],continuations=[...steps.values()].filter(step=>step.failure_policy==='continue_if_independent');let proofBytes=0;
 need(continuations.length*(compiled.graph.nodes.length+compiled.graph.edges.length)<=1048576,'publication_capacity');
 for(const step of continuations){
  const successors=compiled.successors(step.step_id),digests=[hash(input.snapshot.alias_table)];
  for(const id of [step.step_id,...successors]){
   const current=steps.get(id);if(current.kind==='delay')continue;
   digests.push(hash(actions.get(current.action_binding_id)));const plan=compiled.stepPlan(id);
   for(const reference of [...plan.guard.references,...plan.parameters.flatMap(parameter=>parameter.value.references)])digests.push(hash(inputs.get(reference.binding_id)));
  }
  const dependent=compiled.graph.edges.some(edge=>edge.from_step_id===step.step_id&&edge.edge_kind!=='sequence');
  const proof=checked('IndependenceProof',{proof_version:'automation-program/0.1.0-draft.1',generator_version:'seamer-publication-1',program_digest:compiled.program_digest,bindings_digest:compiled.bindings_digest,
   graph_digest:compiled.compiled.graph_digest,failed_step_id:step.step_id,successor_step_ids:successors,footprint_binding_digests:ordered(digests),verdict:dependent?'dependent':'independent'});
  need(proof.verdict==='independent','publication_dependency');proofBytes+=Buffer.byteLength(canonicalize(proof));need(proofBytes<=16777216,'publication_capacity');proofs.push(proof);
 }
 return {compiled,proofs,steps,input};
}
export async function validateStaticParameters(derived){
 const execution=derived.compiled.parameterExecution({snapshots:new Map(),schemas:derived.input.schemas});
 for(const step of derived.steps.values())if(step.kind==='action'){
  const plan=derived.compiled.stepPlan(step.step_id);
  if(plan.parameters.every(parameter=>parameter.value.references.length===0))need((await execution.resolve(step.step_id)).status==='validated','publication_parameters');
 }
}
