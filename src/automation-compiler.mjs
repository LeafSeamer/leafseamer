import crypto from 'node:crypto';
import canonicalize from 'canonicalize';
import {validateAutomationModel} from './sdk/automation-program.mjs';
import {expressionCompiler,freeze,need,pointerTokens,typeValid,valueMatches} from './automation-expression.mjs';
import {createParameterExecution} from './automation-parameters.mjs';

const hash=value=>'sha256:'+crypto.createHash('sha256').update(canonicalize(value)).digest('hex');
const same=(a,b)=>canonicalize(a)===canonicalize(b);
const compare=(a,b)=>Buffer.compare(Buffer.from(canonicalize(a)),Buffer.from(canonicalize(b)));
const prefix=(a,b)=>a===b||b.startsWith(a+'/');
const overlaps=(a,b)=>prefix(a,b)||prefix(b,a);
const footprintKeys=['reads','writes','failure_writes','resources','preconditions','target_constraints'];
const sourceKeys=['definition_id','definition_version','policy_ref','program','input_bindings','action_bindings','alias_table'];

// 本模块只编译固定模型。发布者必须另行取得当前权威批准、参数覆盖证明和人员授权。
export function compileProgram(source){
 need(source&&typeof source==='object'&&[Object.prototype,null].includes(Object.getPrototypeOf(source)),'program_source');
 const descriptors=Object.getOwnPropertyDescriptors(source),keys=Reflect.ownKeys(descriptors);
 need(keys.length===sourceKeys.length&&sourceKeys.every(key=>Object.hasOwn(descriptors,key)&&descriptors[key].enumerable&&Object.hasOwn(descriptors[key],'value')),'program_source');
 const input={...Object.fromEntries(sourceKeys.map(key=>[key,descriptors[key].value])),dsl_version:'automation-program/0.1.0-draft.1',compiler_version:'seamer-program-1',graph_digest:'sha256:'+'0'.repeat(64)};
 validateAutomationModel('CompiledProgram',input);
 const compiled=structuredClone(input),expressions=expressionCompiler(compiled.input_bindings),actions=new Map(),steps=new Map(),edges=new Map(),branches=[],conditions=new Map(),plans=new Map();
 let comparisons=0;
 const intersects=(a,b)=>{need(++comparisons<=1048576,'resource_budget');return overlaps(a,b);};
 const aliases=compiled.alias_table.entries;
 need(hash(aliases)===compiled.alias_table.ref.digest,'alias_digest');
 for(let i=0;i<aliases.length;i++){
  for(let j=i+1;j<aliases.length;j++)need(!intersects(aliases[i].logical,aliases[j].logical),'alias_overlap');
  for(const other of aliases)need(!intersects(aliases[i].physical,other.logical),'alias_recursive');
 }
 const physical=resource=>{for(const entry of aliases)if(prefix(entry.logical,resource))return entry.physical+resource.slice(entry.logical.length);return resource;};
 for(const binding of compiled.action_bindings){
  need(!actions.has(binding.binding_id),'duplicate_action_binding');const slots=new Map();
  for(const slot of binding.parameter_slots){pointerTokens(slot.pointer);typeValid(slot.value_type);need(!slots.has(slot.pointer),'duplicate_slot');slots.set(slot.pointer,slot);}
  actions.set(binding.binding_id,{binding,slots});
 }
 function add(from,to,kind){const key=JSON.stringify([from,to,kind]);if(!edges.has(key)){need(edges.size<131072,'graph_budget');edges.set(key,{from_step_id:from,to_step_id:to,edge_kind:kind});}}
 function branch(id,units){
  let previous=[];const members=[];
  units.forEach((unit,rank)=>{
   for(const step of unit){need(!steps.has(step.step_id),'duplicate_step');need(steps.size<4096,'step_budget');
    const info={step,branch:id,rank,footprint:null};steps.set(step.step_id,info);members.push(info);for(const before of previous)add(before.step_id,step.step_id,'sequence');}
   previous=unit;
  });branches.push(members);
 }
 function trigger(id,condition,key){
  const binding=expressions.binding(id);need(binding&&binding.source==='event','trigger_binding');const result=expressions.compile(condition,{path:key});
  need(result.references.every(item=>item.source!=='step_result'),'trigger_result');conditions.set(key,result);
 }
 const program=compiled.program;
 if(program.kind==='rule'){
  trigger(program.trigger_binding_id,program.condition,'rule');const parallel=program.groups.some(group=>group.mode==='parallel');
  for(const group of program.groups)for(const step of group.steps){need(group.mode!=='parallel'||step.required,'parallel_optional');need(!parallel||step.failure_policy!=='continue_if_independent','parallel_continue');}
  branch('rule',program.groups.flatMap(group=>group.mode==='parallel'?[group.steps]:group.steps.map(step=>[step])));
 }else{
  need(program.states.includes(program.initial_state),'initial_state');const transitions=new Set();
  for(const transition of program.transitions){
   need(!transitions.has(transition.transition_id),'duplicate_transition');transitions.add(transition.transition_id);
   need(program.states.includes(transition.from_state)&&program.states.includes(transition.to_state),'transition_state');trigger(transition.trigger_binding_id,transition.condition,transition.transition_id);
   const sequence=[...transition.exit,...transition.transition,...transition.enter];need(sequence.every(step=>step.required&&step.failure_policy==='stop'),'transition_optional');
   branch(transition.transition_id,sequence.map(step=>[step]));
  }
 }
 for(const binding of compiled.input_bindings)if(binding.source==='step_result'){
  const producer=steps.get(binding.producer_step_id)?.step,action=producer?.kind==='action'&&actions.get(producer.action_binding_id)?.binding;
  need(action&&same(binding.schema_ref,action.result_schema_ref),'result_schema');
 }
 function dependency(from,to,kind){
  const predecessor=steps.get(from),consumer=steps.get(to);
  need(predecessor&&consumer&&predecessor.branch===consumer.branch&&predecessor.rank<consumer.rank,'dependency_order',to);add(from,to,kind);
 }
 function references(expression,consumer,kind,reads){
  for(const reference of expression.references){reads.push(...reference.reads.map(physical));if(reference.source==='step_result')dependency(reference.producer_step_id,consumer,kind);}
 }
 for(const [id,info]of steps){
  const step=info.step;for(const predecessor of step.depends_on)dependency(predecessor,id,'requires_success');
  if(step.kind==='delay'){info.footprint=Object.fromEntries(footprintKeys.map(key=>[key,[]]));continue;}
  const action=actions.get(step.action_binding_id);need(action,'action_binding',id);
  need(step.failure_policy!=='continue_if_independent'||!step.required,'required_continue',id);
  const footprint=Object.fromEntries(footprintKeys.map(key=>[key,action.binding.footprint[key].map(physical)]));
  const guard=expressions.compile(step.guard,{path:id+'/guard'});references(guard,id,'guard',footprint.reads);
  const targets=[],parameters=[];
  for(const parameter of step.parameter_bindings){
   const tokens=pointerTokens(parameter.target_pointer),slot=action.slots.get(parameter.target_pointer);need(slot,'parameter_slot',id);
   for(const previous of targets)need(!(previous.length<=tokens.length&&previous.every((token,i)=>token===tokens[i]))&&!(tokens.length<=previous.length&&tokens.every((token,i)=>token===previous[i])),'parameter_overlap',id);
   targets.push(tokens);const value=expressions.compile(parameter.value,{condition:false,path:id+'/parameters'+parameter.target_pointer});
   need(value.tree.kind==='literal'?valueMatches(value.tree.value,slot.value_type):same(value.tree.value_type,slot.value_type),'parameter_type',id);
   references(value,id,'data',footprint.reads);parameters.push({pointer:parameter.target_pointer,value});
  }
  info.footprint=footprint;plans.set(id,{guard,parameters});
 }
 // 所有足迹参与静态依赖；complete 字段和内容摘要不能替代权威批准。
 for(const members of branches)for(let i=0;i<members.length;i++){
  const before=members[i],writes=[...before.footprint.writes,...before.footprint.failure_writes,...before.footprint.resources];
  for(let j=i+1;j<members.length;j++){
   const after=members[j],observes=footprintKeys.flatMap(key=>after.footprint[key]);
   const conflict=writes.some(a=>observes.some(b=>intersects(a,b)));
   if(before.rank===after.rank){
    const reverse=[...after.footprint.writes,...after.footprint.failure_writes,...after.footprint.resources].some(a=>footprintKeys.some(key=>before.footprint[key].some(b=>intersects(a,b))));
    need(!conflict&&!reverse,'parallel_resource_conflict',after.step.step_id);
   }else if(conflict)add(before.step.step_id,after.step.step_id,'resource');
  }
 }
 const orderedEdges=[...edges.values()].sort((a,b)=>compare(a.from_step_id,b.from_step_id)||compare(a.to_step_id,b.to_step_id)||compare(a.edge_kind,b.edge_kind));
 const graph=freeze({graph_version:'automation-graph/1',nodes:[...steps.keys()].sort(compare),edges:orderedEdges,alias_table_ref:compiled.alias_table.ref});
 compiled.graph_digest=hash(graph);validateAutomationModel('CompiledProgram',compiled);freeze(compiled);
 const adjacency=new Map([...steps.keys()].map(id=>[id,new Set()]));for(const edge of orderedEdges)adjacency.get(edge.from_step_id).add(edge.to_step_id);
 return Object.freeze({compiled,graph,program_digest:hash(compiled),bindings_digest:hash({input_bindings:compiled.input_bindings,action_bindings:compiled.action_bindings,alias_table:compiled.alias_table}),
  approved:false,runtime_ready:false,condition:key=>conditions.get(key),stepPlan:id=>{const plan=plans.get(id);return plan?freeze(plan):null;},
  parameterExecution({snapshots,schemas}){return createParameterExecution({compiled,plans,steps,snapshots,schemas});},
  successors(id){need(steps.has(id),'step_missing');const seen=new Set(),pending=[...adjacency.get(id)];while(pending.length){const next=pending.pop();if(seen.has(next))continue;seen.add(next);pending.push(...adjacency.get(next));}return Object.freeze([...seen].sort(compare));}
 });
}
