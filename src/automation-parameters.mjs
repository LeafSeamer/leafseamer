import crypto from 'node:crypto';
import canonicalize from 'canonicalize';
import {validateAutomationModel} from './sdk/automation-program.mjs';
import {freeze,need,pointerTokens,evaluateValue} from './automation-expression.mjs';
import {validateConfiguration} from './validation/configuration-validator.mjs';

const hash=value=>'sha256:'+crypto.createHash('sha256').update(canonicalize(value)).digest('hex');
const same=(a,b)=>canonicalize(a)===canonicalize(b);
const object=value=>value!==null&&typeof value==='object'&&!Array.isArray(value);
function json(value){validateAutomationModel('Value',{kind:'literal',value});return freeze(structuredClone(value));}
function assign(parameters,pointer,value){
 const tokens=pointerTokens(pointer);if(!tokens.length)return structuredClone(value);
 let parent=parameters;
 for(const [index,token]of tokens.entries()){
  need(parent!==null&&typeof parent==='object','parameter_target',pointer);
  if(Array.isArray(parent))need(/^(0|[1-9][0-9]*)$(?![\s\S])/.test(token)&&BigInt(token)<BigInt(parent.length),'parameter_target',pointer);
  if(index===tokens.length-1){Object.defineProperty(parent,token,{value:structuredClone(value),enumerable:true,writable:true,configurable:true});break;}
  need(Object.hasOwn(parent,token),'parameter_target',pointer);parent=parent[token];
 }
 return parameters;
}

// 仅处理已编译程序的参数。Schema及快照须由可信装配取得；内容摘要不是来源批准。
// 该对象只在本次进程内锁存求值，不替代执行账本、当前权限或最终设备出口复核。
export function createParameterExecution({compiled,plans,steps,snapshots,schemas}){
 need(snapshots instanceof Map&&schemas instanceof Map,'parameter_inputs');
 const fixed=new Map(),documents=new Map(),resolved=new Map();let bytes=0;
 const bounded=value=>{const copy=json(value);bytes+=Buffer.byteLength(canonicalize(copy));need(bytes<=16777216,'parameter_budget');return copy;};
 for(const binding of compiled.input_bindings){
  if(binding.source==='step_result')continue;
  const value=snapshots.get(binding.binding_id);if(value!==undefined)fixed.set(binding.binding_id,bounded(value));
 }
 for(const binding of compiled.action_bindings){
  const selected=schemas.get(binding.binding_id);need(selected!==undefined,'parameter_schema_missing',binding.binding_id);
  const copy=bounded(selected);need(object(copy)&&Object.keys(copy).sort().join(',')==='document,schema_ref'&&same(copy.schema_ref,binding.parameter_schema_ref),'parameter_schema_binding',binding.binding_id);
  need(hash(copy.document)===copy.schema_ref.digest,'parameter_schema_digest',binding.binding_id);
  const encoded=Buffer.from(canonicalize(copy.document));need(encoded.length<=1048576,'parameter_schema_budget');documents.set(binding.binding_id,encoded);
 }
 function resolve(stepId,{results=new Map()}={}){
  need(steps.has(stepId)&&steps.get(stepId).step.kind==='action','parameter_step');
  if(resolved.has(stepId))return resolved.get(stepId);
  // Promise先登记，失败也保留；重复调用不能更换结果快照或重新执行Schema校验。
  let finish,fail;const promise=new Promise((yes,no)=>{finish=yes;fail=no;});resolved.set(stepId,promise);
  try{
   need(results instanceof Map,'parameter_inputs');
   const values=new Map(fixed),plan=plans.get(stepId),step=steps.get(stepId).step;
   for(const parameter of plan.parameters)for(const reference of parameter.value.references){
    if(reference.source!=='step_result'||values.has(reference.binding_id))continue;
    const snapshot=results.get(reference.binding_id);if(snapshot!==undefined)values.set(reference.binding_id,bounded(snapshot));
   }
   let parameters=structuredClone(step.parameters),workBytes=Buffer.byteLength(canonicalize(parameters));const diagnostics=[];
   for(const parameter of plan.parameters){
    const result=evaluateValue(parameter.value,values);
    if(result.state!=='available'){diagnostics.push({code:result.state,pointer:parameter.pointer});continue;}
    workBytes+=Buffer.byteLength(canonicalize(result.value));need(workBytes<=16777216,'parameter_budget');
    parameters=assign(parameters,parameter.pointer,result.value);
   }
   if(diagnostics.length){finish(freeze({status:'unknown',diagnostics}));return promise;}
   need(object(parameters),'parameter_object');
   const encoded=Buffer.from(canonicalize(parameters));need(encoded.length<=1048576,'parameter_budget');
   const binding=compiled.action_bindings.find(item=>item.binding_id===step.action_binding_id);
   validateConfiguration(documents.get(step.action_binding_id),encoded).then(value=>{
    try{need(same(value,parameters),'parameter_validation_changed');finish(freeze({status:'validated',parameters:json(value),parameters_digest:hash(value),parameter_schema_ref:binding.parameter_schema_ref,action_binding_id:step.action_binding_id}));}catch(error){fail(error);}
   },fail);
  }catch(error){fail(error);}
  return promise;
 }
 return Object.freeze({resolve});
}
