import {validateAutomationModel} from './sdk/automation-program.mjs';
import canonicalize from 'canonicalize';
export class CompileError extends Error {constructor(code,path='',related=[]){super(code);this.name='CompileError';this.code=code;this.path=path;this.related_paths=related;}}
export const need=(condition,code,path='')=>{if(!condition)throw new CompileError(code,path);};
export const freeze=value=>{if(value&&typeof value==='object'){for(const child of Object.values(value))freeze(child);Object.freeze(value);}return value;};
const compiledExpressions=new WeakSet();
export function pointerTokens(pointer){
 need(typeof pointer==='string'&&pointer.length<=1024&&/^(?:\/(?:[^~/]|~[01])*)*$(?![\s\S])/.test(pointer)&&pointer.isWellFormed(),'pointer');
 const tokens=pointer===''?[]:pointer.slice(1).split('/').map(part=>part.replaceAll('~1','/').replaceAll('~0','~'));
 need(tokens.every(part=>!['__proto__','prototype','constructor','-'].includes(part)&&!part.includes('${')),'pointer');return tokens;
}
export function readPointer(value,pointer){
 for(const token of pointerTokens(pointer)){
  if(value===null||typeof value!=='object')return {present:false};
  if(Array.isArray(value)){if(!/^(0|[1-9][0-9]*)$(?![\s\S])/.test(token)||BigInt(token)>=BigInt(value.length))return {present:false};}
  const descriptor=Object.getOwnPropertyDescriptor(value,token);if(!descriptor)return {present:false};need(Object.hasOwn(descriptor,'value'),'pointer_accessor');value=descriptor.value;
 }
 return {present:true,value};
}
export function valueMatches(value,type){
 if(type.kind==='json')return true;
 if(type.kind==='null')return value===null;
 if(type.kind==='binary64')return typeof value==='number'&&Number.isFinite(value);
 if(type.kind==='integer_string')return typeof value==='string'&&/^(0|-?[1-9][0-9]{0,19})$(?![\s\S])/.test(value);
 return typeof value===type.kind;
}
export function typeValid(type){need(['binary64','integer_string'].includes(type.kind)||type.unit===null,'unit');return type;}
export function expressionCompiler(inputBindings){
 need(Array.isArray(inputBindings)&&inputBindings.length<=4096,'binding_budget');
 const bindings=new Map();for(const input of inputBindings){validateAutomationModel('InputBinding',input);const binding=freeze(structuredClone(input));need(!bindings.has(binding.binding_id),'duplicate_binding');
  const projections=new Map();for(const p of binding.projections){pointerTokens(p.pointer);typeValid(p.value_type);need(!projections.has(p.pointer),'duplicate_projection');projections.set(p.pointer,p);}
  need((binding.source==='step_result')===(binding.producer_step_id!==null),'producer_binding');bindings.set(binding.binding_id,{binding,projections});}
 let total=0;
 function compile(expression,{condition=true,path=''}={}){
  validateAutomationModel(condition?'Condition':'Value',expression);expression=structuredClone(expression);let nodes=0;const references=[];
  function visit(node,depth,location,boolean){
   need(depth<=16&&++nodes<=256&&total+nodes<=16384,'expression_budget',location);
   if(node.kind==='literal')return {kind:'literal',value:node.value};
   if(node.kind==='ref'){
    const item=bindings.get(node.binding_id);need(item&&item.binding.source===node.source,'reference_binding',location);pointerTokens(node.pointer);
    const projection=item.projections.get(node.pointer);need(projection,'reference_projection',location);references.push({binding_id:node.binding_id,pointer:node.pointer,source:node.source,producer_step_id:item.binding.producer_step_id,reads:projection.reads});
    return {...node,value_type:projection.value_type};
   }
   need(boolean,'value_kind',location);
   if(node.kind==='compare'){
    const type=typeValid(node.value_type),left=visit(node.left,depth+1,location+'/left',false),right=visit(node.right,depth+1,location+'/right',false);
    need(type.kind!=='json'&&(['eq','ne'].includes(node.operator)||['binary64','integer_string'].includes(type.kind)),'comparison_type',location);
    for(const operand of [left,right])need(operand.kind==='literal'?valueMatches(operand.value,type):canonicalize(operand.value_type)===canonicalize(type),'comparison_type',location);
    return {...node,left,right};
   }
   if(node.kind==='all'||node.kind==='any')return {...node,items:node.items.map((item,i)=>visit(item,depth+1,location+'/items/'+i,true))};
   if(node.kind==='not')return {...node,item:visit(node.item,depth+1,location+'/item',true)};
   return {...node,value:visit(node.value,depth+1,location+'/value',false)};
  }
  const tree=visit(expression,1,path,condition);
  total+=nodes;const result=freeze({tree,references,nodes,condition});compiledExpressions.add(result);return result;
 }
 return Object.freeze({compile,binding:id=>bindings.get(id)?.binding,totalNodes:()=>total});
}
// snapshot在执行创建或步骤就绪时由可信来源适配器固定，普通事件payload不拥有这些状态标志。
function expressionValue(node,snapshots){
 if(node.kind==='literal')return {available:true,present:true,value:node.value};
 const snapshot=snapshots.get(node.binding_id);
 if(!snapshot||snapshot.state!=='available'||node.source==='step_result'&&(snapshot.outcome!=='succeeded'||snapshot.evidence_qualified!==true))return {available:false};
 return {available:true,...readPointer(snapshot.value,node.pointer)};
}
export function evaluateValue(compiled,snapshots){
 need(compiledExpressions.has(compiled)&&!compiled.condition,'compiled_value');
 const selected=expressionValue(compiled.tree,snapshots);
 if(!selected.available)return Object.freeze({state:'source_unavailable'});
 if(!selected.present)return Object.freeze({state:'missing'});
 if(compiled.tree.kind==='ref'&&!valueMatches(selected.value,compiled.tree.value_type))return Object.freeze({state:'type_mismatch'});
 validateAutomationModel('Value',{kind:'literal',value:selected.value});
 return freeze({state:'available',value:structuredClone(selected.value)});
}
export function evaluateExpression(compiled,snapshots){
 need(compiledExpressions.has(compiled)&&compiled.condition,'compiled_condition');
 const diagnostics=[],dependencies=compiled.references.map(ref=>({binding_id:ref.binding_id,pointer:ref.pointer}));
 function value(node){
  const selected=expressionValue(node,snapshots);if(!selected.available)diagnostics.push({code:'source_unavailable',binding_id:node.binding_id});return selected;
 }
 function evaluate(node){
  if(node.kind==='literal')return node.value;
  if(node.kind==='compare'){
   const a=value(node.left),b=value(node.right);if(!a.available||!b.available||!a.present||!b.present||!valueMatches(a.value,node.value_type)||!valueMatches(b.value,node.value_type))return 'unknown';
   const left=node.value_type.kind==='integer_string'?BigInt(a.value):a.value,right=node.value_type.kind==='integer_string'?BigInt(b.value):b.value;
   return ({eq:()=>left===right,ne:()=>left!==right,lt:()=>left<right,lte:()=>left<=right,gt:()=>left>right,gte:()=>left>=right})[node.operator]();
  }
  if(node.kind==='all'||node.kind==='any'){const results=node.items.map(evaluate);return node.kind==='all'?(results.includes(false)?false:results.includes('unknown')?'unknown':true):(results.includes(true)?true:results.includes('unknown')?'unknown':false);}
  if(node.kind==='not'){const result=evaluate(node.item);return result==='unknown'?'unknown':!result;}
  const selected=value(node.value);if(!selected.available)return 'unknown';return node.kind==='exists'?selected.present:selected.present?selected.value===null:'unknown';
 }
 return Object.freeze({value:evaluate(compiled.tree),dependencies,diagnostics});
}
