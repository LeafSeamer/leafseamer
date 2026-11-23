import {canonical,digest,same} from './runtime/auth-clock-value.mjs';
import {pointerTokens,readPointer} from './automation-expression.mjs';
import {checked,need} from './automation-publication.mjs';
import {validateConfiguration} from './validation/configuration-validator.mjs';

const exact=(value,names)=>value&&Object.keys(value).sort().join(',')===names;
const compare=(a,b)=>a<b?-1:a>b?1:0;
const tuple=({device_id,action_id,capability_version})=>[device_id,action_id,capability_version];
const escape=token=>token.replaceAll('~','~0').replaceAll('/','~1');
const encoded=value=>Buffer.from(canonical(value));

function pointerSchema(document,tokens){
 let selected=document,path='';
 for(const token of tokens){
  if(selected?.properties&&Object.hasOwn(selected.properties,token)){
   selected=selected.properties[token];path+='/properties/'+escape(token);continue;
  }
  need(/^(0|[1-9][0-9]*)$(?![\s\S])/.test(token),'run_constraint_pointer');
  const index=Number(token),prefix=selected?.prefixItems;
  if(prefix&&Object.hasOwn(prefix,index)){selected=prefix[index];path+='/prefixItems/'+token;}
  else {selected=selected?.items;path+='/items';}
  need(selected!==undefined&&selected!==true,'run_constraint_pointer');
 }
 need(selected!==undefined&&selected!==false,'run_constraint_pointer');
 // 将原参数文档保留为独立的 Schema 资源，内部片段仍相对于原文档解析。
 const resource=typeof document==='object'?{$id:'urn:leafseamer:run-approval-parameters',...document}:document;
 const valuesSchema={type:'array',$defs:{parameter_document:resource},items:{$ref:'#/$defs/parameter_document'+path}};
 return {selected,valuesSchema};
}

export function assertRunGrantParameters(grant,action,parameters){
 need(grant.device_actions.some(value=>same(value,action)),'execution_grant_action');
 for(const condition of grant.parameter_constraints){
  if(!same(tuple(condition),tuple(action)))continue;
  const actual=readPointer(parameters,condition.parameter_pointer),rule=condition.rule;
  need(actual.present,'execution_grant_parameters');
  const {value}=actual;
  const matches=rule.kind==='equals'?same(value,rule.value):rule.kind==='one_of'?rule.values.some(item=>same(value,item)):
   rule.kind==='range'&&typeof value==='number'&&Number.isFinite(value)&&value>=rule.minimum&&value<=rule.maximum;
  need(matches,'execution_grant_parameters');
 }
}

export async function constrainRunTargets(groups,plan,publication){
 need(Array.isArray(groups)&&groups.length<=256,'run_constraint_groups');
 const targetMap=new Map(plan.targets.map(target=>[canonical(target.controller_scope),target])),byTarget=new Map();
 const schemas=new Map(publication.snapshot.schemas.map(schema=>[schema.binding_id,schema]));
 for(const group of groups){
  need(exact(group,'constraints,controller_scope')&&Array.isArray(group.constraints)&&group.constraints.length<=64,'run_constraint_group');
  const key=canonical(group.controller_scope),target=targetMap.get(key);
  need(target&&!byTarget.has(key),'run_constraint_target');
  const entries=new Map();
  for(const condition of group.constraints){
   need(exact(condition,'action_id,capability_version,device_id,parameter_pointer,rule'),'run_constraint');
   const action=target.device_actions.find(value=>same(tuple(value),tuple(condition)));
   need(action,'run_constraint_action');
   const pointer=pointerTokens(condition.parameter_pointer),rule=condition.rule;
   need(pointer.every(segment=>!segment.includes('*')),'run_constraint_pointer');
   const evidence=plan.evidence.filter(item=>same(item.controller_scope,target.controller_scope)&&same(item.device_action,action));
   need(evidence.length>0,'run_constraint_action');
   const schemasForAction=evidence.map(item=>schemas.get(item.binding_id));
   need(schemasForAction.every(schema=>schema&&digest(schema.document)===schema.schema_ref.digest&&same(schema.schema_ref,schemasForAction[0].schema_ref)),'run_constraint_schema');
   const selected=pointerSchema(schemasForAction[0].document,pointer),address=canonical([...tuple(action),condition.parameter_pointer]);
   const entry=entries.get(address)??{schema:selected.valuesSchema,conditions:[],values:null,lower:-Infinity,upper:Infinity};
   if(rule?.kind==='range'){
    need(exact(rule,'kind,maximum,minimum')&&Number.isFinite(rule.minimum)&&Number.isFinite(rule.maximum)&&rule.minimum<=rule.maximum&&
     ['number','integer'].includes(selected.selected?.type),'run_constraint_range');
    need(selected.selected.type!=='integer'||Number.isSafeInteger(rule.minimum)&&Number.isSafeInteger(rule.maximum),'run_constraint_range');
    entry.lower=Math.max(entry.lower,rule.minimum);entry.upper=Math.min(entry.upper,rule.maximum);
   }else{
    const values=rule?.kind==='equals'&&exact(rule,'kind,value')?[rule.value]:rule?.kind==='one_of'&&exact(rule,'kind,values')?rule.values:null;
    need(Array.isArray(values)&&values.length>=1&&values.length<=128,'run_constraint_values');
    const keyed=new Map(values.map(value=>{checked('Value',{kind:'literal',value});const bytes=encoded(value);need(bytes.length<=65536,'run_constraint_capacity');return [bytes.toString(),value];}));
    need(keyed.size===values.length,'run_constraint_values');
    const found=[...keyed].sort(([a],[b])=>compare(a,b));
    await validateConfiguration(encoded(entry.schema),encoded(found.map(([,value])=>value)));
    entry.values=entry.values===null?keyed:new Map([...entry.values].filter(([value])=>keyed.has(value)));
    if(rule.kind==='one_of')condition.rule.values=found.map(([,value])=>value);
   }
   const body=canonical(condition);
   need(!entry.conditions.some(item=>item.key===body),'run_constraint_duplicate');
   entry.conditions.push({key:body,condition});entries.set(address,entry);
  }
  for(const {lower,upper,values,schema} of entries.values()){
   need(lower<=upper&&(values===null||[...values.values()].some(value=>lower===-Infinity&&upper===Infinity||
    typeof value==='number'&&value>=lower&&value<=upper)),'run_constraint_empty');
   if(values===null)await validateConfiguration(encoded(schema),encoded([lower,upper]));
  }
  byTarget.set(key,[...entries].sort(([a],[b])=>compare(a,b)).flatMap(([,entry])=>entry.conditions.sort((a,b)=>compare(a.key,b.key)).map(item=>item.condition)));
 }
 return plan.targets.map(target=>({...target,parameter_constraints:byTarget.get(canonical(target.controller_scope))??[]}));
}
