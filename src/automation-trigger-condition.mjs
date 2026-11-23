import {digest,same} from './runtime/auth-clock-value.mjs';
import {evaluateExpression} from './automation-expression.mjs';

const ordered=(a,b)=>a<b?-1:a>b?1:0;

export function eventInputSnapshots(inputBindings,origin){
 const source=origin.source_key,registration={id:origin.binding_ref.registration_id,revision:origin.binding_ref.revision,digest:digest(origin.binding_ref)};
 const {schema_ref,value}=origin.payload;
 const schema={id:schema_ref.object_id,revision:schema_ref.revision,digest:schema_ref.digest};
 const snapshots=new Map();
 if(schema_ref.scope.authority_service_id===source.authority_service_id&&schema_ref.scope.database_instance_id===source.database_instance_id){
  for(const input of inputBindings){
   if(input.source==='event'&&input.authority_id===source.authority_service_id&&same(input.registration_ref,registration)&&same(input.schema_ref,schema))
    snapshots.set(input.binding_id,{state:'available',value});
  }
 }
 return snapshots;
}

export function evaluateTriggerCondition(compiled,run,target,origin,charge){
 const {program,input_bindings}=compiled.compiled;
 charge(input_bindings.length+(program.kind==='rule'?1:program.transitions.length));
 const snapshots=eventInputSnapshots(input_bindings,origin);
 const bindings=new Set(target.bindings.map(item=>item.binding_id));
 const branches=program.kind==='rule'?[{id:'rule',priority:0,binding:program.trigger_binding_id}]:program.transitions
  .filter(item=>item.from_state===run.business_state?.state_id&&bindings.has(item.trigger_binding_id))
  .map(item=>({id:item.transition_id,priority:item.priority,binding:item.trigger_binding_id}));
 if(branches.some(item=>!snapshots.has(item.binding)))return null;
 const work=branches.map(item=>({...item,condition:compiled.condition(item.id)}));
 charge(work.reduce((total,item)=>total+item.condition.nodes,0));
 const outcomes=work.map(({id,priority,condition})=>({id,priority,value:evaluateExpression(condition,snapshots).value})).sort((a,b)=>ordered(a.id,b.id));
 const known=outcomes.filter(item=>item.value===true),unknown=outcomes.filter(item=>item.value==='unknown');
 const priority=known.length?Math.max(...known.map(item=>item.priority)):null;
 const winners=known.filter(item=>item.priority===priority);
 const outcome=winners.length>1?'ambiguous':unknown.some(item=>priority===null||item.priority>=priority)?'evaluation_unknown':winners.length?'matched':'no_match';
 return {outcome,selected_branch:outcome==='matched'?winners[0].id:null,outcomes,
  qualified_input_binding_ids:[...snapshots.keys()].sort(ordered)};
}
