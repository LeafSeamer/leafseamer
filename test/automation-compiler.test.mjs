import test from 'node:test';
import assert from 'node:assert/strict';
import {compileProgram} from '../dist/automation-compiler.mjs';
import {evaluateScheduledCondition,evaluateTriggerCondition} from '../dist/automation-trigger-condition.mjs';
import {selectedExecutionSteps} from '../dist/automation-execution-store.mjs';
import {source,action,step,delay,input,reference,literal,ref,digest} from './automation-program-fixture.mjs';

test('derive exact ordered graph including result, guard and resource dependencies',()=>{
 const value=source(),first=action('producer'),second=action('consumer');first.footprint.writes=['logical/channel'];second.footprint.reads=['device/channel'];
 value.action_bindings=[first,second];value.alias_table.entries=[{logical:'logical',physical:'device'}];value.alias_table.ref.digest=digest(value.alias_table.entries);
 value.input_bindings.push(input('result','step_result','first'));
 const consumer=step('second','consumer');consumer.depends_on=['first'];consumer.parameter_bindings=[{target_pointer:'/value',value:reference('result','step_result')}];consumer.guard={kind:'exists',value:reference('result','step_result')};
 value.program.groups[0].steps=[step('first','producer'),consumer,delay('third')];const result=compileProgram(value);
 assert.deepEqual(result.graph,{graph_version:'automation-graph/1',nodes:['first','second','third'],edges:[...['data','guard','requires_success','resource','sequence'].map(edge_kind=>({from_step_id:'first',to_step_id:'second',edge_kind})),{from_step_id:'second',to_step_id:'third',edge_kind:'sequence'}],alias_table_ref:value.alias_table.ref});
 assert.equal(result.compiled.graph_digest,digest(result.graph));assert.deepEqual(result.successors('first'),['second','third']);assert.equal(result.approved,false);assert.equal(result.runtime_ready,false);
 assert.deepEqual(result.dependencies.inputs.map(({binding_id,uses})=>[binding_id,uses]),[['result',['guard','parameter']],['trigger',['trigger']]]);
 assert.deepEqual(result.dependencies.resources.reads,['device/channel']);
 assert.deepEqual(result.dependencies.resources.writes,['device/channel']);
});
test('full successor graph is retained beyond 256 steps and literal payload affects identity',()=>{
 const value=source(),steps=Array.from({length:301},(_,i)=>delay('s'+String(i).padStart(3,'0')));value.program.groups=[];
 for(let i=0;i<steps.length;i+=64)value.program.groups.push({mode:'sequence',steps:steps.slice(i,i+64)});
 const result=compileProgram(value);assert.equal(result.graph.nodes.length,301);assert.equal(result.successors('s000').length,300);assert.equal(result.successors('s000').at(-1),'s300');
 const other=source();other.program.groups[0].steps[0].parameters={step_id:'payload',independence_proof:{kind:'ref',binding_id:'unbound'}};
 const before=compileProgram(other);other.program.groups[0].steps[0].parameters.step_id='changed';const after=compileProgram(other);
 assert.notEqual(before.program_digest,after.program_digest);assert.equal(before.compiled.graph_digest,after.compiled.graph_digest);assert.equal(before.compiled.program.groups[0].steps[0].parameters.step_id,'payload');
 assert.deepEqual(before.dependencies,after.dependencies);
});
test('reject duplicate, forward and same parallel unit dependencies',()=>{
 let value=source();value.program.groups[0].steps[1].step_id='first';assert.throws(()=>compileProgram(value),{code:'duplicate_step'});
 value=source();value.program.groups[0].steps[0].depends_on=['second'];assert.throws(()=>compileProgram(value),{code:'dependency_order'});
 value=source();value.program.groups[0].mode='parallel';value.program.groups[0].steps[1].depends_on=['first'];assert.throws(()=>compileProgram(value),{code:'dependency_order'});
 value=source();value.program.groups[0].mode='parallel';value.program.groups[0].steps[0].required=false;assert.throws(()=>compileProgram(value),{code:'parallel_optional'});
 value=source();value.program.groups[0].steps[0].failure_policy='continue_if_independent';assert.throws(()=>compileProgram(value),{code:'required_continue'});
});
test('aliases use segments, verify digest and reveal parallel resource conflicts',()=>{
 let value=source();value.alias_table.ref.digest=ref.digest;assert.throws(()=>compileProgram(value),{code:'alias_digest'});
 value=source();value.alias_table.entries=[{logical:'a',physical:'b'},{logical:'a/c',physical:'d'}];value.alias_table.ref.digest=digest(value.alias_table.entries);assert.throws(()=>compileProgram(value),{code:'alias_overlap'});
 value=source();value.alias_table.entries=[{logical:'a',physical:'a/c'}];value.alias_table.ref.digest=digest(value.alias_table.entries);assert.throws(()=>compileProgram(value),{code:'alias_recursive'});
 value=source();value.program.groups[0].mode='parallel';const other=action('other');value.action_bindings.push(other);value.program.groups[0].steps[1].action_binding_id='other';
 value.action_bindings[0].footprint.writes=['device/a'];other.footprint.reads=['device/ab'];assert.doesNotThrow(()=>compileProgram(value));
 other.footprint.reads=['device/a/sub'];assert.throws(()=>compileProgram(value),{code:'parallel_resource_conflict'});
});
test('parameter slots, overlap, types and producer schemas are checked',()=>{
 let value=source();value.program.groups[0].steps[0].parameter_bindings=[{target_pointer:'/missing',value:literal}];assert.throws(()=>compileProgram(value),{code:'parameter_slot'});
 value=source();value.program.groups[0].steps[0].parameter_bindings=[{target_pointer:'/value',value:literal},{target_pointer:'/value',value:literal}];assert.throws(()=>compileProgram(value),{code:'parameter_overlap'});
 value=source();value.input_bindings.push(input('result','step_result','first'));value.input_bindings[1].schema_ref={...ref,id:'different'};assert.throws(()=>compileProgram(value),{code:'result_schema'});
 value=source();value.program.groups[0].steps[0]=delay('first');value.input_bindings.push(input('result','step_result','first'));assert.throws(()=>compileProgram(value),{code:'result_schema'});
 value=source();value.input_bindings.push(input('result','step_result','second'));value.program.groups[0].steps[0].guard={kind:'exists',value:reference('result','step_result')};assert.throws(()=>compileProgram(value),{code:'dependency_order'});
});
test('state cycles are allowed, transition action branches remain separate',()=>{
 const value=source(),transition=(id,from,to,steps)=>({transition_id:id,from_state:from,to_state:to,priority:0,trigger_binding_id:'trigger',condition:literal,exit:[],transition:steps,enter:[]});
 const other=action('other');other.controller_id='other-controller';other.footprint.target_constraints=['other-device/output'];
 value.action_bindings.push(other,action('unused'));
 const dormant=transition('stop','running','idle',[step('second','other')]);
 dormant.condition={kind:'literal',value:false};
 value.program={kind:'state_machine',states:['idle','running'],initial_state:'idle',transitions:[transition('start','idle','running',[step('first')]),dormant]};
 const result=compileProgram(value);assert.deepEqual(result.graph.edges,[]);assert.deepEqual(result.successors('first'),[]);
 assert.deepEqual(selectedExecutionSteps(result,{business_state:{state_id:'idle'}},'start').map(item=>[item.phase,item.step.step_id]),[['transition','first']]);
 assert.throws(()=>selectedExecutionSteps(result,{business_state:{state_id:'idle'}},'stop'),{code:'execution_plan_branch'});
 assert.deepEqual(result.dependencies.actions.map(({controller_id})=>controller_id),['controller','other-controller']);
 assert.deepEqual(result.dependencies.resources.target_constraints,['other-device/output']);
 value.program.transitions[1].transition[0].depends_on=['first'];assert.throws(()=>compileProgram(value),{code:'dependency_order'});
});

test('状态转换一次求值所有当前候选，低优先未知不覆盖胜者，并列和高优先未知保持保护',()=>{
 const fixed=source(),publisher={authority_service_id:'fixture',database_instance_id:'fixture-db'},binding={registration_id:'source',revision:'1'};
 const trigger=fixed.input_bindings[0];trigger.registration_ref={id:binding.registration_id,revision:binding.revision,digest:digest(binding)};
 fixed.input_bindings.push(input('current','observation'));
 fixed.action_bindings=[];
 const transition=(transition_id,priority,condition,from_state='idle')=>({transition_id,priority,from_state,to_state:'running',
  trigger_binding_id:'trigger',condition,exit:[],transition:[delay('delay-'+transition_id)],enter:[]});
 const unavailable={kind:'exists',value:reference('current','observation')};
 fixed.program={kind:'state_machine',states:['idle','running'],initial_state:'idle',transitions:[
  transition('win',2,literal),transition('uncertain',1,unavailable),transition('unrelated',9,literal,'running')]};
 const origin={source_key:publisher,binding_ref:binding,payload:{schema_ref:{scope:publisher,object_id:ref.id,revision:ref.revision,digest:ref.digest},value:{value:true}}};
 const run={business_state:{state_id:'idle'}},target={bindings:[{binding_id:'trigger'}]};
 const evaluate=(submitted=origin)=>evaluateTriggerCondition(compileProgram(fixed),run,target,submitted,n=>assert.ok(n>=2));
 const schedule=()=>evaluateScheduledCondition(compileProgram(fixed),run,()=>{});
 const selected=evaluate();assert.equal(selected.outcome,'matched');assert.equal(selected.selected_branch,'win');
 assert.deepEqual(selected.outcomes.map(({id,value})=>[id,value]),[['uncertain','unknown'],['win',true]]);
 assert.equal(schedule().selected_branch,'win');
 assert.deepEqual(schedule().qualified_input_binding_ids,[]);
 fixed.program.transitions[1].priority=2;
 assert.equal(evaluate().outcome,'evaluation_unknown');
 assert.equal(schedule().outcome,'evaluation_unknown');
 fixed.program.transitions[1].condition={kind:'exists',value:reference()};
 assert.equal(schedule().outcome,'evaluation_unknown');
 assert.equal(evaluate().outcome,'ambiguous');
 fixed.program.transitions[1].condition=literal;
 assert.equal(schedule().outcome,'ambiguous');
 const wrong=structuredClone(origin);wrong.payload.schema_ref.digest=digest('other schema');assert.equal(evaluate(wrong),null);
});
