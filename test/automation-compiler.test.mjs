import test from 'node:test';
import assert from 'node:assert/strict';
import {compileProgram} from '../dist/automation-compiler.mjs';
import {source,action,step,delay,input,reference,literal,ref,digest} from './automation-program-fixture.mjs';

test('derive exact ordered graph including result, guard and resource dependencies',()=>{
 const value=source(),first=action('producer'),second=action('consumer');first.footprint.writes=['logical/channel'];second.footprint.reads=['device/channel'];
 value.action_bindings=[first,second];value.alias_table.entries=[{logical:'logical',physical:'device'}];value.alias_table.ref.digest=digest(value.alias_table.entries);
 value.input_bindings.push(input('result','step_result','first'));
 const consumer=step('second','consumer');consumer.depends_on=['first'];consumer.parameter_bindings=[{target_pointer:'/value',value:reference('result','step_result')}];consumer.guard={kind:'exists',value:reference('result','step_result')};
 value.program.groups[0].steps=[step('first','producer'),consumer,delay('third')];const result=compileProgram(value);
 assert.deepEqual(result.graph,{graph_version:'automation-graph/1',nodes:['first','second','third'],edges:[...['data','guard','requires_success','resource','sequence'].map(edge_kind=>({from_step_id:'first',to_step_id:'second',edge_kind})),{from_step_id:'second',to_step_id:'third',edge_kind:'sequence'}],alias_table_ref:value.alias_table.ref});
 assert.equal(result.compiled.graph_digest,digest(result.graph));assert.deepEqual(result.successors('first'),['second','third']);assert.equal(result.approved,false);assert.equal(result.runtime_ready,false);
});
test('full successor graph is retained beyond 256 steps and literal payload affects identity',()=>{
 const value=source(),steps=Array.from({length:301},(_,i)=>delay('s'+String(i).padStart(3,'0')));value.program.groups=[];
 for(let i=0;i<steps.length;i+=64)value.program.groups.push({mode:'sequence',steps:steps.slice(i,i+64)});
 const result=compileProgram(value);assert.equal(result.graph.nodes.length,301);assert.equal(result.successors('s000').length,300);assert.equal(result.successors('s000').at(-1),'s300');
 const other=source();other.program.groups[0].steps[0].parameters={step_id:'payload',independence_proof:{kind:'ref',binding_id:'unbound'}};
 const before=compileProgram(other);other.program.groups[0].steps[0].parameters.step_id='changed';const after=compileProgram(other);
 assert.notEqual(before.program_digest,after.program_digest);assert.equal(before.compiled.graph_digest,after.compiled.graph_digest);assert.equal(before.compiled.program.groups[0].steps[0].parameters.step_id,'payload');
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
 value.program={kind:'state_machine',states:['idle','running'],initial_state:'idle',transitions:[transition('start','idle','running',[step('first')]),transition('stop','running','idle',[step('second')])]};
 const result=compileProgram(value);assert.deepEqual(result.graph.edges,[]);assert.deepEqual(result.successors('first'),[]);
 value.program.transitions[1].transition[0].depends_on=['first'];assert.throws(()=>compileProgram(value),{code:'dependency_order'});
});
