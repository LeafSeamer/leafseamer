import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import {pathToFileURL} from 'node:url';
import {source,input,reference,ref,digest} from './automation-program-fixture.mjs';
const packageRoot=process.env.LEAFSEAMER_PARAMETER_PACKAGE_UNDER_TEST;
if(packageRoot)assert.ok(path.isAbsolute(packageRoot));
const {compileProgram}=await import(packageRoot?pathToFileURL(path.join(packageRoot,'dist/automation-compiler.mjs')):new URL('../dist/automation-compiler.mjs',import.meta.url));
const booleanSchema={type:'object',properties:{value:{type:'boolean'}},required:['value'],additionalProperties:false};
const available=value=>({state:'available',value});
function setup({schema=booleanSchema,program=source(),snapshots=new Map()}={}){
 schema=structuredClone(schema);
 const schemaRef={...ref,id:'parameters',digest:digest(schema)};
 for(const action of program.action_bindings)action.parameter_schema_ref=schemaRef;
 const compiled=compileProgram(program),schemas=new Map(program.action_bindings.map(action=>[action.binding_id,{schema_ref:schemaRef,document:schema}]));
 return {compiled,schemas,program,execution:compiled.parameterExecution({snapshots,schemas})};
}
function bind(program,pointer='/value',value=reference()){
 program.program.groups[0].steps[0].parameter_bindings=[{target_pointer:pointer,value}];
}

test('materialize from immutable execution snapshot and validate the entire parameter object',async()=>{
 const program=source(),snapshot=available({value:true});bind(program);
 const snapshots=new Map([['trigger',snapshot]]),selected=setup({program,snapshots});
 snapshot.value.value=false;snapshots.clear();selected.schemas.get('action').document.properties.value.type='string';
 const result=await selected.execution.resolve('first');
 assert.deepEqual(result.parameters,{value:true});assert.equal(result.parameters_digest,digest({value:true}));
 assert.deepEqual(result.parameter_schema_ref,selected.program.action_bindings[0].parameter_schema_ref);
 assert.ok(Object.isFrozen(result.parameters));assert.equal(selected.compiled.approved,false);assert.equal(selected.compiled.runtime_ready,false);
 assert.throws(()=>Function('return 1'),EvalError);
});

test('full schema enforces static fields, conditionals, bounds and additional properties',async()=>{
 const schema={type:'object',properties:{mode:{enum:['small','large']},count:{type:'integer',minimum:1,maximum:10}},required:['mode','count'],additionalProperties:false,
  if:{properties:{mode:{const:'small'}}},then:{properties:{count:{maximum:2}}},else:{properties:{count:{minimum:5}}}};
 const run=async parameters=>{const program=source();program.program.groups[0].steps[0].parameters=parameters;return setup({schema,program}).execution.resolve('first');};
 assert.deepEqual((await run({mode:'small',count:2})).parameters,{mode:'small',count:2});
 for(const value of [{mode:'small',count:3},{mode:'large',count:3},{mode:'large',count:11},{mode:'large',count:5.5},{mode:'large',count:5,extra:true},{count:5}]){
  await assert.rejects(run(value),error=>error.code==='extension_configuration_schema_rejected'||error.code==='extension_configuration_raw_integer');
 }
 const program=source(),type={kind:'binary64',unit:null};program.input_bindings[0].projections[0].value_type=type;
 program.action_bindings[0].parameter_slots=[{pointer:'/count',value_type:type}];program.program.groups[0].steps[0].parameters={mode:'small',count:0};bind(program,'/count');
 const dynamic=value=>setup({program,schema,snapshots:new Map([['trigger',available({value})]])}).execution.resolve('first');
 assert.deepEqual((await dynamic(2)).parameters,{mode:'small',count:2});await assert.rejects(dynamic(3),{code:'extension_configuration_schema_rejected'});
});

test('step results need qualified success and are captured once before schema validation',async()=>{
 const program=source();program.input_bindings.push(input('result','step_result','first'));
 program.program.groups[0].steps[1].parameter_bindings=[{target_pointer:'/value',value:reference('result','step_result')}];
 const {execution}=setup({program}),snapshot={...available({value:true}),outcome:'succeeded',evidence_qualified:true},results=new Map([['result',snapshot]]);
 const first=execution.resolve('second',{results});snapshot.value.value=false;
 const repeated=execution.resolve('second',{results:new Map()});assert.equal(first,repeated);
 assert.deepEqual((await first).parameters,{value:true});
 for(const partial of [{outcome:'failed',evidence_qualified:true},{outcome:'succeeded',evidence_qualified:false},{}]){
  const {execution:other}=setup({program});const failed=other.resolve('second',{results:new Map([['result',{...available({value:true}),...partial}]])});
  assert.equal((await failed).status,'unknown');
  assert.equal(other.resolve('second',{results:new Map([['result',{...available({value:true}),outcome:'succeeded',evidence_qualified:true}]])}),failed);
 }
});

test('missing, unavailable and wrong type remain unknown while explicit null is a value',async()=>{
 const program=source();bind(program);
 for(const [snapshot,code]of [[undefined,'source_unavailable'],[{state:'unavailable'},'source_unavailable'],[available({}),'missing'],[available({value:null}),'type_mismatch'],[available({value:'true'}),'type_mismatch']]){
  const {execution}=setup({program,snapshots:new Map([['trigger',snapshot]])});
  assert.deepEqual(await execution.resolve('first'),{status:'unknown',diagnostics:[{code,pointer:'/value'}]});
 }
 const nullable=source();nullable.input_bindings[0].projections[0].value_type={kind:'json',unit:null};nullable.action_bindings[0].parameter_slots[0].value_type={kind:'json',unit:null};bind(nullable);
 const {execution}=setup({program:nullable,schema:{type:'object',properties:{value:{type:['boolean','null']}},required:['value'],additionalProperties:false},snapshots:new Map([['trigger',available({value:null})]])});
 assert.deepEqual((await execution.resolve('first')).parameters,{value:null});
});

test('pointer assignment preserves escapes and object keys but never appends or invents parents',async()=>{
 async function run(pointer,parameters,value=true){
  const program=source();program.action_bindings[0].parameter_slots=[{pointer,value_type:{kind:'json',unit:null}}];
  program.program.groups[0].steps[0].parameters=parameters;bind(program,pointer,{kind:'literal',value});
  return setup({program,schema:{type:'object'}}).execution.resolve('first');
 }
 assert.deepEqual((await run('/a~1b/~0key',{'a/b':{'~key':false}})).parameters,{'a/b':{'~key':true}});
 assert.deepEqual((await run('/01',{})).parameters,{'01':true});
 assert.deepEqual((await run('/list/0',{list:[false]})).parameters,{list:[true]});
 assert.deepEqual((await run('',{old:true},{new:true})).parameters,{new:true});
 for(const [pointer,parameters]of [['/missing/value',{}],['/list/01',{list:[false,false]}],['/list/1',{list:[false]}],['/value/child',{value:null}]])await assert.rejects(run(pointer,parameters),{code:'parameter_target'});
 await assert.rejects(run('',{},null),{code:'parameter_object'});
 for(const pointer of ['/__proto__/polluted','/constructor/prototype','/list/-'])await assert.rejects(run(pointer,{}),{code:'pointer'});
 assert.equal({}.polluted,undefined);
});

test('schema identity and content are mandatory; invalid and external schemas fail closed',async()=>{
 const {compiled,schemas}=setup();
 assert.throws(()=>compiled.parameterExecution({snapshots:new Map(),schemas:new Map()}),{code:'parameter_schema_missing'});
 const changed=structuredClone(schemas);changed.get('action').schema_ref.id='other';
 assert.throws(()=>compiled.parameterExecution({snapshots:new Map(),schemas:changed}),{code:'parameter_schema_binding'});
 changed.get('action').schema_ref.id='parameters';changed.get('action').document={type:'string'};
 assert.throws(()=>compiled.parameterExecution({snapshots:new Map(),schemas:changed}),{code:'parameter_schema_digest'});
 await assert.rejects(setup({schema:{$ref:'https://example.invalid/schema'}}).execution.resolve('first'),{code:'extension_configuration_external_reference'});
 await assert.rejects(setup({schema:{type:'not-a-type'}}).execution.resolve('first'),{code:'extension_configuration_schema_compile'});
});

test('validation failure cannot be retried with new results in the same execution',async()=>{
 const program=source();program.input_bindings.push(input('result','step_result','first'));
 program.program.groups[0].steps[1].parameter_bindings=[{target_pointer:'/value',value:reference('result','step_result')}];
 const {execution}=setup({program,schema:{type:'object',properties:{value:{const:true}},required:['value']}});
 const result=value=>new Map([['result',{...available({value}),outcome:'succeeded',evidence_qualified:true}]]);
 const failure=execution.resolve('second',{results:result(false)});await assert.rejects(failure,{code:'extension_configuration_schema_rejected'});
 assert.equal(execution.resolve('second',{results:result(true)}),failure);await assert.rejects(failure);
});

test('snapshots reject accessors and resolved parameters remain inside the worker byte budget',async()=>{
 const program=source();bind(program);let reads=0;
 assert.throws(()=>setup({program,snapshots:new Map([['trigger',{state:'available',get value(){++reads;return {value:true};}}]])}),{code:'automation_json_object'});assert.equal(reads,0);
 const large=source();large.program.groups[0].steps[0].parameters={large:'a'.repeat(1048576)};
 await assert.rejects(setup({program:large,schema:{type:'object'}}).execution.resolve('first'),{code:'parameter_budget'});
});
