import test from 'node:test';
import assert from 'node:assert/strict';
import {expressionCompiler,evaluateExpression,readPointer,pointerTokens} from '../dist/automation-expression.mjs';
const ref={id:'fixture',revision:'1',digest:'sha256:'+'a'.repeat(64)};
const type={kind:'integer_string',unit:null};
const binding=()=>({binding_id:'input',source:'event',authority_id:'fixture',registration_ref:ref,schema_ref:ref,producer_step_id:null,projections:[{pointer:'/value',value_type:type,reads:['device/a']}]});
const reference={kind:'ref',source:'event',binding_id:'input',pointer:'/value'};
const compare=(right='9007199254740992')=>({kind:'compare',operator:'gt',value_type:type,left:reference,right:{kind:'literal',value:right}});
const snapshots=value=>new Map([['input',{state:'available',value}]]);
test('integer comparison is exact and unavailable values stay unknown',()=>{
 const compiler=expressionCompiler([binding()]),compiled=compiler.compile(compare());
 assert.equal(evaluateExpression(compiled,snapshots({value:'9007199254740993'})).value,true);
 assert.equal(evaluateExpression(compiled,snapshots({value:'9007199254740992'})).value,false);
 for(const value of [9007199254740992,'1\n','-0','01',null])assert.equal(evaluateExpression(compiled,snapshots({value})).value,'unknown');
 assert.equal(evaluateExpression(compiled,new Map()).value,'unknown');
 assert.throws(()=>compiler.compile(compare('1\n')),{code:'comparison_type'});
});
test('exists distinguishes missing, null and lost sources and preserves all dependencies',()=>{
 const compiler=expressionCompiler([binding()]),exists=compiler.compile({kind:'exists',value:reference}),isNull=compiler.compile({kind:'is_null',value:reference});
 assert.equal(evaluateExpression(exists,snapshots({})).value,false);
 assert.equal(evaluateExpression(exists,snapshots({value:null})).value,true);
 assert.equal(evaluateExpression(isNull,snapshots({})).value,'unknown');
 assert.equal(evaluateExpression(isNull,snapshots({value:null})).value,true);
 const not=compiler.compile({kind:'not',item:{kind:'exists',value:reference}});
 assert.equal(evaluateExpression(not,new Map()).value,'unknown');
 const all=compiler.compile({kind:'all',items:[{kind:'literal',value:false},{kind:'exists',value:reference}]});
 const result=evaluateExpression(all,new Map());assert.equal(result.value,false);assert.deepEqual(result.dependencies,[{binding_id:'input',pointer:'/value'}]);assert.equal(result.diagnostics.length,1);
});
test('compiled inputs are immutable and arbitrary caller trees are rejected',()=>{
 const input=binding(),compiler=expressionCompiler([input]),expression=compare();
 input.projections[0].reads.push('device/changed');const compiled=compiler.compile(expression);expression.right.value='99999999999999999999';
 assert.deepEqual(compiled.references[0].reads,['device/a']);assert.equal(evaluateExpression(compiled,snapshots({value:'9007199254740993'})).value,true);
 assert.throws(()=>compiled.tree.right.value='0',TypeError);
 assert.throws(()=>evaluateExpression({...compiled},new Map()),{code:'compiled_condition'});
 const data={kind:'ref',binding_id:'unbound',step_id:'data',independence_proof:{kind:'not'}};
 const value=compiler.compile({kind:'literal',value:data},{condition:false});assert.deepEqual(value.tree.value,data);assert.equal(value.nodes,1);assert.deepEqual(value.references,[]);
});
test('step result requires successful qualified evidence',()=>{
 const input={...binding(),source:'step_result',producer_step_id:'producer'},compiler=expressionCompiler([input]);
 const condition=compiler.compile({kind:'exists',value:{...reference,source:'step_result'}});
 const result=new Map([['input',{state:'available',value:{value:1},outcome:'succeeded',evidence_qualified:false}]]);
 assert.equal(evaluateExpression(condition,result).value,'unknown');result.get('input').evidence_qualified=true;
 assert.equal(evaluateExpression(condition,result).value,true);result.get('input').outcome='failed';assert.equal(evaluateExpression(condition,result).value,'unknown');
});
test('JSON pointers preserve escaped keys and reject prototype and ambiguous array access',()=>{
 assert.deepEqual(readPointer({'a/b':{'~':{'':7}}},'/a~1b/~0/'),{present:true,value:7});
 assert.deepEqual(readPointer({'01':4},'/01'),{present:true,value:4});assert.deepEqual(readPointer([3,4],'/01'),{present:false});
 for(const p of ['\n','/constructor','/a~2','/-','/${name}','#/a'])assert.throws(()=>pointerTokens(p),{code:'pointer'});
 assert.deepEqual(readPointer(Object.create({a:2}),'/a'),{present:false});
 let invoked=false;assert.throws(()=>readPointer({get a(){invoked=true;}},'/a'),{code:'pointer_accessor'});assert.equal(invoked,false);
});
test('expression budgets count semantic nodes and reject excess depth',()=>{
 const compiler=expressionCompiler([]);let condition={kind:'literal',value:true};for(let i=0;i<15;i++)condition={kind:'not',item:condition};
 assert.equal(compiler.compile(condition).nodes,16);assert.throws(()=>compiler.compile({kind:'not',item:condition}),{code:'expression_budget'});
 assert.equal(compiler.totalNodes(),16);
});
