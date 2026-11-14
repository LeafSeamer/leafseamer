import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
import {pathToFileURL} from 'node:url';
import {source} from './automation-program-fixture.mjs';
const root=process.argv[2];assert.ok(root&&path.isAbsolute(root));
const manifest=JSON.parse(await fs.readFile(path.join(root,'dist/build-manifest.json'),'utf8'));
for(const [name,expected]of Object.entries(manifest.artifacts)){
 assert.match(name,/^[a-zA-Z0-9/_.-]+$/);assert.ok(!name.split('/').some(p=>!p||p==='.'||p==='..'));
 const bytes=await fs.readFile(path.join(root,'dist',name));assert.equal(crypto.createHash('sha256').update(bytes).digest('hex'),expected);
}
const {compileProgram}=await import(pathToFileURL(path.join(root,'dist/automation-compiler.mjs')));
const {evaluateExpression}=await import(pathToFileURL(path.join(root,'dist/automation-expression.mjs')));
const result=compileProgram(source());assert.deepEqual(result.successors('first'),['second']);
assert.equal(evaluateExpression(result.condition('rule'),new Map()).value,true);assert.equal(result.approved,false);assert.equal(result.runtime_ready,false);
for(const name of ['ajv','ajv-formats','fast-deep-equal','fast-uri','json-schema-traverse','require-from-string']){
 const directory=path.resolve(root,'../../',name);await fs.access(path.join(directory,name==='require-from-string'?'license':'LICENSE'));
}
console.log(JSON.stringify({artifacts:Object.keys(manifest.artifacts).length,installed_compiler:true,approved:false,runtime_ready:false}));
