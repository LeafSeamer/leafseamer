import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {openInstalledArmAdmissionPolicy} from '../dist/installed-arm-admission-policy.mjs';
import {canonical,digest} from '../dist/runtime/auth-clock-value.mjs';

test('Linux 布防资格绑定原文件、实例、已审容量和当前认证时间', {skip:process.platform!=='linux'},t=>{
 const directory=fs.mkdtempSync(path.join(os.tmpdir(),'leafseamer-arm-'));
 t.after(()=>fs.rmSync(directory,{recursive:true,force:true}));
 const file=path.join(directory,'approval.json');
 const identity={service_id:crypto.randomUUID(),database_instance_id:crypto.randomUUID()};
 const scope={authority_service_id:identity.service_id,database_instance_id:identity.database_instance_id};
 const controller={authority_service_id:crypto.randomUUID(),database_instance_id:crypto.randomUUID()};
 const required={logical_bytes:'1024',queue_slots:'1',rows:'8',work_units:'8'};
 const approval={format:'leafseamer.arm-admission-approval/1',scope,profile_id:'arm-profile',revision:'1',
  qualification_ref:{id:'reviewed-envelope',revision:'1',digest:digest(required)},
  controller_scopes:[controller],limits:{artifact_bytes:128,preflight_bytes:1024,controller_targets:1,device_actions:1,sources:1},
  required,capacity:required,not_before_ns:'50',expires_ns:'100'};
 const write=value=>{
  const bytes=Buffer.from(canonical(value));
  fs.writeFileSync(file,bytes,{mode:0o600});
  return {approval_file:file,approval_sha256:'sha256:'+crypto.createHash('sha256').update(bytes).digest('hex')};
 };
 let sample={lower_ns:'49',upper_ns:'50'};
 const clock={current:()=>sample},settings=write(approval);
 const open=(config=settings,owner=identity)=>openInstalledArmAdmissionPolicy(config,{identity:owner,clock});
 const policy=open();
 const observation={targets:[{controller_scope:controller,device_actions:[{}]}],sources:[{}],can_arm:false};
 const input={scope,observation,basis:{},artifactBytes:128};
 assert.equal(policy.inspectInTransaction(input),null);
 sample={lower_ns:'50',upper_ns:'99'};
 const expected={profile_ref:{id:approval.profile_id,revision:'1',digest:settings.approval_sha256},
  qualification_ref:approval.qualification_ref,required,capacity:approval.capacity};
 assert.deepEqual(policy.inspectInTransaction(input),expected);
 assert.equal(policy.inspectInTransaction({...input,artifactBytes:129}),null);
 assert.equal(policy.inspectInTransaction({...input,basis:{proof:'a'.repeat(1024)}}),null);
 const wrongTarget={...observation,targets:[{...observation.targets[0],controller_scope:scope}]};
 assert.equal(policy.inspectInTransaction({...input,observation:wrongTarget}),null);
 assert.throws(()=>open(settings,{...identity,database_instance_id:crypto.randomUUID()}),{code:'pairing_profile_arm_admission_invalid'});
 sample={lower_ns:'99',upper_ns:'100'};
 assert.equal(policy.inspectInTransaction(input),null);
 sample={lower_ns:'50',upper_ns:'99'};
 assert.equal(policy.inspectInTransaction(input),null);
 const current=open();
 write({...approval,revision:'2'});
 assert.equal(current.inspectInTransaction(input),null);
 write(approval);
 assert.equal(current.inspectInTransaction(input),null);
 const insecure=open();
 fs.chmodSync(file,0o666);
 assert.equal(insecure.inspectInTransaction(input),null);
 fs.chmodSync(file,0o600);
 const invalid=write({...approval,capacity:{...required,rows:'7'}});
 assert.throws(()=>open(invalid),{code:'pairing_profile_arm_admission_invalid'});
});
