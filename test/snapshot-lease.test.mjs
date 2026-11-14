import test from 'node:test';import assert from 'node:assert/strict';
import path from 'node:path';import {fileURLToPath,pathToFileURL} from 'node:url';
const root=process.env.LEAFSEAMER_SEAMER_PACKAGE_UNDER_TEST??fileURLToPath(new URL('../',import.meta.url));
const {snapshotLeaseDeadline,checkSnapshotLease}=await import(pathToFileURL(path.join(root,'dist/snapshot-lease.mjs')));
const sample=t=>({runtime_domain_ref:{clock_domain_id:'domain',descriptor_digest:'digest'},duration_bounds:{policy_ref:{id:'policy',revision:'1',digest:'digest'},drift_ppb:1000000,monotonic_quantization_ns:'10'},tick_ns:String(t),valid_before_tick_ns:'5000000000'});
const remote={observed_tick_ns:'2000000000',valid_before_tick_ns:'32000000000',drift_ppb:1000000,monotonic_quantization_ns:'20'};
test('跨域租期扣除完整往返、双端量化和漂移，受原本地采集截止约束',()=>{
 const lease=snapshotLeaseDeadline(sample(100),sample(1000000100),remote);assert.equal(lease.valid_before_tick_ns,'5000000000');
 const end={...sample(1000000100),valid_before_tick_ns:'40000000000'},value=snapshotLeaseDeadline(sample(100),end,remote);
 const remoteLower=(30000000000n-40n)*999000000n/1000000000n,roundTrip=((1000000000n+20n)*1001000000n+999999999n)/1000000000n;
 assert.equal(BigInt(value.valid_before_tick_ns),1000000100n+(remoteLower-roundTrip)*1000000000n/1001000000n-20n);
 assert.equal(checkSnapshotLease(value,{...end,tick_ns:String(BigInt(value.valid_before_tick_ns)-1n)}),value);
 assert.throws(()=>checkSnapshotLease(value,{...end,tick_ns:value.valid_before_tick_ns}),{code:'snapshot_lease_expired'});
});
test('超长往返、过期、倒退、零量化、切域、策略变化和整数溢出拒绝',()=>{
 for(const [a,b,r]of [[sample(10),sample(9),remote],[sample(10),sample(4000000000),{...remote,valid_before_tick_ns:'2000000020'}],[sample(1),{...sample(2),duration_bounds:{...sample(2).duration_bounds,drift_ppb:0}},remote],[sample(1),sample(2),{...remote,monotonic_quantization_ns:'0'}],[sample(1),sample(2),{...remote,valid_before_tick_ns:'18446744073709551616'}]])assert.throws(()=>snapshotLeaseDeadline(a,b,r));
 const lease=snapshotLeaseDeadline(sample(1),sample(2),remote);assert.throws(()=>checkSnapshotLease(lease,{...sample(3),runtime_domain_ref:{clock_domain_id:'new'}}),{code:'snapshot_lease_domain'});
 assert.throws(()=>checkSnapshotLease(lease,sample(1)),{code:'snapshot_lease_expired'});
});
