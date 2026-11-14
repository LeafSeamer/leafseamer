import {same} from './runtime/auth-clock-value.mjs';
const N=1000000000n,MAX=(1n<<64n)-1n;
const need=(ok,reason)=>{if(!ok)throw Object.assign(new Error('snapshot_lease_'+reason),{code:'snapshot_lease_'+reason});};
const integer=value=>{need(typeof value==='string'&&/^(0|[1-9][0-9]{0,19})$(?![\s\S])/.test(value),'integer');const n=BigInt(value);need(n<=MAX,'integer');return n;};
const ceil=(n,d)=>(n+d-1n)/d;
function bound(value){need(value&&Number.isSafeInteger(value.drift_ppb)&&value.drift_ppb>=0&&value.drift_ppb<=1000000,'rate');const q=integer(value.monotonic_quantization_ns);need(q>0n,'quantization');return {q,d:BigInt(value.drift_ppb)};}
// 输入须由已绑定通道与本地合格时钟取得；这里只计算保守耗时，不验证发送者或授予权限。
export function snapshotLeaseDeadline(start,received,remote){
 need(same(start.runtime_domain_ref,received.runtime_domain_ref)&&same(start.duration_bounds,received.duration_bounds),'domain');
 const a=integer(start.tick_ns),b=integer(received.tick_ns),localEnd=integer(received.valid_before_tick_ns);need(b>=a&&b<localEnd,'clock');
 const observed=integer(remote.observed_tick_ns),end=integer(remote.valid_before_tick_ns);need(end>observed,'expired');
 const local=bound(received.duration_bounds),publisher=bound(remote),remaining=end-observed-2n*publisher.q;need(remaining>0n,'expired');
 const lower=remaining*(N-publisher.d)/N,roundTrip=ceil((b-a+2n*local.q)*(N+local.d),N),usable=lower-roundTrip;need(usable>0n,'expired');
 const ticks=usable*N/(N+local.d)-2n*local.q;need(ticks>0n&&b+ticks<=MAX,'expired');
 const deadline=b+ticks<localEnd?b+ticks:localEnd;need(deadline>b,'expired');
 return Object.freeze({runtime_domain_ref:Object.freeze({...received.runtime_domain_ref}),duration_bounds:Object.freeze({...received.duration_bounds,policy_ref:Object.freeze({...received.duration_bounds.policy_ref})}),received_tick_ns:String(b),valid_before_tick_ns:String(deadline)});
}
export function checkSnapshotLease(lease,current){
 need(same(lease.runtime_domain_ref,current.runtime_domain_ref)&&same(lease.duration_bounds,current.duration_bounds),'domain');
 const now=integer(current.tick_ns);need(now>=integer(lease.received_tick_ns)&&now<integer(lease.valid_before_tick_ns)&&now<integer(current.valid_before_tick_ns),'expired');return lease;
}
