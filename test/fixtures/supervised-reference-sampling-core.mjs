// 仅联调用的本机批准和密钥提供者。实际 UDP 采样、归档和锚点都由 Seamer 运行。
import {setTimeout as delay} from 'node:timers/promises';
import {runCli} from './cli.mjs';
import {digest} from './runtime/auth-clock-value.mjs';
import {referenceBundleAuthority} from './fixture-reference-authority.mjs';
import {endpoint} from './fixture-reference-endpoint.mjs';
import {referenceSamplingFixture} from './fixture-sampling-authority.mjs';
let approved=true;
const referenceSamplingAuthority=referenceSamplingFixture({endpoint,digest,bundleAuthority:referenceBundleAuthority,approved:()=>approved});

const app=await runCli(process.argv.slice(2),{referenceSamplingAuthority});
if(app){
 const until=performance.now()+5000;
 while(performance.now()<until){
  const current=app.referenceSampling.current(),status=app.referenceSampling.status();
  if(current&&status.every(source=>source.state==='waiting')||status.some(source=>source.state==='blocked')){
   const provider=app.referenceSampling.receiverReference;
   const reference=current?provider.readFirst({tick_ns:current.tick_ns,runtime_domain_ref:current.runtime_domain_ref}):null;
   const check=reference&&{first_tick_ns:current.tick_ns,tick_ns:current.tick_ns,reference};
   const receiver=reference&&{scope:reference.bundle.anchor.binding.domain,context_digest:reference.bundle.bundle_digest,current:provider.isCurrent(check)};
   approved=false;
   if(receiver)receiver.withdrawn=!provider.isCurrent(check)&&app.referenceSampling.current()===null;
   process.stderr.write(JSON.stringify({event:'seamer_reference_observed',current,status,receiver})+'\n');
   break;
  }
  await delay(30);
 }
}
