import {createCoreBootstrapChannel,validateBootstrapPolicy,verifyCoreAdmission,verifyCoreAssignment} from './core-bootstrap.mjs';
import {connectCoreSupervisor} from './supervisor-control.mjs';
import {openTimeProviderLink,sealTimeProviderRuntime} from './time-provider-link.mjs';
import {same} from './runtime/auth-clock-value.mjs';

const require=(condition,code)=>{if(!condition)throw new Error(code);};

export function createSupervisedReference(options,{input,output,onLost,onAdmitted}){
 const policy=validateBootstrapPolicy(JSON.parse(options['--bootstrap-policy']));
 const timeProviderIntent=options['--time-provider-intent'];
 require(process.platform==='win32'&&typeof timeProviderIntent==='string'&&Buffer.byteLength(timeProviderIntent)<=16384,'reference_supervisor_options');
 const bootstrap=createCoreBootstrapChannel(input,output,{timeoutMs:policy.bootstrap_timeout_ms});
 let core,supervision,collector,sampling,closing;
 return Object.freeze({
  async admit({identity,source}){
   const assignment=await bootstrap.assignment();
   verifyCoreAssignment(assignment,policy,identity);
   const assertDeadline=()=>{
    const tick=source.tick();
    require(tick>=BigInt(assignment.issued_tick_ns)&&tick<BigInt(assignment.expires_tick_ns),'bootstrap_expired');
   };
   assertDeadline();
   const receipt=await bootstrap.proveAndReceive(assignment);
   assertDeadline();
   core=await verifyCoreAdmission(assignment,receipt,policy);
   assertDeadline();bootstrap.release();onAdmitted();
   supervision=await connectCoreSupervisor({pipe:options['--supervisor-pipe'],channelId:options['--supervisor-channel'],timeProviderIntent,binding:core,policy});
   supervision.onLost(onLost);
   return core.receipt.domain_ref;
  },
  async connect({identity,serviceBootId,authentication,adopt,openSampling=null}){
   const provider=supervision.currentTimeProviderObservation();
   require(provider!==null,'reference_supervisor_provider');
   supervision.assertTimeProviderObservation(provider);
   require(same(authentication.referenceTimeSample().runtime_domain_ref,core.receipt.domain_ref),'reference_supervisor_auth_domain');
   const binding=sealTimeProviderRuntime({identity,serviceBootId,core,provider});
   adopt({core,policy,binding,assertCurrent:()=>supervision.assertTimeProviderObservation(provider)});
   supervision.assertTimeProviderObservation(provider);
   collector=await openTimeProviderLink(binding,{assertCurrent:supervision.assertCurrent,readAuthentication:authentication.referenceTimeSample});
   collector.onLost(onLost);
   supervision.assertTimeProviderObservation(provider);
   collector.assertCurrent();
   if(openSampling)sampling=await openSampling({binding,supervision,collector,authentication});
   collector.assertCurrent();supervision.assertCurrent();
   return {binding:Object.freeze({runtime_id:binding.runtimeId,runtime_digest:binding.digest,clock_domain_ref:core.receipt.domain_ref,
    provider_pid:provider.process_observation.provider_pid}),sampling};
  },
  close(){return closing??=(async()=>{const drained=sampling?.close();collector?.close();supervision?.close();bootstrap.close();await drained;})();}
 });
}
