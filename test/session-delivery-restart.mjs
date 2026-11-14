// 独立测试进程重开原Controller库；环境资格为测试来源，配对及撤销原件不重建。
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import path from 'node:path';
import {pathToFileURL} from 'node:url';
import {setTimeout as wait} from 'node:timers/promises';
import {fixture} from '../../leafseamer-controller/test/auth-clock-fixture.mjs';
const packageRoot=process.env.LEAFSEAMER_CONTROLLER_PACKAGE_UNDER_TEST;if(packageRoot)assert.ok(path.isAbsolute(packageRoot));
const load=name=>import(packageRoot?pathToFileURL(path.join(packageRoot,'dist',name)):new URL('../../leafseamer-controller/dist/'+name,import.meta.url));
const {startController}=await load('server.mjs'),{digest}=await load('auth-clock-value.mjs');
let input='';for await(const chunk of process.stdin)input+=chunk;
const config=JSON.parse(input);let app;
try{
 const {identity,...transportOptions}=config.transport;
 app=await startController({dataDir:config.directory,pairing:{...transportOptions,credential_id:identity.credential_id,peers:config.peers},
  authenticationProvider({scope,source}){return fixture(scope,{source,descriptor:{clock_domain_id:crypto.randomUUID(),descriptor_digest:digest('synthetic-restart-domain')}},{sampleWindowPolicy:'authentication-clock-sample/2'}).provider;}});
 assert.equal(app.identity.service_id,config.transport.identity.service_id);assert.equal(app.identity.database_instance_id,config.transport.identity.database_instance_id);
 const transport=app.pairing.transport,denials=app.pairing.sessionDenials;
 assert.equal(transport.configuration.issuer_id,config.transport.identity.issuer_id);assert.equal(transport.configuration.issuer_epoch,config.transport.identity.issuer_epoch);
 let status;for(let i=0;i<500;i++){status=transport.sessionDenialStatus();if(status.delivered+status.acknowledged>0)break;await wait(20);}
 assert.ok(status.delivered+status.acknowledged>0,JSON.stringify(status));
 const page=denials.scan(),task=page.items.find(item=>item.operation_id===config.operation_id);assert.ok(task);
 const result=await denials.deliverPending(task,{deliverSessionDenial(){throw new Error('receipt_must_be_persisted');}});
 assert.equal(result.state,'acknowledged');process.stdout.write(JSON.stringify({pid:process.pid,service_boot_id:app.serviceBootId,status,receipt:result.receipt}));
}finally{await app?.close();}
