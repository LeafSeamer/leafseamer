import assert from 'node:assert/strict';
import path from 'node:path';
import {pathToFileURL} from 'node:url';
import Database from 'better-sqlite3';

const [packageRoot,dataDir,collectorFile,consumerId,mode]=process.argv.slice(2);
const {startSeamer}=await import(pathToFileURL(path.join(packageRoot,'dist','server.mjs')));
const collector=new Database(collectorFile);
collector.exec('CREATE TABLE IF NOT EXISTS received(event_id TEXT PRIMARY KEY, body TEXT NOT NULL) STRICT');
const save=collector.prepare('INSERT INTO received VALUES(?,?) ON CONFLICT(event_id) DO NOTHING');
const lookup=collector.prepare('SELECT body FROM received WHERE event_id=?').pluck();
let deliveries=0;
const app=await startSeamer({dataDir,executionNotificationSink:{consumerId,async deliver(packet,signal){
 signal.throwIfAborted();
 const body=JSON.stringify(packet);
 const inserted=save.run(packet.event.event_id,body).changes;
 assert.equal(lookup.get(packet.event.event_id),body);
 process.send({event:'observed',packet,inserted});
 deliveries++;
 if(mode==='crash')return new Promise(()=>{});
 if(mode==='recover'&&deliveries===1)return {...packet.ack,payload_digest:'sha256:'+'0'.repeat(64)};
 return packet.ack;
}}});
process.on('message',async message=>{
 if(message==='status')process.send({event:'status',status:app.executionNotificationStatus()});
 if(message==='close'){
  await app.close();
  collector.close();
  process.disconnect();
 }
});
process.send({event:'running',boot_id:app.serviceBootId});
