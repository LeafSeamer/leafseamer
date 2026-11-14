#!/usr/bin/env node
import path from 'node:path';
import {initializeStorage,migrateStorage,openStorage,StorageError} from './storage.mjs';
import {startSeamer} from './server.mjs';
const emit=value=>process.stdout.write(JSON.stringify(value)+'\n');
try{
 const [command,...args]=process.argv.slice(2),options={};
 if(!['init-storage','migrate-storage','identity-status','start'].includes(command)||args.length%2)throw new Error('arguments');
 for(let i=0;i<args.length;i+=2){if(!['--data-dir',...(command==='start'?['--host','--port']:command==='migrate-storage'?['--to-revision']:[])].includes(args[i])||Object.hasOwn(options,args[i]))throw new Error('arguments');options[args[i]]=args[i+1];}
 if(!options['--data-dir']||!path.isAbsolute(options['--data-dir']))throw new Error('arguments');
 if(command==='init-storage')emit({event:'storage_initialized',...initializeStorage(options['--data-dir']),ready:false});
 else if(command==='migrate-storage')emit({event:'storage_migrated',...migrateStorage(options['--data-dir'],options['--to-revision']),ready:false});
 else if(command==='identity-status'){const storage=openStorage(options['--data-dir']);try{emit({event:'identity_status',...storage.identity,ready:false});}finally{storage.close();}}
 else{
  const port=options['--port']??'0';if(!/^(0|[1-9][0-9]{0,4})$/.test(port))throw new Error('arguments');
  const app=await startSeamer({dataDir:options['--data-dir'],host:options['--host']??'127.0.0.1',port:Number(port)});
  let stopping=false;const stop=async()=>{if(stopping)return;stopping=true;try{await app.close();emit({event:'stopped'});}catch{process.stderr.write('{"error":"shutdown_failed"}\n');process.exitCode=1;}};
  process.once('SIGINT',stop);process.once('SIGTERM',stop);
  emit({event:'listening',...app.identity,service_boot_id:app.serviceBootId,host:app.address.address,port:app.address.port,ready:false});
 }
}catch(error){process.stderr.write(JSON.stringify({error:error instanceof StorageError?error.code:error.message==='arguments'?'arguments':'startup_failed'})+'\n');process.exitCode=1;}
