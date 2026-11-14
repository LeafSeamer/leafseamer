import path from 'node:path';
import assert from 'node:assert/strict';
import {pathToFileURL} from 'node:url';
const [root,directory,issuerText,sessionId]=process.argv.slice(2);assert.ok(path.isAbsolute(root)&&path.isAbsolute(directory));
const {openStorage}=await import(pathToFileURL(path.join(root,'dist/storage.mjs')));
const storage=openStorage(directory);
try{const issuer=JSON.parse(issuerText);if(sessionId)assert.throws(()=>storage.bindSessionDenials({issuer}).assertAllowed({...issuer,principal_kind:'human',session_id:sessionId}),/session_denial_revoked/);
 console.log(JSON.stringify({floor:storage.identityWatermarks.current(issuer),service_boot_id:storage.serviceBootId,...(sessionId?{session_denied:true}:{})}));}
finally{storage.close();}
