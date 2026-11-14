import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
const root=fileURLToPath(new URL('../',import.meta.url));
const args=process.argv.slice(2);if(args.length&&!(args.length===2&&args[0]==='--native-artifacts'&&path.isAbsolute(args[1])))throw new Error('build_arguments');
const names=['cli.mjs','server.mjs','storage.mjs','storage.sql','storage-v2.sql','storage-v3.sql','binding-approval-store.mjs','peer-binding-receiver.mjs','auth-clock.sql','automation-expression.mjs','automation-compiler.mjs','automation-parameters.mjs','automation-draft.sql','automation-draft-store.mjs','automation-publication.mjs','automation-publication-store.mjs','peer-pairing.sql','peer-pairing.mjs','peer-identity.mjs','local-authorization.mjs','local-authorization.sql','draft-http.mjs','runtime/auth-clock.mjs','runtime/auth-clock-value.mjs','runtime/auth-clock-store.mjs','runtime/native-clock.mjs'];
const files=await Promise.all(names.map(async name=>[name,await fs.readFile(path.join(root,'src',name))]));
files.push(['peer-event-receiver.mjs',await fs.readFile(path.join(root,'src/peer-event-receiver.mjs'))]);
files.push(['peer-subscription-client.mjs',await fs.readFile(path.join(root,'src/peer-subscription-client.mjs'))]);
files.push(['snapshot-lease.mjs',await fs.readFile(path.join(root,'src/snapshot-lease.mjs'))]);
for(const name of ['snapshot-content.mjs','peer-snapshot-receiver.mjs'])files.push([name,await fs.readFile(path.join(root,'src',name))]);
for(const name of ['snapshot-ingest-store.mjs','storage-v4.sql','peer-snapshot-confirmation.mjs'])files.push([name,await fs.readFile(path.join(root,'src',name))]);
for(const name of ['peer-identity-watermark.mjs','peer-identity-watermark.sql'])files.push([name,await fs.readFile(path.join(root,'src',name))]);
for(const name of ['session-denial.mjs','session-denial.sql'])files.push([name,await fs.readFile(path.join(root,'src',name))]);
const hash=bytes=>crypto.createHash('sha256').update(bytes).digest('hex');
const sdkRoot=new URL('./',import.meta.resolve('@leafseamer/sdk'));
const sdkLock=JSON.parse(await fs.readFile(path.join(root,'sdk-runtime.json'),'utf8'));
const sdkManifestBytes=await fs.readFile(new URL('build-manifest.json',sdkRoot)),sdkManifest=JSON.parse(sdkManifestBytes);
if(hash(sdkManifestBytes)!==sdkLock.manifest_sha256||JSON.stringify(sdkManifest.selection)!==JSON.stringify(sdkLock.selection))throw new Error('sdk_manifest_drift');
for(const [name,expected]of Object.entries(sdkManifest.artifacts)){
 if(!/^[a-zA-Z0-9/_.-]+$/.test(name)||name.split('/').some(part=>!part||part==='.'||part==='..')||name.startsWith('/'))throw new Error('sdk_artifact_path');
 const bytes=await fs.readFile(new URL(name,sdkRoot));if(hash(bytes)!==expected)throw new Error('sdk_artifact_drift');files.push(['sdk/'+name,bytes]);
}
const sdkLicense=await fs.readFile(new URL('../LICENSE',sdkRoot));if(hash(sdkLicense)!==sdkLock.license_sha256)throw new Error('sdk_license_drift');
files.push(['sdk/build-manifest.json',sdkManifestBytes],['sdk/LICENSE',sdkLicense]);
const lock=JSON.parse(await fs.readFile(path.join(root,'runtime-inputs.json'),'utf8'));
for(const [name,entry]of Object.entries(lock.files))if(hash(await fs.readFile(path.join(root,name)))!==entry.sha256)throw new Error('shared_runtime_drift');
const pairingLock=JSON.parse(await fs.readFile(path.join(root,'pairing-runtime.json'),'utf8'));
const controllerRoot=new URL('./',import.meta.resolve('@leafseamer/controller/package.json'));
const controllerManifestBytes=await fs.readFile(new URL('dist/build-manifest.json',controllerRoot)),controllerManifest=JSON.parse(controllerManifestBytes);
if(hash(controllerManifestBytes)!==pairingLock.manifest_sha256||hash(await fs.readFile(path.join(root,'src/runtime/auth-clock-value.mjs')))!==pairingLock.auth_clock_value_sha256)throw new Error('pairing_runtime_drift');
for(const [name,digest]of Object.entries(pairingLock.artifacts)){
 if(!/^[a-z-]+\.mjs$/.test(name)||controllerManifest.artifacts[name]!==digest)throw new Error('pairing_runtime_asset');
 const bytes=await fs.readFile(new URL('dist/'+name,controllerRoot));if(hash(bytes)!==digest)throw new Error('pairing_runtime_drift');files.push(['runtime/'+name,bytes]);
}
const controllerLicense=await fs.readFile(new URL('LICENSE',controllerRoot));if(hash(controllerLicense)!==pairingLock.license_sha256)throw new Error('pairing_license_drift');
files.push(['runtime/PAIRING-LICENSE',controllerLicense]);
const validationLock=JSON.parse(await fs.readFile(path.join(root,'validation-runtime.json'),'utf8'));
if(validationLock.manifest_sha256!==hash(controllerManifestBytes)||validationLock.license_sha256!==hash(controllerLicense))throw new Error('validation_runtime_drift');
const packageJson=JSON.parse(await fs.readFile(path.join(root,'package.json'),'utf8'));
if(Object.keys(validationLock.dependencies).sort().join(',')!=='ajv,ajv-formats')throw new Error('validation_dependencies');
for(const [name,version]of Object.entries(validationLock.dependencies)){
 const installed=JSON.parse(await fs.readFile(path.join(root,'node_modules',name,'package.json'),'utf8'));
 if(packageJson.dependencies[name]!==version||installed.version!==version)throw new Error('validation_dependency_drift');
}
for(const [name,digest]of Object.entries(validationLock.artifacts)){
 if(!/^configuration-(validator|worker|schema|wire)\.mjs$/.test(name)||controllerManifest.artifacts[name]!==digest)throw new Error('validation_runtime_asset');
 const bytes=await fs.readFile(new URL('dist/'+name,controllerRoot));if(hash(bytes)!==digest)throw new Error('validation_runtime_drift');files.push(['validation/'+name,bytes]);
}
if(Object.keys(validationLock.artifacts).length!==4)throw new Error('validation_runtime_assets');
files.push(['validation/LICENSE',controllerLicense]);
const profile=process.platform+'-'+process.arch,nativeRoot=args.length?args[1]:path.join(root,'logs/native-clock/artifacts',profile);
const manifestBytes=await fs.readFile(path.join(nativeRoot,'manifest.json')),manifest=JSON.parse(manifestBytes),binary=await fs.readFile(path.join(nativeRoot,'leafseamer_clock.node'));
if(manifest.format!=='leafseamer-native-artifact/1'||manifest.platform!==process.platform||manifest.arch!==process.arch||manifest.napi_version!==8||manifest.node_version!==process.versions.node||manifest.binary_sha256!==hash(binary)||
 manifest.source_sha256!==hash(await fs.readFile(path.join(root,'native/clock.cc')))||manifest.inputs_sha256!==hash(await fs.readFile(path.join(root,'native-inputs.json'))))throw new Error('native_build_drift');
if(args.length){const pinned=lock.prebuilt_profiles[profile];if(!pinned||pinned.manifest_sha256!==hash(manifestBytes)||pinned.binary_sha256!==hash(binary)||pinned.source_cmake_sha256!==manifest.cmake_source_sha256)throw new Error('native_prebuilt_mismatch');}
else if(manifest.cmake_source_sha256!==hash(await fs.readFile(path.join(root,'native/CMakeLists.txt'))))throw new Error('native_build_drift');
files.push(['runtime/native/leafseamer_clock.node',binary],['runtime/native/manifest.json',manifestBytes]);
const output=path.resolve(root,'dist');if(path.dirname(output)!==path.resolve(root))throw new Error('output_path');
await fs.rm(output,{recursive:true,force:true});await fs.mkdir(output,{recursive:true});
for(const [name,bytes]of files){await fs.mkdir(path.dirname(path.join(output,name)),{recursive:true});await fs.writeFile(path.join(output,name),bytes);}
await fs.chmod(path.join(output,'cli.mjs'),0o755);
await fs.writeFile(path.join(output,'build-manifest.json'),JSON.stringify({format:'leafseamer-seamer-build/1',artifacts:Object.fromEntries(files.map(([name,bytes])=>[name,crypto.createHash('sha256').update(bytes).digest('hex')]))},null,2)+'\n');
console.log(JSON.stringify({artifacts:files.length,mode:'protected',native:args.length?'pinned_prebuilt':'local_build'}));
