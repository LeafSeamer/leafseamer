import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath,pathToFileURL} from 'node:url';
const root=fileURLToPath(new URL('../',import.meta.url));
const args=process.argv.slice(2),options=new Map();
for(let i=0;i<args.length;i+=2){
 const [name,value]=[args[i],args[i+1]];
 if(!['--native-artifacts','--controller-package'].includes(name)||options.has(name)||!value||!path.isAbsolute(value))throw new Error('build_arguments');
 options.set(name,value);
}
const nativeArtifacts=options.get('--native-artifacts'),controllerPackage=options.get('--controller-package');
const names=['cli.mjs','server.mjs','storage.mjs','storage.sql','storage-v2.sql','storage-v3.sql','binding-approval-store.mjs','peer-binding-receiver.mjs','auth-clock.sql','automation-expression.mjs','automation-compiler.mjs','automation-parameters.mjs','automation-draft.sql','automation-draft-store.mjs','automation-publication.mjs','automation-publication-store.mjs','peer-pairing.sql','peer-pairing.mjs','peer-identity.mjs','local-authorization.mjs','local-authorization.sql','draft-http.mjs','runtime/auth-clock.mjs','runtime/auth-clock-value.mjs','runtime/auth-clock-store.mjs','runtime/native-clock.mjs'];
const files=await Promise.all(names.map(async name=>[name,await fs.readFile(path.join(root,'src',name))]));
files.push(['peer-event-receiver.mjs',await fs.readFile(path.join(root,'src/peer-event-receiver.mjs'))]);
files.push(['peer-subscription-client.mjs',await fs.readFile(path.join(root,'src/peer-subscription-client.mjs'))]);
files.push(['snapshot-lease.mjs',await fs.readFile(path.join(root,'src/snapshot-lease.mjs'))]);
for(const name of ['snapshot-content.mjs','peer-snapshot-receiver.mjs'])files.push([name,await fs.readFile(path.join(root,'src',name))]);
for(const name of ['snapshot-ingest-store.mjs','storage-v4.sql','storage-v5.sql','reconcile-inbox.mjs','peer-snapshot-confirmation.mjs','managed-source-subscription.mjs'])files.push([name,await fs.readFile(path.join(root,'src',name))]);
files.push(['automation-run-store.mjs',await fs.readFile(path.join(root,'src','automation-run-store.mjs'))]);
files.push(['peer-service-approval.mjs',await fs.readFile(path.join(root,'src','peer-service-approval.mjs'))]);
files.push(['peer-run-approval-endorsement.mjs',await fs.readFile(path.join(root,'src','peer-run-approval-endorsement.mjs'))]);
files.push(['run-approval-parameters.mjs',await fs.readFile(path.join(root,'src','run-approval-parameters.mjs'))]);
files.push(['storage-v6.sql',await fs.readFile(path.join(root,'src','storage-v6.sql'))]);
files.push(['storage-v7.sql',await fs.readFile(path.join(root,'src','storage-v7.sql'))]);
files.push(['storage-v8.sql',await fs.readFile(path.join(root,'src','storage-v8.sql'))]);
files.push(['storage-v9.sql',await fs.readFile(path.join(root,'src','storage-v9.sql'))]);
files.push(['storage-v10.sql',await fs.readFile(path.join(root,'src','storage-v10.sql'))]);
files.push(['storage-v11.sql',await fs.readFile(path.join(root,'src','storage-v11.sql'))]);
files.push(['storage-v12.sql',await fs.readFile(path.join(root,'src','storage-v12.sql'))]);
files.push(['storage-v13.sql',await fs.readFile(path.join(root,'src','storage-v13.sql'))]);
for(const name of ['storage-v14.sql','arm-admission-store.mjs'])files.push([name,await fs.readFile(path.join(root,'src',name))]);
for(const name of ['storage-v15.sql','arm-preparation-receipt-store.mjs'])files.push([name,await fs.readFile(path.join(root,'src',name))]);
for(const name of ['storage-v16.sql','arm-completion-store.mjs','storage-v17.sql'])files.push([name,await fs.readFile(path.join(root,'src',name))]);
for(const name of ['storage-v18.sql','storage-v19.sql','first-pairing-import.mjs','first-pairing-enrollment.mjs'])files.push([name,await fs.readFile(path.join(root,'src',name))]);
files.push(['storage-v20.sql',await fs.readFile(path.join(root,'src','storage-v20.sql'))]);
files.push(['storage-v21.sql',await fs.readFile(path.join(root,'src','storage-v21.sql'))]);
files.push(['storage-v22.sql',await fs.readFile(path.join(root,'src','storage-v22.sql'))]);
for(const name of ['storage-v23.sql','automation-trigger-match-store.mjs'])files.push([name,await fs.readFile(path.join(root,'src',name))]);
for(const name of ['storage-v24.sql','automation-trigger-decision-store.mjs','automation-trigger-condition.mjs'])files.push([name,await fs.readFile(path.join(root,'src',name))]);
for(const name of ['storage-v25.sql','automation-execution-store.mjs'])files.push([name,await fs.readFile(path.join(root,'src',name))]);
files.push(['storage-v26.sql',await fs.readFile(path.join(root,'src','storage-v26.sql'))]);
files.push(['storage-v27.sql',await fs.readFile(path.join(root,'src','storage-v27.sql'))]);
files.push(['storage-v28.sql',await fs.readFile(path.join(root,'src','storage-v28.sql'))]);
for(const name of ['storage-v29.sql','storage-v30.sql','storage-v31.sql','storage-v32.sql','storage-v33.sql','storage-v34.sql','storage-v35.sql','automation-execution-notification.mjs','automation-execution-commit.mjs','automation-submission-store.mjs'])files.push([name,await fs.readFile(path.join(root,'src',name))]);
for(const name of ['storage-v36.sql','storage-v37.sql','storage-v38.sql','storage-v39.sql','schedule-store.mjs','schedule-clock.mjs','schedule-monitor.mjs'])files.push([name,await fs.readFile(path.join(root,'src',name))]);
for(const name of ['storage-v40.sql','storage-v41.sql','storage-v42.sql','storage-v43.sql','automation-recovery-preview.mjs','automation-recovery-operation.mjs','automation-recovery-preparation.mjs'])files.push([name,await fs.readFile(path.join(root,'src',name))]);
for(const name of ['trusted-pairing-profile.mjs','protected-ingress-profile.mjs','installed-arm-admission-policy.mjs'])files.push([name,await fs.readFile(path.join(root,'src',name))]);
files.push(['run-approval-store.mjs',await fs.readFile(path.join(root,'src','run-approval-store.mjs'))]);
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
for(const name of ['clock-evidence-envelope.mjs','linux-clock-evidence.mjs','linux-kernel-sleep.mjs']){
 const runtimePath='runtime/'+name;
 if(!Object.hasOwn(lock.files,'src/'+runtimePath))throw new Error('shared_runtime_drift');
 files.push([runtimePath,await fs.readFile(path.join(root,'src',runtimePath))]);
}
const linuxBootstrap=process.platform==='linux';
if(linuxBootstrap){
 for(const name of ['core-bootstrap.mjs','linux-core-bootstrap.mjs'])files.push([name,await fs.readFile(path.join(root,'src',name))]);
}
const firstPairingRuntime=new Map();
for(const name of ['first-pairing-authorization.mjs','peer-pairing-store.mjs']){
 const bytes=await fs.readFile(path.join(root,'src/runtime',name));
 if(hash(bytes)!==lock.first_pairing_runtime[name].sha256)throw new Error('shared_first_pairing_runtime_drift');
 firstPairingRuntime.set(name,bytes);
}
files.push(['runtime/first-pairing-authorization.mjs',firstPairingRuntime.get('first-pairing-authorization.mjs')]);
const pairingLock=JSON.parse(await fs.readFile(path.join(root,'pairing-runtime.json'),'utf8'));
const controllerRoot=controllerPackage===undefined?new URL('./',import.meta.resolve('@leafseamer/controller/package.json')):
 pathToFileURL(path.resolve(controllerPackage)+path.sep);
const controllerManifestBytes=await fs.readFile(new URL('dist/build-manifest.json',controllerRoot)),controllerManifest=JSON.parse(controllerManifestBytes);
const controllerManifestDigest=hash(controllerManifestBytes);
const overrides=controllerManifestDigest===pairingLock.manifest_sha256?{}:pairingLock.additional_manifests?.[controllerManifestDigest];
if(!overrides)throw new Error('pairing_runtime_drift');
const supervisionNames=['core-bootstrap.mjs','supervisor-control.mjs','authority-source-window.mjs','time-provider-link.mjs','runtime/canonicalize.mjs',
 'time-provider-polls.mjs','time-provider-capture.mjs','reference-time-anchor.mjs','reference-time-bundle.mjs','reference-time-scheduler.mjs'];
const {'linux-reference-provider.mjs':linuxReferencePin,'linux-reference-capture.mjs':linuxCapturePin,...serviceOverrides}=overrides;
const supervisionOverrides=Object.fromEntries(Object.entries(serviceOverrides).filter(([name])=>supervisionNames.includes(name)));
const pairingOverrides=Object.fromEntries(Object.entries(serviceOverrides).filter(([name])=>!supervisionNames.includes(name)));
if(Object.keys(pairingOverrides).some(name=>!Object.hasOwn(pairingLock.artifacts,name)&&
 !['peer-action-binding.mjs','peer-automation-command.mjs'].includes(name))||
 hash(await fs.readFile(path.join(root,'src/runtime/auth-clock-value.mjs')))!==pairingLock.auth_clock_value_sha256)throw new Error('pairing_runtime_drift');
for(const [name,digest]of Object.entries({...pairingLock.artifacts,...pairingOverrides})){
 if(!/^[a-z-]+\.mjs$/.test(name)||controllerManifest.artifacts[name]!==digest)throw new Error('pairing_runtime_asset');
 const bytes=await fs.readFile(new URL('dist/'+name,controllerRoot));if(hash(bytes)!==digest)throw new Error('pairing_runtime_drift');files.push(['runtime/'+name,firstPairingRuntime.get(name)??bytes]);
}
if(!Object.hasOwn({...pairingLock.artifacts,...pairingOverrides},'peer-automation-command.mjs')){
 const missing="const unavailable=()=>{throw Object.assign(new Error('execution_submission_transport_unavailable'),{code:'execution_submission_transport_unavailable'});};\n";
 files.push(['runtime/peer-automation-command.mjs',Buffer.from(missing+"export {unavailable as automationCommandRequest,unavailable as automationCommandIdentity,unavailable as automationCommandAdjudication};\n")]);
}
const supervisionPins={...pairingLock.supervision_artifacts,...supervisionOverrides};
if(Object.keys(supervisionPins).length!==supervisionNames.length||!supervisionNames.every(name=>/^[a-f0-9]{64}$/.test(supervisionPins[name])))throw new Error('supervision_runtime_lock');
const supervisionMatches=supervisionNames.every(name=>controllerManifest.artifacts[name]===supervisionPins[name]);
if(Object.keys(supervisionOverrides).length&&!supervisionMatches)throw new Error('supervision_runtime_drift');
if(supervisionMatches){
 for(const name of supervisionNames){
  const bytes=await fs.readFile(new URL('dist/'+name,controllerRoot));
  if(hash(bytes)!==supervisionPins[name])throw new Error('supervision_runtime_drift');
  if(!linuxBootstrap||name!=='core-bootstrap.mjs')files.push([name,bytes]);
 }
 for(const name of ['supervised-reference.mjs','reference-sampling.mjs'])files.push([name,await fs.readFile(path.join(root,'src',name))]);
 files.push(['native-clock.mjs',Buffer.from("export {clockInteger,openNativeClock} from './runtime/native-clock.mjs';\n")]);
 files.push(['auth-clock-value.mjs',Buffer.from("export {canonical,digest,same} from './runtime/auth-clock-value.mjs';\n")]);
}
if(linuxBootstrap&&!supervisionMatches)files.push(['runtime/canonicalize.mjs',Buffer.from("export {default} from 'canonicalize';\n")]);
if(linuxBootstrap){
 for(const [name,expected] of [['linux-reference-provider.mjs',linuxReferencePin],['linux-reference-capture.mjs',linuxCapturePin]]){
  if(expected===undefined)continue;
  const bytes=await fs.readFile(new URL('dist/'+name,controllerRoot));
  if(!supervisionMatches||controllerManifest.artifacts[name]!==expected||hash(bytes)!==expected)throw new Error('linux_reference_runtime_drift');
  files.push([name,bytes]);
 }
}
const controllerLicense=await fs.readFile(new URL('LICENSE',controllerRoot));if(hash(controllerLicense)!==pairingLock.license_sha256)throw new Error('pairing_license_drift');
files.push(['runtime/PAIRING-LICENSE',controllerLicense]);
const validationLock=JSON.parse(await fs.readFile(path.join(root,'validation-runtime.json'),'utf8'));
if(![validationLock.manifest_sha256,...(validationLock.additional_manifests??[])].includes(controllerManifestDigest)||
 validationLock.license_sha256!==hash(controllerLicense))throw new Error('validation_runtime_drift');
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
const profile=process.platform+'-'+process.arch,nativeRoot=nativeArtifacts??path.join(root,'logs/native-clock/artifacts',profile);
const manifestBytes=await fs.readFile(path.join(nativeRoot,'manifest.json')),manifest=JSON.parse(manifestBytes),binary=await fs.readFile(path.join(nativeRoot,'leafseamer_clock.node'));
const pinned=nativeArtifacts===undefined?null:lock.prebuilt_profiles[profile];
if(nativeArtifacts!==undefined&&(!pinned||pinned.manifest_sha256!==hash(manifestBytes)||pinned.binary_sha256!==hash(binary)||
 pinned.source_cmake_sha256!==manifest.cmake_source_sha256))throw new Error('native_prebuilt_mismatch');
const sourceDigest=pinned?pinned.source_sha256:hash(await fs.readFile(path.join(root,'native/clock.cc')));
if(manifest.format!=='leafseamer-native-artifact/1'||manifest.platform!==process.platform||manifest.arch!==process.arch||manifest.napi_version!==8||manifest.node_version!==process.versions.node||manifest.binary_sha256!==hash(binary)||
 !/^[a-f0-9]{64}$/.test(sourceDigest??'')||manifest.source_sha256!==sourceDigest||manifest.inputs_sha256!==hash(await fs.readFile(path.join(root,'native-inputs.json'))))throw new Error('native_build_drift');
if(!pinned&&manifest.cmake_source_sha256!==hash(await fs.readFile(path.join(root,'native/CMakeLists.txt'))))throw new Error('native_build_drift');
files.push(['runtime/native/leafseamer_clock.node',binary],['runtime/native/manifest.json',manifestBytes]);
const output=path.resolve(root,'dist');if(path.dirname(output)!==path.resolve(root))throw new Error('output_path');
await fs.rm(output,{recursive:true,force:true});await fs.mkdir(output,{recursive:true});
for(const [name,bytes]of files){await fs.mkdir(path.dirname(path.join(output,name)),{recursive:true});await fs.writeFile(path.join(output,name),bytes);}
await fs.chmod(path.join(output,'cli.mjs'),0o755);
await fs.writeFile(path.join(output,'build-manifest.json'),JSON.stringify({format:'leafseamer-seamer-build/1',artifacts:Object.fromEntries(files.map(([name,bytes])=>[name,crypto.createHash('sha256').update(bytes).digest('hex')]))},null,2)+'\n');
console.log(JSON.stringify({artifacts:files.length,mode:'protected',native:nativeArtifacts===undefined?'local_build':'pinned_prebuilt'}));
