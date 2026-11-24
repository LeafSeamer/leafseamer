import {createSecureContext} from 'node:tls';
import {fields} from './runtime/auth-clock-value.mjs';
import {pairingUuid} from './runtime/peer-pairing-value.mjs';
import {readLocalFile} from './trusted-pairing-profile.mjs';

export function readProtectedIngressProfile(file){
 if(process.platform!=='linux')throw new Error('protected_ingress_platform');
 try{
  const read=(filename,limit)=>readLocalFile(filename,limit,{noFollow:true});
  const profile=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(read(file,16384)));
  fields(profile,['format','service_id','database_instance_id','address','port','origin','certificate_file','private_key_file']);
  const {format,service_id,database_instance_id,address,port,origin}=profile;
  pairingUuid(service_id);pairingUuid(database_instance_id);
  const url=new URL(origin);
  if(format!=='leafseamer.seamer-protected-ingress/1'||url.protocol!=='https:'||url.origin!==origin)throw new Error('protected_ingress_profile');
  const cert=read(profile.certificate_file,65536),key=read(profile.private_key_file,32768);
  createSecureContext({cert,key});
  return {identity:{service_id,database_instance_id},host:address,port,cert,key,origin};
 }catch{
  throw new Error('protected_ingress_profile');
 }
}
