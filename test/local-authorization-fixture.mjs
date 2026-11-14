// 仅隔离测试预置本地批准，不随产品包发布，也不是首次部署引导接口。
import crypto from 'node:crypto';
import path from 'node:path';
import Database from 'better-sqlite3';
import {canonical,digest,ref,seal} from '../dist/runtime/auth-clock-value.mjs';
export function seedLocalPermission(directory,service,issuer,principal,permissions,{definition_scope={kind:'all',definition_ids:[]},not_before_ns='1',expires_ns='18446744073709551615'}={}){
 const db=new Database(path.join(directory,'seamer.sqlite'));db.exec('PRAGMA foreign_keys=ON');
 try{return db.transaction(()=>{
  const subject=crypto.randomUUID(),operation=crypto.randomUUID(),revision='00000000000000000001';
  const grant=seal({format:'seamer-local-permission/1',id:crypto.randomUUID(),revision:'1',scope:{authority_service_id:service.service_id,database_instance_id:service.database_instance_id},subject_id:subject,
   issuer_id:issuer.issuer_id,issuer_epoch:issuer.issuer_epoch,permissions:[...permissions].sort(),definition_scope,not_before_ns,expires_ns,state:'active',operation_id:operation});
  const result={operation_id:operation,subject_id:subject,previous_ref:null,current_ref:ref(grant),reduced:false,propagation:'pending'};
  db.prepare('INSERT INTO local_subject VALUES(?,?,?,?,?)').run(subject,issuer.issuer_id,issuer.issuer_epoch,principal,'human');
  db.prepare('INSERT INTO local_permission_revision VALUES(?,?,?,?,?,?,?,?)').run(subject,revision,grant.id,null,'active',canonical(grant),grant.digest,operation);
  db.prepare('INSERT INTO local_permission_head VALUES(?,?)').run(subject,revision);
  db.prepare('INSERT INTO local_permission_operation VALUES(?,?,?,?,?,?,?,?)').run(operation,subject,revision,subject,crypto.randomUUID(),digest('synthetic-seed'),canonical(result),digest(result));
  const notification={kind:'synthetic-seed',operation_id:operation};db.prepare('INSERT INTO local_permission_outbox VALUES(?,?,?)').run(operation,canonical(notification),digest(notification));return grant;
 }).immediate();}finally{db.close();}
}
