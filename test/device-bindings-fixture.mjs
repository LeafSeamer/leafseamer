import {digest} from '../dist/runtime/auth-clock-value.mjs';
// 传输测试合成正文；不代表模块接纳或本地发布批准。
export function bindingMaterial(scope,device){
 const document={type:'number'},schema_ref={id:'test-schema',revision:'1',digest:digest(document)};
 const registration={format:'leafseamer-device-automation-registration/1',registration_id:'test-registration',scope,device_id:device};
 const registration_ref={kind:'source_registration',scope,object_id:registration.registration_id,revision:'1',digest:digest(registration)};
 const value={binding_id:'test-input',source:'observation',authority_id:scope.authority_service_id,registration_ref:{id:registration_ref.object_id,revision:'1',digest:registration_ref.digest},schema_ref};
 return {registration_ref,registration,bindings:[{kind:'input',binding_id:value.binding_id,content_ref:{id:value.binding_id,revision:'1',digest:digest(value)},value}],
  schemas:[],input_schemas:[{schema_ref,document}],result_schema:{schema_ref,document,projections:[]},alias_table:{ref:{id:'test-aliases',revision:'1',digest:digest([])},entries:[]}};
}
