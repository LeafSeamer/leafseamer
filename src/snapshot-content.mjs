import {canonical,digest,same} from './runtime/auth-clock-value.mjs';
const need=(ok,why)=>{if(!ok)throw Object.assign(new Error('snapshot_content_'+why),{code:'snapshot_content_'+why});};
const hash=value=>digest(value).slice(7),bytes=value=>Buffer.byteLength(canonical(value));
const reference=v=>({authority_service_id:v.authority_service_id,database_instance_id:v.database_instance_id,entity_kind:v.kind,entity_id:v.entity_id,entity_epoch:v.entity_epoch,revision:v.revision,content_digest:hash(v)});
// 输入必须先经固定SDK解码并绑定真实通道；此处仅核内容集合，不授予持久摄取或执行资格。
export function createSourceSnapshotContent(input){
 const {subscription,context,manifest,graphIdentity}=structuredClone({subscription:input.subscription,context:input.context,manifest:input.manifest,graphIdentity:input.graphIdentity});
 const {scope,binding}=subscription,{projection_profile:profile,selector}=context;
 need(subscription.purpose==='canonical_trigger'&&subscription.owner.kind==='service'&&subscription.state==='snapshot_ready','purpose');
 need(same(context.owner,subscription.owner)&&same(context.delivery_binding,binding)&&binding.connection_generation!=='0'&&binding.scope_digest===hash(scope),'binding');
 need(profile.authority_role==='controller'&&profile.authority_service_id===binding.publisher_service_id&&profile.database_instance_id===binding.publisher_database_instance_id&&same(profile.selection,subscription.selection)&&profile.graph_digest===graphIdentity.digest&&profile.owner_kinds.includes('service')&&profile.purposes.includes('canonical_trigger'),'profile');
 need(same(scope.projection_profile_ref,reference(profile))&&same(selector.profile_ref,scope.projection_profile_ref),'profile');
 need(profile.collections.length===1&&profile.collections[0].entity_kind==='sources'&&scope.collections.length===1&&scope.collections[0].entity_kind==='sources','collections');
 const rule=profile.collections[0],selected=scope.collections[0];
 need(same(rule.view_schema_ref,{schema_id:graphIdentity.id,definition:'snapshot__SourceSnapshotView',schema_digest:graphIdentity.digest}),'schema');
 need(selector.authority_service_id===profile.authority_service_id&&selector.database_instance_id===profile.database_instance_id&&same(selected.selector_ref,reference(selector))&&selector.mode==='entity_set'&&selector.target_collection==='sources'&&selector.window===null&&selector.history_policy==='current'&&selector.relation_policy==='authorized_reference'&&selected.relation_policy===selector.relation_policy&&selected.history_policy===selector.history_policy&&rule.relation_policies.includes(selected.relation_policy)&&rule.history_policies.includes(selected.history_policy),'selector');
 const ids=selector.entity_ids;need(ids.length>0&&ids.length<=32&&ids.length<=profile.maximum_records&&new Set(ids).size===ids.length,'coverage');
 const {connection_generation,...contentBinding}=binding,expectedBinding={snapshot_id:subscription.snapshot_ref.snapshot_id,...contentBinding,projection_profile_ref:scope.projection_profile_ref,publisher_domain_ref:context.lease.publisher_domain_ref};
 const {manifest_digest,...unsigned}=manifest;
 need(manifest.format_version==='snapshot-projection/0.2.0-draft.1'&&same(manifest.binding,expectedBinding)&&manifest_digest===hash(unsigned)&&same(context.snapshot_ref,subscription.snapshot_ref)&&same(subscription.snapshot_ref,{snapshot_id:manifest.binding.snapshot_id,manifest_digest,snapshot_cursor:manifest.snapshot_cursor})&&manifest.snapshot_cursor==='0','manifest');
 need(same(manifest.collections,[{entity_kind:'sources',count:String(ids.length),view_schema_ref:rule.view_schema_ref}]),'collections');
 need(Number.isInteger(manifest.page_count)&&manifest.page_count>=1&&manifest.page_count<=128&&manifest.pages.length===manifest.page_count,'pages');
 need(manifest.pages.every((p,i)=>p.page_index===i&&Number.isInteger(p.content_bytes)&&p.content_bytes>0&&p.content_bytes<=245760)&&manifest.total_content_bytes===manifest.pages.reduce((n,p)=>n+p.content_bytes,0),'bytes');
 need(manifest.source_watermarks.length===ids.length,'watermarks');
 const pages=new Map();let total=bytes(manifest),ended=false;need(total<=8388608,'capacity');
 return Object.freeze({
  add(response){need(!ended,'closed');const {content}=response,index=content.page_index,expected=manifest.pages[index];
   need(expected&&same(response.delivery_binding,binding)&&response.manifest_digest===manifest_digest&&response.content_digest===expected.content_digest&&hash(content)===expected.content_digest&&bytes(content)===expected.content_bytes,'page');
   need(content.format_version===manifest.format_version&&same(content.binding,expectedBinding)&&content.next_page_index===(index+1<manifest.page_count?index+1:null),'page');
   const size=bytes(response);need(size<=262144,'capacity');
   if(pages.has(index)){need(same(pages.get(index),content),'duplicate');return;}
   need(total+size<=8388608,'capacity');total+=size;pages.set(index,structuredClone(content));
  },
  finish(){need(!ended&&pages.size===manifest.page_count,'incomplete');ended=true;const records=[];for(let i=0;i<manifest.page_count;i++){need(pages.has(i),'incomplete');records.push(...pages.get(i).records);}need(records.length===ids.length,'coverage');
   const seen=new Set(),watermarks=[];let previous=null;
   for(const record of records){const key=record.view.source_key,identity=[record.entity_kind,record.entity_id,record.entity_epoch];
    need(record.entity_kind==='sources'&&ids.includes(record.entity_id)&&!seen.has(record.entity_id)&&record.entity_id===key.registration_id&&record.entity_epoch===key.source_epoch,'record');
    need(identity.every(s=>typeof s==='string'&&/^[\x20-\x7e]+$/.test(s))&&(!previous||previous[0]<identity[0]||previous[0]===identity[0]&&(previous[1]<identity[1]||previous[1]===identity[1]&&previous[2]<identity[2])),'order');previous=identity;seen.add(record.entity_id);
    need(key.authority_service_id===binding.publisher_service_id&&key.database_instance_id===binding.publisher_database_instance_id&&BigInt(record.view.contiguous_sequence)<=BigInt(record.view.highest_seen_sequence),'source');
    need(record.view.binding_ref.authority_service_id===key.authority_service_id&&record.view.binding_ref.database_instance_id===key.database_instance_id,'source');
    const gapIds=new Set();for(const gap of record.view.gaps){need(!gapIds.has(gap.gap_id),'gap');gapIds.add(gap.gap_id);
     need(gap.loss_extent==='unknown'?gap.first_missing===null&&gap.last_missing===null:gap.loss_extent==='known'&&gap.first_missing!==null&&gap.last_missing!==null&&BigInt(gap.first_missing)<=BigInt(gap.last_missing),'gap');}
    watermarks.push({source_key:key,binding_ref:record.view.binding_ref,baseline_sequence:record.view.contiguous_sequence,source_state_revision:record.entity_revision,baseline_ref:record.view.baseline_ref,gap_summary:record.view.gaps});
   }
   for(const expected of watermarks)need(manifest.source_watermarks.filter(w=>same(w.source_key,expected.source_key)).length===1&&manifest.source_watermarks.some(w=>same(w,expected)),'watermarks');pages.clear();
   return structuredClone({manifest,records,source_watermarks:watermarks});
  }
 });
}
