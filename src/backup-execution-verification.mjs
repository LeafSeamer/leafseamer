import {same} from './runtime/auth-clock-value.mjs';
import {automationExecutionStore} from './automation-execution-store.mjs';
import {automationExecutionCommit,completionEvidence} from './automation-execution-commit.mjs';
import {automationSubmissionStore} from './automation-submission-store.mjs';

function need(condition){
 if(!condition)throw new Error('backup_execution_inventory');
}

export function verifyBackupExecutions(db,identity){
 const inventory={triggered:0,scheduled:0,steps:0,completed:0,active:0,protected:0};
 const revision=Number(identity.schema_revision);
 if(revision<25)return inventory;
 const scope={authority_service_id:identity.service_id,database_instance_id:identity.database_instance_id};
 const executions=automationExecutionStore(db,scope,null,{withAnchors:revision>=26,withDelays:revision>=27,
  withActionParameters:revision>=28,withCommandDependencies:revision>=31,withRunCommit:revision>=32,withSchedules:revision>=38,
  submissions:revision>=29?automationSubmissionStore(db,{withTerminals:revision>=30}):null});
 const commits=revision>=32?automationExecutionCommit(db):null;
 const entries=db.prepare(`SELECT e.execution_id,h.definition_id FROM automation_execution e
  LEFT JOIN automation_execution_head h USING(execution_id)
  WHERE e.execution_id>? ORDER BY e.execution_id LIMIT 64`).raw();
 let cursor='';
 for(;;){
  const batch=entries.all(cursor);
  if(!batch.length)break;
  for(const [id,activeDefinition] of batch){
   const value=executions.read(id);
   need(value);
   const {execution,execution_ref,anchor,steps}=value;
   const commit=commits?.read(id);
   if(commit){
    const evidence=completionEvidence(steps);
    need(activeDefinition===null&&evidence&&anchor&&same(commit.body.execution_ref,execution_ref)&&
     same(commit.body.artifact_ref,execution.artifact_ref)&&same(commit.body.anchor_ref,anchor.ref)&&same(commit.body.steps,evidence));
    inventory.completed++;
   }
   if(activeDefinition!==null){
    need(activeDefinition===execution.definition_id);
    inventory.active++;
   }
   if(value.protection)inventory.protected++;
   inventory.steps+=steps.length;
   if(execution.origin_kind==='schedule')inventory.scheduled++;
   else inventory.triggered++;
  }
  cursor=batch.at(-1)[0];
 }
 return inventory;
}
