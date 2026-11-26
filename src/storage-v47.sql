-- 显式停机迁移。恢复执行的结算原件和原执行槽释放。
DROP TRIGGER first_pairing_import_target;
DROP TRIGGER schedule_version_parent;
DROP TRIGGER schedule_activation_parent;
DROP TRIGGER automation_recovery_preview_binding;
CREATE TABLE service_state_v47 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='47'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v47 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'47',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v47 RENAME TO service_state;
CREATE TRIGGER service_state_no_update BEFORE UPDATE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;
CREATE TRIGGER service_state_no_delete BEFORE DELETE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;

CREATE TRIGGER first_pairing_import_target BEFORE INSERT ON first_pairing_import
WHEN EXISTS(SELECT 1 FROM peer_pairing_revision WHERE state='activated' LIMIT 1)
 OR EXISTS(SELECT 1 FROM local_permission_head LIMIT 1)
 OR NOT EXISTS(SELECT 1 FROM service_state s,json_each(NEW.body_jcs,'$.authorization.transcript.parties') p
    WHERE json_extract(p.value,'$.service_kind')='seamer'
     AND json_extract(p.value,'$.service_id') IS s.service_id
     AND json_extract(p.value,'$.database_instance_id') IS s.database_instance_id
     AND json_extract(p.value,'$.binding_id') IS NEW.target_binding_id)
BEGIN SELECT RAISE(ABORT,'first_pairing_import_target'); END;

CREATE TABLE schema_migration_v47 (
 migration_id TEXT PRIMARY KEY NOT NULL, from_revision TEXT NOT NULL, to_revision TEXT NOT NULL UNIQUE,
 script_digest TEXT NOT NULL, state TEXT NOT NULL CHECK(state='committed'),
 CHECK((from_revision='0' AND to_revision='1') OR (from_revision='1' AND to_revision='2') OR
  (from_revision='2' AND to_revision='3') OR (from_revision='3' AND to_revision='4') OR
  (from_revision='4' AND to_revision='5') OR (from_revision='5' AND to_revision='6') OR
  (from_revision='6' AND to_revision='7') OR (from_revision='7' AND to_revision='8') OR
  (from_revision='8' AND to_revision='9') OR (from_revision='9' AND to_revision='10') OR
  (from_revision='10' AND to_revision='11') OR (from_revision='11' AND to_revision='12') OR
  (from_revision='12' AND to_revision='13') OR (from_revision='13' AND to_revision='14') OR
  (from_revision='14' AND to_revision='15') OR (from_revision='15' AND to_revision='16') OR
  (from_revision='16' AND to_revision='17') OR (from_revision='17' AND to_revision='18') OR
  (from_revision='18' AND to_revision='19') OR (from_revision='19' AND to_revision='20') OR
  (from_revision='20' AND to_revision='21') OR (from_revision='21' AND to_revision='22') OR
  (from_revision='22' AND to_revision='23') OR
  (from_revision='23' AND to_revision='24') OR
  (from_revision='24' AND to_revision='25') OR (from_revision='25' AND to_revision='26') OR (from_revision='26' AND to_revision='27') OR (from_revision='27' AND to_revision='28') OR (from_revision='28' AND to_revision='29') OR (from_revision='29' AND to_revision='30') OR (from_revision='30' AND to_revision='31') OR (from_revision='31' AND to_revision='32') OR (from_revision='32' AND to_revision='33') OR
  (from_revision='33' AND to_revision='34') OR (from_revision='34' AND to_revision='35') OR (from_revision='35' AND to_revision='36') OR (from_revision='36' AND to_revision='37') OR (from_revision='37' AND to_revision='38') OR (from_revision='38' AND to_revision='39') OR (from_revision='39' AND to_revision='40') OR (from_revision='40' AND to_revision='41') OR (from_revision='41' AND to_revision='42') OR (from_revision='42' AND to_revision='43') OR (from_revision='43' AND to_revision='44') OR (from_revision='44' AND to_revision='45') OR (from_revision='45' AND to_revision='46') OR (from_revision='46' AND to_revision='47'))
) STRICT;
INSERT INTO schema_migration_v47 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v47 RENAME TO schema_migration;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;

CREATE TRIGGER schedule_version_parent BEFORE INSERT ON schedule_version
WHEN NOT EXISTS(
 SELECT 1 FROM schedule_identity i JOIN definition_publication p ON p.definition_id=i.definition_id
 WHERE i.schedule_id=NEW.schedule_id AND p.definition_id=NEW.definition_id AND p.definition_version=NEW.definition_version
 AND json_extract(NEW.body_jcs,'$.scope.authority_service_id') IS (SELECT service_id FROM service_state)
 AND json_extract(NEW.body_jcs,'$.scope.database_instance_id') IS (SELECT database_instance_id FROM service_state)
 AND json_extract(NEW.body_jcs,'$.schedule_id') IS NEW.schedule_id
 AND json_extract(NEW.body_jcs,'$.version') IS CAST(CAST(NEW.version AS INTEGER) AS TEXT)
 AND json_extract(NEW.body_jcs,'$.definition_id') IS NEW.definition_id
 AND json_extract(NEW.body_jcs,'$.definition_version') IS NEW.definition_version
 AND json_extract(NEW.body_jcs,'$.definition_artifact_ref') IS json_extract(p.bundle_jcs,'$.result.artifact_ref')
 AND json_type(NEW.body_jcs,'$.items')='array' AND json_array_length(NEW.body_jcs,'$.items') BETWEEN 1 AND 256
 AND NOT EXISTS(SELECT 1 FROM json_each(NEW.body_jcs,'$.items') item WHERE NOT EXISTS(
   SELECT 1 FROM schedule_occurrence o WHERE o.schedule_id=NEW.schedule_id
    AND o.item_id IS json_extract(item.value,'$.item_id') AND o.one_shot_key IS json_extract(item.value,'$.one_shot_key')
    AND o.occurrence_id IS json_extract(item.value,'$.occurrence_id') AND o.logical_key_digest IS json_extract(item.value,'$.logical_key_digest')))
 AND NEW.version IS printf('%020d',coalesce((SELECT max(CAST(version AS INTEGER)) FROM schedule_version WHERE schedule_id=NEW.schedule_id),0)+1)
)
BEGIN SELECT RAISE(ABORT,'schedule_version_parent'); END;

CREATE TRIGGER schedule_activation_parent BEFORE INSERT ON schedule_activation
WHEN NOT EXISTS (
 SELECT 1 FROM schedule_version v WHERE v.schedule_id=NEW.schedule_id AND v.version=NEW.schedule_version
 AND json_extract(NEW.body_jcs,'$.schedule_version_ref.digest') IS v.body_digest
 AND json_extract(NEW.body_jcs,'$.schedule_version_ref.id') IS NEW.schedule_id
 AND json_extract(NEW.body_jcs,'$.schedule_version_ref.revision') IS CAST(CAST(v.version AS INTEGER) AS TEXT)
 AND json_extract(NEW.body_jcs,'$.schedule_id') IS NEW.schedule_id
 AND json_extract(NEW.body_jcs,'$.revision') IS CAST(CAST(NEW.revision AS INTEGER) AS TEXT)
 AND json_extract(NEW.body_jcs,'$.scope.authority_service_id') IS (SELECT service_id FROM service_state)
 AND json_extract(NEW.body_jcs,'$.scope.database_instance_id') IS (SELECT database_instance_id FROM service_state)
 AND NEW.revision IS printf('%020d',coalesce((SELECT max(CAST(revision AS INTEGER)) FROM schedule_activation WHERE schedule_id=NEW.schedule_id),0)+1)
 AND ((CAST(NEW.revision AS INTEGER)=1 AND json_type(NEW.body_jcs,'$.prior_ref')='null') OR EXISTS (
   SELECT 1 FROM schedule_activation a WHERE a.schedule_id=NEW.schedule_id AND CAST(a.revision AS INTEGER)=CAST(NEW.revision AS INTEGER)-1
   AND json_extract(NEW.body_jcs,'$.prior_ref.id') IS json_extract(a.body_jcs,'$.activation_id')
   AND json_extract(NEW.body_jcs,'$.prior_ref.revision') IS CAST(CAST(a.revision AS INTEGER) AS TEXT)
   AND json_extract(NEW.body_jcs,'$.prior_ref.digest') IS a.body_digest))
)
BEGIN SELECT RAISE(ABORT,'schedule_activation_parent'); END;

CREATE TRIGGER automation_recovery_preview_binding BEFORE INSERT ON automation_recovery_preview
WHEN NOT EXISTS(
 SELECT 1 FROM automation_execution e JOIN automation_execution_head h USING(execution_id)
 JOIN service_state s ON s.singleton=1
 WHERE e.execution_id=NEW.execution_id AND e.run_id=NEW.run_id
 AND json_extract(NEW.body_jcs,'$.format') IS 'leafseamer.recovery-preview/1'
 AND json_extract(NEW.body_jcs,'$.preview_id') IS NEW.preview_id
 AND json_extract(NEW.body_jcs,'$.snapshot.execution_ref.object_id') IS NEW.execution_id
 AND json_extract(NEW.body_jcs,'$.snapshot.execution_ref.digest') IS e.body_digest
 AND json_extract(NEW.body_jcs,'$.snapshot.run_id') IS NEW.run_id
 AND json_extract(NEW.body_jcs,'$.snapshot.scope.authority_service_id') IS s.service_id
 AND json_extract(NEW.body_jcs,'$.snapshot.scope.database_instance_id') IS s.database_instance_id
 AND json_extract(NEW.body_jcs,'$.request_epoch') IS s.request_epoch
 AND json_extract(NEW.body_jcs,'$.window.service_boot_id') IS NEW.service_boot_id
)
BEGIN SELECT RAISE(ABORT,'automation_recovery_preview_binding'); END;

CREATE TABLE automation_recovery_execution_commit (
 execution_id TEXT PRIMARY KEY NOT NULL REFERENCES automation_recovery_execution(execution_id),
 operation_id TEXT NOT NULL UNIQUE REFERENCES automation_recovery_start(operation_id),
 source_execution_id TEXT NOT NULL UNIQUE REFERENCES automation_execution(execution_id),
 run_id TEXT NOT NULL REFERENCES automation_run_creation(run_id),
 run_revision TEXT NOT NULL, slot_revision TEXT NOT NULL,
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=1048576),
 body_digest TEXT NOT NULL CHECK(length(body_digest)=71),
 audit_jcs TEXT NOT NULL CHECK(json_valid(audit_jcs)),
 notification_jcs TEXT NOT NULL CHECK(json_valid(notification_jcs)),
 UNIQUE(run_id,run_revision)
) STRICT;
CREATE INDEX automation_recovery_execution_commit_latest ON automation_recovery_execution_commit(run_id,length(run_revision) DESC,run_revision DESC);

CREATE TRIGGER automation_recovery_execution_commit_binding BEFORE INSERT ON automation_recovery_execution_commit
WHEN NOT EXISTS(
 SELECT 1 FROM automation_recovery_start s
 JOIN automation_recovery_operation o USING(operation_id)
 JOIN automation_recovery_execution e USING(operation_id)
 JOIN automation_recovery_lock l USING(operation_id)
 JOIN automation_execution p ON p.execution_id=NEW.source_execution_id
 JOIN automation_execution_head x ON x.execution_id=p.execution_id
 JOIN automation_activation_slot slot ON slot.definition_id=x.definition_id
 JOIN automation_run_approval_head h ON h.run_id=o.run_id
 JOIN automation_run_approval_state a ON a.approval_id=h.approval_id
 WHERE s.operation_id=NEW.operation_id AND e.execution_id=NEW.execution_id AND o.new_execution_id=e.execution_id
  AND o.run_id=NEW.run_id AND p.run_id=NEW.run_id AND slot.run_id=NEW.run_id AND slot.operation_id=p.arm_operation_id
  AND l.released_by IS NULL AND l.execution_id=e.execution_id AND l.run_id=o.run_id
  AND json_extract(NEW.body_jcs,'$.format') IS 'leafseamer.recovery-execution-commit/1'
  AND json_extract(NEW.body_jcs,'$.run_id') IS NEW.run_id
  AND json_extract(o.body_jcs,'$.source_execution_ref.object_id') IS p.execution_id
  AND json_extract(NEW.body_jcs,'$.source_execution_ref.object_id') IS p.execution_id
  AND json_extract(NEW.body_jcs,'$.source_execution_ref.digest') IS p.body_digest
  AND json_extract(NEW.body_jcs,'$.execution_ref.object_id') IS e.execution_id
  AND json_extract(NEW.body_jcs,'$.operation_ref.object_id') IS o.operation_id
  AND json_extract(NEW.body_jcs,'$.operation_ref.digest') IS o.body_digest
  AND json_extract(NEW.body_jcs,'$.start_ref.object_id') IS o.operation_id
  AND json_extract(NEW.body_jcs,'$.start_ref.digest') IS s.body_digest
  AND json_extract(NEW.body_jcs,'$.arm_generation') IS json_extract(s.body_jcs,'$.arm_generation')
  AND json_extract(s.body_jcs,'$.slot_revision') IS NEW.slot_revision
  AND json_extract(s.body_jcs,'$.run_revision') IS h.run_revision AND a.state='active'
  AND json_extract(s.body_jcs,'$.approval_ref.object_id') IS h.approval_id
  AND json_extract(NEW.body_jcs,'$.approval_ref') IS json_extract(s.body_jcs,'$.approval_ref')
  AND json_extract(NEW.body_jcs,'$.slot_revision') IS NEW.slot_revision
  AND json_extract(NEW.body_jcs,'$.run_revision_before') IS h.run_revision
  AND json_extract(NEW.body_jcs,'$.run_revision_after') IS NEW.run_revision
  AND length(NEW.run_revision) BETWEEN 1 AND 20 AND substr(NEW.run_revision,1,1) BETWEEN '1' AND '9'
  AND NEW.run_revision NOT GLOB '*[^0-9]*'
  AND CAST(substr('00000000000000000000'||NEW.run_revision,-10) AS INTEGER)=
   (CAST(substr('00000000000000000000'||h.run_revision,-10) AS INTEGER)+1)%10000000000
  AND CAST(substr('00000000000000000000'||NEW.run_revision,-20,10) AS INTEGER)=
   CAST(substr('00000000000000000000'||h.run_revision,-20,10) AS INTEGER)+
   (CAST(substr('00000000000000000000'||h.run_revision,-10) AS INTEGER)+1)/10000000000
  AND json_extract(NEW.body_jcs,'$.business_state_before') IS json_extract(p.body_jcs,'$.business_state')
  AND json_extract(NEW.body_jcs,'$.business_state_after.state_id') IS json_extract(p.body_jcs,'$.target_state_id')
  AND json_type(NEW.body_jcs,'$.business_state_after') IS
   CASE json_type(p.body_jcs,'$.target_state_id') WHEN 'text' THEN 'object' ELSE 'null' END
  AND json_extract(NEW.audit_jcs,'$.commit_digest') IS NEW.body_digest
  AND json_extract(NEW.notification_jcs,'$.commit_digest') IS NEW.body_digest
  AND json_extract(NEW.notification_jcs,'$.event_id') IS e.execution_id||'.completed'
  AND NOT EXISTS(SELECT 1 FROM automation_execution_commit WHERE execution_id=p.execution_id)
  AND NOT EXISTS(SELECT 1 FROM automation_recovery_dispatch_protection WHERE execution_id=e.execution_id)
  AND json_type(NEW.body_jcs,'$.steps')='array'
  AND json_array_length(NEW.body_jcs,'$.steps')=json_array_length(s.body_jcs,'$.steps')
  AND NOT EXISTS(
   SELECT 1 FROM json_each(s.body_jcs,'$.steps') step
   LEFT JOIN json_each(NEW.body_jcs,'$.steps') proof ON proof.key=step.key
   LEFT JOIN automation_execution_delay_elapsed old_delay
    ON old_delay.execution_id=p.execution_id AND old_delay.step_id=json_extract(step.value,'$.step_id')
   LEFT JOIN automation_execution_command_terminal old_terminal
    ON old_terminal.execution_id=p.execution_id AND old_terminal.step_id=json_extract(step.value,'$.step_id')
   LEFT JOIN automation_recovery_dispatch_delay_elapsed new_delay
    ON new_delay.execution_id=e.execution_id AND new_delay.step_id=json_extract(step.value,'$.step_id')
   LEFT JOIN automation_recovery_dispatch_command_terminal new_terminal
    ON new_terminal.execution_id=e.execution_id AND new_terminal.step_id=json_extract(step.value,'$.step_id')
   WHERE json_extract(proof.value,'$.step_id') IS NOT json_extract(step.value,'$.step_id')
    OR json_extract(proof.value,'$.outcome') IS NOT 'succeeded'
    OR json_extract(proof.value,'$.decision') IS NOT json_extract(step.value,'$.decision')
    OR json_extract(proof.value,'$.evidence_digest') IS NULL
    OR json_extract(proof.value,'$.evidence_digest') IS NOT
     CASE json_extract(step.value,'$.decision')
      WHEN 'retain_success' THEN coalesce(old_delay.body_digest,
       CASE WHEN json_extract(old_terminal.body_jcs,'$.state.outcome')='succeeded' THEN old_terminal.body_digest END)
      WHEN 'execute_new' THEN coalesce(new_delay.body_digest,
       CASE WHEN json_extract(new_terminal.body_jcs,'$.state.outcome')='succeeded' THEN new_terminal.body_digest END)
     END)
)
BEGIN SELECT RAISE(ABORT,'recovery_execution_commit_binding'); END;

DROP TRIGGER automation_recovery_lock_no_update;
CREATE TRIGGER automation_recovery_lock_no_update BEFORE UPDATE ON automation_recovery_lock
WHEN OLD.released_by IS NOT NULL OR NEW.operation_id IS NOT OLD.operation_id OR NEW.run_id IS NOT OLD.run_id
 OR NEW.execution_id IS NOT OLD.execution_id OR NOT EXISTS(
 SELECT 1 FROM automation_recovery_execution_commit c
 WHERE c.execution_id=OLD.execution_id AND c.operation_id=OLD.operation_id AND c.run_id=OLD.run_id
  AND c.body_digest IS NEW.released_by)
BEGIN SELECT RAISE(ABORT,'immutable_recovery_operation'); END;

DROP TRIGGER automation_execution_head_no_delete;
CREATE TRIGGER automation_execution_head_no_delete BEFORE DELETE ON automation_execution_head
WHEN NOT EXISTS(SELECT 1 FROM automation_execution_commit WHERE execution_id=OLD.execution_id)
 AND NOT EXISTS(SELECT 1 FROM automation_recovery_execution_commit c JOIN automation_recovery_lock l USING(operation_id)
 WHERE c.source_execution_id=OLD.execution_id AND c.body_digest=l.released_by)
BEGIN SELECT RAISE(ABORT,'immutable_execution_head'); END;

CREATE TRIGGER automation_recovery_execution_commit_apply AFTER INSERT ON automation_recovery_execution_commit BEGIN
 UPDATE automation_run_approval_head SET run_revision=NEW.run_revision
 WHERE run_id=NEW.run_id AND run_revision=json_extract(NEW.body_jcs,'$.run_revision_before');
 SELECT CASE WHEN changes()<>1 THEN RAISE(ABORT,'recovery_execution_commit_run') END;
 UPDATE automation_recovery_lock SET released_by=NEW.body_digest WHERE operation_id=NEW.operation_id AND released_by IS NULL;
 SELECT CASE WHEN changes()<>1 THEN RAISE(ABORT,'recovery_execution_commit_lock') END;
 DELETE FROM automation_execution_head WHERE execution_id=NEW.source_execution_id;
 SELECT CASE WHEN changes()<>1 THEN RAISE(ABORT,'recovery_execution_commit_slot') END;
END;
CREATE TRIGGER automation_recovery_execution_commit_no_update BEFORE UPDATE ON automation_recovery_execution_commit
BEGIN SELECT RAISE(ABORT,'immutable_recovery_execution_commit'); END;
CREATE TRIGGER automation_recovery_execution_commit_no_delete BEFORE DELETE ON automation_recovery_execution_commit
BEGIN SELECT RAISE(ABORT,'immutable_recovery_execution_commit'); END;
