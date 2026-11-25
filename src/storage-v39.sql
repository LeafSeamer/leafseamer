-- 定时场次从持久受理原件启动执行锚。
DROP TRIGGER first_pairing_import_target;
DROP TRIGGER schedule_version_parent;
DROP TRIGGER schedule_activation_parent;
CREATE TABLE service_state_v39 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='39'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v39 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'39',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v39 RENAME TO service_state;
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

CREATE TABLE schema_migration_v39 (
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
  (from_revision='33' AND to_revision='34') OR (from_revision='34' AND to_revision='35') OR (from_revision='35' AND to_revision='36') OR (from_revision='36' AND to_revision='37') OR (from_revision='37' AND to_revision='38') OR (from_revision='38' AND to_revision='39'))
) STRICT;
INSERT INTO schema_migration_v39 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v39 RENAME TO schema_migration;
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

DROP TRIGGER automation_execution_anchor_binding;
CREATE TRIGGER automation_execution_anchor_binding BEFORE INSERT ON automation_execution_anchor
WHEN NOT EXISTS(SELECT 1 FROM automation_execution e JOIN automation_execution_head h USING(execution_id)
 LEFT JOIN automation_trigger_decision d USING(decision_id)
 LEFT JOIN automation_trigger_decision_set t ON t.ingest_epoch=d.ingest_epoch AND t.receive_sequence=d.receive_sequence
 LEFT JOIN schedule_execution_admission sa ON sa.execution_id=e.execution_id AND sa.occurrence_id=e.occurrence_id
 WHERE e.execution_id=NEW.execution_id AND json_extract(e.body_jcs,'$.service_boot_id') IS NEW.service_boot_id
  AND json_extract(NEW.body_jcs,'$.execution_ref.digest') IS e.body_digest
  AND ((t.ingest_epoch IS NOT NULL AND json_extract(NEW.body_jcs,'$.origin_ref.object_id') IS d.decision_id
    AND json_extract(NEW.body_jcs,'$.origin_ref.digest') IS d.body_digest)
   OR (sa.execution_id IS NOT NULL AND json_extract(NEW.body_jcs,'$.origin_ref') IS json_extract(e.body_jcs,'$.admission_ref')
    AND sa.body_digest IS json_extract(e.body_jcs,'$.origin_digest') AND json_extract(e.body_jcs,'$.runtime_revision')='3'
    AND json_extract(sa.body_jcs,'$.service_boot_id') IS NEW.service_boot_id
    AND json_extract(sa.body_jcs,'$.clock.domain') IS json_extract(NEW.body_jcs,'$.clock.runtime_domain_ref'))))
BEGIN SELECT RAISE(ABORT,'execution_anchor_binding'); END;

DROP TRIGGER automation_execution_commit_binding;
CREATE TRIGGER automation_execution_commit_binding BEFORE INSERT ON automation_execution_commit
WHEN NOT EXISTS(SELECT 1 FROM automation_execution e JOIN automation_execution_head h USING(execution_id)
 JOIN automation_execution_anchor a USING(execution_id)
 JOIN automation_run_approval_head r ON r.run_id=e.run_id
 JOIN automation_run_approval_state approval ON approval.approval_id=r.approval_id
 WHERE e.execution_id=NEW.execution_id AND e.run_id=NEW.run_id AND approval.state='active'
  AND (e.decision_id IS NOT NULL OR EXISTS(SELECT 1 FROM schedule_execution_admission sa WHERE sa.execution_id=e.execution_id AND sa.occurrence_id=e.occurrence_id AND sa.body_digest IS json_extract(e.body_jcs,'$.origin_digest')))
  AND json_extract(e.body_jcs,'$.activation_slot_revision') IS NEW.slot_revision
  AND json_extract(e.body_jcs,'$.run_revision') IS r.run_revision
  AND json_extract(NEW.body_jcs,'$.run_revision_before') IS r.run_revision
  AND json_extract(NEW.body_jcs,'$.run_revision_after') IS NEW.run_revision
  AND length(NEW.run_revision) BETWEEN 1 AND 20 AND substr(NEW.run_revision,1,1) BETWEEN '1' AND '9'
  AND NEW.run_revision NOT GLOB '*[^0-9]*'
  AND CAST(substr('00000000000000000000'||NEW.run_revision,-10) AS INTEGER)=
   (CAST(substr('00000000000000000000'||r.run_revision,-10) AS INTEGER)+1)%10000000000
  AND CAST(substr('00000000000000000000'||NEW.run_revision,-20,10) AS INTEGER)=
   CAST(substr('00000000000000000000'||r.run_revision,-20,10) AS INTEGER)+
   (CAST(substr('00000000000000000000'||r.run_revision,-10) AS INTEGER)+1)/10000000000
  AND json_extract(NEW.body_jcs,'$.approval_ref.object_id') IS r.approval_id
  AND json_extract(NEW.body_jcs,'$.execution_ref.object_id') IS e.execution_id
  AND json_extract(NEW.body_jcs,'$.execution_ref.digest') IS e.body_digest
  AND json_extract(NEW.body_jcs,'$.anchor_ref.digest') IS a.body_digest
  AND json_extract(NEW.body_jcs,'$.business_state_before') IS json_extract(e.body_jcs,'$.business_state')
  AND json_extract(NEW.body_jcs,'$.business_state_after.state_id') IS json_extract(e.body_jcs,'$.target_state_id')
  AND json_type(NEW.body_jcs,'$.business_state_after') IS CASE json_type(e.body_jcs,'$.target_state_id') WHEN 'text' THEN 'object' ELSE 'null' END
  AND (json_type(NEW.body_jcs,'$.business_state_after')='null' OR json_type(NEW.body_jcs,'$.business_state_after.revision')='text')
  AND json_extract(NEW.audit_jcs,'$.commit_digest') IS NEW.body_digest
  AND json_extract(NEW.notification_jcs,'$.commit_digest') IS NEW.body_digest
  AND json_extract(NEW.notification_jcs,'$.event_id') IS e.execution_id||'.completed'
  AND NOT EXISTS(SELECT 1 FROM automation_execution_protection p WHERE p.execution_id=e.execution_id)
  AND json_type(NEW.body_jcs,'$.steps')='array'
  AND json_array_length(NEW.body_jcs,'$.steps')=(SELECT count(*) FROM automation_execution_step_plan WHERE execution_id=e.execution_id)
  AND NOT EXISTS(SELECT 1 FROM automation_execution_step_plan p
   LEFT JOIN automation_execution_delay_elapsed d USING(execution_id,step_id)
   LEFT JOIN automation_execution_command_terminal t USING(execution_id,step_id)
   LEFT JOIN json_each(NEW.body_jcs,'$.steps') proof ON proof.key=p.ordinal
   WHERE p.execution_id=e.execution_id AND (json_extract(proof.value,'$.step_id') IS NOT p.step_id OR
    CASE json_extract(p.body_jcs,'$.payload.kind')
     WHEN 'delay' THEN d.step_id IS NULL OR json_extract(proof.value,'$.evidence_digest') IS NOT d.body_digest
      OR json_extract(proof.value,'$.outcome') IS NOT 'succeeded'
     WHEN 'action' THEN t.step_id IS NULL OR json_extract(proof.value,'$.evidence_digest') IS NOT t.body_digest
      OR json_extract(proof.value,'$.outcome') IS NOT json_extract(t.body_jcs,'$.state.outcome')
      OR (json_extract(t.body_jcs,'$.state.outcome')='succeeded' OR
       (json_extract(t.body_jcs,'$.state.outcome')='failed' AND json_extract(p.body_jcs,'$.required')=0
        AND json_extract(p.body_jcs,'$.failure_policy')='continue_if_independent'
        AND json_extract(t.body_jcs,'$.state.resource_status')='clear'
        AND json_extract(t.body_jcs,'$.state.automation_continuation')='eligible')) IS NOT TRUE
     ELSE 1 END)))
BEGIN SELECT RAISE(ABORT,'execution_commit_binding'); END;
