-- 普通运行成功提交及持久业务状态投影。
DROP TRIGGER first_pairing_import_target;
CREATE TABLE service_state_v32 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='32'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v32 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'32',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v32 RENAME TO service_state;
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

CREATE TABLE schema_migration_v32 (
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
  (from_revision='24' AND to_revision='25') OR (from_revision='25' AND to_revision='26') OR (from_revision='26' AND to_revision='27') OR (from_revision='27' AND to_revision='28') OR (from_revision='28' AND to_revision='29') OR (from_revision='29' AND to_revision='30') OR (from_revision='30' AND to_revision='31') OR (from_revision='31' AND to_revision='32'))
) STRICT;
INSERT INTO schema_migration_v32 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v32 RENAME TO schema_migration;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;

CREATE TABLE automation_execution_commit (
 execution_id TEXT PRIMARY KEY NOT NULL REFERENCES automation_execution_seal(execution_id),
 run_id TEXT NOT NULL REFERENCES automation_run_creation(run_id),
 run_revision TEXT NOT NULL, slot_revision TEXT NOT NULL,
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=1048576),
 body_digest TEXT NOT NULL CHECK(length(body_digest)=71),
 audit_jcs TEXT NOT NULL CHECK(json_valid(audit_jcs)),
 notification_jcs TEXT NOT NULL CHECK(json_valid(notification_jcs)),
 UNIQUE(run_id,run_revision)
) STRICT;
CREATE INDEX automation_execution_commit_latest ON automation_execution_commit(run_id,length(run_revision) DESC,run_revision DESC);

DROP TRIGGER automation_execution_step_ready_binding;
CREATE TRIGGER automation_execution_step_ready_binding BEFORE INSERT ON automation_execution_step_ready
WHEN NOT EXISTS(SELECT 1 FROM automation_execution_step_plan p JOIN automation_execution_anchor a USING(execution_id)
 WHERE p.execution_id=NEW.execution_id AND p.step_id=NEW.step_id
  AND NEW.service_boot_id=a.service_boot_id AND NEW.clock_domain_id=a.clock_domain_id
  AND json_extract(NEW.body_jcs,'$.execution_anchor_ref.digest') IS a.body_digest
  AND json_extract(NEW.body_jcs,'$.step_plan_digest') IS p.body_digest
  AND NOT EXISTS(SELECT 1 FROM json_each(p.body_jcs,'$.dependencies') dep
   WHERE NOT EXISTS(SELECT 1 FROM automation_execution_delay_elapsed d
     WHERE d.execution_id=p.execution_id AND d.step_id=json_extract(dep.value,'$.from_step_id'))
    AND NOT EXISTS(SELECT 1 FROM automation_execution_command_terminal t
     JOIN automation_execution_step_plan predecessor USING(execution_id,step_id),
     json_each(NEW.body_jcs,'$.command_terminal_refs') evidence
     WHERE t.execution_id=p.execution_id AND t.step_id=json_extract(dep.value,'$.from_step_id')
      AND json_extract(dep.value,'$.edge_kind') IN ('sequence','requires_success')
      AND (json_extract(t.body_jcs,'$.state.outcome')='succeeded' OR (
       json_extract(dep.value,'$.edge_kind')='sequence' AND json_extract(t.body_jcs,'$.state.outcome')='failed'
       AND json_extract(predecessor.body_jcs,'$.required')=0 AND json_extract(predecessor.body_jcs,'$.failure_policy')='continue_if_independent'
       AND json_extract(t.body_jcs,'$.state.resource_status')='clear'
       AND json_extract(t.body_jcs,'$.state.automation_continuation')='eligible'))
      AND json_extract(evidence.value,'$.step_id') IS t.step_id
      AND json_extract(evidence.value,'$.terminal_digest') IS t.body_digest)))
BEGIN SELECT RAISE(ABORT,'execution_step_ready_binding'); END;

DROP TRIGGER automation_execution_head_binding;
CREATE TRIGGER automation_execution_head_binding BEFORE INSERT ON automation_execution_head
WHEN NOT EXISTS(SELECT 1 FROM automation_execution e JOIN automation_activation_slot s ON s.definition_id=NEW.definition_id
 WHERE e.execution_id=NEW.execution_id AND e.run_id=s.run_id AND e.arm_operation_id=s.operation_id
  AND json_extract(e.body_jcs,'$.activation_slot.revision') IS COALESCE((SELECT c.slot_revision
   FROM automation_execution_commit c WHERE c.run_id=e.run_id ORDER BY length(c.run_revision) DESC,c.run_revision DESC LIMIT 1),s.revision)
  AND NOT EXISTS(SELECT 1 FROM automation_execution_commit c WHERE c.execution_id=e.execution_id))
BEGIN SELECT RAISE(ABORT,'execution_slot_binding'); END;

DROP TRIGGER automation_execution_head_no_delete;
CREATE TRIGGER automation_execution_head_no_delete BEFORE DELETE ON automation_execution_head
WHEN NOT EXISTS(SELECT 1 FROM automation_execution_commit WHERE execution_id=OLD.execution_id)
BEGIN SELECT RAISE(ABORT,'immutable_execution_head'); END;

CREATE TRIGGER automation_execution_commit_binding BEFORE INSERT ON automation_execution_commit
WHEN NOT EXISTS(SELECT 1 FROM automation_execution e JOIN automation_execution_head h USING(execution_id)
 JOIN automation_execution_anchor a USING(execution_id)
 JOIN automation_run_approval_head r ON r.run_id=e.run_id
 JOIN automation_run_approval_state approval ON approval.approval_id=r.approval_id
 WHERE e.execution_id=NEW.execution_id AND e.run_id=NEW.run_id AND approval.state='active'
  AND json_extract(e.body_jcs,'$.origin_kind')='trigger'
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

CREATE TRIGGER automation_execution_commit_apply AFTER INSERT ON automation_execution_commit BEGIN
 UPDATE automation_run_approval_head SET run_revision=NEW.run_revision
 WHERE run_id=NEW.run_id AND run_revision=json_extract(NEW.body_jcs,'$.run_revision_before');
 SELECT CASE WHEN changes()<>1 THEN RAISE(ABORT,'execution_commit_run') END;
 DELETE FROM automation_execution_head WHERE execution_id=NEW.execution_id;
 SELECT CASE WHEN changes()<>1 THEN RAISE(ABORT,'execution_commit_slot') END;
END;
CREATE TRIGGER automation_execution_commit_no_update BEFORE UPDATE ON automation_execution_commit BEGIN SELECT RAISE(ABORT,'immutable_execution_commit'); END;
CREATE TRIGGER automation_execution_commit_no_delete BEFORE DELETE ON automation_execution_commit BEGIN SELECT RAISE(ABORT,'immutable_execution_commit'); END;

