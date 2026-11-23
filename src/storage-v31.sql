-- 接通已证实动作终态到依赖步骤就绪。
DROP TRIGGER first_pairing_import_target;
CREATE TABLE service_state_v31 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='31'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v31 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'31',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v31 RENAME TO service_state;
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

CREATE TABLE schema_migration_v31 (
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
  (from_revision='24' AND to_revision='25') OR (from_revision='25' AND to_revision='26') OR (from_revision='26' AND to_revision='27') OR (from_revision='27' AND to_revision='28') OR (from_revision='28' AND to_revision='29') OR (from_revision='29' AND to_revision='30') OR (from_revision='30' AND to_revision='31'))
) STRICT;
INSERT INTO schema_migration_v31 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v31 RENAME TO schema_migration;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;

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
    AND NOT EXISTS(SELECT 1 FROM automation_execution_command_terminal t,
       json_each(NEW.body_jcs,'$.command_terminal_refs') evidence
     WHERE t.execution_id=p.execution_id AND t.step_id=json_extract(dep.value,'$.from_step_id')
      AND json_extract(dep.value,'$.edge_kind') IN ('sequence','requires_success')
      AND json_extract(t.body_jcs,'$.state.outcome')='succeeded'
      AND json_extract(evidence.value,'$.step_id') IS t.step_id
      AND json_extract(evidence.value,'$.terminal_digest') IS t.body_digest)))
BEGIN SELECT RAISE(ABORT,'execution_step_ready_binding'); END;
