-- 封存就绪动作的参数原件。历史运行原件保持原样。
DROP TRIGGER first_pairing_import_target;
CREATE TABLE service_state_v28 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='28'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v28 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'28',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v28 RENAME TO service_state;
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

CREATE TABLE schema_migration_v28 (
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
  (from_revision='24' AND to_revision='25') OR (from_revision='25' AND to_revision='26') OR (from_revision='26' AND to_revision='27') OR (from_revision='27' AND to_revision='28'))
) STRICT;
INSERT INTO schema_migration_v28 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v28 RENAME TO schema_migration;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;

CREATE TABLE automation_execution_action_parameters (
 execution_id TEXT NOT NULL, step_id TEXT NOT NULL,
 service_boot_id TEXT NOT NULL, clock_domain_id TEXT NOT NULL, acquisition_id TEXT NOT NULL,
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=1048576),
 body_digest TEXT NOT NULL CHECK(length(body_digest)=71), PRIMARY KEY(execution_id,step_id),
 FOREIGN KEY(execution_id,step_id) REFERENCES automation_execution_step_ready(execution_id,step_id),
 FOREIGN KEY(service_boot_id,clock_domain_id,acquisition_id)
  REFERENCES authentication_clock_acquisition(service_boot_id,clock_domain_id,id),
 CHECK(json_extract(body_jcs,'$.execution_ref.object_id') IS execution_id
  AND json_extract(body_jcs,'$.step_id') IS step_id
  AND json_type(body_jcs,'$.parameters') IS 'object'
  AND json_extract(body_jcs,'$.clock.runtime_domain_ref.clock_domain_id') IS clock_domain_id
  AND json_extract(body_jcs,'$.clock.acquisition_ref.id') IS acquisition_id)
) STRICT;
CREATE TRIGGER automation_execution_action_parameters_binding BEFORE INSERT ON automation_execution_action_parameters
WHEN NOT EXISTS(SELECT 1 FROM automation_execution_step_plan p JOIN automation_execution_step_ready r USING(execution_id,step_id)
 JOIN automation_execution e USING(execution_id) JOIN automation_execution_anchor a USING(execution_id)
 JOIN automation_execution_head h USING(execution_id)
 LEFT JOIN automation_execution_protection x USING(execution_id)
 WHERE p.execution_id=NEW.execution_id AND p.step_id=NEW.step_id AND x.execution_id IS NULL
  AND a.service_boot_id=NEW.service_boot_id AND a.clock_domain_id=NEW.clock_domain_id
  AND json_extract(p.body_jcs,'$.payload.kind') IS 'action'
  AND json_extract(e.body_jcs,'$.runtime_revision') IS '3'
  AND json_extract(NEW.body_jcs,'$.execution_ref.digest') IS e.body_digest
  AND json_extract(NEW.body_jcs,'$.origin_digest') IS json_extract(e.body_jcs,'$.origin_digest')
  AND json_extract(NEW.body_jcs,'$.step_ready_ref.digest') IS r.body_digest
  AND json_extract(NEW.body_jcs,'$.step_plan_digest') IS p.body_digest)
BEGIN SELECT RAISE(ABORT,'execution_action_parameters_binding'); END;
CREATE TRIGGER automation_execution_action_parameters_no_update BEFORE UPDATE ON automation_execution_action_parameters
BEGIN SELECT RAISE(ABORT,'immutable_execution_action_parameters'); END;
CREATE TRIGGER automation_execution_action_parameters_no_delete BEFORE DELETE ON automation_execution_action_parameters
BEGIN SELECT RAISE(ABORT,'immutable_execution_action_parameters'); END;
