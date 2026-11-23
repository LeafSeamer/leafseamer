-- 为已选分支封存执行计划与初始步骤。历史摄取和决定保持原样。
DROP TRIGGER first_pairing_import_target;
CREATE TABLE service_state_v25 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='25'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v25 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'25',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v25 RENAME TO service_state;
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

CREATE TABLE schema_migration_v25 (
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
  (from_revision='24' AND to_revision='25'))
) STRICT;
INSERT INTO schema_migration_v25 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v25 RENAME TO schema_migration;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;

CREATE TABLE automation_execution (
 execution_id TEXT PRIMARY KEY NOT NULL, run_id TEXT NOT NULL REFERENCES automation_run_creation(run_id),
 decision_id TEXT NOT NULL UNIQUE REFERENCES automation_trigger_decision(decision_id),
 arm_operation_id TEXT NOT NULL REFERENCES automation_arm_completion(operation_id),
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=1048576),
 body_digest TEXT NOT NULL CHECK(length(body_digest)=71),
 FOREIGN KEY(execution_id) REFERENCES automation_execution_seal(execution_id) DEFERRABLE INITIALLY DEFERRED,
 CHECK(json_extract(body_jcs,'$.execution_id') IS execution_id AND json_extract(body_jcs,'$.run_id') IS run_id
  AND json_extract(body_jcs,'$.decision_ref.object_id') IS decision_id
  AND json_extract(body_jcs,'$.arm_completion_ref.object_id') IS arm_operation_id
  AND json_type(body_jcs,'$.steps') IS 'array')
) STRICT;
CREATE TABLE automation_execution_step_plan (
 execution_id TEXT NOT NULL REFERENCES automation_execution(execution_id), step_id TEXT NOT NULL, ordinal INTEGER NOT NULL CHECK(ordinal>=0),
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=1048576),
 body_digest TEXT NOT NULL CHECK(length(body_digest)=71), PRIMARY KEY(execution_id,step_id), UNIQUE(execution_id,ordinal),
 CHECK(json_extract(body_jcs,'$.execution_id') IS execution_id AND json_extract(body_jcs,'$.step_id') IS step_id
  AND json_extract(body_jcs,'$.ordinal') IS ordinal)
) STRICT;
CREATE TABLE automation_execution_step_state (
 execution_id TEXT NOT NULL, step_id TEXT NOT NULL, initial_jcs TEXT NOT NULL CHECK(json_valid(initial_jcs)),
 initial_digest TEXT NOT NULL CHECK(length(initial_digest)=71), PRIMARY KEY(execution_id,step_id),
 FOREIGN KEY(execution_id,step_id) REFERENCES automation_execution_step_plan(execution_id,step_id),
 CHECK(json_extract(initial_jcs,'$.execution_id') IS execution_id AND json_extract(initial_jcs,'$.step_id') IS step_id
  AND json_extract(initial_jcs,'$.state') IS 'waiting' AND json_extract(initial_jcs,'$.revision') IS '1')
) STRICT;
CREATE TABLE automation_execution_seal (
 execution_id TEXT PRIMARY KEY NOT NULL REFERENCES automation_execution(execution_id),
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs)), body_digest TEXT NOT NULL CHECK(length(body_digest)=71)
) STRICT;
CREATE TABLE automation_execution_head (
 definition_id TEXT PRIMARY KEY NOT NULL REFERENCES automation_activation_slot(definition_id),
 execution_id TEXT NOT NULL UNIQUE REFERENCES automation_execution_seal(execution_id)
) STRICT;

CREATE TRIGGER automation_execution_seal_complete BEFORE INSERT ON automation_execution_seal BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM automation_execution e JOIN automation_trigger_decision d USING(decision_id)
  JOIN automation_arm_completion c ON c.operation_id=e.arm_operation_id AND c.run_id=e.run_id
  WHERE e.execution_id=NEW.execution_id AND e.run_id=d.run_id
   AND json_extract(NEW.body_jcs,'$.execution_ref.object_id') IS e.execution_id
   AND json_extract(NEW.body_jcs,'$.execution_ref.digest') IS e.body_digest
   AND json_extract(d.body_jcs,'$.disposition') IS 'accepted' AND json_extract(d.body_jcs,'$.execution_id') IS e.execution_id
   AND json_extract(e.body_jcs,'$.decision_ref.digest') IS d.body_digest
   AND json_extract(e.body_jcs,'$.arm_completion_ref.digest') IS c.body_digest
   AND json_array_length(e.body_jcs,'$.steps')=(SELECT count(*) FROM automation_execution_step_plan p WHERE p.execution_id=e.execution_id)
   AND NOT EXISTS(SELECT 1 FROM json_each(e.body_jcs,'$.steps') s
    LEFT JOIN automation_execution_step_plan p ON p.execution_id=e.execution_id AND p.ordinal=s.key
    LEFT JOIN automation_execution_step_state w ON w.execution_id=p.execution_id AND w.step_id=p.step_id
    WHERE p.step_id IS NOT json_extract(s.value,'$.step_id') OR p.body_digest IS NOT json_extract(s.value,'$.plan_digest')
     OR w.initial_digest IS NOT json_extract(s.value,'$.state_digest')
     OR json_extract(w.initial_jcs,'$.plan_digest') IS NOT p.body_digest)
 ) THEN RAISE(ABORT,'execution_plan_incomplete') END;
END;
CREATE TRIGGER automation_execution_head_binding BEFORE INSERT ON automation_execution_head
WHEN NOT EXISTS(SELECT 1 FROM automation_execution e JOIN automation_activation_slot s ON s.definition_id=NEW.definition_id
 WHERE e.execution_id=NEW.execution_id AND e.run_id=s.run_id AND e.arm_operation_id=s.operation_id
  AND json_extract(e.body_jcs,'$.activation_slot.revision') IS s.revision)
BEGIN SELECT RAISE(ABORT,'execution_slot_binding'); END;
CREATE TRIGGER automation_execution_acceptance BEFORE INSERT ON automation_trigger_decision_set BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM automation_trigger_decision d
  LEFT JOIN automation_execution e USING(decision_id) LEFT JOIN automation_execution_head h ON h.execution_id=e.execution_id
  WHERE d.ingest_epoch=NEW.ingest_epoch AND d.receive_sequence=NEW.receive_sequence AND (
   (json_extract(d.body_jcs,'$.disposition') IS 'accepted' AND h.execution_id IS NULL)
   OR (json_extract(d.body_jcs,'$.disposition') IS NOT 'accepted' AND e.execution_id IS NOT NULL)))
 THEN RAISE(ABORT,'execution_acceptance_incomplete') END;
END;
CREATE TRIGGER automation_execution_plan_closed BEFORE INSERT ON automation_execution_step_plan
WHEN EXISTS(SELECT 1 FROM automation_execution_seal s WHERE s.execution_id=NEW.execution_id)
BEGIN SELECT RAISE(ABORT,'immutable_execution_plan'); END;
CREATE TRIGGER automation_execution_no_update BEFORE UPDATE ON automation_execution BEGIN SELECT RAISE(ABORT,'immutable_execution'); END;
CREATE TRIGGER automation_execution_no_delete BEFORE DELETE ON automation_execution BEGIN SELECT RAISE(ABORT,'immutable_execution'); END;
CREATE TRIGGER automation_execution_step_plan_no_update BEFORE UPDATE ON automation_execution_step_plan BEGIN SELECT RAISE(ABORT,'immutable_execution_plan'); END;
CREATE TRIGGER automation_execution_step_plan_no_delete BEFORE DELETE ON automation_execution_step_plan BEGIN SELECT RAISE(ABORT,'immutable_execution_plan'); END;
CREATE TRIGGER automation_execution_step_state_no_update BEFORE UPDATE ON automation_execution_step_state BEGIN SELECT RAISE(ABORT,'immutable_initial_step_state'); END;
CREATE TRIGGER automation_execution_step_state_no_delete BEFORE DELETE ON automation_execution_step_state BEGIN SELECT RAISE(ABORT,'immutable_initial_step_state'); END;
CREATE TRIGGER automation_execution_seal_no_update BEFORE UPDATE ON automation_execution_seal BEGIN SELECT RAISE(ABORT,'immutable_execution_seal'); END;
CREATE TRIGGER automation_execution_seal_no_delete BEFORE DELETE ON automation_execution_seal BEGIN SELECT RAISE(ABORT,'immutable_execution_seal'); END;
CREATE TRIGGER automation_execution_head_no_update BEFORE UPDATE ON automation_execution_head BEGIN SELECT RAISE(ABORT,'immutable_execution_head'); END;
CREATE TRIGGER automation_execution_head_no_delete BEFORE DELETE ON automation_execution_head BEGIN SELECT RAISE(ABORT,'immutable_execution_head'); END;

