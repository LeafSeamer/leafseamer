-- 单次场次与事件共享执行封存和活动槽，分别保留各自来源。
DROP TRIGGER first_pairing_import_target;
DROP TRIGGER schedule_version_parent;
DROP TRIGGER schedule_activation_parent;
CREATE TABLE service_state_v38 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='38'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v38 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'38',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v38 RENAME TO service_state;
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

CREATE TABLE schema_migration_v38 (
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
  (from_revision='33' AND to_revision='34') OR (from_revision='34' AND to_revision='35') OR (from_revision='35' AND to_revision='36') OR (from_revision='36' AND to_revision='37') OR (from_revision='37' AND to_revision='38'))
) STRICT;
INSERT INTO schema_migration_v38 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v38 RENAME TO schema_migration;
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

DROP TRIGGER automation_execution_seal_complete;
PRAGMA legacy_alter_table=ON;
ALTER TABLE automation_execution RENAME TO automation_execution_v37;
CREATE TABLE automation_execution (
 execution_id TEXT PRIMARY KEY NOT NULL, run_id TEXT NOT NULL REFERENCES automation_run_creation(run_id),
 decision_id TEXT UNIQUE REFERENCES automation_trigger_decision(decision_id),
 arm_operation_id TEXT NOT NULL REFERENCES automation_arm_completion(operation_id),
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=1048576),
 body_digest TEXT NOT NULL CHECK(length(body_digest)=71),
 occurrence_id TEXT UNIQUE REFERENCES schedule_execution_admission(occurrence_id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(execution_id) REFERENCES automation_execution_seal(execution_id) DEFERRABLE INITIALLY DEFERRED,
 CHECK(json_extract(body_jcs,'$.execution_id') IS execution_id AND json_extract(body_jcs,'$.run_id') IS run_id
  AND json_extract(body_jcs,'$.arm_completion_ref.object_id') IS arm_operation_id AND json_type(body_jcs,'$.steps') IS 'array'
  AND ((decision_id IS NOT NULL AND occurrence_id IS NULL AND json_extract(body_jcs,'$.origin_kind') IS 'trigger'
    AND json_extract(body_jcs,'$.decision_ref.object_id') IS decision_id AND json_type(body_jcs,'$.admission_ref') IS NULL)
   OR (decision_id IS NULL AND occurrence_id IS NOT NULL AND json_extract(body_jcs,'$.origin_kind') IS 'schedule'
    AND json_extract(body_jcs,'$.admission_ref.object_id') IS occurrence_id
    AND json_type(body_jcs,'$.decision_ref') IS NULL AND json_type(body_jcs,'$.receive_cursor') IS NULL)))
) STRICT;
INSERT INTO automation_execution(execution_id,run_id,decision_id,arm_operation_id,body_jcs,body_digest)
 SELECT execution_id,run_id,decision_id,arm_operation_id,body_jcs,body_digest FROM automation_execution_v37;
DROP TABLE automation_execution_v37;
PRAGMA legacy_alter_table=OFF;
CREATE TRIGGER automation_execution_no_update BEFORE UPDATE ON automation_execution BEGIN SELECT RAISE(ABORT,'immutable_execution'); END;
CREATE TRIGGER automation_execution_no_delete BEFORE DELETE ON automation_execution BEGIN SELECT RAISE(ABORT,'immutable_execution'); END;

CREATE TABLE schedule_execution_admission (
 occurrence_id TEXT PRIMARY KEY NOT NULL REFERENCES schedule_occurrence(occurrence_id),
 execution_id TEXT NOT NULL UNIQUE REFERENCES automation_execution(execution_id),
 schedule_id TEXT NOT NULL,
 activation_revision TEXT NOT NULL,
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB)) BETWEEN 1 AND 8192),
 body_digest TEXT NOT NULL CHECK(length(body_digest)=71),
 FOREIGN KEY(occurrence_id,activation_revision) REFERENCES schedule_occurrence_clock(occurrence_id,activation_revision),
 FOREIGN KEY(schedule_id,activation_revision) REFERENCES schedule_activation(schedule_id,revision),
 CHECK(json_extract(body_jcs,'$.format') IS 'leafseamer.schedule-admission/1'
  AND json_extract(body_jcs,'$.occurrence_id') IS occurrence_id AND json_extract(body_jcs,'$.schedule_id') IS schedule_id
  AND json_extract(body_jcs,'$.execution_id') IS execution_id)
) STRICT;

CREATE TRIGGER schedule_execution_admission_binding BEFORE INSERT ON schedule_execution_admission
WHEN NOT EXISTS(
 SELECT 1 FROM automation_execution e JOIN automation_execution_head h USING(execution_id)
 JOIN automation_run_approval_head r ON r.run_id=e.run_id
 JOIN automation_run_approval_state approval ON approval.approval_id=r.approval_id
 JOIN schedule_activation a ON a.schedule_id=NEW.schedule_id AND a.revision=NEW.activation_revision
 JOIN schedule_version v ON v.schedule_id=a.schedule_id AND v.version=a.schedule_version AND v.definition_id=h.definition_id
 JOIN schedule_occurrence o ON o.occurrence_id=NEW.occurrence_id AND o.schedule_id=v.schedule_id
 JOIN schedule_occurrence_clock c ON c.occurrence_id=o.occurrence_id AND c.schedule_id=a.schedule_id AND c.activation_revision=a.revision
 JOIN automation_execution_step_plan p ON p.execution_id=e.execution_id AND p.step_id=json_extract(NEW.body_jcs,'$.first_step_id')
 JOIN json_each(v.body_jcs,'$.items') item ON json_extract(item.value,'$.occurrence_id') IS o.occurrence_id
 JOIN json_each(item.value,'$.step_windows') w ON json_extract(w.value,'$.step_id') IS p.step_id
 WHERE e.execution_id=NEW.execution_id AND e.occurrence_id=o.occurrence_id
  AND e.run_id IS json_extract(NEW.body_jcs,'$.run_id') AND approval.state='active'
  AND r.run_revision IS json_extract(e.body_jcs,'$.run_revision')
  AND json_extract(NEW.body_jcs,'$.scope') IS json_extract(e.body_jcs,'$.scope')
  AND json_extract(e.body_jcs,'$.admission_ref.digest') IS NEW.body_digest
  AND json_extract(NEW.body_jcs,'$.logical_key_digest') IS o.logical_key_digest
  AND json_extract(NEW.body_jcs,'$.activation_ref.id') IS json_extract(a.body_jcs,'$.activation_id')
  AND json_extract(NEW.body_jcs,'$.activation_ref.digest') IS a.body_digest
  AND json_extract(NEW.body_jcs,'$.activation_ref.revision') IS CAST(CAST(a.revision AS INTEGER) AS TEXT)
  AND json_extract(NEW.body_jcs,'$.schedule_version_ref') IS json_extract(a.body_jcs,'$.schedule_version_ref')
  AND json_extract(NEW.body_jcs,'$.artifact_ref') IS json_extract(v.body_jcs,'$.definition_artifact_ref')
  AND json_extract(NEW.body_jcs,'$.artifact_ref') IS json_extract(e.body_jcs,'$.artifact_ref')
  AND json_extract(NEW.body_jcs,'$.boundary_ref.digest') IS c.body_digest AND c.fault=0
  AND json_extract(NEW.body_jcs,'$.boundary_ref.id') IS json_extract(c.body_jcs,'$.boundary_id')
  AND json_extract(NEW.body_jcs,'$.step_window_ref') IS json_extract(c.body_jcs,'$.step_window_ref')
  AND json_extract(NEW.body_jcs,'$.step_window_ref.id') IS json_extract(w.value,'$.step_window_id')
  AND json_extract(NEW.body_jcs,'$.step_window_ref.digest') IS json_extract(w.value,'$.step_window_digest')
  AND json_extract(NEW.body_jcs,'$.clock.domain') IS json_extract(c.body_jcs,'$.domain')
  AND json_type(NEW.body_jcs,'$.clock.tick_ns')='text'
  AND length(json_extract(NEW.body_jcs,'$.clock.tick_ns')) BETWEEN 1 AND 20
  AND json_extract(NEW.body_jcs,'$.clock.tick_ns') NOT GLOB '*[^0-9]*'
  AND substr('00000000000000000000'||json_extract(NEW.body_jcs,'$.clock.tick_ns'),-20)>=max(c.window_enter_tick,c.observed_tick)
  AND substr('00000000000000000000'||json_extract(NEW.body_jcs,'$.clock.tick_ns'),-20)<c.qualified_before_tick
  AND json_extract(NEW.body_jcs,'$.service_boot_id') IS json_extract(c.body_jcs,'$.service_boot_id')
  AND json_extract(NEW.body_jcs,'$.service_boot_id') IS json_extract(e.body_jcs,'$.service_boot_id')
  AND json_extract(p.body_jcs,'$.dependencies') IS '[]'
  AND json_extract(p.body_jcs,'$.payload.binding.action_binding_ref.id') IS json_extract(w.value,'$.action_binding_id')
  AND NOT EXISTS(SELECT 1 FROM schedule_activation newer WHERE newer.schedule_id=a.schedule_id AND newer.revision>a.revision)
  AND NOT EXISTS(SELECT 1 FROM schedule_occurrence_stop stop WHERE stop.occurrence_id=o.occurrence_id)
)
BEGIN SELECT RAISE(ABORT,'schedule_execution_admission_binding'); END;
CREATE TRIGGER schedule_execution_admission_no_update BEFORE UPDATE ON schedule_execution_admission
BEGIN SELECT RAISE(ABORT,'immutable_schedule_execution_admission'); END;
CREATE TRIGGER schedule_execution_admission_no_delete BEFORE DELETE ON schedule_execution_admission
BEGIN SELECT RAISE(ABORT,'immutable_schedule_execution_admission'); END;

CREATE TRIGGER automation_execution_seal_complete BEFORE INSERT ON automation_execution_seal BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM automation_execution e LEFT JOIN automation_trigger_decision d USING(decision_id)
  JOIN automation_arm_completion c ON c.operation_id=e.arm_operation_id AND c.run_id=e.run_id
  WHERE e.execution_id=NEW.execution_id
   AND json_extract(NEW.body_jcs,'$.execution_ref.object_id') IS e.execution_id
   AND json_extract(NEW.body_jcs,'$.execution_ref.digest') IS e.body_digest
   AND ((e.decision_id IS NOT NULL AND e.run_id=d.run_id
    AND json_extract(d.body_jcs,'$.disposition') IS 'accepted' AND json_extract(d.body_jcs,'$.execution_id') IS e.execution_id
    AND json_extract(e.body_jcs,'$.decision_ref.digest') IS d.body_digest)
    OR (e.occurrence_id IS NOT NULL AND json_extract(NEW.body_jcs,'$.admission_ref') IS json_extract(e.body_jcs,'$.admission_ref')))
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
