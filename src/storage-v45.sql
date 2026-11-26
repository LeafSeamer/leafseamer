-- 显式停机迁移。恢复动作和正式命令提交原件。
DROP TRIGGER first_pairing_import_target;
DROP TRIGGER schedule_version_parent;
DROP TRIGGER schedule_activation_parent;
DROP TRIGGER automation_recovery_preview_binding;
CREATE TABLE service_state_v45 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='45'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v45 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'45',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v45 RENAME TO service_state;
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

CREATE TABLE schema_migration_v45 (
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
  (from_revision='33' AND to_revision='34') OR (from_revision='34' AND to_revision='35') OR (from_revision='35' AND to_revision='36') OR (from_revision='36' AND to_revision='37') OR (from_revision='37' AND to_revision='38') OR (from_revision='38' AND to_revision='39') OR (from_revision='39' AND to_revision='40') OR (from_revision='40' AND to_revision='41') OR (from_revision='41' AND to_revision='42') OR (from_revision='42' AND to_revision='43') OR (from_revision='43' AND to_revision='44') OR (from_revision='44' AND to_revision='45'))
) STRICT;
INSERT INTO schema_migration_v45 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v45 RENAME TO schema_migration;
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

CREATE TABLE automation_recovery_dispatch_action_parameters (
 execution_id TEXT NOT NULL REFERENCES automation_recovery_execution(execution_id), step_id TEXT NOT NULL,
 execution_digest TEXT NOT NULL CHECK(length(execution_digest)=71),
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=131072),
 body_digest TEXT NOT NULL CHECK(length(body_digest)=71),
 ready_jcs TEXT NOT NULL CHECK(json_valid(ready_jcs) AND length(CAST(ready_jcs AS BLOB))<=32768),
 ready_digest TEXT NOT NULL CHECK(length(ready_digest)=71),
 PRIMARY KEY(execution_id,step_id)
) STRICT;
CREATE TABLE automation_recovery_dispatch_protection (
 execution_id TEXT PRIMARY KEY NOT NULL REFERENCES automation_recovery_execution(execution_id), reason TEXT NOT NULL
) STRICT;
CREATE TRIGGER recovery_dispatch_parameters_binding BEFORE INSERT ON automation_recovery_dispatch_action_parameters
WHEN NOT EXISTS (
 SELECT 1 FROM automation_recovery_execution e JOIN automation_recovery_start s USING(operation_id)
 JOIN automation_recovery_lock l USING(operation_id),json_each(s.body_jcs,'$.steps') p
 WHERE e.execution_id=NEW.execution_id AND l.released_by IS NULL
 AND json_extract(p.value,'$.step_id') IS NEW.step_id AND json_extract(p.value,'$.decision') IS 'execute_new'
 AND json_extract(p.value,'$.grant_ref') IS json_extract(NEW.body_jcs,'$.grant_ref') AND json_type(p.value,'$.grant_ref') IS 'object'
 AND json_extract(NEW.body_jcs,'$.execution_ref.object_id') IS NEW.execution_id
 AND json_extract(NEW.body_jcs,'$.execution_ref.revision') IS '2'
 AND json_extract(NEW.body_jcs,'$.execution_ref.digest') IS NEW.execution_digest
 AND json_extract(NEW.body_jcs,'$.step_id') IS NEW.step_id
 AND json_extract(NEW.body_jcs,'$.step_ready_ref.digest') IS NEW.ready_digest
 AND json_extract(NEW.ready_jcs,'$.execution_ref') IS json_extract(NEW.body_jcs,'$.execution_ref')
) OR EXISTS(SELECT 1 FROM automation_recovery_dispatch_protection WHERE execution_id=NEW.execution_id)
BEGIN SELECT RAISE(ABORT,'recovery_dispatch_parameters_binding'); END;
CREATE TRIGGER recovery_dispatch_parameters_no_update BEFORE UPDATE ON automation_recovery_dispatch_action_parameters
BEGIN SELECT RAISE(ABORT,'immutable_recovery_dispatch'); END;
CREATE TRIGGER recovery_dispatch_parameters_no_delete BEFORE DELETE ON automation_recovery_dispatch_action_parameters
BEGIN SELECT RAISE(ABORT,'immutable_recovery_dispatch'); END;
CREATE TRIGGER recovery_dispatch_protection_no_update BEFORE UPDATE ON automation_recovery_dispatch_protection
BEGIN SELECT RAISE(ABORT,'immutable_recovery_dispatch'); END;
CREATE TRIGGER recovery_dispatch_protection_no_delete BEFORE DELETE ON automation_recovery_dispatch_protection
BEGIN SELECT RAISE(ABORT,'immutable_recovery_dispatch'); END;

CREATE TABLE automation_recovery_dispatch_submission (
 execution_id TEXT NOT NULL, step_id TEXT NOT NULL, idempotency_key TEXT NOT NULL UNIQUE,
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=8192),
 body_digest TEXT NOT NULL CHECK(length(body_digest)=71),
 PRIMARY KEY(execution_id,step_id), UNIQUE(execution_id,step_id,body_digest),
 FOREIGN KEY(execution_id,step_id) REFERENCES automation_recovery_dispatch_action_parameters(execution_id,step_id),
 CHECK(json_extract(body_jcs,'$.request.action.execution_ref.object_id') IS execution_id
  AND json_extract(body_jcs,'$.request.action.step_id') IS step_id
  AND json_extract(body_jcs,'$.request.idempotency_key') IS idempotency_key)
) STRICT;
CREATE TRIGGER automation_recovery_dispatch_submission_binding BEFORE INSERT ON automation_recovery_dispatch_submission
WHEN NOT EXISTS(SELECT 1 FROM automation_recovery_dispatch_action_parameters p
 WHERE p.execution_id=NEW.execution_id AND p.step_id=NEW.step_id
 AND json_extract(NEW.body_jcs,'$.request.action.parameters_ref.digest') IS p.body_digest
 AND json_extract(NEW.body_jcs,'$.request.action.execution_ref.digest') IS p.execution_digest)
 OR EXISTS(SELECT 1 FROM automation_recovery_dispatch_protection WHERE execution_id=NEW.execution_id)
BEGIN SELECT RAISE(ABORT,'execution_submission_binding'); END;
CREATE TRIGGER automation_recovery_dispatch_submission_no_update BEFORE UPDATE ON automation_recovery_dispatch_submission
BEGIN SELECT RAISE(ABORT,'immutable_execution_submission'); END;
CREATE TRIGGER automation_recovery_dispatch_submission_no_delete BEFORE DELETE ON automation_recovery_dispatch_submission
BEGIN SELECT RAISE(ABORT,'immutable_execution_submission'); END;

CREATE TABLE automation_recovery_dispatch_submission_attempt (
 execution_id TEXT NOT NULL, step_id TEXT NOT NULL, submission_digest TEXT NOT NULL, attempt_id TEXT NOT NULL UNIQUE,
 service_boot_id TEXT NOT NULL, clock_domain_id TEXT NOT NULL, acquisition_id TEXT NOT NULL,
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=2048),
 body_digest TEXT NOT NULL CHECK(length(body_digest)=71), PRIMARY KEY(execution_id,step_id),
 FOREIGN KEY(execution_id,step_id,submission_digest) REFERENCES automation_recovery_dispatch_submission(execution_id,step_id,body_digest),
 CHECK(json_extract(body_jcs,'$.attempt_id') IS attempt_id AND json_extract(body_jcs,'$.submission_digest') IS submission_digest
  AND json_extract(body_jcs,'$.service_boot_id') IS service_boot_id
  AND json_extract(body_jcs,'$.clock.runtime_domain_ref.clock_domain_id') IS clock_domain_id
  AND json_extract(body_jcs,'$.clock.acquisition_ref.id') IS acquisition_id)
) STRICT;
CREATE TRIGGER automation_recovery_dispatch_submission_attempt_binding BEFORE INSERT ON automation_recovery_dispatch_submission_attempt
WHEN NOT EXISTS(SELECT 1 FROM automation_recovery_execution e JOIN automation_recovery_start s USING(operation_id)
 JOIN automation_recovery_lock l USING(operation_id)
 WHERE e.execution_id=NEW.execution_id AND l.released_by IS NULL
 AND json_extract(s.body_jcs,'$.service_boot_id') IS NEW.service_boot_id
 AND json_extract(s.body_jcs,'$.observed_at.domain.clock_domain_id') IS NEW.clock_domain_id)
 OR EXISTS(SELECT 1 FROM automation_recovery_dispatch_protection WHERE execution_id=NEW.execution_id)
BEGIN SELECT RAISE(ABORT,'execution_submission_attempt_binding'); END;
CREATE TRIGGER automation_recovery_dispatch_submission_attempt_no_update BEFORE UPDATE ON automation_recovery_dispatch_submission_attempt
BEGIN SELECT RAISE(ABORT,'immutable_execution_submission_attempt'); END;
CREATE TRIGGER automation_recovery_dispatch_submission_attempt_no_delete BEFORE DELETE ON automation_recovery_dispatch_submission_attempt
BEGIN SELECT RAISE(ABORT,'immutable_execution_submission_attempt'); END;

CREATE TABLE automation_recovery_dispatch_submission_uncertain (
 execution_id TEXT NOT NULL, step_id TEXT NOT NULL, PRIMARY KEY(execution_id,step_id),
 FOREIGN KEY(execution_id,step_id) REFERENCES automation_recovery_dispatch_submission_attempt(execution_id,step_id)
) STRICT;
CREATE TRIGGER automation_recovery_dispatch_submission_uncertain_no_update BEFORE UPDATE ON automation_recovery_dispatch_submission_uncertain
BEGIN SELECT RAISE(ABORT,'immutable_execution_submission_uncertain'); END;
CREATE TRIGGER automation_recovery_dispatch_submission_uncertain_no_delete BEFORE DELETE ON automation_recovery_dispatch_submission_uncertain
BEGIN SELECT RAISE(ABORT,'immutable_execution_submission_uncertain'); END;

CREATE TABLE automation_recovery_dispatch_submission_adjudication (
 execution_id TEXT NOT NULL, step_id TEXT NOT NULL,
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=8192),
 body_digest TEXT NOT NULL CHECK(length(body_digest)=71), PRIMARY KEY(execution_id,step_id),
 FOREIGN KEY(execution_id,step_id) REFERENCES automation_recovery_dispatch_submission_attempt(execution_id,step_id)
) STRICT;
CREATE TRIGGER automation_recovery_dispatch_submission_adjudication_no_update BEFORE UPDATE ON automation_recovery_dispatch_submission_adjudication
BEGIN SELECT RAISE(ABORT,'immutable_execution_submission_adjudication'); END;
CREATE TRIGGER automation_recovery_dispatch_submission_adjudication_no_delete BEFORE DELETE ON automation_recovery_dispatch_submission_adjudication
BEGIN SELECT RAISE(ABORT,'immutable_execution_submission_adjudication'); END;


CREATE TABLE automation_recovery_dispatch_command_terminal (
 execution_id TEXT NOT NULL, step_id TEXT NOT NULL,
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=8192),
 body_digest TEXT NOT NULL CHECK(length(body_digest)=71),
 PRIMARY KEY(execution_id,step_id),
 FOREIGN KEY(execution_id,step_id) REFERENCES automation_recovery_dispatch_submission_adjudication(execution_id,step_id)
) STRICT;
CREATE TRIGGER automation_recovery_dispatch_command_terminal_binding BEFORE INSERT ON automation_recovery_dispatch_command_terminal
WHEN NOT EXISTS(SELECT 1 FROM automation_recovery_dispatch_submission_adjudication a
 WHERE a.execution_id=NEW.execution_id AND a.step_id=NEW.step_id
 AND json_extract(a.body_jcs,'$.decision')='accepted'
 AND json_extract(NEW.body_jcs,'$.command_ref') IS json_extract(a.body_jcs,'$.command_ref'))
BEGIN SELECT RAISE(ABORT,'execution_command_terminal_binding'); END;
CREATE TRIGGER automation_recovery_dispatch_command_terminal_no_update BEFORE UPDATE ON automation_recovery_dispatch_command_terminal
BEGIN SELECT RAISE(ABORT,'immutable_execution_command_terminal'); END;
CREATE TRIGGER automation_recovery_dispatch_command_terminal_no_delete BEFORE DELETE ON automation_recovery_dispatch_command_terminal
BEGIN SELECT RAISE(ABORT,'immutable_execution_command_terminal'); END;
