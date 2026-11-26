-- 显式停机迁移。恢复执行与全目标准备回执的绑定。
DROP TRIGGER first_pairing_import_target;
DROP TRIGGER schedule_version_parent;
DROP TRIGGER schedule_activation_parent;
DROP TRIGGER automation_recovery_preview_binding;
CREATE TABLE service_state_v43 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='43'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v43 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'43',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v43 RENAME TO service_state;
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

CREATE TABLE schema_migration_v43 (
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
  (from_revision='33' AND to_revision='34') OR (from_revision='34' AND to_revision='35') OR (from_revision='35' AND to_revision='36') OR (from_revision='36' AND to_revision='37') OR (from_revision='37' AND to_revision='38') OR (from_revision='38' AND to_revision='39') OR (from_revision='39' AND to_revision='40') OR (from_revision='40' AND to_revision='41') OR (from_revision='41' AND to_revision='42') OR (from_revision='42' AND to_revision='43'))
) STRICT;
INSERT INTO schema_migration_v43 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v43 RENAME TO schema_migration;
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

CREATE TABLE automation_recovery_execution_binding (
 operation_id TEXT PRIMARY KEY NOT NULL REFERENCES automation_recovery_preparation(operation_id),
 context_id TEXT NOT NULL REFERENCES local_request_context(context_id),
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs)), body_digest TEXT NOT NULL
) STRICT;
CREATE INDEX automation_recovery_execution_binding_context ON automation_recovery_execution_binding(context_id);
CREATE TRIGGER recovery_execution_binding_targets BEFORE INSERT ON automation_recovery_execution_binding
WHEN NOT EXISTS (
 SELECT 1 FROM automation_recovery_operation o
 JOIN automation_recovery_execution e USING(operation_id)
 JOIN automation_recovery_lock l USING(operation_id)
 JOIN automation_recovery_preparation p USING(operation_id)
 WHERE o.operation_id=NEW.operation_id AND l.released_by IS NULL AND
 json_extract(NEW.body_jcs,'$.operation_ref.digest') IS o.body_digest AND
 json_extract(NEW.body_jcs,'$.execution_ref.object_id') IS e.execution_id AND
 json_extract(NEW.body_jcs,'$.execution_ref.digest') IS e.body_digest AND
 json_extract(NEW.body_jcs,'$.registration_digest') IS p.body_digest AND
 json_type(NEW.body_jcs,'$.dispatch_eligible')='false'
) OR NOT EXISTS (
 SELECT 1 FROM automation_recovery_preparation_target t WHERE t.operation_id=NEW.operation_id
) OR EXISTS (
 SELECT 1 FROM automation_recovery_preparation_target t
 LEFT JOIN automation_recovery_preparation_receipt r USING(preparation_id)
 WHERE t.operation_id=NEW.operation_id AND r.preparation_id IS NULL
)
BEGIN SELECT RAISE(ABORT,'recovery_execution_binding_targets'); END;
CREATE TRIGGER recovery_execution_binding_no_update BEFORE UPDATE ON automation_recovery_execution_binding
BEGIN SELECT RAISE(ABORT,'immutable_recovery_execution_binding'); END;
CREATE TRIGGER recovery_execution_binding_no_delete BEFORE DELETE ON automation_recovery_execution_binding
BEGIN SELECT RAISE(ABORT,'immutable_recovery_execution_binding'); END;
