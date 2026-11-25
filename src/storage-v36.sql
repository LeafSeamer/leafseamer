-- 不可变排期版本、激活及稳定逻辑场次。
DROP TRIGGER first_pairing_import_target;
CREATE TABLE service_state_v36 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='36'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v36 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'36',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v36 RENAME TO service_state;
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

CREATE TABLE schema_migration_v36 (
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
  (from_revision='33' AND to_revision='34') OR (from_revision='34' AND to_revision='35') OR (from_revision='35' AND to_revision='36'))
) STRICT;
INSERT INTO schema_migration_v36 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v36 RENAME TO schema_migration;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;

CREATE TABLE schedule_identity (
 schedule_id TEXT PRIMARY KEY NOT NULL,
 definition_id TEXT NOT NULL REFERENCES definition_head(definition_id)
) STRICT;
CREATE TABLE schedule_occurrence (
 schedule_id TEXT NOT NULL REFERENCES schedule_identity(schedule_id),
 item_id TEXT NOT NULL,
 one_shot_key TEXT NOT NULL,
 occurrence_id TEXT NOT NULL UNIQUE,
 logical_key_digest TEXT NOT NULL UNIQUE,
 PRIMARY KEY(schedule_id,item_id)
) STRICT;
CREATE TABLE schedule_version (
 schedule_id TEXT NOT NULL REFERENCES schedule_identity(schedule_id),
 version TEXT NOT NULL CHECK(length(version)=20 AND version NOT GLOB '*[^0-9]*'),
 definition_id TEXT NOT NULL,
 definition_version TEXT NOT NULL,
 body_jcs TEXT NOT NULL CHECK(length(CAST(body_jcs AS BLOB)) BETWEEN 1 AND 1048576),
 body_digest TEXT NOT NULL CHECK(length(body_digest)=71),
 PRIMARY KEY(schedule_id,version),
 FOREIGN KEY(definition_id,definition_version) REFERENCES definition_publication(definition_id,definition_version)
) STRICT;
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

CREATE TABLE schedule_activation (
 schedule_id TEXT NOT NULL,
 revision TEXT NOT NULL CHECK(length(revision)=20 AND revision NOT GLOB '*[^0-9]*'),
 schedule_version TEXT NOT NULL,
 body_jcs TEXT NOT NULL CHECK(length(CAST(body_jcs AS BLOB)) BETWEEN 1 AND 16384),
 body_digest TEXT NOT NULL CHECK(length(body_digest)=71),
 PRIMARY KEY(schedule_id,revision),
 FOREIGN KEY(schedule_id,schedule_version) REFERENCES schedule_version(schedule_id,version)
) STRICT;
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

CREATE TABLE schedule_request (
 request_key TEXT PRIMARY KEY NOT NULL,
 context_id TEXT NOT NULL REFERENCES local_request_context(context_id),
 operation TEXT NOT NULL CHECK(operation IN ('publishSchedule','activateSchedule')),
 request_digest TEXT NOT NULL CHECK(length(request_digest)=71),
 schedule_id TEXT NOT NULL REFERENCES schedule_identity(schedule_id),
 result_revision TEXT NOT NULL,
 result_digest TEXT NOT NULL CHECK(length(result_digest)=71)
) STRICT;
CREATE INDEX schedule_request_context ON schedule_request(context_id);
CREATE TRIGGER schedule_request_parent BEFORE INSERT ON schedule_request
WHEN NOT EXISTS (
 SELECT 1 FROM local_request_context c,json_each(c.body_jcs,'$.context.operations') o
 WHERE c.context_id=NEW.context_id AND o.value=NEW.operation
) OR NOT (
 (NEW.operation='publishSchedule' AND EXISTS(SELECT 1 FROM schedule_version v WHERE v.schedule_id=NEW.schedule_id
   AND v.version=NEW.result_revision AND v.body_digest=NEW.result_digest))
 OR (NEW.operation='activateSchedule' AND EXISTS(SELECT 1 FROM schedule_activation a WHERE a.schedule_id=NEW.schedule_id
   AND a.revision=NEW.result_revision AND a.body_digest=NEW.result_digest))
)
BEGIN SELECT RAISE(ABORT,'schedule_request_parent'); END;

CREATE TRIGGER schedule_identity_no_update BEFORE UPDATE ON schedule_identity
BEGIN SELECT RAISE(ABORT,'immutable_schedule_identity'); END;
CREATE TRIGGER schedule_identity_no_delete BEFORE DELETE ON schedule_identity
BEGIN SELECT RAISE(ABORT,'immutable_schedule_identity'); END;
CREATE TRIGGER schedule_occurrence_no_update BEFORE UPDATE ON schedule_occurrence
BEGIN SELECT RAISE(ABORT,'immutable_schedule_occurrence'); END;
CREATE TRIGGER schedule_occurrence_no_delete BEFORE DELETE ON schedule_occurrence
BEGIN SELECT RAISE(ABORT,'immutable_schedule_occurrence'); END;
CREATE TRIGGER schedule_version_no_update BEFORE UPDATE ON schedule_version
BEGIN SELECT RAISE(ABORT,'immutable_schedule_version'); END;
CREATE TRIGGER schedule_version_no_delete BEFORE DELETE ON schedule_version
BEGIN SELECT RAISE(ABORT,'immutable_schedule_version'); END;
CREATE TRIGGER schedule_activation_no_update BEFORE UPDATE ON schedule_activation
BEGIN SELECT RAISE(ABORT,'immutable_schedule_activation'); END;
CREATE TRIGGER schedule_activation_no_delete BEFORE DELETE ON schedule_activation
BEGIN SELECT RAISE(ABORT,'immutable_schedule_activation'); END;
CREATE TRIGGER schedule_request_no_update BEFORE UPDATE ON schedule_request
BEGIN SELECT RAISE(ABORT,'immutable_schedule_request'); END;
CREATE TRIGGER schedule_request_no_delete BEFORE DELETE ON schedule_request
BEGIN SELECT RAISE(ABORT,'immutable_schedule_request'); END;
