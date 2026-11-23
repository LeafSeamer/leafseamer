-- 为新摄取封存完整触发目标。历史接收原件保持原样。
DROP TRIGGER first_pairing_import_target;
CREATE TABLE service_state_v23 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='23'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v23 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'23',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v23 RENAME TO service_state;
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

CREATE TABLE schema_migration_v23 (
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
  (from_revision='22' AND to_revision='23'))
) STRICT;
INSERT INTO schema_migration_v23 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v23 RENAME TO schema_migration;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;

CREATE TABLE automation_trigger_binding (
 definition_id TEXT NOT NULL, definition_version TEXT NOT NULL, binding_id TEXT NOT NULL,
 authority_id TEXT NOT NULL, registration_id TEXT NOT NULL, registration_revision TEXT NOT NULL,
 registration_digest TEXT NOT NULL CHECK(length(registration_digest)=71),
 PRIMARY KEY(definition_id,definition_version,binding_id),
 FOREIGN KEY(definition_id,definition_version) REFERENCES definition_program_dependencies(definition_id,definition_version)
) STRICT;
CREATE INDEX automation_trigger_binding_source ON automation_trigger_binding(authority_id,registration_id,registration_revision,registration_digest);
CREATE VIEW automation_trigger_binding_original AS
 SELECT d.definition_id,d.definition_version,json_extract(i.value,'$.binding_id') AS binding_id,
  json_extract(i.value,'$.authority_id') AS authority_id,json_extract(i.value,'$.registration_ref.id') AS registration_id,
  json_extract(i.value,'$.registration_ref.revision') AS registration_revision,json_extract(i.value,'$.registration_ref.digest') AS registration_digest
 FROM definition_program_dependencies d,json_each(d.body_jcs,'$.dependencies.inputs') i
 WHERE json_extract(i.value,'$.source')='event' AND EXISTS(SELECT 1 FROM json_each(i.value,'$.uses') u WHERE u.value='trigger');
INSERT INTO automation_trigger_binding SELECT * FROM automation_trigger_binding_original;
CREATE TRIGGER automation_trigger_bindings_index AFTER INSERT ON definition_program_dependencies BEGIN
 INSERT INTO automation_trigger_binding SELECT * FROM automation_trigger_binding_original
  WHERE definition_id=NEW.definition_id AND definition_version=NEW.definition_version;
 SELECT CASE WHEN changes()<>(SELECT count(*) FROM automation_trigger_binding_original
  WHERE definition_id=NEW.definition_id AND definition_version=NEW.definition_version)
 THEN RAISE(ABORT,'trigger_binding_index_incomplete') END;
END;
CREATE TRIGGER automation_trigger_binding_origin BEFORE INSERT ON automation_trigger_binding
WHEN NOT EXISTS(SELECT 1 FROM automation_trigger_binding_original b
 WHERE b.definition_id=NEW.definition_id AND b.definition_version=NEW.definition_version AND b.binding_id=NEW.binding_id
  AND b.authority_id=NEW.authority_id AND b.registration_id=NEW.registration_id AND b.registration_revision=NEW.registration_revision
  AND b.registration_digest=NEW.registration_digest)
BEGIN SELECT RAISE(ABORT,'trigger_binding_origin'); END;
CREATE TRIGGER automation_trigger_binding_no_update BEFORE UPDATE ON automation_trigger_binding BEGIN SELECT RAISE(ABORT,'immutable_trigger_binding'); END;
CREATE TRIGGER automation_trigger_binding_no_delete BEFORE DELETE ON automation_trigger_binding BEGIN SELECT RAISE(ABORT,'immutable_trigger_binding'); END;

CREATE TABLE automation_trigger_match_set (
 ingest_epoch TEXT NOT NULL, receive_sequence INTEGER NOT NULL, origin_digest TEXT NOT NULL CHECK(length(origin_digest)=71),
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=1048576),
 body_digest TEXT NOT NULL CHECK(length(body_digest)=71),
 PRIMARY KEY(ingest_epoch,receive_sequence),
 FOREIGN KEY(ingest_epoch,receive_sequence) REFERENCES subscription_reconcile_inbox(ingest_epoch,receive_sequence),
 CHECK(json_extract(body_jcs,'$.origin_digest') IS origin_digest
  AND json_extract(body_jcs,'$.receive_cursor.ingest_epoch') IS ingest_epoch
  AND json_extract(body_jcs,'$.receive_cursor.receive_sequence') IS CAST(receive_sequence AS TEXT))
) STRICT;
CREATE TRIGGER automation_trigger_match_origin BEFORE INSERT ON automation_trigger_match_set
WHEN NOT EXISTS(SELECT 1 FROM subscription_reconcile_inbox i
 JOIN event_ingest_epoch e ON e.ingest_epoch=i.ingest_epoch
 WHERE i.ingest_epoch=NEW.ingest_epoch AND i.receive_sequence=NEW.receive_sequence
  AND json_extract(NEW.body_jcs,'$.receive_cursor.scope.authority_service_id') IS e.service_id
  AND json_extract(NEW.body_jcs,'$.receive_cursor.scope.database_instance_id') IS e.database_instance_id
  AND i.event_id=json_extract(NEW.body_jcs,'$.event_id') AND json_extract(i.body_jcs,'$.payload.origin_digest')=NEW.origin_digest)
BEGIN SELECT RAISE(ABORT,'trigger_match_origin'); END;
CREATE TRIGGER automation_trigger_match_no_update BEFORE UPDATE ON automation_trigger_match_set BEGIN SELECT RAISE(ABORT,'immutable_trigger_match'); END;
CREATE TRIGGER automation_trigger_match_no_delete BEFORE DELETE ON automation_trigger_match_set BEGIN SELECT RAISE(ABORT,'immutable_trigger_match'); END;
