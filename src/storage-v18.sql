-- 显式扩展首次配对导入原件；保留业务库身份、全部旧行和迁移历史。
CREATE TABLE service_state_v18 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='18'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v18 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'18',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v18 RENAME TO service_state;
CREATE TRIGGER service_state_no_update BEFORE UPDATE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;
CREATE TRIGGER service_state_no_delete BEFORE DELETE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;

CREATE TABLE schema_migration_v18 (
 migration_id TEXT PRIMARY KEY NOT NULL, from_revision TEXT NOT NULL, to_revision TEXT NOT NULL UNIQUE,
 script_digest TEXT NOT NULL, state TEXT NOT NULL CHECK(state='committed'),
 CHECK((from_revision='0' AND to_revision='1') OR (from_revision='1' AND to_revision='2') OR
  (from_revision='2' AND to_revision='3') OR (from_revision='3' AND to_revision='4') OR
  (from_revision='4' AND to_revision='5') OR (from_revision='5' AND to_revision='6') OR
  (from_revision='6' AND to_revision='7') OR (from_revision='7' AND to_revision='8') OR
  (from_revision='8' AND to_revision='9') OR (from_revision='9' AND to_revision='10') OR
  (from_revision='10' AND to_revision='11') OR (from_revision='11' AND to_revision='12') OR
  (from_revision='12' AND to_revision='13') OR (from_revision='13' AND to_revision='14') OR
  (from_revision='14' AND to_revision='15') OR (from_revision='15' AND to_revision='16') OR (from_revision='16' AND to_revision='17') OR (from_revision='17' AND to_revision='18'))
) STRICT;
INSERT INTO schema_migration_v18 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v18 RENAME TO schema_migration;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;

CREATE TABLE first_pairing_import (
 authorization_id TEXT PRIMARY KEY NOT NULL,
 pair_attempt_id TEXT NOT NULL UNIQUE,
 target_binding_id TEXT NOT NULL UNIQUE,
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=32768),
 body_digest TEXT NOT NULL CHECK(length(body_digest)=71),
 controller_certificate BLOB NOT NULL CHECK(length(controller_certificate) BETWEEN 1 AND 65536),
 seamer_certificate BLOB NOT NULL CHECK(length(seamer_certificate) BETWEEN 1 AND 65536),
 CHECK(json_extract(body_jcs,'$.authorization.format') IS 'leafseamer.first-pairing-authorization/1'
  AND json_extract(body_jcs,'$.authorization.authorization_id') IS authorization_id
  AND json_extract(body_jcs,'$.authorization.transcript.pair_attempt_id') IS pair_attempt_id)
) STRICT;

CREATE TRIGGER first_pairing_import_target BEFORE INSERT ON first_pairing_import
WHEN EXISTS(SELECT 1 FROM peer_pairing_revision WHERE state='activated')
 OR EXISTS(SELECT 1 FROM local_permission_head)
 OR NOT EXISTS(SELECT 1 FROM service_state s,json_each(NEW.body_jcs,'$.authorization.transcript.parties') p
    WHERE json_extract(p.value,'$.service_kind')='seamer'
     AND json_extract(p.value,'$.service_id') IS s.service_id
     AND json_extract(p.value,'$.database_instance_id') IS s.database_instance_id
     AND json_extract(p.value,'$.binding_id') IS NEW.target_binding_id)
BEGIN SELECT RAISE(ABORT,'first_pairing_import_target'); END;
CREATE TRIGGER first_pairing_import_no_update BEFORE UPDATE ON first_pairing_import
BEGIN SELECT RAISE(ABORT,'immutable_first_pairing_import'); END;
CREATE TRIGGER first_pairing_import_no_delete BEFORE DELETE ON first_pairing_import
BEGIN SELECT RAISE(ABORT,'immutable_first_pairing_import'); END;
