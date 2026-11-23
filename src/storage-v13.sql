-- 显式停机迁移。既有运行保持原修订，预检原件只由新的观察产生。
CREATE TABLE service_state_v13 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='13'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v13 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'13',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v13 RENAME TO service_state;
CREATE TRIGGER service_state_no_update BEFORE UPDATE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;
CREATE TRIGGER service_state_no_delete BEFORE DELETE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;

CREATE TABLE schema_migration_v13 (
 migration_id TEXT PRIMARY KEY NOT NULL, from_revision TEXT NOT NULL, to_revision TEXT NOT NULL UNIQUE,
 script_digest TEXT NOT NULL, state TEXT NOT NULL CHECK(state='committed'),
 CHECK((from_revision='0' AND to_revision='1') OR (from_revision='1' AND to_revision='2') OR (from_revision='2' AND to_revision='3') OR
  (from_revision='3' AND to_revision='4') OR (from_revision='4' AND to_revision='5') OR (from_revision='5' AND to_revision='6') OR
  (from_revision='6' AND to_revision='7') OR (from_revision='7' AND to_revision='8') OR (from_revision='8' AND to_revision='9') OR
  (from_revision='9' AND to_revision='10') OR (from_revision='10' AND to_revision='11') OR (from_revision='11' AND to_revision='12') OR
  (from_revision='12' AND to_revision='13'))
) STRICT;
INSERT INTO schema_migration_v13 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v13 RENAME TO schema_migration;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;

CREATE TABLE automation_run_preflight (
 preflight_id TEXT PRIMARY KEY NOT NULL, run_id TEXT NOT NULL REFERENCES automation_run_creation(run_id),
 service_boot_id TEXT NOT NULL REFERENCES service_boot(service_boot_id),
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=1048576), body_digest TEXT NOT NULL,
 CHECK(json_extract(body_jcs,'$.preflight_id') IS preflight_id AND json_extract(body_jcs,'$.observation.run_id') IS run_id AND
  json_extract(body_jcs,'$.window.service_boot_id') IS service_boot_id AND json_extract(body_jcs,'$.format') IS 'leafseamer.run-arm-preflight/1')
) STRICT;
CREATE INDEX automation_run_preflight_run ON automation_run_preflight(run_id);
CREATE TRIGGER automation_run_preflight_no_update BEFORE UPDATE ON automation_run_preflight
BEGIN SELECT RAISE(ABORT,'immutable_run_preflight'); END;
CREATE TRIGGER automation_run_preflight_no_delete BEFORE DELETE ON automation_run_preflight
BEGIN SELECT RAISE(ABORT,'immutable_run_preflight'); END;
