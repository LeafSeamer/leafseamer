-- 显式停机迁移，追加独立回执。原撤销请求和待交付通知保持原样。
CREATE TABLE service_state_v9 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='9'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v9 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'9',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v9 RENAME TO service_state;
CREATE TRIGGER service_state_no_update BEFORE UPDATE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;
CREATE TRIGGER service_state_no_delete BEFORE DELETE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;

CREATE TABLE schema_migration_v9 (
 migration_id TEXT PRIMARY KEY NOT NULL, from_revision TEXT NOT NULL, to_revision TEXT NOT NULL UNIQUE,
 script_digest TEXT NOT NULL, state TEXT NOT NULL CHECK(state='committed'),
 CHECK((from_revision='0' AND to_revision='1') OR (from_revision='1' AND to_revision='2') OR (from_revision='2' AND to_revision='3') OR
  (from_revision='3' AND to_revision='4') OR (from_revision='4' AND to_revision='5') OR (from_revision='5' AND to_revision='6') OR
  (from_revision='6' AND to_revision='7') OR (from_revision='7' AND to_revision='8') OR (from_revision='8' AND to_revision='9'))
) STRICT;
INSERT INTO schema_migration_v9 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v9 RENAME TO schema_migration;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;

CREATE TABLE automation_run_approval_protection_receipt (
 operation_id TEXT NOT NULL,target_service_id TEXT NOT NULL,target_database_instance_id TEXT NOT NULL,
 receipt_id TEXT NOT NULL,body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=16384),body_digest TEXT NOT NULL,
 PRIMARY KEY(operation_id,target_service_id,target_database_instance_id),
 UNIQUE(target_service_id,target_database_instance_id,receipt_id),
 FOREIGN KEY(operation_id,target_service_id,target_database_instance_id)
  REFERENCES automation_run_approval_protection_outbox(operation_id,target_service_id,target_database_instance_id)
) STRICT;
CREATE TRIGGER automation_run_approval_protection_receipt_no_update BEFORE UPDATE ON automation_run_approval_protection_receipt
BEGIN SELECT RAISE(ABORT,'immutable_run_approval_protection_receipt'); END;
CREATE TRIGGER automation_run_approval_protection_receipt_no_delete BEFORE DELETE ON automation_run_approval_protection_receipt
BEGIN SELECT RAISE(ABORT,'immutable_run_approval_protection_receipt'); END;
