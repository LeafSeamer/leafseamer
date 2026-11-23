-- 显式停机迁移。准备回执与原目标、审计及通知共同提交。
CREATE TABLE service_state_v15 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='15'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v15 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'15',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v15 RENAME TO service_state;
CREATE TRIGGER service_state_no_update BEFORE UPDATE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;
CREATE TRIGGER service_state_no_delete BEFORE DELETE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;

CREATE TABLE schema_migration_v15 (
 migration_id TEXT PRIMARY KEY NOT NULL, from_revision TEXT NOT NULL, to_revision TEXT NOT NULL UNIQUE,
 script_digest TEXT NOT NULL, state TEXT NOT NULL CHECK(state='committed'),
 CHECK((from_revision='0' AND to_revision='1') OR (from_revision='1' AND to_revision='2') OR
  (from_revision='2' AND to_revision='3') OR (from_revision='3' AND to_revision='4') OR
  (from_revision='4' AND to_revision='5') OR (from_revision='5' AND to_revision='6') OR
  (from_revision='6' AND to_revision='7') OR (from_revision='7' AND to_revision='8') OR
  (from_revision='8' AND to_revision='9') OR (from_revision='9' AND to_revision='10') OR
  (from_revision='10' AND to_revision='11') OR (from_revision='11' AND to_revision='12') OR
  (from_revision='12' AND to_revision='13') OR (from_revision='13' AND to_revision='14') OR (from_revision='14' AND to_revision='15'))
) STRICT;
INSERT INTO schema_migration_v15 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v15 RENAME TO schema_migration;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;

CREATE TABLE automation_arm_preparation_receipt (
 preparation_id TEXT PRIMARY KEY NOT NULL REFERENCES automation_arm_target(preparation_id),
 operation_id TEXT NOT NULL,
 controller_service_id TEXT NOT NULL,
 controller_database_instance_id TEXT NOT NULL,
 grant_id TEXT NOT NULL,
 receipt_id TEXT NOT NULL,
 request_digest TEXT NOT NULL,
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=4194304), body_digest TEXT NOT NULL,
 audit_jcs TEXT NOT NULL CHECK(json_valid(audit_jcs) AND length(CAST(audit_jcs AS BLOB))<=16384), audit_digest TEXT NOT NULL,
 UNIQUE(controller_service_id,controller_database_instance_id,grant_id),
 UNIQUE(controller_service_id,controller_database_instance_id,receipt_id),
 FOREIGN KEY(preparation_id) REFERENCES automation_arm_preparation_notification(preparation_id) DEFERRABLE INITIALLY DEFERRED
) STRICT;
CREATE TRIGGER automation_arm_preparation_target BEFORE INSERT ON automation_arm_preparation_receipt WHEN NOT EXISTS (
 SELECT 1 FROM automation_arm_target t WHERE t.preparation_id=NEW.preparation_id AND t.operation_id=NEW.operation_id AND
  t.controller_service_id=NEW.controller_service_id AND t.controller_database_instance_id=NEW.controller_database_instance_id)
BEGIN SELECT RAISE(ABORT,'preparation_receipt_target'); END;
CREATE TRIGGER automation_arm_preparation_receipt_no_update BEFORE UPDATE ON automation_arm_preparation_receipt BEGIN SELECT RAISE(ABORT,'immutable_arm_preparation_receipt'); END;
CREATE TRIGGER automation_arm_preparation_receipt_no_delete BEFORE DELETE ON automation_arm_preparation_receipt BEGIN SELECT RAISE(ABORT,'immutable_arm_preparation_receipt'); END;

CREATE TABLE automation_arm_preparation_notification (
 preparation_id TEXT PRIMARY KEY NOT NULL REFERENCES automation_arm_preparation_receipt(preparation_id),
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=16384), body_digest TEXT NOT NULL
) STRICT;
CREATE TRIGGER automation_arm_preparation_notification_no_update BEFORE UPDATE ON automation_arm_preparation_notification BEGIN SELECT RAISE(ABORT,'immutable_arm_preparation_notification'); END;
CREATE TRIGGER automation_arm_preparation_notification_no_delete BEFORE DELETE ON automation_arm_preparation_notification BEGIN SELECT RAISE(ABORT,'immutable_arm_preparation_notification'); END;
