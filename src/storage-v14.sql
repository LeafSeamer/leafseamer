-- 显式停机迁移。记录普通首次布防的槽、请求和准备身份。
CREATE TABLE service_state_v14 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='14'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v14 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'14',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v14 RENAME TO service_state;
CREATE TRIGGER service_state_no_update BEFORE UPDATE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;
CREATE TRIGGER service_state_no_delete BEFORE DELETE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;

CREATE TABLE schema_migration_v14 (
 migration_id TEXT PRIMARY KEY NOT NULL, from_revision TEXT NOT NULL, to_revision TEXT NOT NULL UNIQUE,
 script_digest TEXT NOT NULL, state TEXT NOT NULL CHECK(state='committed'),
 CHECK((from_revision='0' AND to_revision='1') OR (from_revision='1' AND to_revision='2') OR
  (from_revision='2' AND to_revision='3') OR (from_revision='3' AND to_revision='4') OR
  (from_revision='4' AND to_revision='5') OR (from_revision='5' AND to_revision='6') OR
  (from_revision='6' AND to_revision='7') OR (from_revision='7' AND to_revision='8') OR
  (from_revision='8' AND to_revision='9') OR (from_revision='9' AND to_revision='10') OR
  (from_revision='10' AND to_revision='11') OR (from_revision='11' AND to_revision='12') OR
  (from_revision='12' AND to_revision='13') OR (from_revision='13' AND to_revision='14'))
) STRICT;
INSERT INTO schema_migration_v14 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v14 RENAME TO schema_migration;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;

CREATE TABLE automation_run_arm (
 operation_id TEXT PRIMARY KEY NOT NULL, run_id TEXT NOT NULL UNIQUE REFERENCES automation_run_creation(run_id),
 definition_id TEXT NOT NULL REFERENCES definition_identity(definition_id),
 preflight_id TEXT NOT NULL UNIQUE REFERENCES automation_run_preflight(preflight_id),
 context_id TEXT NOT NULL REFERENCES local_request_context(context_id), request_identity TEXT NOT NULL UNIQUE, request_digest TEXT NOT NULL,
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=1048576), body_digest TEXT NOT NULL,
 UNIQUE(operation_id,definition_id,run_id),
 FOREIGN KEY(definition_id,run_id,operation_id) REFERENCES automation_activation_slot(definition_id,run_id,operation_id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(operation_id) REFERENCES automation_arm_budget(operation_id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(operation_id) REFERENCES automation_arm_notification(operation_id) DEFERRABLE INITIALLY DEFERRED
) STRICT;
CREATE INDEX automation_run_arm_context ON automation_run_arm(context_id);
CREATE TABLE automation_activation_slot (
 definition_id TEXT PRIMARY KEY NOT NULL, run_id TEXT NOT NULL UNIQUE, operation_id TEXT NOT NULL UNIQUE, revision TEXT NOT NULL CHECK(revision='1'),
 UNIQUE(definition_id,run_id,operation_id),
 FOREIGN KEY(operation_id,definition_id,run_id) REFERENCES automation_run_arm(operation_id,definition_id,run_id) DEFERRABLE INITIALLY DEFERRED
) STRICT;
CREATE TABLE automation_arm_budget (
 operation_id TEXT PRIMARY KEY NOT NULL REFERENCES automation_run_arm(operation_id) DEFERRABLE INITIALLY DEFERRED,
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=65536), body_digest TEXT NOT NULL
) STRICT;
CREATE TABLE automation_arm_target (
 operation_id TEXT NOT NULL REFERENCES automation_run_arm(operation_id) DEFERRABLE INITIALLY DEFERRED,
 controller_service_id TEXT NOT NULL, controller_database_instance_id TEXT NOT NULL, preparation_id TEXT NOT NULL UNIQUE,
 PRIMARY KEY(operation_id,controller_service_id,controller_database_instance_id)
) STRICT;
CREATE TABLE automation_arm_notification (
 operation_id TEXT PRIMARY KEY NOT NULL REFERENCES automation_run_arm(operation_id) DEFERRABLE INITIALLY DEFERRED,
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs)), body_digest TEXT NOT NULL
) STRICT;
CREATE TRIGGER automation_run_arm_no_update BEFORE UPDATE ON automation_run_arm BEGIN SELECT RAISE(ABORT,'immutable_run_arm'); END;
CREATE TRIGGER automation_run_arm_no_delete BEFORE DELETE ON automation_run_arm BEGIN SELECT RAISE(ABORT,'immutable_run_arm'); END;
CREATE TRIGGER automation_activation_slot_no_update BEFORE UPDATE ON automation_activation_slot BEGIN SELECT RAISE(ABORT,'immutable_activation_slot'); END;
CREATE TRIGGER automation_activation_slot_no_delete BEFORE DELETE ON automation_activation_slot BEGIN SELECT RAISE(ABORT,'immutable_activation_slot'); END;
CREATE TRIGGER automation_arm_budget_no_update BEFORE UPDATE ON automation_arm_budget BEGIN SELECT RAISE(ABORT,'immutable_arm_budget'); END;
CREATE TRIGGER automation_arm_budget_no_delete BEFORE DELETE ON automation_arm_budget BEGIN SELECT RAISE(ABORT,'immutable_arm_budget'); END;
CREATE TRIGGER automation_arm_target_no_update BEFORE UPDATE ON automation_arm_target BEGIN SELECT RAISE(ABORT,'immutable_arm_target'); END;
CREATE TRIGGER automation_arm_target_no_delete BEFORE DELETE ON automation_arm_target BEGIN SELECT RAISE(ABORT,'immutable_arm_target'); END;
CREATE TRIGGER automation_arm_notification_no_update BEFORE UPDATE ON automation_arm_notification BEGIN SELECT RAISE(ABORT,'immutable_arm_notification'); END;
CREATE TRIGGER automation_arm_notification_no_delete BEFORE DELETE ON automation_arm_notification BEGIN SELECT RAISE(ABORT,'immutable_arm_notification'); END;
