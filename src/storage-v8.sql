-- 显式停机迁移，保留既有批准正文、状态和回执。
CREATE TABLE service_state_v8 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='8'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v8 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'8',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v8 RENAME TO service_state;
CREATE TRIGGER service_state_no_update BEFORE UPDATE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;
CREATE TRIGGER service_state_no_delete BEFORE DELETE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;

CREATE TABLE schema_migration_v8 (
 migration_id TEXT PRIMARY KEY NOT NULL, from_revision TEXT NOT NULL, to_revision TEXT NOT NULL UNIQUE,
 script_digest TEXT NOT NULL, state TEXT NOT NULL CHECK(state='committed'),
 CHECK((from_revision='0' AND to_revision='1') OR (from_revision='1' AND to_revision='2') OR (from_revision='2' AND to_revision='3') OR
  (from_revision='3' AND to_revision='4') OR (from_revision='4' AND to_revision='5') OR (from_revision='5' AND to_revision='6') OR
  (from_revision='6' AND to_revision='7') OR (from_revision='7' AND to_revision='8'))
) STRICT;
INSERT INTO schema_migration_v8 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v8 RENAME TO schema_migration;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;

CREATE TABLE automation_run_approval_state_v8 (
 approval_id TEXT PRIMARY KEY NOT NULL REFERENCES automation_run_approval(approval_id), status_revision INTEGER NOT NULL,
 state TEXT NOT NULL CHECK((status_revision=1 AND state='active') OR (status_revision=2 AND state IN ('expired','revoking','revoked')))
) STRICT;
INSERT INTO automation_run_approval_state_v8 SELECT * FROM automation_run_approval_state;
DROP TABLE automation_run_approval_state;
ALTER TABLE automation_run_approval_state_v8 RENAME TO automation_run_approval_state;
CREATE TRIGGER automation_run_approval_state_step BEFORE UPDATE ON automation_run_approval_state
WHEN NEW.approval_id<>OLD.approval_id OR OLD.state<>'active' OR NEW.state NOT IN ('expired','revoking','revoked') OR NEW.status_revision<>2
BEGIN SELECT RAISE(ABORT,'run_approval_state'); END;
CREATE TRIGGER automation_run_approval_state_no_delete BEFORE DELETE ON automation_run_approval_state
BEGIN SELECT RAISE(ABORT,'immutable_run_approval_state'); END;

CREATE TABLE automation_run_approval_revocation (
 operation_id TEXT PRIMARY KEY NOT NULL, approval_id TEXT NOT NULL UNIQUE REFERENCES automation_run_approval(approval_id),
 context_id TEXT NOT NULL REFERENCES local_request_context(context_id), identity_digest TEXT NOT NULL UNIQUE, request_digest TEXT NOT NULL,
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=1048576), body_digest TEXT NOT NULL,
 audit_jcs TEXT NOT NULL CHECK(json_valid(audit_jcs) AND length(CAST(audit_jcs AS BLOB))<=65536), audit_digest TEXT NOT NULL
) STRICT;
CREATE INDEX automation_run_approval_revocation_context ON automation_run_approval_revocation(context_id);
CREATE TABLE automation_run_approval_protection_outbox (
 operation_id TEXT NOT NULL REFERENCES automation_run_approval_revocation(operation_id),
 target_service_id TEXT NOT NULL,target_database_instance_id TEXT NOT NULL,
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs)),body_digest TEXT NOT NULL,
 PRIMARY KEY(operation_id,target_service_id,target_database_instance_id)
) STRICT;
CREATE TRIGGER automation_run_approval_revocation_no_update BEFORE UPDATE ON automation_run_approval_revocation
BEGIN SELECT RAISE(ABORT,'immutable_run_approval_revocation'); END;
CREATE TRIGGER automation_run_approval_revocation_no_delete BEFORE DELETE ON automation_run_approval_revocation
BEGIN SELECT RAISE(ABORT,'immutable_run_approval_revocation'); END;
CREATE TRIGGER automation_run_approval_protection_outbox_no_update BEFORE UPDATE ON automation_run_approval_protection_outbox
BEGIN SELECT RAISE(ABORT,'immutable_run_approval_protection_outbox'); END;
CREATE TRIGGER automation_run_approval_protection_outbox_no_delete BEFORE DELETE ON automation_run_approval_protection_outbox
BEGIN SELECT RAISE(ABORT,'immutable_run_approval_protection_outbox'); END;
