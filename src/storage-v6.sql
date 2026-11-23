-- 显式停机迁移。保留 Schema 5 已提交的运行、接收游标和迁移原件。
CREATE TABLE service_state_v6 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='6'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v6 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'6',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v6 RENAME TO service_state;
CREATE TRIGGER service_state_no_update BEFORE UPDATE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;
CREATE TRIGGER service_state_no_delete BEFORE DELETE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;

CREATE TABLE schema_migration_v6 (
 migration_id TEXT PRIMARY KEY NOT NULL, from_revision TEXT NOT NULL, to_revision TEXT NOT NULL UNIQUE,
 script_digest TEXT NOT NULL, state TEXT NOT NULL CHECK(state='committed'),
 CHECK((from_revision='0' AND to_revision='1') OR (from_revision='1' AND to_revision='2') OR (from_revision='2' AND to_revision='3') OR
  (from_revision='3' AND to_revision='4') OR (from_revision='4' AND to_revision='5') OR (from_revision='5' AND to_revision='6'))
) STRICT;
INSERT INTO schema_migration_v6 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v6 RENAME TO schema_migration;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;

CREATE TABLE automation_run_approval_proposal (
 proposal_id TEXT PRIMARY KEY NOT NULL,
 run_id TEXT NOT NULL REFERENCES automation_run_creation(run_id),
 context_id TEXT NOT NULL REFERENCES local_request_context(context_id),
 identity_digest TEXT NOT NULL UNIQUE, request_digest TEXT NOT NULL,
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=1048576),
 body_digest TEXT NOT NULL,
 audit_jcs TEXT NOT NULL CHECK(json_valid(audit_jcs) AND length(CAST(audit_jcs AS BLOB))<=32768),
 audit_digest TEXT NOT NULL
) STRICT;
CREATE INDEX automation_run_approval_proposal_context ON automation_run_approval_proposal(context_id);
CREATE TRIGGER automation_run_approval_proposal_no_update BEFORE UPDATE ON automation_run_approval_proposal
BEGIN SELECT RAISE(ABORT,'immutable_run_approval_proposal'); END;
CREATE TRIGGER automation_run_approval_proposal_no_delete BEFORE DELETE ON automation_run_approval_proposal
BEGIN SELECT RAISE(ABORT,'immutable_run_approval_proposal'); END;
