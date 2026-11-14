-- 调用方须在事务外关闭外键，再在同一排他写事务内执行并核验所有外键。
CREATE TABLE service_state_v3 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE,
 database_instance_id TEXT NOT NULL UNIQUE,
 request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='3'),
 maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'),
 UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v3 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'3',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v3 RENAME TO service_state;
CREATE TRIGGER service_state_no_update BEFORE UPDATE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;
CREATE TRIGGER service_state_no_delete BEFORE DELETE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;

CREATE TABLE schema_migration_v3 (
 migration_id TEXT PRIMARY KEY NOT NULL,
 from_revision TEXT NOT NULL,
 to_revision TEXT NOT NULL UNIQUE,
 script_digest TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state='committed'),
 CHECK((from_revision='0' AND to_revision='1') OR (from_revision='1' AND to_revision='2') OR (from_revision='2' AND to_revision='3'))
) STRICT;
INSERT INTO schema_migration_v3 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v3 RENAME TO schema_migration;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;

CREATE TABLE binding_approval_revision (
 approval_id TEXT NOT NULL,
 revision INTEGER NOT NULL CHECK(revision IN (1,2)),
 definition_id TEXT NOT NULL REFERENCES definition_identity(definition_id),
 state TEXT NOT NULL CHECK((revision=1 AND state='approved') OR (revision=2 AND state='revoked')),
 body_jcs TEXT NOT NULL CHECK(length(CAST(body_jcs AS BLOB))<=524288),
 body_digest TEXT NOT NULL,
 transaction_id TEXT NOT NULL UNIQUE,
 PRIMARY KEY(approval_id,revision),
 FOREIGN KEY(transaction_id) REFERENCES binding_approval_operation(transaction_id) DEFERRABLE INITIALLY DEFERRED
) STRICT;
CREATE TABLE binding_approval_head (
 approval_id TEXT PRIMARY KEY NOT NULL,
 revision INTEGER NOT NULL,
 FOREIGN KEY(approval_id,revision) REFERENCES binding_approval_revision(approval_id,revision)
) STRICT;
CREATE TABLE binding_approval_operation (
 transaction_id TEXT PRIMARY KEY NOT NULL,
 actor_digest TEXT NOT NULL,
 request_id TEXT NOT NULL,
 request_digest TEXT NOT NULL,
 approval_id TEXT NOT NULL,
 revision INTEGER NOT NULL,
 UNIQUE(actor_digest,request_id),
 UNIQUE(approval_id,revision),
 FOREIGN KEY(approval_id,revision) REFERENCES binding_approval_revision(approval_id,revision) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(transaction_id) REFERENCES binding_approval_outbox(transaction_id) DEFERRABLE INITIALLY DEFERRED
) STRICT;
CREATE TABLE binding_approval_outbox (
 transaction_id TEXT PRIMARY KEY NOT NULL REFERENCES binding_approval_operation(transaction_id) DEFERRABLE INITIALLY DEFERRED,
 body_jcs TEXT NOT NULL,
 body_digest TEXT NOT NULL
) STRICT;
CREATE TRIGGER binding_approval_head_insert BEFORE INSERT ON binding_approval_head WHEN NEW.revision<>1 BEGIN SELECT RAISE(ABORT,'binding_approval_head'); END;
CREATE TRIGGER binding_approval_head_step BEFORE UPDATE ON binding_approval_head WHEN NEW.approval_id<>OLD.approval_id OR NEW.revision<>OLD.revision+1 BEGIN SELECT RAISE(ABORT,'binding_approval_head'); END;
CREATE TRIGGER binding_approval_head_no_delete BEFORE DELETE ON binding_approval_head BEGIN SELECT RAISE(ABORT,'immutable_binding_approval'); END;
CREATE TRIGGER binding_approval_revision_no_update BEFORE UPDATE ON binding_approval_revision BEGIN SELECT RAISE(ABORT,'immutable_binding_approval'); END;
CREATE TRIGGER binding_approval_revision_no_delete BEFORE DELETE ON binding_approval_revision BEGIN SELECT RAISE(ABORT,'immutable_binding_approval'); END;
CREATE TRIGGER binding_approval_operation_no_update BEFORE UPDATE ON binding_approval_operation BEGIN SELECT RAISE(ABORT,'immutable_binding_approval'); END;
CREATE TRIGGER binding_approval_operation_no_delete BEFORE DELETE ON binding_approval_operation BEGIN SELECT RAISE(ABORT,'immutable_binding_approval'); END;
CREATE TRIGGER binding_approval_outbox_no_update BEFORE UPDATE ON binding_approval_outbox BEGIN SELECT RAISE(ABORT,'immutable_binding_approval'); END;
CREATE TRIGGER binding_approval_outbox_no_delete BEFORE DELETE ON binding_approval_outbox BEGIN SELECT RAISE(ABORT,'immutable_binding_approval'); END;
