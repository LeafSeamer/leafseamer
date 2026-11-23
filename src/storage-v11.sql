-- 显式停机迁移。事件绑定批准以独立的不可变账本保存。
CREATE TABLE service_state_v11 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='11'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v11 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'11',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v11 RENAME TO service_state;
CREATE TRIGGER service_state_no_update BEFORE UPDATE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;
CREATE TRIGGER service_state_no_delete BEFORE DELETE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;

CREATE TABLE schema_migration_v11 (
 migration_id TEXT PRIMARY KEY NOT NULL, from_revision TEXT NOT NULL, to_revision TEXT NOT NULL UNIQUE,
 script_digest TEXT NOT NULL, state TEXT NOT NULL CHECK(state='committed'),
 CHECK((from_revision='0' AND to_revision='1') OR (from_revision='1' AND to_revision='2') OR (from_revision='2' AND to_revision='3') OR
  (from_revision='3' AND to_revision='4') OR (from_revision='4' AND to_revision='5') OR (from_revision='5' AND to_revision='6') OR
  (from_revision='6' AND to_revision='7') OR (from_revision='7' AND to_revision='8') OR (from_revision='8' AND to_revision='9') OR
  (from_revision='9' AND to_revision='10') OR (from_revision='10' AND to_revision='11'))
) STRICT;
INSERT INTO schema_migration_v11 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v11 RENAME TO schema_migration;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;

CREATE TABLE event_binding_approval_revision (
 approval_id TEXT NOT NULL, revision INTEGER NOT NULL,
 definition_id TEXT NOT NULL REFERENCES definition_identity(definition_id), binding_id TEXT NOT NULL,
 context_id TEXT NOT NULL REFERENCES local_request_context(context_id), actor_digest TEXT NOT NULL,
 request_id TEXT NOT NULL, request_digest TEXT NOT NULL, state TEXT NOT NULL,
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=262144), body_digest TEXT NOT NULL,
 previous_revision INTEGER, previous_digest TEXT,
 PRIMARY KEY(approval_id,revision), UNIQUE(actor_digest,request_id),
 UNIQUE(approval_id,definition_id,binding_id,revision,body_digest),
 CHECK((revision=1 AND state='approved' AND previous_revision IS NULL AND previous_digest IS NULL) OR
  (revision=2 AND state='revoked' AND previous_revision IS 1 AND previous_digest IS NOT NULL)),
 FOREIGN KEY(approval_id,definition_id,binding_id,previous_revision,previous_digest)
  REFERENCES event_binding_approval_revision(approval_id,definition_id,binding_id,revision,body_digest)
) STRICT;
CREATE INDEX event_binding_approval_scope ON event_binding_approval_revision(definition_id,binding_id);
CREATE INDEX event_binding_approval_context ON event_binding_approval_revision(context_id);
CREATE TRIGGER event_binding_approval_no_update BEFORE UPDATE ON event_binding_approval_revision BEGIN SELECT RAISE(ABORT,'immutable_event_binding_approval'); END;
CREATE TRIGGER event_binding_approval_no_delete BEFORE DELETE ON event_binding_approval_revision BEGIN SELECT RAISE(ABORT,'immutable_event_binding_approval'); END;
