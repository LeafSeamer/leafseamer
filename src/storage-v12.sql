-- 显式停机迁移。绑定撤销可触发现有运行批准保护，保留旧批准和传输回执。
CREATE TABLE service_state_v12 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='12'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v12 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'12',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v12 RENAME TO service_state;
CREATE TRIGGER service_state_no_update BEFORE UPDATE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;
CREATE TRIGGER service_state_no_delete BEFORE DELETE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;

CREATE TABLE schema_migration_v12 (
 migration_id TEXT PRIMARY KEY NOT NULL, from_revision TEXT NOT NULL, to_revision TEXT NOT NULL UNIQUE,
 script_digest TEXT NOT NULL, state TEXT NOT NULL CHECK(state='committed'),
 CHECK((from_revision='0' AND to_revision='1') OR (from_revision='1' AND to_revision='2') OR (from_revision='2' AND to_revision='3') OR
  (from_revision='3' AND to_revision='4') OR (from_revision='4' AND to_revision='5') OR (from_revision='5' AND to_revision='6') OR
  (from_revision='6' AND to_revision='7') OR (from_revision='7' AND to_revision='8') OR (from_revision='8' AND to_revision='9') OR
  (from_revision='9' AND to_revision='10') OR (from_revision='10' AND to_revision='11') OR (from_revision='11' AND to_revision='12'))
) STRICT;
INSERT INTO schema_migration_v12 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v12 RENAME TO schema_migration;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;

CREATE TABLE automation_run_approval_revocation_v12 (
 operation_id TEXT PRIMARY KEY NOT NULL, approval_id TEXT NOT NULL UNIQUE REFERENCES automation_run_approval(approval_id),
 context_id TEXT REFERENCES local_request_context(context_id), identity_digest TEXT NOT NULL UNIQUE, request_digest TEXT NOT NULL,
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=1048576), body_digest TEXT NOT NULL,
 audit_jcs TEXT NOT NULL CHECK(json_valid(audit_jcs) AND length(CAST(audit_jcs AS BLOB))<=65536), audit_digest TEXT NOT NULL,
 CHECK(context_id IS NOT NULL OR json_type(audit_jcs,'$.endorsement_revocation_ref') IS 'object' OR
  (json_type(audit_jcs,'$.binding_revocation.approval_ref') IS 'object' AND json_type(audit_jcs,'$.binding_revocation.revocation_ref') IS 'object'))
) STRICT;
INSERT INTO automation_run_approval_revocation_v12 SELECT * FROM automation_run_approval_revocation;
DROP TABLE automation_run_approval_revocation;
ALTER TABLE automation_run_approval_revocation_v12 RENAME TO automation_run_approval_revocation;
CREATE INDEX automation_run_approval_revocation_context ON automation_run_approval_revocation(context_id);
CREATE TRIGGER automation_run_approval_revocation_no_update BEFORE UPDATE ON automation_run_approval_revocation
BEGIN SELECT RAISE(ABORT,'immutable_run_approval_revocation'); END;
CREATE TRIGGER automation_run_approval_revocation_no_delete BEFORE DELETE ON automation_run_approval_revocation
BEGIN SELECT RAISE(ABORT,'immutable_run_approval_revocation'); END;

CREATE TABLE definition_publication_binding_pin (
 definition_id TEXT NOT NULL, definition_version TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('event','device')), approval_id TEXT NOT NULL, approval_revision TEXT NOT NULL, approval_digest TEXT NOT NULL,
 PRIMARY KEY(definition_id,definition_version,kind,approval_id,approval_revision,approval_digest),
 FOREIGN KEY(definition_id,definition_version) REFERENCES definition_publication(definition_id,definition_version)
) STRICT;
INSERT INTO definition_publication_binding_pin
 SELECT DISTINCT p.definition_id,p.definition_version,json_extract(pin.value,'$.kind'),json_extract(pin.value,'$.approval_ref.id'),
  json_extract(pin.value,'$.approval_ref.revision'),json_extract(pin.value,'$.approval_ref.digest')
 FROM definition_publication p,json_each(p.bundle_jcs,'$.snapshot.review.binding_approval_refs') pin;
CREATE INDEX definition_publication_binding_pin_lookup ON definition_publication_binding_pin(approval_id,approval_digest,kind,approval_revision);
CREATE TRIGGER definition_publication_binding_pin_no_update BEFORE UPDATE ON definition_publication_binding_pin
BEGIN SELECT RAISE(ABORT,'immutable_publication_binding_pin'); END;
CREATE TRIGGER definition_publication_binding_pin_no_delete BEFORE DELETE ON definition_publication_binding_pin
BEGIN SELECT RAISE(ABORT,'immutable_publication_binding_pin'); END;

CREATE VIEW revoked_publication_binding AS
 SELECT 'event' AS kind, approval_id, definition_id, previous_digest AS approval_digest
 FROM event_binding_approval_revision WHERE revision=2 AND state='revoked'
 UNION ALL
 SELECT 'device', a.approval_id, a.definition_id, a.body_digest
 FROM binding_approval_revision a JOIN binding_approval_revision r USING(approval_id,definition_id)
 WHERE a.revision=1 AND a.state='approved' AND r.revision=2 AND r.state='revoked';
