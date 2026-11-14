-- 调用方须在事务外关闭外键，再在同一排他写事务内执行并核验所有外键。
CREATE TABLE service_state_v2 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE,
 database_instance_id TEXT NOT NULL UNIQUE,
 request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='2'),
 maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'),
 UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v2 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'2',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v2 RENAME TO service_state;
CREATE TRIGGER service_state_no_update BEFORE UPDATE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;
CREATE TRIGGER service_state_no_delete BEFORE DELETE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;

CREATE TABLE schema_migration_v2 (
 migration_id TEXT PRIMARY KEY NOT NULL,
 from_revision TEXT NOT NULL,
 to_revision TEXT NOT NULL UNIQUE,
 script_digest TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state='committed'),
 CHECK((from_revision='0' AND to_revision='1') OR (from_revision='1' AND to_revision='2'))
) STRICT;
INSERT INTO schema_migration_v2 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v2 RENAME TO schema_migration;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;

CREATE TABLE definition_request_v2 (
 transaction_id TEXT PRIMARY KEY NOT NULL,
 identity_digest TEXT NOT NULL UNIQUE,
 request_digest TEXT NOT NULL,
 actor_jcs TEXT NOT NULL,
 operation_id TEXT NOT NULL CHECK(operation_id IN ('createDefinition','updateDraft','updateDefinitionMetadata','publishDefinition')),
 definition_id TEXT NOT NULL REFERENCES definition_identity(definition_id),
 result_jcs TEXT NOT NULL CHECK(length(CAST(result_jcs AS BLOB))<=16777216),
 result_digest TEXT NOT NULL,
 UNIQUE(transaction_id,definition_id),
 FOREIGN KEY(transaction_id) REFERENCES definition_notification(transaction_id) DEFERRABLE INITIALLY DEFERRED
) STRICT;
INSERT INTO definition_request_v2 SELECT * FROM definition_request;
DROP TABLE definition_request;
ALTER TABLE definition_request_v2 RENAME TO definition_request;
CREATE TRIGGER definition_request_no_update BEFORE UPDATE ON definition_request BEGIN SELECT RAISE(ABORT,'immutable_definition_request'); END;
CREATE TRIGGER definition_request_no_delete BEFORE DELETE ON definition_request BEGIN SELECT RAISE(ABORT,'immutable_definition_request'); END;

-- 预留和发布分开提交，失败形成空号，后续发布永不重用该版本。
CREATE TABLE definition_version_reservation (
 definition_id TEXT NOT NULL,
 definition_version TEXT NOT NULL CHECK(length(definition_version) BETWEEN 1 AND 20 AND definition_version NOT GLOB '*[^0-9]*' AND substr(definition_version,1,1)<>'0'),
 source_draft_revision TEXT NOT NULL,
 reservation_id TEXT NOT NULL UNIQUE,
 PRIMARY KEY(definition_id,definition_version),
 FOREIGN KEY(definition_id,source_draft_revision) REFERENCES definition_content(definition_id,revision)
) STRICT;
CREATE TABLE definition_publication (
 definition_id TEXT NOT NULL,
 definition_version TEXT NOT NULL,
 transaction_id TEXT NOT NULL UNIQUE,
 bundle_jcs TEXT NOT NULL CHECK(length(CAST(bundle_jcs AS BLOB))<=67108864),
 bundle_digest TEXT NOT NULL,
 PRIMARY KEY(definition_id,definition_version),
 FOREIGN KEY(definition_id,definition_version) REFERENCES definition_version_reservation(definition_id,definition_version),
 FOREIGN KEY(transaction_id,definition_id) REFERENCES definition_request(transaction_id,definition_id) DEFERRABLE INITIALLY DEFERRED
) STRICT;
CREATE TRIGGER definition_version_reservation_no_update BEFORE UPDATE ON definition_version_reservation BEGIN SELECT RAISE(ABORT,'immutable_definition_version_reservation'); END;
CREATE TRIGGER definition_version_reservation_no_delete BEFORE DELETE ON definition_version_reservation BEGIN SELECT RAISE(ABORT,'immutable_definition_version_reservation'); END;
CREATE TRIGGER definition_publication_no_update BEFORE UPDATE ON definition_publication BEGIN SELECT RAISE(ABORT,'immutable_definition_publication'); END;
CREATE TRIGGER definition_publication_no_delete BEFORE DELETE ON definition_publication BEGIN SELECT RAISE(ABORT,'immutable_definition_publication'); END;
CREATE TABLE definition_publication_proof (
 definition_id TEXT NOT NULL,
 proof_id TEXT NOT NULL,
 revision TEXT NOT NULL,
 definition_version TEXT NOT NULL,
 proof_digest TEXT NOT NULL,
 PRIMARY KEY(definition_id,proof_id,revision),
 FOREIGN KEY(definition_id,definition_version) REFERENCES definition_publication(definition_id,definition_version)
) STRICT;
CREATE TRIGGER definition_publication_proof_no_update BEFORE UPDATE ON definition_publication_proof BEGIN SELECT RAISE(ABORT,'immutable_definition_publication_proof'); END;
CREATE TRIGGER definition_publication_proof_no_delete BEFORE DELETE ON definition_publication_proof BEGIN SELECT RAISE(ABORT,'immutable_definition_publication_proof'); END;
