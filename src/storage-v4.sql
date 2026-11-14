-- 仅显式停机迁移；调用方在事务外关闭外键，提交前验证全部引用。
CREATE TABLE service_state_v4 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='4'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v4 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'4',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v4 RENAME TO service_state;
CREATE TRIGGER service_state_no_update BEFORE UPDATE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;
CREATE TRIGGER service_state_no_delete BEFORE DELETE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;
CREATE TABLE schema_migration_v4 (
 migration_id TEXT PRIMARY KEY NOT NULL, from_revision TEXT NOT NULL, to_revision TEXT NOT NULL UNIQUE,
 script_digest TEXT NOT NULL, state TEXT NOT NULL CHECK(state='committed'),
 CHECK((from_revision='0' AND to_revision='1') OR (from_revision='1' AND to_revision='2') OR (from_revision='2' AND to_revision='3') OR (from_revision='3' AND to_revision='4'))
) STRICT;
INSERT INTO schema_migration_v4 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v4 RENAME TO schema_migration;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;

CREATE TABLE snapshot_ingest_apply (
 apply_id TEXT PRIMARY KEY NOT NULL, slot_key TEXT NOT NULL, generation INTEGER NOT NULL CHECK(generation BETWEEN 1 AND 65536),
 owner_key TEXT NOT NULL, subscription_id TEXT NOT NULL, snapshot_id TEXT NOT NULL, manifest_digest TEXT NOT NULL,
 service_boot_id TEXT NOT NULL, body_jcs TEXT NOT NULL, body_digest TEXT NOT NULL,
 stored_bytes INTEGER NOT NULL CHECK(stored_bytes BETWEEN 1 AND 9437184), source_count INTEGER NOT NULL CHECK(source_count BETWEEN 1 AND 64),
 receipt_jcs TEXT NOT NULL, receipt_digest TEXT NOT NULL,
 UNIQUE(owner_key,subscription_id,snapshot_id), UNIQUE(slot_key,generation,apply_id), CHECK(length(CAST(body_jcs AS BLOB))=stored_bytes),
 FOREIGN KEY(apply_id) REFERENCES snapshot_ingest_seal(apply_id) DEFERRABLE INITIALLY DEFERRED
) STRICT;
CREATE TABLE snapshot_projection_head (
 slot_key TEXT PRIMARY KEY NOT NULL, generation INTEGER NOT NULL, apply_id TEXT NOT NULL UNIQUE,
 FOREIGN KEY(slot_key,generation,apply_id) REFERENCES snapshot_ingest_apply(slot_key,generation,apply_id)
) STRICT;
CREATE TABLE ingest_source_revision (
 source_key_digest TEXT NOT NULL, revision INTEGER NOT NULL CHECK(revision BETWEEN 1 AND 65536), lineage_key TEXT NOT NULL,
 apply_id TEXT NOT NULL REFERENCES snapshot_ingest_apply(apply_id), body_jcs TEXT NOT NULL CHECK(length(CAST(body_jcs AS BLOB))<=262144), body_digest TEXT NOT NULL,
 PRIMARY KEY(source_key_digest,revision), UNIQUE(source_key_digest,apply_id)
) STRICT;
CREATE INDEX ingest_source_lineage ON ingest_source_revision(lineage_key);
CREATE TABLE ingest_source_head (
 source_key_digest TEXT PRIMARY KEY NOT NULL, revision INTEGER NOT NULL,
 FOREIGN KEY(source_key_digest,revision) REFERENCES ingest_source_revision(source_key_digest,revision)
) STRICT;
CREATE TABLE snapshot_ingest_audit (apply_id TEXT PRIMARY KEY NOT NULL REFERENCES snapshot_ingest_apply(apply_id), body_jcs TEXT NOT NULL, body_digest TEXT NOT NULL) STRICT;
CREATE TABLE snapshot_ingest_outbox (apply_id TEXT PRIMARY KEY NOT NULL REFERENCES snapshot_ingest_apply(apply_id), body_jcs TEXT NOT NULL, body_digest TEXT NOT NULL) STRICT;
CREATE TABLE snapshot_ingest_seal (apply_id TEXT PRIMARY KEY NOT NULL REFERENCES snapshot_ingest_apply(apply_id), receipt_digest TEXT NOT NULL) STRICT;
CREATE TRIGGER snapshot_ingest_complete BEFORE INSERT ON snapshot_ingest_seal BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM snapshot_ingest_apply a JOIN snapshot_projection_head h USING(slot_key,generation,apply_id)
  JOIN snapshot_ingest_audit d USING(apply_id) JOIN snapshot_ingest_outbox o USING(apply_id)
  WHERE a.apply_id=NEW.apply_id AND a.receipt_digest=NEW.receipt_digest
  AND a.source_count=(SELECT count(*) FROM ingest_source_revision WHERE apply_id=a.apply_id)
  AND a.source_count=(SELECT count(*) FROM ingest_source_revision r JOIN ingest_source_head s USING(source_key_digest,revision) WHERE r.apply_id=a.apply_id))
 THEN RAISE(ABORT,'snapshot_ingest_incomplete') END;
END;
CREATE TRIGGER snapshot_projection_first BEFORE INSERT ON snapshot_projection_head WHEN NEW.generation<>1 BEGIN SELECT RAISE(ABORT,'snapshot_projection_generation'); END;
CREATE TRIGGER snapshot_projection_step BEFORE UPDATE ON snapshot_projection_head WHEN NEW.slot_key<>OLD.slot_key OR NEW.generation<>OLD.generation+1 BEGIN SELECT RAISE(ABORT,'snapshot_projection_generation'); END;
CREATE TRIGGER snapshot_projection_no_delete BEFORE DELETE ON snapshot_projection_head BEGIN SELECT RAISE(ABORT,'immutable_snapshot_projection'); END;
CREATE TRIGGER ingest_source_first BEFORE INSERT ON ingest_source_head WHEN NEW.revision<>1 BEGIN SELECT RAISE(ABORT,'ingest_source_revision'); END;
CREATE TRIGGER ingest_source_step BEFORE UPDATE ON ingest_source_head WHEN NEW.source_key_digest<>OLD.source_key_digest OR NEW.revision<>OLD.revision+1 BEGIN SELECT RAISE(ABORT,'ingest_source_revision'); END;
CREATE TRIGGER ingest_source_no_delete BEFORE DELETE ON ingest_source_head BEGIN SELECT RAISE(ABORT,'immutable_ingest_source'); END;
CREATE TRIGGER ingest_source_sealed BEFORE INSERT ON ingest_source_revision WHEN EXISTS(SELECT 1 FROM snapshot_ingest_seal WHERE apply_id=NEW.apply_id) BEGIN SELECT RAISE(ABORT,'immutable_snapshot_ingest'); END;
CREATE TRIGGER snapshot_ingest_apply_no_update BEFORE UPDATE ON snapshot_ingest_apply BEGIN SELECT RAISE(ABORT,'immutable_snapshot_ingest'); END;
CREATE TRIGGER snapshot_ingest_apply_no_delete BEFORE DELETE ON snapshot_ingest_apply BEGIN SELECT RAISE(ABORT,'immutable_snapshot_ingest'); END;
CREATE TRIGGER ingest_source_revision_no_update BEFORE UPDATE ON ingest_source_revision BEGIN SELECT RAISE(ABORT,'immutable_ingest_source'); END;
CREATE TRIGGER ingest_source_revision_no_delete BEFORE DELETE ON ingest_source_revision BEGIN SELECT RAISE(ABORT,'immutable_ingest_source'); END;
CREATE TRIGGER snapshot_ingest_audit_no_update BEFORE UPDATE ON snapshot_ingest_audit BEGIN SELECT RAISE(ABORT,'immutable_snapshot_ingest'); END;
CREATE TRIGGER snapshot_ingest_audit_no_delete BEFORE DELETE ON snapshot_ingest_audit BEGIN SELECT RAISE(ABORT,'immutable_snapshot_ingest'); END;
CREATE TRIGGER snapshot_ingest_outbox_no_update BEFORE UPDATE ON snapshot_ingest_outbox BEGIN SELECT RAISE(ABORT,'immutable_snapshot_ingest'); END;
CREATE TRIGGER snapshot_ingest_outbox_no_delete BEFORE DELETE ON snapshot_ingest_outbox BEGIN SELECT RAISE(ABORT,'immutable_snapshot_ingest'); END;
CREATE TRIGGER snapshot_ingest_seal_no_update BEFORE UPDATE ON snapshot_ingest_seal BEGIN SELECT RAISE(ABORT,'immutable_snapshot_ingest'); END;
CREATE TRIGGER snapshot_ingest_seal_no_delete BEFORE DELETE ON snapshot_ingest_seal BEGIN SELECT RAISE(ABORT,'immutable_snapshot_ingest'); END;
