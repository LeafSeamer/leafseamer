-- 仅显式停机迁移；调用方在事务外关闭外键，提交前验证全部引用。
CREATE TABLE service_state_v5 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='5'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v5 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'5',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v5 RENAME TO service_state;
CREATE TRIGGER service_state_no_update BEFORE UPDATE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;
CREATE TRIGGER service_state_no_delete BEFORE DELETE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;
CREATE TABLE schema_migration_v5 (
 migration_id TEXT PRIMARY KEY NOT NULL, from_revision TEXT NOT NULL, to_revision TEXT NOT NULL UNIQUE,
 script_digest TEXT NOT NULL, state TEXT NOT NULL CHECK(state='committed'),
 CHECK((from_revision='0' AND to_revision='1') OR (from_revision='1' AND to_revision='2') OR (from_revision='2' AND to_revision='3') OR (from_revision='3' AND to_revision='4') OR (from_revision='4' AND to_revision='5'))
) STRICT;
INSERT INTO schema_migration_v5 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v5 RENAME TO schema_migration;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;

CREATE TABLE event_ingest_epoch (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 service_id TEXT NOT NULL, database_instance_id TEXT NOT NULL, ingest_epoch TEXT NOT NULL UNIQUE,
 FOREIGN KEY(service_id,database_instance_id) REFERENCES service_state(service_id,database_instance_id)
) STRICT;
CREATE TRIGGER event_ingest_epoch_no_update BEFORE UPDATE ON event_ingest_epoch
BEGIN SELECT RAISE(ABORT,'immutable_ingest_epoch'); END;
CREATE TRIGGER event_ingest_epoch_no_delete BEFORE DELETE ON event_ingest_epoch
BEGIN SELECT RAISE(ABORT,'immutable_ingest_epoch'); END;

CREATE TABLE definition_program_dependencies (
 definition_id TEXT NOT NULL, definition_version TEXT NOT NULL,
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=67108864), body_digest TEXT NOT NULL,
 PRIMARY KEY(definition_id,definition_version),
 UNIQUE(definition_id,definition_version,body_digest),
 FOREIGN KEY(definition_id,definition_version) REFERENCES definition_publication(definition_id,definition_version)
) STRICT;
CREATE TRIGGER definition_program_dependencies_no_update BEFORE UPDATE ON definition_program_dependencies
BEGIN SELECT RAISE(ABORT,'immutable_program_dependencies'); END;
CREATE TRIGGER definition_program_dependencies_no_delete BEFORE DELETE ON definition_program_dependencies
BEGIN SELECT RAISE(ABORT,'immutable_program_dependencies'); END;

CREATE TABLE automation_run_creation (
 run_id TEXT PRIMARY KEY NOT NULL,
 definition_id TEXT NOT NULL, definition_version TEXT NOT NULL,
 context_id TEXT NOT NULL REFERENCES local_request_context(context_id),
 identity_digest TEXT NOT NULL UNIQUE, request_digest TEXT NOT NULL,
 ingest_epoch TEXT NOT NULL REFERENCES event_ingest_epoch(ingest_epoch),
 receive_sequence INTEGER NOT NULL CHECK(receive_sequence BETWEEN 0 AND 65537),
 dependency_digest TEXT NOT NULL,
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=32768), body_digest TEXT NOT NULL,
 FOREIGN KEY(definition_id,definition_version,dependency_digest) REFERENCES definition_program_dependencies(definition_id,definition_version,body_digest),
 CHECK(json_extract(body_jcs,'$.run.lifecycle') IS 'disarmed' AND json_type(body_jcs,'$.run.dispatch_eligible') IS 'false')
) STRICT;
CREATE INDEX automation_run_creation_context ON automation_run_creation(context_id);
CREATE TRIGGER automation_run_creation_no_update BEFORE UPDATE ON automation_run_creation
BEGIN SELECT RAISE(ABORT,'immutable_run_creation'); END;
CREATE TRIGGER automation_run_creation_no_delete BEFORE DELETE ON automation_run_creation
BEGIN SELECT RAISE(ABORT,'immutable_run_creation'); END;

CREATE TABLE subscription_reconcile_inbox (
 apply_id TEXT NOT NULL REFERENCES snapshot_ingest_apply(apply_id),
 cursor INTEGER NOT NULL CHECK(cursor BETWEEN 1 AND 65537),
 connection_generation TEXT NOT NULL,
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=65536),
 body_digest TEXT NOT NULL,
 source_key_digest TEXT, source_sequence TEXT, event_id TEXT,
 observation_jcs TEXT NOT NULL CHECK(json_valid(observation_jcs)), observation_digest TEXT NOT NULL,
 receiver_boot_id TEXT NOT NULL, receiver_clock_domain_id TEXT NOT NULL, acquisition_id TEXT NOT NULL,
 ingest_epoch TEXT REFERENCES event_ingest_epoch(ingest_epoch), receive_sequence INTEGER CHECK(receive_sequence BETWEEN 1 AND 65537),
 PRIMARY KEY(apply_id,cursor),
 UNIQUE(source_key_digest,source_sequence),
 UNIQUE(source_key_digest,event_id),
 UNIQUE(ingest_epoch,receive_sequence),
 FOREIGN KEY(receiver_boot_id,receiver_clock_domain_id,acquisition_id)
  REFERENCES authentication_clock_acquisition(service_boot_id,clock_domain_id,id),
 CHECK(CASE json_extract(body_jcs,'$.entry_kind')
  WHEN 'source.event' THEN source_key_digest IS NOT NULL AND source_sequence IS NOT NULL AND event_id IS NOT NULL
   AND ingest_epoch IS NOT NULL AND receive_sequence IS NOT NULL
   AND json_extract(body_jcs,'$.payload.origin.source_sequence') IS source_sequence
   AND json_extract(body_jcs,'$.payload.origin.event_id') IS event_id
  WHEN 'entity.upsert' THEN source_key_digest IS NULL AND source_sequence IS NULL AND event_id IS NULL AND ingest_epoch IS NULL AND receive_sequence IS NULL
  WHEN 'sync.ready' THEN source_key_digest IS NULL AND source_sequence IS NULL AND event_id IS NULL AND ingest_epoch IS NULL AND receive_sequence IS NULL
  ELSE 0 END)
) STRICT;
CREATE INDEX subscription_reconcile_source_frontier ON subscription_reconcile_inbox(
 source_key_digest,length(source_sequence) DESC,source_sequence DESC
);
CREATE INDEX subscription_reconcile_apply_source ON subscription_reconcile_inbox(apply_id,source_key_digest,cursor)
 WHERE source_key_digest IS NOT NULL;
CREATE INDEX subscription_reconcile_ready_checkpoint ON subscription_reconcile_inbox(apply_id,connection_generation,cursor)
 WHERE json_extract(body_jcs,'$.entry_kind')='sync.ready';
CREATE INDEX subscription_reconcile_source_projection ON subscription_reconcile_inbox(apply_id,json_extract(body_jcs,'$.payload.entity_id'),cursor)
 WHERE json_extract(body_jcs,'$.entry_kind')='entity.upsert';
CREATE TRIGGER subscription_reconcile_inbox_parent BEFORE INSERT ON subscription_reconcile_inbox
WHEN NEW.cursor<>1+coalesce((SELECT max(cursor) FROM subscription_reconcile_inbox WHERE apply_id=NEW.apply_id),0)
 OR NOT EXISTS(SELECT 1 FROM snapshot_projection_head WHERE apply_id=NEW.apply_id)
BEGIN SELECT RAISE(ABORT,'reconcile_inbox_parent'); END;
CREATE TRIGGER subscription_reconcile_receive_sequence BEFORE INSERT ON subscription_reconcile_inbox
WHEN NEW.receive_sequence IS NOT NULL AND NEW.receive_sequence<>1+coalesce(
 (SELECT max(receive_sequence) FROM subscription_reconcile_inbox WHERE ingest_epoch=NEW.ingest_epoch),0)
BEGIN SELECT RAISE(ABORT,'reconcile_receive_sequence'); END;
CREATE TRIGGER subscription_reconcile_inbox_no_update BEFORE UPDATE ON subscription_reconcile_inbox
BEGIN SELECT RAISE(ABORT,'immutable_reconcile_inbox'); END;
CREATE TRIGGER subscription_reconcile_inbox_no_delete BEFORE DELETE ON subscription_reconcile_inbox
BEGIN SELECT RAISE(ABORT,'immutable_reconcile_inbox'); END;

CREATE TABLE ingest_source_fault (
 fault_key TEXT PRIMARY KEY NOT NULL,
 source_key_digest TEXT NOT NULL,
 apply_id TEXT NOT NULL REFERENCES snapshot_ingest_apply(apply_id),
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=8192),
 body_digest TEXT NOT NULL,
 receiver_boot_id TEXT NOT NULL, receiver_clock_domain_id TEXT NOT NULL, acquisition_id TEXT NOT NULL,
 FOREIGN KEY(receiver_boot_id,receiver_clock_domain_id,acquisition_id)
  REFERENCES authentication_clock_acquisition(service_boot_id,clock_domain_id,id)
) STRICT;
CREATE INDEX ingest_source_fault_source ON ingest_source_fault(source_key_digest,fault_key);
CREATE TRIGGER ingest_source_fault_no_update BEFORE UPDATE ON ingest_source_fault
BEGIN SELECT RAISE(ABORT,'immutable_source_fault'); END;
CREATE TRIGGER ingest_source_fault_no_delete BEFORE DELETE ON ingest_source_fault
BEGIN SELECT RAISE(ABORT,'immutable_source_fault'); END;
