-- 显式升级接收侧年龄账本，保留首次观察及旧记录。
DROP TRIGGER first_pairing_import_target;
CREATE TABLE service_state_v22 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='22'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v22 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'22',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v22 RENAME TO service_state;
CREATE TRIGGER service_state_no_update BEFORE UPDATE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;
CREATE TRIGGER service_state_no_delete BEFORE DELETE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;

CREATE TRIGGER first_pairing_import_target BEFORE INSERT ON first_pairing_import
WHEN EXISTS(SELECT 1 FROM peer_pairing_revision WHERE state='activated')
 OR EXISTS(SELECT 1 FROM local_permission_head)
 OR NOT EXISTS(SELECT 1 FROM service_state s,json_each(NEW.body_jcs,'$.authorization.transcript.parties') p
    WHERE json_extract(p.value,'$.service_kind')='seamer'
     AND json_extract(p.value,'$.service_id') IS s.service_id
     AND json_extract(p.value,'$.database_instance_id') IS s.database_instance_id
     AND json_extract(p.value,'$.binding_id') IS NEW.target_binding_id)
BEGIN SELECT RAISE(ABORT,'first_pairing_import_target'); END;

CREATE TABLE schema_migration_v22 (
 migration_id TEXT PRIMARY KEY NOT NULL, from_revision TEXT NOT NULL, to_revision TEXT NOT NULL UNIQUE,
 script_digest TEXT NOT NULL, state TEXT NOT NULL CHECK(state='committed'),
 CHECK((from_revision='0' AND to_revision='1') OR (from_revision='1' AND to_revision='2') OR
  (from_revision='2' AND to_revision='3') OR (from_revision='3' AND to_revision='4') OR
  (from_revision='4' AND to_revision='5') OR (from_revision='5' AND to_revision='6') OR
  (from_revision='6' AND to_revision='7') OR (from_revision='7' AND to_revision='8') OR
  (from_revision='8' AND to_revision='9') OR (from_revision='9' AND to_revision='10') OR
  (from_revision='10' AND to_revision='11') OR (from_revision='11' AND to_revision='12') OR
  (from_revision='12' AND to_revision='13') OR (from_revision='13' AND to_revision='14') OR
  (from_revision='14' AND to_revision='15') OR (from_revision='15' AND to_revision='16') OR
  (from_revision='16' AND to_revision='17') OR (from_revision='17' AND to_revision='18') OR
  (from_revision='18' AND to_revision='19') OR (from_revision='19' AND to_revision='20') OR
  (from_revision='20' AND to_revision='21') OR
  (from_revision='21' AND to_revision='22'))
) STRICT;
INSERT INTO schema_migration_v22 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v22 RENAME TO schema_migration;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;

CREATE TABLE event_age_revision (
 apply_id TEXT NOT NULL, cursor INTEGER NOT NULL, revision INTEGER NOT NULL CHECK(revision BETWEEN 2 AND 65536),
 acquisition_id TEXT NOT NULL REFERENCES authentication_clock_acquisition(id),
 evaluated_tick TEXT NOT NULL CHECK(length(evaluated_tick)=20 AND evaluated_tick NOT GLOB '*[^0-9]*'),
 first_digest TEXT NOT NULL CHECK(length(first_digest)=71), previous_digest TEXT NOT NULL CHECK(length(previous_digest)=71),
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=8192),
 body_digest TEXT NOT NULL UNIQUE CHECK(length(body_digest)=71),
 PRIMARY KEY(apply_id,cursor,revision), UNIQUE(apply_id,cursor,evaluated_tick),
 FOREIGN KEY(apply_id,cursor) REFERENCES subscription_reconcile_inbox(apply_id,cursor),
 CHECK(json_extract(body_jcs,'$.revision') IS CAST(revision AS TEXT) AND json_extract(body_jcs,'$.digest') IS body_digest
  AND json_extract(body_jcs,'$.basis') IS 'trusted_utc' AND json_type(body_jcs,'$.evaluated_tick_ns') IS 'text'
  AND substr('00000000000000000000'||json_extract(body_jcs,'$.evaluated_tick_ns'),-20) IS evaluated_tick)
) STRICT;
CREATE TRIGGER event_age_revision_first BEFORE INSERT ON event_age_revision
WHEN NOT EXISTS(SELECT 1 FROM subscription_reconcile_inbox i JOIN authentication_clock_acquisition a
 ON a.id=NEW.acquisition_id AND a.service_boot_id=i.receiver_boot_id AND a.clock_domain_id=i.receiver_clock_domain_id
 WHERE i.apply_id=NEW.apply_id AND i.cursor=NEW.cursor
 AND json_extract(i.observation_jcs,'$.age_evidence.basis') IS 'trusted_utc'
 AND json_extract(i.observation_jcs,'$.age_evidence.digest') IS NEW.first_digest
 AND json_extract(i.observation_jcs,'$.age_evidence.id') IS json_extract(NEW.body_jcs,'$.id')
 AND json_extract(i.observation_jcs,'$.age_evidence.origin_digest') IS json_extract(NEW.body_jcs,'$.origin_digest')
 AND json_extract(i.observation_jcs,'$.age_evidence.first_receive_tick_ns') IS json_extract(NEW.body_jcs,'$.first_receive_tick_ns')
 AND json_extract(i.observation_jcs,'$.age_evidence.receiver_domain') IS json_extract(NEW.body_jcs,'$.receiver_domain')
 AND NEW.evaluated_tick>substr('00000000000000000000'||json_extract(i.observation_jcs,'$.received_tick_ns'),-20))
BEGIN SELECT RAISE(ABORT,'event_age_revision_first'); END;
CREATE TRIGGER event_age_revision_chain BEFORE INSERT ON event_age_revision
WHEN NEW.revision<>coalesce((SELECT max(revision)+1 FROM event_age_revision WHERE apply_id=NEW.apply_id AND cursor=NEW.cursor),2)
 OR (NEW.revision=2 AND NEW.previous_digest<>NEW.first_digest)
 OR (NEW.revision>2 AND NOT EXISTS(SELECT 1 FROM event_age_revision p WHERE p.apply_id=NEW.apply_id AND p.cursor=NEW.cursor
  AND p.revision=NEW.revision-1 AND p.body_digest=NEW.previous_digest AND p.evaluated_tick<NEW.evaluated_tick))
BEGIN SELECT RAISE(ABORT,'event_age_revision_chain'); END;
CREATE TRIGGER event_age_revision_no_update BEFORE UPDATE ON event_age_revision BEGIN SELECT RAISE(ABORT,'immutable_event_age_revision'); END;
CREATE TRIGGER event_age_revision_no_delete BEFORE DELETE ON event_age_revision BEGIN SELECT RAISE(ABORT,'immutable_event_age_revision'); END;

