-- 为新摄取封存逐运行决定。历史接收和匹配原件保持原样。
DROP TRIGGER first_pairing_import_target;
CREATE TABLE service_state_v24 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='24'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v24 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'24',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v24 RENAME TO service_state;
CREATE TRIGGER service_state_no_update BEFORE UPDATE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;
CREATE TRIGGER service_state_no_delete BEFORE DELETE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;

CREATE TRIGGER first_pairing_import_target BEFORE INSERT ON first_pairing_import
WHEN EXISTS(SELECT 1 FROM peer_pairing_revision WHERE state='activated' LIMIT 1)
 OR EXISTS(SELECT 1 FROM local_permission_head LIMIT 1)
 OR NOT EXISTS(SELECT 1 FROM service_state s,json_each(NEW.body_jcs,'$.authorization.transcript.parties') p
    WHERE json_extract(p.value,'$.service_kind')='seamer'
     AND json_extract(p.value,'$.service_id') IS s.service_id
     AND json_extract(p.value,'$.database_instance_id') IS s.database_instance_id
     AND json_extract(p.value,'$.binding_id') IS NEW.target_binding_id)
BEGIN SELECT RAISE(ABORT,'first_pairing_import_target'); END;

CREATE TABLE schema_migration_v24 (
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
  (from_revision='20' AND to_revision='21') OR (from_revision='21' AND to_revision='22') OR
  (from_revision='22' AND to_revision='23') OR
  (from_revision='23' AND to_revision='24'))
) STRICT;
INSERT INTO schema_migration_v24 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v24 RENAME TO schema_migration;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;

CREATE TABLE automation_trigger_decision_set (
 ingest_epoch TEXT NOT NULL, receive_sequence INTEGER NOT NULL, match_digest TEXT NOT NULL CHECK(length(match_digest)=71),
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=1048576),
 body_digest TEXT NOT NULL CHECK(length(body_digest)=71), PRIMARY KEY(ingest_epoch,receive_sequence),
 FOREIGN KEY(ingest_epoch,receive_sequence) REFERENCES automation_trigger_match_set(ingest_epoch,receive_sequence),
 CHECK(json_extract(body_jcs,'$.match_digest') IS match_digest
   AND json_extract(body_jcs,'$.receive_cursor.ingest_epoch') IS ingest_epoch
   AND json_extract(body_jcs,'$.receive_cursor.receive_sequence') IS CAST(receive_sequence AS TEXT)
   AND json_type(body_jcs,'$.decisions') IS 'array')
) STRICT;

CREATE TABLE automation_trigger_decision (
 ingest_epoch TEXT NOT NULL, receive_sequence INTEGER NOT NULL, run_id TEXT NOT NULL REFERENCES automation_run_creation(run_id),
 decision_id TEXT NOT NULL UNIQUE, body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=8192),
 body_digest TEXT NOT NULL CHECK(length(body_digest)=71), PRIMARY KEY(ingest_epoch,receive_sequence,run_id),
 FOREIGN KEY(ingest_epoch,receive_sequence) REFERENCES automation_trigger_decision_set(ingest_epoch,receive_sequence) DEFERRABLE INITIALLY DEFERRED,
 CHECK(json_extract(body_jcs,'$.run_id') IS run_id AND json_extract(body_jcs,'$.decision_id') IS decision_id
   AND json_extract(body_jcs,'$.receive_cursor.ingest_epoch') IS ingest_epoch
   AND json_extract(body_jcs,'$.receive_cursor.receive_sequence') IS CAST(receive_sequence AS TEXT))
) STRICT;
CREATE INDEX automation_run_ingest_head ON automation_trigger_decision(run_id,ingest_epoch,receive_sequence DESC);

CREATE TRIGGER automation_trigger_decision_seal BEFORE INSERT ON automation_trigger_decision_set BEGIN
 SELECT CASE WHEN NOT EXISTS (
  SELECT 1 FROM automation_trigger_match_set m JOIN subscription_reconcile_inbox i USING(ingest_epoch,receive_sequence)
   WHERE m.ingest_epoch=NEW.ingest_epoch AND m.receive_sequence=NEW.receive_sequence AND m.body_digest=NEW.match_digest
    AND i.observation_digest=json_extract(NEW.body_jcs,'$.first_observation_digest')
    AND json_extract(m.body_jcs,'$.receive_cursor')=json_extract(NEW.body_jcs,'$.receive_cursor')
    AND json_array_length(m.body_jcs,'$.targets')=json_array_length(NEW.body_jcs,'$.decisions')
    AND json_array_length(m.body_jcs,'$.targets')=(SELECT count(*) FROM automation_trigger_decision d
     WHERE d.ingest_epoch=NEW.ingest_epoch AND d.receive_sequence=NEW.receive_sequence)
    AND NOT EXISTS(SELECT 1 FROM json_each(m.body_jcs,'$.targets') t
     LEFT JOIN automation_trigger_decision d ON d.ingest_epoch=NEW.ingest_epoch AND d.receive_sequence=NEW.receive_sequence
      AND d.run_id=json_extract(t.value,'$.run_id')
     WHERE d.decision_id IS NULL OR json_extract(d.body_jcs,'$.match_digest') IS NOT NEW.match_digest
      OR json_extract(d.body_jcs,'$.artifact_ref') IS NOT json_extract(t.value,'$.artifact_ref')
      OR json_extract(NEW.body_jcs,'$.decisions['||t.key||'].run_id') IS NOT d.run_id
      OR json_extract(NEW.body_jcs,'$.decisions['||t.key||'].decision_id') IS NOT d.decision_id
      OR json_extract(NEW.body_jcs,'$.decisions['||t.key||'].digest') IS NOT d.body_digest)
 ) THEN RAISE(ABORT,'trigger_decision_incomplete') END;
END;
CREATE TRIGGER automation_trigger_decision_closed BEFORE INSERT ON automation_trigger_decision
WHEN EXISTS(SELECT 1 FROM automation_trigger_decision_set s WHERE s.ingest_epoch=NEW.ingest_epoch AND s.receive_sequence=NEW.receive_sequence)
BEGIN SELECT RAISE(ABORT,'immutable_trigger_decision_set'); END;
CREATE TRIGGER automation_trigger_decision_no_update BEFORE UPDATE ON automation_trigger_decision BEGIN SELECT RAISE(ABORT,'immutable_trigger_decision'); END;
CREATE TRIGGER automation_trigger_decision_no_delete BEFORE DELETE ON automation_trigger_decision BEGIN SELECT RAISE(ABORT,'immutable_trigger_decision'); END;
CREATE TRIGGER automation_trigger_decision_set_no_update BEFORE UPDATE ON automation_trigger_decision_set BEGIN SELECT RAISE(ABORT,'immutable_trigger_decision_set'); END;
CREATE TRIGGER automation_trigger_decision_set_no_delete BEFORE DELETE ON automation_trigger_decision_set BEGIN SELECT RAISE(ABORT,'immutable_trigger_decision_set'); END;

