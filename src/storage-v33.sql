-- 本机执行完成通知的持久消费回执。
DROP TRIGGER first_pairing_import_target;
CREATE TABLE service_state_v33 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='33'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v33 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'33',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v33 RENAME TO service_state;
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

CREATE TABLE schema_migration_v33 (
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
  (from_revision='23' AND to_revision='24') OR
  (from_revision='24' AND to_revision='25') OR (from_revision='25' AND to_revision='26') OR (from_revision='26' AND to_revision='27') OR (from_revision='27' AND to_revision='28') OR (from_revision='28' AND to_revision='29') OR (from_revision='29' AND to_revision='30') OR (from_revision='30' AND to_revision='31') OR (from_revision='31' AND to_revision='32') OR (from_revision='32' AND to_revision='33'))
) STRICT;
INSERT INTO schema_migration_v33 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v33 RENAME TO schema_migration;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;

CREATE TABLE automation_execution_notification_receipt (
 consumer_id TEXT NOT NULL,
 execution_id TEXT NOT NULL REFERENCES automation_execution_commit(execution_id),
 event_id TEXT NOT NULL,
 payload_digest TEXT NOT NULL CHECK(length(payload_digest)=71),
 commit_digest TEXT NOT NULL CHECK(length(commit_digest)=71),
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=4096),
 PRIMARY KEY(consumer_id,execution_id)
) STRICT;
CREATE TRIGGER automation_execution_notification_receipt_binding BEFORE INSERT ON automation_execution_notification_receipt
WHEN NOT EXISTS(SELECT 1 FROM automation_execution_commit c WHERE c.execution_id=NEW.execution_id
 AND c.body_digest=NEW.commit_digest AND json_extract(c.notification_jcs,'$.event_id') IS NEW.event_id
 AND json_extract(NEW.body_jcs,'$.format')='leafseamer.execution-notification-ack/1'
 AND json_extract(NEW.body_jcs,'$.scope') IS json_extract(c.body_jcs,'$.execution_ref.scope')
 AND json_extract(NEW.body_jcs,'$.consumer_id') IS NEW.consumer_id
 AND json_extract(NEW.body_jcs,'$.event_id') IS NEW.event_id
 AND json_extract(NEW.body_jcs,'$.payload_digest') IS NEW.payload_digest)
BEGIN SELECT RAISE(ABORT,'execution_notification_binding'); END;
CREATE TRIGGER automation_execution_notification_receipt_no_update BEFORE UPDATE ON automation_execution_notification_receipt
BEGIN SELECT RAISE(ABORT,'immutable_execution_notification_receipt'); END;
CREATE TRIGGER automation_execution_notification_receipt_no_delete BEFORE DELETE ON automation_execution_notification_receipt
BEGIN SELECT RAISE(ABORT,'immutable_execution_notification_receipt'); END;

