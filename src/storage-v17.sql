-- 显式迁移。保留撤销受理和拒绝回执，追加最终覆盖证明。
CREATE TABLE service_state_v17 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='17'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v17 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'17',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v17 RENAME TO service_state;
CREATE TRIGGER service_state_no_update BEFORE UPDATE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;
CREATE TRIGGER service_state_no_delete BEFORE DELETE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;

CREATE TABLE schema_migration_v17 (
 migration_id TEXT PRIMARY KEY NOT NULL, from_revision TEXT NOT NULL, to_revision TEXT NOT NULL UNIQUE,
 script_digest TEXT NOT NULL, state TEXT NOT NULL CHECK(state='committed'),
 CHECK((from_revision='0' AND to_revision='1') OR (from_revision='1' AND to_revision='2') OR
  (from_revision='2' AND to_revision='3') OR (from_revision='3' AND to_revision='4') OR
  (from_revision='4' AND to_revision='5') OR (from_revision='5' AND to_revision='6') OR
  (from_revision='6' AND to_revision='7') OR (from_revision='7' AND to_revision='8') OR
  (from_revision='8' AND to_revision='9') OR (from_revision='9' AND to_revision='10') OR
  (from_revision='10' AND to_revision='11') OR (from_revision='11' AND to_revision='12') OR
  (from_revision='12' AND to_revision='13') OR (from_revision='13' AND to_revision='14') OR
  (from_revision='14' AND to_revision='15') OR (from_revision='15' AND to_revision='16') OR (from_revision='16' AND to_revision='17'))
) STRICT;
INSERT INTO schema_migration_v17 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v17 RENAME TO schema_migration;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;

CREATE TABLE automation_run_approval_protection_completion (
 operation_id TEXT NOT NULL,target_service_id TEXT NOT NULL,target_database_instance_id TEXT NOT NULL,
 receipt_id TEXT NOT NULL,body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=16384),body_digest TEXT NOT NULL,
 PRIMARY KEY(operation_id,target_service_id,target_database_instance_id),
 UNIQUE(target_service_id,target_database_instance_id,receipt_id),
 FOREIGN KEY(operation_id,target_service_id,target_database_instance_id)
  REFERENCES automation_run_approval_protection_receipt(operation_id,target_service_id,target_database_instance_id)
) STRICT;
CREATE TRIGGER automation_run_approval_protection_completion_parent BEFORE INSERT ON automation_run_approval_protection_completion
WHEN NOT EXISTS(SELECT 1 FROM automation_run_approval_protection_receipt r JOIN automation_run_approval_protection_outbox o
 USING(operation_id,target_service_id,target_database_instance_id)
 WHERE r.operation_id=NEW.operation_id AND r.target_service_id=NEW.target_service_id AND r.target_database_instance_id=NEW.target_database_instance_id
 AND json_extract(NEW.body_jcs,'$.receipt_id')=NEW.receipt_id AND json_extract(NEW.body_jcs,'$.rejection.receipt_id')=r.receipt_id
 AND json_extract(NEW.body_jcs,'$.rejection.request_digest')=o.body_digest AND json_extract(NEW.body_jcs,'$.protection_state')='applied')
BEGIN SELECT RAISE(ABORT,'run_approval_protection_completion_binding'); END;
CREATE TRIGGER automation_run_approval_protection_completion_no_update BEFORE UPDATE ON automation_run_approval_protection_completion
BEGIN SELECT RAISE(ABORT,'immutable_run_approval_protection_completion'); END;
CREATE TRIGGER automation_run_approval_protection_completion_no_delete BEFORE DELETE ON automation_run_approval_protection_completion
BEGIN SELECT RAISE(ABORT,'immutable_run_approval_protection_completion'); END;

PRAGMA legacy_alter_table=ON;
ALTER TABLE automation_run_approval_state RENAME TO automation_run_approval_state_v16;
CREATE TABLE automation_run_approval_state (
 approval_id TEXT PRIMARY KEY NOT NULL REFERENCES automation_run_approval(approval_id),
 status_revision INTEGER NOT NULL,
 state TEXT NOT NULL CHECK((status_revision=1 AND state='active') OR (status_revision=2 AND state IN ('expired','revoking','revoked'))
  OR (status_revision=3 AND state='revoked'))
) STRICT;
INSERT INTO automation_run_approval_state SELECT * FROM automation_run_approval_state_v16;
DROP TABLE automation_run_approval_state_v16;
PRAGMA legacy_alter_table=OFF;
CREATE TRIGGER automation_run_approval_state_step BEFORE UPDATE ON automation_run_approval_state
WHEN NEW.approval_id<>OLD.approval_id OR NOT (
 (OLD.state='active' AND NEW.state IN ('expired','revoking','revoked') AND NEW.status_revision=2) OR
 (OLD.state IN ('revoking','expired') AND NEW.state='revoked' AND NEW.status_revision=3 AND EXISTS(
  SELECT 1 FROM automation_run_approval_revocation r WHERE r.approval_id=NEW.approval_id
  AND json_array_length(r.body_jcs,'$.targets')=(SELECT count(*) FROM automation_run_approval_protection_completion c WHERE c.operation_id=r.operation_id)
  AND NOT EXISTS(SELECT 1 FROM automation_run_approval_protection_outbox o LEFT JOIN automation_run_approval_protection_completion c
   USING(operation_id,target_service_id,target_database_instance_id) WHERE o.operation_id=r.operation_id AND c.receipt_id IS NULL))))
BEGIN SELECT RAISE(ABORT,'run_approval_state'); END;
CREATE TRIGGER automation_run_approval_state_no_delete BEFORE DELETE ON automation_run_approval_state
BEGIN SELECT RAISE(ABORT,'immutable_run_approval_state'); END;
