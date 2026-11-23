-- 显式停机迁移。普通布防完成与运行修订及通知同事务。
CREATE TABLE service_state_v16 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='16'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v16 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'16',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v16 RENAME TO service_state;
CREATE TRIGGER service_state_no_update BEFORE UPDATE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;
CREATE TRIGGER service_state_no_delete BEFORE DELETE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;

CREATE TABLE schema_migration_v16 (
 migration_id TEXT PRIMARY KEY NOT NULL, from_revision TEXT NOT NULL, to_revision TEXT NOT NULL UNIQUE,
 script_digest TEXT NOT NULL, state TEXT NOT NULL CHECK(state='committed'),
 CHECK((from_revision='0' AND to_revision='1') OR (from_revision='1' AND to_revision='2') OR
  (from_revision='2' AND to_revision='3') OR (from_revision='3' AND to_revision='4') OR
  (from_revision='4' AND to_revision='5') OR (from_revision='5' AND to_revision='6') OR
  (from_revision='6' AND to_revision='7') OR (from_revision='7' AND to_revision='8') OR
  (from_revision='8' AND to_revision='9') OR (from_revision='9' AND to_revision='10') OR
  (from_revision='10' AND to_revision='11') OR (from_revision='11' AND to_revision='12') OR
  (from_revision='12' AND to_revision='13') OR (from_revision='13' AND to_revision='14') OR
  (from_revision='14' AND to_revision='15') OR (from_revision='15' AND to_revision='16'))
) STRICT;
INSERT INTO schema_migration_v16 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v16 RENAME TO schema_migration;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;

CREATE TABLE automation_arm_completion (
 operation_id TEXT PRIMARY KEY NOT NULL REFERENCES automation_run_arm(operation_id),
 run_id TEXT NOT NULL UNIQUE REFERENCES automation_run_creation(run_id),
 approval_id TEXT NOT NULL REFERENCES automation_run_approval(approval_id),
 previous_run_revision TEXT NOT NULL,
 completed_run_revision TEXT NOT NULL,
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=1048576), body_digest TEXT NOT NULL,
 FOREIGN KEY(operation_id) REFERENCES automation_arm_completion_notification(operation_id) DEFERRABLE INITIALLY DEFERRED
) STRICT;
CREATE TRIGGER automation_arm_completion_no_update BEFORE UPDATE ON automation_arm_completion BEGIN SELECT RAISE(ABORT,'immutable_arm_completion'); END;
CREATE TRIGGER automation_arm_completion_no_delete BEFORE DELETE ON automation_arm_completion BEGIN SELECT RAISE(ABORT,'immutable_arm_completion'); END;

CREATE TABLE automation_arm_completion_notification (
 operation_id TEXT PRIMARY KEY NOT NULL REFERENCES automation_arm_completion(operation_id),
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs)), body_digest TEXT NOT NULL
) STRICT;
CREATE TRIGGER automation_arm_completion_seal AFTER INSERT ON automation_arm_completion_notification BEGIN
 SELECT CASE WHEN NOT EXISTS (
  SELECT 1 FROM automation_arm_completion c JOIN automation_run_arm a USING(operation_id)
  JOIN automation_activation_slot s USING(operation_id) JOIN automation_arm_budget b USING(operation_id)
  JOIN automation_run_approval_head h ON h.run_id=c.run_id AND h.approval_id=c.approval_id
  JOIN automation_run_approval_state v ON v.approval_id=c.approval_id
  WHERE c.operation_id=NEW.operation_id AND c.run_id=a.run_id AND s.run_id=a.run_id AND s.definition_id=a.definition_id
   AND s.revision=json_extract(a.body_jcs,'$.committed_slot_revision')
   AND c.previous_run_revision=h.run_revision AND c.previous_run_revision=json_extract(a.body_jcs,'$.committed_run_revision')
   AND c.approval_id=json_extract(a.body_jcs,'$.approval_ref.object_id') AND v.state='active'
   AND length(c.completed_run_revision) BETWEEN 1 AND 20 AND substr(c.completed_run_revision,1,1) BETWEEN '1' AND '9'
   AND c.completed_run_revision NOT GLOB '*[^0-9]*'
   AND CAST(substr('00000000000000000000'||c.completed_run_revision,-10) AS INTEGER)=
    (CAST(substr('00000000000000000000'||c.previous_run_revision,-10) AS INTEGER)+1)%10000000000
   AND CAST(substr('00000000000000000000'||c.completed_run_revision,-20,10) AS INTEGER)=
    CAST(substr('00000000000000000000'||c.previous_run_revision,-20,10) AS INTEGER)+
    (CAST(substr('00000000000000000000'||c.previous_run_revision,-10) AS INTEGER)+1)/10000000000
   AND json_extract(c.body_jcs,'$.state')='ready' AND json_extract(c.body_jcs,'$.completed_run_revision')=c.completed_run_revision
   AND NOT EXISTS (
    SELECT 1 FROM automation_arm_target t LEFT JOIN automation_arm_preparation_receipt r USING(preparation_id)
    WHERE t.operation_id=c.operation_id AND (r.preparation_id IS NULL OR NOT EXISTS (
     SELECT 1 FROM json_each(c.body_jcs,'$.evidence.targets') i
     WHERE json_extract(i.value,'$.request.lookup.preparation_id')=t.preparation_id
      AND json_extract(i.value,'$.request.lookup.controller_scope.authority_service_id')=t.controller_service_id
      AND json_extract(i.value,'$.request.lookup.controller_scope.database_instance_id')=t.controller_database_instance_id
      AND json_extract(i.value,'$.request.lookup.request_digest')=r.request_digest
      AND json_extract(i.value,'$.request.receipt_ref.object_id')=r.receipt_id
      AND json_extract(i.value,'$.request.grant_ref.object_id')=r.grant_id
      AND json_extract(i.value,'$.request.grant_ref.digest')=json_extract(r.body_jcs,'$.applied_receipt.grant_ref.digest')
      AND json_extract(i.value,'$.state')='eligible')))
   AND json_type(c.body_jcs,'$.evidence.targets')='array'
   AND json_array_length(c.body_jcs,'$.evidence.targets')=(SELECT count(*) FROM automation_arm_target WHERE operation_id=c.operation_id)
 ) THEN RAISE(ABORT,'arm_completion_precondition') END;
 UPDATE automation_run_approval_head SET run_revision=(SELECT completed_run_revision FROM automation_arm_completion WHERE operation_id=NEW.operation_id)
  WHERE (run_id,approval_id,run_revision)=(SELECT run_id,approval_id,previous_run_revision FROM automation_arm_completion WHERE operation_id=NEW.operation_id);
 SELECT CASE WHEN changes()<>1 THEN RAISE(ABORT,'arm_completion_revision') END;
END;
CREATE TRIGGER automation_arm_completion_notification_no_update BEFORE UPDATE ON automation_arm_completion_notification BEGIN SELECT RAISE(ABORT,'immutable_arm_completion_notification'); END;
CREATE TRIGGER automation_arm_completion_notification_no_delete BEFORE DELETE ON automation_arm_completion_notification BEGIN SELECT RAISE(ABORT,'immutable_arm_completion_notification'); END;
