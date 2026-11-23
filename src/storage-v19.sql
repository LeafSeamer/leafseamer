-- 显式扩展首次配对完成原件；保留业务库身份、全部旧行和迁移历史。
DROP TRIGGER first_pairing_import_target;
CREATE TABLE service_state_v19 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='19'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v19 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'19',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v19 RENAME TO service_state;
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

CREATE TABLE schema_migration_v19 (
 migration_id TEXT PRIMARY KEY NOT NULL, from_revision TEXT NOT NULL, to_revision TEXT NOT NULL UNIQUE,
 script_digest TEXT NOT NULL, state TEXT NOT NULL CHECK(state='committed'),
 CHECK((from_revision='0' AND to_revision='1') OR (from_revision='1' AND to_revision='2') OR
  (from_revision='2' AND to_revision='3') OR (from_revision='3' AND to_revision='4') OR
  (from_revision='4' AND to_revision='5') OR (from_revision='5' AND to_revision='6') OR
  (from_revision='6' AND to_revision='7') OR (from_revision='7' AND to_revision='8') OR
  (from_revision='8' AND to_revision='9') OR (from_revision='9' AND to_revision='10') OR
  (from_revision='10' AND to_revision='11') OR (from_revision='11' AND to_revision='12') OR
  (from_revision='12' AND to_revision='13') OR (from_revision='13' AND to_revision='14') OR
  (from_revision='14' AND to_revision='15') OR (from_revision='15' AND to_revision='16') OR (from_revision='16' AND to_revision='17') OR (from_revision='17' AND to_revision='18') OR (from_revision='18' AND to_revision='19'))
) STRICT;
INSERT INTO schema_migration_v19 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v19 RENAME TO schema_migration;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;

CREATE TABLE first_pairing_completion (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 authorization_id TEXT NOT NULL UNIQUE REFERENCES first_pairing_import(authorization_id),
 binding_id TEXT NOT NULL,
 binding_revision INTEGER NOT NULL,
 permission_operation_id TEXT NOT NULL UNIQUE REFERENCES local_permission_operation(id),
 acquisition_id TEXT NOT NULL REFERENCES authentication_clock_acquisition(id),
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=8192),
 body_digest TEXT NOT NULL CHECK(length(body_digest)=71),
 FOREIGN KEY(binding_id,binding_revision) REFERENCES peer_pairing_revision(binding_id,revision),
 CHECK(json_extract(body_jcs,'$.format') IS 'leafseamer.first-pairing-completion/1'
  AND json_extract(body_jcs,'$.state') IS 'activated'
  AND json_extract(body_jcs,'$.authorization_id') IS authorization_id
  AND json_extract(body_jcs,'$.pairing_ref.id') IS binding_id
  AND json_extract(body_jcs,'$.pairing_ref.revision') IS CAST(binding_revision AS TEXT))
) STRICT;

CREATE TRIGGER first_pairing_completion_parent BEFORE INSERT ON first_pairing_completion
WHEN NOT EXISTS (SELECT 1 FROM first_pairing_import i
 JOIN peer_pairing_operation stage ON stage.request_id=i.authorization_id AND stage.binding_id=NEW.binding_id AND stage.revision=1
 JOIN peer_pairing_revision pair ON pair.binding_id=NEW.binding_id AND pair.revision=NEW.binding_revision
 JOIN peer_pairing_head h ON h.binding_id=pair.binding_id AND h.revision=pair.revision
 JOIN authentication_clock_acquisition a ON a.id=NEW.acquisition_id AND a.service_boot_id=pair.service_boot_id
 JOIN local_permission_operation operation ON operation.id=NEW.permission_operation_id
 JOIN local_subject subject ON subject.subject_id=operation.subject_id AND operation.actor_subject_id=subject.subject_id
 JOIN local_permission_revision grant ON grant.operation_id=operation.id
 JOIN local_permission_head g ON g.subject_id=grant.subject_id AND g.revision=grant.revision
 WHERE i.authorization_id=NEW.authorization_id AND i.target_binding_id=NEW.binding_id
  AND pair.state='activated' AND operation.request_id=i.authorization_id AND operation.request_digest=i.body_digest
  AND grant.state='active' AND grant.revision='00000000000000000001' AND subject.principal_kind='human'
  AND subject.principal_id=stage.principal_id
  AND subject.principal_id IS json_extract(i.body_jcs,'$.authorization.administrator.principal_id')
  AND subject.issuer_id IS json_extract(i.body_jcs,'$.authorization.administrator.issuer_id')
  AND subject.issuer_epoch IS json_extract(i.body_jcs,'$.authorization.administrator.issuer_epoch')
  AND json_extract(grant.body_jcs,'$.permissions') IS '["auth.manage","auth.read"]'
  AND json_extract(grant.body_jcs,'$.expires_ns') IS json_extract(pair.body_jcs,'$.expires_ns')
  AND json_extract(pair.body_jcs,'$.approval.authorization_ref.id') IS i.authorization_id
  AND json_extract(pair.body_jcs,'$.approval.authorization_ref.digest') IS i.body_digest
  AND json_extract(NEW.body_jcs,'$.authorization_digest') IS i.body_digest
  AND json_extract(NEW.body_jcs,'$.pairing_ref.digest') IS pair.body_digest
  AND json_extract(NEW.body_jcs,'$.local_permission_ref.id') IS grant.id
  AND json_extract(NEW.body_jcs,'$.local_permission_ref.digest') IS grant.digest)
BEGIN SELECT RAISE(ABORT,'first_pairing_completion_parent'); END;
CREATE TRIGGER first_pairing_completion_no_update BEFORE UPDATE ON first_pairing_completion
BEGIN SELECT RAISE(ABORT,'immutable_first_pairing_completion'); END;
CREATE TRIGGER first_pairing_completion_no_delete BEFORE DELETE ON first_pairing_completion
BEGIN SELECT RAISE(ABORT,'immutable_first_pairing_completion'); END;
