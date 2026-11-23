-- 显式升级监督采集账本。旧身份、启动与业务原件逐行保留。
DROP TRIGGER first_pairing_import_target;
CREATE TABLE service_state_v20 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='20'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v20 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'20',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v20 RENAME TO service_state;
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

CREATE TABLE schema_migration_v20 (
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
  (from_revision='18' AND to_revision='19') OR (from_revision='19' AND to_revision='20'))
) STRICT;
INSERT INTO schema_migration_v20 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v20 RENAME TO schema_migration;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;

CREATE TABLE core_runtime_binding (
 service_boot_id TEXT PRIMARY KEY NOT NULL REFERENCES service_boot(service_boot_id),
 receipt_id TEXT NOT NULL UNIQUE, bootstrap_id TEXT NOT NULL UNIQUE, member_boot_id TEXT NOT NULL UNIQUE,
 intent_jcs TEXT NOT NULL CHECK(json_valid(intent_jcs) AND length(CAST(intent_jcs AS BLOB))<=8192),
 descriptor_jcs TEXT NOT NULL CHECK(json_valid(descriptor_jcs) AND length(CAST(descriptor_jcs AS BLOB))<=8192),
 receipt_jcs TEXT NOT NULL CHECK(json_valid(receipt_jcs) AND length(CAST(receipt_jcs AS BLOB))<=8192),
 policy_jcs TEXT NOT NULL CHECK(json_valid(policy_jcs) AND length(CAST(policy_jcs AS BLOB))<=8192),
 CHECK(json_extract(intent_jcs,'$.role') IS 'core' AND json_extract(receipt_jcs,'$.gate_state') IS 'closed'
  AND json_extract(receipt_jcs,'$.receipt_id') IS receipt_id AND json_extract(receipt_jcs,'$.bootstrap_id') IS bootstrap_id
  AND json_extract(receipt_jcs,'$.member_boot_id') IS member_boot_id)
) STRICT;
CREATE TRIGGER core_runtime_parent BEFORE INSERT ON core_runtime_binding
WHEN NOT EXISTS (SELECT 1 FROM service_boot b WHERE b.service_boot_id=NEW.service_boot_id AND b.state='started'
 AND b.service_id IS json_extract(NEW.intent_jcs,'$.service_id') AND b.service_id IS json_extract(NEW.policy_jcs,'$.service_id')
 AND b.database_instance_id IS json_extract(NEW.policy_jcs,'$.database_instance_id'))
BEGIN SELECT RAISE(ABORT,'core_runtime_boot'); END;
CREATE TRIGGER core_runtime_no_update BEFORE UPDATE ON core_runtime_binding BEGIN SELECT RAISE(ABORT,'immutable_core_runtime'); END;
CREATE TRIGGER core_runtime_no_delete BEFORE DELETE ON core_runtime_binding BEGIN SELECT RAISE(ABORT,'immutable_core_runtime'); END;

CREATE TABLE time_provider_runtime_binding (
 service_boot_id TEXT PRIMARY KEY NOT NULL REFERENCES core_runtime_binding(service_boot_id),
 runtime_id TEXT NOT NULL UNIQUE, launch_id TEXT NOT NULL UNIQUE, receipt_id TEXT NOT NULL UNIQUE,
 bootstrap_id TEXT NOT NULL UNIQUE, member_boot_id TEXT NOT NULL UNIQUE,
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=32768),
 body_digest TEXT NOT NULL UNIQUE CHECK(length(body_digest)=71),
 CHECK(json_extract(body_jcs,'$.format') IS 'leafseamer-time-provider-runtime-binding/1'
  AND json_extract(body_jcs,'$.service_boot_id') IS service_boot_id AND json_extract(body_jcs,'$.runtime_binding_id') IS runtime_id
  AND json_extract(body_jcs,'$.launch_intent.launch_id') IS launch_id AND json_extract(body_jcs,'$.launch_intent.role') IS 'time_provider'
  AND json_extract(body_jcs,'$.admission_receipt.receipt_id') IS receipt_id AND json_extract(body_jcs,'$.admission_receipt.bootstrap_id') IS bootstrap_id
  AND json_extract(body_jcs,'$.admission_receipt.member_boot_id') IS member_boot_id AND json_extract(body_jcs,'$.gate_state') IS 'closed')
) STRICT;
CREATE TRIGGER time_provider_runtime_parent BEFORE INSERT ON time_provider_runtime_binding
WHEN NOT EXISTS (SELECT 1 FROM service_boot b JOIN core_runtime_binding c USING(service_boot_id)
 WHERE b.service_boot_id=NEW.service_boot_id AND b.state='started' AND c.receipt_id IS json_extract(NEW.body_jcs,'$.core_receipt_id')
 AND b.service_id IS json_extract(NEW.body_jcs,'$.service_id') AND b.database_instance_id IS json_extract(NEW.body_jcs,'$.database_instance_id')
 AND c.descriptor_jcs IS json_extract(NEW.body_jcs,'$.clock_domain_descriptor'))
BEGIN SELECT RAISE(ABORT,'time_provider_runtime_boot'); END;
CREATE TRIGGER time_provider_runtime_no_update BEFORE UPDATE ON time_provider_runtime_binding BEGIN SELECT RAISE(ABORT,'immutable_time_provider_runtime'); END;
CREATE TRIGGER time_provider_runtime_no_delete BEFORE DELETE ON time_provider_runtime_binding BEGIN SELECT RAISE(ABORT,'immutable_time_provider_runtime'); END;
