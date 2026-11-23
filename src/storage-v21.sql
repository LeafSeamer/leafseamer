-- 显式升级参考采样账本，保留旧身份及业务原件。
DROP TRIGGER first_pairing_import_target;
CREATE TABLE service_state_v21 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='21'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v21 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'21',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v21 RENAME TO service_state;
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

CREATE TABLE schema_migration_v21 (
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
  (from_revision='20' AND to_revision='21'))
) STRICT;
INSERT INTO schema_migration_v21 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v21 RENAME TO schema_migration;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;

-- 与当前 Controller 参考采样账本保持相同的表和约束。
CREATE TABLE time_provider_poll_reservation (
  poll_id TEXT PRIMARY KEY NOT NULL,
  service_boot_id TEXT NOT NULL REFERENCES time_provider_runtime_binding(service_boot_id),
  source_slot TEXT NOT NULL,
  source_digest TEXT NOT NULL,
  acquisition_id TEXT NOT NULL REFERENCES authentication_clock_acquisition(id),
  reserved_bytes INTEGER NOT NULL CHECK(reserved_bytes BETWEEN 1 AND 2147483647),
  issued_tick TEXT NOT NULL CHECK(length(issued_tick)=20 AND issued_tick NOT GLOB '*[^0-9]*'),
  expires_tick TEXT NOT NULL CHECK(length(expires_tick)=20 AND expires_tick NOT GLOB '*[^0-9]*' AND expires_tick>issued_tick),
  body_jcs TEXT NOT NULL CHECK(length(body_jcs) BETWEEN 1 AND 8192),
  body_digest TEXT NOT NULL,
  UNIQUE(poll_id,body_digest),
  CHECK(json_extract(body_jcs,'$.poll_id') IS poll_id
    AND json_extract(body_jcs,'$.service_boot_id') IS service_boot_id
    AND json_extract(body_jcs,'$.source_slot') IS source_slot
    AND json_extract(body_jcs,'$.authorization.source_digest') IS source_digest
    AND json_extract(body_jcs,'$.acquisition_ref.id') IS acquisition_id
    AND json_extract(body_jcs,'$.reserved_bytes') IS reserved_bytes
    AND substr('00000000000000000000'||json_extract(body_jcs,'$.issued_tick_ns'),-20) IS issued_tick
    AND substr('00000000000000000000'||json_extract(body_jcs,'$.expires_tick_ns'),-20) IS expires_tick)
) STRICT;

CREATE INDEX time_provider_poll_source ON time_provider_poll_reservation(source_slot);

CREATE INDEX time_provider_poll_source_digest ON time_provider_poll_reservation(source_digest);

CREATE TRIGGER time_provider_poll_no_update BEFORE UPDATE ON time_provider_poll_reservation
BEGIN SELECT RAISE(ABORT,'immutable_time_provider_poll'); END;

CREATE TRIGGER time_provider_poll_no_delete BEFORE DELETE ON time_provider_poll_reservation
BEGIN SELECT RAISE(ABORT,'immutable_time_provider_poll'); END;

CREATE TABLE time_provider_poll_attempt (
  poll_id TEXT PRIMARY KEY NOT NULL,
  poll_digest TEXT NOT NULL,
  attempt_id TEXT NOT NULL UNIQUE,
  acquisition_id TEXT NOT NULL REFERENCES authentication_clock_acquisition(id),
  attempted_tick TEXT NOT NULL CHECK(length(attempted_tick)=20 AND attempted_tick NOT GLOB '*[^0-9]*'),
  body_jcs TEXT NOT NULL CHECK(length(body_jcs) BETWEEN 1 AND 8192),
  body_digest TEXT NOT NULL,
  FOREIGN KEY(poll_id,poll_digest) REFERENCES time_provider_poll_reservation(poll_id,body_digest),
  CHECK(json_extract(body_jcs,'$.poll_ref.id') IS poll_id
    AND json_extract(body_jcs,'$.poll_ref.digest') IS poll_digest
    AND json_extract(body_jcs,'$.attempt_id') IS attempt_id
    AND json_extract(body_jcs,'$.acquisition_ref.id') IS acquisition_id
    AND substr('00000000000000000000'||json_extract(body_jcs,'$.attempted_tick_ns'),-20) IS attempted_tick
    AND json_type(body_jcs,'$.valid_before_tick_ns') IS 'text'
    AND substr('00000000000000000000'||json_extract(body_jcs,'$.valid_before_tick_ns'),-20)>attempted_tick
    AND substr('00000000000000000000'||json_extract(body_jcs,'$.valid_before_tick_ns'),-20)
      <=substr('00000000000000000000'||json_extract(body_jcs,'$.expires_tick_ns'),-20))
) STRICT;

CREATE TRIGGER time_provider_poll_attempt_scope BEFORE INSERT ON time_provider_poll_attempt
WHEN NOT EXISTS (SELECT 1 FROM time_provider_poll_reservation p JOIN service_boot b USING(service_boot_id)
  JOIN authentication_clock_acquisition a ON a.id=NEW.acquisition_id AND a.service_boot_id=p.service_boot_id
  WHERE p.poll_id=NEW.poll_id AND p.body_digest=NEW.poll_digest AND b.state='started'
    AND b.rowid=(SELECT max(rowid) FROM service_boot)
    AND a.digest=json_extract(NEW.body_jcs,'$.acquisition_ref.digest')
    AND p.issued_tick<=NEW.attempted_tick AND NEW.attempted_tick<p.expires_tick
    AND json_extract(NEW.body_jcs,'$.expires_tick_ns') IS json_extract(p.body_jcs,'$.expires_tick_ns'))
BEGIN SELECT RAISE(ABORT,'time_provider_poll_attempt_scope'); END;

CREATE TRIGGER time_provider_poll_attempt_no_update BEFORE UPDATE ON time_provider_poll_attempt
BEGIN SELECT RAISE(ABORT,'immutable_time_provider_poll_attempt'); END;

CREATE TRIGGER time_provider_poll_attempt_no_delete BEFORE DELETE ON time_provider_poll_attempt
BEGIN SELECT RAISE(ABORT,'immutable_time_provider_poll_attempt'); END;

CREATE TABLE time_provider_capture_nonce (
  key_digest TEXT NOT NULL,
  nonce BLOB NOT NULL CHECK(length(nonce)=12),
  key_id TEXT NOT NULL,
  PRIMARY KEY(key_digest,nonce)
) STRICT;

CREATE INDEX time_provider_capture_nonce_key_id ON time_provider_capture_nonce(key_id);

CREATE TRIGGER time_provider_capture_nonce_key_identity BEFORE INSERT ON time_provider_capture_nonce
WHEN EXISTS (SELECT 1 FROM time_provider_capture_nonce WHERE key_id=NEW.key_id AND key_digest<>NEW.key_digest)
BEGIN SELECT RAISE(ABORT,'time_provider_capture_key_identity'); END;

CREATE TRIGGER time_provider_capture_nonce_no_update BEFORE UPDATE ON time_provider_capture_nonce
BEGIN SELECT RAISE(ABORT,'immutable_time_provider_capture_nonce'); END;

CREATE TRIGGER time_provider_capture_nonce_no_delete BEFORE DELETE ON time_provider_capture_nonce
BEGIN SELECT RAISE(ABORT,'immutable_time_provider_capture_nonce'); END;

CREATE UNIQUE INDEX time_provider_capture_attempt_ref ON time_provider_poll_attempt(poll_id,attempt_id,poll_digest);

CREATE TABLE time_provider_capture (
  poll_id TEXT PRIMARY KEY NOT NULL,
  attempt_id TEXT NOT NULL,
  poll_digest TEXT NOT NULL,
  capture_id TEXT NOT NULL UNIQUE,
  key_id TEXT NOT NULL,
  key_digest TEXT NOT NULL,
  nonce BLOB NOT NULL CHECK(length(nonce)=12),
  tag BLOB NOT NULL CHECK(length(tag)=16),
  ciphertext BLOB NOT NULL CHECK(length(ciphertext) BETWEEN 1 AND 98304),
  body_jcs TEXT NOT NULL CHECK(length(body_jcs) BETWEEN 1 AND 8192),
  body_digest TEXT NOT NULL,
  UNIQUE(key_digest,nonce),
  FOREIGN KEY(poll_id,attempt_id,poll_digest) REFERENCES time_provider_poll_attempt(poll_id,attempt_id,poll_digest),
  FOREIGN KEY(key_digest,nonce) REFERENCES time_provider_capture_nonce(key_digest,nonce),
  CHECK(json_extract(body_jcs,'$.format') IS 'leafseamer-time-provider-capture/1'
    AND json_extract(body_jcs,'$.poll_ref.id') IS poll_id
    AND json_extract(body_jcs,'$.poll_ref.digest') IS poll_digest
    AND json_extract(body_jcs,'$.attempt_id') IS attempt_id
    AND json_extract(body_jcs,'$.capture_id') IS capture_id
    AND json_extract(body_jcs,'$.key_id') IS key_id
    AND json_extract(body_jcs,'$.plain_bytes') IS length(ciphertext))
) STRICT;

CREATE TRIGGER time_provider_capture_scope BEFORE INSERT ON time_provider_capture
WHEN NOT EXISTS (
  SELECT 1 FROM time_provider_poll_reservation p JOIN time_provider_poll_attempt a USING(poll_id)
  JOIN service_boot b USING(service_boot_id) JOIN time_provider_capture_nonce n
    ON n.key_digest=NEW.key_digest AND n.nonce=NEW.nonce AND n.key_id=NEW.key_id
  WHERE p.poll_id=NEW.poll_id AND b.state='started' AND b.rowid=(SELECT max(rowid) FROM service_boot)
    AND length(CAST(NEW.body_jcs AS BLOB))+length(NEW.ciphertext)+length(NEW.nonce)+length(NEW.tag)<=p.reserved_bytes
    AND json_extract(NEW.body_jcs,'$.runtime_digest') IS json_extract(p.body_jcs,'$.runtime_digest')
    AND json_extract(NEW.body_jcs,'$.service_id') IS b.service_id
    AND json_extract(NEW.body_jcs,'$.database_instance_id') IS b.database_instance_id
    AND substr('00000000000000000000'||json_extract(NEW.body_jcs,'$.m1_tick_ns'),-20)>=a.attempted_tick
    AND substr('00000000000000000000'||json_extract(NEW.body_jcs,'$.m4_tick_ns'),-20)
      >=substr('00000000000000000000'||json_extract(NEW.body_jcs,'$.m1_tick_ns'),-20)
    AND substr('00000000000000000000'||json_extract(NEW.body_jcs,'$.m4_tick_ns'),-20)
      <substr('00000000000000000000'||json_extract(a.body_jcs,'$.valid_before_tick_ns'),-20))
BEGIN SELECT RAISE(ABORT,'time_provider_capture_scope'); END;

CREATE TRIGGER time_provider_capture_no_update BEFORE UPDATE ON time_provider_capture
BEGIN SELECT RAISE(ABORT,'immutable_time_provider_capture'); END;

CREATE TRIGGER time_provider_capture_no_delete BEFORE DELETE ON time_provider_capture
BEGIN SELECT RAISE(ABORT,'immutable_time_provider_capture'); END;

CREATE UNIQUE INDEX time_provider_release_capture_ref ON time_provider_capture(poll_id,capture_id,body_digest);

CREATE TABLE time_provider_poll_release (
  poll_id TEXT PRIMARY KEY NOT NULL,
  capture_id TEXT NOT NULL,
  capture_digest TEXT NOT NULL,
  next_allowed_tick TEXT NOT NULL CHECK(length(next_allowed_tick)=20 AND next_allowed_tick NOT GLOB '*[^0-9]*'),
  confirmation_jcs TEXT NOT NULL CHECK(length(confirmation_jcs) BETWEEN 1 AND 2048),
  confirmation_digest TEXT NOT NULL,
  FOREIGN KEY(poll_id,capture_id,capture_digest) REFERENCES time_provider_capture(poll_id,capture_id,body_digest),
  CHECK(json_extract(confirmation_jcs,'$.type') IS 'POLL_STORED_ACK'
    AND json_extract(confirmation_jcs,'$.receipt.poll_ref.id') IS poll_id
    AND json_extract(confirmation_jcs,'$.receipt.capture_ref.id') IS capture_id
    AND json_extract(confirmation_jcs,'$.receipt.capture_ref.digest') IS capture_digest)
) STRICT;

CREATE TRIGGER time_provider_release_scope BEFORE INSERT ON time_provider_poll_release
WHEN NOT EXISTS (
  SELECT 1 FROM time_provider_capture c JOIN time_provider_poll_reservation p USING(poll_id)
    JOIN service_boot b USING(service_boot_id)
  WHERE c.poll_id=NEW.poll_id AND c.capture_id=NEW.capture_id AND c.body_digest=NEW.capture_digest
    AND b.state='started' AND b.rowid=(SELECT max(rowid) FROM service_boot)
    AND json_extract(NEW.confirmation_jcs,'$.binding_digest') IS json_extract(p.body_jcs,'$.runtime_digest')
    AND json_extract(NEW.confirmation_jcs,'$.receipt.attempt_id') IS c.attempt_id
    AND json_extract(NEW.confirmation_jcs,'$.receipt.poll_ref.digest') IS c.poll_digest
    AND json_extract(NEW.confirmation_jcs,'$.receipt.m4_tick_ns') IS json_extract(c.body_jcs,'$.m4_tick_ns')
    AND NEW.next_allowed_tick>substr('00000000000000000000'||json_extract(c.body_jcs,'$.m4_tick_ns'),-20))
BEGIN SELECT RAISE(ABORT,'time_provider_release_scope'); END;

CREATE TRIGGER time_provider_release_no_update BEFORE UPDATE ON time_provider_poll_release
BEGIN SELECT RAISE(ABORT,'immutable_time_provider_poll_release'); END;

CREATE TRIGGER time_provider_release_no_delete BEFORE DELETE ON time_provider_poll_release
BEGIN SELECT RAISE(ABORT,'immutable_time_provider_poll_release'); END;

CREATE TABLE time_provider_source_recovery (
  receiver_boot_id TEXT NOT NULL REFERENCES time_provider_runtime_binding(service_boot_id),
  source_digest TEXT NOT NULL,
  predecessor_poll_id TEXT NOT NULL REFERENCES time_provider_poll_reservation(poll_id),
  received_tick TEXT NOT NULL CHECK(length(received_tick)=20 AND received_tick NOT GLOB '*[^0-9]*'),
  wait_until_tick TEXT CHECK(length(wait_until_tick)=20 AND wait_until_tick NOT GLOB '*[^0-9]*' AND wait_until_tick>received_tick),
  body_jcs TEXT NOT NULL CHECK(length(body_jcs) BETWEEN 1 AND 32768),
  body_digest TEXT NOT NULL,
  PRIMARY KEY(receiver_boot_id,source_digest),
  CHECK(json_extract(body_jcs,'$.format') IS 'leafseamer-time-provider-source-recovery/1'
    AND json_extract(body_jcs,'$.receiver_boot_id') IS receiver_boot_id
    AND json_extract(body_jcs,'$.source_digest') IS source_digest
    AND json_extract(body_jcs,'$.predecessor_poll_id') IS predecessor_poll_id
    AND json_extract(body_jcs,'$.observation.format') IS 'leafseamer-time-provider-retirement-observation/1'
    AND json_extract(body_jcs,'$.observation.record.provider_exit.active_processes') IS 0
    AND json_extract(body_jcs,'$.observation.record.core_exit.active_processes') IS 0
    AND substr('00000000000000000000'||json_extract(body_jcs,'$.observation.received_tick_ns'),-20) IS received_tick
    AND substr('00000000000000000000'||json_extract(body_jcs,'$.wait_until_tick_ns'),-20) IS wait_until_tick)
) STRICT;

CREATE INDEX time_provider_recovery_predecessor ON time_provider_source_recovery(predecessor_poll_id);

CREATE TRIGGER time_provider_source_recovery_no_update BEFORE UPDATE ON time_provider_source_recovery
BEGIN SELECT RAISE(ABORT,'immutable_time_provider_source_recovery'); END;

CREATE TRIGGER time_provider_source_recovery_no_delete BEFORE DELETE ON time_provider_source_recovery
BEGIN SELECT RAISE(ABORT,'immutable_time_provider_source_recovery'); END;

CREATE TABLE time_provider_poll_control (
  poll_id TEXT PRIMARY KEY NOT NULL REFERENCES time_provider_poll_attempt(poll_id),
  source_digest TEXT NOT NULL,
  attempt_id TEXT NOT NULL UNIQUE,
  received_tick TEXT NOT NULL CHECK(length(received_tick)=20 AND received_tick NOT GLOB '*[^0-9]*'),
  wait_ns TEXT NOT NULL CHECK(length(wait_ns)=20 AND wait_ns NOT GLOB '*[^0-9]*' AND wait_ns<='00000003600000000000'),
  next_allowed_tick TEXT CHECK(length(next_allowed_tick)=20 AND next_allowed_tick NOT GLOB '*[^0-9]*' AND next_allowed_tick>received_tick),
  body_jcs TEXT NOT NULL CHECK(length(body_jcs) BETWEEN 1 AND 8192),
  body_digest TEXT NOT NULL,
  UNIQUE(poll_id,body_digest),
  CHECK((wait_ns='00000000000000000000') IS (next_allowed_tick IS NULL)
    AND json_extract(body_jcs,'$.format') IS 'leafseamer-time-provider-poll-control/1'
    AND json_extract(body_jcs,'$.poll_ref.id') IS poll_id
    AND json_extract(body_jcs,'$.source_digest') IS source_digest
    AND json_extract(body_jcs,'$.attempt_id') IS attempt_id
    AND substr('00000000000000000000'||json_extract(body_jcs,'$.control.wait_ns'),-20) IS wait_ns
    AND (next_allowed_tick IS NULL OR json_extract(body_jcs,'$.control.code') IS 1380013125)
    AND substr('00000000000000000000'||json_extract(body_jcs,'$.received_tick_ns'),-20) IS received_tick
    AND substr('00000000000000000000'||json_extract(body_jcs,'$.next_allowed_tick_ns'),-20) IS next_allowed_tick)
) STRICT;

CREATE INDEX time_provider_poll_control_source ON time_provider_poll_control(source_digest);

CREATE TRIGGER time_provider_poll_control_parent BEFORE INSERT ON time_provider_poll_control
WHEN NOT EXISTS (SELECT 1 FROM time_provider_poll_reservation p JOIN time_provider_poll_attempt a USING(poll_id)
  JOIN service_boot b USING(service_boot_id)
  WHERE p.poll_id=NEW.poll_id AND p.source_digest=NEW.source_digest AND a.attempt_id=NEW.attempt_id
    AND b.state='started' AND b.rowid=(SELECT max(rowid) FROM service_boot)
    AND p.service_boot_id IS json_extract(NEW.body_jcs,'$.service_boot_id')
    AND p.body_digest IS json_extract(NEW.body_jcs,'$.poll_ref.digest')
    AND json_extract(p.body_jcs,'$.runtime_digest') IS json_extract(NEW.body_jcs,'$.runtime_digest')
    AND json_type(p.body_jcs,'$.effective_poll_interval_ns') IS 'text'
    AND a.attempted_tick<=substr('00000000000000000000'||json_extract(NEW.body_jcs,'$.control.observed_tick_ns'),-20)
    AND substr('00000000000000000000'||json_extract(NEW.body_jcs,'$.control.observed_tick_ns'),-20)<=NEW.received_tick
    AND NEW.received_tick<substr('00000000000000000000'||json_extract(a.body_jcs,'$.valid_before_tick_ns'),-20))
 OR EXISTS (SELECT 1 FROM time_provider_capture c WHERE c.poll_id=NEW.poll_id)
BEGIN SELECT RAISE(ABORT,'time_provider_poll_control_parent'); END;

CREATE TRIGGER time_provider_capture_control_exclusive BEFORE INSERT ON time_provider_capture
WHEN EXISTS (SELECT 1 FROM time_provider_poll_control c WHERE c.poll_id=NEW.poll_id)
BEGIN SELECT RAISE(ABORT,'time_provider_poll_control_already_stored'); END;

CREATE TRIGGER time_provider_poll_control_no_update BEFORE UPDATE ON time_provider_poll_control
BEGIN SELECT RAISE(ABORT,'immutable_time_provider_poll_control'); END;

CREATE TRIGGER time_provider_poll_control_no_delete BEFORE DELETE ON time_provider_poll_control
BEGIN SELECT RAISE(ABORT,'immutable_time_provider_poll_control'); END;

CREATE TABLE time_provider_control_release (
  poll_id TEXT PRIMARY KEY NOT NULL,
  control_digest TEXT NOT NULL,
  next_allowed_tick TEXT,
  confirmation_jcs TEXT NOT NULL CHECK(length(confirmation_jcs) BETWEEN 1 AND 2048),
  confirmation_digest TEXT NOT NULL,
  FOREIGN KEY(poll_id,control_digest) REFERENCES time_provider_poll_control(poll_id,body_digest),
  CHECK(json_extract(confirmation_jcs,'$.type') IS 'POLL_CONTROL_STORED_ACK'
    AND json_extract(confirmation_jcs,'$.receipt.poll_ref.id') IS poll_id
    AND json_extract(confirmation_jcs,'$.receipt.control_ref.id') IS poll_id
    AND json_extract(confirmation_jcs,'$.receipt.control_ref.digest') IS control_digest)
) STRICT;

CREATE TRIGGER time_provider_control_release_parent BEFORE INSERT ON time_provider_control_release
WHEN NOT EXISTS (SELECT 1 FROM time_provider_poll_control c JOIN time_provider_poll_reservation p USING(poll_id)
  JOIN service_boot b USING(service_boot_id)
  WHERE c.poll_id=NEW.poll_id AND c.body_digest=NEW.control_digest AND c.next_allowed_tick IS NEW.next_allowed_tick
    AND b.state='started' AND b.rowid=(SELECT max(rowid) FROM service_boot)
    AND c.attempt_id IS json_extract(NEW.confirmation_jcs,'$.receipt.attempt_id')
    AND p.body_digest IS json_extract(NEW.confirmation_jcs,'$.receipt.poll_ref.digest')
    AND json_extract(p.body_jcs,'$.runtime_digest') IS json_extract(NEW.confirmation_jcs,'$.binding_digest'))
BEGIN SELECT RAISE(ABORT,'time_provider_control_release_parent'); END;

CREATE TRIGGER time_provider_control_release_no_update BEFORE UPDATE ON time_provider_control_release
BEGIN SELECT RAISE(ABORT,'immutable_time_provider_control_release'); END;

CREATE TRIGGER time_provider_control_release_no_delete BEFORE DELETE ON time_provider_control_release
BEGIN SELECT RAISE(ABORT,'immutable_time_provider_control_release'); END;

CREATE TRIGGER time_provider_source_recovery_parent BEFORE INSERT ON time_provider_source_recovery
WHEN NOT EXISTS (
  SELECT 1 FROM time_provider_poll_reservation p JOIN time_provider_runtime_binding old USING(service_boot_id)
  JOIN time_provider_runtime_binding current ON current.service_boot_id=NEW.receiver_boot_id
  JOIN service_boot b ON b.service_boot_id=current.service_boot_id
  WHERE p.poll_id=NEW.predecessor_poll_id AND p.source_digest=NEW.source_digest
    AND p.rowid=(SELECT max(rowid) FROM time_provider_poll_reservation WHERE source_digest=NEW.source_digest)
    AND p.service_boot_id<>NEW.receiver_boot_id AND b.state='started' AND b.rowid=(SELECT max(rowid) FROM service_boot)
    AND json_extract(NEW.body_jcs,'$.predecessor_digest') IS p.body_digest
    AND json_extract(NEW.body_jcs,'$.old_runtime_digest') IS old.body_digest
    AND json_extract(NEW.body_jcs,'$.observation.record.launch_intent.launch_id') IS old.launch_id
    AND json_extract(NEW.body_jcs,'$.observation.record.admission_receipt.receipt_id') IS old.receipt_id
    AND json_extract(NEW.body_jcs,'$.observation.record.core_receipt_id') IS json_extract(old.body_jcs,'$.core_receipt_id')
    AND json_extract(NEW.body_jcs,'$.observation.receiving_domain_ref.clock_domain_id') IS json_extract(current.body_jcs,'$.clock_domain_descriptor.clock_domain_id')
    AND json_extract(NEW.body_jcs,'$.observation.receiving_domain_ref.descriptor_digest') IS json_extract(current.body_jcs,'$.clock_domain_descriptor.descriptor_digest')
    AND (NEW.wait_until_tick IS NULL OR (json_type(p.body_jcs,'$.authorization.minimum_poll_interval_ns') IS 'text'
      AND (NOT EXISTS (SELECT 1 FROM time_provider_poll_attempt a WHERE a.poll_id=p.poll_id)
        OR EXISTS (SELECT 1 FROM time_provider_capture c WHERE c.poll_id=p.poll_id)
        OR EXISTS (SELECT 1 FROM time_provider_poll_control z WHERE z.poll_id=p.poll_id AND z.next_allowed_tick IS NOT NULL)))))
BEGIN SELECT RAISE(ABORT,'time_provider_source_recovery_parent'); END;

CREATE TRIGGER time_provider_poll_runtime BEFORE INSERT ON time_provider_poll_reservation
WHEN NOT EXISTS (SELECT 1 FROM time_provider_runtime_binding r JOIN service_boot b USING(service_boot_id)
  JOIN authentication_clock_acquisition a ON a.id=NEW.acquisition_id AND a.service_boot_id=r.service_boot_id
  WHERE r.service_boot_id=NEW.service_boot_id AND b.state='started'
    AND b.rowid=(SELECT max(rowid) FROM service_boot)
    AND a.digest=json_extract(NEW.body_jcs,'$.acquisition_ref.digest')
    AND r.body_digest=json_extract(NEW.body_jcs,'$.runtime_digest'))
 OR json_type(NEW.body_jcs,'$.effective_poll_interval_ns') IS NOT 'text'
 OR substr('00000000000000000000'||json_extract(NEW.body_jcs,'$.effective_poll_interval_ns'),-20)
    <substr('00000000000000000000'||json_extract(NEW.body_jcs,'$.authorization.minimum_poll_interval_ns'),-20)
 OR EXISTS (SELECT 1 FROM time_provider_poll_control c WHERE c.source_digest=NEW.source_digest
    AND (c.next_allowed_tick IS NULL OR substr('00000000000000000000'||json_extract(NEW.body_jcs,'$.effective_poll_interval_ns'),-20)<c.wait_ns))
 OR EXISTS (SELECT 1 FROM time_provider_poll_reservation p WHERE p.source_slot=NEW.source_slot AND p.source_digest<>NEW.source_digest)
 OR EXISTS (SELECT 1 FROM time_provider_poll_reservation p LEFT JOIN time_provider_poll_release x USING(poll_id)
   LEFT JOIN time_provider_control_release z USING(poll_id)
   LEFT JOIN time_provider_source_recovery q ON q.predecessor_poll_id=p.poll_id AND q.receiver_boot_id=NEW.service_boot_id
   WHERE p.rowid=(SELECT max(rowid) FROM time_provider_poll_reservation WHERE source_digest=NEW.source_digest)
     AND ((p.service_boot_id=NEW.service_boot_id AND (coalesce(x.next_allowed_tick,z.next_allowed_tick) IS NULL OR NEW.issued_tick<coalesce(x.next_allowed_tick,z.next_allowed_tick)))
       OR (p.service_boot_id<>NEW.service_boot_id AND (q.wait_until_tick IS NULL OR NEW.issued_tick<q.wait_until_tick))))
BEGIN SELECT RAISE(ABORT,'time_provider_poll_runtime_or_source'); END;

CREATE TABLE time_provider_reference_anchor (
  anchor_id TEXT PRIMARY KEY NOT NULL,
  service_boot_id TEXT NOT NULL REFERENCES time_provider_runtime_binding(service_boot_id),
  poll_id TEXT NOT NULL UNIQUE,
  capture_id TEXT NOT NULL,
  capture_digest TEXT NOT NULL,
  m4_tick TEXT NOT NULL CHECK(length(m4_tick)=20 AND m4_tick NOT GLOB '*[^0-9]*'),
  predecessor_digest TEXT UNIQUE REFERENCES time_provider_reference_anchor(body_digest),
  body_jcs TEXT NOT NULL CHECK(length(body_jcs) BETWEEN 1 AND 16384),
  body_digest TEXT NOT NULL UNIQUE,
  FOREIGN KEY(poll_id,capture_id,capture_digest) REFERENCES time_provider_capture(poll_id,capture_id,body_digest),
  CHECK(json_extract(body_jcs,'$.format') IS 'leafseamer-reference-anchor/1'
    AND json_extract(body_jcs,'$.model') IS 'reference-bound/1'
    AND json_extract(body_jcs,'$.anchor_id') IS anchor_id
    AND json_extract(body_jcs,'$.service_boot_id') IS service_boot_id
    AND json_extract(body_jcs,'$.poll_ref.id') IS poll_id
    AND json_extract(body_jcs,'$.capture_ref.id') IS capture_id
    AND json_extract(body_jcs,'$.capture_ref.digest') IS capture_digest
    AND json_extract(body_jcs,'$.previous_anchor_digest') IS predecessor_digest
    AND substr('00000000000000000000'||json_extract(body_jcs,'$.m4_tick_ns'),-20) IS m4_tick)
) STRICT;

CREATE INDEX time_provider_reference_anchor_boot ON time_provider_reference_anchor(service_boot_id);

CREATE TRIGGER time_provider_reference_anchor_parent BEFORE INSERT ON time_provider_reference_anchor
WHEN NOT EXISTS (SELECT 1 FROM time_provider_capture c JOIN time_provider_poll_reservation p USING(poll_id)
  JOIN time_provider_poll_attempt a USING(poll_id) JOIN service_boot b USING(service_boot_id)
  WHERE c.poll_id=NEW.poll_id AND c.capture_id=NEW.capture_id AND c.body_digest=NEW.capture_digest
    AND b.service_boot_id=NEW.service_boot_id AND b.state='started' AND b.rowid=(SELECT max(rowid) FROM service_boot)
    AND json_extract(NEW.body_jcs,'$.runtime_digest') IS json_extract(p.body_jcs,'$.runtime_digest')
    AND json_extract(NEW.body_jcs,'$.source_slot') IS p.source_slot
    AND json_extract(NEW.body_jcs,'$.poll_ref.digest') IS c.poll_digest
    AND json_extract(NEW.body_jcs,'$.attempt_id') IS c.attempt_id
    AND json_extract(NEW.body_jcs,'$.authorization') IS json_extract(p.body_jcs,'$.authorization')
    AND json_extract(NEW.body_jcs,'$.plaintext_digest') IS json_extract(c.body_jcs,'$.plaintext_digest')
    AND json_extract(NEW.body_jcs,'$.m1_tick_ns') IS json_extract(c.body_jcs,'$.m1_tick_ns')
    AND json_extract(NEW.body_jcs,'$.m4_tick_ns') IS json_extract(c.body_jcs,'$.m4_tick_ns')
    AND json_type(NEW.body_jcs,'$.published_tick_ns') IS 'text'
    AND substr('00000000000000000000'||json_extract(NEW.body_jcs,'$.published_tick_ns'),-20)>=NEW.m4_tick
    AND substr('00000000000000000000'||json_extract(NEW.body_jcs,'$.published_tick_ns'),-20)
      <substr('00000000000000000000'||json_extract(a.body_jcs,'$.valid_before_tick_ns'),-20))
 OR NEW.predecessor_digest IS NOT (SELECT body_digest FROM time_provider_reference_anchor
   WHERE service_boot_id=NEW.service_boot_id ORDER BY rowid DESC LIMIT 1)
 OR EXISTS (SELECT 1 FROM time_provider_reference_anchor p WHERE p.body_digest=NEW.predecessor_digest
   AND (p.m4_tick>=NEW.m4_tick OR json_extract(NEW.body_jcs,'$.approval') IS NOT json_extract(p.body_jcs,'$.approval')
     OR json_extract(NEW.body_jcs,'$.authorization') IS NOT json_extract(p.body_jcs,'$.authorization')))
BEGIN SELECT RAISE(ABORT,'time_provider_reference_anchor_parent'); END;

CREATE TRIGGER time_provider_reference_anchor_no_update BEFORE UPDATE ON time_provider_reference_anchor
BEGIN SELECT RAISE(ABORT,'immutable_time_provider_reference_anchor'); END;

CREATE TRIGGER time_provider_reference_anchor_no_delete BEFORE DELETE ON time_provider_reference_anchor
BEGIN SELECT RAISE(ABORT,'immutable_time_provider_reference_anchor'); END;
