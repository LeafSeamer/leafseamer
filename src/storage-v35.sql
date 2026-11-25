-- 受监督 Linux 授时来源切换。
DROP TRIGGER first_pairing_import_target;
CREATE TABLE service_state_v35 (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE, database_instance_id TEXT NOT NULL UNIQUE, request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='35'), maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'), UNIQUE(service_id,database_instance_id)
) STRICT;
INSERT INTO service_state_v35 SELECT singleton,authority,service_id,database_instance_id,request_epoch,'35',maintenance_state FROM service_state;
DROP TABLE service_state;
ALTER TABLE service_state_v35 RENAME TO service_state;
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

CREATE TABLE schema_migration_v35 (
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
  (from_revision='24' AND to_revision='25') OR (from_revision='25' AND to_revision='26') OR (from_revision='26' AND to_revision='27') OR (from_revision='27' AND to_revision='28') OR (from_revision='28' AND to_revision='29') OR (from_revision='29' AND to_revision='30') OR (from_revision='30' AND to_revision='31') OR (from_revision='31' AND to_revision='32') OR (from_revision='32' AND to_revision='33') OR
  (from_revision='33' AND to_revision='34') OR (from_revision='34' AND to_revision='35'))
) STRICT;
INSERT INTO schema_migration_v35 SELECT * FROM schema_migration;
DROP TABLE schema_migration;
ALTER TABLE schema_migration_v35 RENAME TO schema_migration;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;

DROP TRIGGER time_provider_source_recovery_parent;
CREATE TRIGGER time_provider_source_recovery_parent BEFORE INSERT ON time_provider_source_recovery
WHEN NOT EXISTS (
  SELECT 1 FROM time_provider_poll_reservation p JOIN time_provider_runtime_binding old USING(service_boot_id)
  JOIN time_provider_runtime_binding current ON current.service_boot_id=NEW.receiver_boot_id
  JOIN service_boot b ON b.service_boot_id=current.service_boot_id
  WHERE p.poll_id=NEW.predecessor_poll_id
    AND (
      (json_type(NEW.body_jcs,'$.transition') IS NULL AND p.source_digest=NEW.source_digest
        AND p.rowid=(SELECT max(x.rowid) FROM time_provider_poll_reservation x WHERE x.source_digest=NEW.source_digest))
      OR (json_type(NEW.body_jcs,'$.transition') IS 'object' AND p.source_digest<>NEW.source_digest
        AND json_extract(old.body_jcs,'$.format') IS 'leafseamer-time-provider-runtime-binding/2'
        AND json_extract(current.body_jcs,'$.format') IS 'leafseamer-time-provider-runtime-binding/2'
        AND json_extract(NEW.body_jcs,'$.observation.record.format') IS 'leafseamer-linux-reference-retired/1'
        AND json_extract(NEW.body_jcs,'$.transition.prior_source_digest') IS p.source_digest
        AND json_extract(NEW.body_jcs,'$.transition.source_slot') IS p.source_slot
        AND json_extract(NEW.body_jcs,'$.transition.authorization.source_slot') IS p.source_slot
        AND json_extract(NEW.body_jcs,'$.transition.authorization.source_digest') IS NEW.source_digest
        AND p.rowid=(SELECT max(x.rowid) FROM time_provider_poll_reservation x
          WHERE x.source_slot=p.source_slot OR x.source_digest=NEW.source_digest)))
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

DROP TRIGGER time_provider_poll_runtime;
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
 OR EXISTS (SELECT 1 FROM time_provider_poll_reservation p JOIN time_provider_runtime_binding r ON r.service_boot_id=NEW.service_boot_id
    WHERE (json_extract(r.body_jcs,'$.format') IS NOT 'leafseamer-time-provider-runtime-binding/2'
          AND p.source_slot=NEW.source_slot AND p.source_digest<>NEW.source_digest)
       OR (json_extract(r.body_jcs,'$.format') IS 'leafseamer-time-provider-runtime-binding/2'
          AND p.source_digest=NEW.source_digest AND p.source_slot<>NEW.source_slot))
 OR EXISTS (SELECT 1 FROM time_provider_poll_reservation p LEFT JOIN time_provider_poll_release x USING(poll_id)
   LEFT JOIN time_provider_control_release z USING(poll_id)
   LEFT JOIN time_provider_source_recovery q ON q.predecessor_poll_id=p.poll_id AND q.receiver_boot_id=NEW.service_boot_id
     AND q.source_digest=NEW.source_digest
   JOIN time_provider_runtime_binding r ON r.service_boot_id=NEW.service_boot_id
   WHERE p.rowid=(SELECT max(last.rowid) FROM time_provider_poll_reservation last WHERE last.source_digest=NEW.source_digest
     OR (json_extract(r.body_jcs,'$.format') IS 'leafseamer-time-provider-runtime-binding/2' AND last.source_slot=NEW.source_slot))
     AND ((p.service_boot_id=NEW.service_boot_id AND (p.source_digest<>NEW.source_digest
        OR coalesce(x.next_allowed_tick,z.next_allowed_tick) IS NULL OR NEW.issued_tick<coalesce(x.next_allowed_tick,z.next_allowed_tick)))
       OR (p.service_boot_id<>NEW.service_boot_id AND (q.wait_until_tick IS NULL OR NEW.issued_tick<q.wait_until_tick
         OR (p.source_digest<>NEW.source_digest AND
           (json_extract(q.body_jcs,'$.transition.authorization') IS NOT json_extract(NEW.body_jcs,'$.authorization')
            OR json_extract(q.body_jcs,'$.transition.source_slot') IS NOT NEW.source_slot))))))
BEGIN SELECT RAISE(ABORT,'time_provider_poll_runtime_or_source'); END;
