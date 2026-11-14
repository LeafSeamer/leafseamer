CREATE TABLE authentication_clock_evidence (
  id TEXT NOT NULL COLLATE BINARY,
  revision TEXT NOT NULL COLLATE BINARY,
  digest TEXT NOT NULL COLLATE BINARY,
  kind TEXT NOT NULL CHECK(kind IN ('policy', 'runtime', 'health')),
  body BLOB NOT NULL CHECK(length(body) <= 32768),
  PRIMARY KEY(id, revision),
  UNIQUE(id, revision, digest)
) STRICT;
CREATE TRIGGER auth_clock_evidence_no_update BEFORE UPDATE ON authentication_clock_evidence
BEGIN SELECT RAISE(ABORT, 'immutable_clock_evidence'); END;
CREATE TRIGGER auth_clock_evidence_no_delete BEFORE DELETE ON authentication_clock_evidence
BEGIN SELECT RAISE(ABORT, 'immutable_clock_evidence'); END;

CREATE TABLE authentication_clock_acquisition (
  id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
  revision TEXT NOT NULL CHECK(revision = '1'),
  digest TEXT NOT NULL COLLATE BINARY,
  service_boot_id TEXT NOT NULL REFERENCES service_boot(service_boot_id),
  clock_domain_id TEXT NOT NULL COLLATE BINARY,
  sequence TEXT NOT NULL COLLATE BINARY CHECK(length(sequence) = 20),
  parent_id TEXT REFERENCES authentication_clock_acquisition(id),
  policy_id TEXT NOT NULL, policy_revision TEXT NOT NULL, policy_digest TEXT NOT NULL,
  runtime_id TEXT NOT NULL, runtime_revision TEXT NOT NULL, runtime_digest TEXT NOT NULL,
  health_id TEXT NOT NULL, health_revision TEXT NOT NULL, health_digest TEXT NOT NULL,
  body BLOB NOT NULL CHECK(length(body) <= 32768),
  UNIQUE(service_boot_id, clock_domain_id, sequence),
  UNIQUE(service_boot_id, clock_domain_id, id),
  FOREIGN KEY(policy_id, policy_revision, policy_digest) REFERENCES authentication_clock_evidence(id, revision, digest),
  FOREIGN KEY(runtime_id, runtime_revision, runtime_digest) REFERENCES authentication_clock_evidence(id, revision, digest),
  FOREIGN KEY(health_id, health_revision, health_digest) REFERENCES authentication_clock_evidence(id, revision, digest)
) STRICT;
CREATE TRIGGER auth_clock_acquisition_no_update BEFORE UPDATE ON authentication_clock_acquisition
BEGIN SELECT RAISE(ABORT, 'immutable_clock_acquisition'); END;
CREATE TRIGGER auth_clock_acquisition_no_delete BEFORE DELETE ON authentication_clock_acquisition
BEGIN SELECT RAISE(ABORT, 'immutable_clock_acquisition'); END;

CREATE TABLE authentication_clock_head (
  service_boot_id TEXT NOT NULL,
  clock_domain_id TEXT NOT NULL COLLATE BINARY,
  acquisition_id TEXT NOT NULL,
  PRIMARY KEY(service_boot_id, clock_domain_id),
  FOREIGN KEY(service_boot_id, clock_domain_id, acquisition_id)
    REFERENCES authentication_clock_acquisition(service_boot_id, clock_domain_id, id)
) STRICT;
CREATE TRIGGER auth_clock_head_scope BEFORE UPDATE ON authentication_clock_head
WHEN NEW.service_boot_id != OLD.service_boot_id OR NEW.clock_domain_id != OLD.clock_domain_id
BEGIN SELECT RAISE(ABORT, 'immutable_clock_domain'); END;
CREATE TRIGGER auth_clock_head_no_delete BEFORE DELETE ON authentication_clock_head
BEGIN SELECT RAISE(ABORT, 'clock_history_required'); END;

CREATE TABLE authentication_clock_fault (
  service_boot_id TEXT NOT NULL REFERENCES service_boot(service_boot_id),
  clock_domain_id TEXT NOT NULL COLLATE BINARY,
  id TEXT NOT NULL UNIQUE COLLATE BINARY,
  digest TEXT NOT NULL COLLATE BINARY,
  body BLOB NOT NULL CHECK(length(body) <= 32768),
  PRIMARY KEY(service_boot_id, clock_domain_id)
) STRICT;
CREATE TRIGGER auth_clock_fault_no_update BEFORE UPDATE ON authentication_clock_fault
BEGIN SELECT RAISE(ABORT, 'clock_fault_latched'); END;
CREATE TRIGGER auth_clock_fault_no_delete BEFORE DELETE ON authentication_clock_fault
BEGIN SELECT RAISE(ABORT, 'clock_fault_latched'); END;
