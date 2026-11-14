CREATE TABLE peer_pairing_manager (
 principal_id TEXT PRIMARY KEY NOT NULL,
 issuer_id TEXT NOT NULL,
 issuer_epoch TEXT NOT NULL,
 body_jcs TEXT NOT NULL,
 body_digest TEXT NOT NULL
) STRICT;
CREATE TRIGGER peer_pairing_manager_no_update BEFORE UPDATE ON peer_pairing_manager BEGIN SELECT RAISE(ABORT,'immutable_remote_pairing_manager'); END;
CREATE TRIGGER peer_pairing_manager_no_delete BEFORE DELETE ON peer_pairing_manager BEGIN SELECT RAISE(ABORT,'immutable_remote_pairing_manager'); END;
CREATE TABLE peer_pairing_revision (
 binding_id TEXT NOT NULL COLLATE BINARY,
 revision INTEGER NOT NULL CHECK(revision>=1),
 attempt_id TEXT NOT NULL COLLATE BINARY,
 peer_service_id TEXT NOT NULL COLLATE BINARY,
 service_boot_id TEXT NOT NULL COLLATE BINARY,
 state TEXT NOT NULL CHECK(state IN ('staged','proving','proved','activated','rejected','expired','revoked')),
 transcript_digest TEXT NOT NULL COLLATE BINARY,
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs)),
 body_digest TEXT NOT NULL COLLATE BINARY,
 PRIMARY KEY(binding_id,revision),
 FOREIGN KEY(service_boot_id) REFERENCES service_boot(service_boot_id)
) STRICT;
CREATE TABLE peer_pairing_head (
 binding_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
 revision INTEGER NOT NULL,
 FOREIGN KEY(binding_id,revision) REFERENCES peer_pairing_revision(binding_id,revision)
) STRICT;
CREATE TABLE peer_pairing_operation (
 request_id TEXT PRIMARY KEY NOT NULL COLLATE BINARY,
 principal_id TEXT NOT NULL COLLATE BINARY,
 request_digest TEXT NOT NULL COLLATE BINARY,
 binding_id TEXT NOT NULL COLLATE BINARY,
 revision INTEGER NOT NULL,
 acquisition_id TEXT NOT NULL,
 FOREIGN KEY(binding_id,revision) REFERENCES peer_pairing_revision(binding_id,revision),
 FOREIGN KEY(principal_id) REFERENCES peer_pairing_manager(principal_id),
 FOREIGN KEY(acquisition_id) REFERENCES authentication_clock_acquisition(id)
) STRICT;
CREATE TRIGGER peer_pairing_revision_no_update BEFORE UPDATE ON peer_pairing_revision BEGIN SELECT RAISE(ABORT,'immutable_peer_pairing'); END;
CREATE TRIGGER peer_pairing_revision_no_delete BEFORE DELETE ON peer_pairing_revision BEGIN SELECT RAISE(ABORT,'immutable_peer_pairing'); END;
CREATE TRIGGER peer_pairing_head_no_delete BEFORE DELETE ON peer_pairing_head BEGIN SELECT RAISE(ABORT,'immutable_peer_pairing_head'); END;
CREATE TRIGGER peer_pairing_head_step BEFORE UPDATE ON peer_pairing_head WHEN NEW.binding_id<>OLD.binding_id OR NEW.revision<>OLD.revision+1
 BEGIN SELECT RAISE(ABORT,'peer_pairing_revision_step'); END;
CREATE TRIGGER peer_pairing_operation_no_update BEFORE UPDATE ON peer_pairing_operation BEGIN SELECT RAISE(ABORT,'immutable_peer_pairing_operation'); END;
CREATE TRIGGER peer_pairing_operation_no_delete BEFORE DELETE ON peer_pairing_operation BEGIN SELECT RAISE(ABORT,'immutable_peer_pairing_operation'); END;
CREATE TABLE peer_pairing_challenge (
 challenge_id TEXT PRIMARY KEY NOT NULL,
 binding_id TEXT NOT NULL REFERENCES peer_pairing_head(binding_id),
 verifier BLOB NOT NULL CHECK(length(verifier)=32),
 expires_tick_ns TEXT NOT NULL,
 expires_ns TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('pending','consumed','superseded'))
) STRICT;
CREATE UNIQUE INDEX peer_pairing_one_challenge ON peer_pairing_challenge(binding_id) WHERE state='pending';
CREATE TRIGGER peer_pairing_challenge_no_delete BEFORE DELETE ON peer_pairing_challenge BEGIN SELECT RAISE(ABORT,'immutable_peer_challenge'); END;
CREATE TRIGGER peer_pairing_challenge_transition BEFORE UPDATE ON peer_pairing_challenge WHEN
 NEW.challenge_id<>OLD.challenge_id OR NEW.binding_id<>OLD.binding_id OR NEW.verifier<>OLD.verifier OR NEW.expires_tick_ns<>OLD.expires_tick_ns OR NEW.expires_ns<>OLD.expires_ns
 OR OLD.state<>'pending' OR NEW.state NOT IN ('consumed','superseded') BEGIN SELECT RAISE(ABORT,'peer_challenge_transition'); END;
CREATE TABLE peer_pairing_proof (
 proof_id TEXT PRIMARY KEY NOT NULL,
 binding_id TEXT NOT NULL REFERENCES peer_pairing_head(binding_id),
 direction TEXT NOT NULL CHECK(direction IN ('inbound','outbound')),
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs)),
 body_digest TEXT NOT NULL,
 UNIQUE(binding_id,direction)
) STRICT;
CREATE TRIGGER peer_pairing_proof_no_update BEFORE UPDATE ON peer_pairing_proof BEGIN SELECT RAISE(ABORT,'immutable_peer_proof'); END;
CREATE TRIGGER peer_pairing_proof_no_delete BEFORE DELETE ON peer_pairing_proof BEGIN SELECT RAISE(ABORT,'immutable_peer_proof'); END;
