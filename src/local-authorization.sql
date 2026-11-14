CREATE TABLE local_subject (
 subject_id TEXT PRIMARY KEY NOT NULL,
 issuer_id TEXT NOT NULL,
 issuer_epoch TEXT NOT NULL,
 principal_id TEXT NOT NULL,
 principal_kind TEXT NOT NULL CHECK(principal_kind IN ('human','service')),
 UNIQUE(issuer_id,issuer_epoch,principal_id)
) STRICT;
CREATE TRIGGER local_subject_no_update BEFORE UPDATE ON local_subject BEGIN SELECT RAISE(ABORT,'immutable_local_subject'); END;
CREATE TRIGGER local_subject_no_delete BEFORE DELETE ON local_subject BEGIN SELECT RAISE(ABORT,'immutable_local_subject'); END;
CREATE TABLE local_permission_revision (
 subject_id TEXT NOT NULL REFERENCES local_subject(subject_id),
 revision TEXT NOT NULL CHECK(length(revision)=20 AND revision NOT GLOB '*[^0-9]*' AND revision<>'00000000000000000000'),
 id TEXT NOT NULL UNIQUE,
 previous_revision TEXT,
 state TEXT NOT NULL CHECK(state IN ('active','pending')),
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=65536),
 digest TEXT NOT NULL,
 operation_id TEXT NOT NULL UNIQUE,
 PRIMARY KEY(subject_id,revision),
 UNIQUE(subject_id,previous_revision),
 UNIQUE(operation_id,subject_id,revision),
 CHECK((revision='00000000000000000001' AND previous_revision IS NULL) OR (revision<>'00000000000000000001' AND previous_revision IS NOT NULL)),
 FOREIGN KEY(subject_id,previous_revision) REFERENCES local_permission_revision(subject_id,revision),
 FOREIGN KEY(operation_id,subject_id,revision) REFERENCES local_permission_operation(id,subject_id,revision) DEFERRABLE INITIALLY DEFERRED
) STRICT;
CREATE TABLE local_permission_head (
 subject_id TEXT PRIMARY KEY NOT NULL REFERENCES local_subject(subject_id),
 revision TEXT NOT NULL,
 FOREIGN KEY(subject_id,revision) REFERENCES local_permission_revision(subject_id,revision)
) STRICT;
CREATE TRIGGER local_permission_first BEFORE INSERT ON local_permission_head WHEN NEW.revision<>'00000000000000000001' BEGIN SELECT RAISE(ABORT,'local_permission_order'); END;
CREATE TRIGGER local_permission_next BEFORE UPDATE ON local_permission_head WHEN NEW.subject_id<>OLD.subject_id OR NOT EXISTS (
 SELECT 1 FROM local_permission_revision WHERE subject_id=NEW.subject_id AND revision=NEW.revision AND previous_revision=OLD.revision
) BEGIN SELECT RAISE(ABORT,'local_permission_order'); END;
CREATE TRIGGER local_permission_head_no_delete BEFORE DELETE ON local_permission_head BEGIN SELECT RAISE(ABORT,'immutable_local_permission'); END;
CREATE TABLE local_permission_operation (
 id TEXT PRIMARY KEY NOT NULL,
 subject_id TEXT NOT NULL,
 revision TEXT NOT NULL,
 actor_subject_id TEXT NOT NULL REFERENCES local_subject(subject_id),
 request_id TEXT NOT NULL,
 request_digest TEXT NOT NULL,
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs)),
 digest TEXT NOT NULL,
 UNIQUE(id,subject_id,revision),
 UNIQUE(actor_subject_id,request_id),
 FOREIGN KEY(id,subject_id,revision) REFERENCES local_permission_revision(operation_id,subject_id,revision) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(id) REFERENCES local_permission_outbox(operation_id) DEFERRABLE INITIALLY DEFERRED
) STRICT;
CREATE TABLE local_permission_outbox (
 operation_id TEXT PRIMARY KEY NOT NULL REFERENCES local_permission_operation(id) DEFERRABLE INITIALLY DEFERRED,
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs)),
 digest TEXT NOT NULL
) STRICT;
CREATE TRIGGER local_permission_revision_no_update BEFORE UPDATE ON local_permission_revision BEGIN SELECT RAISE(ABORT,'immutable_local_permission'); END;
CREATE TRIGGER local_permission_revision_no_delete BEFORE DELETE ON local_permission_revision BEGIN SELECT RAISE(ABORT,'immutable_local_permission'); END;
CREATE TRIGGER local_permission_operation_no_update BEFORE UPDATE ON local_permission_operation BEGIN SELECT RAISE(ABORT,'immutable_local_permission'); END;
CREATE TRIGGER local_permission_operation_no_delete BEFORE DELETE ON local_permission_operation BEGIN SELECT RAISE(ABORT,'immutable_local_permission'); END;
CREATE TRIGGER local_permission_outbox_no_update BEFORE UPDATE ON local_permission_outbox BEGIN SELECT RAISE(ABORT,'immutable_local_permission'); END;
CREATE TRIGGER local_permission_outbox_no_delete BEFORE DELETE ON local_permission_outbox BEGIN SELECT RAISE(ABORT,'immutable_local_permission'); END;
CREATE TABLE local_request_context (
 context_id TEXT PRIMARY KEY NOT NULL,
 subject_id TEXT NOT NULL REFERENCES local_subject(subject_id),
 service_boot_id TEXT NOT NULL REFERENCES service_boot(service_boot_id),
 body_jcs TEXT NOT NULL CHECK(json_valid(body_jcs) AND length(CAST(body_jcs AS BLOB))<=65536),
 digest TEXT NOT NULL
) STRICT;
CREATE TRIGGER local_request_context_no_update BEFORE UPDATE ON local_request_context BEGIN SELECT RAISE(ABORT,'immutable_local_context'); END;
CREATE TRIGGER local_request_context_no_delete BEFORE DELETE ON local_request_context BEGIN SELECT RAISE(ABORT,'immutable_local_context'); END;
CREATE TABLE local_context_request (
 context_id TEXT NOT NULL REFERENCES local_request_context(context_id),
 transaction_id TEXT PRIMARY KEY NOT NULL REFERENCES definition_request(transaction_id) DEFERRABLE INITIALLY DEFERRED
) STRICT;
CREATE TRIGGER local_context_request_no_update BEFORE UPDATE ON local_context_request BEGIN SELECT RAISE(ABORT,'immutable_context_charge'); END;
CREATE TRIGGER local_context_request_no_delete BEFORE DELETE ON local_context_request BEGIN SELECT RAISE(ABORT,'immutable_context_charge'); END;
