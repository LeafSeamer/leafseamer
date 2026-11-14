CREATE TABLE definition_identity (
 definition_id TEXT PRIMARY KEY NOT NULL,
 service_id TEXT NOT NULL,
 database_instance_id TEXT NOT NULL,
 created_transaction_id TEXT NOT NULL UNIQUE,
 FOREIGN KEY(service_id,database_instance_id) REFERENCES service_state(service_id,database_instance_id),
 FOREIGN KEY(created_transaction_id,definition_id) REFERENCES definition_request(transaction_id,definition_id) DEFERRABLE INITIALLY DEFERRED
) STRICT;
CREATE TABLE definition_content (
 definition_id TEXT NOT NULL REFERENCES definition_identity(definition_id),
 revision TEXT NOT NULL CHECK(length(revision) BETWEEN 1 AND 20 AND revision NOT GLOB '*[^0-9]*' AND substr(revision,1,1)<>'0'),
 previous_revision TEXT,
 content_jcs TEXT NOT NULL CHECK(length(CAST(content_jcs AS BLOB))<=16777216),
 content_digest TEXT NOT NULL,
 selection_digest TEXT NOT NULL,
 transaction_id TEXT NOT NULL UNIQUE,
 PRIMARY KEY(definition_id,revision),
 UNIQUE(definition_id,previous_revision),
 CHECK((revision='1' AND previous_revision IS NULL) OR (revision<>'1' AND previous_revision IS NOT NULL)),
 FOREIGN KEY(definition_id,previous_revision) REFERENCES definition_content(definition_id,revision),
 FOREIGN KEY(transaction_id,definition_id) REFERENCES definition_request(transaction_id,definition_id) DEFERRABLE INITIALLY DEFERRED
) STRICT;
CREATE TABLE definition_metadata (
 definition_id TEXT NOT NULL REFERENCES definition_identity(definition_id),
 revision TEXT NOT NULL CHECK(length(revision) BETWEEN 1 AND 20 AND revision NOT GLOB '*[^0-9]*' AND substr(revision,1,1)<>'0'),
 previous_revision TEXT,
 metadata_jcs TEXT NOT NULL CHECK(length(CAST(metadata_jcs AS BLOB))<=16777216),
 metadata_digest TEXT NOT NULL,
 transaction_id TEXT NOT NULL,
 PRIMARY KEY(definition_id,revision),
 UNIQUE(definition_id,previous_revision),
 CHECK((revision='1' AND previous_revision IS NULL) OR (revision<>'1' AND previous_revision IS NOT NULL)),
 FOREIGN KEY(definition_id,previous_revision) REFERENCES definition_metadata(definition_id,revision),
 FOREIGN KEY(transaction_id,definition_id) REFERENCES definition_request(transaction_id,definition_id) DEFERRABLE INITIALLY DEFERRED
) STRICT;
CREATE TABLE definition_head (
 definition_id TEXT PRIMARY KEY NOT NULL REFERENCES definition_identity(definition_id),
 content_revision TEXT NOT NULL,
 metadata_revision TEXT NOT NULL,
 FOREIGN KEY(definition_id,content_revision) REFERENCES definition_content(definition_id,revision),
 FOREIGN KEY(definition_id,metadata_revision) REFERENCES definition_metadata(definition_id,revision)
) STRICT;
CREATE TABLE definition_request (
 transaction_id TEXT PRIMARY KEY NOT NULL,
 identity_digest TEXT NOT NULL UNIQUE,
 request_digest TEXT NOT NULL,
 actor_jcs TEXT NOT NULL,
 operation_id TEXT NOT NULL CHECK(operation_id IN ('createDefinition','updateDraft','updateDefinitionMetadata')),
 definition_id TEXT NOT NULL REFERENCES definition_identity(definition_id),
 result_jcs TEXT NOT NULL CHECK(length(CAST(result_jcs AS BLOB))<=16777216),
 result_digest TEXT NOT NULL,
 UNIQUE(transaction_id,definition_id),
 FOREIGN KEY(transaction_id) REFERENCES definition_notification(transaction_id) DEFERRABLE INITIALLY DEFERRED
) STRICT;
-- 同一不可变通知同时保留审计事实与待投递正文；后续投递确认另存，不能删除历史。
CREATE TABLE definition_notification (
 transaction_id TEXT PRIMARY KEY NOT NULL REFERENCES definition_request(transaction_id) DEFERRABLE INITIALLY DEFERRED,
 body_jcs TEXT NOT NULL,
 body_digest TEXT NOT NULL
) STRICT;
CREATE TRIGGER definition_head_transition BEFORE UPDATE ON definition_head WHEN
 NEW.definition_id<>OLD.definition_id OR
 NOT ((NEW.content_revision<>OLD.content_revision AND NEW.metadata_revision=OLD.metadata_revision AND EXISTS(
 SELECT 1 FROM definition_content WHERE definition_id=OLD.definition_id AND revision=NEW.content_revision AND previous_revision=OLD.content_revision)) OR
 (NEW.content_revision=OLD.content_revision AND NEW.metadata_revision<>OLD.metadata_revision AND EXISTS(
 SELECT 1 FROM definition_metadata WHERE definition_id=OLD.definition_id AND revision=NEW.metadata_revision AND previous_revision=OLD.metadata_revision)))
 BEGIN SELECT RAISE(ABORT,'invalid_definition_head'); END;
CREATE TRIGGER definition_head_no_delete BEFORE DELETE ON definition_head BEGIN SELECT RAISE(ABORT,'immutable_definition_head'); END;
CREATE TRIGGER definition_identity_no_update BEFORE UPDATE ON definition_identity BEGIN SELECT RAISE(ABORT,'immutable_definition_identity'); END;
CREATE TRIGGER definition_identity_no_delete BEFORE DELETE ON definition_identity BEGIN SELECT RAISE(ABORT,'immutable_definition_identity'); END;
CREATE TRIGGER definition_content_no_update BEFORE UPDATE ON definition_content BEGIN SELECT RAISE(ABORT,'immutable_definition_content'); END;
CREATE TRIGGER definition_content_no_delete BEFORE DELETE ON definition_content BEGIN SELECT RAISE(ABORT,'immutable_definition_content'); END;
CREATE TRIGGER definition_metadata_no_update BEFORE UPDATE ON definition_metadata BEGIN SELECT RAISE(ABORT,'immutable_definition_metadata'); END;
CREATE TRIGGER definition_metadata_no_delete BEFORE DELETE ON definition_metadata BEGIN SELECT RAISE(ABORT,'immutable_definition_metadata'); END;
CREATE TRIGGER definition_request_no_update BEFORE UPDATE ON definition_request BEGIN SELECT RAISE(ABORT,'immutable_definition_request'); END;
CREATE TRIGGER definition_request_no_delete BEFORE DELETE ON definition_request BEGIN SELECT RAISE(ABORT,'immutable_definition_request'); END;
CREATE TRIGGER definition_notification_no_update BEFORE UPDATE ON definition_notification BEGIN SELECT RAISE(ABORT,'immutable_definition_notification'); END;
CREATE TRIGGER definition_notification_no_delete BEFORE DELETE ON definition_notification BEGIN SELECT RAISE(ABORT,'immutable_definition_notification'); END;
