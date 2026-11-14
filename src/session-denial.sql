CREATE TABLE received_session_denial (
 issuer_service_id TEXT NOT NULL,
 issuer_id TEXT NOT NULL,
 issuer_epoch TEXT NOT NULL,
 operation_id TEXT NOT NULL,
 session_id TEXT NOT NULL,
 service_boot_id TEXT NOT NULL REFERENCES service_boot(service_boot_id),
 notice_jcs TEXT NOT NULL,
 notice_digest TEXT NOT NULL,
 receipt_jcs TEXT NOT NULL,
 receipt_digest TEXT NOT NULL,
 PRIMARY KEY(issuer_service_id,issuer_id,issuer_epoch,operation_id)
) STRICT;
CREATE INDEX received_session_denial_subject ON received_session_denial(issuer_id,issuer_epoch,session_id);
CREATE TRIGGER received_session_denial_no_update BEFORE UPDATE ON received_session_denial
BEGIN SELECT RAISE(ABORT,'immutable_received_session_denial'); END;
CREATE TRIGGER received_session_denial_no_delete BEFORE DELETE ON received_session_denial
BEGIN SELECT RAISE(ABORT,'immutable_received_session_denial'); END;
