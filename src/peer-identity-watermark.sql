CREATE TABLE peer_identity_watermark (
 issuer_service_id TEXT NOT NULL COLLATE BINARY,
 issuer_id TEXT NOT NULL COLLATE BINARY,
 issuer_epoch TEXT NOT NULL COLLATE BINARY,
 revision TEXT NOT NULL COLLATE BINARY CHECK(length(revision)=20 AND revision NOT GLOB '*[^0-9]*' AND revision>'00000000000000000000' AND revision<='18446744073709551615'),
 service_boot_id TEXT NOT NULL REFERENCES service_boot(service_boot_id),
 body_jcs TEXT NOT NULL,
 body_digest TEXT NOT NULL,
 PRIMARY KEY(issuer_service_id,issuer_id,issuer_epoch,revision)
) STRICT;
CREATE TRIGGER peer_identity_watermark_forward BEFORE INSERT ON peer_identity_watermark
WHEN EXISTS(SELECT 1 FROM peer_identity_watermark WHERE issuer_service_id=NEW.issuer_service_id AND issuer_id=NEW.issuer_id
 AND issuer_epoch=NEW.issuer_epoch AND revision>=NEW.revision)
BEGIN SELECT RAISE(ABORT,'identity_watermark_not_forward'); END;
CREATE TRIGGER peer_identity_watermark_no_update BEFORE UPDATE ON peer_identity_watermark
BEGIN SELECT RAISE(ABORT,'immutable_identity_watermark'); END;
CREATE TRIGGER peer_identity_watermark_no_delete BEFORE DELETE ON peer_identity_watermark
BEGIN SELECT RAISE(ABORT,'immutable_identity_watermark'); END;
