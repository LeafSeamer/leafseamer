CREATE TABLE service_state (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 authority TEXT NOT NULL CHECK(authority='seamer'),
 service_id TEXT NOT NULL UNIQUE,
 database_instance_id TEXT NOT NULL UNIQUE,
 request_epoch TEXT NOT NULL,
 schema_revision TEXT NOT NULL CHECK(schema_revision='1'),
 maintenance_state TEXT NOT NULL CHECK(maintenance_state='protected'),
 UNIQUE(service_id,database_instance_id)
) STRICT;
CREATE TRIGGER service_state_no_update BEFORE UPDATE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;
CREATE TRIGGER service_state_no_delete BEFORE DELETE ON service_state BEGIN SELECT RAISE(ABORT,'immutable_service_identity'); END;
CREATE TABLE schema_migration (
 migration_id TEXT PRIMARY KEY NOT NULL,
 from_revision TEXT NOT NULL CHECK(from_revision='0'),
 to_revision TEXT NOT NULL UNIQUE CHECK(to_revision='1'),
 script_digest TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state='committed')
) STRICT;
CREATE TRIGGER schema_migration_no_update BEFORE UPDATE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TRIGGER schema_migration_no_delete BEFORE DELETE ON schema_migration BEGIN SELECT RAISE(ABORT,'immutable_migration'); END;
CREATE TABLE service_boot (
 service_id TEXT NOT NULL,
 database_instance_id TEXT NOT NULL,
 service_boot_id TEXT PRIMARY KEY NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('started','stopped')),
 FOREIGN KEY(service_id,database_instance_id) REFERENCES service_state(service_id,database_instance_id)
) STRICT;
CREATE TRIGGER service_boot_transition BEFORE UPDATE ON service_boot WHEN
 NEW.service_id<>OLD.service_id OR NEW.database_instance_id<>OLD.database_instance_id OR NEW.service_boot_id<>OLD.service_boot_id
 OR OLD.state<>'started' OR NEW.state<>'stopped' BEGIN SELECT RAISE(ABORT,'invalid_boot_transition'); END;
CREATE TRIGGER service_boot_no_delete BEFORE DELETE ON service_boot BEGIN SELECT RAISE(ABORT,'immutable_boot_history'); END;
