-- DBA-operated template. Run only after reviewing the chosen schema:
-- psql "$ADMIN_DSN" -v ON_ERROR_STOP=1 -v schema=duraflow -f roles.sql
-- NO passwords or login accounts are created here. Never use DBA credentials
-- for engine/worker/SDK processes. Runtime writers are trusted, not adversarial.
BEGIN;
SELECT format('CREATE ROLE %I NOLOGIN', role_name)
FROM (VALUES ('duraflow_reader'), ('duraflow_runtime'), ('duraflow_operator'), ('duraflow_migrator')) AS groups(role_name)
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) \gexec
GRANT duraflow_runtime TO duraflow_operator;
REVOKE ALL ON SCHEMA :"schema" FROM PUBLIC;
GRANT USAGE ON SCHEMA :"schema" TO duraflow_reader, duraflow_runtime;
GRANT SELECT ON ALL TABLES IN SCHEMA :"schema" TO duraflow_reader;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA :"schema" TO duraflow_runtime;
ALTER SCHEMA :"schema" OWNER TO duraflow_migrator;
ALTER TABLE :"schema".runs OWNER TO duraflow_migrator;
ALTER TABLE :"schema".requests OWNER TO duraflow_migrator;
ALTER TABLE :"schema".heads OWNER TO duraflow_migrator;
ALTER TABLE :"schema".schema_version OWNER TO duraflow_migrator;
ALTER DEFAULT PRIVILEGES FOR ROLE duraflow_migrator IN SCHEMA :"schema" GRANT SELECT ON TABLES TO duraflow_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE duraflow_migrator IN SCHEMA :"schema" GRANT SELECT, INSERT, UPDATE ON TABLES TO duraflow_runtime;
COMMIT;
-- Grant the appropriate group to separately created LOGIN roles using SCRAM or
-- certificate authentication. Do not grant migration/owner/superuser to runtime.
-- No role here grants membership to arbitrary callers based on --actor.
