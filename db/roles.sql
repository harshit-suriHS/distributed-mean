-- Creates the app login roles, or resets their passwords. Safe to run on every start.
-- Usage: psql -v api_pw=... -v orchestrator_pw=... -v supervisor_pw=... -f roles.sql
-- (Workers have no database access: they lease tasks from the orchestrator over gRPC.)

SELECT format('CREATE ROLE %I LOGIN', r)
  FROM unnest(ARRAY['api', 'orchestrator', 'supervisor']) AS r
 WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r)
\gexec

ALTER ROLE api          PASSWORD :'api_pw';
ALTER ROLE orchestrator PASSWORD :'orchestrator_pw';
ALTER ROLE supervisor   PASSWORD :'supervisor_pw';
