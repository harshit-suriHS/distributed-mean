-- Creates the app login roles, or resets their passwords. Safe to run on every start.
-- Usage: psql -v api_pw=... -v orchestrator_pw=... -v worker_pw=... -f roles.sql

SELECT format('CREATE ROLE %I LOGIN', r)
  FROM unnest(ARRAY['api', 'orchestrator', 'worker_ro']) AS r
 WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r)
\gexec

ALTER ROLE api          PASSWORD :'api_pw';
ALTER ROLE orchestrator PASSWORD :'orchestrator_pw';
ALTER ROLE worker_ro    PASSWORD :'worker_pw';
