-- migrate:up
-- Workers lease tasks (with everything they need) from the orchestrator over gRPC,
-- so they no longer read Postgres.

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'worker_ro') THEN
    REVOKE ALL ON ALL TABLES IN SCHEMA public FROM worker_ro;
    DROP ROLE worker_ro;
  END IF;
END $$;

-- migrate:down

CREATE ROLE worker_ro LOGIN;
GRANT SELECT ON jobs, tasks, files, task_files TO worker_ro;
