-- migrate:up
-- Roles and passwords are created by db/roles.sql (reads .env); this file only grants.

-- Workers only read. (Role dropped again in 20261020000001 when workers moved to gRPC; created
-- here if missing so the migration history replays on a fresh database.)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'worker_ro') THEN
    CREATE ROLE worker_ro LOGIN;
  END IF;
END $$;
GRANT SELECT ON jobs, tasks, files, task_files TO worker_ro;

-- API: creates jobs with their uploaded files, changes W, reads everything for status and stats.
GRANT SELECT ON settings, jobs, tasks, files, task_files TO api;
GRANT INSERT ON jobs, files TO api;
GRANT UPDATE (desired_workers) ON settings TO api;

-- Orchestrator: the only writer of task and job state.
GRANT SELECT ON settings TO orchestrator;
GRANT SELECT, INSERT, UPDATE ON jobs, tasks, files, task_files TO orchestrator;

-- migrate:down

REVOKE ALL ON settings, jobs, tasks, files, task_files FROM worker_ro, api, orchestrator;
