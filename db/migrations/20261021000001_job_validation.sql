-- migrate:up
-- POST /jobs returns at once with the job 'validating'; the API then lists the upload's prefix
-- in the background and checks every file (count, .csv, size) before the job is 'queued' for the
-- orchestrator. validating_until is a lease, not a lock: whoever holds it lists the prefix with no
-- transaction open, and another API replica takes over once it lapses.

ALTER TABLE jobs DROP CONSTRAINT jobs_status_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_status_check
  CHECK (status IN ('validating', 'queued', 'running', 'succeeded', 'failed'));
ALTER TABLE jobs ALTER COLUMN status SET DEFAULT 'validating';

ALTER TABLE jobs ADD COLUMN validating_until timestamptz;

CREATE INDEX jobs_validating ON jobs (id) WHERE status = 'validating';

-- The API owns a job until it's queued (validating -> queued | failed); the orchestrator from then on
GRANT UPDATE (status, failure_type, error, completed_at, validating_until) ON jobs TO api;

-- migrate:down

REVOKE UPDATE (status, failure_type, error, completed_at, validating_until) ON jobs FROM api;
DROP INDEX jobs_validating;
ALTER TABLE jobs DROP COLUMN validating_until;
ALTER TABLE jobs ALTER COLUMN status SET DEFAULT 'queued';
ALTER TABLE jobs DROP CONSTRAINT jobs_status_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_status_check
  CHECK (status IN ('queued', 'running', 'succeeded', 'failed'));
