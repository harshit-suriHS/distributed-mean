-- migrate:up

-- Clients see uuid (v7, time-ordered); bigint id stays the internal primary key.
ALTER TABLE jobs ADD COLUMN uuid uuid NOT NULL UNIQUE DEFAULT uuidv7();

-- An upload session: files go to inputs/{uuid}/ via a presigned POST policy, then POST /jobs commits it.
-- Uncommitted uploads expire (expires_at); the orchestrator's cleanup deletes their objects.
CREATE TABLE uploads (
  id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  uuid       uuid   NOT NULL UNIQUE DEFAULT uuidv7(),
  status     text   NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'committed', 'expired')),
  job_id     bigint UNIQUE REFERENCES jobs,                          -- set at commit
  expires_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK ((status = 'committed') = (job_id IS NOT NULL))
);

CREATE INDEX uploads_expiring ON uploads (expires_at) WHERE status = 'open';

GRANT SELECT, INSERT ON uploads TO api;
GRANT UPDATE (status, job_id) ON uploads TO api;            -- commit
GRANT SELECT ON uploads TO orchestrator;
GRANT UPDATE (status) ON uploads TO orchestrator;           -- expire

-- migrate:down

DROP TABLE uploads;
ALTER TABLE jobs DROP COLUMN uuid;
