-- migrate:up
-- Pull scheduling: workers lease tasks from the orchestrator, so tasks go straight
-- pending -> running (no `sent`), and retries wait out a backoff in `not_before`.

ALTER TABLE tasks ADD COLUMN not_before timestamptz;   -- retry backoff: not leasable before this
ALTER TABLE tasks DROP COLUMN sent_at;

ALTER TABLE tasks DROP CONSTRAINT tasks_status_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_status_check
  CHECK (status IN ('pending', 'running', 'done', 'failed', 'skipped'));

-- Production target is ~1K workers; 10K is the extreme case
ALTER TABLE settings DROP CONSTRAINT settings_desired_workers_check;
ALTER TABLE settings ADD CONSTRAINT settings_desired_workers_check
  CHECK (desired_workers BETWEEN 0 AND 10000);

-- migrate:down

ALTER TABLE settings DROP CONSTRAINT settings_desired_workers_check;
ALTER TABLE settings ADD CONSTRAINT settings_desired_workers_check
  CHECK (desired_workers BETWEEN 0 AND 64);

ALTER TABLE tasks DROP CONSTRAINT tasks_status_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_status_check
  CHECK (status IN ('pending', 'sent', 'running', 'done', 'failed', 'skipped'));

ALTER TABLE tasks ADD COLUMN sent_at timestamptz;
ALTER TABLE tasks DROP COLUMN not_before;
