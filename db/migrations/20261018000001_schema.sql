-- migrate:up

CREATE FUNCTION set_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END $$;

-- Runtime settings changed through the API. Everything else is env config.
CREATE TABLE settings (
  id              int PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  desired_workers int NOT NULL DEFAULT 4 CHECK (desired_workers BETWEEN 0 AND 64),
  updated_at      timestamptz NOT NULL DEFAULT now()
);
INSERT INTO settings DEFAULT VALUES;

CREATE TABLE jobs (
  id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  no_of_files    int  NOT NULL CHECK (no_of_files > 0),                -- F
  no_of_columns  int  NOT NULL CHECK (no_of_columns > 0),              -- C
  status         text NOT NULL DEFAULT 'queued'
                 CHECK (status IN ('queued', 'running', 'succeeded', 'failed')),
  files_per_task int  CHECK (files_per_task > 0),                      -- N, set when planned; merge fan-in = max(2, N)
  failure_type   text CHECK (failure_type IN ('internal', 'user_input')),
  error          text,
  created_at     timestamptz NOT NULL DEFAULT now(),
  started_at     timestamptz,
  completed_at   timestamptz,
  updated_at     timestamptz NOT NULL DEFAULT now(),
  CHECK ((status = 'failed') = (failure_type IS NOT NULL))
);

CREATE TABLE tasks (
  id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  job_id       bigint  NOT NULL REFERENCES jobs,
  kind         text    NOT NULL CHECK (kind IN ('map', 'merge')),
  no_of_files  int     NOT NULL CHECK (no_of_files > 0),               -- original files covered
  is_final     boolean NOT NULL DEFAULT false,                         -- no_of_files = F: writes the result
  status       text    NOT NULL DEFAULT 'pending'
               CHECK (status IN ('pending', 'sent', 'running', 'done', 'failed', 'skipped')),
  attempts     int     NOT NULL DEFAULT 0,                             -- counted from `started` reports
  worker_id    text,
  error        text,
  created_at   timestamptz NOT NULL DEFAULT now(),
  sent_at      timestamptz,
  started_at   timestamptz,
  completed_at timestamptz,
  updated_at   timestamptz NOT NULL DEFAULT now()
);

-- Uploaded inputs, partials and the result: every blob in storage has a row.
CREATE TABLE files (
  id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  job_id      bigint NOT NULL REFERENCES jobs,
  type        text   NOT NULL CHECK (type IN ('input', 'partial', 'result')),
  key         text   NOT NULL UNIQUE,                                  -- object key in the bucket
  no_of_files int    NOT NULL CHECK (no_of_files > 0),                 -- original files covered
  produced_by bigint REFERENCES tasks,                                 -- null for uploaded inputs
  size_bytes  bigint,
  created_at  timestamptz NOT NULL DEFAULT now(),
  deleted_at  timestamptz,                                             -- object removed by cleanup
  CHECK ((type = 'input') = (produced_by IS NULL)),
  CHECK (type <> 'input' OR no_of_files = 1)
);

-- A task's inputs (files for a map, partials for a merge).
CREATE TABLE task_files (
  task_id bigint NOT NULL REFERENCES tasks,
  file_id bigint NOT NULL REFERENCES files,
  PRIMARY KEY (task_id, file_id),
  UNIQUE (file_id)                                                     -- each file is read by exactly one task
);

CREATE INDEX tasks_job_status ON tasks (job_id, status);
CREATE INDEX jobs_active      ON jobs (created_at) WHERE status IN ('queued', 'running');
CREATE INDEX files_job_type   ON files (job_id, type);
CREATE INDEX files_to_clean   ON files (job_id) WHERE deleted_at IS NULL AND type <> 'result';

CREATE TRIGGER settings_updated_at BEFORE UPDATE ON settings FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER jobs_updated_at     BEFORE UPDATE ON jobs     FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER tasks_updated_at    BEFORE UPDATE ON tasks    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- migrate:down

DROP TABLE task_files;
DROP TABLE files;
DROP TABLE tasks;
DROP TABLE jobs;
DROP TABLE settings;
DROP FUNCTION set_updated_at();
