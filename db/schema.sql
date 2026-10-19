\restrict dbmate

-- Dumped from database version 18.6
-- Dumped by pg_dump version 18.6

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: set_updated_at(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.set_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END $$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: files; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.files (
    id bigint NOT NULL,
    job_id bigint NOT NULL,
    type text NOT NULL,
    key text NOT NULL,
    no_of_files integer NOT NULL,
    produced_by bigint,
    size_bytes bigint,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    deleted_at timestamp with time zone,
    CONSTRAINT files_check CHECK (((type = 'input'::text) = (produced_by IS NULL))),
    CONSTRAINT files_check1 CHECK (((type <> 'input'::text) OR (no_of_files = 1))),
    CONSTRAINT files_no_of_files_check CHECK ((no_of_files > 0)),
    CONSTRAINT files_type_check CHECK ((type = ANY (ARRAY['input'::text, 'partial'::text, 'result'::text])))
);


--
-- Name: files_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.files ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.files_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: jobs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.jobs (
    id bigint NOT NULL,
    no_of_files integer NOT NULL,
    no_of_columns integer NOT NULL,
    status text DEFAULT 'queued'::text NOT NULL,
    files_per_task integer,
    failure_type text,
    error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    uuid uuid DEFAULT uuidv7() NOT NULL,
    CONSTRAINT jobs_check CHECK (((status = 'failed'::text) = (failure_type IS NOT NULL))),
    CONSTRAINT jobs_failure_type_check CHECK ((failure_type = ANY (ARRAY['internal'::text, 'user_input'::text]))),
    CONSTRAINT jobs_files_per_task_check CHECK ((files_per_task > 0)),
    CONSTRAINT jobs_no_of_columns_check CHECK ((no_of_columns > 0)),
    CONSTRAINT jobs_no_of_files_check CHECK ((no_of_files > 0)),
    CONSTRAINT jobs_status_check CHECK ((status = ANY (ARRAY['queued'::text, 'running'::text, 'succeeded'::text, 'failed'::text])))
);


--
-- Name: jobs_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.jobs ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.jobs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: schema_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_migrations (
    version character varying NOT NULL
);


--
-- Name: settings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.settings (
    id integer DEFAULT 1 NOT NULL,
    desired_workers integer DEFAULT 4 NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT settings_desired_workers_check CHECK (((desired_workers >= 0) AND (desired_workers <= 10000))),
    CONSTRAINT settings_id_check CHECK ((id = 1))
);


--
-- Name: task_files; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.task_files (
    task_id bigint NOT NULL,
    file_id bigint NOT NULL
);


--
-- Name: tasks; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.tasks (
    id bigint NOT NULL,
    job_id bigint NOT NULL,
    kind text NOT NULL,
    no_of_files integer NOT NULL,
    is_final boolean DEFAULT false NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    worker_id text,
    error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    not_before timestamp with time zone,
    CONSTRAINT tasks_kind_check CHECK ((kind = ANY (ARRAY['map'::text, 'merge'::text]))),
    CONSTRAINT tasks_no_of_files_check CHECK ((no_of_files > 0)),
    CONSTRAINT tasks_status_check CHECK ((status = ANY (ARRAY['pending'::text, 'running'::text, 'done'::text, 'failed'::text, 'skipped'::text])))
);


--
-- Name: tasks_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.tasks ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.tasks_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: uploads; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.uploads (
    id bigint NOT NULL,
    uuid uuid DEFAULT uuidv7() NOT NULL,
    status text DEFAULT 'open'::text NOT NULL,
    job_id bigint,
    expires_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT uploads_check CHECK (((status = 'committed'::text) = (job_id IS NOT NULL))),
    CONSTRAINT uploads_status_check CHECK ((status = ANY (ARRAY['open'::text, 'committed'::text, 'expired'::text])))
);


--
-- Name: uploads_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.uploads ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.uploads_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: files files_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.files
    ADD CONSTRAINT files_key_key UNIQUE (key);


--
-- Name: files files_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.files
    ADD CONSTRAINT files_pkey PRIMARY KEY (id);


--
-- Name: jobs jobs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.jobs
    ADD CONSTRAINT jobs_pkey PRIMARY KEY (id);


--
-- Name: jobs jobs_uuid_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.jobs
    ADD CONSTRAINT jobs_uuid_key UNIQUE (uuid);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: settings settings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.settings
    ADD CONSTRAINT settings_pkey PRIMARY KEY (id);


--
-- Name: task_files task_files_file_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_files
    ADD CONSTRAINT task_files_file_id_key UNIQUE (file_id);


--
-- Name: task_files task_files_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_files
    ADD CONSTRAINT task_files_pkey PRIMARY KEY (task_id, file_id);


--
-- Name: tasks tasks_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tasks
    ADD CONSTRAINT tasks_pkey PRIMARY KEY (id);


--
-- Name: uploads uploads_job_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.uploads
    ADD CONSTRAINT uploads_job_id_key UNIQUE (job_id);


--
-- Name: uploads uploads_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.uploads
    ADD CONSTRAINT uploads_pkey PRIMARY KEY (id);


--
-- Name: uploads uploads_uuid_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.uploads
    ADD CONSTRAINT uploads_uuid_key UNIQUE (uuid);


--
-- Name: files_job_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX files_job_type ON public.files USING btree (job_id, type);


--
-- Name: files_to_clean; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX files_to_clean ON public.files USING btree (job_id) WHERE ((deleted_at IS NULL) AND (type <> 'result'::text));


--
-- Name: jobs_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX jobs_active ON public.jobs USING btree (created_at) WHERE (status = ANY (ARRAY['queued'::text, 'running'::text]));


--
-- Name: tasks_job_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX tasks_job_status ON public.tasks USING btree (job_id, status);


--
-- Name: uploads_expiring; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX uploads_expiring ON public.uploads USING btree (expires_at) WHERE (status = 'open'::text);


--
-- Name: jobs jobs_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER jobs_updated_at BEFORE UPDATE ON public.jobs FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();


--
-- Name: settings settings_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER settings_updated_at BEFORE UPDATE ON public.settings FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();


--
-- Name: tasks tasks_updated_at; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER tasks_updated_at BEFORE UPDATE ON public.tasks FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();


--
-- Name: files files_job_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.files
    ADD CONSTRAINT files_job_id_fkey FOREIGN KEY (job_id) REFERENCES public.jobs(id);


--
-- Name: files files_produced_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.files
    ADD CONSTRAINT files_produced_by_fkey FOREIGN KEY (produced_by) REFERENCES public.tasks(id);


--
-- Name: task_files task_files_file_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_files
    ADD CONSTRAINT task_files_file_id_fkey FOREIGN KEY (file_id) REFERENCES public.files(id);


--
-- Name: task_files task_files_task_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_files
    ADD CONSTRAINT task_files_task_id_fkey FOREIGN KEY (task_id) REFERENCES public.tasks(id);


--
-- Name: tasks tasks_job_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.tasks
    ADD CONSTRAINT tasks_job_id_fkey FOREIGN KEY (job_id) REFERENCES public.jobs(id);


--
-- Name: uploads uploads_job_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.uploads
    ADD CONSTRAINT uploads_job_id_fkey FOREIGN KEY (job_id) REFERENCES public.jobs(id);


--
-- PostgreSQL database dump complete
--

\unrestrict dbmate


--
-- Dbmate schema migrations
--

INSERT INTO public.schema_migrations (version) VALUES
    ('20261018000001'),
    ('20261018000002'),
    ('20261019000001'),
    ('20261019000002'),
    ('20261020000001');
