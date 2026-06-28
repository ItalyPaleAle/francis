-- Add the job discriminator and job-specific columns to the alarms table
-- Jobs ride on the existing alarm engine and are stored as alarm rows with alarm_kind = 'job'
ALTER TABLE alarms ADD COLUMN alarm_kind text NOT NULL DEFAULT 'alarm';
ALTER TABLE alarms ADD COLUMN job_method text;
ALTER TABLE alarms ADD COLUMN alarm_cron text;

-- Holds jobs that exhausted their retries or failed permanently
-- A dead job preserves the original alarm_id as its job_id so it can be looked up and replayed
CREATE TABLE dead_jobs (
    -- Job ID, equal to the original alarm_id (as UUID)
    job_id text PRIMARY KEY NOT NULL,
    -- Actor type
    actor_type text NOT NULL,
    -- Actor ID
    actor_id text NOT NULL,
    -- Job handler method
    job_method text NOT NULL,
    -- Opaque job input data
    job_data blob,
    -- Number of attempts made before dead-lettering
    attempts integer NOT NULL,
    -- Last error recorded for the job
    last_error text,
    -- Time the job was dead-lettered, as a unix timestamp in milliseconds
    failed_at integer NOT NULL,
    -- Original due time of the failed occurrence, as a unix timestamp in milliseconds
    original_due integer NOT NULL,
    -- Repetition interval of the original job, if any
    job_interval text,
    -- Cron schedule of the original job, if any
    job_cron text
) WITHOUT ROWID, STRICT;

CREATE INDEX dead_jobs_actor_idx ON dead_jobs (actor_type, actor_id);
