-- Add the job discriminator and job-specific columns to the alarms table
ALTER TABLE %salarms ADD COLUMN alarm_kind TEXT NOT NULL DEFAULT 'alarm';
ALTER TABLE %salarms ADD COLUMN job_method TEXT;
ALTER TABLE %salarms ADD COLUMN alarm_cron TEXT;

-- Dead-lettered jobs
CREATE TABLE %sdead_jobs (
    job_id UUID PRIMARY KEY NOT NULL,   -- equal to the original alarm_id
    actor_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    job_method TEXT NOT NULL,
    job_data BYTEA,
    attempts INTEGER NOT NULL,
    last_error TEXT,
    failed_at TIMESTAMP NOT NULL,       -- stored as UTC
    original_due TIMESTAMP NOT NULL,    -- stored as UTC
    job_interval TEXT,                  -- ISO8601 duration string
    job_cron TEXT
);

CREATE INDEX %sdead_jobs_actor_idx ON %sdead_jobs (actor_type, actor_id);
