-- Add the job discriminator and job-specific columns to the alarms table
ALTER TABLE %salarms ADD COLUMN alarm_kind TEXT NOT NULL DEFAULT 'alarm';
ALTER TABLE %salarms ADD COLUMN job_method TEXT;
ALTER TABLE %salarms ADD COLUMN alarm_cron TEXT;

-- Dead-lettered jobs
CREATE TABLE %sdead_jobs (
    job_id TEXT PRIMARY KEY NOT NULL,   -- equal to the original alarm_id (as UUID)
    actor_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    job_method TEXT NOT NULL,
    job_data BLOB,
    attempts INTEGER NOT NULL,
    last_error TEXT,
    failed_at INTEGER NOT NULL,         -- unix timestamp in milliseconds
    original_due INTEGER NOT NULL,      -- unix timestamp in milliseconds
    job_interval TEXT,                  -- ISO8601 duration string
    job_cron TEXT
) WITHOUT ROWID, STRICT;

CREATE INDEX %sdead_jobs_actor_idx ON %sdead_jobs (actor_type, actor_id);
