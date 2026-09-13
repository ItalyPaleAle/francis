-- The dead-letter store becomes the terminal-job store, holding jobs that completed as well as jobs that failed
-- A job's record can now outlive its occurrence either way, which is what lets an operator see that a job ran at all rather than only that one failed
ALTER TABLE %sdead_jobs RENAME TO %sterminal_jobs;
ALTER TABLE %sterminal_jobs RENAME COLUMN failed_at TO ended_at;

-- How the job ended: 'completed' or 'dead'
-- Every row that existed before this migration was dead-lettered, which is what the default records
ALTER TABLE %sterminal_jobs ADD COLUMN job_status text NOT NULL DEFAULT 'dead';

-- If set, the time after which the record is garbage collected
-- Stored as UTC
-- A record with no expiration is kept until something removes it, which is what a job whose actor type asked for no retention gets
ALTER TABLE %sterminal_jobs ADD COLUMN expiration_time timestamp;

DROP INDEX %sdead_jobs_actor_idx;
CREATE INDEX %sterminal_jobs_actor_idx ON %sterminal_jobs (actor_type, actor_id);
CREATE INDEX %sterminal_jobs_expiration_time_idx ON %sterminal_jobs (expiration_time)
    WHERE expiration_time IS NOT NULL;
