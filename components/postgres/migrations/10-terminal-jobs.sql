-- The dead-letter store becomes the terminal-job store, holding jobs that completed as well as jobs that failed
-- A job's record can now outlive its occurrence either way, which is what lets an operator see that a job ran at all rather than only that one failed
ALTER TABLE %sdead_jobs RENAME TO %pterminal_jobs;
ALTER TABLE %sterminal_jobs RENAME COLUMN failed_at TO ended_at;

-- How the job ended: 'completed' or 'dead'
-- Every row that existed before this migration was dead-lettered, so we set a default to start
-- The default only exists to backfill the rows that were already there, so we drop it right after
ALTER TABLE %sterminal_jobs ADD COLUMN job_status text NOT NULL DEFAULT 'dead';
ALTER TABLE %sterminal_jobs ALTER COLUMN job_status DROP DEFAULT;

-- If set, the time after which the record is garbage collected (as UTC)
ALTER TABLE %sterminal_jobs ADD COLUMN expiration_time timestamp;

DROP INDEX %sdead_jobs_actor_idx;
CREATE INDEX %pterminal_jobs_actor_idx ON %sterminal_jobs (actor_type, actor_id);
CREATE INDEX %pterminal_jobs_expiration_time_idx ON %sterminal_jobs (expiration_time)
    WHERE expiration_time IS NOT NULL;
