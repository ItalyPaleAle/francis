-- The dead-letter store becomes the terminal-job store, holding jobs that completed as well as jobs that failed
-- The standalone provider serves every query from memory, so this table is only the durable copy
ALTER TABLE %sdead_jobs RENAME TO %sterminal_jobs;
ALTER TABLE %sterminal_jobs RENAME COLUMN failed_at TO ended_at;
ALTER TABLE %sterminal_jobs ADD COLUMN job_status TEXT NOT NULL DEFAULT 'dead';
ALTER TABLE %sterminal_jobs ADD COLUMN expiration_time timestamp;

DROP INDEX %sdead_jobs_actor_idx;
CREATE INDEX %sterminal_jobs_actor_idx ON %sterminal_jobs (actor_type, actor_id);
