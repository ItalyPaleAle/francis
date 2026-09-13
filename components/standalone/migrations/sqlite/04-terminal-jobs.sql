-- The dead-letter store becomes the terminal-job store, holding jobs that completed as well as jobs that failed
-- The standalone provider serves every query from memory, so this table is only the durable copy
--
-- The table is re-created and copied rather than renamed in place, for the same reason as the SQLite provider's own migration: ALTER TABLE ... RENAME opens the connection's temporary database
CREATE TABLE %sterminal_jobs (
    job_id TEXT PRIMARY KEY NOT NULL,   -- equal to the original alarm_id (as UUID)
    actor_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    job_method TEXT NOT NULL,
    job_data BLOB,
    job_status TEXT NOT NULL,           -- 'completed' or 'dead'
    attempts INTEGER NOT NULL,
    last_error TEXT,
    ended_at INTEGER NOT NULL,          -- unix timestamp in milliseconds
    original_due INTEGER NOT NULL,      -- unix timestamp in milliseconds
    job_interval TEXT,                  -- ISO8601 duration string
    job_cron TEXT,
    expiration_time INTEGER             -- unix timestamp in milliseconds, null when the record is kept until something removes it
) WITHOUT ROWID, STRICT;

INSERT INTO %sterminal_jobs
    (job_id, actor_type, actor_id, job_method, job_data, job_status, attempts, last_error, ended_at, original_due, job_interval, job_cron, expiration_time)
SELECT
    job_id, actor_type, actor_id, job_method, job_data, 'dead', attempts, last_error, failed_at, original_due, job_interval, job_cron, NULL
FROM %sdead_jobs;

DROP TABLE %sdead_jobs;

CREATE INDEX %sterminal_jobs_actor_idx ON %sterminal_jobs (actor_type, actor_id);
