-- The dead-letter store becomes the terminal-job store, holding jobs that completed as well as jobs that failed
-- A job's record can now outlive its occurrence either way, which is what lets an operator see that a job ran at all rather than only that one failed
-- Here, we need to re-create the table and copy the data
CREATE TABLE %sterminal_jobs (
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
    -- How the job ended: 'completed' or 'dead'
    job_status text NOT NULL,
    -- Number of attempts the occurrence took
    attempts integer NOT NULL,
    -- Last error recorded for a job that dead-lettered, and null for one that completed
    last_error text,
    -- Time the job ended, as a unix timestamp in milliseconds
    ended_at integer NOT NULL,
    -- Original due time of the occurrence, as a unix timestamp in milliseconds
    original_due integer NOT NULL,
    -- Repetition interval of the original job, if any
    job_interval text,
    -- Cron schedule of the original job, if any
    job_cron text,
    -- If set, the time after which the record is garbage collected, as a unix timestamp in milliseconds
    expiration_time integer
) WITHOUT ROWID, STRICT;

-- Copy the data
-- Every row that existed before this migration was dead-lettered
INSERT INTO %sterminal_jobs
    (job_id, actor_type, actor_id, job_method, job_data, job_status, attempts, last_error, ended_at, original_due, job_interval, job_cron, expiration_time)
SELECT
    job_id, actor_type, actor_id, job_method, job_data, 'dead', attempts, last_error, failed_at, original_due, job_interval, job_cron, NULL
FROM %sdead_jobs;

-- Drop the old table
DROP TABLE %sdead_jobs;

CREATE INDEX %sterminal_jobs_actor_idx ON %sterminal_jobs (actor_type, actor_id);
CREATE INDEX %sterminal_jobs_expiration_time_idx ON %sterminal_jobs (expiration_time)
    WHERE expiration_time IS NOT NULL;
