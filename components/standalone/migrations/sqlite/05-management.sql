-- The job retentions each host registered for an actor type, in milliseconds like the idle timeout, with the sign preserved since zero and negative values are meaningful
-- Rows that existed before this migration get zero, as if they had not asked for a retention
ALTER TABLE %shost_actor_types ADD COLUMN actor_completed_job_retention INTEGER NOT NULL DEFAULT 0;
ALTER TABLE %shost_actor_types ADD COLUMN actor_dead_lettered_job_retention INTEGER NOT NULL DEFAULT 0;

-- The append-only event history of workflow instances, keyed by the actor whose state holds the instance's journal
-- The standalone provider serves every query from memory, so this table is only the durable copy
-- Events are removed by the provider together with the state row they belong to, rather than by a trigger, since REPLACE INTO deletes and re-inserts state rows on every write
CREATE TABLE %sworkflow_events (
    actor_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    event_seq INTEGER NOT NULL,               -- sequence number, starting at 1 for each actor
    event_time INTEGER NOT NULL,              -- unix timestamp in milliseconds
    event_kind TEXT NOT NULL,
    event_data BLOB,                          -- opaque to the provider
    PRIMARY KEY (actor_type, actor_id, event_seq)
) WITHOUT ROWID, STRICT;
