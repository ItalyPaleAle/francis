-- The runtime replica that owns each host's session
-- It is null for embedded hosts, which are not connected through a runtime
ALTER TABLE %shosts ADD COLUMN host_runtime_id text;

-- The job retentions each host registered for an actor type, stored like the idle timeou
ALTER TABLE %shost_actor_types ADD COLUMN actor_completed_job_retention interval NOT NULL DEFAULT '0';
ALTER TABLE %shost_actor_types ADD COLUMN actor_dead_lettered_job_retention interval NOT NULL DEFAULT '0';

-- Index on the created workflow label, so instances can be listed by creation time range
CREATE INDEX %pactor_state_wf_created_idx ON %sactor_state (actor_type, (workflow_labels->>'created'), actor_id)
    WHERE workflow_labels IS NOT NULL;

-- Append-only event history of workflow instances, written together with the state row it belongs to
CREATE TABLE %sworkflow_events (
    -- Actor type of the state row the event belongs to
    actor_type text NOT NULL,
    -- Actor ID of the state row the event belongs to
    actor_id text NOT NULL,
    -- Sequence number of the event, starting at 1 and unique per actor
    event_seq bigint NOT NULL,
    -- Time the event happened
    -- Stored as UTC
    event_time timestamp NOT NULL,
    -- Kind of the event
    event_kind text NOT NULL,
    -- Event details, opaque to the provider
    event_data bytea,

    PRIMARY KEY (actor_type, actor_id, event_seq)
);

-- Trigger that removes the events of every deleted actor state row
CREATE OR REPLACE FUNCTION %sactor_state_delete_workflow_events_fn()
RETURNS trigger AS $$
BEGIN
    -- Batch delete: join the events with the transition table of deleted rows
    DELETE FROM %sworkflow_events AS e
    USING old_rows AS r
    WHERE
        e.actor_type = r.actor_type
        AND e.actor_id = r.actor_id;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Use a statement-level trigger with a transition table, so the function runs once per DELETE statement and removes the events of all deleted rows in a single batch
CREATE TRIGGER %pactor_state_delete_workflow_events
AFTER DELETE ON %sactor_state
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT
EXECUTE FUNCTION %sactor_state_delete_workflow_events_fn();

-- Membership of the runtime replicas, each holding a renewable lease on its runtime ID
-- Times are Unix milliseconds in the database clock, like the exclusive-access lease in cluster_config
CREATE TABLE %sruntimes (
    -- Unique ID of the runtime replica
    runtime_id text NOT NULL PRIMARY KEY,
    -- Peer address other replicas dial to reach this one
    runtime_address text NOT NULL,
    -- When the lease was last registered or renewed
    runtime_last_heartbeat bigint NOT NULL,
    -- When the lease expires unless renewed
    runtime_expires_at bigint NOT NULL
);
