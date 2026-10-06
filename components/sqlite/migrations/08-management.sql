-- The runtime replica that owns each host's session
-- It is null for embedded hosts, which are not connected through a runtime
ALTER TABLE %shosts ADD COLUMN host_runtime_id text;

-- The job retentions each host registered for an actor type, in milliseconds
ALTER TABLE %shost_actor_types ADD COLUMN completed_job_retention integer NOT NULL DEFAULT 0;
ALTER TABLE %shost_actor_types ADD COLUMN dead_lettered_job_retention integer NOT NULL DEFAULT 0;

-- Index on the created workflow label, used for creation-time range filtering
-- It follows the same json_extract expression as the other label indexes, which the query must repeat as-is for SQLite to use it
CREATE INDEX %sactor_state_wf_created_idx ON %sactor_state (actor_type, json_extract(workflow_labels, '$.created'), actor_id)
    WHERE workflow_labels IS NOT NULL;

-- Index on the job rows of the alarms table in job ID order, so jobs can be listed and counted without reading the plain alarms
CREATE INDEX %salarms_job_id_idx ON %salarms (alarm_id)
    WHERE alarm_kind = 'job';

-- Index on the status of terminal jobs in job ID order, so they can be listed and counted by status
CREATE INDEX %sterminal_jobs_status_idx ON %sterminal_jobs (job_status, job_id);

-- Append-only event history of workflow instances, keyed by the actor whose state holds the instance's journal
CREATE TABLE %sworkflow_events (
    -- Actor type
    actor_type text NOT NULL,
    -- Actor ID
    actor_id text NOT NULL,
    -- Sequence number of the event, starting at 1 for each actor
    event_seq integer NOT NULL,
    -- Time of the event, as a unix timestamp in milliseconds
    event_time integer NOT NULL,
    -- Event kind
    event_kind text NOT NULL,
    -- Event details, opaque to the provider
    event_data blob,

    PRIMARY KEY (actor_type, actor_id, event_seq)
) WITHOUT ROWID, STRICT;

-- Trigger that removes an actor's workflow events when its state row is deleted
CREATE TRIGGER %sactor_state_delete_workflow_events
AFTER DELETE ON %sactor_state
BEGIN
    DELETE FROM %sworkflow_events
    WHERE
        actor_type = OLD.actor_type
        AND actor_id = OLD.actor_id;
END;

-- Membership of the runtime replicas, each holding a renewable lease on its runtime ID
-- Times are Unix milliseconds, like the exclusive-access lease in cluster_config
CREATE TABLE %sruntimes (
    -- Unique ID of the runtime replica
    runtime_id text NOT NULL PRIMARY KEY,
    -- Peer address other replicas dial to reach this one
    runtime_address text NOT NULL,
    -- When the lease was last registered or renewed
    runtime_last_heartbeat integer NOT NULL,
    -- When the lease expires unless renewed
    runtime_expires_at integer NOT NULL
) WITHOUT ROWID, STRICT;

ALTER TABLE %shosts ADD COLUMN host_drain_token TEXT NOT NULL DEFAULT '';
