-- Ensure required extensions
-- CREATE EXTENSION IF NOT EXISTS is not safe under concurrency: two sessions can both observe the extension as missing and then both insert into pg_catalog.pg_extension, so one fails with a unique-constraint violation
-- Several hosts can initialize the schema against the same fresh database at once, so the concurrent-creation error is swallowed here
DO $$
BEGIN
    CREATE EXTENSION IF NOT EXISTS pgcrypto;
EXCEPTION WHEN unique_violation OR duplicate_object THEN
    NULL;
END
$$;

-- Contains the active actor hosts
CREATE TABLE %shosts (
    -- ID of the host
    host_id uuid NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Address and port of the host
    host_address text NOT NULL,
    -- Last health check received
    -- Stored as UTC
    host_last_health_check timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
);

CREATE UNIQUE INDEX %shost_address_idx ON %shosts (host_address);
CREATE INDEX %shost_last_health_check_idx ON %shosts (host_last_health_check);

-- Contains the actor types supported by each host
CREATE TABLE %shost_actor_types (
    -- ID of the host
    -- References the "hosts" table
    host_id uuid NOT NULL,
    -- Actor type name
    actor_type text NOT NULL,
    -- Idle timeout
    actor_idle_timeout interval NOT NULL,
    -- Maximum number of concurrent actors of this type that can be active on the host
    -- A value of 0 means no limit
    actor_concurrency_limit int NOT NULL DEFAULT 0,

    PRIMARY KEY (host_id, actor_type),
    FOREIGN KEY (host_id) REFERENCES %shosts (host_id) ON DELETE CASCADE
);

CREATE INDEX %sactor_type_idx ON %shost_actor_types (actor_type);

-- Contains the actors currently active on a host
CREATE TABLE %sactive_actors (
    -- Actor type
    actor_type text NOT NULL,
    -- Actor ID
    actor_id text NOT NULL,
    -- Host on which the actor is active
    host_id uuid NOT NULL,
    -- Idle timeout
    -- This is copied from the host_actor_types table
    actor_idle_timeout interval NOT NULL,
    -- Time the actor was originally activated at
    -- Stored as UTC
    actor_activation timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),

    PRIMARY KEY (actor_type, actor_id),
    FOREIGN KEY (host_id) REFERENCES %shosts (host_id) ON DELETE CASCADE
);

CREATE INDEX %sactive_actors_host_scan_idx ON %sactive_actors (host_id, actor_type);

-- Reports the active actors per each host
CREATE VIEW %shost_active_actor_count
	(host_id, actor_type, active_count)
AS
    SELECT host_id, actor_type, COUNT(*)
    FROM %sactive_actors
    GROUP BY host_id, actor_type;

-- Contains the state for each actor
CREATE TABLE %sactor_state (
    -- Actor type
    actor_type text NOT NULL,
    -- Actor ID
    actor_id text NOT NULL,
    -- State data
    actor_state_data bytea NOT NULL,
    -- If set, indicates the expiration time for this state
    -- Expired rows are automatically garbage collected
    -- Stored as UTC
    actor_state_expiration_time timestamp,

    PRIMARY KEY (actor_type, actor_id)
);

CREATE INDEX %sactor_state_expiration_time_idx ON %sactor_state (actor_state_expiration_time)
    WHERE actor_state_expiration_time IS NOT NULL;

-- Contains the list of alarms created
CREATE TABLE %salarms (
    -- Alarm ID
    alarm_id uuid PRIMARY KEY NOT NULL DEFAULT gen_random_uuid(),
    -- Actor type
    actor_type text NOT NULL,
    -- Actor ID
    actor_id text NOT NULL,
    -- Name of the alarm
    alarm_name text NOT NULL,
    -- Due time for the alarm
    -- Stored as UTC
    alarm_due_time timestamp NOT NULL,
    -- For repeating alarms, as an ISO8601-formatted duration string.
    alarm_interval text,
    -- For repeating alarms, time at which to stop repeating
    -- Stored as UTC
    alarm_ttl_time timestamp,
    -- Optional data associated with the alarm
    alarm_data bytea,
    -- For alarms that have been fetched and have a lease, this is a unique ID for the lease
    alarm_lease_id uuid,
    -- For alarms that have been fetched and have a lease, indicates the time the lease expires
    -- Note that leases can be renewed
    -- Stored as UTC
    alarm_lease_expiration_time timestamp
);

CREATE UNIQUE INDEX %salarm_ref_idx ON %salarms (actor_type, actor_id, alarm_name);
CREATE INDEX %salarm_due_time_idx ON %salarms (alarm_due_time);
CREATE UNIQUE INDEX %salarm_lease_id_idx ON %salarms (alarm_lease_id)
    WHERE alarm_lease_id IS NOT NULL;

-- Trigger that nullifies the leases on the alarms table when an actor is deactivated
-- (deleted from the active_actors table)
CREATE OR REPLACE FUNCTION %sactive_actors_delete_update_alarms_fn()
RETURNS trigger AS $$
BEGIN
    -- Batch update: join alarms with the transition table of deleted rows
    UPDATE %salarms
    SET
        alarm_lease_id = NULL,
        alarm_lease_expiration_time = NULL
    FROM old_rows r
    WHERE
        %salarms.actor_type = r.actor_type
        AND %salarms.actor_id = r.actor_id
        AND %salarms.alarm_lease_id IS NOT NULL;

    -- Statement-level trigger return value is ignored
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Use a statement-level trigger with a transition table, so the function runs once per DELETE statement and can operate on all deleted rows in a single batch.
CREATE TRIGGER %sactive_actors_delete_update_alarms
AFTER DELETE ON %sactive_actors
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT
EXECUTE FUNCTION %sactive_actors_delete_update_alarms_fn();

-- Used for creating IDs for advisory locks
-- Adapted from https://stackoverflow.com/a/9812029/192024
CREATE FUNCTION %sh_bigint(text)
RETURNS bigint AS $$
    SELECT ('x' || substr(encode(sha256($1::bytea), 'hex'), 1, 16))::bit(64)::bigint;
$$ LANGUAGE sql;
