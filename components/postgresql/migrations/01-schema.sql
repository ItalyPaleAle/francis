-- Contains the active actor hosts
CREATE TABLE hosts (
    -- ID of the host
    host_id uuid NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Address and port of the host
    host_address text NOT NULL,
    -- Last health check received
    host_last_health_check timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX host_address_idx ON hosts (host_address);
CREATE INDEX host_last_health_check_idx ON hosts (host_last_health_check);

-- Contains the actor types supported by each host
CREATE TABLE host_actor_types (
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
    FOREIGN KEY (host_id) REFERENCES hosts (host_id) ON DELETE CASCADE
);

CREATE INDEX actor_type_idx ON host_actor_types (actor_type);

-- Contains the actors currently active on a host
CREATE TABLE active_actors (
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
    actor_activation timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (actor_type, actor_id),
    FOREIGN KEY (host_id) REFERENCES hosts (host_id) ON DELETE CASCADE
);

CREATE INDEX active_actors_host_scan_idx ON active_actors (host_id, actor_type);

-- Reports the active actors per each host
CREATE VIEW host_active_actor_count
	(host_id, actor_type, active_count)
AS
    SELECT host_id, actor_type, COUNT(*)
    FROM active_actors
    GROUP BY host_id, actor_type;

-- Contains the state for each actor
CREATE TABLE actor_state (
    -- Actor type
    actor_type text NOT NULL,
    -- Actor ID
    actor_id text NOT NULL,
    -- State data
    actor_state_data blob NOT NULL,
    -- If set, indicates the expiration time for this state
    -- Expired rows are automatically garbage collected
    actor_state_expiration_time timestamp,

    PRIMARY KEY (actor_type, actor_id)
);

CREATE INDEX actor_state_expiration_time_idx ON actor_state (actor_state_expiration_time)
    WHERE actor_state_expiration_time IS NOT NULL;

-- Contains the list of alarms created
CREATE TABLE alarms (
    -- Alarm ID
    alarm_id uuid PRIMARY KEY NOT NULL DEFAULT gen_random_uuid(),
    -- Actor type
    actor_type text NOT NULL,
    -- Actor ID
    actor_id text NOT NULL,
    -- Name of the alarm
    alarm_name text NOT NULL,
    -- Due time for the alarm
    alarm_due_time timestamp NOT NULL,
    -- For repeating alarms, as an ISO8601-formatted duration string.
    alarm_interval text,
    -- For repeating alarms, time at which to stop repeating
    alarm_ttl_time timestamp,
    -- Optional data associated with the alarm
    alarm_data blob,
    -- For alarms that have been fetched and have a lease, this is a unique ID for the lease
    alarm_lease_id uuid,
    -- For alarms that have been fetched and have a lease, indicates the time the lease expires
    -- Note that leases can be renewed
    alarm_lease_expiration_time timestamp
);

CREATE UNIQUE INDEX alarm_ref_idx ON alarms (actor_type, actor_id, alarm_name);
CREATE INDEX alarm_due_time_idx ON alarms (alarm_due_time);
CREATE UNIQUE INDEX alarm_lease_id_idx ON alarms (alarm_lease_id)
    WHERE alarm_lease_id IS NOT NULL;

-- Trigger that nullifies the leases on the alarms table when an actor is deactivated
-- (deleted from the active_actors table)
CREATE TRIGGER active_actors_delete_update_alarms
AFTER DELETE ON active_actors
BEGIN
    UPDATE alarms
    SET
        alarm_lease_id = NULL,
        alarm_lease_expiration_time = NULL
    WHERE
        actor_type = OLD.actor_type
        AND actor_id = OLD.actor_id
        AND alarm_lease_id IS NOT NULL;
END;
