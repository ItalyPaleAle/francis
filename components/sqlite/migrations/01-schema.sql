-- Contains the active actor hosts
CREATE TABLE hosts (
    -- ID of the host, as UUID
    host_id text NOT NULL PRIMARY KEY,
    -- Address and port of the host
    host_address text NOT NULL,
    -- Last health check received, as unix timestamp in milliseconds
    host_last_health_check integer NOT NULL DEFAULT (unixepoch('subsec') * 1000)
) WITHOUT ROWID, STRICT;

CREATE UNIQUE INDEX host_address_idx ON hosts (host_address);
CREATE INDEX host_last_health_check_idx ON hosts (host_last_health_check);

-- Contains the actor types supported by each host
CREATE TABLE host_actor_types (
    -- ID of the host, as UUID
    -- References the "hosts" table
    host_id text NOT NULL,
    -- Actor type name
    actor_type text NOT NULL,
    -- Idle timeout, in milliseconds
    actor_idle_timeout integer NOT NULL,
    -- Maximum number of concurrent actors of this type that can be active on the host
    -- A value of 0 means no limit
    actor_concurrency_limit int NOT NULL DEFAULT 0,

    PRIMARY KEY (host_id, actor_type),
    FOREIGN KEY (host_id) REFERENCES hosts (host_id) ON DELETE CASCADE
) WITHOUT ROWID, STRICT;

CREATE INDEX actor_type_idx ON host_actor_types (actor_type);

-- Contains the actors currently active on a host
CREATE TABLE active_actors (
    -- Actor type
    actor_type text NOT NULL,
    -- Actor ID
    actor_id text NOT NULL,
    -- Host on which the actor is active
    host_id text NOT NULL,
    -- Idle timeout, in milliseconds
    -- This is copied from the host_actor_types table
    actor_idle_timeout integer NOT NULL,
    -- Time the actor was originally activated at, as a unix timestmap in milliseconds
    actor_activation integer NOT NULL DEFAULT (unixepoch('subsec') * 1000),

    PRIMARY KEY (actor_type, actor_id),
    FOREIGN KEY (host_id) REFERENCES hosts (host_id) ON DELETE CASCADE
) WITHOUT ROWID, STRICT;

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
    actor_state_expiration_time integer,

    PRIMARY KEY (actor_type, actor_id)
) WITHOUT ROWID, STRICT;

CREATE INDEX actor_state_expiration_time_idx ON actor_state (actor_state_expiration_time) WHERE actor_state_expiration_time IS NOT NULL;

-- Contains the list of alarms created
CREATE TABLE alarms (
    -- Alarm ID, as UUID
    alarm_id text PRIMARY KEY NOT NULL, 
    -- Actor type
    actor_type text NOT NULL,
    -- Actor ID
    actor_id text NOT NULL,
    -- Name of the alarm
    alarm_name text NOT NULL,
    -- Due time for the alarm, as a unix timestamp in milliseconds
    alarm_due_time integer NOT NULL,
    -- For repeating alarms, string indicating the interval
    -- This could be an ISO duration or Go duration
    alarm_interval text,
    -- For repeating alarms, time at which to stop repeating
    -- This is a unix timestamp in millisecond
    alarm_ttl_time integer,
    -- Optional data associated with the alarm
    alarm_data blob,
    -- For alarms that have been fetched and have a lease, this is a unique ID for the lease
    alarm_lease_id text,
    -- For alarms that have been fetched and have a lease, indicates the time the lease expires
    -- Note that leases can be renewed
    -- This is a unix timestamp in millisecond
    alarm_lease_expiration_time integer,
    -- For alarms that have been fetched and have a lease, this is the "process id" of the owner of the lease
    alarm_lease_pid text
) WITHOUT ROWID, STRICT;

CREATE UNIQUE INDEX alarm_ref_idx ON alarms (actor_type, actor_id, alarm_name);
CREATE INDEX alarm_due_time_idx ON alarms (alarm_due_time);
CREATE INDEX alarm_lease_pid_idx ON alarms (alarm_lease_pid);
