-- Hosts table
CREATE TABLE %shosts (
    host_id TEXT PRIMARY KEY NOT NULL,
    host_address TEXT NOT NULL,
    host_last_health_check INTEGER NOT NULL  -- unix timestamp in milliseconds
) WITHOUT ROWID, STRICT;

CREATE UNIQUE INDEX %sidx_hosts_address ON %shosts (host_address);

-- Host actor types table
CREATE TABLE %shost_actor_types (
    host_id TEXT NOT NULL,
    actor_type TEXT NOT NULL,
    actor_idle_timeout INTEGER NOT NULL,      -- milliseconds
    actor_concurrency_limit INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (host_id, actor_type)
) WITHOUT ROWID, STRICT;

-- Active actors table
CREATE TABLE %sactive_actors (
    actor_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    host_id TEXT NOT NULL,
    actor_idle_timeout INTEGER NOT NULL,      -- milliseconds
    actor_activation INTEGER NOT NULL,        -- unix timestamp in milliseconds
    PRIMARY KEY (actor_type, actor_id)
) WITHOUT ROWID, STRICT;

-- Alarms table
CREATE TABLE %salarms (
    alarm_id TEXT PRIMARY KEY NOT NULL,
    actor_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    alarm_name TEXT NOT NULL,
    alarm_due_time INTEGER NOT NULL,          -- unix timestamp in milliseconds
    alarm_interval TEXT,                      -- ISO8601 duration string
    alarm_ttl_time INTEGER,                   -- unix timestamp in milliseconds
    alarm_data BLOB,
    alarm_lease_id TEXT,
    alarm_lease_expiration_time INTEGER       -- unix timestamp in milliseconds
) WITHOUT ROWID, STRICT;

CREATE UNIQUE INDEX %sidx_alarms_ref ON %salarms (actor_type, actor_id, alarm_name);

-- Actor state table
CREATE TABLE %sactor_state (
    actor_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    actor_state_data BLOB NOT NULL,
    actor_state_expiration_time INTEGER,      -- unix timestamp in milliseconds
    PRIMARY KEY (actor_type, actor_id)
) WITHOUT ROWID, STRICT;
