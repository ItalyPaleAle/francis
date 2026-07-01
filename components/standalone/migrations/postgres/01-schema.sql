-- Hosts table
CREATE TABLE %shosts (
    host_id UUID PRIMARY KEY NOT NULL,
    host_address TEXT NOT NULL UNIQUE,
    host_last_health_check TIMESTAMP NOT NULL  -- stored as UTC
);

-- Host actor types table
CREATE TABLE %shost_actor_types (
    host_id UUID NOT NULL,
    actor_type TEXT NOT NULL,
    actor_idle_timeout BIGINT NOT NULL,           -- milliseconds
    actor_concurrency_limit INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (host_id, actor_type)
);

-- Active actors table
CREATE TABLE %sactive_actors (
    actor_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    host_id UUID NOT NULL,
    actor_idle_timeout BIGINT NOT NULL,           -- milliseconds
    actor_activation TIMESTAMP NOT NULL,          -- stored as UTC
    PRIMARY KEY (actor_type, actor_id)
);

-- Alarms table
CREATE TABLE %salarms (
    alarm_id UUID PRIMARY KEY NOT NULL,
    actor_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    alarm_name TEXT NOT NULL,
    alarm_due_time TIMESTAMP NOT NULL,            -- stored as UTC
    alarm_interval TEXT,                          -- ISO8601 duration string
    alarm_ttl_time TIMESTAMP,                     -- stored as UTC
    alarm_data BYTEA,
    alarm_lease_id TEXT,
    alarm_lease_expiration_time TIMESTAMP,        -- stored as UTC
    UNIQUE (actor_type, actor_id, alarm_name)
);

-- Actor state table
CREATE TABLE %sactor_state (
    actor_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    actor_state_data BYTEA NOT NULL,
    actor_state_expiration_time TIMESTAMP,        -- stored as UTC
    PRIMARY KEY (actor_type, actor_id)
);
