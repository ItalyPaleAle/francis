CREATE TABLE hosts (
    host_id uuid NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    host_address text NOT NULL,
    host_last_health_check timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX host_address_idx ON hosts (host_address);
CREATE INDEX host_last_health_check_idx ON hosts (host_last_health_check);

CREATE TABLE host_actor_types (
    host_id uuid NOT NULL,
    actor_type text NOT NULL,
    actor_idle_timeout interval NOT NULL,
    actor_concurrency_limit int NOT NULL DEFAULT 0,

    PRIMARY KEY (host_id, actor_type),
    FOREIGN KEY (host_id) REFERENCES hosts (host_id) ON DELETE CASCADE
);

CREATE INDEX actor_type_idx ON host_actor_types (actor_type);

CREATE TABLE active_actors (
    actor_type text NOT NULL,
    actor_id text NOT NULL,
    host_id uuid NOT NULL,
    actor_idle_timeout interval NOT NULL,
    actor_activation timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (actor_type, actor_id),
    FOREIGN KEY (host_id) REFERENCES hosts (host_id) ON DELETE CASCADE
);

CREATE TABLE actor_state (
    actor_type text NOT NULL,
    actor_id text NOT NULL,
    actor_state_data blob NOT NULL,
    actor_state_expiration_time timestamp,

    PRIMARY KEY (actor_type, actor_id)
);

CREATE INDEX actor_state_expiration_time_idx ON actor_state (actor_state_expiration_time) WHERE actor_state_expiration_time IS NOT NULL;
