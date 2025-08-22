CREATE TABLE hosts (
    host_id text NOT NULL PRIMARY KEY,
    host_address text NOT NULL,
    host_last_health_check timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX host_address_idx ON hosts (host_address);
CREATE INDEX host_last_health_check_idx ON hosts (host_last_health_check);

CREATE TABLE host_actor_types (
    host_id text NOT NULL,
    actor_type text NOT NULL,
    actor_idle_timeout interval NOT NULL,
    actor_concurrency_limit int NOT NULL DEFAULT 0,
    actor_alarm_concurrency_limit int NOT NULL DEFAULT 0,

    PRIMARY KEY (host_id, actor_type),
    FOREIGN KEY (host_id) REFERENCES hosts (host_id) ON DELETE CASCADE
);

CREATE INDEX actor_type_idx ON host_actor_types (actor_type);

CREATE TABLE actors (
    actor_id text NOT NULL,
    actor_type text NOT NULL,
    host_id text,
    actor_idle_timeout integer NOT NULL,
    actor_activation timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (actor_type, actor_id),
    FOREIGN KEY (host_id) REFERENCES hosts (host_id) ON DELETE CASCADE
);
