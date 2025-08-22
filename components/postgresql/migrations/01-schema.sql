CREATE TABLE hosts (
    host_id uuid NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
    host_address text NOT NULL,
    host_last_healthcheck timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE host_actor_types (
    host_id uuid NOT NULL,
    actor_type text NOT NULL,
    actor_idle_timeout interval NOT NULL,
    actor_concurrency_limit int NOT NULL DEFAULT 0,
    actor_alarm_concurrency_limit int NOT NULL DEFAULT 0,

    PRIMARY KEY (host_id, actor_type),
    FOREIGN KEY (host_id) REFERENCES hosts (host_id) ON DELETE CASCADE
);

CREATE TABLE actors (
    actor_id text NOT NULL,
    actor_type text NOT NULL,
    host_id uuid PRIMARY KEY,
    actor_idle_timeout integer NOT NULL,
    actor_activation timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (actor_type, actor_id),
    FOREIGN KEY (host_id) REFERENCES hosts (host_id) ON DELETE CASCADE
);
