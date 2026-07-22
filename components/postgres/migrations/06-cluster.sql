-- cluster_config holds the cluster-wide admission state in typed columns, so the hot-path health check compares plain integers instead of parsing JSON
-- It is a single-row table, managed through the provider and the ClusterAdmin, and never written directly by embedders
CREATE TABLE %scluster_config (
    -- Enforces a single row
    cluster_config_id integer NOT NULL PRIMARY KEY CHECK (cluster_config_id = 1),
    -- max_hosts is null until the first host claims it (a claimed value of 0 means no limit)
    max_hosts integer,
    -- exclusive_owner and exclusive_expires_at are null when no exclusive-access lease is held
    exclusive_owner text,
    exclusive_expires_at bigint
);

INSERT INTO %scluster_config (cluster_config_id) VALUES (1);
