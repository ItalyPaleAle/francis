-- cluster_config holds the cluster-wide admission state (host limit and exclusive-access lease) in typed columns
-- It is a single-row table, managed through the provider, and never written directly by embedders
CREATE TABLE %scluster_config (
    -- Enforces a single row
    cluster_config_id INTEGER NOT NULL PRIMARY KEY CHECK (cluster_config_id = 1),
    -- max_hosts is null until the first host claims it (a claimed value of 0 means no limit)
    max_hosts INTEGER,
    -- exclusive_owner and exclusive_expires_at are null when no exclusive-access lease is held
    exclusive_owner TEXT,
    exclusive_expires_at INTEGER  -- unix timestamp in milliseconds
) STRICT;

INSERT INTO %scluster_config (cluster_config_id) VALUES (1);
