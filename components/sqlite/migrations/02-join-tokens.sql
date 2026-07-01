-- consumed_join_tokens records each JWT jti that has been used to register a host
-- Rows are deleted automatically when the host is removed (graceful unregister, stale GC, or health failure) via the CASCADE FK
-- Expired rows are pruned lazily during RegisterHost to avoid a dedicated background job
CREATE TABLE %sconsumed_join_tokens (
    join_token text NOT NULL PRIMARY KEY,
    host_id text NOT NULL REFERENCES %shosts(host_id) ON DELETE CASCADE,
    -- This is a unix timestamp in millisecond
    expires_at integer NOT NULL
) WITHOUT ROWID, STRICT;
