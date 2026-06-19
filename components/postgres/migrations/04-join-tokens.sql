-- consumed_join_tokens records each JWT jti that has been used to register a host
-- Rows are deleted automatically when the host is removed (graceful unregister, stale GC, or health failure) via the CASCADE FK
-- Expired rows are pruned lazily during RegisterHost to avoid a dedicated background job
CREATE TABLE consumed_join_tokens (
    join_token text PRIMARY KEY,
    host_id uuid NOT NULL REFERENCES hosts(host_id) ON DELETE CASCADE,
    expires_at timestamp NOT NULL
);
