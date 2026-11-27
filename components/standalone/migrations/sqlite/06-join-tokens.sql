-- Consumed JWT join tokens (jti) retained until their expiration time
CREATE TABLE %sconsumed_join_tokens (
    join_token TEXT PRIMARY KEY NOT NULL,
    expires_at INTEGER NOT NULL -- unix timestamp in milliseconds
) WITHOUT ROWID, STRICT;

CREATE INDEX %sconsumed_join_tokens_expires_at_idx ON %sconsumed_join_tokens (expires_at);
