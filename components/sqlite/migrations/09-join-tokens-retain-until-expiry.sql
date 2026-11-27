-- Recreate consumed_join_tokens without the ON DELETE CASCADE foreign key to hosts so consumed jti values remain blocked until expires_at
DROP TABLE %sconsumed_join_tokens;

CREATE TABLE %sconsumed_join_tokens (
    join_token text NOT NULL PRIMARY KEY,
    -- Unix timestamp in milliseconds
    expires_at integer NOT NULL
) WITHOUT ROWID, STRICT;

CREATE INDEX %sconsumed_join_tokens_expires_at_idx ON %sconsumed_join_tokens (expires_at);
