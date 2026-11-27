-- Retain consumed join tokens until their own expiry rather than deleting them when the host unregisters
-- Dropping host_id also removes its ON DELETE CASCADE foreign key so a short-lived host's JWT jti cannot be replayed after the host leaves
ALTER TABLE %sconsumed_join_tokens DROP COLUMN host_id;

CREATE INDEX %pconsumed_join_tokens_expires_at_idx ON %sconsumed_join_tokens (expires_at);
