-- The session that owns each host registration, replaced whenever the host registers or reattaches
-- Removing a drained host is made conditional on it, so a session that was superseded cannot remove a registration that moved on
ALTER TABLE %shosts ADD COLUMN host_session_id text;

-- Whether the host is draining (0 or 1), which excludes it from new actor placements
-- It is reset whenever the host registers or reattaches again
ALTER TABLE %shosts ADD COLUMN host_draining integer NOT NULL DEFAULT 0;
