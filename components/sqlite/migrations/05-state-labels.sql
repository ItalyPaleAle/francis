-- Holds the labels attached to an actor's state, so a listing can filter on them by equality without reading every stored state
-- Labels are written in the same transaction as the state they belong to, and the cascade removes them with it
CREATE TABLE %sactor_state_labels (
    -- Actor type
    actor_type text NOT NULL,
    -- Actor ID
    actor_id text NOT NULL,
    -- Label name
    label_key text NOT NULL,
    -- Label value
    label_value text NOT NULL,

    PRIMARY KEY (actor_type, actor_id, label_key),
    FOREIGN KEY (actor_type, actor_id) REFERENCES %sactor_state (actor_type, actor_id) ON DELETE CASCADE
) WITHOUT ROWID, STRICT;

-- The lookup index orders by actor_id last, so a filtered listing pages in actor-ID order exactly as an unfiltered one does
CREATE INDEX %sactor_state_labels_lookup_idx ON %sactor_state_labels (actor_type, label_key, label_value, actor_id);
