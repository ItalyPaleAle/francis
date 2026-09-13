-- Labels attached to an actor's state, stored as a JSON object in the row they describe
-- Keeping them in the state row is what makes them written, replaced, and removed with it in a single statement, with no second table to keep consistent and nothing left behind when the row expires
ALTER TABLE %sactor_state ADD COLUMN actor_state_labels jsonb;

-- jsonb_path_ops indexes containment alone, which is the only operator a listing uses, and makes a filtered listing an index lookup rather than a walk of every stored state
-- The index is partial because most actor types carry no labels at all, and a containment test never matches a NULL, so the planner can still use it
CREATE INDEX %sactor_state_labels_idx ON %sactor_state USING gin (actor_state_labels jsonb_path_ops)
    WHERE actor_state_labels IS NOT NULL;
