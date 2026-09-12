-- Labels attached to an actor's state, stored as a JSON object so the durable copy travels with the row it belongs to
-- The standalone provider serves every query from memory, so the labels need no index of their own here
ALTER TABLE %sactor_state ADD COLUMN actor_state_labels TEXT;
