-- Labels attached to an actor's state, stored as a JSON object in the row they describe
-- Keeping them in the state row is what makes them written, replaced, and removed with it in a single statement, with no second table to keep consistent and nothing left behind when the row expires
-- SQLite has no general-purpose index for arbitrary JSON keys, so a filtered listing evaluates the labels per row, within the actor_type range the primary key already narrows it to
ALTER TABLE %sactor_state ADD COLUMN actor_state_labels text;
