-- The workflow engine's labels for an instance, stored as a JSON object in the row they describe
-- Keeping them in the state row means they are written, replaced, and removed with it in a single statement, with no second table to keep consistent and nothing left behind when the row expires
ALTER TABLE %sactor_state ADD COLUMN workflow_labels jsonb;

-- The label set is closed, so each field gets an expression index of its own rather than one GIN index over the whole object
-- actor_id is the last column so that a listing filtered on one label is an index range scan already in the order it pages in, which a GIN index could not provide
-- Each index is partial because only a workflow instance's state carries labels at all, and ->> of a NULL column is NULL, so the partial index still covers every row a filter can match
CREATE INDEX %sactor_state_wf_status_idx ON %sactor_state (actor_type, (workflow_labels->>'status'), actor_id)
    WHERE workflow_labels IS NOT NULL;
CREATE INDEX %sactor_state_wf_version_idx ON %sactor_state (actor_type, (workflow_labels->>'version'), actor_id)
    WHERE workflow_labels IS NOT NULL;
CREATE INDEX %sactor_state_wf_parent_idx ON %sactor_state (actor_type, (workflow_labels->>'parent'), actor_id)
    WHERE workflow_labels IS NOT NULL;
