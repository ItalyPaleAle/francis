-- The workflow engine's labels for an instance, stored as a JSON object in the row they describe
ALTER TABLE %sactor_state ADD COLUMN workflow_labels jsonb;

-- The label set is closed, so each field gets an expression index of its own (rather than one GIN index over the whole object)
CREATE INDEX %sactor_state_wf_status_idx ON %sactor_state (actor_type, (workflow_labels->>'status'), actor_id)
    WHERE workflow_labels IS NOT NULL;
CREATE INDEX %sactor_state_wf_version_idx ON %sactor_state (actor_type, (workflow_labels->>'version'), actor_id)
    WHERE workflow_labels IS NOT NULL;
CREATE INDEX %sactor_state_wf_parent_idx ON %sactor_state (actor_type, (workflow_labels->>'parent'), actor_id)
    WHERE workflow_labels IS NOT NULL;
