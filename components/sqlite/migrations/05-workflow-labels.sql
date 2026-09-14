-- The workflow engine's labels for an instance, stored as a JSON object in the row they describe
-- Keeping them in the state row means they are written, replaced, and removed with it in a single statement, with no second table to keep consistent and nothing left behind when the row expires
ALTER TABLE %sactor_state ADD COLUMN workflow_labels text;

-- SQLite has no index for arbitrary JSON keys, but the label set is closed, so each field gets an expression index of its own
-- SQLite only uses one of these when the query repeats the indexed expression verbatim, which is why ListStates writes out the same json_extract path
-- actor_id is the last column so that a listing filtered on one label is an index range scan already in the order it pages in
-- Each index is partial because only a workflow instance's state carries labels at all, and json_extract of a NULL column is NULL, so the partial index still covers every row a filter can match
CREATE INDEX %sactor_state_wf_status_idx ON %sactor_state (actor_type, json_extract(workflow_labels, '$.status'), actor_id)
    WHERE workflow_labels IS NOT NULL;
CREATE INDEX %sactor_state_wf_version_idx ON %sactor_state (actor_type, json_extract(workflow_labels, '$.version'), actor_id)
    WHERE workflow_labels IS NOT NULL;
CREATE INDEX %sactor_state_wf_parent_idx ON %sactor_state (actor_type, json_extract(workflow_labels, '$.parent'), actor_id)
    WHERE workflow_labels IS NOT NULL;
