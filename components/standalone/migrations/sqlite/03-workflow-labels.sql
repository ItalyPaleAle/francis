-- The workflow engine's labels for an instance, stored as a JSON object so the durable copy travels with the row it belongs to
-- The standalone provider serves every query from memory, so the labels need no index of their own here
ALTER TABLE %sactor_state ADD COLUMN workflow_labels TEXT;
