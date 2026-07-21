-- Seed the singleton cluster-admission row in the metadata table
-- This row holds the cluster-wide max hosts limit and the exclusive-access lease, both managed through the provider and the ClusterAdmin, never written directly by embedders
-- max_hosts is null until the first host claims it, and exclusive is null when no exclusive-access lease is held
INSERT INTO %smetadata (key, value) VALUES ('cluster', '{"max_hosts":null,"exclusive":null}')
ON CONFLICT (key) DO NOTHING;
