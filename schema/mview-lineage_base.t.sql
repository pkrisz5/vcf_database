CREATE MATERIALIZED VIEW IF NOT EXISTS lineage_base%%POSTFIX%% AS
SELECT DISTINCT ON (ena_run) ena_run, variant_id, n, required_mutation FROM lineage0%%POSTFIX%% ORDER BY ena_run, required_mutation DESC;

