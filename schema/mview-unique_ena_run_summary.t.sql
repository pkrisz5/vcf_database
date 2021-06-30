CREATE MATERIALIZED VIEW IF NOT EXISTS unique_ena_run_summary%%POSTFIX%% AS
SELECT 'vcf' table_name,COUNT(*) count FROM (SELECT DISTINCT ena_run FROM vcf%%POSTFIX%%) AS tmp1
union
SELECT 'cov' table_name,COUNT( DISTINCT ena_run) count FROM cov%%POSTFIX%% WHERE pos=1
union
SELECT 'meta' table_name,COUNT(*) count FROM (SELECT DISTINCT ena_run FROM meta%%POSTFIX%%) AS tmp2;
