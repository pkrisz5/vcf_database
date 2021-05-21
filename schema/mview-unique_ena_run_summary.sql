CREATE MATERIALIZED VIEW IF NOT EXISTS unique_ena_run_summary AS
SELECT 'vcf' table_name,COUNT(*) count FROM (SELECT DISTINCT ena_run FROM vcf) AS tmp1
union
SELECT 'cov' table_name,COUNT( DISTINCT ena_run) count FROM cov WHERE pos=1
union
SELECT 'meta' table_name,COUNT(*) count FROM (SELECT DISTINCT ena_run FROM meta) AS tmp2;
