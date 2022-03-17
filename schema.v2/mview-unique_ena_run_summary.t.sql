CREATE MATERIALIZED VIEW IF NOT EXISTS ebi.unique_ena_run_summary%%POSTFIX%% AS
SELECT 'vcf' table_name,COUNT(*) count FROM (SELECT DISTINCT ena_run FROM ebi.vcf%%POSTFIX%%) AS tmp1
union
SELECT 'cov' table_name,COUNT( DISTINCT ena_run) count FROM ebi.cov%%POSTFIX%% WHERE pos=1
union
SELECT 'meta' table_name, COUNT(*) count FROM (SELECT DISTINCT ena_run FROM ebi.metadata%%POSTFIX%%) AS tmp2
