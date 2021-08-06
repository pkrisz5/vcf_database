CREATE MATERIALIZED VIEW IF NOT EXISTS lineage_not_analyzed%%POSTFIX%% AS
SELECT "ena_run", "variant_id", "n", 0.0 AS "required_mutation"
FROM (SELECT "ena_run", "variant_id", 0 AS "n"
FROM (SELECT "ena_run", TEXT('Not analysed yet ') AS "variant_id"
FROM (SELECT * FROM (SELECT "ena_run"
FROM "meta%%POSTFIX%%") "LHS"
WHERE NOT EXISTS (
  SELECT 1 FROM (SELECT DISTINCT "ena_run"
FROM "vcf%%POSTFIX%%") "RHS"
  WHERE ("LHS"."ena_run" = "RHS"."ena_run")
)) "dbplyr_129") "dbplyr_130") "dbplyr_131";

