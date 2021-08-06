CREATE MATERIALIZED VIEW IF NOT EXISTS lineage_other%%POSTFIX%% AS
SELECT "ena_run", "variant_id", "n", 0.0 AS "required_mutation"
FROM (SELECT "ena_run", "variant_id", 0.0 AS "n"
FROM (SELECT "ena_run", TEXT('Other variant') AS "variant_id"
FROM (SELECT DISTINCT *
FROM (SELECT "ena_run"
FROM (SELECT * FROM "vcf%%POSTFIX%%" AS "LHS"
WHERE NOT EXISTS (
  SELECT 1 FROM "lineage_base%%POSTFIX%%" AS "RHS"
  WHERE ("LHS"."ena_run" = "RHS"."ena_run")
)) "dbplyr_123") "dbplyr_124") "dbplyr_125") "dbplyr_126") "dbplyr_127";

