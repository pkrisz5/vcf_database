CREATE MATERIALIZED VIEW IF NOT EXISTS lineage0%%POSTFIX%% AS
SELECT *
FROM (SELECT "LHS"."ena_run" AS "ena_run", "LHS"."variant_id" AS "variant_id", "LHS"."n" AS "n", "RHS"."required_mutation" AS "required_mutation"
FROM (SELECT "ena_run", "variant_id", COUNT(*) AS "n"
FROM (SELECT "LHS"."ena_run" AS "ena_run", "LHS"."chrom" AS "chrom", "LHS"."pos" AS "pos", "LHS"."ref" AS "ref", "LHS"."alt" AS "alt", "LHS"."qual" AS "qual", "LHS"."filter" AS "filter", "LHS"."dp" AS "dp", "LHS"."af" AS "af", "LHS"."sb" AS "sb", "LHS"."count_ref_forward_base" AS "count_ref_forward_base", "LHS"."count_ref_reverse_base" AS "count_ref_reverse_base", "LHS"."count_alt_forward_base" AS "count_alt_forward_base", "LHS"."count_alt_reverse_base" AS "count_alt_reverse_base", "LHS"."hrun" AS "hrun", "LHS"."indel" AS "indel", "LHS"."lof" AS "lof", "LHS"."nmd" AS "nmd", "LHS"."major" AS "major", "RHS"."variant_id" AS "variant_id", "RHS"."pango" AS "pango", "RHS"."type_variant" AS "type_variant", "RHS"."amino_acid_change" AS "amino_acid_change", "RHS"."protein_codon_position" AS "protein_codon_position", "RHS"."ref_protein" AS "ref_protein", "RHS"."alt_protein" AS "alt_protein", "RHS"."gene" AS "gene", "RHS"."effect" AS "effect", "RHS"."snpeff_original_mut" AS "snpeff_original_mut", "RHS"."ref_pos_alt" AS "ref_pos_alt", "RHS"."description" AS "description"
FROM (SELECT "ena_run", "chrom", "pos", "ref", "alt", "qual", "filter", "dp", "af", "sb", "count_ref_forward_base", "count_ref_reverse_base", "count_alt_forward_base", "count_alt_reverse_base", "hrun", "indel", "lof", "nmd", "major"
FROM "vcf%%POSTFIX%%"
WHERE ("af" > 0.5 OR "major"=1)) "LHS"
INNER JOIN "lineage_def%%POSTFIX%%" AS "RHS"
ON ("LHS"."pos" = "RHS"."pos" AND "LHS"."ref" = "RHS"."ref" AND "LHS"."alt" = "RHS"."alt")
) "dbplyr_144"
GROUP BY "ena_run", "variant_id") "LHS"
LEFT JOIN (SELECT "variant_id", COUNT(*) AS "required_mutation"
FROM "lineage_def%%POSTFIX%%"
GROUP BY "variant_id") "RHS"
ON ("LHS"."variant_id" = "RHS"."variant_id")
) "dbplyr_145"
WHERE ("n" = "required_mutation");




