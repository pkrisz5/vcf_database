CREATE MATERIALIZED VIEW IF NOT EXISTS lineage%%POSTFIX%% AS
SELECT *
FROM (SELECT "LHS"."ena_run" AS "ena_run", "LHS"."variant_id" AS "variant_id", "LHS"."n" AS "n", "RHS"."required_mutation" AS "required_mutation"
FROM (SELECT "ena_run", "variant_id", COUNT(*) AS "n"
FROM (SELECT "LHS"."ena_run" AS "ena_run", "LHS"."chrom" AS "chrom", "LHS"."pos" AS "pos", "LHS"."ref" AS "ref", "LHS"."alt" AS "alt", "LHS"."qual" AS "qual", "LHS"."filter" AS "filter", "LHS"."dp" AS "dp", "LHS"."af" AS "af", "LHS"."sb" AS "sb", "LHS"."count_ref_forward_base" AS "count_ref_forward_base", "LHS"."count_ref_reverse_base" AS "count_ref_reverse_base", "LHS"."count_alt_forward_base" AS "count_alt_forward_base", "LHS"."count_alt_reverse_base" AS "count_alt_reverse_base", "LHS"."hrun" AS "hrun", "LHS"."indel" AS "indel", "LHS"."lof" AS "lof", "LHS"."nmd" AS "nmd", "RHS"."variant_id" AS "variant_id", "RHS"."pango" AS "pango", "RHS"."nextstrain" AS "nextstrain", "RHS"."ref_pos_alt" AS "ref_pos_alt", "RHS"."codon_change" AS "codon_change", "RHS"."gene" AS "gene", "RHS"."predicted_effect" AS "predicted_effect", "RHS"."protein" AS "protein", "RHS"."protein_codon_position" AS "protein_codon_position", "RHS"."type" AS "type", "RHS"."amino_acid_change" AS "amino_acid_change", "RHS"."description" AS "description", "RHS"."snp_codon_position" AS "snp_codon_position"
FROM (SELECT "ena_run", "chrom", "pos", "ref", "alt", "qual", "filter", "dp", "af", "sb", "count_ref_forward_base", "count_ref_reverse_base", "count_alt_forward_base", "count_alt_reverse_base", "hrun", "indel", "lof", "nmd"
FROM (SELECT *
FROM "vcf%%POSTFIX%%"
WHERE ("ann_num" = 1.0)) "dbplyr_065"
WHERE ("af" > 0.5)) "LHS"
INNER JOIN "lineage_def" AS "RHS"
ON ("LHS"."pos" = "RHS"."pos" AND "LHS"."ref" = "RHS"."ref" AND "LHS"."alt" = "RHS"."alt")
) "dbplyr_066"
GROUP BY "ena_run", "variant_id") "LHS"
LEFT JOIN (SELECT "variant_id", COUNT(*) AS "required_mutation"
FROM "lineage_def"
GROUP BY "variant_id") "RHS"
ON ("LHS"."variant_id" = "RHS"."variant_id")
) "dbplyr_067"
WHERE ("n" >= "required_mutation")
