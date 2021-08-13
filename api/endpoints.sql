-- This contains how the R variables are generated for the app
-- "country_samples"

SELECT "clean_country", COUNT(*) AS "n_sample"
FROM (SELECT *
FROM "meta"
WHERE ("clean_host" = 'Homo sapiens')) "dbplyr_357"
WHERE (NOT((("clean_collection_date") IS NULL)))
GROUP BY "clean_country";
--------------------------------------------------------------


-- "lineage"

SELECT "clean_collection_date", "clean_country", "variant_id", COUNT(*) AS "n"
FROM (SELECT "ena_run", "collection_date", "clean_country", "clean_collection_date", "variant_id"
FROM (SELECT "LHS"."ena_run" AS "ena_run", "LHS"."collection_date" AS "collection_date", "LHS"."clean_country" AS "clean_country", "LHS"."clean_collection_date" AS "clean_collection_date", "RHS"."variant_id" AS "variant_id", "RHS"."n" AS "n", "RHS"."required_mutation" AS "required_mutation"
FROM (SELECT "ena_run", "collection_date", "clean_country", "clean_collection_date"
FROM (SELECT *
FROM (SELECT "ena_run", "collection_date", CASE WHEN ("clean_country" = 'USA') THEN ('United States') WHEN NOT("clean_country" = 'USA') THEN ("clean_country") END AS "clean_country", "clean_host", "accession", "sample_accession", "experiment_accession", "study_accession", "description", "country", "first_created", "first_public", "host", "host_sex", "host_tax_id", "host_body_site", "bio_material", "culture_collection", "instrument_model", "instrument_platform", "library_layout", "library_name", "library_selection", "library_source", "library_strategy", "sequencing_method", "isolate", "strain", "base_count", "collected_by", "broker_name", "center_name", "sample_capture_status", "fastq_ftp", "collection_date_submitted", "checklist", "clean_collection_date", "date_isoweek", "date_isoyear"
FROM "meta") "dbplyr_359"
WHERE (NOT((("clean_collection_date") IS NULL)))) "dbplyr_360"
WHERE ("clean_host" = 'Homo sapiens')) "LHS"
INNER JOIN "lineage" AS "RHS"
ON ("LHS"."ena_run" = "RHS"."ena_run")
) "dbplyr_361") "dbplyr_362"
WHERE ("clean_collection_date" > CAST('2020-01-01' AS DATE))
GROUP BY "clean_collection_date", "clean_country", "variant_id";
--------------------------------------------------------------

-- "lineage0"

SELECT "clean_collection_date", "clean_country", COUNT(*) AS "n"
FROM (SELECT "ena_run", "collection_date", "clean_country", "clean_collection_date"
FROM (SELECT *
FROM (SELECT *
FROM (SELECT "ena_run", "collection_date", CASE WHEN ("clean_country" = 'USA') THEN ('United States') WHEN NOT("clean_country" = 'USA') THEN ("clean_country") END AS "clean_country", "clean_host", "accession", "sample_accession", "experiment_accession", "study_accession", "description", "country", "first_created", "first_public", "host", "host_sex", "host_tax_id", "host_body_site", "bio_material", "culture_collection", "instrument_model", "instrument_platform", "library_layout", "library_name", "library_selection", "library_source", "library_strategy", "sequencing_method", "isolate", "strain", "base_count", "collected_by", "broker_name", "center_name", "sample_capture_status", "fastq_ftp", "collection_date_submitted", "checklist", "clean_collection_date", "date_isoweek", "date_isoyear"
FROM "meta") "dbplyr_367"
WHERE (NOT((("clean_collection_date") IS NULL)))) "dbplyr_368"
WHERE ("clean_host" = 'Homo sapiens')) "dbplyr_369") "dbplyr_370"
WHERE ("clean_collection_date" > CAST('2020-01-01' AS DATE))
GROUP BY "clean_collection_date", "clean_country"
--------------------------------------------------------------

-- "lineage_def "
SELECT "variant_id", "pango", "description"
FROM "lineage_def"
--------------------------------------------------------------


-- "new_cases"

-------------------------------------------------------------

-- variants_weekly

SELECT "country_name", "date_year", "date_week", "variant_id", COUNT(*) AS "weekly_variant_sample"
FROM (SELECT "LHS"."ena_run" AS "ena_run", "LHS"."country_name" AS "country_name", "LHS"."clean_collection_date" AS "clean_collection_date", "LHS"."date_year" AS "date_year", "LHS"."date_week" AS "date_week", "RHS"."variant_id" AS "variant_id", "RHS"."n" AS "n", "RHS"."required_mutation" AS "required_mutation"
FROM (SELECT "ena_run", "clean_country" AS "country_name", "clean_collection_date", "date_isoyear" AS "date_year", "date_isoweek" AS "date_week"
FROM (SELECT *
FROM (SELECT *
FROM (SELECT "ena_run", "collection_date", CASE WHEN ("clean_country" = 'USA') THEN ('United States') WHEN NOT("clean_country" = 'USA') THEN ("clean_country") END AS "clean_country", "clean_host", "accession", "sample_accession", "experiment_accession", "study_accession", "description", "country", "first_created", "first_public", "host", "host_sex", "host_tax_id", "host_body_site", "bio_material", "culture_collection", "instrument_model", "instrument_platform", "library_layout", "library_name", "library_selection", "library_source", "library_strategy", "sequencing_method", "isolate", "strain", "base_count", "collected_by", "broker_name", "center_name", "sample_capture_status", "fastq_ftp", "collection_date_submitted", "checklist", "clean_collection_date", "date_isoweek", "date_isoyear"
FROM "meta") "dbplyr_376"
WHERE (NOT((("clean_collection_date") IS NULL)))) "dbplyr_377"
WHERE ("clean_host" = 'Homo sapiens')) "dbplyr_378"
WHERE ("clean_collection_date" > CAST('2020-03-15' AS DATE))) "LHS"
INNER JOIN "lineage" AS "RHS"
ON ("LHS"."ena_run" = "RHS"."ena_run")
) "dbplyr_379"
GROUP BY "country_name", "date_year", "date_week", "variant_id"
-------------------------------------------------------------

-- "coutry_population"
-------------------------------------------------------------


-- "ebi_weekly_samples"

SELECT "Country", "date_year", "date_week", COUNT(*) AS "weekly_sample"
FROM (SELECT "ena_run", "clean_country" AS "Country", "clean_collection_date", "date_isoyear" AS "date_year", "date_isoweek" AS "date_week"
FROM (SELECT *
FROM (SELECT *
FROM (SELECT *
FROM (SELECT "ena_run", "collection_date", CASE WHEN ("clean_country" = 'USA') THEN ('United States') WHEN NOT("clean_country" = 'USA') THEN ("clean_country") END AS "clean_country", "clean_host", "accession", "sample_accession", "experiment_accession", "study_accession", "description", "country", "first_created", "first_public", "host", "host_sex", "host_tax_id", "host_body_site", "bio_material", "culture_collection", "instrument_model", "instrument_platform", "library_layout", "library_name", "library_selection", "library_source", "library_strategy", "sequencing_method", "isolate", "strain", "base_count", "collected_by", "broker_name", "center_name", "sample_capture_status", "fastq_ftp", "collection_date_submitted", "checklist", "clean_collection_date", "date_isoweek", "date_isoyear"
FROM "meta") "dbplyr_982"
WHERE (NOT((("clean_collection_date") IS NULL)))) "dbplyr_983"
WHERE (NOT((("clean_country") IS NULL)))) "dbplyr_984"
WHERE ("clean_host" = 'Homo sapiens')) "dbplyr_985") "dbplyr_986"
GROUP BY "Country", "date_year", "date_week"
-------------------------------------------------------------

-- "ebi_ecdc_weekly"

-------------------------------------------------------------


-- "head_vcf"

SELECT *
FROM "vcf"
LIMIT 6
-------------------------------------------------------------

-- "head_cov"

SELECT *
FROM "cov"
LIMIT 6
-------------------------------------------------------------

-- "head_lineage_def"

SELECT *
FROM "lineage_def"
LIMIT 6
-------------------------------------------------------------


-- head_lineage

SELECT *
FROM "lineage"
LIMIT 6
-------------------------------------------------------------


-- "head_meta"

SELECT *
FROM "meta"
LIMIT 6
-------------------------------------------------------------

-- "unique_ena_run_summary"

SELECT *
FROM "unique_ena_run_summary"
-------------------------------------------------------------

-- "country_weekly_data"

-------------------------------------------------------------
