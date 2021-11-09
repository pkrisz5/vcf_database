-- app_country_samples

CREATE MATERIALIZED VIEW IF NOT EXISTS app_country_samples AS
  WITH temp_human AS (
        SELECT * 
          FROM "meta"
          WHERE ("clean_host" = 'Homo sapiens')
    )
    , temp_group_country AS (
        SELECT "clean_country", COUNT(*) AS "n_sample"
          FROM temp_human
          WHERE (NOT(("clean_collection_date") IS NULL))
          GROUP BY "clean_country"
    )
  SELECT clean_country, n_sample, LOG(n_sample) AS "log_n_sample"
    FROM temp_group_country 
    WHERE (NOT(("clean_country") IS NULL));
-----------------------------------------------------------------

-- app_lineage_def_description

CREATE MATERIALIZED VIEW IF NOT EXISTS app_lineage_def_description AS
  SELECT DISTINCT variant_id, pango, description
  FROM lineage_def
-----------------------------------------------------------------

-- app_lineage

CREATE MATERIALIZED VIEW IF NOT EXISTS app_lineage AS
  WITH temp_lineage1 AS (
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
        GROUP BY "clean_collection_date", "clean_country", "variant_id"
    )
    , temp_lineage2 AS (
        SELECT "clean_collection_date", "clean_country", COUNT(*) AS "n_all"
        FROM (SELECT "ena_run", "collection_date", "clean_country", "clean_collection_date"
        FROM (SELECT *
        FROM (SELECT *
        FROM (SELECT "ena_run", "collection_date", CASE WHEN ("clean_country" = 'USA') THEN ('United States') WHEN NOT("clean_country" = 'USA') THEN ("clean_country") END AS "clean_country", "clean_host", "accession", "sample_accession", "experiment_accession", "study_accession", "description", "country", "first_created", "first_public", "host", "host_sex", "host_tax_id", "host_body_site", "bio_material", "culture_collection", "instrument_model", "instrument_platform", "library_layout", "library_name", "library_selection", "library_source", "library_strategy", "sequencing_method", "isolate", "strain", "base_count", "collected_by", "broker_name", "center_name", "sample_capture_status", "fastq_ftp", "collection_date_submitted", "checklist", "clean_collection_date", "date_isoweek", "date_isoyear"
        FROM "meta") "dbplyr_367"
        WHERE (NOT((("clean_collection_date") IS NULL)))) "dbplyr_368"
        WHERE ("clean_host" = 'Homo sapiens')) "dbplyr_369") "dbplyr_370"
        WHERE ("clean_collection_date" > CAST('2020-01-01' AS DATE))
        GROUP BY "clean_collection_date", "clean_country"
    )
  SELECT "clean_collection_date", "clean_country", "variant_id", "n", "n_all", CAST ("n" AS numeric)/ CAST ("n_all" AS numeric)*100 AS "pct" 
    FROM temp_lineage1 
    INNER JOIN temp_lineage2 USING ("clean_collection_date", "clean_country");
---------------------------------------------------------------------------------

-- app_new_cases

CREATE MATERIALIZED VIEW IF NOT EXISTS app_new_cases AS
    SELECT "LHS"."country_name" AS "country_name", "LHS"."date_year" AS "date_year", "LHS"."date_week" AS "date_week", "LHS"."weekly_sample" AS "weekly_sample", "RHS"."iso_a3" AS "iso_a3", "RHS"."iso_a2" AS "iso_a2", "RHS"."country_name_local" AS "country_name_local", "RHS"."population" AS "population", "RHS"."ecdc_covid_country_weekly_cases" AS "ecdc_covid_country_weekly_cases", "RHS"."ecdc_covid_country_weekly_deaths" AS "ecdc_covid_country_weekly_deaths"
      FROM (SELECT "country_name", "date_year", "date_week", COUNT(*) AS "weekly_sample"
      FROM (SELECT "ena_run", "clean_country" AS "country_name", "clean_collection_date", "date_isoyear" AS "date_year", "date_isoweek" AS "date_week"
      FROM (SELECT *
      FROM (SELECT *
      FROM (SELECT *
      FROM (SELECT "ena_run", "collection_date", CASE WHEN ("clean_country" = 'USA') THEN ('United States') WHEN NOT("clean_country" = 'USA') THEN ("clean_country") END AS "clean_country", "clean_host", "accession", "sample_accession", "experiment_accession", "study_accession", "description", "country", "first_created", "first_public", "host", "host_sex", "host_tax_id", "host_body_site", "bio_material", "culture_collection", "instrument_model", "instrument_platform", "library_layout", "library_name", "library_selection", "library_source", "library_strategy", "sequencing_method", "isolate", "strain", "base_count", "collected_by", "broker_name", "center_name", "sample_capture_status", "fastq_ftp", "collection_date_submitted", "checklist", "clean_collection_date", "date_isoweek", "date_isoyear"
      FROM "meta") "dbplyr_153"
      WHERE (NOT((("clean_collection_date") IS NULL)))) "dbplyr_154"
      WHERE ("clean_host" = 'Homo sapiens')) "dbplyr_155"
      WHERE ("clean_collection_date" > CAST('2020-03-15' AS DATE))) "dbplyr_156") "dbplyr_157"
      GROUP BY "country_name", "date_year", "date_week") "LHS"
      LEFT JOIN "ecdc_covid_country_weekly" AS "RHS"
      ON ("LHS"."country_name" = "RHS"."country_name" AND "LHS"."date_year" = "RHS"."date_year" AND "LHS"."date_week" = "RHS"."date_week")
---------------------------------------------------------------------

-- app_variants_weekly

CREATE MATERIALIZED VIEW IF NOT EXISTS app_variants_weekly AS
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
-------------------------------------------------------------------

-- app_worldplot_data

CREATE MATERIALIZED VIEW IF NOT EXISTS app_worldplot_data AS
      SELECT "Country", "date_year", "date_week", COUNT(*) AS "weekly_sample"
        FROM (SELECT "ena_run", "clean_country" AS "Country", "clean_collection_date", "date_isoyear" AS "date_year", "date_isoweek" AS "date_week"
        FROM (SELECT *
        FROM (SELECT *
        FROM (SELECT *
        FROM (SELECT *
        FROM (SELECT *
        FROM (SELECT "ena_run", "collection_date", CASE WHEN ("clean_country" = 'USA') THEN ('United States') WHEN NOT("clean_country" = 'USA') THEN ("clean_country") END AS "clean_country", "clean_host", "accession", "sample_accession", "experiment_accession", "study_accession", "description", "country", "first_created", "first_public", "host", "host_sex", "host_tax_id", "host_body_site", "bio_material", "culture_collection", "instrument_model", "instrument_platform", "library_layout", "library_name", "library_selection", "library_source", "library_strategy", "sequencing_method", "isolate", "strain", "base_count", "collected_by", "broker_name", "center_name", "sample_capture_status", "fastq_ftp", "collection_date_submitted", "checklist", "clean_collection_date", "date_isoweek", "date_isoyear"
        FROM "meta") "dbplyr_158"
        WHERE (NOT((("clean_collection_date") IS NULL)))) "dbplyr_159"
        WHERE (NOT((("clean_country") IS NULL)))) "dbplyr_160"
        WHERE ("clean_host" = 'Homo sapiens')) "dbplyr_161"
        WHERE ("clean_collection_date" > CAST('2020-03-15' AS DATE))) "dbplyr_162"
        WHERE ("clean_collection_date" < CURRENT_DATE)) "dbplyr_163") "dbplyr_164"
        GROUP BY "Country", "date_year", "date_week"


