-- app_country_samples
CREATE MATERIALIZED VIEW IF NOT EXISTS app_country_samples AS
  WITH temp_human AS (
        SELECT * 
          FROM "metadata"
          WHERE ("host_id" = (SELECT id FROM host WHERE host='Homo sapiens'))
    )
    , temp_group_country AS (
        SELECT "country", COUNT(*) AS "n_sample"
          FROM temp_human
          WHERE (collection_date_valid=TRUE)
          GROUP BY "country"
    )
 SELECT country, n_sample, LOG(n_sample) AS "log_n_sample"
    FROM temp_group_country 
    WHERE (NOT(("country") IS NULL));   


-- app_lineage_def_description
CREATE MATERIALIZED VIEW IF NOT EXISTS app_lineage_def_description AS
  SELECT DISTINCT variant_id, pango, description
  FROM lineage_def;


-- app_lineage 

CREATE MATERIALIZED VIEW IF NOT EXISTS app_lineage AS
  WITH tmp_meta1 AS (
        SELECT ena_run, collection_date, country
        FROM metadata
        WHERE (collection_date_valid=TRUE)
          AND (collection_date > CAST('2020-01-01' AS DATE))
          AND (host_id = (SELECT id FROM host WHERE host='Homo sapiens'))
          AND (NOT((country) IS NULL))
  )
  , tmp_joined AS (
        SELECT lineage.ena_run AS "ena_run", collection_date, country, variant_id 
        FROM tmp_meta1
        INNER JOIN lineage 
          ON (tmp_meta1.ena_run = lineage.ena_run)
  )
  , temp_lineage1 AS (
        SELECT collection_date, country, variant_id, COUNT(*) AS n
        FROM tmp_joined
        GROUP BY collection_date, country, variant_id
  )
  , temp_lineage2 AS (
        SELECT collection_date, country, COUNT(*) AS n_all
        FROM tmp_meta1
        GROUP BY collection_date, country
  )
  SELECT collection_date, country, variant_id, n, n_all, CAST (n AS numeric)/ CAST ("n_all" AS numeric)*100 AS pct 
  FROM temp_lineage1 
  INNER JOIN temp_lineage2 USING (collection_date, country);


-- app_new_cases

CREATE MATERIALIZED VIEW IF NOT EXISTS app_new_cases AS
    WITH tmp_meta1 as (
        SELECT ena_run, country, collection_date, EXTRACT(ISOYEAR FROM collection_date) AS date_year, EXTRACT(WEEK FROM collection_date) AS date_week
        FROM metadata
        WHERE (collection_date_valid=TRUE)
         AND (host_id = (SELECT id FROM host WHERE host='Homo sapiens'))
         AND (collection_date > CAST('2020-03-15' AS DATE))
         AND (NOT((country) IS NULL))
    )
    , tmp_grouped as (
        SELECT country, date_year, date_week, COUNT(*) AS weekly_sample
        FROM tmp_meta1
        GROUP BY country, date_year, date_week
    )
    , tmp_ecdc_covid_country_weekly AS (
        SELECT CASE WHEN (country_name = 'United States') THEN ('USA') WHEN NOT(country_name = 'United States') THEN (country_name) END AS country, date_year, date_week, iso_a3, iso_a2, country_name_local, population, ecdc_covid_country_weekly_cases, ecdc_covid_country_weekly_deaths
        FROM ecdc_covid_country_weekly
    )
    SELECT country, date_year, date_week, weekly_sample, iso_a3, iso_a2, country_name_local, population, ecdc_covid_country_weekly_cases, ecdc_covid_country_weekly_deaths
    FROM tmp_grouped
    LEFT JOIN tmp_ecdc_covid_country_weekly USING (country, date_year, date_week);

-- app_variants_weekly

CREATE MATERIALIZED VIEW IF NOT EXISTS app_variants_weekly AS
    WITH tmp_meta1 AS (
        SELECT ena_run, country, collection_date, EXTRACT(ISOYEAR FROM collection_date) AS date_year, EXTRACT(WEEK FROM collection_date) AS date_week
        FROM metadata
        WHERE (collection_date_valid=TRUE)
        AND (host_id = (SELECT id FROM host WHERE host='Homo sapiens'))
        AND (collection_date > CAST('2020-03-15' AS DATE))
        AND (NOT((country) IS NULL))
   )
   , tmp_joined AS (
        SELECT ena_run, country, collection_date, date_year, date_week, variant_id
        FROM tmp_meta1 
        INNER JOIN lineage USING (ena_run)
   )
    SELECT country, date_year, date_week, variant_id, COUNT(*) AS "weekly_variant_sample"
    FROM tmp_joined
    GROUP BY country, date_year, date_week, variant_id;


-- app_worldplot_data

CREATE MATERIALIZED VIEW IF NOT EXISTS app_worldplot_data AS
    WITH tmp_meta1 as (
        SELECT ena_run, country, collection_date, EXTRACT(ISOYEAR FROM collection_date) AS date_year, EXTRACT(WEEK FROM collection_date) AS date_week
        FROM metadata
        WHERE (collection_date_valid=TRUE)
        AND (host_id = (SELECT id FROM host WHERE host='Homo sapiens'))
        AND (collection_date > CAST('2020-03-15' AS DATE))
        AND (NOT((country) IS NULL))
        AND collection_date < CURRENT_DATE
    )
    SELECT country, date_year, date_week, COUNT(*) AS weekly_sample
    FROM tmp_meta1
    GROUP BY country, date_year, date_week;

