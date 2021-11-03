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
