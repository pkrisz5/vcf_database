#!/usr/bin/env python

"""
@summary:
helper script to initialize database.
Implements:
- create database
- create role
- create schema
"""


import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import re
import os
import io
import sys
import datetime
import argparse
import pandas


##################################
# argument parser helpers

cli = argparse.ArgumentParser()
subparsers = cli.add_subparsers(dest="subcommand")

def subcommand(args=[], parent=subparsers):
    args.extend([
        argument("-H", "--server", action="store", help="database server name/ip address", default=os.getenv('DB_HOST')),
        argument("-P", "--port", action="store", help="database server port", default=os.getenv('DB_PORT', 5432)),
        argument("-D", "--database", action="store", help="database name", default=os.getenv('DB', 'coveo')),
        argument("-u", "--user", action="store", help="database user", default=os.getenv('SECRET_USERNAME')),
        argument("-p", "--password", action = "store", help="database password", default = os.getenv('SECRET_PASSWORD')),
    ])
    def decorator(func):
        parser = parent.add_parser(func.__name__, description=func.__doc__)
        for arg in args:
            parser.add_argument(*arg[0], **arg[1])
        parser.set_defaults(func=func)
    return decorator

def argument(*name_or_flags, **kwargs):
    return ([*name_or_flags], kwargs)



##################################
# implemented functionalities

def exec_autocommit(args, sql):
    c = psycopg2.connect(
        host = args.server,
        port = args.port,
        user = args.user,
        password = args.password,
    )
    c.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    C = c.cursor()
    C.execute(sql)
    C.close()
    c.close()

def exec_commit(args, sql):
    c = psycopg2.connect(
        host = args.server,
        port = args.port,
        user = args.user,
        password = args.password,
        database = args.database,
    )
    C = c.cursor()
    C.execute(sql)
    c.commit()
    C.close()
    c.close()



@subcommand([argument("-S", "--schema", action="store", help="schema name", required=True)])
def idx_vcf_af(args):
    sql = f"""
CREATE INDEX idx_vcf_af ON {args.schema}.vcf USING btree (af);
    """
    exec_commit(args, sql)

@subcommand([argument("-S", "--schema", action="store", help="schema name", required=True)])
def idx_vcf_key(args):
    sql = f"""
CREATE INDEX idx_vcf_key ON {args.schema}.vcf USING btree (key);    
    """
    exec_commit(args, sql)
    
@subcommand([argument("-S", "--schema", action="store", help="schema name", required=True)])
def idx_vcf_key_runid(args):
    sql = f"""
CREATE INDEX idx_vcf_key_runid ON {args.schema}.vcf_key USING btree (runid);
    """
    exec_commit(args, sql)

@subcommand([argument("-S", "--schema", action="store", help="schema name", required=True)])
def idx_vcf_key_key(args):
    sql = f"""
CREATE INDEX idx_vcf_key_key ON {args.schema}.vcf_key USING btree (key)
    """
    exec_commit(args, sql)


@subcommand([argument("-S", "--schema", action="store", help="schema name", required=True)])
def unique_ena_run_summary(args):
    sql = f"""
CREATE MATERIALIZED VIEW {args.schema}.unique_ena_run_summary
 TABLESPACE pg_default
  AS SELECT 'vcf'::text AS table_name,
            count(*) AS count
     FROM (SELECT DISTINCT vk.runid FROM {args.schema}.vcf_key vk) tmp1
     UNION
     SELECT 'cov'::text AS table_name,
            count(*) AS count
     FROM (SELECT DISTINCT c.runid from {args.schema}.cov c WHERE c.pos = 1) tmp2
     UNION
     SELECT 'meta'::text AS table_name,
            count(*) AS count
     FROM (SELECT DISTINCT m.runid FROM {args.schema}.metadata m) tmp3
     WITH DATA;
    """
    exec_commit(args, sql)


@subcommand([argument("-S", "--schema", action="store", help="schema name", required=True)])
def app_variants_weekly(args):
    sql = f"""
CREATE MATERIALIZED VIEW {args.schema}.app_variants_weekly
 TABLESPACE pg_default
  AS WITH tmp_meta1 AS (
        SELECT m.runid,
               m.country_id,
               m.collection_date,
               date_part('isoyear'::text, m.collection_date) AS date_year,
               date_part('week'::text, m.collection_date) AS date_week
            FROM {args.schema}.metadata m
            WHERE m.collection_date_valid AND m.host_id = {args.schema}.host_human_id() AND m.collection_date > '2020-03-15'::date AND NOT m.country_id IS NULL
    ), tmp_joined AS (
        SELECT count(*) as weekly_variant_sample,
               tmp_meta1.country_id,
               tmp_meta1.date_year,
               tmp_meta1.date_week,
               l.variant_id
            FROM tmp_meta1
            JOIN {args.schema}.lineage l
            USING (runid)
            GROUP BY tmp_meta1.country_id, tmp_meta1.date_year, tmp_meta1.date_week, l.variant_id
    )
  SELECT c.country_name as country,
         t.date_year,
         t.date_week,
         t.variant_id,
         t.weekly_variant_sample
      FROM tmp_joined t
      JOIN {args.schema}.country c
      ON c.id = t.country_id
      WITH DATA;
    """
    exec_commit(args, sql)


@subcommand([argument("-S", "--schema", action="store", help="schema name", required=True)])
def app_lineage_def_description(args):
    sql = f"""
CREATE OR REPLACE VIEW {args.schema}.app_lineage_def_description
 AS SELECT DISTINCT ld.variant_id,
                    ld.pango,
                    ld.description
    FROM {args.schema}.lineage_def ld
    """
    exec_commit(args, sql)


@subcommand([argument("-S", "--schema", action="store", help="schema name", required=True)])
def app_lineage(args):
    sql = f"""
CREATE MATERIALIZED VIEW {args.schema}.app_lineage
 TABLESPACE pg_default
  AS WITH tmp_meta1 AS (
        SELECT count(*) as n,
               m.collection_date,
               m.country_id,
               l.variant_id
        FROM {args.schema}.metadata m
        JOIN {args.schema}.lineage l
        ON l.runid = m.runid
        WHERE m.collection_date_valid AND m.collection_date > '2020-01-01'::date AND m.host_id = {args.schema}.host_human_id() AND NOT m.country_id IS null
        GROUP BY m.collection_date, m.country_id, l.variant_id
    ), temp_lineage2 AS (
        SELECT tmp_meta1.collection_date,
               tmp_meta1.country_id,
               count(*) AS n_all
        FROM tmp_meta1
        GROUP BY tmp_meta1.collection_date, tmp_meta1.country_id
    )
  SELECT t1.collection_date,
         c.country_name as country,
         t1.variant_id,
         t1.n,
         temp_lineage2.n_all,
         t1.n::numeric / temp_lineage2.n_all::numeric * 100::numeric AS pct
      FROM tmp_meta1 t1
      JOIN temp_lineage2
      USING (collection_date, country_id)
      JOIN {args.schema}.country c
      ON c.id = t1.country_id
      WITH DATA;
    """
    exec_commit(args, sql)


@subcommand([argument("-S", "--schema", action="store", help="schema name", required=True)])
def app_country_samples(args):
    sql = f"""
CREATE MATERIALIZED VIEW {args.schema}.app_country_samples
 TABLESPACE pg_default
  AS WITH temp_stat AS (
        SELECT m.country_id,
               count(*) AS n_sample
        FROM {args.schema}.metadata m
        WHERE m.host_id = host_human_id() and m.collection_date_valid
        GROUP BY m.country_id
    )
  SELECT c.country_name AS country,
         temp_stat.n_sample,
         log(temp_stat.n_sample::double precision) AS log_n_sample
  FROM temp_stat
  LEFT JOIN country c 
  ON c.id = temp_stat.country_id
  WITH DATA;
    """
    exec_commit(args, sql)


@subcommand([argument("-S", "--schema", action="store", help="schema name", required=True)])
def lineage(args):
    sql = f"""
CREATE MATERIALIZED VIEW IF NOT EXISTS {args.schema}.lineage 
  AS WITH lineage_not_analyzed_w AS (
        SELECT m.runid, TEXT('Not analysed yet') AS variant_id, 0 AS n, 0 AS required_mutation
        FROM {args.schema}.runid_ok ro
        RIGHT OUTER JOIN {args.schema}.metadata m
        ON ro.runid = m.runid 
        WHERE ro.runid IS NULL
    ), lineage0_w AS (
        WITH LHS AS (
           SELECT runid, variant_id, COUNT(*) AS n 
           FROM (
                SELECT * 
                FROM {args.schema}.vcf_key vk
                JOIN {args.schema}.vcf v
                ON vk.key = v.key
                WHERE v.af > 0.5 OR v.major
           ) AS LHS_vcf
           INNER JOIN {args.schema}.lineage_def ld
           ON LHS_vcf.pos = ld.pos AND LHS_vcf."ref" = ld."ref" AND LHS_vcf.alt = ld.alt
           GROUP BY runid, variant_id
        )
      SELECT LHS.runid, LHS.variant_id AS variant_id, LHS.n AS n, vm.mutations AS required_mutation
      FROM LHS 
      LEFT JOIN {args.schema}.variant_mutations vm
      ON LHS.variant_id = vm.variant_id
      WHERE LHS.n = vm.mutations
    ), lineage_base_w AS (
        SELECT DISTINCT ON (runid) runid, variant_id, n, required_mutation 
        FROM lineage0_w 
        ORDER BY runid, required_mutation DESC
    ), lineage_other_w AS (
        SELECT DISTINCT rs.runid, TEXT('Other variant') AS variant_id, 0 AS n, 0 AS required_mutation
        FROM {args.schema}.vcf_key vk
        RIGHT OUTER JOIN lineage_base_w rs
        ON rs.runid = vk.runid
        WHERE vk.runid IS NULL
    )
  SELECT * FROM lineage_base_w
  UNION
  SELECT * FROM lineage_other_w
  UNION
  SELECT * FROM lineage_not_analyzed_w;
    """
    exec_commit(args, sql)


@subcommand([argument("-S", "--schema", action="store", help="schema name", required=True)])
def variant_mutations(args):
    sql = f"""
CREATE OR REPLACE VIEW {args.schema}.variant_mutations
 AS SELECT ld.variant_id, count(*) AS mutations
    FROM {args.schema}.lineage_def ld
    GROUP BY ld.variant_id
    """
    exec_commit(args, sql)


@subcommand([argument("-S", "--schema", action="store", help="schema name", required=True)])
def app_new_cases(args):
    sql = f"""
CREATE MATERIALIZED VIEW datahub_0.app_new_cases
TABLESPACE pg_default
 AS WITH tmp_meta1 AS (
         SELECT count(*) AS weekly_sample,
            m.country_id,
            date_part('isoyear'::text, m.collection_date) AS date_year,
            date_part('week'::text, m.collection_date) AS date_week
           FROM metadata m
          WHERE m.collection_date_valid AND m.host_id = host_human_id() AND m.collection_date > '2020-03-15'::date AND NOT m.country_id IS NULL
          GROUP BY m.country_id, (date_part('isoyear'::text, m.collection_date)), (date_part('week'::text, m.collection_date))
        )
 SELECT c.country_name AS country,
 	to_date((cast(t1.date_year as text ) || cast(t1.date_week as text )), 'iyyyiw') as date,
    t1.date_year,
    t1.date_week,
    t1.weekly_sample,
    eccw.cases
   FROM tmp_meta1 t1
     LEFT JOIN ecdc_covid_country_weekly eccw USING (country_id, date_year, date_week)
     JOIN country c ON c.id = t1.country_id
 WITH DATA;
    """
    exec_commit(args, sql)


@subcommand([argument("-S", "--schema", action="store", help="schema name", required=True)])
def vcf_key_selected(args):
    sql = f"""
CREATE MATERIALIZED VIEW {args.schema}.vcf_key_selected
TABLESPACE pg_default
AS SELECT tmp1.key,
    vk.runid,
    vk.pos,
    vk.ref,
    vk.alt
   FROM {args.schema}.vcf_key vk
     JOIN ( SELECT v.key
           FROM {args.schema}.vcf v
          WHERE v.af > 0.1::double precision) tmp1 ON vk.key = tmp1.key  -- FIXME: hard coded threshold
WITH DATA;
    """
    exec_commit(args, sql)


@subcommand([argument("-S", "--schema", action="store", help="schema name", required=True)])
def app_worldplot_data(args):
    sql = f"""
CREATE MATERIALIZED VIEW {args.schema}.app_worldplot_data
TABLESPACE pg_default
AS WITH tmp_meta1 AS (
    SELECT count(*) AS weekly_sample,
        m.country_id,
        date_part('isoyear'::text, m.collection_date) AS date_year,
        date_part('week'::text, m.collection_date) AS date_week
    FROM {args.schema}.metadata m
    WHERE m.collection_date_valid AND m.host_id = {args.schema}.host_human_id() AND m.collection_date > '2020-03-15'::date AND NOT m.country_id IS NULL AND m.collection_date < 'now'::text::date
    GROUP BY m.country_id, date_part('isoyear'::text, m.collection_date), date_part('week'::text, m.collection_date)
  )
  SELECT c.country_name AS country,
    tmp_meta1.date_year,
    tmp_meta1.date_week,
    tmp_meta1.weekly_sample
  FROM tmp_meta1
  JOIN {args.schema}.country c
  ON c.id = tmp_meta1.country_id
WITH DATA;
    """
    exec_commit(args, sql)

    
@subcommand([argument("-S", "--schema", action="store", help="schema name", required=True)])
def summary_insert(args):
    sql = f"""
CREATE OR REPLACE VIEW {args.schema}.summary_insert
AS SELECT
    foo.snapshot,
    foo.count AS "coverage_count",
    foo.load_start AS "coverage_start_insert",
    foo.load_duration AS "coverage_duration_insert",
    bar.count AS "vcf_count",
    bar.load_start AS "vcf_start_insert",
    bar.load_duration AS "vcf_duration_insert"
  FROM (
    SELECT uc."snapshot" AS snapshot, count(*), min(uc.insertion_ts) AS load_start, max(uc.insertion_ts)-min(uc.insertion_ts) AS load_duration
    FROM {args.schema}.unique_cov uc
    GROUP BY uc."snapshot"
  ) AS foo
  FULL OUTER JOIN (
    SELECT uv."snapshot" AS snapshot, count(*), min(uv.insertion_ts) AS load_start, max(uv.insertion_ts)-min(uv.insertion_ts) AS load_duration
    FROM {args.schema}.unique_vcf uv
    GROUP BY uv."snapshot"
  ) AS bar
  ON foo.snapshot = bar.snapshot
  ORDER BY foo.load_start
    """
    exec_commit(args, sql)

    
@subcommand([argument("-S", "--schema", action="store", help="schema name", required=True)])
def runid_ok(args):
    sql = f"""
 CREATE OR REPLACE VIEW {args.schema}.runid_ok
 AS SELECT uc.runid
    FROM {args.schema}.unique_cov uc
    JOIN {args.schema}.unique_vcf uv ON uc.runid = uv.runid AND uc.integrity = uv.integrity
    WHERE uc.integrity = 'ok'::{args.schema}.type_integrity;
    """
    exec_commit(args, sql)

    
if __name__ == "__main__":
    args = cli.parse_args()
    if args.subcommand is None:
        cli.print_help()
    else:
        args.func(args)


