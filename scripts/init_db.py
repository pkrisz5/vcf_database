import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import re
import os
import sys
import datetime
import argparse

host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT', 5432)
db = os.getenv('DB')

p = os.getenv('SCHEMA_PATH', '../schema')

#tables = [ 'cov', 'vcf_all', 'vcf', 'meta', 'lineage_def', 'ecdc_covid_country_weekly', 'operation', 'unique_cov', 'unique_vcf' ]
tables = [ 'cov', 'vcf_all', 'vcf', 'meta', 'ecdc_covid_country_weekly', 'operation', 'unique_cov', 'unique_vcf' ]
mviews = [ 'unique_ena_run_summary', 'lineage0', 'lineage_base', 'lineage_other', 'lineage_not_analyzed', 'lineage' ]


def create_db(db):
    #return "SELECT 'CREATE DATABASE \"{0}\"' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '{0}')".format(db)
    return "CREATE DATABASE \"{0}\"".format(db)

def create_user(user, pw):
    return "CREATE USER \"{0}\" PASSWORD '{1}'".format(user, pw)

def grant_read(user, db):
    return [ "GRANT CONNECT ON DATABASE \"{1}\" TO \"{0}\"".format(user, db), "GRANT SELECT ON ALL TABLES IN SCHEMA public TO \"{0}\"".format(user) ]

def db_exec(statement, transaction = True):
    try:
        t0 = datetime.datetime.now()
        print ("{0} SQL: {1}".format(t0, statement))
        cur = None
        cur = myConnection.cursor()
        cur.execute( statement )
        if transaction:
            myConnection.commit()
            print ("commited")
    except Exception as e:
        print ("ERROR: {}".format(e))
        if transaction:
            myConnection.rollback()
            print ("rolled back")
    finally:
        if cur:
            cur.close()
        t1 = datetime.datetime.now()
        print ("{0} the duration of running statement {1}".format(t1, t1 - t0))

def con(db = db):
    if db:
        return psycopg2.connect(
            host = host,
            port = port,
            user = os.getenv('SECRET_USERNAME'),
            password = os.getenv('SECRET_PASSWORD'),
            database = db,
        )
    else:
        c = psycopg2.connect(
            host = host,
            port = port,
            user = os.getenv('SECRET_USERNAME'),
            password = os.getenv('SECRET_PASSWORD'),
        )
        c.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return c

if __name__ == '__main__':
    tc = tables[:]
    tc.append('all')

    parser = argparse.ArgumentParser()
    parser.add_argument("-X", "--drop_db", action = "store_true",
                    help = "drop database")
    parser.add_argument("-I", "--init_db", action = "store_true",
                    help = "create database")
    parser.add_argument("-u", "--create_user", action = "store_true",
                    help = "create a database user")
    parser.add_argument("-a", "--grant_access", action = "store_true",
                    help="grant read only right to database user")
    parser.add_argument("-t", "--create_table", choices = tc,
                     help = "create a table")
    parser.add_argument("-p", "--create_tables_append", action = "store_true",
                     help = "create a copy table of vcf and cov to append new data")
    parser.add_argument("-D", "--drop_table", choices = tables,
                     help = "drop a table")
    parser.add_argument("-B", "--backup_table", choices = tables,
                     help = "backup a table")
    parser.add_argument("-f", "--filter_vcf",
                    help = "filter vcf_all_append (recent consensus af>.1)")
    parser.add_argument("-i", "--create_indexes", action = "store_true",
                    help = "create table indexes on *_append")
    parser.add_argument("-r", "--rename_tables", action = "store_true",
                    help = "rename *_append tables")
    parser.add_argument("-m", "--create_materialized_views", action = "store_true",
                    help = "create materialized_views")
    parser.add_argument("-A", "--operate_on_append", action = "store_true",
                    help = "operate on *_append")
    args = parser.parse_args()

    # drop databases
    if args.drop_db:
        myConnection = con(None)
        print ("{0} connected to db engine to create db {1}".format(datetime.datetime.now(), db))
        #TODO: check postgres v >=13, DROP DATABASE xy WITH (FORCE)
        db_exec( "DROP DATABASE IF EXISTS \"{0}\"".format(db), transaction = False )
        myConnection.close()
        print ("{} disconnected from db engine".format(datetime.datetime.now()))
        sys.exit(0)

    # create databases
    if args.init_db:
        myConnection = con(None)
        print ("{0} connected to db engine to create db {1}".format(datetime.datetime.now(), db))
        db_exec( create_db(db), transaction = False )
        myConnection.close()
        print ("{} disconnected from db engine".format(datetime.datetime.now()))

    myConnection = con(db)
    print ("{0} connected to db {1}".format(datetime.datetime.now(), db))

    # create user
    if args.create_user:
        statement = create_user(os.getenv('READONLY_USERNAME', 'kooplex-reader'), os.getenv('READONLY_PASSWORD', 'reader-pw'))
        db_exec( statement, transaction = True )

    # grant read only right to user
    if args.grant_access:
        for statement in grant_read(os.getenv('READONLY_USERNAME', 'kooplex-reader'), db):
            db_exec( statement, transaction = True )

    # create tables
    if args.create_table:
        if args.create_table == 'all':
            for t in tables:
                statement = open(os.path.join(p, "table-{}.sql".format(t))).read()
                db_exec( statement, transaction = True )
        else:
            statement = open(os.path.join(p, "table-{}.sql".format(args.create_table))).read()
            db_exec( statement, transaction = True )

    # drop tables
    if args.drop_table:
        db_exec( "DROP TABLE IF EXISTS {} CASCADE".format(args.drop_table), transaction = True )

    # copy production tables for appending new data
    if args.create_tables_append:
        db_exec( "DROP TABLE IF EXISTS vcf_all_append", transaction = True )
        db_exec( "CREATE TABLE vcf_all_append AS SELECT * FROM vcf_all", transaction = True )
        db_exec( "DROP TABLE IF EXISTS cov_append", transaction = True )
        db_exec( "CREATE TABLE cov_append AS SELECT * FROM cov", transaction = True )
        db_exec( "DROP TABLE IF EXISTS meta_append", transaction = True )
        db_exec( "CREATE TABLE meta_append AS TABLE meta WITH NO DATA", transaction = True )
        db_exec( "DROP TABLE IF EXISTS unique_cov_append", transaction = True )
        db_exec( "CREATE TABLE unique_cov_append AS SELECT * FROM unique_cov", transaction = True )
        db_exec( "DROP TABLE IF EXISTS unique_vcf_append", transaction = True )
        db_exec( "CREATE TABLE unique_vcf_append AS SELECT * FROM unique_vcf", transaction = True )

    # backup a table
    if args.backup_table:
        db_exec( "DROP TABLE IF EXISTS {}_backup".format(args.backup_table), transaction = True )
        db_exec( "CREATE TABLE {0}_backup AS SELECT * FROM {0}".format(args.backup_table), transaction = True )

    # filter vcf_all above threshold
    if args.filter_vcf:
        db_exec( "DROP TABLE IF EXISTS vcf_append", transaction = True )
        statement = """
CREATE TABLE vcf_append AS 
  SELECT *
  FROM (
    SELECT *, row_number() OVER (PARTITION BY ena_run, pos, ref, alt ORDER BY ena_run) AS row_number
    FROM vcf_all_append WHERE "af" >= {}
  ) AS rows
  WHERE row_number = 1
        """.format(args.filter_vcf)
        db_exec( statement, transaction = True )
        db_exec( "ALTER TABLE vcf_append DROP COLUMN row_number", transaction = True )

    # create indexes
    if args.create_indexes:
        if args.operate_on_append:
            for statement in [
                "CREATE INDEX IF NOT EXISTS idx_vcf_af_ on vcf_append(af)",
                "CREATE INDEX IF NOT EXISTS idx_vcf_hgvs_p_ on vcf_append(hgvs_p)",
                "CREATE INDEX IF NOT EXISTS idx_cov_pos_coverage_ on cov_append(pos, coverage)",
                "CREATE INDEX IF NOT EXISTS idx_vcf_pos_ on vcf_append(pos)",
                "CREATE INDEX IF NOT EXISTS idx_vcf_ena_run_ on vcf_append(ena_run)",
            ]:
                db_exec( statement, transaction = True )
        else:
            for statement in [
                "CREATE INDEX IF NOT EXISTS idx_vcf_af on vcf(af)",
                "CREATE INDEX IF NOT EXISTS idx_vcf_hgvs_p on vcf(hgvs_p)",
                "CREATE INDEX IF NOT EXISTS idx_cov_pos_coverage on cov(pos, coverage)",
                "CREATE INDEX IF NOT EXISTS idx_vcf_pos on vcf(pos)",
                "CREATE INDEX IF NOT EXISTS idx_vcf_ena_run on vcf(ena_run)",
            ]:
                db_exec( statement, transaction = True )

    # create materialized views
    if args.create_materialized_views:
        for v in mviews:
            statement = open(os.path.join(p, "mview-{}.t.sql".format(v))).read()
            if args.operate_on_append:
                statement = re.sub('%%POSTFIX%%', '_append', statement )
            else:
                statement = re.sub('%%POSTFIX%%', '', statement )
            db_exec( statement, transaction = True )

    # rename tables
    if args.rename_tables:
        db_exec( "DROP TABLE IF EXISTS unique_cov", transaction = True )
        db_exec( "DROP TABLE IF EXISTS unique_vcf", transaction = True )
        db_exec( "DROP TABLE IF EXISTS vcf_all CASCADE", transaction = True )
        db_exec( "DROP TABLE IF EXISTS vcf CASCADE", transaction = True )
        db_exec( "DROP TABLE IF EXISTS cov CASCADE", transaction = True )
        db_exec( "DROP TABLE IF EXISTS meta CASCADE", transaction = True )
        db_exec( "ALTER TABLE IF EXISTS unique_cov_append RENAME TO unique_cov", transaction = True )
        db_exec( "ALTER TABLE IF EXISTS unique_vcf_append RENAME TO unique_vcf", transaction = True )
        db_exec( "ALTER TABLE IF EXISTS vcf_all_append RENAME TO vcf_all", transaction = True )
        db_exec( "ALTER TABLE IF EXISTS vcf_append RENAME TO vcf", transaction = True )
        db_exec( "ALTER TABLE IF EXISTS cov_append RENAME TO cov", transaction = True )
        db_exec( "ALTER TABLE IF EXISTS meta_append RENAME TO meta", transaction = True )
        db_exec( "ALTER INDEX IF EXISTS idx_vcf_af_ RENAME TO idx_vcf_af", transaction = True )
        db_exec( "ALTER INDEX IF EXISTS idx_vcf_hgvs_p_ RENAME TO idx_vcf_hgvs_p", transaction = True )
        db_exec( "ALTER INDEX IF EXISTS idx_cov_pos_coverage_ RENAME TO idx_cov_pos_coverage", transaction = True )
        db_exec( "ALTER INDEX IF EXISTS idx_vcf_pos_ RENAME TO idx_vcf_pos", transaction = True )
        db_exec( "ALTER INDEX IF EXISTS idx_vcf_ena_run_ RENAME TO idx_vcf_ena_run", transaction = True )
        for mv in mviews:
            db_exec( f"ALTER MATERIALIZED VIEW IF EXISTS {mv}_append RENAME TO {mv}", transaction = True )


    myConnection.close()
    print ("{} disconnected from db engine".format(datetime.datetime.now()))
