import psycopg2
import re
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
import sys
import datetime
import argparse

host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT', 5432)
db = os.getenv('DB')

p = os.getenv('SCHEMA_PATH', '/x_scripts')

tables = [ 'cov', 'vcf_all', 'vcf', 'meta', 'lineage_def', 'ecdc_covid_country_weekly' ]
mviews = [ 'lineage', 'unique_ena_run_summary' ]

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
    parser = argparse.ArgumentParser()
    parser.parse_args()

    tc = tables[:]
    tc.append('all')

    parser.add_argument("-I", "--init_db", action = "store_true",
                    help = "create database")
    parser.add_argument("-u", "--create_user", action = "store_true",
                    help = "create a database user")
    parser.add_argument("-a", "--grant_access", action = "store_true",
                    help="grant read only right to database user")
    parser.add_argument("-t", "--create_table", choices = tc, default = 'all',
                     help = "create a table")
    parser.add_argument("-p", "--create_tables_append", action = "store_true",
                     help = "create a copy table of vcf and cov to append new data")
    #parser.add_argument("-D", "--drop_table", choices = tc,
    #                 help = "drop a table")
    parser.add_argument("-f", "--filter_vcf", type = float, default = .1,
                    help = "filter vcf_all_append")
    parser.add_argument("-i", "--create_indexes", action = "store_true",
                    help = "create table indexes on *_append")
    parser.add_argument("-r", "--rename_tables", action = "store_true",
                    help = "rename *_append tables")
    parser.add_argument("-m", "--create_materialized_views", action = "store_true",
                    help = "create materialized_views")
    parser.add_argument("-A", "--mv_on_append", action = "store_true",
                    help = "create materialized_views on *_append")

    # create databases
    if args.init_db:
        myConnection = con(None)
        print ("connected to db engine to create db {0}".format(db))
        db_exec( create_db(db), transaction = False )
        myConnection.close()
        print ("disconnected from db engine")

    myConnection = con()
    print ("connected to db {0}".format(db))

    # create user
    if args.create_user:
        statement = create_user(os.getenv('READONLY_USERNAME', 'kooplex-reader'), os.getenv('READONLY_PASSWORD', 'reader-pw'))
        db_exec( statement, transaction = False )

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

    # copy production tables for appending new data
    if args.create_tables_append:
        db_exec( "DROP TABLE IF EXISTS vcf_all_append", transaction = True )
        db_exec( "CREATE TABLE vcf_all_append AS SELECT * FROM vcf_all", transaction = True )
        db_exec( "DROP TABLE IF EXISTS cov_append", transaction = True )
        db_exec( "CREATE TABLE cov_append AS SELECT * FROM cov", transaction = True )

    # filter vcf_all above threshold
    if args.filter_vcf:
        db_exec( "TRUNCATE TABLE IF EXISTS vcf_append", transaction = True )
        db_exec( "SELECT * INTO vcf_append FROM vcf_all_append WHERE (\"af\" >= {args.filter_vcf})", transaction = False )

    # create indexes
    if args.create_indexes:
        for statement in [
            "CREATE INDEX IF NOT EXISTS idx_vcf_af_ on vcf_append(af)",
            "CREATE INDEX IF NOT EXISTS idx_vcf_hgvs_p_ on vcf_append(hgvs_p)",
            "CREATE INDEX IF NOT EXISTS idx_cov_pos_coverage_ on cov_append(pos, coverage)",
            "CREATE INDEX IF NOT EXISTS idx_vcf_pos_ on vcf_append(pos)",
            "CREATE INDEX IF NOT EXISTS idx_vcf_ena_run_ on vcf_append(ena_run)",
        ]:
            db_exec( statement, transaction = True )

    # create materialized views
    if args.create_materialized_views:
        for v in mviews:
            statement = open(os.path.join(p, "mview-{}.sql".format(v))).read()
            if args.mv_on_append:
                statement = re.sub('(FROM cov)', '\1_append', statement )
                statement = re.sub('(FROM vcf)', '\1_append', statement )
            db_exec( statement, transaction = True )

    # rename tables
    if args.rename_tables:
        db_exec( "DROP TABLE IF EXISTS vcf_all CASCADE", transaction = True )
        db_exec( "DROP TABLE IF EXISTS vcf CASCADE", transaction = True )
        db_exec( "DROP TABLE IF EXISTS cov CASCADE", transaction = True )
        db_exec( "ALTER TABLE IF EXISTS vcf_all_append RENAME TO vcf_all", transaction = True )
        db_exec( "ALTER TABLE IF EXISTS vcf_append RENAME TO vcf", transaction = True )
        db_exec( "ALTER TABLE IF EXISTS cov_append RENAME TO cov", transaction = True )
        db_exec( "ALTER INDEX IF EXISTS idx_vcf_af_ RENAME TO idx_vcf_af", transaction = True )
        db_exec( "ALTER INDEX IF EXISTS idx_vcf_hgvs_p_ RENAME TO idx_vcf_hgvs_p", transaction = True )
        db_exec( "ALTER INDEX IF EXISTS idx_cov_pos_coverage_ RENAME TO idx_cov_pos_coverage", transaction = True )
        db_exec( "ALTER INDEX IF EXISTS idx_vcf_pos_ RENAME TO idx_vcf_pos", transaction = True )
        db_exec( "ALTER INDEX IF EXISTS idx_vcf_ena_run_ RENAME TO idx_vcf_ena_run", transaction = True )


    myConnection.close()
