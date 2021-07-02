import argparse
import datetime
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
import sys

host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT', 5432)
db = os.getenv('DB')


def db_exec(myConnection, statement, transaction = False, fetch = True):
    try:
        t0 = datetime.datetime.now()
        print ("{0} SQL: {1}".format(t0, statement), file = sys.stderr)
        cur = None
        cur = myConnection.cursor()
        cur.execute( statement )
        if transaction:
            myConnection.commit()
            print ("commited", file = sys.stderr)
        if fetch:
            for r in cur.fetchall():
                print (r)
    except Exception as e:
        print ("ERROR: {}".format(e))
        if transaction:
            myConnection.rollback()
            print ("rolled back", file = sys.stderr)
    finally:
        if cur:
            cur.close()
        t1 = datetime.datetime.now()
        print ("{0} the duration of running statement {1} s".format(t1, t1 - t0), file = sys.stderr)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--list_tables", action = "store_true", help = "list tables")
    parser.add_argument("-i", "--list_indexes", action = "store_true", help = "list indexes")
    parser.add_argument("-v", "--list_materialized_views", action = "store_true", help = "list materialized_views")
    parser.add_argument("-c", "--count", type = str, help = "count records in the table")
    parser.add_argument("-s", "--schema", type = str, help = "describe columns of the table")
    parser.add_argument("-S", "--top10", type = str, help = "select 10 records of the table")
    parser.add_argument("-l", "--locks", action = "store_true", help = "show running queries")
    parser.add_argument("-k", "--kill", type = int, help = "kill a pid")
    args = parser.parse_args()

    con = psycopg2.connect(
            host = host,
            port = port,
            user = os.getenv('SECRET_USERNAME'),
            password = os.getenv('SECRET_PASSWORD'),
            database = db,
            )
    print ("connected to db {0}".format(db), file = sys.stderr)
    try:

        if args.list_indexes:
            statement = """
SELECT tablename, indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'public'
ORDER BY tablename, indexname
            """
            db_exec(con, statement, False, True)
    
        if args.list_tables:
            statement = """
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name
            """
            db_exec(con, statement, False, True)

        if args.list_materialized_views:
            statement = """
SELECT schemaname AS schema_name, matviewname AS view_name, matviewowner AS owner, ispopulated AS is_populated, definition
FROM pg_matviews
ORDER BY schema_name, view_name
            """
            db_exec(con, statement, False, True)
    
        if args.count:
            statement = """
SELECT COUNT(*)
FROM {}
            """.format(args.count)
            db_exec(con, statement, False, True)
    
        if args.schema:
            statement = """
SELECT table_name, column_name, data_type 
FROM information_schema.columns
WHERE table_name = '{}'
            """.format(args.schema)
            db_exec(con, statement, False, True)
    
        if args.top10:
            statement = """
SELECT * 
FROM {} LIMIT 10
            """.format(args.top10)
            db_exec(con, statement, False, True)
    
        if args.locks:
            statement = """
SELECT pid, now() - pg_stat_activity.query_start AS duration, query, state
FROM pg_stat_activity
            """
            db_exec(con, statement, False, True)

        if args.kill:
            statement = """
SELECT pg_terminate_backend({})
            """.format(args.kill)
            db_exec(con, statement, True, True)
    
    finally:
        con.close()
        print ("connected to db {0}".format(db), file = sys.stderr)

