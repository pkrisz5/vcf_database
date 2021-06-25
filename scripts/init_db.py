import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
import sys

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
        print ("SQL: {}".format(statement))
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

    try:
        myConnection = con()
        print ("connected to db {0}".format(db))
    except psycopg2.OperationalError as e:
        myConnection = con(None)
        print ("connected to db engine to create db {0}".format(db))
        # create databases
        db_exec( create_db(db), transaction = False )
        myConnection.close()
        print ("disconnected from db engine")
        myConnection = con()
        print ("connected to db {0}".format(db))
    
    # create user
    statement = create_user(os.getenv('READONLY_USERNAME', 'kooplex-reader'), os.getenv('READONLY_PASSWORD', 'reader-pw'))
    #db_exec( statement, transaction = False )

    # grant read only right to user
    for statement in grant_read(os.getenv('READONLY_USERNAME', 'kooplex-reader'), db):
        db_exec( statement, transaction = True )

    # create tables
    for t in tables:
        statement = open(os.path.join(p, "table-{}.sql".format(t))).read()
        db_exec( statement, transaction = True )
    
#    # create indexes
#    for statement in [
#        "CREATE INDEX IF NOT EXISTS idx_vcf_af on vcf(af)",
#        "CREATE INDEX IF NOT EXISTS idx_vcf_hgvs_p on vcf(hgvs_p)",
#        "CREATE INDEX IF NOT EXISTS idx_cov_pos_coverage on cov(pos, coverage)",
#        "CREATE INDEX IF NOT EXISTS idx_vcf_pos on vcf(pos)",
#        "CREATE INDEX IF NOT EXISTS idx_vcf_ena_run on vcf(ena_run)",
#    ]:
#        db_exec( statement, transaction = True )
#    
#    # create materialized views
#    for v in mviews:
#        statement = open(os.path.join(p, "mview-{}.sql".format(v))).read()
#        db_exec( statement, transaction = True )

#    # filter vcf above threshold
#    db_exec( "TRUNCATE TABLE vcf", transaction = True )
#    db_exec( "SELECT * INTO vcf FROM vcf_all WHERE (\"af\" >= 0.1)", transaction = False )

    myConnection.close()
