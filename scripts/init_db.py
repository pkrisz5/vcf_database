import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
import sys

host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT', 5432)
db = os.getenv('DB')

p = os.getenv('SCHEMA_PATH', '/x_scripts')

tables = [ 'cov', 'vcf_all', 'vcf', 'meta', 'lineage_def' ]
mviews = [ 'lineage', 'unique_ena_run_summary' ]

def create_db(db):
    #return "SELECT 'CREATE DATABASE \"{0}\"' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '{0}')".format(db)
    return "CREATE DATABASE \"{0}\"".format(db)

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
    except psycopg2.OperationalError as e:
        myConnection = con(None)
        # create databases
        db_exec( create_db(db), transaction = False )
        myConnection.close()
        myConnection = con()
    
    # create tables
    for t in tables:
        statement = open(os.path.join(p, "table-{}.sql".format(t))).read()
        db_exec( statement, transaction = True )
    
#    # create indexes
#    for statement in [
#        "CREATE INDEX IF NOT EXISTS idx_vcf_af on vcf(af);",
#        "CREATE INDEX IF NOT EXISTS idx_vcf_hgvs_p on vcf(hgvs_p)",
#        "CREATE INDEX IF NOT EXISTS idx_cov_pos_coverage on cov(pos, coverage);",
#    ]:
#        db_exec( statement, transaction = True )
#    
#    # create materialized views
#    for v in mviews:
#        statement = open(os.path.join(p, "mview-{}.sql".format(v))).read()
#        db_exec( statement, transaction = True )

    myConnection.close()
