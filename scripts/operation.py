import psycopg2
import psycopg2.extras
import json
import os
import sys
import datetime
import argparse

host = os.getenv('DB_HOST')
port = os.getenv('DB_PORT', 5432)
db = os.getenv('DB')

def db_exec(statement, transaction, fetch):
    try:
        t0 = datetime.datetime.now()
        print ("{0} SQL: {1}".format(t0, statement), file = sys.stderr)
        cur = None
        cur = myConnection.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
        cur.execute( statement )
        response = cur.fetchall() if fetch else None
        if transaction:
            myConnection.commit()
            print ("commited", file = sys.stderr)
    except Exception as e:
        print ("ERROR: {}".format(e), file = sys.stderr)
        if transaction:
            myConnection.rollback()
            print ("rolled back", file = sys.stderr)
    finally:
        if cur:
            cur.close()
        t1 = datetime.datetime.now()
        print ("{0} the duration of running statement {1}".format(t1, t1 - t0), file = sys.stderr)
        if response:
            for r in response:
                if 'event_date' in r:
                    r['event_date'] = r['event_date'].isoformat()
        return response


def con(db):
    return psycopg2.connect(
        host = host,
        port = port,
        user = os.getenv('SECRET_USERNAME'),
        password = os.getenv('SECRET_PASSWORD'),
        database = db,
    )

if __name__ == '__main__':
## the table schema
## CREATE TABLE IF NOT EXISTS operation (
##      event_date date,
##      last_stage int,
##      last_exit_code int,
##      stage int,
##      exit_code int,
## 	extra_info text -- json encoded information
## );
    parser = argparse.ArgumentParser()
    #parser.add_argument("-c", "--command", choices = [ 'init', 'truncate', 'dump', 'append', 'get' ], default = 'get',
    #                help = "operation command")

    subparsers = parser.add_subparsers(help = 'Choose a command')
    init_parser = subparsers.add_parser('init', help = '"init" help')
    init_parser.set_defaults(action = lambda: 'init')
    truncate_parser = subparsers.add_parser('truncate', help = '"truncate" help')
    truncate_parser.set_defaults(action = lambda: 'truncate')
    dump_parser = subparsers.add_parser('dump', help = '"dump" help')
    dump_parser.set_defaults(action = lambda: 'dump')
    get_parser = subparsers.add_parser('get', help = '"get" help')
    get_parser.set_defaults(action = lambda: 'get')
    append_parser = subparsers.add_parser('append', help = '"append" help')
    append_parser.set_defaults(action = lambda: 'append')
    append_parser.add_argument('-s', '--stage', required = True, type = int, 
            help = 'set new stage')
    append_parser.add_argument('-c', '--code', required = True, type = int, 
            help = 'set exit code')
    append_parser.add_argument('-e', '--extra', required = True, 
            help = 'set json serialized extra information')
    assert_parser = subparsers.add_parser('assert', help = '"assert" help')
    assert_parser.set_defaults(action = lambda: 'assert')
    assert_parser.add_argument('-s', '--stage', required = True, type = int, 
            help = 'make sure the current stage is matched')

    args = parser.parse_args()
    try:
        command = args.action()
    except:
        print ("make sure {0} is run with proper command line argiments".format(sys.argv[0]), file = sys.stderr)
        raise

    try:
        myConnection = con(db)
        print ("{0} connected to db {1}".format(datetime.datetime.now(), db), file = sys.stderr)
    
        if command == 'init':
            resp = db_exec("SELECT * FROM operation", transaction = False, fetch = True)
            assert len(resp) == 0, "Table operation must be initialized already"
            db_exec("INSERT INTO operation (event_date, last_stage, last_exit_code, stage, exit_code, extra_info) VALUES ('{0}', -1, -1, 0, 0, '{1}')".format(datetime.datetime.now(), {}), transaction = True, fetch = False)
    
        if command == 'truncate':
            db_exec("TRUNCATE TABLE operation", transactio = True, fetch = False)
    
        if command == 'append':
            resp = db_exec("SELECT stage, exit_code FROM operation ORDER BY event_date DESC LIMIT 1", transaction = False, fetch = True)
            print (json.dumps(resp[0]))
            db_exec("INSERT INTO operation (event_date, last_stage, last_exit_code, stage, exit_code, extra_info) VALUES ('{0}', {1}, {2}, {3}, {4}, '{5}')".format(
                datetime.datetime.now(), resp[0]['stage'], resp[0]['exit_code'], args.stage, args.code, args.extra
                ), transaction = True, fetch = False)
    
        if command == 'dump':
            print (db_exec("SELECT * FROM operation ORDER BY event_date DESC", transaction = False, fetch = True))
    
        if command == 'get':
            resp = db_exec("SELECT * FROM operation ORDER BY event_date DESC LIMIT 1", transaction = False, fetch = True)
            print (json.dumps(resp[0]))
    
        if command == 'assert':
            resp = db_exec("SELECT stage, exit_code FROM operation ORDER BY event_date DESC LIMIT 1", transaction = False, fetch = True)
            assert resp[0]['exit_code'] == 0, 'Last command was not exited cleanly'
            assert resp[0]['stage'] == args.stage, 'Stage mismatch'
    
    
    finally:
        myConnection.close()
        print ("{} disconnected from db engine".format(datetime.datetime.now()), file = sys.stderr)
