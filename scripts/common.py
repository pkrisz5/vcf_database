import argparse
import os
import io
import pandas
import psycopg2
import datetime

class Map:
    def __init__(self, conn, cursor, table):
        self.t = table
        self.cursor = cursor
        self.from_db = pandas.read_sql("SELECT id, ena_run FROM {}".format(table), con = conn)
        map_size = self.from_db.shape[0]
        largest_id = 0 if map_size == 0 else self.from_db['id'].max()
        print ("{0} #{1} ena_run items in db, largest id={2}".format(datetime.datetime.now(), map_size, largest_id))
        self.largest_id = largest_id + 1
        self.new = {}

    def get_id(self, ena_run, auto_add = True):
        p = self.from_db[self.from_db['ena_run'] == ena_run]
        if p.empty and auto_add:
            if not ena_run in self.new:
                self.new[ena_run] = self.largest_id
                self.largest_id += 1
            return self.new[ena_run]
        elif not p.empty:
            return p['id'].values[0]
        else:
            print ("ena_run: {0} not found in table {1}".format(ena_run, self.t))

    def get_ids(self, ena_run_series, auto_add = True):
        run_id_map = pandas.merge(
            left = pandas.DataFrame(ena_run_series), right = self.from_db,
            left_on = 'run_accession', right_on = 'ena_run',
            how = 'left'
        )
        na =run_id_map['ena_run'].isna()
        runid_new = run_id_map[na].reset_index()
        sz = runid_new.shape[0]
        if sz > 0 and auto_add:
            runid_new['id'] = runid_new.index + self.largest_id
            runid_new.drop(columns=['index', 'ena_run'], inplace=True)
            runid_new.rename(columns={'run_accession': 'ena_run'}, inplace=True)
            self.new.update(dict(zip(runid_new['ena_run'], runid_new['id'])))
            self.largest_id += sz
        return pandas.concat([runid_new[['ena_run', 'id']].copy(), run_id_map[~na][['ena_run', 'id']].copy()])


    def insert(self):
        cnt = len(self.new)
        if cnt == 0:
            return
        pipe = io.StringIO()
        n = pandas.DataFrame(self.new.items(), columns = ['ena_run', 'id'])
        n[['id', 'ena_run']].to_csv(
            pipe, sep = '\t', header = False, index = False
        )
        pipe.seek(0)
        self.cursor.copy_expert(f"COPY {self.t} FROM STDIN", pipe)
        self.from_db = pandas.concat([self.from_db, n])
        self.new = {}
        print ("{0} #{1} new ena_run items inserted".format(datetime.datetime.now(), cnt))

def uniq(conn, table):
    v = pandas.read_sql("SELECT runid FROM {}".format(table), con = conn)['runid'].values
    print ("{0} #{1} runid items in table {2}".format(datetime.datetime.now(), len(v), table))
    return v


def bulk_insert(table, conn, C, db_table):
    pipe = io.StringIO()
    table.to_csv(
        pipe, sep = '\t', header = False, index = False
    )
    pipe.seek(0)
    C.copy_expert(f"COPY {db_table} FROM STDIN WITH (format csv, delimiter '\t')", pipe)
    pipe.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-H", "--server", action = "store",
                    help="database server name/ip address", default = os.getenv('DB_HOST'))
    parser.add_argument("-P", "--port", action = "store",
                     help = "database server port", default = os.getenv('DB_PORT', 5432))
    parser.add_argument("-D", "--database", action = "store",
                     help = "database name", default = os.getenv('DB', 'coveo'))
    parser.add_argument("-S", "--schema", action = "store",
                     help = "schema name", default = os.getenv('DB_SCHEMA', 'ebi'))
    parser.add_argument("-u", "--user", action = "store",
                     help = "database user", default = os.getenv('SECRET_USERNAME'))
    parser.add_argument("-p", "--password", action = "store",
                     help = "database password", default = os.getenv('SECRET_PASSWORD'))
    parser.add_argument("-r", "--runid_table_name", action = "store",
                     help = "the ena run_id map table", default = 'runid')
    parser.add_argument('run_ids', metavar='ids', type=str, nargs='+',
                    help='a list of ena_run ids to look up')
    args = parser.parse_args()

    table = "{}.{}".format(args.schema, args.runid_table_name)

    conn = psycopg2.connect(
        dbname = args.database,
        host = args.server,
        port = args.port,
        user = args.user,
        password = args.password        
    )
    C = conn.cursor()
    print ("{0} connected to db engine to use db {1}".format(datetime.datetime.now(), args.database))

    the_map = Map(conn, C, table)
    
    for i in args.run_ids:
        ii = the_map.get_id(i, auto_add = False)
        if ii is not None:
            print ("ena_run: {0} is {1}".format(i, ii))

    
    C.close()
    conn.close()
    print ("{0} end".format(datetime.datetime.now()))

