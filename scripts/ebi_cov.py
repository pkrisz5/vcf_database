import argparse
import os
import io
import tarfile
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

    def get_id(self, ena_run):
        p = self.from_db['ena_run'] == ena_run
        if sum(p) == 0:
            if not ena_run in self.new:
                self.new[ena_run] = self.largest_id
                self.largest_id += 1
            return self.new[ena_run]
        elif sum(p) == 1:
            return self.from_db[p]['id'].values[0]

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

def bulk_insert(tables, conn, C, snapshot, COV, uniq, cnt):
    COVC = pandas.concat(COV)
    print ("{0} pushing {1} records in db".format(datetime.datetime.now(), COVC.shape[0]))
    pipe = io.StringIO()
    COVC[['ena_run', 'id', 'coverage']].to_csv(
        pipe, sep = '\t', header = False, index = False
    )
    pipe.seek(0)
    C.copy_expert(f"COPY {tables['t_cov']} FROM STDIN", pipe)
    while len(COV):
        cov = COV.pop()
        del cov
    del COVC
    pipe.close()

    pipe = io.StringIO()
    status = pandas.DataFrame(
        columns = ('timestamp', 'ena_run', 'integrity'),
        data = uniq
    )
    status['snapshot'] = snapshot
        
    status[['ena_run', 'timestamp', 'snapshot', 'integrity']].to_csv(
        pipe, sep = '\t', header = False, index = False
    )
    pipe.seek(0)
    print ("{0} pushing {1} unique records in db".format(datetime.datetime.now(), cnt))
    C.copy_expert(f"COPY {tables['t_unique']} FROM STDIN", pipe)
    pipe.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", action = "store",
                    help = "input coverage tar(.gz) file")
    parser.add_argument("-s", "--snapshot", action = "store",
                    help = "snapshot label")
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
    parser.add_argument("-f", "--filter_coverage_upper_threshold", action = "store",
                     help = "keep only those positions where the coverage value is below the threshold", default = 100)
    parser.add_argument("-b", "--batch_size", action = "store",
                     help = "insert maximum batch size samples in a single database transaction", default = 500)
    parser.add_argument("-t", "--cov_table_name", action = "store",
                     help = "the name of the target coverage table in the database", default = 'cov')
    parser.add_argument("-m", "--covunique_table_name", action = "store",
                     help = "the name of the target cov unique table in the database", default = 'unique_cov')
    parser.add_argument("-F", "--commit_when_finished", action = "store_true",
                     help = "commit transaction only in the very end")
    args = parser.parse_args()

    assert os.path.exists(args.input), "File not found error: {0}".format(args.input)
    extract_ena_run = lambda x: x.split('/')[-1].split('.')[0]


    tables = {
        't_runid': "{}.{}".format(args.schema, args.runid_table_name),
        't_cov': "{}.{}".format(args.schema, args.cov_table_name),
        't_unique': "{}.{}".format(args.schema, args.covunique_table_name),
    }

    conn = psycopg2.connect(
        dbname = args.database,
        host = args.server,
        port = args.port,
        user = args.user,
        password = args.password        
    )
    C = conn.cursor()
    print ("{0} connected to db engine to use db {1}".format(datetime.datetime.now(), args.database))

    snapshot = args.snapshot if args.snapshot else extract_ena_run(args.input)

    the_map = Map(conn, C, tables['t_runid'])

    T = tarfile.open(args.input)
    print ("{0} open tar file {1}, snapshot: {2}".format(datetime.datetime.now(), args.input, snapshot))

    ts = []
    ena_run = []
    integrity = []
    COV = []

    counter = 0
    while True:
        ti = T.next()
        if ti is None:
            T.close()
            print ("{0} loop ends closed tarfile".format(datetime.datetime.now()))
            break
        if not ti.isfile():
            continue

        counter += 1
        now = datetime.datetime.now()
        ts.append( now.isoformat() )
        runid = the_map.get_id( extract_ena_run(ti.name) )
        ena_run.append( runid )
        #print ("{0} start to process {1}, ena_run {2}".format(now, ti.name, runid))
    
        buf = T.extractfile(ti)
        try:
            cov = pandas.read_csv(buf, 
                on_bad_lines = 'error',
                names = ('id', 'ref', 'coverage'),
                compression = 'gzip'
            )
            records = cov.shape[0]
        except Exception as e:
            print ("{0} cannot parse file {1}: reason {2}".format(now, ti.name, str(e)))
            records = -1
            cov = None
        finally:
            buf.close()
            del buf

        if records == 0:
            integrity.append('empty file')
            print ("{0} empty file {1}".format(now, ti.name))
            del cov
            continue
        if records != 29903:
            integrity.append('corrupt file')
            print ("{0} corrupt file {1}, size: {2}".format(now, ti.name, records))
            del cov
            continue
    
        integrity.append('ok')

        if args.filter_coverage_upper_threshold > 0:
            cov = cov[cov['coverage'] <= args.filter_coverage_upper_threshold ]

        cov['ena_run'] = runid

        COV.append(cov)

        if counter == args.batch_size:
            the_map.insert()
            uniq = zip(ts, ena_run, integrity)
            bulk_insert(tables, conn, C, snapshot, COV, uniq, counter)
            counter = 0
            COV = []
            ts = []
            ena_run = []
            integrity = []
    
    if counter:
        the_map.insert()
        uniq = zip(ts, ena_run, integrity)
        bulk_insert(tables, conn, C, snapshot, COV, uniq, counter)

    conn.commit()
    
    C.close()
    conn.close()
    print ("{0} end".format(datetime.datetime.now()))

