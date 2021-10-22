import argparse
import os
import io
import tarfile
import pandas
import psycopg2
import datetime

def extract_ena_run(fn):
    return fn.split('/')[-1].split('.')[0]

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
    parser.add_argument("-u", "--user", action = "store",
                     help = "database user", default = os.getenv('SECRET_USERNAME'))
    parser.add_argument("-p", "--password", action = "store",
                     help = "database password", default = os.getenv('SECRET_PASSWORD'))
    parser.add_argument("-f", "--filter_coverage_upper_threshold", action = "store",
                     help = "keep only those positions where the coverage value is below the threshold", default = 100)
    parser.add_argument("-b", "--batch_size", action = "store",
                     help = "insert maximum batch size samples in a single database transaction", default = 500)
    parser.add_argument("-t", "--coverage_table_name", action = "store",
                     help = "the name of the target table in the database", default = 'cov')
    args = parser.parse_args()

    assert os.path.exists(args.input), "File not found error: {0}".format(args.input)

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
            print ("{0} loop ends".format(datetime.datetime.now()))
            break
        if not ti.isfile():
            continue

        counter += 1
        now = datetime.datetime.now()
        ts.append( now.isoformat() )
        runid = extract_ena_run(ti.name)
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
            del buf
        except Exception as e:
            print ("{0} cannot parse file {1}: reason {2}".format(now, ti.name, str(e)))
            records = -1
            cov = None

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
            COVC = pandas.concat(COV)
            print ("{0} pushing {1} records in db".format(datetime.datetime.now(), COVC.shape[0]))
            pipe = io.StringIO()
            COVC[['ena_run', 'id', 'coverage']].to_csv(
                    pipe, sep = '\t', header = False, index = False
            )
            pipe.seek(0)
            contents = pipe.getvalue()
            C.copy_from(pipe, args.coverage_table_name)
            conn.commit()
            counter = 0
            while len(COV):
                cov = COV.pop()
                del cov
            del COVC
            del pipe
    
    T.close()
    print ("{0} closed tarfile, processed records {1}".format(datetime.datetime.now(), len(ts)))
    
    if counter:
        COVC = pandas.concat(COV)
        print ("{0} pushing {1} records in db".format(datetime.datetime.now(), COVC.shape[0]))
        pipe = io.StringIO()
        COVC[['ena_run', 'id', 'coverage']].to_csv(
                pipe, sep = '\t', header = False, index = False
        )
        pipe.seek(0)
        contents = pipe.getvalue()
        C.copy_from(pipe, args.coverage_table_name)
        conn.commit()
        del pipe
        while len(COV):
            cov = COV.pop()
            del cov
        del COVC

    if args.coverage_table_name == 'cov':
        pipe = io.StringIO()
        status = pandas.DataFrame(
            columns = ('timestamp', 'ena_run', 'integrity'),
            data = zip(ts, ena_run, integrity)
        )
        status['snapshot'] = snapshot
        
        status[['timestamp', 'snapshot', 'ena_run', 'integrity']].to_csv(
            pipe, sep = '\t', header = False, index = False
        )
        pipe.seek(0)
        contents = pipe.getvalue()
        C.copy_from(pipe, 'unique_cov')
        conn.commit()
    
    C.close()
    conn.close()
    print ("{0} end".format(datetime.datetime.now()))

