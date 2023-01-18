import argparse
import os
import io
import pandas
import psycopg2
import datetime
from common import bulk_insert



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", action = "store",
                    help = "input ecdc table (.gz) file")
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
    parser.add_argument("-e", "--ecdc_table_name", action = "store",
                     help = "the ecdc table", default = 'ecdc_covid_country_weekly')
    parser.add_argument("-c", "--country_table_name", action = "store",
                     help = "the ecdc table", default = 'country')
    args = parser.parse_args()

    assert os.path.exists(args.input), "File not found error: {0}".format(args.input)
    ecdc = pandas.read_csv(args.input, on_bad_lines = 'error', sep = ',')
    ecdc['year'] = ecdc['year_week'].apply(lambda x: int(x.split('-')[0]))
    ecdc['week'] = ecdc['year_week'].apply(lambda x: int(x.split('-')[1]))

    tables = {
        't_ecdc': "{}.{}".format(args.schema, args.ecdc_table_name),
        't_country': "{}.{}".format(args.schema, args.country_table_name),
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

    # lookup
    country = pandas.read_sql(f"SELECT id, iso_a3 FROM {tables['t_country']}", con = conn)

    dataset = pandas.merge(
        left = ecdc[['country_code', 'population', 'indicator', 'weekly_count', 'year', 'week']],
        right = country[['id', 'iso_a3']],
        left_on = 'country_code', right_on = 'iso_a3',
        how = 'left'
    )

    ds_pivot = pandas.pivot_table(
        dataset[['id', 'population', 'year', 'week', 'weekly_count', 'indicator']],
        values = ['weekly_count'],
        columns = ['indicator'],
        index = ['id', 'population', 'year', 'week']
    ).reset_index().astype({
        ('weekly_count', 'cases'): int,
        ('weekly_count', 'deaths'): int,
    })

    bulk_insert(
        ds_pivot[[('id', ''), ('population', ''), ('year', ''), ('week', ''), ('weekly_count', 'cases'), ('weekly_count', 'deaths')]], 
        conn, C, tables['t_ecdc']
    )

    conn.commit()
    C.close()
    conn.close()
    print ("{0} end".format(datetime.datetime.now()))

