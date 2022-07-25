import argparse
import os
import io
import tarfile
import pandas
import psycopg2
import datetime
from common import Map, bulk_insert


def valiDate(x):
    cd, cds = x
    if pandas.isna(cd) | pandas.isna(cds):
        return False
    if cd.count('-') != 2 and cds.count('-') != 2:
        return False
    return cd <= cds


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", action = "store",
                    help = "input metadata tar(.gz) file")
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
    parser.add_argument("-s", "--host_table_name", action = "store",
                     help = "the host map table", default = 'host')
    parser.add_argument("-m", "--metadata_table_name", action = "store",
                     help = "the metadata table", default = 'metadata')
    parser.add_argument("-I", "--instrument_table_name", action = "store",
                     help = "the instrument table", default = 'instrument')
    parser.add_argument("-c", "--country_table_name", action = "store",
                     help = "the country table", default = 'country')
    parser.add_argument("-l", "--library_table_name", action = "store",
                     help = "the library table", default = 'library')
    parser.add_argument("-C", "--collector_table_name", action = "store",
                     help = "the collector table", default = 'collector')
    parser.add_argument("-e", "--extension_table_name", action = "store",
                     help = "the meta extension table", default = 'metaextension')
    args = parser.parse_args()

    assert os.path.exists(args.input), "File not found error: {0}".format(args.input)
    extract_ena_run = lambda x: x.split('/')[-1].split('.')[0]


    tables = {
        't_runid': "{}.{}".format(args.schema, args.runid_table_name),
        't_host': "{}.{}".format(args.schema, args.host_table_name),
        't_metadata': "{}.{}".format(args.schema, args.metadata_table_name),
        't_instrument': "{}.{}".format(args.schema, args.instrument_table_name),
        't_country': "{}.{}".format(args.schema, args.country_table_name),
        't_library': "{}.{}".format(args.schema, args.library_table_name),
        't_collector': "{}.{}".format(args.schema, args.collector_table_name),
        't_extension': "{}.{}".format(args.schema, args.extension_table_name),
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

    the_map = Map(conn, C, tables['t_runid'])

    print ("{0} open tar file {1}".format(datetime.datetime.now(), args.input))
    meta = pandas.read_csv(args.input, on_bad_lines = 'error', sep = '\t')
    meta.drop(columns = ['host_body_site', 'bio_material', 'sample_accession'], inplace = True)

    # lookup runids
    print ("{0} lookup run ids and store new items".format(datetime.datetime.now()))
    runid_map = the_map.get_ids(meta['run_accession'])
    the_map.insert()

    # host table
    print ("{0} process hosts information".format(datetime.datetime.now()))
    host_map = {
        'Homo Sapien': 'Homo sapiens',
        'Homo Sapiens': 'Homo sapiens',
        'homo sapiens': 'Homo sapiens',
        'Homo sapiens': 'Homo sapiens', 
        'HomoSapiens': 'Homo sapiens', 
        'homo sapien': 'Homo sapiens', 
        'Human': 'Homo sapiens', 
    }
    host_mapper = lambda x: host_map[x] if x in host_map else x
    host_db = pandas.read_sql(f"SELECT id, host, tax_id FROM {tables['t_host']}", con = conn)
    meta['host'] = meta['host'].apply(host_mapper)
    host = meta[['host', 'host_tax_id']].drop_duplicates().dropna().sort_values(['host']).reset_index(drop = True)
    host = host
    host_joined = host.merge(host_db, left_on = 'host_tax_id', right_on = 'tax_id', how = 'left')
    m_new = host_joined['id'].isna()
    host_new = host_joined[m_new][['host_x', 'host_tax_id']].reset_index(drop = True).reset_index()
    host_new['index'] += 1 + host_db['id'].max()
    sz = host_new.shape[0]
    if sz > 0:
        print ("{0} #{1} new hosts".format(datetime.datetime.now(), sz))
        bulk_insert(host_new[['index', 'host_x', 'host_tax_id']], conn, C, tables['t_host'])
        host_db = pandas.read_sql(f"SELECT id, host, tax_id FROM {tables['t_host']}", con = conn)
    else:
        print ("{0} no new hosts".format(datetime.datetime.now()))

    # lookup
    print ("{0} load instruments".format(datetime.datetime.now()))
    instrument_db = pandas.read_sql(sql = f"SELECT * FROM {tables['t_instrument']}", con = conn)
    print ("{0} load countries".format(datetime.datetime.now()))
    country_db = pandas.read_sql(sql = f"SELECT * FROM {tables['t_country']}", con = conn)

    # metadata table
    print ("{0} process metadata information".format(datetime.datetime.now()))
    metadata_db = pandas.read_sql(f"""
        SELECT runid, collection_date, collection_date_valid, country_id,
               host_id, host_sex, instrument_id, 
               sample_accession, study_accession, experiment_accession
        FROM {tables['t_metadata']}
    """, con = conn)

    metadata = pandas.merge(
        left = meta[[
            'run_accession', 'collection_date', 'collection_date_submitted', 'country', 
            'host_tax_id', 'host_sex', 'instrument_model', 'instrument_platform', 'accession', 
            'study_accession', 'experiment_accession'
        ]], right = runid_map,
        left_on = 'run_accession', right_on = 'ena_run',
        how = 'inner'
    )
    metadata.rename(columns = {'id': 'runid'}, inplace = True)
    metadata.drop(columns = ['run_accession'], inplace = True)
    metadata['collection_date_valid'] = metadata[['collection_date', 'collection_date_submitted']].apply(valiDate, axis = 1)
    country_map = {
        'Czech Republic': 'Czechia',
        'Myanmar': 'Myanmar/Burma',
        'Russia': 'Russian Federation',
        'State of Palestine': 'Palestine',
        'USA': 'United States'
        }
    xtr_map = lambda x: country_map[x] if x in country_map else x
    metadata['country'] = metadata['country'].apply(lambda x: None if pandas.isna(x) else xtr_map(x.split(':')[0]))
    metadata = pandas.merge(
        left = metadata,  right = country_db,
        left_on = 'country', right_on = 'country_name',
        how = 'left'
    )
    metadata.rename(columns = {'id': 'country_id'}, inplace = True)
    metadata.drop(columns = ['country_name', 'country_name_local', 'iso_a3', 'iso_a2', 'country'], inplace = True)
    metadata['host_sex'] = metadata['host_sex'].apply(lambda x: None if pandas.isna(x) else x.lower())
    metadata = metadata.astype({'host_tax_id': pandas.Int64Dtype()})
    tax_id_missing = metadata['host_tax_id'].isna()
    m_ok = metadata[~ tax_id_missing].set_index('host_tax_id').join(host_db.set_index('tax_id'), how = 'left').reset_index().rename(columns = {'id': 'host_id'}).drop(columns = ['host', 'index'])
    m_ok = m_ok.astype({'host_id': pandas.Int64Dtype()})
    m_nok = metadata[tax_id_missing].rename(columns = { 'host_tax_id': 'host_id' })
    metadata = pandas.concat([m_ok, m_nok])
    metadata = metadata.set_index(['instrument_platform', 'instrument_model']).join(instrument_db.set_index(['instrument_platform', 'instrument_model']), how = 'left').reset_index().rename(columns = {'id': 'instrument_id'}).drop(columns = ['instrument_platform', 'instrument_model'])
    metadata_db['dummy'] = 1
    metadata_join = metadata.merge(metadata_db, left_on = 'runid', right_on = 'runid', how = 'left')
    m_new = metadata_join['dummy'].isna()

    sz = sum(m_new) 
    if sz:
        print ("{0} #{1} new metadata records".format(datetime.datetime.now(), sz))
        K = [
            'runid', 'collection_date_x', 'collection_date_valid_x', 'country_id_x', 
            'host_id_x', 'host_sex_x', 'instrument_id_x', 
            'accession', 'study_accession_x', 'experiment_accession_x'
        ]
        bulk_insert(metadata_join[m_new][K].astype({
            'runid': pandas.Int64Dtype(),
            'country_id_x': pandas.Int64Dtype(),
        }), conn, C, tables['t_metadata'])
        metadata_db = pandas.read_sql(f"""
            SELECT runid, collection_date, collection_date_valid, country_id,
                   host_id, host_sex, instrument_id, 
                   sample_accession, study_accession, experiment_accession
            FROM {tables['t_metadata']}
        """, con = conn)
    else:
        print ("{0} no new metadata records".format(datetime.datetime.now()))
        metadata_db.drop(columns = ['dummy'], inplace = True)

    #library
    print ("{0} process library information".format(datetime.datetime.now()))
    library_db = pandas.read_sql(f"""
        SELECT id, layout, source, selection, strategy
        FROM {tables['t_library']}
    """, con = conn)
    library = meta[['library_layout', 'library_selection', 'library_source', 'library_strategy']].drop_duplicates().sort_values(['library_source', 'library_strategy']).reset_index(drop=True)
    library['library_layout'] = library['library_layout'].map(lambda x: x.lower())
    K_left = ['library_layout', 'library_source', 'library_selection', 'library_strategy']
    K_right = ['layout', 'source', 'selection', 'strategy']
    library_joined = library.merge(library_db, left_on = K_left, right_on = K_right, how = 'left')
    library_joined = library_joined.astype({'id': pandas.Int64Dtype()})
    m_new = library_joined['id'].isna()
    library_new = library_joined[m_new][K_left].reset_index(drop = True).reset_index()
    library_new['index'] += 1 + library_db['id'].max()
    sz = library_new.shape[0]
    if sz > 0:
        print ("{0} #{1} new library records".format(datetime.datetime.now(), sz))
        bulk_insert(library_new[['index', 'library_layout', 'library_source', 'library_selection', 'library_strategy']], conn, C, tables['t_library'])
        library_db = pandas.read_sql(f"""
            SELECT id, layout, source, selection, strategy
            FROM {tables['t_library']}
        """, con = conn)
    else:
        print ("{0} no new library records".format(datetime.datetime.now()))

    # collector
    print ("{0} process collector information".format(datetime.datetime.now()))
    collector_db = pandas.read_sql(f"""
        SELECT id, broker_name, collected_by, center_name
        FROM {tables['t_collector']}
    """, con = conn)
    collector = meta[['broker_name', 'collected_by', 'center_name']].drop_duplicates().sort_values(['center_name', 'broker_name']).reset_index(drop=True)
    nullidx = collector[collector['broker_name'].isna() & collector['collected_by'].isna() & collector['center_name'].isna()].index
    collector.drop(index = nullidx, inplace = True)
    br_nan = collector['broker_name'].isna()
    collector['broker_name'][br_nan] = None
    K = ['broker_name', 'collected_by', 'center_name']
    collector_joined = collector.merge(collector_db, left_on = K, right_on = K, how = 'left')
    m_new = collector_joined['id'].isna()
    collector_joined = collector_joined.astype({'id': pandas.Int64Dtype()})
    collector_new = collector_joined[m_new][K].reset_index(drop = True).reset_index()
    collector_new['index'] += 1 + collector_db['id'].max()
    sz = collector_new.shape[0]
    if sz > 0:
        print ("{0} #{1} new collector records".format(datetime.datetime.now(), sz))
        bulk_insert(collector_new[['index', 'broker_name', 'collected_by', 'center_name']], conn, C, tables['t_collector'])
        collector_db = pandas.read_sql(f"""
            SELECT id, broker_name, collected_by, center_name
            FROM {tables['t_collector']}
        """, con = conn)
    else:
        print ("{0} no new collector records".format(datetime.datetime.now()))

    # meta extension
    print ("{0} process extension information".format(datetime.datetime.now()))
    meta_extension_db = pandas.read_sql(f"""
        SELECT runid, description, fastq_ftp, isolate, sample_capture_status,
            strain, checklist, base_count, library_name, library_id, 
            first_created, first_public, collector_id, country_raw
        FROM {tables['t_extension']}
    """, con = conn)
    K = ['library_layout', 'library_source', 'library_selection', 'library_strategy']
    Kb = ['layout', 'source', 'selection', 'strategy']
    lib_slice = meta[K].copy()
    lib_slice['library_layout'] = lib_slice['library_layout'].apply(lambda x: x.lower())
    lib_id = lib_slice.merge(library_db, left_on = K, right_on = Kb, how = 'left')['id']
    K = [ 'broker_name', 'collected_by', 'center_name' ]
    col_slice = meta[K].copy()
    col_id = col_slice.merge(collector_db, left_on = K, right_on = K, how = 'left')['id']
    extension = pandas.merge(
        left = meta[[
            'run_accession', 'description', 'fastq_ftp', 'isolate', 'sample_capture_status', 'strain',
            'checklist', 'base_count', 'library_name', 'first_created', 'first_public', 'country'
        ]], right = runid_map,
        left_on = 'run_accession', right_on = 'ena_run',
        how = 'inner'
    ).rename(columns = { 'id': 'runid' })
    extension['library_id'] = lib_id
    extension['collector_id'] = col_id
    extension = extension.astype({
        'runid': pandas.Int64Dtype(),
        'collector_id': pandas.Int64Dtype(),
        'library_id': pandas.Int64Dtype(),
    })
    meta_extension_db['dummy'] = 1
    metadata_extension_join = extension.merge(meta_extension_db, left_on = 'runid', right_on = 'runid', how = 'left')
    m_new = metadata_extension_join['dummy'].isna()
    K = [
        'runid', 'description_x', 'fastq_ftp_x', 'isolate_x', 'sample_capture_status_x', 'strain_x',
        'checklist_x', 'base_count_x', 'library_name_x', 'library_id_x', 'first_created_x', 'first_public_x', 
        'collector_id_x', 'country_raw'
    ]
    metadata_extension_new = metadata_extension_join[m_new][K]
    sz = metadata_extension_new.shape[0]
    if sz > 0:
        print ("{0} #{1} new meta extension records".format(datetime.datetime.now(), sz))
        bulk_insert(metadata_extension_new, conn, C, tables['t_extension'])
    else:
        print ("{0} no new meta extension records".format(datetime.datetime.now()))

    conn.commit()
    C.close()
    conn.close()
    print ("{0} end".format(datetime.datetime.now()))

