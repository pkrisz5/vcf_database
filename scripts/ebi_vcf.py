import argparse
import os
import io
import tarfile
import pandas
import psycopg2
import datetime
#import numpy
import gzip


def bulk_insert(skip_commit, tables, offset, conn, C, snapshot, VCF, ANN, uniq, cnt):
    VCFC = pandas.concat(VCF)
    VCFC.reset_index(inplace = True)
    VCFC.drop(columns = ['index'], inplace = True)
    VCFC.reset_index(inplace = True)
    VCFC['index'] += offset
    keymax = VCFC['index'].max() + 1
    print ("{0} pushing {1} records in db".format(datetime.datetime.now(), VCFC.shape[0]))
    C.execute("SET search_path TO ebi")
    pipe = io.StringIO()
    VCFC[['index', 'ena_run', 'pos', 'ref', 'alt']].to_csv(
            pipe, sep = '\t', header = False, index = False, na_rep = 'None'
    )
    pipe.seek(0)
    C.copy_from(pipe, tables['t_key'], null = 'None')
    pipe.close()
    pipe = io.StringIO()
    VCFC[['index', 'qual', 'dp', 'af', 'sb',
     'count_ref_forward_base', 'count_ref_reverse_base',
     'count_alt_forward_base', 'count_alt_reverse_base',
     'hrun', 'indel', 'lof', 'nmd', 'major', 'ann_num']].to_csv(
            pipe, sep = '\t', header = False, index = False, na_rep = 'None'
    )
    pipe.seek(0)
    C.copy_from(pipe, tables['t_vcf'], null = 'None')
    pipe.close()
    #del pipe
    
    ANNC = pandas.concat(ANN).merge(VCFC, how = 'left', on = ('ena_run', 'pos', 'ref', 'alt'))
    pipe = io.StringIO()
    ANNC[['index', 'annotation', 'annotation_impact', 'gene_name', 'feature_type',
      'feature_id', 'transcript_biotype', 'rank_', 'hgvs_c', 'hgvs_p', 'cdna_pos',
      'cdna_length', 'cds_pos', 'cds_length', 'aa_pos', 'aa_length', 'distance',
      'errors_warnings_info']].to_csv(
            pipe, sep = '\t', header = False, index = False, na_rep = 'None'
    )
    pipe.seek(0)
    C.copy_from(pipe, tables['t_ann'], null = 'None')
    pipe.close()

    pipe = io.StringIO()
    status = pandas.DataFrame(
        columns = ('timestamp', 'ena_run', 'integrity'),
        data = uniq
    )
    status['snapshot'] = snapshot
        
    status[['timestamp', 'snapshot', 'ena_run', 'integrity']].to_csv(
        pipe, sep = '\t', header = False, index = False
    )
    pipe.seek(0)
    print ("{0} pushing {1} unique records in db".format(datetime.datetime.now(), cnt))
    C.copy_from(pipe, tables['t_meta'])
    pipe.close()

    if not skip_commit:
        conn.commit()
    del pipe
    return keymax



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", action = "store",
                    help = "input vcf tar(.gz) file")
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
    parser.add_argument("-b", "--batch_size", action = "store",
                     help = "insert maximum batch size samples in a single database transaction", default = 500)
    parser.add_argument("-t", "--vcf_table_name", action = "store",
                     help = "the name of the target vcf table in the database", default = 'vcf')
    parser.add_argument("-k", "--vcfkey_table_name", action = "store",
                     help = "the name of the target vcf key table in the database", default = 'vcf_key')
    parser.add_argument("-m", "--vcfunique_table_name", action = "store",
                     help = "the name of the target vcf unique table in the database", default = 'unique_vcf')
    parser.add_argument("-a", "--vcfannotation_table_name", action = "store",
                     help = "the name of the target vcf annotation table in the database", default = 'annotation')
    parser.add_argument("-F", "--commit_when_finished", action = "store_true",
                     help = "commit transaction only in the very end")
    args = parser.parse_args()

    
    assert os.path.exists(args.input), "File not found error: {0}".format(args.input)
    extract_ena_run = lambda x: x.split('/')[-1].split('.')[0]
    not_indel = lambda x: x != 'INDEL'
    mysplitrec = lambda x: x.split(';')
    info = lambda x: filter(not_indel, mysplitrec(x))
    info_dict = lambda x: dict(map(lambda y: tuple(y.split('=')), info(x)))
    indel = lambda x: 'INDEL' in x
    proc_dp4 = lambda x: dict(zip(dp4_labels, x['DP4'].split(',')))
    proc_ann = lambda x: list(map(lambda y: dict(zip(ann_labels, y.split('|'))), x['ANN'].split(',')))
    before_per = lambda x: x.split('/')[0] if '/' in x else None
    after_per = lambda x: x.split('/')[1] if '/' in x else None
    qual = lambda x: None if (x is None) or (x == '.') else int(x)
    na = lambda x: None if x == '' else x

    dp4_labels = [
        'count_ref_forward_base', 'count_ref_reverse_base', 
        'count_alt_forward_base', 'count_alt_reverse_base'
    ]
    
    ann_labels = [
        'allele', 'annotation', 'annotation_impact', 'gene_name', 
        'gene_id', 'feature_type', 'feature_id', 'transcript_biotype', 
        'rank_', 'hgvs_c', 'hgvs_p', 'cdna_pos__cdna_length', 
        'cds_pos__cds_length', 'aa_pos__aa_length', 'distance', 'errors_warnings_info'
    ]

    tables = {
        't_vcf': "{}.{}".format(args.schema, args.vcf_table_name),
        't_key': "{}.{}".format(args.schema, args.vcfkey_table_name),
        't_ann': "{}.{}".format(args.schema, args.vcfannotation_table_name),
        't_meta': "{}.{}".format(args.schema, args.vcfunique_table_name),
    }
    
    conn = psycopg2.connect(
        dbname = args.database,
        host = args.server,
        port = args.port,
        user = args.user,
        password = args.password        
    )
    C = conn.cursor()
    C.execute('SELECT MAX(key) FROM {}'.format(tables['t_key']))
    offset, = C.fetchall()[0]
    if offset is None:
        offset = 0
    else:
        offset += 1
    print ("{0} connected to db engine to use db {1} offset is {2}".format(datetime.datetime.now(), args.database, offset))

    snapshot = args.snapshot if args.snapshot else extract_ena_run(args.input)

    T = tarfile.open(args.input)
    print ("{0} open tar file {1}, snapshot: {2}".format(datetime.datetime.now(), args.input, snapshot))

    ts = []
    ena_run = []
    integrity = []
    VCF = []
    ANN = []

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
        runid = extract_ena_run(ti.name)
        ena_run.append( runid )
        #print ("{0} start to process {1}, ena_run {2}".format(now, ti.name, runid))
    
        buf = T.extractfile(ti)
        try:
            vcf = pandas.read_csv(buf, 
                  comment = '#',
#                  on_bad_lines = 'error',
                  sep = '\t',
                  names = ('CHROM', 'pos', 'ID', 'ref', 'alt', 'QUAL', 'filter', 'INFO'),
                  dtype = { 'pos': int },
                  compression = 'gzip'
            )
            records = vcf.shape[0]
        except Exception as e:
            print ("{0} cannot parse file {1}: reason {2}".format(now, ti.name, str(e)))
            integrity.append('corrupt file')
            continue
        finally:
            buf.close()
            del buf

        if records == 0:
            integrity.append('empty file')
            print ("{0} empty file {1}".format(now, ti.name))
            del vcf
            continue
    
        integrity.append('ok')

        info_dict_seq = vcf['INFO'].apply(info_dict)
        dp4_seq = info_dict_seq.apply(proc_dp4)
        ann_seq = info_dict_seq.apply(proc_ann)
        
        vcf['ena_run'] = runid
        vcf['indel'] = vcf['INFO'].apply(indel)
        vcf.drop(columns = ['INFO', 'CHROM'], inplace = True)
        vcf['dp'] = info_dict_seq.apply(lambda x: x.get('DP'))
        vcf['af'] = info_dict_seq.apply(lambda x: x.get('AF'))
        for l in dp4_labels:
            vcf[l] = dp4_seq.apply(lambda x: x.get(l))
        vcf['major'] = info_dict_seq.apply(lambda x: x.get('MAJOR'))
        vcf['lof'] = info_dict_seq.apply(lambda x: x.get('LOF'))
        vcf['nmd'] = info_dict_seq.apply(lambda x: x.get('NMD'))
        vcf['sb'] = info_dict_seq.apply(lambda x: x.get('SB'))
        vcf['hrun'] = info_dict_seq.apply(lambda x: x.get('HRUN'))
        vcf['ann_num'] = ann_seq.apply(len)
        vcf['qual'] = vcf['QUAL'].apply(qual)
        vcf.drop(columns = ['QUAL'], inplace = True)

        data = []
        index = []
        for x, l in ann_seq.iteritems():
            for i in l:
                data.append(i)
                index.append(x)

        ann = pandas.DataFrame(data = data, index = index).join(vcf)
        ann.drop(columns = ['ID', 'filter', 'indel', 'dp', 'af',
               'count_ref_forward_base', 'count_ref_reverse_base',
               'count_alt_forward_base', 'count_alt_reverse_base', 'major', 'lof',
               'nmd', 'sb', 'hrun', 'ann_num', 'qual', 'allele', 'gene_id'],
              inplace = True)
        ann['cdna_pos'] = ann['cdna_pos__cdna_length'].apply(before_per)
        ann['cdna_length'] = ann['cdna_pos__cdna_length'].apply(after_per)
        ann['cds_pos'] = ann['cds_pos__cds_length'].apply(before_per)
        ann['cds_length'] = ann['cds_pos__cds_length'].apply(after_per)
        ann['aa_pos'] = ann['aa_pos__aa_length'].apply(before_per)
        ann['aa_length'] = ann['aa_pos__aa_length'].apply(after_per)
        ann.drop(columns = ['cdna_pos__cdna_length', 'cds_pos__cds_length', 'aa_pos__aa_length'], inplace = True)
        ann['transcript_biotype'] = ann['transcript_biotype'].apply(na)
        ann['rank_'] = ann['rank_'].apply(na)
        ann['distance'] = ann['distance'].apply(lambda x: int(x) if x else None)
        
        
        VCF.append(vcf)
        ANN.append(ann)

        if counter == args.batch_size:
            uniq = zip(ts, ena_run, integrity)
            offset = bulk_insert(args.commit_when_finished, tables, offset, conn, C, snapshot, VCF, ANN, uniq, counter)
            counter = 0
            VCF = []
            ANN = []
            ts = []
            ena_run = []
            integrity = []
    
    if counter:
        uniq = zip(ts, ena_run, integrity)
        bulk_insert(args.commit_when_finished, tables, offset, conn, C, snapshot, VCF, ANN, uniq, counter)

    if args.commit_when_finished:
        conn.commit()

    
    C.close()
    conn.close()
    print ("{0} end".format(datetime.datetime.now()))


