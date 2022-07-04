#!/usr/bin/env python

"""
@summary:
helper script to initialize database.
Implements:
- create database
- create role
- create schema
"""


import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import re
import os
import io
import sys
import datetime
import argparse
import pandas


##################################
# argument parser helpers

cli = argparse.ArgumentParser()
subparsers = cli.add_subparsers(dest="subcommand")

def subcommand(args=[], parent=subparsers):
    args.extend([
        argument("-H", "--server", action="store", help="database server name/ip address", default=os.getenv('DB_HOST')),
        argument("-P", "--port", action="store", help="database server port", default=os.getenv('DB_PORT', 5432)),
        argument("-D", "--database", action="store", help="database name", default=os.getenv('DB', 'coveo')),
        argument("-u", "--user", action="store", help="database user", default=os.getenv('SECRET_USERNAME')),
        argument("-p", "--password", action = "store", help="database password", default = os.getenv('SECRET_PASSWORD')),
    ])
    def decorator(func):
        parser = parent.add_parser(func.__name__, description=func.__doc__)
        for arg in args:
            parser.add_argument(*arg[0], **arg[1])
        parser.set_defaults(func=func)
    return decorator

def argument(*name_or_flags, **kwargs):
    return ([*name_or_flags], kwargs)



##################################
# implemented functionalities

def exec_autocommit(args, sql):
    c = psycopg2.connect(
        host = args.server,
        port = args.port,
        user = args.user,
        password = args.password,
    )
    c.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    C = c.cursor()
    C.execute(sql)
    C.close()
    c.close()

def exec_commit(args, sql):
    c = psycopg2.connect(
        host = args.server,
        port = args.port,
        user = args.user,
        password = args.password,
        database = args.database,
    )
    C = c.cursor()
    C.execute(sql)
    c.commit()
    C.close()
    c.close()

datafile = lambda fn: os.path.join(os.path.dirname(sys.argv[0]), '../data', fn)


@subcommand([])
def create_database(args):
    sql = "CREATE DATABASE {}".format(args.database)
    exec_autocommit(args, sql)


@subcommand([])
def drop_database(args):
    sql = "DROP DATABASE {}".format(args.database)
    exec_autocommit(args, sql)


@subcommand([argument("-S", "--schema", action="store", help="schema name", required=True)])
def create_schema(args):
    sql = f"""
CREATE SCHEMA {args.schema};
REVOKE ALL ON SCHEMA {args.schema} FROM PUBLIC;
    """
    exec_commit(args, sql)


@subcommand([argument("-S", "--schema", action="store", help="schema name", required=True)])
def populate_schema_structure(args):
    schema = args.schema
    def _r(fn, t):
        with open(datafile(fn)) as f:
            A = f.readlines()
        return "CREATE TYPE {}.{} AS ENUM ('{}')".format(schema, t, "', '".join(map(lambda x: x.strip(), A)))
    types_many = {
        'type_annotation_atom': 'enum_annotation_atom.dat',
        'type_lof': 'enum_lof.dat',
        'type_genename': 'enum_genename.dat',
        'type_featureid': 'enum_featureid.dat',
    }

    sql = """
CREATE TYPE {schema}.type_sex AS ENUM ('male', 'female');
CREATE TYPE {schema}.type_layout AS ENUM ('single', 'paired');
CREATE TYPE {schema}.type_status AS ENUM ('active surveillance in response to outbreak', 'active surveillance not initiated by an outbreak', 'other');
CREATE TYPE {schema}.type_integrity AS ENUM ('ok', 'empty file', 'corrupt file');
CREATE TYPE {schema}.type_nmd AS ENUM ('(ORF1ab|GU280_gp01|1|1.00)');
CREATE TYPE {schema}.type_featuretype AS ENUM ('intergenic_region', 'transcript', 'gene_variant');
CREATE TYPE {schema}.type_rank AS ENUM ('1/1', '2/2', '1/2');
CREATE TYPE {schema}.type_transcriptbiotype AS ENUM ('protein_coding');
CREATE TYPE {schema}.type_annotationimpact AS ENUM ('HIGH', 'MODERATE', 'LOW', 'MODIFIER');
CREATE TYPE {schema}.type_quality AS ENUM ('bad', 'good', 'mediocre');
{create_types};

CREATE TABLE IF NOT EXISTS {schema}.runid (
    id                          SERIAL PRIMARY KEY,
    ena_run                     VARCHAR(16) UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS {schema}.country (
    id                                SERIAL PRIMARY KEY,
    iso_a3                            CHAR(3),
    iso_a2                            CHAR(2),
    country_name                      VARCHAR(64),
    country_name_local                TEXT
);
CREATE TABLE IF NOT EXISTS {schema}.collector (
        id                SERIAL PRIMARY KEY,
        broker_name       VARCHAR(64) NULL,
        collected_by      TEXT NULL,
        center_name       TEXT NULL
);
CREATE TABLE IF NOT EXISTS {schema}.host (
        id                SERIAL PRIMARY KEY,
        host              VARCHAR(128) NOT NULL,
        tax_id            int
);
CREATE TABLE IF NOT EXISTS {schema}.instrument (
        id                    SERIAL PRIMARY KEY,
        instrument_platform   VARCHAR(16) NOT NULL,
        instrument_model      VARCHAR(32) NOT NULL,
        UNIQUE (instrument_platform, instrument_model)
);
CREATE TABLE IF NOT EXISTS {schema}.library (
        id                  SERIAL PRIMARY KEY,
        layout              {schema}.type_layout NOT NULL,
        source              VARCHAR(32),
        selection           VARCHAR(32),
        strategy            VARCHAR(32)
        -- FIXME: UNIQUE () ?
);
CREATE TABLE IF NOT EXISTS {schema}.metadata (
        runid                       INT PRIMARY KEY REFERENCES {schema}.runid(id),
        collection_date             DATE NULL,
        collection_date_valid       BOOL,
        country_id                  INT REFERENCES {schema}.country(id) NULL,
        host_id                     INT REFERENCES {schema}.host(id) NULL,
        host_sex                    {schema}.type_sex DEFAULT NULL,
        instrument_id               INT REFERENCES {schema}.instrument(id) NULL,
        sample_accession            VARCHAR(16),
        study_accession             VARCHAR(16),
        experiment_accession        VARCHAR(16)
);
CREATE TABLE IF NOT EXISTS {schema}.metaextension (
        runid                       INT PRIMARY KEY REFERENCES {schema}.runid(id),
        description                 TEXT NULL,
        fastq_ftp                   TEXT,
        isolate                     VARCHAR(128) NULL,
        sample_capture_status       {schema}.type_status NULL,
        strain                      VARCHAR(128),
        checklist                   VARCHAR(16),
        base_count                  DOUBLE PRECISION,
        library_name                VARCHAR(128),
        library_id                  INT REFERENCES {schema}.library(id) NULL,
        first_created               DATE,
        first_public                DATE NULL,
        collector_id                INT REFERENCES {schema}.collector(id),
        country_raw                 TEXT
);
CREATE TABLE IF NOT EXISTS {schema}.unique_cov (
        runid                       INT PRIMARY KEY REFERENCES {schema}.runid(id),
        insertion_ts                TIMESTAMP,
        snapshot                    VARCHAR(32) NOT NULL,
        integrity                   {schema}.type_integrity NOT NULL
);
CREATE TABLE IF NOT EXISTS {schema}.cov (
--    runid                       INT REFERENCES {schema}.runid(id),
    runid                       INT REFERENCES {schema}.unique_cov(runid),
    pos                         int,               -- Position in the sequence
    coverage                    int                -- Coverage in the given position
);
CREATE TABLE IF NOT EXISTS {schema}.unique_vcf (
        runid                       INT PRIMARY KEY REFERENCES {schema}.runid(id),
        insertion_ts                TIMESTAMP,
        snapshot                    VARCHAR(32) NOT NULL,
        integrity                   {schema}.type_integrity NOT NULL
);
CREATE TABLE IF NOT EXISTS {schema}.vcf_key (
    key                         INT PRIMARY KEY,
--    runid                       INT REFERENCES {schema}.runid(id),
    runid                       INT REFERENCES {schema}.unique_vcf(runid),
    pos                         INT NOT NULL,
    ref                         TEXT NOT NULL,
    alt                         TEXT
);
CREATE TABLE IF NOT EXISTS {schema}.annotation_binding (
    key                         INT REFERENCES {schema}.vcf_key(key),
    gene_name                   {schema}.type_genename,
    annotation_atom             {schema}.type_annotation_atom
);
CREATE TABLE IF NOT EXISTS {schema}.vcf (
    key                         INT PRIMARY KEY REFERENCES {schema}.vcf_key(key),
    qual                        INT,
    dp                          INT,
    af                          REAL,
    sb                          INT,
    count_ref_forward_base      INT,
    count_ref_reverse_base      INT,
    count_alt_forward_base      INT,
    count_alt_reverse_base      INT,
    hrun                        INT,
    indel                       BOOLEAN,
    nmd                         {schema}.type_nmd,
    major                       BOOLEAN,
    ann_num                     INT
);
CREATE TABLE IF NOT EXISTS {schema}.annotation (
    key                         INT REFERENCES {schema}.vcf_key(key),
    annotation_impact           {schema}.type_annotationimpact,
    gene_name                   {schema}.type_genename,
    feature_type                {schema}.type_featuretype,
    feature_id                  {schema}.type_featureid,
    transcript_biotype          {schema}.type_transcriptbiotype,
    rank_                       {schema}.type_rank,
    hgvs_c                      TEXT,
    hgvs_p                      TEXT,
    cdna_pos                    INT,
    cdna_length                 INT,
    cds_pos                     INT,
    cds_length                  INT,
    aa_pos                      INT,
    aa_length                   INT,
    distance                    INT,
    errors_warnings_info        TEXT
);
CREATE TABLE IF NOT EXISTS {schema}.gene_id (
        gene_id             VARCHAR(32) UNIQUE NOT NULL,
        gene_name           {schema}.type_genename NOT NULL
);
CREATE TABLE IF NOT EXISTS {schema}.vcf_lof (
    key                         INT PRIMARY KEY REFERENCES {schema}.vcf_key(key),
    lof                         {schema}.type_lof
);
CREATE TABLE IF NOT EXISTS {schema}.lineage_def (
    variant_id             text,
    pango                  text,
    type_variant           text,
    amino_acid_change      text,
    protein_codon_position int,
    ref_protein            text,
    alt_protein            text,
    gene                   text,
    effect                 text,
    snpeff_original_mut    text,
    ref_pos_alt            text,
    ref                    text,
    alt                    text,
    pos                    int,
    description            text
);
CREATE TABLE IF NOT EXISTS {schema}.primer_artic_v3 (
    chrom       CHAR(10), 
    p_start     INT, 
    p_end       INT, 
    name        VARCHAR(32), 
    primerpool  INT, 
    strand      CHAR(1), 
    sequence    TEXT
);
CREATE TABLE IF NOT EXISTS {schema}.primer_artic_v4 (
    chrom       CHAR(10), 
    p_start     INT, 
    p_end       INT, 
    name        VARCHAR(32), 
    primerpool  INT, 
    strand      CHAR(1), 
    sequence    TEXT
);
CREATE TABLE IF NOT EXISTS {schema}.pcr_primers (
    target_gene                VARCHAR(8),
    origin                     VARCHAR(16),
    country_id                 INT REFERENCES {schema}.country(id) NULL,
    type                       VARCHAR(8), 
    primer_name                VARCHAR(16), 
    primer_set                 VARCHAR(16),
    original_primer_name       VARCHAR(32), 
    target_sequence            TEXT, 
    target_sequence_start_pos  INT,
    target_sequence_end_pos    INT, 
    primer_size_bp             INT, 
    reference_genome           VARCHAR(16),
    update_time                DATE, 
    doi                        VARCHAR(48), 
    reference                  VARCHAR(32), 
    other_reference            VARCHAR(32)  -- FIXME: ez gusztustalan oszlop, kell?
);
CREATE TABLE IF NOT EXISTS {schema}.amino_acid_symbol (
    name                VARCHAR(16),
    symbol_3letter      CHAR(3),
    symbol_2letter      CHAR(1)
);
CREATE TABLE IF NOT EXISTS {schema}.lamp_primers (
    target_gene                  VARCHAR(8), 
    origin                       VARCHAR(32), 
    country_id                   INT REFERENCES {schema}.country(id) NULL,
    cat_type                     VARCHAR(8), 
    primer_set                   VARCHAR(16),
    primer_name                  VARCHAR(16), 
    primer_name_type             VARCHAR(20), 
    type                         CHAR(1), 
    original_primer_name         VARCHAR(16),
    primer_sequence_5_3          TEXT, 
    target_sequence_start_pos    INT,
    target_sequence_end_pos      INT, 
    primer_size_bp               INT, 
    reference_genome             VARCHAR(16),
    update_time                  DATE, 
    doi                          VARCHAR(32), 
    reference                    VARCHAR(32)
);
CREATE TABLE IF NOT EXISTS {schema}.ecdc_covid_country_weekly (
    iso_a3                            CHAR(3),
    iso_a2                            CHAR(2),
    country_id                        INT REFERENCES {schema}.country(id) NULL,
    country_name_local                TEXT,
    population                        INT,
    date_year                         INT,
    date_week                         INT,
    ecdc_covid_country_weekly_cases   INT,
    ecdc_covid_country_weekly_deaths  INT
);
-- FIXME: is it used somewhere?
--CREATE TABLE IF NOT EXISTS {schema}.n_content (
--    runid                              INT PRIMARY KEY REFERENCES {schema}.runid(id),
--    num_of_pos_with_cov_nothigher_10   int,
--    estimated_n_content                real,
--    quality_status                     {schema}.type_quality
--);

--FIXME:: CREATE FUNCTION {schema}.lookup_annotation(key INTEGER, gene_name {schema}.type_genename) RETURNS VARCHAR(128)
--FIXME::     LANGUAGE SQL
--FIXME::     IMMUTABLE
--FIXME::     RETURNS NULL ON NULL INPUT
--FIXME::     RETURN CONCAT_WS('&', (
--FIXME::       SELECT CAST(annotation_atom AS VARCHAR(64))
--FIXME::         FROM {schema}.annotation_binding
--FIXME::         WHERE key = key AND gene_name = gene_name
--FIXME::         ));

    """.format(
        schema = schema,
        create_types = ';\n'.join([ _r(fn, t) for t, fn in types_many.items() ]),
    )
    exec_commit(args, sql)


@subcommand([argument("-S", "--schema", action="store", help="schema name", required=True)])
def drop_schema(args):
    sql = "DROP SCHEMA {} CASCADE".format(args.schema)
    exec_commit(args, sql)


@subcommand([argument("-S", "--schema", action="store", help="schema name", required=True)])
def populate_tables(args):
    c = psycopg2.connect(
        host = args.server,
        port = args.port,
        user = args.user,
        password = args.password,
        database = args.database,
    )
    C = c.cursor()
    schema = args.schema

    # country
    country = pandas.read_csv(datafile('country_iso.tsv'), sep = '\t')
    country.index += 1
    pipe = io.StringIO()
    country.to_csv(pipe, sep = '\t', header = False, index = True)
    pipe.seek(0)
    C.copy_expert(f"COPY {schema}.country FROM STDIN WITH (format csv, delimiter '\t')", pipe)
    pipe.close()


    # collector
    collector = pandas.read_csv(datafile('table_collector.tsv'), sep = '\t')
    pipe = io.StringIO()
    collector.to_csv(pipe, sep = '\t', header = False, index = False)
    pipe.seek(0)
    C.copy_expert(f"COPY {schema}.collector FROM STDIN WITH (format csv, delimiter '\t', force_null (broker_name))", pipe)
    pipe.close()

    #host
    host = pandas.read_csv(datafile('table_host.tsv'), sep = '\t')
    pipe = io.StringIO()
    host.to_csv(pipe, sep = '\t', header = False, index = False)
    pipe.seek(0)
    C.copy_expert(f"COPY {schema}.host FROM STDIN WITH (format csv, delimiter '\t')", pipe)
    pipe.close()

    #instrument
    instrument = pandas.read_csv(datafile('table_instrument.tsv'), sep = '\t')
    pipe = io.StringIO()
    instrument.to_csv(pipe, sep = '\t', header = False, index = False)
    pipe.seek(0)
    C.copy_expert(f"COPY {schema}.instrument FROM STDIN WITH (format csv, delimiter '\t')", pipe)
    pipe.close()

    #library
    library = pandas.read_csv(datafile('table_library.tsv'), sep = '\t')
    pipe = io.StringIO()
    library.to_csv(pipe, sep = '\t', header = False, index = False)
    pipe.seek(0)
    C.copy_expert(f"COPY {schema}.library FROM STDIN WITH (format csv, delimiter '\t')", pipe)
    pipe.close()

    #lineage def
    lineage_def = pandas.read_csv(datafile('table_lineage_def.tsv'), sep = '\t').astype({'protein_codon_position': pandas.Int64Dtype()})
    pipe = io.StringIO()
    lineage_def.to_csv(pipe, sep = '\t', header = False, index = False)
    pipe.seek(0)
    C.copy_expert(f"COPY {schema}.lineage_def FROM STDIN WITH (format csv, delimiter '\t')", pipe)
    pipe.close()


    #primer artic v3
    t_pav3_seq = pandas.read_csv('https://raw.githubusercontent.com/joshquick/artic-ncov2019/master/primer_schemes/nCoV-2019/V3/nCoV-2019.tsv', 
                         sep = '\t')[['name', 'seq']]
    t_pav3_seq.rename(columns = { 'seq': 'sequence' }, inplace = True)
    t_pav3 = pandas.read_csv('https://raw.githubusercontent.com/joshquick/artic-ncov2019/master/primer_schemes/nCoV-2019/V3/nCoV-2019.primer.bed', 
                         names = ["chrom", "start", "end", "name", "primerpool", "strand"],
                         sep = '\t')
    t_pav3['start'] += 1
    t_pav3_join = t_pav3.merge(t_pav3_seq, left_on = 'name', right_on = 'name', how = 'inner')
    pipe = io.StringIO()
    t_pav3_join.to_csv(
        pipe, sep = '\t', header = False, index = False
    )
    pipe.seek(0)
    C.copy_expert(f"COPY {schema}.primer_artic_v3 FROM STDIN WITH (format csv, delimiter '\t')", pipe)
    pipe.close()

    #primer artic v4
    t_pav4 = pandas.read_csv('https://raw.githubusercontent.com/joshquick/artic-ncov2019/master/primer_schemes/nCoV-2019/V4/SARS-CoV-2.primer.bed', 
                         names = ["chrom", "start", "end", "name", "primerpool", "strand", "sequence"],
                         sep = '\t')
    t_pav4['start'] += 1
    pipe = io.StringIO()
    t_pav4.to_csv(
        pipe, sep = '\t', header = False, index = False
    )
    pipe.seek(0)
    C.copy_expert(f"COPY {schema}.primer_artic_v4 FROM STDIN WITH (format csv, delimiter '\t')", pipe)
    pipe.close()


    #pcr primer
    t_pcr = pandas.read_csv(datafile('pcr_primers.tsv'), sep = '\t')
    db_country = pandas.read_sql(f'SELECT * FROM {schema}.country', con = c).astype({'id': pandas.Int64Dtype()})
    db_country[db_country['iso_a3'] == 'USA']
    t_pcr['country'] = t_pcr['country'].apply(lambda x: 'United States' if x == 'USA' else x)
    K = list(t_pcr.columns)
    K[K.index('country')] = 'id'
    pcr = pandas.merge(
        left = t_pcr, right = db_country,
        left_on = 'country', right_on = 'country_name',
        how = 'left'
    )
    assert sum(pcr['id'].isna()) == 0, "there are unmapped countries in pcr primer"
    pipe = io.StringIO()
    pcr[K].to_csv(pipe, sep = '\t', header = False, index = False)
    pipe.seek(0)
    C.copy_expert(f"COPY {schema}.pcr_primers FROM STDIN WITH (format csv, delimiter '\t')", pipe)
    pipe.close()

    #amino acid symbols
    t_aas = pandas.read_csv(datafile('amino_acid_symbol.tsv'), sep = '\t')
    pipe = io.StringIO()
    t_aas.to_csv(
        pipe, sep = '\t', header = False, index = False
    )
    pipe.seek(0)
    C.copy_expert(f"COPY {schema}.amino_acid_symbol FROM STDIN WITH (format csv, delimiter '\t')", pipe)
    pipe.close()

    #lamp primer
    t_lamp = pandas.read_csv(datafile('lamp_primers.tsv'), sep = '\t')
    t_lamp['country'] = t_lamp['country'].apply(lambda x: 'United States' if x == 'USA' else x.strip())  # NOTE: China has extra spaces!
    K = list(t_lamp.columns)
    K[K.index('country')] = 'id'
    lamp = pandas.merge(
        left = t_lamp, right = db_country,
        left_on = 'country', right_on = 'country_name',
        how = 'left'
    )
    assert sum(lamp['id'].isna()) == 0, "there are unmapped countries"
    pipe = io.StringIO()
    lamp[K].to_csv(pipe, sep = '\t', header = False, index = False)
    pipe.seek(0)
    C.copy_expert(f"COPY {schema}.lamp_primers FROM STDIN WITH (format csv, delimiter '\t')", pipe)
    pipe.close()

    c.commit()
    C.close()
    c.close()


def create_role(role, password):
    return f"""
DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles
      WHERE  rolname = '{role}') THEN

      CREATE ROLE {role} LOGIN PASSWORD '{password}' NOINHERIT;
   END IF;
END
$do$;
"""


@subcommand([ argument("-R", "--role", action="store", help="role to drop", required=True) ])
def drop_role(args):
    sql = f"""
DROP OWNED BY {args.role};
DROP ROLE {args.role};
    """
    exec_commit(args, sql)


@subcommand([
    argument("-S", "--schema", action="store", help="schema name", required=True),
    argument("-R", "--role_ro", action="store", help="read only role", required=True),
    argument("-W", "--role_rw", action="store", help="read/write role", required=True),
    argument("-r", "--role_ro_pw", action="store", help="read only role's password"),
    argument("-w", "--role_rw_pw", action="store", help="read/write role's password"),
])
def grant_role(args):
    sql = """
{create_ro}
{create_rw}
GRANT CONNECT ON DATABASE {database} TO {role_ro};
GRANT CONNECT ON DATABASE {database} TO {role_rw};
GRANT CREATE ON SCHEMA {schema} TO {role_rw};
GRANT USAGE ON SCHEMA {schema} TO {role_ro};
ALTER ROLE {role_ro} SET search_path={schema};
ALTER ROLE {role_rw} SET search_path={schema};
ALTER DEFAULT PRIVILEGES FOR USER {role_rw} IN SCHEMA {schema} GRANT ALL PRIVILEGES ON TABLES TO {role_rw};
ALTER DEFAULT PRIVILEGES FOR USER {role_rw} IN SCHEMA {schema} GRANT SELECT ON TABLES TO {role_ro};
    """.format(
        role_ro = args.role_ro,
        role_rw = args.role_rw,
        database = args.database,
        create_ro = create_role(args.role_ro, args.role_ro_pw),
        create_rw = create_role(args.role_rw, args.role_rw_pw),
        schema = args.schema,
    )
    exec_commit(args, sql)


if __name__ == "__main__":
    args = cli.parse_args()
    if args.subcommand is None:
        cli.print_help()
    else:
        args.func(args)


