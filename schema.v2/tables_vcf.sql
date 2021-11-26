CREATE TABLE IF NOT EXISTS ebi.unique_vcf (
        insertion_ts TIMESTAMP,
        snapshot VARCHAR(32) NOT NULL,
        ena_run VARCHAR(16) NOT NULL,
        integrity ebi.type_integrity NOT NULL
);

CREATE TABLE IF NOT EXISTS ebi.gene_id (
        gene_id VARCHAR(32) NOT NULL,
        gene_name ebi.type_genename NOT NULL
);

-- populate with data
-- ```python
-- gene = pandas.read_csv('table_gene.csv')
-- C.executemany("INSERT INTO ebi.gene_id (gene_id, gene_name) VALUES (%s, %s)", gene[['gene_id', 'gene_name']].values)
-- conn.commit()
-- ```

CREATE TABLE IF NOT EXISTS ebi.vcf_key (
    key                         INT PRIMARY KEY,-- PRIMARY
    ena_run                     VARCHAR(16),       -- ENA ID
    pos                         INT NOT NULL,
    ref                         TEXT NOT NULL,
    alt                         TEXT
);

CREATE TABLE IF NOT EXISTS ebi.vcf (
    key                         INT PRIMARY KEY, --- ONE2ONE
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
    nmd                         ebi.type_nmd,
    major                       BOOLEAN,
    ann_num                     INT
);

CREATE TABLE IF NOT EXISTS ebi.vcf_lof (
    key                         INT, -- MANY2ONE
    lof                         ebi.type_lof
);

CREATE TABLE IF NOT EXISTS ebi.annotation (
    key                         INT, -- ONE2MANY
    annotation                  ebi.type_annotation,
    annotation_impact           ebi.type_annotationimpact,
    gene_name                   ebi.type_genename,
    feature_type                ebi.type_featuretype,
    feature_id                  ebi.type_featureid,
    transcript_biotype          ebi.type_transcriptbiotype,
    rank_                       ebi.type_rank,
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

