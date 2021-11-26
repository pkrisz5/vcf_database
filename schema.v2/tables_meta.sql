CREATE TABLE IF NOT EXISTS instrument (
        id SERIAL PRIMARY KEY,
        instrument_platform VARCHAR(16) NOT NULL,
        instrument_model VARCHAR(32) NOT NULL
);

CREATE TABLE IF NOT EXISTS host (
        id SERIAL PRIMARY KEY,
        host VARCHAR(32) NOT NULL,
        tax_id int
);

CREATE TABLE IF NOT EXISTS metadata (
        ena_run VARCHAR(16) PRIMARY KEY,
        collection_date DATE NULL,
        collection_date_valid BOOL,
        country VARCHAR(32) NULL,
        host_id INT REFERENCES ebi.host(id) NULL,
        host_sex ebi.type_sex NULL,
        instrument_id INT REFERENCES ebi.instrument(id) NULL,
        sample_accession VARCHAR(16),
        study_accession VARCHAR(16),
        experiment_accession VARCHAR(16)
);

CREATE TABLE IF NOT EXISTS library (
        id SERIAL PRIMARY KEY,
        layout ebi.type_layout NOT NULL,
        source VARCHAR(32),
        selection VARCHAR(32),
        strategy VARCHAR(32)
);

CREATE TABLE IF NOT EXISTS collector (
        id SERIAL PRIMARY KEY,
        broker_name VARCHAR(64),
        collected_by TEXT,
        center_name TEXT
);

CREATE TABLE IF NOT EXISTS metaextension (
        ena_run VARCHAR(16) REFERENCES ebi.metadata(ena_run),
        description TEXT NULL,
        fastq_ftp TEXT,
        isolate VARCHAR(128) NULL,
        sample_capture_status ebi.type_status NULL,
        strain VARCHAR(128),
        checklist VARCHAR(16),
        base_count DOUBLE PRECISION,
        library_name VARCHAR(128),
        library_id INT REFERENCES ebi.library(id) NULL,
        first_created DATE,
        first_public DATE NULL,
        collector_id INT REFERENCES ebi.collector(id),
        country_raw TEXT
);


