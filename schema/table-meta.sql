CREATE TABLE IF NOT EXISTS meta (
	ena_run varchar(20), -- ENA ID
	collection_date date,
	clean_country text,
	clean_host text,
	accession text,
	sample_accession text,
	experiment_accession text,
	study_accession text,
	description text,
	country text,
	first_created date,
	first_public date,
	host text,
	host_sex text,
	host_tax_id int,
	host_body_site text,
	bio_material text,
	culture_collection text,
	instrument_model text,
	instrument_platform text,
	library_layout text,
	library_name text,
	library_selection text,
	library_source text,
	library_strategy text,
	sequencing_method text,
	isolate text,
	strain text,
	base_count double precision,
	collected_by text,
	broker_name text,
	center_name text,
	sample_capture_status text,
	fastq_ftp text,
	checklist text,
	date_week int
);
