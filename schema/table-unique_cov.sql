CREATE TABLE IF NOT EXISTS unique_cov (
	insertion_ts timestamp,
	ena_run_id varchar(16),
	snapshot_date date,
	integrity int -- 0: okay, 1: empty, 2: junk
);
