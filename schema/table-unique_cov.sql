CREATE TABLE IF NOT EXISTS unique_cov (
	insertion_ts timestamp,
	ena_run varchar(16),
	snapshot varchar(32),
	integrity int -- 0: okay, 1: empty, 2: junk
);
