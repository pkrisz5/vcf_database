CREATE TABLE IF NOT EXISTS operation (
	event_ts timestamp,
	last_stage int,
	last_exit_code int,
	stage int,
        exit_code int,
	extra_info text -- json encoded information
);
