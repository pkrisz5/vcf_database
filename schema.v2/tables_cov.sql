CREATE TABLE IF NOT EXISTS unique_cov (
        insertion_ts TIMESTAMP,
        ena_run VARCHAR(16) NOT NULL,
        snapshot VARCHAR(32) NOT NULL,
        integrity type_integrity NOT NULL
);

CREATE TABLE IF NOT EXISTS cov (
    ena_run     varchar(16),       -- ENA ID
    pos         int,               -- Position in the sequence
    coverage    int                -- Coverage in the given position
);


