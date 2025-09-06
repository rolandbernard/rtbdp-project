-- This file contains the schema for the PostgreSQL database. The database will
-- be filled from the Flink processor.

CREATE TABLE counts_live (
    kind VARCHAR NOT NULL,
    window_size VARCHAR NOT NULL,
    num_events BIGINT NOT NULL,
    seq_num BIGINT NOT NULL,
    PRIMARY KEY (window_size, kind)
);

CREATE TABLE counts_ranking (
    kind VARCHAR,
    window_size VARCHAR NOT NULL,
    row_number INT NOT NULL,
    rank INT NOT NULL,
    seq_num BIGINT NOT NULL,
    PRIMARY KEY (window_size, row_number)
);

CREATE TABLE counts_history (
    ts_start TIMESTAMP NOT NULL,
    ts_end TIMESTAMP NOT NULL,
    kind VARCHAR NOT NULL,
    num_events BIGINT NOT NULL,
    seq_num BIGINT NOT NULL,
    PRIMARY KEY (kind, ts_start, ts_end)
);
