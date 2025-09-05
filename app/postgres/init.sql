-- This file contains the schema for the PostgreSQL database. The database will
-- be filled from the Flink processor.

CREATE TABLE counts_ranking (
    ts_write TIMESTAMP NOT NULL,
    kind VARCHAR NOT NULL,
    window_size VARCHAR NOT NULL,
    row_number INT NOT NULL,
    rank INT NOT NULL,
    num_events INT NOT NULL,
    PRIMARY KEY (kind, window_size)
);

CREATE INDEX ON counts_ranking (row_number);

CREATE TABLE counts_history (
    ts_write TIMESTAMP NOT NULL,
    ts_start TIMESTAMP NOT NULL,
    ts_end TIMESTAMP NOT NULL,
    kind VARCHAR NOT NULL,
    num_events INT NOT NULL,
    PRIMARY KEY (kind, ts_start, ts_end)
);
