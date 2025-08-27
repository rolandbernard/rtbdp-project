-- This file contains the schema for the PostgreSQL database. The database will
-- be filled from the Flink processor.

CREATE TABLE counts_live (
    ts_start TIMESTAMP,
    ts_end TIMESTAMP,
    ts_write TIMESTAMP,
    kind VARCHAR,
    window_size VARCHAR,
    num_events INT,
    PRIMARY KEY (kind, window_size)
);

CREATE TABLE counts_history (
    ts_start TIMESTAMP,
    ts_end TIMESTAMP,
    ts_write TIMESTAMP,
    kind VARCHAR,
    num_events INT,
    PRIMARY KEY (kind, ts_start, ts_end)
);
