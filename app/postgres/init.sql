CREATE TABLE counts_live (
    ts_start TIMESTAMP,
    ts_end TIMESTAMP,
    ts_write TIMESTAMP,
    kind VARCHAR,
    window_size VARCHAR,
    num_events INT,
    PRIMARY KEY (window_size, kind)
);

CREATE TABLE counts_history (
    ts_start TIMESTAMP,
    ts_end TIMESTAMP,
    ts_write TIMESTAMP,
    kind VARCHAR,
    window_size VARCHAR,
    num_events INT,
    PRIMARY KEY (ts_start, ts_end, window_size, kind)
);
