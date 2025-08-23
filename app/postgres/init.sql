CREATE TABLE counts_live (
    ts_start TIMESTAMP(3) WITH TIME ZONE,
    ts_end TIMESTAMP (3) WITH TIME ZONE,
    kind VARCHAR,
    window_size VARCHAR,
    num_events INT,
    PRIMARY KEY (window_size, kind)
);

CREATE TABLE counts_history (
    ts_start TIMESTAMP(3) WITH TIME ZONE,
    ts_end TIMESTAMP (3) WITH TIME ZONE,
    kind VARCHAR,
    window_size VARCHAR,
    num_events INT,
    PRIMARY KEY (ts_start, ts_end, window_size, kind)
);

