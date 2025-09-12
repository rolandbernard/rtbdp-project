-- This file contains the schema for the PostgreSQL database. The database will

CREATE TYPE event_kind AS ENUM (
    'all', 'push', 'watch', 'create_repo', 'create_branch', 'create_tag',
    'fork', 'wiki', 'issue_open', 'issue_close', 'pull_open', 'pull_close',
    'commit_comment', 'issue_comment', 'pull_comment', 'other'
);

CREATE TYPE window_size AS ENUM (
    '5m', '1h', '6h', '24h'
);

CREATE TABLE counts_live (
    kind event_kind NOT NULL,
    window_size window_size NOT NULL,
    num_events BIGINT NOT NULL,
    seq_num BIGINT NOT NULL,
    PRIMARY KEY (window_size, kind)
);

CREATE TABLE counts_ranking (
    kind event_kind,
    window_size window_size NOT NULL,
    row_number INT NOT NULL,
    rank INT NOT NULL,
    seq_num BIGINT NOT NULL,
    PRIMARY KEY (window_size, row_number)
);

CREATE TABLE counts_history (
    ts_start TIMESTAMP NOT NULL,
    ts_end TIMESTAMP NOT NULL,
    kind event_kind NOT NULL,
    num_events BIGINT NOT NULL,
    seq_num BIGINT NOT NULL,
    PRIMARY KEY (kind, ts_start, ts_end)
);
