-- This file contains the schema for the PostgreSQL database. The database will
-- be filled from the Flink processor and enable the frontend te obtain the latest
-- state without replaying all of the messages from Kafka.

-- We use TimescaleDB for easier handling of partitioned tables and automatic
-- retention period.
CREATE EXTENSION timescaledb;

CREATE TYPE event_kind AS ENUM (
    'all', 'push', 'watch', 'create_repo', 'create_branch', 'create_tag',
    'fork', 'wiki', 'issue_open', 'issue_close', 'pull_open', 'pull_close',
    'commit_comment', 'issue_comment', 'pull_comment', 'other'
);

CREATE TYPE window_size AS ENUM (
    '5m', '1h', '6h', '24h'
);

-- ===================
-- Table of all events
-- ===================

-- These are not the raw events, but the processed ones that contain only enough
-- information to display in the stream on the frontend.
CREATE TABLE events (
    created_at TIMESTAMP NOT NULL,
    id BIGINT NOT NULL,
    kind event_kind NOT NULL,
    repo_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    details TEXT NOT NULL,
    seq_num BIGINT NOT NULL,
    PRIMARY KEY (created_at, id)
);

CREATE INDEX ON events(kind);
CREATE INDEX ON events(repo_id);
CREATE INDEX ON events(user_id);

-- This is a high volume table, so we setup a partition based on the creation
-- timestamp and then a retention period of one month.
-- ~100 events/second * 60 seconds/minute * 10 minutes/partition = 60000 /partition
SELECT create_hypertable('events', by_range('created_at', INTERVAL '10 minutes'));
-- We only keep one month worth of events (~250M events), due to the volume of data.
SELECT add_retention_policy('events', INTERVAL '30 days');

-- ======================================
-- Aggregated user and repository details
-- ======================================

CREATE TABLE users (
    id BIGINT NOT NULL,
    username TEXT NOT NULL,
    avatar_url TEXT NOT NULL,
    html_url TEXT,
    user_type TEXT,
    seq_num BIGINT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE repos (
    id BIGINT NOT NULL,
    reponame TEXT NOT NULL,
    fullname TEXT NOT NULL,
    owner_id BIGINT,
    html_url TEXT,
    homepage TEXT,
    descr TEXT,
    topics TEXT,
    lang TEXT,
    license TEXT,
    is_fork BOOLEAN,
    is_archived BOOLEAN,
    fork_count BIGINT,
    issue_count BIGINT,
    star_count BIGINT,
    seq_num BIGINT NOT NULL,
    PRIMARY KEY (id)
);

-- =====================
-- Per-kind event counts
-- =====================

CREATE TABLE counts_live (
    window_size window_size NOT NULL,
    kind event_kind NOT NULL,
    num_events BIGINT NOT NULL,
    seq_num BIGINT NOT NULL,
    PRIMARY KEY (window_size, kind)
);

CREATE TABLE counts_ranking (
    window_size window_size NOT NULL,
    row_number INT NOT NULL,
    kind event_kind,
    rank INT NOT NULL,
    seq_num BIGINT NOT NULL,
    PRIMARY KEY (window_size, row_number)
);

CREATE TABLE counts_history (
    kind event_kind NOT NULL,
    ts_start TIMESTAMP NOT NULL,
    ts_end TIMESTAMP NOT NULL,
    num_events BIGINT NOT NULL,
    seq_num BIGINT NOT NULL,
    PRIMARY KEY (kind, ts_start, ts_end)
);

-- We partition by week. This table does not contain such a large amount of data
-- so we can use a larger time period.
-- ~20 kinds * 0.2 events/kind/minute * 60 minutes/hour * 24 hours/day * 7 days/partition = 40320 /partition
SELECT create_hypertable('counts_history', by_range('ts_start', INTERVAL '7 day'));

-- =====================
-- Per-user event counts
-- =====================

-- TODO

-- ===========================
-- Per-repository event counts
-- ===========================

-- TODO

-- =============================
-- Trending repository detection
-- =============================

-- TODO

