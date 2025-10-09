-- This file contains the schema for the PostgreSQL database. The database will
-- be filled from the Flink processor and enable the frontend te obtain the latest
-- state without replaying all of the messages from Kafka.

-- We use TimescaleDB for easier handling of partitioned tables and automatic
-- retention period.
CREATE EXTENSION timescaledb;
-- Use the trigram index to speed up `LIKE` queries. Will be used on username and
-- reponame.
CREATE EXTENSION pg_trgm;

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
    username TEXT,
    username_seq_num BIGINT NOT NULL,
    avatar_url TEXT,
    avatar_url_seq_num BIGINT NOT NULL,
    html_url TEXT,
    html_url_seq_num BIGINT NOT NULL,
    user_type TEXT,
    user_type_seq_num BIGINT NOT NULL,
    seq_num BIGINT NOT NULL,
    PRIMARY KEY (id)
);

CREATE INDEX ON users USING GIN (LOWER(username) gin_trgm_ops);

CREATE TABLE repos (
    id BIGINT NOT NULL,
    reponame TEXT,
    reponame_seq_num BIGINT NOT NULL,
    fullname TEXT,
    fullname_seq_num BIGINT NOT NULL,
    owner_id BIGINT,
    owner_id_seq_num BIGINT NOT NULL,
    html_url TEXT,
    html_url_seq_num BIGINT NOT NULL,
    homepage TEXT,
    homepage_seq_num BIGINT NOT NULL,
    descr TEXT,
    descr_seq_num BIGINT NOT NULL,
    topics TEXT,
    topics_seq_num BIGINT NOT NULL,
    lang TEXT,
    lang_seq_num BIGINT NOT NULL,
    license TEXT,
    license_seq_num BIGINT NOT NULL,
    is_fork BOOLEAN,
    is_fork_seq_num BIGINT NOT NULL,
    is_archived BOOLEAN,
    is_archived_seq_num BIGINT NOT NULL,
    fork_count BIGINT,
    fork_count_seq_num BIGINT NOT NULL,
    issue_count BIGINT,
    issue_count_seq_num BIGINT NOT NULL,
    star_count BIGINT,
    star_count_seq_num BIGINT NOT NULL,
    seq_num BIGINT NOT NULL,
    PRIMARY KEY (id)
);

CREATE INDEX ON repos USING GIN (LOWER(reponame) gin_trgm_ops);
CREATE INDEX ON repos USING GIN (LOWER(fullname) gin_trgm_ops);

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

-- This index helps slightly with the performance of the ranking view.
CREATE INDEX ON counts_live(window_size, num_events DESC, kind ASC);

-- This is a virtual view that also contains row numbers and ranks. Unfortunately,
-- PostgreSQL does not currently have any index structures to make this more efficient.
-- Since it is only used for initializing the view in the client, this is fine for now.
CREATE VIEW counts_ranking AS
SELECT window_size, kind, num_events,
        MAX(seq_num) OVER (PARTITION BY window_size) as seq_num,
        ROW_NUMBER() OVER (
            PARTITION BY window_size ORDER BY num_events DESC, kind ASC
        ) - 1 AS row_number,
        RANK() OVER (
            PARTITION BY window_size ORDER BY num_events DESC
        ) - 1 AS rank
    FROM counts_live
    WHERE num_events > 0;

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

CREATE TABLE users_live (
    window_size window_size NOT NULL,
    user_id BIGINT NOT NULL,
    num_events BIGINT NOT NULL,
    seq_num BIGINT NOT NULL,
    PRIMARY KEY (window_size, user_id)
);

-- This index helps slightly with the performance of the ranking view.
CREATE INDEX ON users_live(window_size, num_events DESC, user_id ASC);

-- This is a virtual view that also contains row numbers and ranks.
CREATE VIEW users_ranking AS
SELECT window_size, user_id, num_events,
        MAX(seq_num) OVER (PARTITION BY window_size) as seq_num,
        ROW_NUMBER() OVER (
            PARTITION BY window_size ORDER BY num_events DESC, user_id ASC
        ) - 1 AS row_number,
        RANK() OVER (
            PARTITION BY window_size ORDER BY num_events DESC
        ) - 1 AS rank
    FROM users_live
    WHERE num_events > 0;

CREATE TABLE users_history (
    user_id BIGINT NOT NULL,
    ts_start TIMESTAMP NOT NULL,
    ts_end TIMESTAMP NOT NULL,
    num_events BIGINT NOT NULL,
    seq_num BIGINT NOT NULL,
    PRIMARY KEY (user_id, ts_start, ts_end)
);

-- We partition by hour because this is relatively high volume.
SELECT create_hypertable('users_history', by_range('ts_start', INTERVAL '1 hour'));

-- ===========================
-- Per-repository event counts
-- ===========================

CREATE TABLE repos_live (
    window_size window_size NOT NULL,
    repo_id BIGINT NOT NULL,
    num_events BIGINT NOT NULL,
    seq_num BIGINT NOT NULL,
    PRIMARY KEY (window_size, repo_id)
);

-- This index helps slightly with the performance of the ranking view.
CREATE INDEX ON repos_live(window_size, num_events DESC, repo_id ASC);

-- This is a virtual view that also contains row numbers and ranks.
CREATE VIEW repos_ranking AS
SELECT window_size, repo_id, num_events,
        MAX(seq_num) OVER (PARTITION BY window_size) as seq_num,
        ROW_NUMBER() OVER (
            PARTITION BY window_size ORDER BY num_events DESC, repo_id ASC
        ) - 1 AS row_number,
        RANK() OVER (
            PARTITION BY window_size ORDER BY num_events DESC
        ) - 1 AS rank
    FROM repos_live
    WHERE num_events > 0;

CREATE TABLE repos_history (
    repo_id BIGINT NOT NULL,
    ts_start TIMESTAMP NOT NULL,
    ts_end TIMESTAMP NOT NULL,
    num_events BIGINT NOT NULL,
    seq_num BIGINT NOT NULL,
    PRIMARY KEY (repo_id, ts_start, ts_end)
);

-- We partition by hour because this is relatively high volume.
SELECT create_hypertable('repos_history', by_range('ts_start', INTERVAL '1 hour'));

-- =============================
-- Trending repository detection
-- =============================

-- TODO

