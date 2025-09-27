import { Table, UpdateTable } from "./client";

export const event_kinds = {
    all: "All",
    push: "Push",
    watch: "Watch",
    create_repo: "Create Repository",
    create_branch: "Create Branch",
    create_tag: "Create Tag",
    fork: "Fork",
    wiki: "Edit Wiki",
    issue_open: "Open Issue",
    issue_close: "Close Issue",
    pull_open: "Open Pull Request",
    pull_close: "Close Pull Request",
    commit_comment: "Comment on Commit",
    issue_comment: "Comment on Issue",
    pull_comment: "Comment on Pull Request",
    other: "Other",
};
type EventKind = keyof typeof event_kinds;

export const window_sizes = {
    "5m": 60 * 5,
    "1h": 60 * 60,
    "6h": 60 * 60 * 6,
    "24h": 60 * 60 * 24,
};
type WindowSize = keyof typeof window_sizes;

export const events = new Table<{
    created_at: string;
    id: number;
    kind: EventKind;
    repo_id: number;
    user_id: number;
    details: string;
}>("events", ["created_at", "id"]);

export const users = new UpdateTable<
    {
        id: number;
    },
    {
        username?: string;
        avatar_url?: string;
        html_url?: string;
        user_type?: string;
    }
>("users", ["id"]);

export const repos = new UpdateTable<
    {
        id: number;
    },
    {
        reponame?: string;
        fullname?: string;
        owner_id?: number;
        html_url?: string;
        homepage?: string;
        descr?: string;
        topics?: string;
        lang?: string;
        license?: string;
        fork_count?: number;
        issue_count?: number;
        star_count?: number;
    }
>("repos", ["id"]);

export const countsLive = new Table<{
    window_size: WindowSize;
    kind: EventKind;
    num_events: number;
}>("counts_live", ["window_size", "kind"]);

export const countsRanking = new Table<{
    window_size: WindowSize;
    row_number: number;
    kind?: EventKind;
    rank: number;
}>("counts_ranking", ["window_size", "row_number"]);

export const countsHistory = new Table<{
    kind: EventKind;
    ts_start: string;
    ts_end: string;
    num_events: number;
}>("counts_live", ["kind", "ts_start", "ts_end"]);
