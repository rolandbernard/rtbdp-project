import { NormalTable, UpdateTable } from "./table";
import { RankingTable } from "./ranking";

export const EVENT_KINDS = {
    all: "All",
    push: "Push Commit",
    watch: "Star Repository",
    create_repo: "Create Repository",
    create_branch: "Create Branch",
    delete_branch: "Delete Branch",
    fork: "Fork Repository",
    wiki: "Edit Wiki",
    issue_open: "Open Issue",
    issue_close: "Close Issue",
    pull_open: "Open Pull Request",
    pull_close: "Close Pull Request",
    commit_comment: "Comment Commit",
    issue_comment: "Comment Issue",
    pull_comment: "Comment Pull Request",
    other: "Other",
};
export type EventKind = keyof typeof EVENT_KINDS;

export const WINDOW_SIZES = {
    "5m": 60 * 5,
    "1h": 60 * 60,
    "6h": 60 * 60 * 6,
    "24h": 60 * 60 * 24,
};
export type WindowSize = keyof typeof WINDOW_SIZES;

export const events = new NormalTable<{
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

export const countsLive = new NormalTable<{
    window_size: WindowSize;
    kind: EventKind;
    num_events: number;
}>("counts_live", ["window_size", "kind"]);

export const countsRanking = new RankingTable<{
    window_size: WindowSize;
    kind: EventKind;
    num_events: number;
}>("counts_ranking", ["window_size", "kind"]).rankingsBy(["window_size"]);

export const countsHistory = new NormalTable<{
    kind: EventKind;
    ts_start: string;
    ts_end: string;
    num_events: number;
}>("counts_history", ["kind", "ts_start", "ts_end"]);

export const reposLive = new NormalTable<{
    window_size: WindowSize;
    repo_id: number;
    num_events: number;
}>("repos_live", ["window_size", "repo_id"]);

export const reposRanking = new RankingTable<{
    window_size: WindowSize;
    repo_id: number;
    num_events: number;
}>("repos_ranking", ["window_size", "repo_id"]).rankingsBy(["window_size"]);

export const reposHistory = new NormalTable<{
    repo_id: number;
    ts_start: string;
    ts_end: string;
    num_events: number;
}>("repos_history", ["repo_id", "ts_start", "ts_end"]);

export const usersLive = new NormalTable<{
    window_size: WindowSize;
    user_id: number;
    num_events: number;
}>("users_live", ["window_size", "user_id"]);

export const usersRanking = new RankingTable<{
    window_size: WindowSize;
    user_id: number;
    num_events: number;
}>("users_ranking", ["window_size", "user_id"]).rankingsBy(["window_size"]);

export const usersHistory = new NormalTable<{
    user_id: number;
    ts_start: string;
    ts_end: string;
    num_events: number;
}>("users_history", ["user_id", "ts_start", "ts_end"]);

export const starsLive = new NormalTable<{
    window_size: WindowSize;
    repo_id: number;
    num_stars: number;
}>("stars_live", ["window_size", "repo_id"]);

export const starsRanking = new RankingTable<{
    window_size: WindowSize;
    repo_id: number;
    num_stars: number;
}>("stars_ranking", ["window_size", "repo_id"]).rankingsBy(["window_size"]);

export const starsHistory = new NormalTable<{
    repo_id: number;
    ts_start: string;
    ts_end: string;
    num_stars: number;
}>("stars_history", ["repo_id", "ts_start", "ts_end"]);

export const trendingLive = new NormalTable<{
    repo_id: number;
    trending_score: number;
}>("trending_live", ["repo_id"]);

export const trendingRanking = new RankingTable<{
    repo_id: number;
    trending_score: number;
}>("trending_ranking", ["repo_id"]).rankingsBy([]);
