import { useMemo } from "react";
import { Link, useViewTransitionState } from "react-router";

import {
    repos,
    reposHistory,
    reposHistoryFine,
    reposRanking,
    starsHistory,
    starsHistoryFine,
    starsRanking,
    trendingRanking,
    type WindowSize,
} from "../api/tables";
import type { RankingTable } from "../api/ranking";
import { useTable } from "../api/hooks";
import type { NormalTable } from "../api/table";
import { useParam } from "../hooks";

import RankingList, { RankingRow } from "./RankingList";
import Selector from "./Selector";
import Counter from "./Counter";
import HistorySpark from "./HistorySpark";

interface RepoNameProps {
    repoId: number;
    transitionName: boolean;
}

function RepoName(props: RepoNameProps) {
    const repo = useTable(repos.where("id", [props.repoId]))[0];
    return (
        <div
            className="flex-2 min-w-0 whitespace-nowrap overflow-hidden overflow-ellipsis contain-content select-text
                    text-primary font-semibold text-left dark:hover:text-primary/90 hover:text-primary/75"
            style={{ direction: "rtl" }}
        >
            <Link
                viewTransition
                to={"/repo/" + props.repoId}
                className={
                    repo?.fullname ?? repo?.reponame ? "" : "text-primary/50"
                }
                title={repo?.fullname ?? repo?.reponame}
                state={{
                    from: "ranking",
                    name: repo?.fullname ?? repo?.reponame,
                }}
                style={{
                    viewTransitionName: props.transitionName
                        ? "nameranking"
                        : "none",
                }}
            >
                {repo?.fullname ?? repo?.reponame}
            </Link>
        </div>
    );
}

type TableType = NormalTable<{
    repo_id: number;
    ts_start: string;
    num_events?: number;
    num_stars?: number;
}>;

interface RepoRowProps {
    repoId: number;
    value: number;
    kind: "trending" | "stars" | "activity";
    windowSize: WindowSize;
}

function RepoRankRow(props: RepoRowProps) {
    const inTransition = useViewTransitionState("/repo/" + props.repoId);
    const history = useMemo(
        () =>
            (
                (props.kind === "activity"
                    ? reposHistory
                    : starsHistory) as TableType
            ).where("repo_id", [props.repoId]),
        [props.repoId, props.kind]
    );
    const historyFine = useMemo(
        () =>
            (
                (props.kind === "activity"
                    ? reposHistoryFine
                    : starsHistoryFine) as TableType
            ).where("repo_id", [props.repoId]),
        [props.repoId, props.kind]
    );
    return (
        <div
            className="flex-1 min-h-0 min-w-0 flex flex-row items-center pl-4 contain-strict"
            style={{
                viewTransitionName: inTransition ? "pageranking" : "none",
            }}
        >
            <RepoName repoId={props.repoId} transitionName={inTransition} />
            <div className="flex-3 min-w-0 min-h-0 h-full flex flex-row items-center">
                <div
                    className="w-18 pr-1 flex flex-col"
                    style={{
                        viewTransitionName: inTransition ? "ranking" : "none",
                    }}
                >
                    <Counter
                        value={props.value}
                        className="text-lg"
                        maxDigits={5}
                    />
                </div>
                <div
                    className="w-full h-full"
                    style={{
                        viewTransitionName: inTransition ? "chart" : "none",
                    }}
                >
                    <HistorySpark
                        table={history}
                        tableFine={historyFine}
                        windowSize={props.windowSize}
                        chartColor={
                            props.kind === "stars"
                                ? "#78b120"
                                : props.kind === "trending"
                                ? "#d12eab"
                                : undefined
                        }
                    />
                </div>
            </div>
        </div>
    );
}

export default function RepoRanking() {
    const [kind, setKind] = useParam<"trending" | "stars" | "activity">(
        "rrkind",
        "trending"
    );
    const [windowSize, setWindowSize] = useParam<WindowSize>("rrwin", "24h");
    const table = (
        kind === "trending"
            ? trendingRanking
            : kind === "stars"
            ? starsRanking.where("window_size", [windowSize])
            : reposRanking.where("window_size", [windowSize])
    ) as RankingTable<{
        repo_id: number;
        num_events?: number;
        num_stars?: number;
        trending_score?: number;
    }>;
    return (
        <div className="flex flex-col h-full flex-1 min-h-0 mx-1 mt-2">
            <div className="flex flex-row justify-end items-end gap-2">
                <div className="w-1/2 max-w-32">
                    <Selector
                        options={{
                            trending: "Trending",
                        }}
                        name="repos-rank-kind"
                        group="trending"
                        className="w-full text-sm"
                        value={kind === "trending" ? "trending" : undefined}
                        onChange={_w => {
                            setKind("trending");
                            setWindowSize("24h");
                        }}
                    />
                </div>
                <div className="w-full max-w-64">
                    <div className="text-xs">Stars</div>
                    <Selector
                        options={{
                            "5m": "5m",
                            "1h": "1h",
                            "6h": "6h",
                            "24h": "24h",
                        }}
                        name="repos-rank-kind"
                        group="stars"
                        className="w-full text-sm"
                        value={kind === "stars" ? windowSize : undefined}
                        onChange={w => {
                            setKind("stars");
                            setWindowSize(w);
                        }}
                    />
                </div>
                <div className="w-full max-w-64">
                    <div className="text-xs">Activity</div>
                    <Selector
                        options={{
                            "5m": "5m",
                            "1h": "1h",
                            "6h": "6h",
                            "24h": "24h",
                        }}
                        name="repos-rank-kind"
                        group="activity"
                        className="w-full text-sm"
                        value={kind === "activity" ? windowSize : undefined}
                        onChange={w => {
                            setKind("activity");
                            setWindowSize(w);
                        }}
                    />
                </div>
            </div>
            <RankingList
                name={"rr" + kind + windowSize}
                table={table}
                rows={row => (
                    <RankingRow key={row.repo_id} rank={row.rank}>
                        <RepoRankRow
                            repoId={row.repo_id}
                            value={
                                row.num_events ??
                                row.num_stars ??
                                row.trending_score!
                            }
                            kind={kind as "trending" | "stars" | "activity"}
                            windowSize={windowSize}
                        />
                    </RankingRow>
                )}
            />
        </div>
    );
}
