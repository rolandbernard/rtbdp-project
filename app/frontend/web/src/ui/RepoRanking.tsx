import { useState } from "react";

import {
    reposRanking,
    starsRanking,
    trendingRanking,
    type WindowSize,
} from "../api/tables";
import type { RankingTable } from "../api/ranking";

import RankingList from "./RankingList";
import Selector from "./Selector";

export default function RepoRanking() {
    const [[kind, windowSize], setKind] = useState<
        ["trending" | "stars" | "activity", WindowSize]
    >(["trending", "5m"]);
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
                        onChange={_w => setKind(["trending", "5m"])}
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
                        onChange={w => setKind(["stars", w])}
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
                        onChange={w => setKind(["activity", w])}
                    />
                </div>
            </div>
            <RankingList
                table={table}
                rows={row => (
                    <div key={row.repo_id} className="flex-1 flex flex-row">
                        <span className="pt-1 px-3">{row.rank}</span>
                        <span className="pt-1 px-3 text-right">{row.repo_id}</span>
                        <span className="pt-1 px-3">
                            {row.trending_score ??
                                row.num_events ??
                                row.num_stars}
                        </span>
                    </div>
                )}
            />
        </div>
    );
}
