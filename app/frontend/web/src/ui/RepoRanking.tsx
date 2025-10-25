import { useState } from "react";

import { trendingRanking } from "../api/tables";

import RankingList from "./RankingList";

export default function RepoRanking() {
    const [start, setStart] = useState(0);
    return (
        <div className="md:w-full min-w-0">
            <button
                className="ms-1 p-2 bg-gray-600 hover:cursor-pointer"
                onClick={_e => setStart(start - 10)}
            >
                &lt;
            </button>
            <button
                className="ms-1 p-2 bg-gray-600 hover:cursor-pointer"
                onClick={_e => setStart(start + 10)}
            >
                &gt;
            </button>
            <table className="mx-3">
                <thead>
                    <tr>
                        <th className="pt-1">Rank</th>
                        <th className="pt-1">RepoId</th>
                        <th className="pt-1">Score</th>
                    </tr>
                </thead>
                <tbody>
                    <RankingList
                        table={trendingRanking}
                        from={start}
                        to={start + 10}
                        rows={row => (
                            <tr key={row.repo_id}>
                                <td className="pt-1 px-3">{row.rank}</td>
                                <td className="pt-1 px-3 text-right">
                                    {row.repo_id}
                                </td>
                                <td className="pt-1 px-3">
                                    {row.trending_score}
                                </td>
                            </tr>
                        )}
                    ></RankingList>
                </tbody>
            </table>
        </div>
    );
}
