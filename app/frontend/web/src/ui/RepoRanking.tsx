import { trendingRanking } from "../api/tables";

import RankingList from "./RankingList";

export default function RepoRanking() {
    return (
        <RankingList
            table={trendingRanking}
            rows={row => (
                <tr key={row.repo_id}>
                    <td className="pt-1 px-3">{row.rank}</td>
                    <td className="pt-1 px-3 text-right">{row.repo_id}</td>
                    <td className="pt-1 px-3">{row.trending_score}</td>
                </tr>
            )}
        />
    );
}
