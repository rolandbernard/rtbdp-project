import RankingList from "./RankingList";
import { reposRanking } from "../api/tables";

export default function RepoRanking() {
    return (
        <table className="mx-3">
            <thead>
                <tr>
                    <th className="pt-1">Rank</th>
                    <th className="pt-1">RepoId</th>
                    <th className="pt-1">#Events</th>
                </tr>
            </thead>
            <tbody>
                <RankingList
                    table={reposRanking.where("window_size", ["5m"])}
                    from={50}
                    to={60}
                    rows={row => (
                        <tr key={row.repo_id}>
                            <td className="pt-1 px-3">{row.rank}</td>
                            <td className="pt-1 px-3 text-right">{row.repo_id}</td>
                            <td className="pt-1 px-3">{row.num_events}</td>
                        </tr>
                    )}
                ></RankingList>
            </tbody>
        </table>
    );
}
