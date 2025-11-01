import { usersRanking } from "../api/tables";

import RankingList from "./RankingList";

export default function UserRanking() {
    return (
        <RankingList
            table={usersRanking.where("window_size", ["5m"])}
            rows={row => (
                <tr key={row.user_id}>
                    <td className="pt-1 px-3">{row.rank}</td>
                    <td className="pt-1 px-3 text-right">{row.user_id}</td>
                    <td className="pt-1 px-3">{row.num_events}</td>
                </tr>
            )}
        />
    );
}
