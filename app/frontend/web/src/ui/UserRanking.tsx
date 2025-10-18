import { useState } from "react";

import { usersRanking } from "../api/tables";

import RankingList from "./RankingList";

export default function UserRanking() {
    const [start, setStart] = useState(0);
    return (
        <>
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
                        <th className="pt-1">UserId</th>
                        <th className="pt-1">#Events</th>
                    </tr>
                </thead>
                <tbody>
                    <RankingList
                        table={usersRanking.where("window_size", ["5m"])}
                        from={start}
                        to={start + 10}
                        rows={row => (
                            <tr key={row.user_id}>
                                <td className="pt-1 px-3">{row.rank}</td>
                                <td className="pt-1 px-3 text-right">
                                    {row.user_id}
                                </td>
                                <td className="pt-1 px-3">{row.num_events}</td>
                            </tr>
                        )}
                    ></RankingList>
                </tbody>
            </table>
        </>
    );
}
