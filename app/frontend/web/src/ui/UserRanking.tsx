import { useState } from "react";

import { usersRanking, type WindowSize } from "../api/tables";

import RankingList from "./RankingList";
import Selector from "./Selector";

export default function UserRanking() {
    const [windowSize, setKind] = useState<WindowSize>("24h");
    const table = usersRanking.where("window_size", [windowSize]);
    return (
        <div className="flex flex-col h-full flex-1 min-h-0 mx-1 mt-2">
            <div className="flex flex-row justify-end items-end gap-2">
                <div className="w-1/2 max-w-32"></div>
                <div className="w-full max-w-64"></div>
                <div className="w-full max-w-64">
                    <div className="text-xs">Activity</div>
                    <Selector
                        options={{
                            "5m": "5m",
                            "1h": "1h",
                            "6h": "6h",
                            "24h": "24h",
                        }}
                        name="users-rank-kind"
                        group="activity"
                        className="w-full text-sm"
                        value={windowSize}
                        onChange={w => setKind(w)}
                    />
                </div>
            </div>
            <RankingList
                table={table}
                rows={row => (
                    <div key={row.user_id} className="flex-1 flex flex-row">
                        <span className="pt-1 px-3">{row.rank}</span>
                        <span className="pt-1 px-3 text-right">
                            {row.user_id}
                        </span>
                        <span className="pt-1 px-3">{row.num_events}</span>
                    </div>
                )}
            />
        </div>
    );
}
