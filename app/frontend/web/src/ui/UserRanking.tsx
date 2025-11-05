import { useMemo } from "react";
import { Link, useSearchParams } from "react-router";

import {
    users,
    usersHistory,
    usersHistoryFine,
    usersRanking,
    type WindowSize,
} from "../api/tables";
import { useTable } from "../api/hooks";

import RankingList, { RankingRow } from "./RankingList";
import Selector from "./Selector";
import HistorySpark from "./HistorySpark";
import Counter from "./Counter";

interface UserRowProps {
    userId: number;
    value: number;
    windowSize: WindowSize;
}

function UserRankRow(props: UserRowProps) {
    const user = useTable(users.where("id", [props.userId]))[0];
    const history = useMemo(
        () => usersHistory.where("user_id", [props.userId]),
        [props.userId]
    );
    const historyFine = useMemo(
        () => usersHistoryFine.where("user_id", [props.userId]),
        [props.userId]
    );
    return (
        <div className="flex-1 min-w- min-w-0 flex flex-row items-center pl-4">
            <div className="flex-2 min-w-0 whitespace-nowrap overflow-hidden overflow-ellipsis text-primary font-semibold">
                <Link
                    to={"/user/" + props.userId}
                    className={user?.username ? "" : "text-primary/50"}
                    title={user?.username}
                >
                    <span className="font-bold">@</span>
                    {user?.username}
                </Link>
            </div>
            <div className="flex-3 min-w-0 min-h-0 h-full flex flex-row items-center">
                <div className="w-18 pr-1 flex flex-col">
                    <Counter
                        value={props.value}
                        className="text-lg"
                        maxDigits={7}
                    />
                </div>
                <div className="w-full h-full">
                    <HistorySpark
                        table={history}
                        tableFine={historyFine}
                        windowSize={props.windowSize}
                    />
                </div>
            </div>
        </div>
    );
}

export default function UserRanking() {
    const [searchParams, setSearchParams] = useSearchParams();
    const windowSize = (searchParams.get("urWin") ?? "24h") as WindowSize;
    const table = usersRanking.where("window_size", [windowSize]);
    return (
        <div className="flex flex-col h-full flex-1 min-h-0 mx-1 mt-2">
            <div className="flex flex-row justify-end xl:justify-start items-end gap-2">
                <div className="w-1/2 max-w-32 xl:hidden"></div>
                <div className="w-full max-w-64 xl:hidden"></div>
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
                        onChange={w =>
                            setSearchParams(p => {
                                p.set("urWin", w);
                                return p;
                            })
                        }
                    />
                </div>
                <div className="w-1/2 max-w-32 not-xl:hidden"></div>
                <div className="w-full max-w-64 not-xl:hidden"></div>
            </div>
            <RankingList
                table={table}
                rows={row => (
                    <RankingRow key={row.user_id} rank={row.rank}>
                        <UserRankRow
                            userId={row.user_id}
                            value={row.num_events}
                            windowSize={windowSize}
                        />
                    </RankingRow>
                )}
            />
        </div>
    );
}
