import { Link, useParams } from "react-router";

import { useLoadingTable, useTable } from "../api/hooks";
import {
    users,
    usersHistory,
    usersRanking,
    type WindowSize,
} from "../api/tables";
import type { RankingRow } from "../api/ranking";

import HistoryLong from "../ui/HistoryLong";
import Counter, { Letters } from "../ui/Counter";

interface DetailsProps {
    username?: string;
    avatar_url?: string;
    html_url?: string;
    user_type?: string;
}

function UserDetails(props: DetailsProps) {
    return (
        <div className="flex flex-row items-center">
            {props.avatar_url ? (
                <img
                    src={props.avatar_url}
                    className="rounded-box bg-white w-35 h-35 mr-4"
                />
            ) : undefined}
            <div className="flex flex-wrap gap-3 min-h-16">
                {props.username ? (
                    <div className="p-2 flex flex-col border border-border/50 rounded-box min-w-0 bg-base-200">
                        <div className="text-xs pb-1">User Name</div>
                        {props.username}
                    </div>
                ) : undefined}
                {props.html_url || props.username ? (
                    <div className="p-2 flex flex-col border border-border/50 rounded-box min-w-0 bg-base-200">
                        <div className="text-xs pb-1">GitHub URL</div>
                        <a
                            href={
                                props.html_url ??
                                "https://github.com/" + props.username
                            }
                            target="_blank"
                            className="text-primary underline"
                        >
                            {props.html_url ??
                                "https://github.com/" + props.username}
                        </a>
                    </div>
                ) : undefined}
                {props.user_type ? (
                    <div className="p-2 flex flex-col border border-border/50 rounded-box min-w-0 bg-base-200">
                        <div className="text-xs pb-1">User Type</div>
                        {props.user_type}
                    </div>
                ) : undefined}
            </div>
        </div>
    );
}

const ORDINAL = ["st", "nd", "rd", "th"];

interface CounterProps {
    rows: RankingRow<{ window_size: WindowSize }>[];
    windowSize: WindowSize;
}

function UserRankingCounter(props: CounterProps) {
    const row = props.rows.find(r => r.window_size === props.windowSize);
    const inner = (
        <>
            <div className="text-xs">Activity {props.windowSize}</div>
            <div
                className={
                    "w-fill h-full flex flex-row justify-center items-center pb-4 " +
                    (["text-4xl", "text-3xl", "text-2xl"][row?.rank ?? 3] ??
                        "text-xl")
                }
            >
                <Counter value={row ? row.rank + 1 : undefined} />
                <Letters
                    value={
                        row
                            ? row.rank > 10 && row.rank <= 20
                                ? "th"
                                : ORDINAL[row.rank % 10] ?? "th"
                            : ""
                    }
                    options={[...ORDINAL, ""]}
                    className="text-xs w-3.5 h-4 mb-2 ml-0.25"
                />
            </div>
        </>
    );
    return row ? (
        <Link
            to={`/?ur${props.windowSize}=${Math.max(
                0,
                row?.row_number - 4
            )}&urwin="${props.windowSize}"&ranking="user"`}
            className="m-2 p-2 border border-border/50 rounded-box min-w-0 block bg-base-200/80"
        >
            {inner}
        </Link>
    ) : (
        <div className="m-2 p-2 border border-border/50 rounded-box min-w-0 block bg-base-200/80">
            {inner}
        </div>
    );
}

interface RankingsProps {
    id: number;
}

function UserRankings(props: RankingsProps) {
    const activityRank = useTable(usersRanking.where("user_id", [props.id]));
    return (
        <>
            <UserRankingCounter rows={activityRank} windowSize="5m" />
            <UserRankingCounter rows={activityRank} windowSize="1h" />
            <UserRankingCounter rows={activityRank} windowSize="6h" />
            <UserRankingCounter rows={activityRank} windowSize="24h" />
        </>
    );
}

export default function UserPage() {
    const params = useParams();
    const userId = parseInt(params["userId"]!);
    const [loaded, userData] = useLoadingTable(users.where("id", [userId]));
    const user = userData[0];
    if (loaded && !user) {
        throw new Response("Invalid User", {
            status: 404,
            statusText: "Not Found",
        });
    }
    return (
        <div className="flex flex-col grow p-3">
            <div className="text-3xl font-semibold m-3 mt-0">
                {user ? (
                    <>
                        <span className="font-bold">@</span>
                        {user?.username}
                    </>
                ) : (
                    <span className="text-content/80">Loading...</span>
                )}
            </div>
            <div className="flex flex-col grow">
                <div className="m-2 p-2 flex flex-col border border-border/50 rounded-box min-w-0 min-h-20">
                    {user ? (
                        <UserDetails {...user} />
                    ) : (
                        <div className="w-full h-full flex justify-center items-center text-content/80">
                            Loading...
                        </div>
                    )}
                </div>
                <div className="md:flex-1 not-md:h-[30dvh] m-2 p-2 flex flex-col border border-border/50 rounded-box min-w-0">
                    <div className="text-xs">Rankings</div>
                    <div className="w-fill h-full grid grid-cols-2 grid-rows-2 md:grid-cols-4 md:grid-rows-1">
                        <UserRankings id={userId} />
                    </div>
                </div>
                <div className="md:flex-2 not-md:w-full not-md:h-[50dvh] m-2 p-2 flex flex-col border border-border/50 rounded-box min-w-0">
                    <div className="text-xs">Activity History</div>
                    <HistoryLong
                        table={usersHistory.where("user_id", [userId])}
                    />
                </div>
            </div>
        </div>
    );
}
