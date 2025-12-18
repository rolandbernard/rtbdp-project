import { useLocation, useNavigate, useParams } from "react-router";

import { useTable } from "../api/hooks";
import {
    users,
    usersHistory,
    usersRanking,
    type WindowSize,
} from "../api/tables";
import type { RankingRow } from "../api/ranking";

import HistoryLong from "../ui/HistoryLong";
import Counter, { Letters } from "../ui/Counter";
import { useState } from "react";

interface DetailsProps {
    username?: string;
    avatar_url?: string;
    html_url?: string;
    user_type?: string;
    from: string;
}

function UserDetails(props: DetailsProps) {
    return (
        <div className="flex flex-row items-center">
            {props.avatar_url ? (
                <img
                    src={props.avatar_url}
                    className="rounded-box bg-white w-35 h-35 mr-4"
                    style={{
                        viewTransitionName: props.from.startsWith("search")
                            ? "avatar" + props.from
                            : "none",
                    }}
                />
            ) : undefined}
            <div className="flex flex-wrap gap-3 min-h-16">
                {props.username ? (
                    <div className="p-2 flex flex-col border border-border/50 rounded-box min-w-0 bg-base-200">
                        <div className="text-xs pb-1">User Name</div>
                        <span className="select-text">{props.username}</span>
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
                            className="text-primary underline dark:hover:text-primary/90 hover:text-primary/75 select-text"
                        >
                            {props.html_url ??
                                "https://github.com/" + props.username}
                        </a>
                    </div>
                ) : undefined}
                {props.user_type ? (
                    <div className="p-2 flex flex-col border border-border/50 rounded-box min-w-0 bg-base-200">
                        <div className="text-xs pb-1">User Type</div>
                        <span className="select-text">{props.user_type}</span>
                    </div>
                ) : undefined}
            </div>
        </div>
    );
}

const ORDINAL = ["st", "nd", "rd", "th"];

interface CounterProps {
    rows: RankingRow<{ window_size: WindowSize; num_events: number }>[];
    loaded: boolean;
    windowSize: WindowSize;
    onNavigate: (to: string) => void;
}

function UserRankingCounter(props: CounterProps) {
    const row = props.rows.find(r => r.window_size === props.windowSize);
    const inner = (
        <>
            <div className="text-xs">Activity {props.windowSize}</div>
            <div
                className={
                    "w-full h-full flex flex-col justify-center items-center"
                }
            >
                <div
                    className={
                        "flex flex-row justify-center items-center " +
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
                                    : (ORDINAL[row.rank % 10] ?? "th")
                                : ""
                        }
                        options={[...ORDINAL, ""]}
                        className="text-xs w-3.5 h-4 mb-2 ml-px"
                    />
                </div>
                <div className="flex flex-row justify-center items-center pt-1 text-content/70">
                    <Counter value={row?.num_events ?? 0} className="pr-1" />
                    events
                </div>
            </div>
        </>
    );
    return row ? (
        <div
            className={
                "m-2 p-2 border border-border/50 rounded-box min-w-0 cursor-pointer " +
                "bg-base-200/80 hover:bg-content/7 hover:dark:bg-content/10 " +
                (props.loaded ? "" : "loading")
            }
            onClick={() =>
                props.onNavigate(
                    `/?ur${props.windowSize}=${Math.max(
                        0,
                        row?.row_number - 4,
                    )}&urwin=${props.windowSize}&ranking=user`,
                )
            }
        >
            {inner}
        </div>
    ) : (
        <div
            className={
                "m-2 p-2 border border-border/50 rounded-box min-w-0 block bg-base-200/80 " +
                (props.loaded ? "" : "loading")
            }
        >
            {inner}
        </div>
    );
}

interface RankingsProps {
    id: number;
    onNavigate: (to: string) => void;
}

function UserRankings(props: RankingsProps) {
    const [loaded, activityRank] = useTable(
        usersRanking.where("user_id", [props.id]),
    );
    return (
        <>
            <UserRankingCounter
                rows={activityRank}
                loaded={loaded}
                windowSize="5m"
                onNavigate={props.onNavigate}
            />
            <UserRankingCounter
                rows={activityRank}
                loaded={loaded}
                windowSize="1h"
                onNavigate={props.onNavigate}
            />
            <UserRankingCounter
                rows={activityRank}
                loaded={loaded}
                windowSize="6h"
                onNavigate={props.onNavigate}
            />
            <UserRankingCounter
                rows={activityRank}
                loaded={loaded}
                windowSize="24h"
                onNavigate={props.onNavigate}
            />
        </>
    );
}

export default function UserPage() {
    const navigate = useNavigate();
    const [navigating, setNavigating] = useState(false);
    const location = useLocation();
    const params = useParams();
    const userId = parseInt(params["userId"]!);
    const [loaded, userData] = useTable(users.where("id", [userId]));
    const user = userData[0];
    if (loaded && !user) {
        throw new Response("Invalid User", {
            status: 404,
            statusText: "Not Found",
        });
    }
    return (
        <div
            className="flex flex-col grow p-3"
            style={{
                viewTransitionName:
                    "page" + (navigating ? "ranking" : location.state?.from),
            }}
        >
            <div className="text-3xl font-semibold m-3 mt-0 select-text">
                <span
                    style={{
                        viewTransitionName:
                            "name" +
                            (navigating ? "ranking" : location.state?.from),
                    }}
                >
                    {user || location.state?.name ? (
                        <>
                            <span className="font-bold">@</span>
                            {user?.username ?? location.state?.name}
                        </>
                    ) : (
                        <span className="text-content/80">Loading...</span>
                    )}
                </span>
            </div>
            <div className="flex flex-col grow">
                <div
                    className={
                        "m-2 p-2 flex flex-col border border-border/50 rounded-box min-w-0 min-h-40 " +
                        (loaded ? "" : "loading")
                    }
                >
                    {user ? (
                        <UserDetails
                            {...user}
                            from={location.state?.from ?? ""}
                        />
                    ) : (
                        <div className="w-full h-full flex justify-center items-center text-content/80">
                            Loading...
                        </div>
                    )}
                </div>
                <div
                    className="md:flex-1 not-md:h-85 m-2 p-2 flex flex-col border border-border/50 rounded-box min-w-0"
                    style={{
                        viewTransitionName:
                            navigating || location.state?.from === "ranking"
                                ? "ranking"
                                : "none",
                    }}
                >
                    <div className="text-xs">Rankings</div>
                    <div className="w-fill h-full grid grid-cols-2 grid-rows-2 md:grid-cols-4 md:grid-rows-1">
                        <UserRankings
                            id={userId}
                            onNavigate={to => {
                                setNavigating(true);
                                navigate(to, { viewTransition: true });
                            }}
                        />
                    </div>
                </div>
                <div
                    className="md:flex-2 not-md:w-full not-md:h-128 m-2 p-2 flex flex-col border border-border/50 rounded-box min-w-0"
                    style={{
                        viewTransitionName:
                            navigating || location.state?.from === "ranking"
                                ? "chart"
                                : "none",
                    }}
                >
                    <div className="text-xs">Activity History</div>
                    <HistoryLong
                        table={usersHistory.where("user_id", [userId])}
                    />
                </div>
            </div>
        </div>
    );
}
