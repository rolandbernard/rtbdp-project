import {
    Link,
    useLocation,
    useNavigate,
    useParams,
    useViewTransitionState,
} from "react-router";

import { useLoadingTable, useTable } from "../api/hooks";
import {
    repos,
    reposHistory,
    reposRanking,
    starsHistory,
    starsRanking,
    users,
    type WindowSize,
} from "../api/tables";
import type { RankingRow } from "../api/ranking";

import HistoryLong from "../ui/HistoryLong";
import Counter, { Letters } from "../ui/Counter";
import { useState } from "react";

interface OwnerProps {
    id?: number;
    name?: string;
}

function OwnerCard(props: OwnerProps) {
    const table = props.id
        ? users.where("id", [props.id])
        : users.where("username", [props.name]);
    const user = useTable(table)[0];
    const inTransition = useViewTransitionState(
        "/user/" + (user?.id ?? props.id)
    );
    return user ? (
        <div
            className="p-2 flex flex-col border border-border/50 rounded-box min-w-0 bg-base-200"
            style={{
                viewTransitionName: inTransition ? "pagerepo" : "none",
            }}
        >
            <div className="text-xs pb-1">Owner</div>
            <Link
                viewTransition
                to={"/user/" + user.id}
                state={{ from: "repo", name: user?.username }}
                className={
                    "text-primary font-semibold dark:hover:text-primary/90 hover:text-primary/75 " +
                    (user?.username ? "" : "text-primary/50")
                }
                title={user?.username}
            >
                <span
                    style={{
                        viewTransitionName: inTransition ? "namerepo" : "none",
                    }}
                >
                    <span className="font-bold">@</span>
                    {user?.username}
                </span>
            </Link>
        </div>
    ) : props.name ? (
        <div className="p-2 flex flex-col border border-border/50 rounded-box min-w-0 bg-base-200">
            <div className="text-xs pb-1">Owner</div>
            <div className="font-semibold">
                <span className="font-bold">@</span>
                {props.name}
            </div>
        </div>
    ) : undefined;
}

interface DetailsProps {
    reponame?: string;
    fullname?: string;
    owner_id?: number;
    html_url?: string;
    homepage?: string;
    descr?: string;
    topics?: string;
    lang?: string;
    license?: string;
    fork_count?: number;
    issue_count?: number;
    star_count?: number;
}

function RepoDetails(props: DetailsProps) {
    return (
        <div className="flex flex-wrap gap-3">
            {props.reponame || props.fullname ? (
                <div className="p-2 flex flex-col border border-border/50 rounded-box min-w-0 bg-base-200">
                    <div className="text-xs pb-1">Repository Name</div>
                    {props.reponame ??
                        props.fullname?.substring(
                            props.fullname.indexOf("/") + 1
                        )}
                </div>
            ) : undefined}
            {props.owner_id || props.fullname ? (
                <OwnerCard
                    id={props.owner_id}
                    name={
                        props.fullname
                            ? props.fullname.substring(
                                  0,
                                  props.fullname.indexOf("/")
                              )
                            : undefined
                    }
                />
            ) : undefined}
            {props.html_url || props.fullname ? (
                <div className="p-2 flex flex-col border border-border/50 rounded-box min-w-0 bg-base-200">
                    <div className="text-xs pb-1">GitHub URL</div>
                    <a
                        href={
                            props.html_url ??
                            "https://github.com/" + props.fullname
                        }
                        target="_blank"
                        className="text-primary underline dark:hover:text-primary/90 hover:text-primary/75"
                    >
                        {props.html_url ??
                            "https://github.com/" + props.fullname}
                    </a>
                </div>
            ) : undefined}
            {props.homepage ? (
                <div className="p-2 flex flex-col border border-border/50 rounded-box min-w-0 bg-base-200">
                    <div className="text-xs pb-1">Homepage</div>
                    <a
                        href={props.homepage}
                        target="_blank"
                        className="text-primary underline"
                    >
                        {props.homepage}
                    </a>
                </div>
            ) : undefined}
            {props.topics ? (
                <div className="p-2 flex flex-col border border-border/50 rounded-box min-w-0 bg-base-200">
                    <div className="text-xs pb-1">Topics</div>
                    {props.topics.split(" ").map(e => (
                        <span className="pe-1">{e}</span>
                    ))}
                </div>
            ) : undefined}
            {props.lang ? (
                <div className="p-2 flex flex-col border border-border/50 rounded-box min-w-0 bg-base-200">
                    <div className="text-xs pb-1">Language</div>
                    {props.lang}
                </div>
            ) : undefined}
            {props.license ? (
                <div className="p-2 flex flex-col border border-border/50 rounded-box min-w-0 bg-base-200">
                    <div className="text-xs pb-1">License</div>
                    {props.license}
                </div>
            ) : undefined}
            {props.fork_count ? (
                <div className="p-2 flex flex-col border border-border/50 rounded-box min-w-0 bg-base-200">
                    <div className="text-xs pb-1">Forks</div>
                    {props.fork_count}
                </div>
            ) : undefined}
            {props.issue_count ? (
                <div className="p-2 flex flex-col border border-border/50 rounded-box min-w-0 bg-base-200">
                    <div className="text-xs pb-1">Issues</div>
                    {props.issue_count}
                </div>
            ) : undefined}
            {props.star_count ? (
                <div className="p-2 flex flex-col border border-border/50 rounded-box min-w-0 bg-base-200">
                    <div className="text-xs pb-1">Stars</div>
                    {props.star_count}
                </div>
            ) : undefined}
            {props.descr ? (
                <div className="p-2 flex flex-col border border-border/50 rounded-box min-w-0 bg-base-200">
                    <div className="text-xs pb-1">Description</div>
                    {props.descr}
                </div>
            ) : undefined}
        </div>
    );
}

const ORDINAL = ["st", "nd", "rd", "th"];

interface CounterProps {
    rows: RankingRow<{
        window_size: WindowSize;
        num_stars?: number;
        num_events?: number;
    }>[];
    windowSize: WindowSize;
    kind: string;
    onNavigate: (to: string) => void;
}

function RepoRankingCounter(props: CounterProps) {
    const row = props.rows.find(r => r.window_size === props.windowSize);
    const inner = (
        <>
            <div className="text-xs">
                {props.kind[0]?.toUpperCase() + props.kind.slice(1)}{" "}
                {props.windowSize}
            </div>
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
                                    : ORDINAL[row.rank % 10] ?? "th"
                                : ""
                        }
                        options={[...ORDINAL, ""]}
                        className="text-xs w-3.5 h-4 mb-2 ml-px"
                    />
                </div>
                <div className="flex flex-row justify-center items-center pt-1 text-content/70">
                    <span className="pr-1">
                        <Counter
                            value={row?.num_events ?? row?.num_stars ?? 0}
                        />
                    </span>
                    {props.kind === "stars" ? "stars" : "events"}
                </div>
            </div>
        </>
    );
    return row ? (
        <div
            className="m-2 p-2 border border-border/50 rounded-box min-w-0 cursor-pointer
                bg-base-200/80 hover:bg-content/7 hover:dark:bg-content/10"
            onClick={() =>
                props.onNavigate(
                    `/?rrkind=${props.kind}&rr${props.kind}${
                        props.windowSize
                    }=${Math.max(0, row?.row_number - 4)}&rrwin=${
                        props.windowSize
                    }`
                )
            }
        >
            {inner}
        </div>
    ) : (
        <div className="m-2 p-2 border border-border/50 rounded-box min-w-0 block bg-base-200/80">
            {inner}
        </div>
    );
}

interface RankingsProps {
    id: number;
    onNavigate: (to: string) => void;
}

function RepoRankings(props: RankingsProps) {
    const activityRank = useTable(reposRanking.where("repo_id", [props.id]));
    const starsRank = useTable(starsRanking.where("repo_id", [props.id]));
    return (
        <>
            <RepoRankingCounter
                rows={activityRank}
                windowSize="5m"
                kind="activity"
                onNavigate={props.onNavigate}
            />
            <RepoRankingCounter
                rows={activityRank}
                windowSize="1h"
                kind="activity"
                onNavigate={props.onNavigate}
            />
            <RepoRankingCounter
                rows={activityRank}
                windowSize="6h"
                kind="activity"
                onNavigate={props.onNavigate}
            />
            <RepoRankingCounter
                rows={activityRank}
                windowSize="24h"
                kind="activity"
                onNavigate={props.onNavigate}
            />
            <RepoRankingCounter
                rows={starsRank}
                windowSize="5m"
                kind="stars"
                onNavigate={props.onNavigate}
            />
            <RepoRankingCounter
                rows={starsRank}
                windowSize="1h"
                kind="stars"
                onNavigate={props.onNavigate}
            />
            <RepoRankingCounter
                rows={starsRank}
                windowSize="6h"
                kind="stars"
                onNavigate={props.onNavigate}
            />
            <RepoRankingCounter
                rows={starsRank}
                windowSize="24h"
                kind="stars"
                onNavigate={props.onNavigate}
            />
        </>
    );
}

export default function RepoPage() {
    const navigate = useNavigate();
    const [navigating, setNavigating] = useState(false);
    const location = useLocation();
    const params = useParams();
    const repoId = parseInt(params["repoId"]!);
    const [loaded, repoData] = useLoadingTable(repos.where("id", [repoId]));
    const repo = repoData[0];
    if (loaded && !repo) {
        throw new Response("Invalid Repository", {
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
            <div className="text-3xl font-semibold m-3 mt-0">
                <span
                    style={{
                        viewTransitionName:
                            "name" +
                            (navigating ? "ranking" : location.state?.from),
                    }}
                >
                    {repo || location.state?.name ? (
                        repo?.fullname ?? repo?.reponame ?? location.state.name
                    ) : (
                        <span className="text-content/80">Loading...</span>
                    )}
                </span>
            </div>
            <div className="flex flex-col grow">
                <div className="m-2 p-2 flex flex-col border border-border/50 rounded-box min-w-0 min-h-20">
                    {repo ? (
                        <RepoDetails {...repo} />
                    ) : (
                        <div className="w-full h-full flex justify-center items-center text-content/80">
                            Loading...
                        </div>
                    )}
                </div>
                <div
                    className="md:flex-1 not-md:h-[50dvh] m-2 p-2 flex flex-col border border-border/50 rounded-box min-w-0"
                    style={{
                        viewTransitionName:
                            navigating || location.state?.from === "ranking"
                                ? "ranking"
                                : "none",
                    }}
                >
                    <div className="text-xs">Rankings</div>
                    <div className="w-fill h-full grid grid-cols-2 grid-rows-4 md:grid-cols-4 md:grid-rows-2">
                        <RepoRankings
                            id={repoId}
                            onNavigate={to => {
                                setNavigating(true);
                                navigate(to, { viewTransition: true });
                            }}
                        />
                    </div>
                </div>
                <div
                    className="flex flex-wrap grow"
                    style={{
                        viewTransitionName:
                            navigating || location.state?.from === "ranking"
                                ? "chart"
                                : "none",
                    }}
                >
                    <div className="md:flex-1 not-md:w-full not-md:h-[50dvh] m-2 p-2 flex flex-col border border-border/50 rounded-box min-w-0">
                        <div className="text-xs">Activity History</div>
                        <div className="w-full h-full">
                            <HistoryLong
                                table={reposHistory.where("repo_id", [repoId])}
                            />
                        </div>
                    </div>
                    <div className="md:flex-1 not-md:w-full not-md:h-[50dvh] m-2 p-2 flex flex-col border border-border/50 rounded-box min-w-0">
                        <div className="text-xs">Stars History</div>
                        <div className="w-full h-full">
                            <HistoryLong
                                table={starsHistory.where("repo_id", [repoId])}
                                chartColor="#78b120"
                            />
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}
