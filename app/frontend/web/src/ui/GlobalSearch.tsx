import { useMemo, useState } from "react";
import { Link } from "react-router";
import { Bot, Building2, FolderGit2, Search, User } from "lucide-react";

import { users, repos } from "../api/tables";
import { useLoadingTable } from "../api/hooks";
import { useClickOutside, useDebounce } from "../hooks";
import { sortedKey } from "../util";
import { boldQuery } from "../utils";

interface UserRowProps {
    userId: number;
    username: string;
    avatarUrl?: string;
    kind: string;
    query: string;
}

function UserSearchRow(props: UserRowProps) {
    const kindIcon = {
        Bot: <Bot />,
        User: <User />,
        Organization: <Building2 />,
    }[props.kind] ?? <User />;
    return (
        <div className="p-1 flex flex-row items-center">
            <div className="w-12 h-6 inline-flex items-center justify-center shrink-0">
                {props.avatarUrl ? (
                    <img
                        src={props.avatarUrl}
                        className="w-6 h-6 rounded-box bg-white"
                    />
                ) : (
                    kindIcon
                )}
            </div>
            <Link
                to={"/user/" + props.userId}
                title={props.username}
                className="min-w-0 whitespace-nowrap overflow-hidden overflow-ellipsis
                    text-primary font-semibold dark:hover:text-primary/90 hover:text-primary/75"
            >
                <span className="font-bold">@</span>
                {boldQuery(props.username, props.query)}
            </Link>
        </div>
    );
}

interface RepoRowProps {
    repoId: number;
    reponame: string;
    query: string;
}

function RepoSearchRow(props: RepoRowProps) {
    return (
        <div className="p-1 flex flex-row items-center">
            <div className="w-12 inline-flex items-center justify-center shrink-0">
                <FolderGit2 />
            </div>
            <Link
                to={"/repo/" + props.repoId}
                title={props.reponame}
                className="flex-1 min-w-0 whitespace-nowrap overflow-hidden overflow-ellipsis
                    text-primary font-semibold text-left dark:hover:text-primary/90 hover:text-primary/75"
                style={{ direction: "rtl" }}
            >
                {boldQuery(props.reponame, props.query)}
            </Link>
        </div>
    );
}

interface Props {
    autoFocus?: boolean;
}

export default function GlobalSearch(props: Props) {
    const [query, setQuery] = useState("");
    const debounced = useDebounce(query.toLowerCase(), 250);
    const [userComplete, userResults] = useLoadingTable(
        users
            .where("username", [debounced])
            .or()
            .where("username", { substr: debounced })
            .limit(10),
        debounced.length === 0
    );
    const [repoComplete, repoResults] = useLoadingTable(
        repos
            .where("reponame", [debounced])
            .or()
            .where("fullname", [debounced])
            .or()
            .where("reponame", { substr: debounced })
            .or()
            .where("fullname", { substr: debounced })
            .limit(10),
        debounced.length === 0
    );
    const results = useMemo(() => {
        const combined = [];
        for (const user of userResults ?? []) {
            combined.push({
                ...user,
                name: user.username,
                kind: user.user_type ?? "User",
            });
        }
        for (const repo of repoResults ?? []) {
            combined.push({
                ...repo,
                name: repo.fullname ?? repo.reponame,
            });
        }
        combined.sort(sortedKey([e => e.name?.length ?? 0]));
        return combined;
    }, [userResults, repoResults]);
    const trueResults = results
        .filter(row => row.name?.toLowerCase()?.includes(query.toLowerCase()))
        .slice(0, 10);
    useClickOutside("div#global-search", () => setQuery(""));
    return (
        <div id="global-search" className="relative">
            <div className="peer w-full relative block">
                <div className="absolute inset-y-0 start-0 flex items-center ps-3 pointer-events-none">
                    <Search className="w-4 h-4 text-border stroke-3" />
                </div>
                <input
                    type="text"
                    className="block w-full ps-9 pe-3 border-2 border-border outline-none text-sm
                        rounded-field px-2 py-2 focus-visible:border-primary placeholder:text-content/65
                        placeholder:italic hover:bg-content/3 dark:hover:bg-content/8"
                    placeholder="Search for users or repositories..."
                    value={query}
                    autoFocus={props.autoFocus}
                    onChange={e => setQuery(e.target.value)}
                />
            </div>
            {query.length !== 0 ? (
                <div
                    className="absolute inset-y-full end-0 w-full hidden focus-within:block
                        peer-focus-within:block hover:block active:block z-50"
                    onClick={() => setQuery("")}
                >
                    <div className="flex flex-col w-full bg-base-300 rounded-box p-1 py-3 shadow-xl dark:shadow-2xl">
                        {trueResults.length ? (
                            trueResults.map(row => {
                                return "kind" in row ? (
                                    <UserSearchRow
                                        key={row.id}
                                        userId={row.id}
                                        username={row.username!}
                                        avatarUrl={row.avatar_url}
                                        kind={row.kind}
                                        query={query}
                                    />
                                ) : (
                                    <RepoSearchRow
                                        key={row.id}
                                        repoId={row.id}
                                        reponame={row.fullname ?? row.reponame!}
                                        query={query}
                                    />
                                );
                            })
                        ) : (
                            <div className="text-content/80 px-4">
                                {query !== debounced ||
                                !userComplete ||
                                !repoComplete
                                    ? "Searching..."
                                    : "No results found."}
                            </div>
                        )}
                    </div>
                </div>
            ) : undefined}
        </div>
    );
}
