import { useMemo, useState } from "react";
import { Search } from "lucide-react";

import { users, repos } from "../api/tables";
import { useLoadingTable } from "../api/hooks";
import { useClickOutside, useDebounce } from "../hooks";
import { sortedKey } from "../util";
import { boldQuery } from "../utils";

interface Props {
    autoFocus?: boolean;
}

export default function GlobalSearch(props: Props) {
    const [query, setQuery] = useState("");
    const debounced = useDebounce(query.toLowerCase(), 250);
    const [userComplete, userResults] = useLoadingTable(
        users.where("username", { substr: debounced }).limit(10),
        debounced.length === 0
    );
    const [repoComplete, repoResults] = useLoadingTable(
        repos
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
                kind: "Repo",
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
                    <Search className="w-4 h-4 text-border stroke-3"></Search>
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
                <div className="absolute inset-y-full end-0 w-full hidden peer-focus-within:block z-50">
                    <div className="flex flex-col w-full bg-base-300 rounded-box p-4 py-3 shadow-xl dark:shadow-2xl">
                        {trueResults.length ? (
                            trueResults.map(row => {
                                return (
                                    <div key={row.id}>
                                        <div className="w-15 inline-block whitespace-nowrap overflow-hidden">
                                            {row.kind}
                                        </div>
                                        <div className="w-60 inline-block whitespace-nowrap overflow-hidden overflow-ellipsis">
                                            {boldQuery(row.name!, query)}
                                        </div>
                                    </div>
                                );
                            })
                        ) : (
                            <div className="text-content/80">
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
