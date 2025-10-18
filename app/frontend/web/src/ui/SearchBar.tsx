import { useMemo, useState, type Ref } from "react";
import { FaMagnifyingGlass } from "react-icons/fa6";

import { users, repos } from "../api/tables";
import { useLoadingTable } from "../api/hooks";
import { useDebounce } from "../hooks";
import { sortedKey } from "../util";

function boldQuery(text: string, query: string) {
    const index = text.toLowerCase().indexOf(query.toLowerCase());
    if (index == -1) {
        return <span>{text}</span>;
    } else {
        return (
            <>
                <span>{text.substring(0, index)}</span>
                <span className="font-bold">
                    {text.substring(index, index + query.length)}
                </span>
                <span>{text.substring(index + query.length)}</span>
            </>
        );
    }
}

interface Props {
    inputRef?: Ref<HTMLInputElement>;
}

export default function SearchBar(props: Props) {
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
        .filter(row => row.name?.includes(query))
        .slice(0, 10);
    return (
        <div className="relative">
            <div className="peer w-full relative block">
                <div className="absolute inset-y-0 start-0 flex items-center ps-3 pointer-events-none">
                    <FaMagnifyingGlass className="w-4 h-4 text-base-content/65"></FaMagnifyingGlass>
                </div>
                <input
                    type="text"
                    className="block w-full ps-9 pe-3 border-2 border-base-content/65 outline-none text-sm rounded-field px-2 py-2 focus-visible:border-primary placeholder:text-base-content/65 placeholder:italic"
                    placeholder="Search for users or repositories..."
                    value={query}
                    ref={props.inputRef}
                    onBlur={() => setQuery("")}
                    onChange={e => setQuery(e.target.value)}
                />
            </div>
            {query.length !== 0 ? (
                <div className="absolute inset-y-full end-0 w-full hidden peer-focus-within:block">
                    <div className="flex flex-col w-full bg-base-300 rounded-box p-4">
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
                        ) : query !== debounced ||
                          !userComplete ||
                          !repoComplete ? (
                            <div>Searching...</div>
                        ) : (
                            <div>No results found.</div>
                        )}
                    </div>
                </div>
            ) : undefined}
        </div>
    );
}
